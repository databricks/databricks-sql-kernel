# Copyright (c) 2026 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sync execute / fetch / close against a local SEA stand-in (no network)."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import signal
import threading
import time
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import databricks_sql_kernel as k
import pyarrow as pa
import pytest

SESSIONS = "/api/2.0/sql/warehouses/test-wh/sessions"


def _ipc(column):
    batch = pa.record_batch([column], names=["n"])
    buffer = pa.BufferOutputStream()
    with pa.ipc.new_stream(buffer, batch.schema) as writer:
        writer.write_batch(batch)
    return buffer.getvalue().to_pybytes()


@contextlib.contextmanager
def _server(execute_delay_s=0.0, column=None, type_name="BIGINT", chunk_delay_s=None):
    """Yields ``(port, requests)``; ``requests`` collects ``(method, path)``.
    With ``chunk_delay_s`` the result is an external link served after that delay."""
    requests = []
    ipc = _ipc(column if column is not None else pa.array([1, 2], type=pa.int64()))
    ports = []

    class Handler(BaseHTTPRequestHandler):
        def respond(self, body, headers=()):
            requests.append((self.command, self.path))
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            payload = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            if self.path == SESSIONS:
                self.respond({"session_id": "test-session"}, [("x-databricks-rt-session", "true")])
                return
            if self.path.endswith("/statements"):
                time.sleep(execute_delay_s)
            chunk = {"chunk_index": 0, "row_offset": 0, "row_count": 2, "byte_count": len(ipc)}
            if chunk_delay_s is None:
                result = dict(chunk, attachment=base64.b64encode(ipc).decode())
            else:
                link = f"http://127.0.0.1:{ports[0]}/chunk0"
                result = {"external_links": [dict(chunk, external_link=link, expiration="2099-01-01T00:00:00.000Z")]}
            column_info = {"name": "n", "type_name": type_name, "type_text": type_name, "position": 0}
            self.respond(
                {
                    "statement_id": "test-statement",
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "format": "ARROW_STREAM",
                        "schema": {"column_count": 1, "columns": [column_info]},
                        "total_chunk_count": 1,
                        "total_row_count": 2,
                    },
                    "result": result,
                }
            )

        def do_GET(self):
            time.sleep(chunk_delay_s or 0)
            self.respond(ipc)

        def do_DELETE(self):
            self.respond({})

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.daemon_threads = True
        ports.append(server.server_port)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            yield server.server_port, requests
        finally:
            server.shutdown()


def _session(port):
    return k.Session(
        host=f"http://127.0.0.1:{port}",
        http_path="/sql/1.0/warehouses/test-wh",
        access_token="test-token",
        telemetry_enabled=False,
    )


def _statement_deletes(requests):
    return [p for m, p in requests if m == "DELETE" and "/statements/" in p]


@pytest.mark.parametrize(
    "server_kwargs,rows",
    [
        # Inline batch already in memory: read without releasing the GIL.
        pytest.param({}, [1, 2], id="inline"),
        # Chunk still downloading on the first read: falls back to the blocking path.
        pytest.param({"chunk_delay_s": 0.3}, [1, 2], id="cloudfetch-pending"),
        # Untyped NULL is rewritten to Utf8 per batch: read on the blocking path.
        pytest.param({"column": pa.nulls(2), "type_name": "STRING"}, [None, None], id="rewritten"),
    ],
)
def test_worker_thread_fetch_closes_statement_once(server_kwargs, rows):
    with _server(**server_kwargs) as (port, requests):
        session = _session(port)
        result = {}

        def run():
            stmt = session.statement()
            stmt.set_sql("SELECT 1")
            executed = stmt.execute()
            first = executed.fetch_next_batch()
            result["rows"] = first.column(0).to_pylist()
            result["end"] = executed.fetch_next_batch()
            result["again"] = executed.fetch_next_batch()
            executed.close()
            stmt.close()

        worker = threading.Thread(target=run)
        worker.start()
        worker.join(10)
        assert not worker.is_alive()
        assert result == {"rows": rows, "end": None, "again": None}
        if "chunk_delay_s" in server_kwargs:
            assert ("GET", "/chunk0") in requests
        # Draining fires CloseStatement; the explicit close sends no second one.
        deadline = time.monotonic() + 5
        while not _statement_deletes(requests) and time.monotonic() < deadline:
            time.sleep(0.02)
        time.sleep(0.1)
        assert len(_statement_deletes(requests)) == 1
        session.close()


@pytest.mark.skipif(not hasattr(signal, "SIGINT") or os.name == "nt", reason="POSIX signals")
@pytest.mark.parametrize("greenlet_ident", [False, True], ids=["plain", "greenlet-ident"])
def test_main_thread_execute_stays_interruptible(monkeypatch, greenlet_ident):
    assert threading.current_thread() is threading.main_thread()
    if greenlet_ident:
        # Under gevent/eventlet, get_ident() is a greenlet id that matches no
        # thread's ident; only the OS-level native id still identifies main.
        main = threading.main_thread()
        fake = types.SimpleNamespace(ident=object(), native_id=main.native_id)
        monkeypatch.setattr(threading, "main_thread", lambda: fake)
    with _server(execute_delay_s=5.0) as (port, _):
        session = _session(port)
        stmt = session.statement()
        stmt.set_sql("SELECT 1")
        timer = threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGINT))
        started = time.monotonic()
        timer.start()
        try:
            with pytest.raises(KeyboardInterrupt):
                stmt.execute()
        finally:
            timer.cancel()
        assert time.monotonic() - started < 2.0
        stmt.close()
        session.close()
