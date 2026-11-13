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

"""Sessions share one process-wide tokio runtime (no network)."""

from __future__ import annotations

import base64
import contextlib
import gc
import itertools
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import databricks_sql_kernel as k
import pyarrow as pa
import pytest

SESSIONS = "/api/2.0/sql/warehouses/test-wh/sessions"


def _attachment():
    batch = pa.record_batch([pa.array([1, 2], type=pa.int64())], names=["n"])
    buffer = pa.BufferOutputStream()
    with pa.ipc.new_stream(buffer, batch.schema) as writer:
        writer.write_batch(batch)
    return base64.b64encode(buffer.getvalue()).decode()


@contextlib.contextmanager
def _server():
    """Local SEA stand-in: unique session / statement ids, inline results.
    Yields ``(port, requests)``; ``requests`` collects ``(method, path)``."""
    requests = []
    ids = itertools.count(1)
    attachment = _attachment()

    class Handler(BaseHTTPRequestHandler):
        def respond(self, body, headers=()):
            requests.append((self.command, self.path))
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            payload = json.dumps(body).encode()
            try:
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                for name, value in headers:
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass  # The client went away (e.g. a pooled connection was dropped).

        def do_POST(self):
            if self.path == SESSIONS:
                self.respond({"session_id": f"s{next(ids)}"}, [("x-databricks-rt-session", "true")])
                return
            self.respond(
                {
                    "statement_id": f"q{next(ids)}",
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "format": "ARROW_STREAM",
                        "schema": {
                            "column_count": 1,
                            "columns": [
                                {"name": "n", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0}
                            ],
                        },
                        "total_chunk_count": 1,
                        "total_row_count": 2,
                    },
                    "result": {"chunk_index": 0, "row_offset": 0, "row_count": 2, "attachment": attachment},
                }
            )

        def do_DELETE(self):
            self.respond({})

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
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


def _query(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 1")
    executed = stmt.execute()
    rows = 0
    while (batch := executed.fetch_next_batch()) is not None:
        rows += batch.num_rows
    executed.close()
    stmt.close()
    return rows


def _wait_for(predicate, timeout_s=5.0):
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.02)
    return True


@pytest.mark.skipif(not os.path.isdir("/proc/self/task"), reason="counts threads via /proc")
def test_sessions_do_not_add_runtime_threads():
    with _server() as (port, _):
        first = _session(port)
        baseline = len(os.listdir("/proc/self/task"))
        others = [_session(port) for _ in range(8)]
        try:
            # A runtime per session added a worker per CPU for each one.
            assert len(os.listdir("/proc/self/task")) < baseline + len(others)
        finally:
            for session in [first, *others]:
                session.close()


def test_cleanup_of_closed_and_dropped_sessions_completes():
    with _server() as (port, requests):
        keep = _session(port)
        closed = _session(port)
        assert _query(closed) == 2
        closed.close()
        dropped = _session(port)
        assert _query(dropped) == 2
        dropped_path = f"{SESSIONS}/{dropped.session_id}"
        # Drop fires DeleteSession in the background on the shared runtime.
        del dropped
        gc.collect()
        assert _query(keep) == 2

        def deletes():
            return [path for method, path in requests if method == "DELETE"]

        def session_deletes():
            return [path for path in deletes() if "/statements/" not in path]

        assert _wait_for(lambda: len(deletes()) - len(session_deletes()) == 3)
        assert _wait_for(lambda: dropped_path in session_deletes())
        keep.close()
        assert len(session_deletes()) == 3


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork()")
def test_forked_child_opens_sessions():
    with _server() as (port, _):
        parent = _session(port)
        assert _query(parent) == 2
        pid = os.fork()
        if pid == 0:
            # The parent's workers don't exist here; the child needs its own runtime.
            code = 1
            try:
                child = _session(port)
                if _query(child) == 2:
                    code = 0
                child.close()
            finally:
                sys.stdout.flush()
                os._exit(code)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                assert os.waitstatus_to_exitcode(status) == 0
                break
            time.sleep(0.05)
        else:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            pytest.fail("query hung in the forked child")
        assert _query(parent) == 2
        parent.close()
