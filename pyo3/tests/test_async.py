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

"""Async statement: submit / status / await_result / fetch_result /
Failed status."""

from __future__ import annotations

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import databricks_sql_kernel as k
import pyarrow as pa
import pytest

from conftest import live


def _wait_terminal(async_exec, timeout_s: float = 2.0):
    """Poll status() until terminal or timeout. Returns the final
    (state, failure) tuple."""
    deadline = time.monotonic() + timeout_s
    while True:
        state, failure = async_exec.status()
        if state in ("Succeeded", "Closed", "Failed", "Cancelled"):
            return state, failure
        if time.monotonic() > deadline:
            return state, failure
        time.sleep(0.1)


@live
def test_submit_and_await_result(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 7 AS n")
    async_exec = stmt.submit()
    try:
        assert async_exec.statement_id
        state, failure = async_exec.status()
        assert state in ("Pending", "Running", "Succeeded", "Closed"), state
        assert failure is None
        rs = async_exec.await_result()
        try:
            table = rs.fetch_all_arrow()
            assert table.num_rows == 1
            assert table.column(0).to_pylist() == [7]
        finally:
            rs.close()
        # After await_result, status must be terminal.
        state, _ = async_exec.status()
        assert state in ("Succeeded", "Closed"), state
    finally:
        async_exec.close()
        stmt.close()


@live
def test_submit_then_fetch_result_after_terminal(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 99 AS n")
    async_exec = stmt.submit()
    try:
        state, _ = _wait_terminal(async_exec)
        assert state in ("Succeeded", "Closed"), state
        rs = async_exec.fetch_result()
        try:
            table = rs.fetch_all_arrow()
            assert table.num_rows == 1
            assert table.column(0).to_pylist() == [99]
        finally:
            rs.close()
    finally:
        async_exec.close()
        stmt.close()


@live
def test_submit_failed_status_carries_kernel_error(session):
    stmt = session.statement()
    stmt.set_sql("SELECT * FROM definitely_not_a_table_xyz_async_test")
    async_exec = stmt.submit()
    try:
        state, failure = _wait_terminal(async_exec)
        assert state == "Failed", state
        assert isinstance(failure, k.KernelError), type(failure)
        assert failure.code == "SqlError", failure.code
        assert failure.sql_state == "42P01", failure.sql_state
        assert failure.query_id, "query_id should be populated"
    finally:
        async_exec.close()
        stmt.close()


@live
def test_use_after_close_raises_kernel_error(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 1")
    async_exec = stmt.submit()
    async_exec.close()
    async_exec.close()  # idempotent
    try:
        async_exec.status()
    except k.KernelError as e:
        assert "closed" in str(e), str(e)
        assert e.code == "InvalidStatementHandle", e.code
    else:
        raise AssertionError("expected KernelError on use-after-close")
    stmt.close()


@pytest.mark.parametrize(
    "failed,count",
    [
        pytest.param(False, 3, id="succeeded-positive"),
        pytest.param(False, 0, id="succeeded-zero"),
        pytest.param(True, None, id="failed"),
    ],
)
def test_reader_preserves_session_and_maps_errors(failed, count):
    requests = []
    statement_path = "/api/2.0/sql/warehouses/test-wh/sessions/test-session/statements/test-statement"
    response = {
        "statement_id": "test-statement",
        "status": {
            "state": "FAILED",
            "sql_state": "42P01",
            "error": {"message": "invalid query", "error_code": "TABLE_OR_VIEW_NOT_FOUND"},
        },
    }
    if not failed:
        batch = pa.record_batch([pa.array([1, 2], type=pa.int64())], names=["n"])
        buffer = pa.BufferOutputStream()
        with pa.ipc.new_stream(buffer, batch.schema) as writer:
            writer.write_batch(batch)
        attachment = base64.b64encode(buffer.getvalue()).decode()
        response = {
            "statement_id": "test-statement",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": 1,
                    "columns": [{"name": "n", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0}],
                },
                "total_chunk_count": 1,
                "total_row_count": 2,
            },
            "result": {"chunk_index": 0, "row_offset": 0, "row_count": 2, "attachment": attachment},
        }
        if count is not None:
            response["status"]["num_modified_rows"] = count

    class Handler(BaseHTTPRequestHandler):
        def respond(self, body, realtime=False):
            requests.append((self.command, self.path))
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            if realtime:
                self.send_header("x-databricks-rt-session", "true")
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            if self.path.endswith("/sessions"):
                self.respond({"session_id": "test-session"}, realtime=True)
            else:
                self.respond({"statement_id": "test-statement", "status": {"state": "PENDING"}})

        def do_GET(self):
            self.respond(response)

        def do_DELETE(self):
            self.respond({})

        def log_message(self, *_args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        session = None
        try:
            session = k.Session(
                host=f"http://127.0.0.1:{server.server_port}",
                http_path="/sql/1.0/warehouses/test-wh",
                access_token="test-token",
                telemetry_enabled=False,
            )
            stmt = session.statement()
            stmt.set_sql("SELECT 1")
            owner = stmt.submit()
            assert owner.num_modified_rows is None
            before_reader = len(requests)
            reader = owner.reader()
            assert len(requests) == before_reader
            assert isinstance(reader, k.AsyncStatementReader)
            assert reader.statement_id == owner.statement_id
            assert not hasattr(reader, "cancel")
            assert not hasattr(reader, "close")
            assert reader.num_modified_rows is None
            state, failure = reader.status()
            assert reader.num_modified_rows == count
            assert owner.num_modified_rows is None
            owner.status()
            assert owner.num_modified_rows == count
            streams = []
            if failed:
                assert state == "Failed"
                assert isinstance(failure, k.KernelError)
                assert failure.code == "SqlError"
                assert failure.sql_state == "42P01"
                for read in (reader.await_result, reader.fetch_result):
                    with pytest.raises(k.KernelError) as error:
                        read()
                    assert error.value.code == "SqlError"
                    assert error.value.sql_state == "42P01"
                del read
            else:
                assert (state, failure) == ("Succeeded", None)
                streams = [reader.await_result(), reader.fetch_result()]
                assert all(isinstance(stream, k.ResultStream) for stream in streams)
            expected_reads = 4 if failed else 5
            assert [path for method, path in requests if method == "GET"] == [statement_path] * expected_reads
            owner.close()
            with pytest.raises(k.KernelError) as error:
                _ = owner.num_modified_rows
            assert error.value.code == "InvalidStatementHandle"
            with pytest.raises(k.KernelError) as error:
                _ = reader.num_modified_rows
            assert error.value.code == "InvalidStatementHandle"
            before_release_reads = len(requests)
            for read in (reader.status, reader.await_result, reader.fetch_result):
                with pytest.raises(k.KernelError) as error:
                    read()
                assert error.value.code == "InvalidStatementHandle"
            del read
            with pytest.raises(k.KernelError) as error:
                owner.reader()
            assert error.value.code == "InvalidStatementHandle"
            assert len(requests) == before_release_reads
            stmt.close()
            session.close()
            session = None
            del owner, stmt
            with pytest.raises(k.KernelError) as error:
                reader.status()
            assert error.value.code == "InvalidStatementHandle"
            del reader
            for stream in streams:
                assert stream.fetch_all_arrow().to_pydict() == {"n": [1, 2]}
                stream.close()
        finally:
            if session is not None:
                session.close()
            server.shutdown()
            worker.join(timeout=5)
