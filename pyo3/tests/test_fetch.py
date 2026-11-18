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

"""Buffered reads, pending I/O and result cleanup over a loopback SQL API."""

import base64
import json
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pyarrow as pa
import pytest

import databricks_sql_kernel as k


def arrow_bytes(batches):
    schema = pa.schema([("n", pa.int64())])
    output = pa.BufferOutputStream()
    with pa.ipc.new_stream(output, schema) as writer:
        for rows in batches:
            writer.write_batch(
                pa.record_batch([pa.array(rows, pa.int64())], schema=schema)
            )
    return output.getvalue().to_pybytes()


@pytest.fixture
def sql_api():
    state = SimpleNamespace(
        batches=[[1, 2]],
        mode="buffered",
        requests=[],
        chunk_requested=threading.Event(),
        release_chunk=threading.Event(),
        statement_closed=threading.Event(),
    )
    statement_id = "00000001-0000-0000-0000-000000000001"
    statement_path = f"/api/2.0/sql/statements/{statement_id}"

    class Handler(BaseHTTPRequestHandler):
        def reply(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            path = self.path.split("?")[0]
            state.requests.append((self.command, path))
            status, response = 200, {}
            if self.command == "POST" and path.endswith("/sessions"):
                response = {"session_id": "00000000-0000-0000-0000-000000000001"}
            elif self.command == "POST" and path.endswith("/statements"):
                pending = state.mode != "buffered"
                raw = arrow_bytes(state.batches)
                count = sum(map(len, state.batches))
                response = {
                    "statement_id": statement_id,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "format": "ARROW_STREAM",
                        "schema": {
                            "column_count": 1,
                            "columns": [
                                {
                                    "name": "n",
                                    "type_name": "BIGINT",
                                    "type_text": "BIGINT",
                                    "position": 0,
                                }
                            ],
                        },
                        "total_chunk_count": 2 if pending else 1,
                        "total_row_count": count + (2 if pending else 0),
                    },
                    "result": {
                        "chunk_index": 0,
                        "row_offset": 0,
                        "row_count": count,
                        "attachment": base64.b64encode(raw).decode(),
                    },
                }
                if pending:
                    response["result"]["next_chunk_index"] = 1
                if state.mode == "cloud":
                    response["result"] = {"external_links": [self.link(0)]}
                if not state.batches:
                    response.pop("result")
                state.response = response
            elif self.command == "GET" and path == statement_path:
                response = state.response
            elif self.command == "DELETE" and path == statement_path:
                state.statement_closed.set()
            elif path == f"{statement_path}/result/chunks/1" and state.mode == "cloud":
                response = {"external_links": [self.link(1)]}
            elif path in (f"{statement_path}/result/chunks/1", "/storage/1"):
                state.chunk_requested.set()
                if not state.release_chunk.wait(5):
                    status, response = 408, {"message": "chunk gate timed out"}
                elif state.mode == "error":
                    status, response = (
                        404,
                        {"error_code": "NOT_FOUND", "message": "missing chunk"},
                    )
                elif state.mode == "cloud":
                    response = arrow_bytes([[3, 4]])
                else:
                    response = {
                        "chunk_index": 1,
                        "row_offset": 2,
                        "row_count": 2,
                        "attachment": base64.b64encode(arrow_bytes([[3, 4]])).decode(),
                    }
            elif path == "/storage/0":
                response = arrow_bytes([[1, 2]])
            encoded = (
                response
                if isinstance(response, bytes)
                else json.dumps(response).encode()
            )
            self.send_response(status)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def link(self, index):
            return {
                "external_link": f"{state.host}/storage/{index}",
                "chunk_index": index,
                "row_offset": index * 2,
                "row_count": 2,
                "byte_count": len(arrow_bytes(state.batches if index == 0 else [[3, 4]])),
                "next_chunk_index": 1 if index == 0 else None,
                "expiration": "2099-01-01T00:00:00Z",
            }

        do_POST = do_GET = do_DELETE = reply

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    state.host = f"http://127.0.0.1:{server.server_port}"
    state.statement_path = statement_path
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        state.session = k.Session(
            host=state.host,
            http_path="/sql/1.0/warehouses/0000000000000001",
            access_token="test-token",
            telemetry_enabled=False,
            request_timeout_secs=2,
            retry_max_attempts=1,
        )
        yield state
    finally:
        state.release_chunk.set()
        if hasattr(state, "session"):
            state.session.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def execute(sql_api):
    statement = sql_api.session.statement()
    statement.set_sql("SELECT n")
    return statement, statement.execute()


@pytest.mark.parametrize("batches", [[], [[1, 2]], [[], [1, 2], [3]]])
def test_buffered_fetch_closes_once_after_exhaustion(sql_api, batches):
    sql_api.batches = batches
    statement, result = execute(sql_api)
    schema = result.arrow_schema()
    observed = []
    while (batch := result.fetch_next_batch()) is not None:
        observed.append(batch.column(0).to_pylist())
    assert observed == batches
    assert result.fetch_next_batch() is None
    assert sql_api.statement_closed.wait(5)
    assert Counter(sql_api.requests)["DELETE", sql_api.statement_path] == 1
    result.close()
    result.close()
    statement.close()
    assert Counter(sql_api.requests)["DELETE", sql_api.statement_path] == 1
    assert result.arrow_schema() == schema
    with pytest.raises(k.KernelError, match="closed"):
        result.fetch_next_batch()


@pytest.mark.parametrize("async_result", [False, True])
@pytest.mark.parametrize("mode", ["inline", "cloud"])
def test_pending_fetch_releases_gil_and_preserves_continuation(
    sql_api, async_result, mode
):
    sql_api.mode = mode
    statement = sql_api.session.statement()
    statement.set_sql("SELECT n")
    owner = statement.submit() if async_result else statement.execute()
    result = owner.await_result() if async_result else owner
    assert result.fetch_next_batch().column(0).to_pylist() == [1, 2]
    assert sql_api.chunk_requested.wait(5)
    assert not sql_api.statement_closed.is_set()
    timer = threading.Timer(0.05, sql_api.release_chunk.set)
    timer.start()
    try:
        assert result.fetch_next_batch().column(0).to_pylist() == [3, 4]
    finally:
        timer.join(timeout=5)
    assert result.fetch_next_batch() is None
    result.close()
    if async_result:
        owner.close()
    statement.close()
    assert sql_api.statement_closed.wait(5)
    assert Counter(sql_api.requests)["DELETE", sql_api.statement_path] == 1


@pytest.mark.parametrize("mode", ["buffered", "error"])
def test_partial_or_failed_fetch_retains_explicit_cleanup(sql_api, mode):
    sql_api.mode = mode
    sql_api.batches = [[1, 2], [3]] if mode == "buffered" else [[1, 2]]
    statement, result = execute(sql_api)
    assert result.fetch_next_batch().column(0).to_pylist() == [1, 2]
    if mode == "error":
        assert sql_api.chunk_requested.wait(5)
        sql_api.release_chunk.set()
        for _ in range(2):
            with pytest.raises(k.KernelError, match="404|missing chunk"):
                result.fetch_next_batch()
    result.close()
    statement.close()
    assert sql_api.statement_closed.wait(5)
    assert Counter(sql_api.requests)["DELETE", sql_api.statement_path] == 1


def test_drained_close_releases_last_runtime_owner(sql_api):
    statement, result = execute(sql_api)
    assert result.fetch_all_arrow().column(0).to_pylist() == [1, 2]
    statement.close()
    sql_api.session.close()
    result.close()
    result.close()
    with pytest.raises(k.KernelError, match="closed"):
        result.fetch_next_batch()
