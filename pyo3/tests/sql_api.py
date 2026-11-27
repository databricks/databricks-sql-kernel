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

"""Loopback SQL API shared by native binding and Python fetch tests."""

import base64
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace


@contextmanager
def sql_api(session_type, arrow_bytes=None):
    state = SimpleNamespace(
        batches=[[1, 2]] if arrow_bytes else [],
        mode="buffered",
        requests=[],
        statement_bodies=[],
        chunk_requested=threading.Event(),
        release_chunk=threading.Event(),
        statement_closed=threading.Event(),
    )
    statement_id = "00000001-0000-0000-0000-000000000001"
    statement_path = f"/api/2.0/sql/statements/{statement_id}"

    class Handler(BaseHTTPRequestHandler):
        def reply(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            path = self.path.split("?")[0]
            state.requests.append((self.command, path))
            status, response = 200, {}
            if self.command == "POST" and path.endswith("/sessions"):
                response = {"session_id": "00000000-0000-0000-0000-000000000001"}
            elif self.command == "POST" and path.endswith("/statements"):
                state.statement_bodies.append(json.loads(body))
                pending = state.mode != "buffered"
                raw = arrow_bytes(state.batches) if state.batches else b""
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
                        "total_chunk_count": (
                            3 if state.mode == "cloud" else 2 if pending else 1
                        ),
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
                response = {"external_links": [self.link(1), self.link(2)]}
            elif path == "/storage/2":
                response = arrow_bytes([[3, 4]])
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
                    response = arrow_bytes([])
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
            batches = state.batches if index == 0 else [] if index == 1 else [[3, 4]]
            return {
                "external_link": f"{state.host}/storage/{index}",
                "chunk_index": index,
                "row_offset": 0 if index == 0 else 2,
                "row_count": 0 if index == 1 else 2,
                "byte_count": len(arrow_bytes(batches)),
                "next_chunk_index": index + 1 if index < 2 else None,
                "expiration": "2099-01-01T00:00:00Z",
            }

        do_POST = do_GET = do_DELETE = reply

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    state.host = f"http://127.0.0.1:{server.server_port}"
    state.statement_path = statement_path
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        state.session = session_type(
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
        try:
            if hasattr(state, "session"):
                state.session.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
