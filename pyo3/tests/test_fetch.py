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

import threading
from collections import Counter

import pyarrow as pa
import pytest

import databricks_sql_kernel as k
from sql_api import sql_api as local_sql_api


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
    with local_sql_api(k.Session, arrow_bytes) as state:
        yield state


def execute(sql_api):
    statement = sql_api.session.statement()
    statement.set_sql("SELECT n")
    return statement, statement.execute()


def test_statement_forwards_lz4_compression_override(sql_api):
    statement = sql_api.session.statement()
    try:
        statement.set_sql("SELECT n")
        statement.set_lz4_compression(False)
        statement.execute().close()
        assert sql_api.statement_bodies[-1]["result_compression"] == "NONE"

        statement.set_lz4_compression(None)
        statement.execute().close()
        assert sql_api.statement_bodies[-1]["result_compression"] == "LZ4_FRAME"
    finally:
        statement.close()


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
    assert not sql_api.statement_closed.is_set()
    result.close()
    assert sql_api.statement_closed.wait(5)
    assert Counter(sql_api.requests)["DELETE", sql_api.statement_path] == 1
    statement.close()
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
