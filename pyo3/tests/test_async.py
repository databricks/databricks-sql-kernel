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

import time

import databricks_sql_kernel as k

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
