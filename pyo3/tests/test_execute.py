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

"""Sync execute: SELECT 1, drain ranges, idempotent close, bad SQL."""

from __future__ import annotations

import pyarrow as pa

import databricks_sql_kernel as k

from conftest import live


@live
def test_select_one_via_fetch_next_batch(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 1 AS n")
    exec_ = stmt.execute()
    try:
        assert exec_.statement_id
        schema = exec_.arrow_schema()
        assert isinstance(schema, pa.Schema)
        batch = exec_.fetch_next_batch()
        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 1
        assert batch.column(0).to_pylist() == [1]
        # Second fetch is None — stream exhausted.
        assert exec_.fetch_next_batch() is None
    finally:
        exec_.close()
        stmt.close()


@live
def test_range_drains_to_pyarrow_table(session):
    stmt = session.statement()
    stmt.set_sql("SELECT * FROM range(10000)")
    exec_ = stmt.execute()
    try:
        table = exec_.fetch_all_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 10000
    finally:
        exec_.close()
        stmt.close()


@live
def test_executed_statement_double_close_is_idempotent(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 42")
    exec_ = stmt.execute()
    exec_.close()
    exec_.close()  # idempotent
    stmt.close()


@live
def test_use_after_close_raises_kernel_error(session):
    stmt = session.statement()
    stmt.set_sql("SELECT 42")
    exec_ = stmt.execute()
    exec_.close()
    try:
        exec_.fetch_next_batch()
    except k.KernelError as e:
        assert "closed" in str(e), str(e)
        assert e.code == "InvalidStatementHandle", e.code
    else:
        raise AssertionError("expected KernelError on use-after-close")
    stmt.close()


@live
def test_statement_close_invalidates_held_executed_statement(session):
    """`Statement.close()` is the user-facing "release everything"
    operation. After calling it, the kernel sends `CloseStatement`
    against the held executed handle and flips its local validity
    flag; subsequent **wire-touching** calls on the handle (cancel)
    short-circuit to `KernelError(code='InvalidStatementHandle')`.

    Note: already-buffered Arrow batches still drain locally —
    `fetch_next_batch` doesn't go through the validity flag because
    it operates on in-memory data the kernel already pulled. That
    matches JDBC semantics: rows already in the client buffer
    aren't unfetched by `close()`. The server-side state is what
    actually gets released."""
    stmt = session.statement()
    stmt.set_sql("SELECT 1 AS n")
    exec_ = stmt.execute()
    # Close the parent Statement while the user still holds exec_.
    stmt.close()
    # Cancel/close routes through `valid.check()` and now errors.
    try:
        exec_.cancel()
    except k.KernelError as e:
        assert e.code == "InvalidStatementHandle", e.code
    else:
        raise AssertionError(
            "expected KernelError(InvalidStatementHandle) on cancel after Statement.close"
        )


@live
def test_statement_close_is_idempotent(session):
    """A second `Statement.close()` is a no-op (does not error)."""
    stmt = session.statement()
    stmt.set_sql("SELECT 1")
    stmt.execute().close()  # close the executed handle ourselves
    stmt.close()
    stmt.close()  # idempotent


@live
def test_bad_sql_surfaces_structured_kernel_error(session):
    stmt = session.statement()
    stmt.set_sql("SELECT * FROM definitely_not_a_table_xyz_kernel_test")
    try:
        stmt.execute()
    except k.KernelError as e:
        assert e.code == "SqlError", f"got {e.code!r}"
        assert e.sql_state == "42P01", f"got {e.sql_state!r}"
        # query_id is server-issued and may or may not be populated
        # depending on when the failure surfaced; assert only that
        # the attribute exists.
        assert hasattr(e, "query_id")
    else:
        raise AssertionError("expected KernelError on bad SQL")
    finally:
        stmt.close()
