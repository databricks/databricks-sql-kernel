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
import pytest

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
def test_bind_param_positional_round_trips_all_simple_types(session):
    """Bind positional parameters across the primary type spectrum
    and confirm the server echoes the same values back. Single
    SELECT bundles all bindings so we exercise multiple ordinals
    in one round-trip."""
    stmt = session.statement()
    stmt.set_sql(
        "SELECT ? AS i, ? AS l, ? AS s, ? AS b, ? AS d, ? AS dec, ? AS dt, ? AS ts, ? AS n"
    )
    stmt.bind_param(1, "42", "INT")
    stmt.bind_param(2, "9999999999", "BIGINT")
    stmt.bind_param(3, "hello world", "STRING")
    stmt.bind_param(4, "true", "BOOLEAN")
    stmt.bind_param(5, "3.14", "DOUBLE")
    stmt.bind_param(6, "-123.45", "DECIMAL(10,2)")
    stmt.bind_param(7, "2026-05-15", "DATE")
    stmt.bind_param(8, "2026-05-15 12:30:45", "TIMESTAMP")
    stmt.bind_param(9, None, "STRING")  # SQL NULL
    exec_ = stmt.execute()
    try:
        table = exec_.fetch_all_arrow()
        assert table.num_rows == 1
        row = {table.schema[i].name: table.column(i).to_pylist()[0]
               for i in range(table.num_columns)}
        # Server-side type widening: BIGINT stays bigint, but the
        # column types are server-decided. Compare values not types.
        assert row["i"] == 42
        assert row["l"] == 9999999999
        assert row["s"] == "hello world"
        assert row["b"] is True
        assert row["d"] == 3.14
        # Decimal comes back as Python Decimal — match string form.
        assert str(row["dec"]) == "-123.45"
        import datetime
        assert row["dt"] == datetime.date(2026, 5, 15)
        # TIMESTAMP comes back tz-aware from the server. The kernel
        # treats `TypedValue::Timestamp` as a naive UTC value, so the
        # echo MUST be UTC-aware and the wall-clock fields must match
        # the input. Asserting both pins the round-trip contract
        # instead of just normalising it away.
        ts = row["ts"]
        assert ts.tzinfo is not None, "expected tz-aware timestamp"
        assert ts.utcoffset() == datetime.timedelta(0), ts.utcoffset()
        assert ts.replace(tzinfo=None) == datetime.datetime(2026, 5, 15, 12, 30, 45)
        assert row["n"] is None
    finally:
        exec_.close()
        stmt.close()


@live
def test_bind_param_void_treated_as_null(session):
    """`VOID` SQL type is the connector's way of expressing
    `cursor.execute(..., [None])` — value_str is ignored, result
    is SQL NULL."""
    stmt = session.statement()
    stmt.set_sql("SELECT ? IS NULL AS is_null")
    # Connector's VoidParameter passes stringValue("None") with
    # type "VOID" — the parser ignores the value.
    stmt.bind_param(1, "None", "VOID")
    exec_ = stmt.execute()
    try:
        table = exec_.fetch_all_arrow()
        assert table.column(0).to_pylist() == [True]
    finally:
        exec_.close()
        stmt.close()


@live
def test_bind_param_compound_type_rejected(session):
    """ARRAY/MAP/STRUCT parameter binding is a documented gap."""
    stmt = session.statement()
    stmt.set_sql("SELECT ?")
    try:
        try:
            stmt.bind_param(1, "[1,2,3]", "ARRAY<INT>")
        except k.KernelError as e:
            assert e.code == "InvalidArgument", e.code
            assert "compound" in e.message.lower(), e.message
        else:
            raise AssertionError("expected KernelError for ARRAY parameter")
    finally:
        stmt.close()


@live
def test_bind_param_invalid_value_surfaces_error(session):
    """A non-numeric value for an INT param should surface as
    KernelError(InvalidArgument) at bind time, before execute."""
    stmt = session.statement()
    stmt.set_sql("SELECT ?")
    try:
        try:
            stmt.bind_param(1, "not-a-number", "INT")
        except k.KernelError as e:
            assert e.code == "InvalidArgument", e.code
        else:
            raise AssertionError("expected KernelError for bad INT literal")
    finally:
        stmt.close()


@live
@pytest.mark.parametrize("ordinal", [0, -1, -1000])
def test_bind_param_non_positive_ordinal_rejected(session, ordinal):
    """SEA parameter ordinals are 1-based; both zero and negative
    ordinals must raise ``KernelError(InvalidArgument)`` — not
    Python ``OverflowError`` (which a ``usize`` extractor would
    raise before the kernel could surface a typed error)."""
    stmt = session.statement()
    stmt.set_sql("SELECT ?")
    try:
        try:
            stmt.bind_param(ordinal, "1", "INT")
        except k.KernelError as e:
            assert e.code == "InvalidArgument", e.code
        else:
            raise AssertionError(f"expected KernelError for ordinal={ordinal}")
    finally:
        stmt.close()


@live
def test_bind_param_duplicate_ordinal_last_wins(session):
    """Re-binding the same ordinal overwrites the prior value
    (last-write-wins). This documents the contract rather than
    discovering it later."""
    stmt = session.statement()
    stmt.set_sql("SELECT ? AS x")
    stmt.bind_param(1, "1", "INT")
    stmt.bind_param(1, "42", "INT")  # overwrites
    exec_ = stmt.execute()
    try:
        table = exec_.fetch_all_arrow()
        assert table.column(0).to_pylist() == [42]
    finally:
        exec_.close()
        stmt.close()


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
