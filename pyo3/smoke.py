"""Live smoke test for the databricks_sql_kernel PyO3 wheel.

Exercises the surface that's currently implemented (PAT auth, sync
execute, metadata, ResultStream draining, error mapping) against a
live Databricks warehouse. Reads creds from env vars (set via
`set -a && source ~/.databricks/pecotesting-creds && set +a`).

Run from `pyo3/` so maturin picks up the project-local `.venv`:

    cd pyo3
    .venv/bin/maturin develop --release
    .venv/bin/python smoke.py
"""

from __future__ import annotations
import os
import sys
import traceback

import databricks_sql_kernel as k
import pyarrow as pa


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        sys.exit(f"missing env var: {name}")
    return v


HOST = require_env("DATABRICKS_HOST")
HTTP_PATH = require_env("DATABRICKS_HTTP_PATH")
TOKEN = require_env("DATABRICKS_TOKEN")


def banner(s: str) -> None:
    print(f"\n=== {s} ===")


def check(cond: bool, msg: str) -> None:
    print(f"  {'OK ' if cond else 'FAIL '} {msg}")
    if not cond:
        sys.exit(1)


def main() -> None:
    banner("module + version")
    print(f"  databricks_sql_kernel.__version__ = {k.__version__}")

    banner("open session")
    session = k.Session(
        host=HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN,
    )
    check(bool(session.session_id), f"session_id non-empty: {session.session_id!r}")
    print(f"  repr: {session!r}")

    banner("SELECT 1 — fetch_next_batch path")
    stmt = session.statement()
    stmt.set_sql("SELECT 1 AS n")
    exec_ = stmt.execute()
    check(bool(exec_.statement_id), f"statement_id non-empty: {exec_.statement_id!r}")
    schema = exec_.arrow_schema()
    check(isinstance(schema, pa.Schema), f"arrow_schema is pyarrow.Schema: {schema}")
    batch = exec_.fetch_next_batch()
    check(isinstance(batch, pa.RecordBatch), f"first batch is RecordBatch: {batch.num_rows} rows")
    check(batch.num_rows == 1, "one row")
    check(batch.column(0).to_pylist() == [1], f"value is 1: {batch.column(0).to_pylist()}")
    nxt = exec_.fetch_next_batch()
    check(nxt is None, "second fetch returns None")
    exec_.close()
    print(f"  repr after close: {exec_!r}")
    stmt.close()

    banner("SELECT range(10000) — fetch_all_arrow path")
    stmt = session.statement()
    stmt.set_sql("SELECT * FROM range(10000)")
    exec_ = stmt.execute()
    table = exec_.fetch_all_arrow()
    check(isinstance(table, pa.Table), f"got pyarrow.Table with {table.num_rows} rows")
    check(table.num_rows == 10000, f"expected 10000 rows, got {table.num_rows}")
    exec_.close()
    stmt.close()

    banner("idempotent close")
    stmt = session.statement()
    stmt.set_sql("SELECT 42")
    exec_ = stmt.execute()
    exec_.close()
    exec_.close()  # second call should be a no-op
    check(True, "double-close on ExecutedStatement did not raise")

    banner("operations on closed ExecutedStatement raise KernelError")
    try:
        exec_.fetch_next_batch()
        check(False, "should have raised")
    except k.KernelError as e:
        check("closed" in str(e), f"KernelError mentions closed: {e}")

    # --- metadata surface ---
    meta = session.metadata()

    banner("metadata.list_catalogs")
    rs = meta.list_catalogs()
    check(isinstance(rs.arrow_schema(), pa.Schema), "ResultStream schema is pyarrow.Schema")
    cat_table = rs.fetch_all_arrow()
    check(isinstance(cat_table, pa.Table), f"got Table with {cat_table.num_rows} catalogs")
    check(cat_table.num_rows > 0, "at least one catalog visible")
    print(f"  first few: {cat_table.column(0).to_pylist()[:5]}")
    rs.close()

    banner("metadata.list_schemas — main catalog")
    rs = meta.list_schemas(catalog="main")
    schemas_table = rs.fetch_all_arrow()
    check(schemas_table.num_rows > 0, f"main has schemas: {schemas_table.num_rows}")
    rs.close()

    banner("metadata.list_tables — system.information_schema")
    rs = meta.list_tables(catalog="system", schema_pattern="information_schema")
    tables_table = rs.fetch_all_arrow()
    check(tables_table.num_rows > 0, f"information_schema has tables: {tables_table.num_rows}")
    print(f"  columns in result: {[f.name for f in tables_table.schema]}")
    rs.close()

    banner("metadata.list_columns — system.information_schema.tables")
    rs = meta.list_columns(
        catalog="system", schema_pattern="information_schema", table_pattern="tables"
    )
    cols_table = rs.fetch_all_arrow()
    check(cols_table.num_rows > 0, f"tables has columns: {cols_table.num_rows}")
    rs.close()

    banner("metadata.type_info — static, no wire call")
    rs = meta.type_info()
    ti_table = rs.fetch_all_arrow()
    check(ti_table.num_rows > 0, f"type_info rows: {ti_table.num_rows}")
    rs.close()

    banner("metadata.table_types — static, no wire call")
    rs = meta.table_types()
    tt_table = rs.fetch_all_arrow()
    check(tt_table.num_rows > 0, f"table_types rows: {tt_table.num_rows}")
    rs.close()

    banner("metadata input validation — empty identifier is rejected")
    try:
        meta.list_schemas(catalog="")
        check(False, "empty catalog should have raised")
    except k.KernelError as e:
        attrs = e.args[1]
        check(attrs.get("code") == "InvalidArgument", f"code=InvalidArgument, got: {attrs.get('code')}")
        check(
            "identifier" in attrs.get("message", "").lower(),
            f"message mentions identifier: {attrs.get('message')}",
        )

    banner("metadata input validation — list_columns requires catalog kwarg")
    try:
        meta.list_columns(schema_pattern="x", table_pattern="y")  # type: ignore[call-arg]
        check(False, "missing catalog kwarg should have raised TypeError")
    except TypeError as e:
        check(True, f"TypeError as expected: {e}")

    banner("ResultStream close + use-after-close")
    rs = meta.list_catalogs()
    rs.close()
    rs.close()  # idempotent
    try:
        rs.fetch_next_batch()
        check(False, "should have raised")
    except k.KernelError as e:
        check("closed" in str(e), f"KernelError mentions closed: {e}")

    # --- async statement surface ---
    banner("async submit + await_result")
    stmt = session.statement()
    stmt.set_sql("SELECT 7 AS n")
    async_exec = stmt.submit()
    check(bool(async_exec.statement_id), f"async statement_id: {async_exec.statement_id!r}")
    state, failure = async_exec.status()
    check(state in ("Pending", "Running", "Succeeded", "Closed"), f"status state={state!r}")
    check(failure is None, f"non-failed status has failure=None: {failure!r}")
    rs = async_exec.await_result()
    table = rs.fetch_all_arrow()
    check(table.num_rows == 1, f"1 row, got {table.num_rows}")
    check(table.column(0).to_pylist() == [7], f"value is 7: {table.column(0).to_pylist()}")
    # After await_result, status should be terminal.
    state, _ = async_exec.status()
    check(state in ("Succeeded", "Closed"), f"terminal status after await_result: {state!r}")
    rs.close()
    async_exec.close()
    stmt.close()

    banner("async submit + fetch_result (after explicit wait via status loop)")
    stmt = session.statement()
    stmt.set_sql("SELECT 99 AS n")
    async_exec = stmt.submit()
    # Poll a few times until terminal — should reach Succeeded almost immediately for SELECT 99.
    import time

    for _ in range(20):
        state, _ = async_exec.status()
        if state in ("Succeeded", "Closed", "Failed", "Cancelled"):
            break
        time.sleep(0.1)
    check(state in ("Succeeded", "Closed"), f"reached terminal state: {state!r}")
    rs = async_exec.fetch_result()
    table = rs.fetch_all_arrow()
    check(table.num_rows == 1 and table.column(0).to_pylist() == [99], f"value is 99")
    rs.close()
    async_exec.close()
    stmt.close()

    banner("async submit + Failed status surfaces structured error")
    stmt = session.statement()
    stmt.set_sql("SELECT * FROM definitely_not_a_table_xyz_async")
    async_exec = stmt.submit()
    # Poll until terminal.
    for _ in range(20):
        state, failure = async_exec.status()
        if state in ("Succeeded", "Closed", "Failed", "Cancelled"):
            break
        time.sleep(0.1)
    check(state == "Failed", f"expected Failed, got: {state!r}")
    check(isinstance(failure, dict), f"failure is dict: {failure!r}")
    check(failure.get("code") == "SqlError", f"failure.code=SqlError, got: {failure.get('code')}")
    check("sql_state" in failure, "failure has sql_state")
    check("query_id" in failure, "failure has query_id")
    async_exec.close()
    stmt.close()

    banner("operations on closed ExecutedAsyncStatement raise KernelError")
    stmt = session.statement()
    stmt.set_sql("SELECT 1")
    async_exec = stmt.submit()
    async_exec.close()
    async_exec.close()  # idempotent
    try:
        async_exec.status()
        check(False, "should have raised")
    except k.KernelError as e:
        check("closed" in str(e), f"KernelError mentions closed: {e}")
    stmt.close()

    banner("close session")
    session.close()
    session.close()  # idempotent
    check(True, "double-close on Session did not raise")

    banner("operations on closed Session raise KernelError")
    try:
        session.statement()
        check(False, "should have raised")
    except k.KernelError as e:
        check("closed" in str(e), f"KernelError mentions closed: {e}")

    banner("error mapping — bad SQL surfaces as KernelError")
    s2 = k.Session(host=HOST, http_path=HTTP_PATH, access_token=TOKEN)
    stmt = s2.statement()
    stmt.set_sql("SELECT * FROM definitely_not_a_table_xyz_smoke")
    try:
        stmt.execute()
        check(False, "should have raised on bad SQL")
    except k.KernelError as e:
        args = e.args
        check(len(args) >= 2 and isinstance(args[1], dict), f"args carry structured dict: {args!r}")
        attrs = args[1]
        check("code" in attrs, "attrs has 'code'")
        check("message" in attrs, "attrs has 'message'")
    s2.close()

    print("\nAll smoke checks passed.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(2)
