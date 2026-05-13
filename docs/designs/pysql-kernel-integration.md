<!--
  Copyright (c) 2026 Databricks, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# PySQL × Kernel integration via PyO3

> Status: draft. Owner: @vikrant-puppala. Last updated: 2026-05-13.

## Summary

Route the Python connector's `use_sea=True` code path through a PyO3
binding over the v0 Rust kernel (`databricks-sql-kernel`). When
`use_sea=True` is passed to `databricks.sql.connect(...)`, the
connector hands session, statement, result, and metadata operations to
the kernel instead of speaking SEA in Python.

The connector keeps treating this as a single user-facing flag
(`use_sea`). The wire protocol the kernel picks underneath is a
kernel-internal concern (v0 uses SEA REST; future versions may use
SEA-gRPC or other transports). The flag name is retained for
backward compatibility, not as a protocol contract.

Behavioral parity target is the existing **Thrift** code path in the
Python connector. The new backend has to match Thrift's observable
behavior — same auth flows, same parameter semantics, same async
execution shape, same metadata results — before it's considered
production-ready.

Strategy reference: [Heilmeier — Rust Kernel Strategy for OSS SQL
Drivers](https://docs.google.com/document/d/1ZcsH0RP1fnmXEw3SeOi9e8Yi2PtOhYaBLr4jwwswifY/edit?tab=t.0#heading=h.mnkp0kk05pi0).

## Why

1. **One implementation across drivers.** The kernel is already the
   source of truth for SEA, CloudFetch, auth, retries, and result
   handling. Re-implementing the same logic in Python (and again in
   Go, Node.js, ODBC) is the duplication the kernel exists to remove.
2. **Decoupling from ADBC.** Prior POCs
   ([adbc-drivers/databricks#423](https://github.com/adbc-drivers/databricks/pull/423),
   [databricks/databricks-sql-python#782](https://github.com/databricks/databricks-sql-python/pull/782))
   layered the connector on top of the ADBC Rust crate (via either a
   custom PyO3 wrapper or `adbc_driver_manager` ctypes). That coupled
   PySQL to ADBC's Database/Connection/Statement shape and to the
   driver-manager binary protocol. The v0 kernel exposes a
   Databricks-native API directly intended for PyO3/napi-rs/C-ABI
   consumers; we use that instead.
3. **Thrift parity as a precondition.** Before the connector flips any
   default behavior, the kernel-backed path has to match every Thrift
   code path the connector ships today. This doc tracks that parity
   work.

## Non-goals

- Replacing the Thrift backend. `use_sea=False` continues to route to
  `ThriftDatabricksClient` unchanged.
- Deleting or restructuring the existing native SEA backend
  (`src/databricks/sql/backend/sea/`). It stays in the repo. This
  work only changes what `use_sea=True` is wired to.
- Exposing the kernel's transport choice (SEA REST today, possibly
  SEA-gRPC tomorrow) to the connector or to end users. The kernel
  owns that knob internally; the connector backend never names it.
- Volume / staging operations (`PUT`/`GET`). The kernel does not ship
  a Volume API yet; this work is tracked separately.
- `napi-rs` Node.js bindings or the kernel's own C-ABI. Both are
  out-of-scope here and live behind their own design docs.

## What's already in place

### Kernel (`~/databricks-sql-kernel`)

The v0 public Rust surface (see `docs/spec-v0.md` + addenda A1–A8) is
the contract this work binds. Relevant pieces, all `'static` and
`Send + Sync` (asserted in `src/lib.rs:115`):

| Type | Construction | Key methods |
|------|--------------|-------------|
| `Session` | `Session::builder()...open().await` (or `Session::open(SessionConfig)`) | `statement()`, `metadata()`, `close()`, `session_id()` |
| `SessionBuilder` | `Session::builder()` | `connection / http_path / auth / defaults / http / proxy / tls / cloudfetch / logging / telemetry / session_conf / transport / open` |
| `Statement` | `Session::statement()` | `spec()` (mut) / `spec_ref()`, `execute().await -> ExecutedStatement`, `submit().await`, `status / await_result / fetch_result / cancel / close` |
| `StatementSpec` | direct or via `Statement::spec()` | `sql`, `param(ordinal, TypedValue)`, `param_named`, `statement_conf`, `result_format` |
| `ExecutedStatement` | `Statement::execute()` | `schema()`, `result_stream_mut()`, `cancel()`, `close()`, `statement_id()` |
| `ExecutedAsyncStatement` | `Statement::submit()` | same handle trait + `status()` / `await_result()` / `fetch_result()` |
| `Metadata` | `Session::metadata()` | `list_catalogs / list_schemas / list_tables / list_columns / primary_keys / foreign_keys / functions / procedures / type_info / table_types` |
| `ResultStream` | inside executed handles | `schema()`, `async next_batch() -> Option<&ResultBatch>`, `next_row_in_batch() -> Option<Row<'_>>`, `close()`, `into_owned_batches()` |

Auth (`config.rs:196`): `Pat`, `OAuthM2M`, `OAuthU2M`,
`OAuthFederation` (NYI), `External(Box<dyn TokenProvider>)`.

The `src/ffi/` C-ABI skeleton is reserved for ODBC / Go; PyO3 is meant
to be a **direct Rust crate dependency**, not a ctypes-over-C consumer.
`Cargo.toml` already declares `crate-type = ["lib", "cdylib",
"staticlib"]` so the same crate can ship both.

### Python connector (`~/databricks-sql-python`)

- `DatabricksClient` abstract (`src/databricks/sql/backend/databricks_client.py:14`)
  is the integration seam: `open_session / close_session /
  execute_command / cancel_command / close_command / get_query_state /
  get_execution_result / get_catalogs / get_schemas / get_tables /
  get_columns / max_download_threads`.
- `Session._create_backend` (`src/databricks/sql/session.py:113`)
  branches on `use_sea` and currently routes to one of two prior-POC
  backends on the `adbc-rust-backend-via-dm` branch:
  - `backend/adbc/client.py` (PyO3 against the ADBC kernel crate)
  - `backend/adbc_dm/client.py` (`adbc_driver_manager` over the cdylib)
  Both are PAT-only, sync-only, no metadata, with synthetic
  session/statement IDs. They are the structural template for the new
  backend but are **discarded** — neither is on the migration path.
- Native SEA backend (`backend/sea/`) exists today. This work does
  not modify or remove it; whether and when it gets cleaned up is
  out of scope for this doc.

### Prior PyO3 work (`~/databricks-adbc/rust-pyo3`)

`databricks_adbc_pyo3` (~240 lines, `pyo3 0.26 / abi3-py39`, maturin
build, `pyarrow>=14`). Useful as a shape reference for `Python::detach`
around blocking work and `arrow-pyarrow` handoff. Not reused as code —
it wraps ADBC's `Optionable` layer, not the kernel's v0 API.

## Target architecture

```
                   databricks-sql-connector (Python)
                                │
   use_sea=False  ──┐           ▼           ┌── use_sea=True
                    │   _create_backend()   │
                    ▼                       ▼
         ThriftDatabricksClient      KernelDatabricksClient
                                            │
                              databricks_sql_kernel  (PyO3 ext module)
                                            │
                          databricks-sql-kernel  (Rust crate, v0 API)
                                            │
                                  (kernel-internal transport)
                                            │
                                    Databricks endpoint
```

Two repos, two artifacts:

- **`databricks-sql-kernel`** ships the Rust crate **and** a PyO3
  sub-crate (`pyo3/`) that builds a wheel named
  `databricks-sql-kernel` (importable as `databricks_sql_kernel`).
- **`databricks-sql-python`** gains a `backend/kernel/` module that
  imports `databricks_sql_kernel` and implements `DatabricksClient`.
  The module is named after the **kernel** (the dependency it wraps),
  not the wire protocol the kernel happens to use today — that
  protocol may change without any change to this module's name.

## Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | PyO3 crate lives **inside the kernel repo** as `pyo3/` | Single repo keeps the Rust API and its Python binding in lockstep; the kernel's CLAUDE.md already names PyO3 / napi-rs as kernel consumers. |
| D2 | Python module name: `databricks_sql_kernel` | Drops the `_adbc_` infix; reflects the decoupling from ADBC. |
| D3 | Fresh Python-connector branch off `master`, not a continuation of `adbc-rust-backend-via-dm` | The old branch is structurally useful but semantically wrong (ADBC names, no metadata, synthetic IDs). Easier to copy patterns than rebase. |
| D4 | New connector module is `backend/kernel/` (not `backend/sea_kernel/`) | The module's identity is "the backend that delegates to the kernel," not the wire protocol. The kernel may switch its default transport (SEA REST → SEA gRPC → …) without renaming this module. |
| D5 | One `tokio::runtime::Runtime` per `Session`, owned on the Rust side | Spec says wrappers own the runtime. Per-session keeps lifetime simple and isolates failure to the session it belongs to. |
| D6 | `databricks-sql-kernel` is an **optional extra** of the connector | `pip install 'databricks-sql-connector[kernel]'`. Without it, `use_sea=True` raises `ImportError`. Mirrors how `[pyarrow]` is handled today. |
| D7 | `use_sea` keeps its name in the connector's public API | The flag's name is a backward-compat constraint; changing it would break every user already passing `use_sea=True`. The flag's *meaning* shifts from "talk SEA in Python" to "delegate to the kernel," but the surface stays put. |
| D8 | The connector backend never names the kernel's transport | No `transport=`/`use_grpc=`/etc. knob exposed to users or carried in the backend's API. If the kernel grows transport variants later, they're picked kernel-side based on kernel config. |
| D9 | OAuth delegates to the connector's auth provider via `AuthConfig::External` | Avoids running two OAuth implementations in parallel. Connector keeps owning credential caches and refresh; kernel just asks for a bearer when it needs one. |

## Rollout plan

The work splits into five phases that can land independently. Each
phase ships behind its own PR(s); the connector keeps working
throughout. Cutover happens at the end of Phase 5.

### Phase 1 — PyO3 crate (kernel repo)

New sub-crate `databricks-sql-kernel/pyo3/`.

**Layout:**

```
pyo3/
  Cargo.toml         # name = "databricks-sql-kernel-pyo3"
                     # crate-type = ["cdylib"]
  pyproject.toml     # module-name = "databricks_sql_kernel"
                     # build-backend = "maturin"
  src/
    lib.rs           # #[pymodule] databricks_sql_kernel
    session.rs       # PySession + auth-config translation
    statement.rs     # PyStatement, PyExecutedStatement, PyExecutedAsyncStatement
    result.rs        # batch + schema handoff to pyarrow
    metadata.rs      # PyMetadata
    errors.rs        # KernelError exception class + From<kernel::Error>
  tests/             # python-side smoke tests via pytest + maturin develop
```

**Dependencies (pin to kernel crate versions):**

```toml
[dependencies]
databricks-sql-kernel = { path = ".." }
pyo3      = { version = "0.26", features = ["extension-module", "abi3-py39"] }
tokio     = { version = "1",    features = ["rt-multi-thread"] }
arrow-array   = "57"
arrow-schema  = "57"
arrow-pyarrow = "57"
```

**Python surface (initial):**

```python
import databricks_sql_kernel as k

session = k.Session(
    host="https://my-workspace.databricks.com",
    http_path="/sql/1.0/warehouses/abc123",
    auth_type="pat",                       # "pat" | "oauth_m2m" | "oauth_u2m" | "external"
    access_token="dapi...",                # for "pat"
    catalog="main", schema="default",
    session_conf={"ANSI_MODE": "true"},
)
stmt = session.statement()
stmt.set_sql("SELECT * FROM t WHERE id = :id")
stmt.bind_param_named("id", 42)            # accepts Python int/float/str/bytes/date/datetime/Decimal
exec_ = stmt.execute()                     # blocks; returns ExecutedStatement
print(exec_.statement_id, exec_.arrow_schema())
while (batch := exec_.fetch_next_batch()) is not None:
    ...                                     # pyarrow.RecordBatch
exec_.close()
session.close()
```

`AuthConfig::External` is exposed by passing `auth_type="external"`
with a `token_callback: Callable[[], str]`. Python keeps ownership of
auth state; the kernel calls back whenever it needs a fresh bearer.

**Runtime + GIL discipline:**

- Each `PySession` owns one `tokio::runtime::Runtime` (multi-thread,
  default worker count). Stored in an `Arc` so child handles share it.
- Every method that calls into the kernel does
  `py.detach(|| handle.block_on(fut))` so the GIL is released for the
  whole network/decode/decompress phase. Pattern matches
  `databricks-adbc/rust-pyo3` (see `~/databricks-adbc/rust-pyo3/src/lib.rs:120`).
- Arrow batches cross the boundary via `arrow-pyarrow`'s `ToPyArrow`,
  which is zero-copy on the buffer side.

**Errors:**

- Define a single `KernelError` Python exception with attributes
  `code`, `message`, `sql_state`, `error_code`, `vendor_code`,
  `http_status`, `retryable`, `query_id`.
- `From<kernel::Error> for PyErr` is the only conversion site; every
  `#[pymethods]` returns `PyResult<_>`. The connector backend
  re-raises as `DatabaseError` / `OperationalError` /
  `ProgrammingError` based on `code`.

**Test signal (Phase 1 exit):**

- Cargo: `cargo test -p databricks-sql-kernel-pyo3` (compile-only +
  doctests).
- Python: `maturin develop --release` then a small pytest that opens
  a session against DBX dogfood, runs `SELECT 1`, streams the batch,
  closes everything cleanly. Live-creds tests gated on env vars (same
  pattern the kernel uses for its `#[ignore]` E2E tests).

### Phase 2 — `kernel` backend (connector repo)

New directory `src/databricks/sql/backend/kernel/`.

```
kernel/
  __init__.py
  client.py          # KernelDatabricksClient(DatabricksClient)
  result_set.py      # KernelResultSet(ResultSet)
  auth_bridge.py     # auth_provider -> databricks_sql_kernel auth kwargs
  type_mapping.py    # TSparkParameter <-> kernel TypedValue + Arrow type -> dbapi str
```

**`KernelDatabricksClient` shape:**

- Construction takes the connector's already-built `auth_provider`
  (not a raw `access_token`), `server_hostname`, `http_path`,
  `catalog`, `schema`, `session_configuration`. Calls
  `auth_bridge.kernel_auth_kwargs(auth_provider)` to translate.
- `open_session(...)` builds one `databricks_sql_kernel.Session`,
  stores it, returns `SessionId.from_sea_session_id(session.session_id)`
  using the **real** server-issued ID.
- `execute_command(...)` creates a fresh kernel `Statement`, binds
  params via `type_mapping.tspark_to_typed_value`, picks
  `execute()` (sync) or `submit()` (`async_op=True`), wraps the
  returned handle in a `KernelResultSet`. `CommandId` comes from
  the kernel handle's `statement_id`.
- `cancel_command` / `close_command` call the kernel handle's
  `cancel()` / `close()` (real ops, no longer no-ops).
- `get_query_state` / `get_execution_result` route to the kernel's
  async surface (`status()` / `await_result()`).
- `get_catalogs / get_schemas / get_tables / get_columns` call
  `session.metadata().list_*(...)` directly — no `SHOW …` SQL in
  Python. `get_tables` applies the existing
  `backend/sea/utils/filters.py::ResultSetFilter.filter_tables_by_type`
  for the `table_types` post-filter (the kernel returns server shape;
  client-side filtering matches today's behavior). When the kernel
  ships native `table_types` filtering this code gets deleted.
- `max_download_threads` returns the connector's configured value;
  CloudFetch tuning happens kernel-side via `CloudFetchConfig`.

**`KernelResultSet` shape:**

- Drop-in port of `backend/adbc/result_set.py` — same FIFO batch
  buffer, same `fetchone / fetchmany / fetchall`, same
  `fetchall_arrow / fetchmany_arrow`. Differences:
  - Holds a real kernel `ExecutedStatement` (not just the result
    handle), so `close()` calls
    `kernel_handle.close()` and propagates errors.
  - `description` built from the Arrow schema via the existing
    `description_from_arrow_schema` helper.
- Reuses (or hoists into a shared util) the Arrow→dbapi-type-string
  table from `backend/adbc/result_set.py:26`.

**Auth bridge (`auth_bridge.py`):**

| Connector auth provider | Kernel `auth_type` | Notes |
|------|---|---|
| `AccessTokenAuthProvider` | `pat` | Pass `dapi...` directly. |
| `DatabricksOAuthProvider` (M2M) | `oauth_m2m` | Pass `client_id` + `client_secret`. |
| `DatabricksOAuthProvider` (U2M) | `external` w/ token_callback | Connector already owns the browser flow + refresh; kernel gets a callback. |
| `ExternalAuthProvider` / federation / Azure MSAL | `external` w/ token_callback | Same pattern — Python owns credentials, kernel asks. |

Day-1 ships PAT only. M2M, U2M, external follow in order.

### Phase 3 — Connector wiring

Edit `src/databricks/sql/session.py::_create_backend`:

```python
if self.use_sea:
    from databricks.sql.backend.kernel.client import KernelDatabricksClient
    logger.debug("Creating kernel-backed client for use_sea=True")
    return KernelDatabricksClient(
        server_hostname=server_hostname,
        http_path=http_path,
        auth_provider=auth_provider,
        catalog=kwargs.get("catalog"),
        schema=kwargs.get("schema"),
        session_configuration=self.session_configuration,
        http_headers=all_headers,
        ssl_options=self.ssl_options,
    )
```

- Lazy import so the connector doesn't `ImportError` at startup just
  because the kernel wheel is missing — the error surfaces only on
  `use_sea=True`.
- Delete `backend/adbc/`, `backend/adbc_dm/`, and the related
  imports/branches in `session.py`. Those POC backends were the
  staging ground for this work; they have no users.
- `backend/sea/` is **not** touched. It's no longer reachable via
  `use_sea=True` but stays in the codebase as-is.

### Phase 4 — Packaging and build

- Maturin builds `databricks_sql_kernel` wheels per
  (Linux/macOS/Windows × x86_64/arm64 × abi3-py39) matrix from
  `databricks-sql-kernel/pyo3/`.
- The connector's `pyproject.toml` gains:

  ```toml
  [tool.poetry.extras]
  kernel = ["databricks-sql-kernel"]
  ```

  and a corresponding optional dependency declaration. `use_sea=True`
  with no kernel wheel installed raises:

  ```
  ImportError: use_sea=True requires the databricks-sql-kernel package.
  Install it with: pip install 'databricks-sql-connector[kernel]'
  ```

- Local dev: `maturin develop --release` from `kernel/pyo3/` into the
  connector's venv. Documented in
  `databricks-sql-python/CONTRIBUTING.md` under a new "Working on the
  kernel backend" section.

### Phase 5 — Parity sign-off

When the parity matrix below is green:

1. `use_sea=True` is documented as a supported, production-ready path
   alongside `use_sea=False` (Thrift). No flag flip; no behavior
   change for users already on Thrift.
2. Update `CHANGELOG.md` to note that `use_sea=True` is now backed by
   the Rust kernel via PyO3.

This work does **not** touch the existing `backend/sea/` native SEA
backend or the Thrift backend. `use_sea=True` becomes a stable
public route to the kernel; everything else stays put. Any later
cleanup of `backend/sea/` is a separate decision and a separate doc.

## Parity matrix (Thrift is the target)

The gating signal for Phase 5 sign-off is "every Thrift integration
test passes against `use_sea=True`." This list is the explicit
checklist.

| Capability | Thrift status | Kernel status (today) | `backend/kernel/` work |
|---|---|---|---|
| PAT auth | ✓ | ✓ | Wire through `auth_bridge` |
| OAuth M2M | ✓ | ✓ | Wire through `auth_bridge` |
| OAuth U2M (browser) | ✓ | ✓ (kernel can drive) | Use `External` callback so connector keeps owning the browser flow |
| External token providers (Azure MSAL, federation, etc.) | ✓ | ✓ (via `External`) | Trampoline class on PyO3 side |
| `SELECT` + Arrow streaming | ✓ | ✓ | Phase 2 |
| Cloud fetch (external links) | ✓ | ✓ | Kernel default; verify on wire |
| Hybrid disposition | ✓ | ✓ (kernel handles internally) | Verify; no caller knob needed |
| LZ4 compression | ✓ | ✓ | No-op on caller side |
| Parameter binding (positional) | ✓ | ✓ | `tspark_to_typed_value` mapper |
| Parameter binding (named) | ✓ | ✓ | Same mapper |
| `async_op=True` execute | ✓ | ✓ | `submit()` + `ExecutedAsyncStatement` |
| `get_query_state` polling | ✓ | ✓ | `status()` |
| `get_execution_result` | ✓ | ✓ | `await_result()` |
| `cancel_command` | ✓ | ✓ | `cancel()` |
| `close_command` | ✓ | ✓ | `close()` |
| `get_catalogs` | ✓ | ✓ | `metadata().list_catalogs()` |
| `get_schemas` | ✓ | ✓ | `metadata().list_schemas()` |
| `get_tables` (+ table_types filter) | ✓ | partial | `metadata().list_tables()` + client-side type filter; kernel-side filter follows |
| `get_columns` | ✓ | ✓ | `metadata().list_columns()` |
| `primary_keys / foreign_keys / functions` | ✓ | ✓ | Wire through if connector exposes them today |
| Session-level `query_tags` | ✓ | passes through `session_conf` | Verify wire shape |
| Statement-level `query_tags` | ✓ | passes through `statement_conf` | Verify wire shape |
| `SET ...` / `session_configuration` map | ✓ | `session_conf` field | 1:1 mapping |
| SPOG routing (`?o=` → `x-databricks-org-id`) | ✓ | kernel respects `connection.org_id` | Connector parses, passes to kernel |
| Custom CA / TLS verify off / hostname-mismatch | ✓ | `TlsConfig` (stubbed today) | Track kernel-side completion |
| HTTP proxy | ✓ | `ProxyConfig` (stubbed today) | Track kernel-side completion |
| User-Agent / `agent/<product>` | ✓ | needs `http.user_agent` | Connector builds UA, passes via `http` config |
| Telemetry | ✓ | partial | Follow kernel telemetry plan |
| Volume `PUT` / `GET` | ✓ (Thrift) | ✗ | **Out of scope.** `use_sea=True` raises `NotImplementedError` for staging ops until kernel ships the surface. Documented limitation. |

Anything in this table that is missing on the kernel side blocks
Phase 5 sign-off. Each missing item gets a tracked issue against the
kernel repo before sign-off.

## Test plan

1. **Unit (PyO3 crate):** Rust-side tests stay minimal (compile,
   error mapping). Heavy testing is Python-side.
2. **Unit (connector backend):** new tests under
   `tests/unit/test_kernel_backend_*` for the auth bridge, the type
   mapper, and the result-set buffer logic. Mock the kernel module
   via the same pattern the connector uses for `thrift` mocks.
3. **Integration (live):** the existing
   `tests/e2e/` suite runs against `use_sea=True`. Add a pytest
   parametrization that runs the same tests under
   `use_sea=False` (Thrift) and `use_sea=True` (kernel) and asserts
   identical observable behavior — same `description`, same row
   values, same exception classes on the same failures. The
   `use_sea=True` legs are skipped if `databricks-sql-kernel` is not
   installed.
4. **Performance baseline:** a small benchmark (TPC-H-like wide
   scan, 10M rows, mix of types) recorded for Thrift and for
   `use_sea=True` (kernel-backed) before Phase 5 sign-off. The
   kernel-backed path should be within striking distance of Thrift
   on throughput and not regress memory by more than a small
   constant factor.

## Risks

- **GIL contention under high fetch concurrency.** Mitigated by
  detaching the GIL for every kernel call, but Python callers that
  drive many cursors concurrently from threads will see different
  scheduling than today. Benchmark covers this.
- **`pyarrow` version skew.** The kernel pins `arrow-pyarrow 57`;
  the connector supports `pyarrow>=14`. arrow-pyarrow's compatibility
  range needs verification against the connector's minimum pyarrow.
- **OAuth divergence.** If the kernel's OAuth flows ever diverge from
  the connector's (e.g., different cache locations, different
  refresh-token semantics), users hit subtle bugs. Mitigated by
  decision D8 — the kernel delegates back to the connector for
  OAuth, not the other way around.
- **Volume operations gap.** Documented limitation at Phase 5
  sign-off. Users who rely on `PUT`/`GET` stay on `use_sea=False`.
  Surfaces as a clear `NotImplementedError`, not a hang.
- **Single-runtime-per-session cost.** A tokio runtime is ~1 MB of
  stacks per session. For high session counts we may want a shared
  runtime. Defer until measured.

## Open questions

1. **Where do session-scoped HTTP options live?** `_tls_*`,
   `_http_proxy`, `http_headers`, custom user-agent — connector
   passes them as `kwargs` today. The auth_bridge takes them; how do
   they reach `SessionBuilder.http / .proxy / .tls`? Likely a small
   `connection_options.py` in `backend/kernel/`.
2. **Do we expose `Statement.submit()` to async-cursor users today?**
   Connector's `async_op=True` path runs through `execute_async() →
   get_query_state → get_execution_result`. Kernel's
   `ExecutedAsyncStatement` matches that contract. Verify the state
   transitions are 1:1 (`PENDING`/`RUNNING`/`SUCCEEDED`/`FAILED`/
   `CANCELLED`/`CLOSED`).
3. **`SessionId` GUID encoding.** Connector uses
   `SessionId.from_sea_session_id(str)`. Kernel session IDs are
   server-issued strings. Confirm no hex/bytes massaging needed.
4. **Telemetry merging.** Connector has its own telemetry pipeline;
   kernel ships another. Decide whose events ship for kernel-SEA
   sessions and whether they merge. Likely punt to a separate doc.

## Sequencing

The PR order, smallest first:

1. **(kernel)** This doc → `docs/designs/pysql-sea-kernel-integration.md`.
2. **(kernel)** `pyo3/` sub-crate scaffold: `Session`, `Statement`,
   `ExecutedStatement`, `ResultStream` (sync execute path only, PAT
   auth only). Lands behind a `cargo-workspace` member but ships a
   wheel.
3. **(kernel)** `Metadata` exposure on the PyO3 side.
4. **(kernel)** `ExecutedAsyncStatement` + `submit / status /
   await_result` on PyO3.
5. **(kernel)** `External` token provider trampoline (enables OAuth
   delegation).
6. **(connector)** `backend/kernel/` skeleton with `client.py`
   and `result_set.py` (sync path, PAT). Wire `_create_backend` for
   `use_sea=True`. Delete `backend/adbc/` and `backend/adbc_dm/` in
   the same PR. `backend/sea/` is not touched.
7. **(connector)** OAuth bridge through auth_bridge.py.
8. **(connector)** Async execution path + metadata.
9. **(connector)** Parametric e2e parity suite + benchmarks.
10. **(connector)** Phase 5 sign-off: docs flip `use_sea=True` to
    "supported, kernel-backed"; CHANGELOG entry.

Each step in steps 2–4 is independently shippable: the wheel exists
and works for its slice, even if the connector hasn't caught up.

## References

- v0 spec: `databricks-sql-kernel/docs/spec-v0.md`
- Kernel CLAUDE.md: `databricks-sql-kernel/CLAUDE.md`
- Prior ADBC-PyO3 POC: `databricks-adbc/rust-pyo3/src/lib.rs`
- Prior ADBC-driver-manager POC PR:
  [databricks/databricks-sql-python#782](https://github.com/databricks/databricks-sql-python/pull/782)
- Prior ADBC-Rust integration PR:
  [adbc-drivers/databricks#423](https://github.com/adbc-drivers/databricks/pull/423)
- Heilmeier strategy doc: <https://docs.google.com/document/d/1ZcsH0RP1fnmXEw3SeOi9e8Yi2PtOhYaBLr4jwwswifY>
