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

# Kernel-v0 Migration Plan

> **Scope.** This document describes how we migrate the existing Rust ADBC
> driver (now living under `src/adbc/` with feature `legacy-adbc`) into the
> v0 Kernel surface defined in `docs/spec-v0.md`.
>
> **Status.** Plan only — no code changes. Each PR section below is the
> contract for a future PR; nothing here is implemented yet.

## 1. Background

The kernel was seeded from the `adbc-drivers/databricks` Rust driver. PRs
#1–#3 quarantined the ADBC trait surface under `src/adbc/` (gated on the
`legacy-adbc` feature) and laid down the v0 public-API skeleton at the
crate root: `Session`, `SessionBuilder`, `Statement`, `ExecutedStatement`,
`ExecutedAsyncStatement`, `Metadata`, `ResultStream`, `kernel_error::Error`,
`SessionConfig`, and a custom C ABI under `src/ffi/`. Today every method on
that surface is `unimplemented!()`.

The kernel internals — `auth/`, `client/` (HTTP + retry + SEA REST),
`reader/` (CloudFetch + inline Arrow), `metadata/` (SHOW-/`information_schema`
SQL builders), `types/`, `telemetry/` — are production code today, reached
through the ADBC traits in `src/adbc/`. They were always *behind* the ADBC
edge, never *in* it. The migration is therefore mostly about replacing the
edge — typed config, typed error, typed `Statement`/`ResultStream`,
custom C ABI — while leaving the internals largely intact.

Two POC repos prove that the ADBC-Rust core works end-to-end:

- `adbc-drivers/databricks#423` — PyO3 satellite `databricks-adbc-pyo3`
  wrapping the kernel's ADBC `Database`/`Connection`/`Statement`.
- `databricks/databricks-sql-python#782` — PySQL `AdbcDatabricksClient`
  backend that delegates to the PyO3 satellite when `use_sea=True`.

These run PAT-only, sync-execute-only, no metadata, no params; they hit
parity-or-better perf vs the existing Thrift backend on a dogfood
warehouse. They are the existence proof that the kernel internals are
ready; the migration's job is to put a typed, Databricks-native edge on
them and let the wrapper repos cut over.

## 2. Goals

- **Public surface.** Match `docs/spec-v0.md` exactly: `Session` /
  `SessionBuilder`, `Statement` (with `StatementSpec`),
  `ExecutedStatement` / `ExecutedAsyncStatement`, `Metadata` (with
  `Identifier` / `LikePattern`), `ResultStream` (batch-async + row-sync),
  `SessionConfig` (typed groups), `Error` / `ErrorCode`, custom C ABI.
- **`legacy-adbc` stays green** until the cutover PR. Each intermediate
  PR keeps `cargo test --features legacy-adbc` and the default-feature
  build passing. Examples and integration tests don't break mid-stack.
- **Internals stay reused.** `auth/`, `client/sea.rs`, `client/http.rs`,
  `client/retry.rs`, `reader/`, `metadata/sql.rs`, `metadata/parse.rs`,
  `types/sea.rs`, `types/cloudfetch.rs`, `telemetry/` — all reused from
  inside the new surface. We don't rewrite them.
- **No regressions vs the POC.** PAT auth, OAuth M2M/U2M, CloudFetch
  prefetch, inline Arrow, retry semantics, proxy/TLS knobs, metadata
  propagation through Arrow field metadata — all must keep working.
- **Wrapper-friendly.** Public types are `Send + Sync + 'static` (already
  asserted in `lib.rs`). Cancellation works concurrent with `execute()`
  via `&self` + interior mutability.

## 3. Non-goals

- **Transactions** (`set_autocommit` / `commit` / `rollback`). Per spec
  addendum A5, deferred to v0.1. Server semantics still in flux.
- **Thrift / SEA-gRPC transports.** The `Transport` trait lands as
  `pub(crate)` with one impl (`SeaRestTransport`). Other variants are
  future work; the `TransportMode` enum is `#[non_exhaustive]` so they
  slot in without breaking wrappers.
- **Volume operations / staging ingestion**, **Substrait plans**,
  **partitioned reads** — out of v0.
- **Wrapper repos.** This plan stops at the kernel boundary. Migrating
  `databricks-adbc-pyo3`, `databricks-sql-python`, `databricks-sql-go`,
  and `databricks-sql-nodejs` onto the v0 surface is tracked separately
  (see §6).

## 4. Migration order — first-principles design

The skeleton's existing `PR 4a / 5 / 6 / 7a–d / 8` breadcrumbs in
`unimplemented!()` strings are **superseded** by this plan. We
re-derive the order from three constraints:

1. **Each PR keeps the build green and the legacy ADBC tests passing.**
2. **One cohesive theme per PR.** A PR's title should describe its
   work in one phrase ("the Statement core", "the C ABI"). If you
   can't, split it. If two adjacent PRs would touch the same files
   for the same reason, merge them.
3. **Test-after-each-step is real.** Every PR ships either a passing
   unit test against the new surface, or a passing E2E test against a
   live warehouse. Skeleton-only PRs (no behaviour) don't count.

This gives **7 PRs** end-to-end:

| PR  | Theme                                      | What lands                                                                                                                              |
|----:|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| 1   | Foundations                                | `kernel_error::Error` end-to-end across all internals; `DatabricksClient` trait → `pub(crate) Transport`; `SeaClient` → `SeaRestTransport`. No public-API changes. |
| 2   | Session                                    | `Session::open`, `SessionBuilder`, fully-typed `SessionConfig` groups (`ConnectionConfig`, `AuthConfig`, `HttpConfig`, `ResultConfig`, `LoggingConfig`, `TlsConfig`, `ProxyConfig`, `TelemetryConfig`). PAT + OAuth M2M / U2M wired. First E2E test. |
| 3   | Metadata                                   | All ten `Metadata::*` methods (`list_catalogs` / `_schemas` / `_tables` / `_columns` / `primary_keys` / `foreign_keys` / `functions` / `procedures` / `type_info` / `table_types`); `Identifier` / `LikePattern` validation; new `information_schema` SQL builders for the calls SEA doesn't have today. |
| 4   | Statement + Results (core)                 | `StatementSpec`, `Statement::execute`, `ExecutedStatement`, `ResultStream::next_batch` (borrowed) + `next_row_in_batch` + `Row<'a>` accessors + `into_rows` + `into_owned_batches`, `TypedValue` + parameter binding, cancellation (`Statement::cancel` concurrent with `execute()`), drop-in-flight `CancelStatement`. |
| 5   | Async Statement                            | `Statement::submit`, `ExecutedAsyncStatement::status / await_result / fetch_result / cancel / close`; mirrors PR 4 but with caller-driven polling. |
| 6   | C ABI                                      | `include/databricks_kernel.h`; `kernel_error_*`, `kernel_session_*`, `kernel_statement_*`, `kernel_executed_*`, `kernel_result_*` (Arrow C Data Interface for batches), `kernel_metadata_*`. Process-wide tokio runtime. C smoke tests in `tests/c/`. |
| 7   | Cutover                                    | Delete `src/adbc/`, `legacy-adbc` / `ffi` / `metadata-ffi` features, `adbc_core` / `adbc_ffi` / `driverbase` deps, `manifest.toml`, `include/legacy/`. Rename `kernel_error.rs` → `error.rs`, `result_stream.rs` → `result.rs`, `metadata_v0.rs` → `metadata.rs`. Migrate examples + integration tests. Bump to `0.2.0`. |

Independent track:

| PR  | Theme                | Why                                                          |
|----:|----------------------|--------------------------------------------------------------|
| 8+  | Wrapper repo cutovers | Separate repos, separate cadences. See §6.                  |

### Why these bundles

- **PR 1 (Foundations).** `kernel_error::Error` and the `Transport`
  rename both touch error returns across `auth/` / `client/` /
  `reader/` / `metadata/`. Doing them in one pass means each
  internal `Result<T, …>` line gets edited once. Neither changes a
  single public-API signature, so there's no review value in
  splitting them.
- **PR 2 (Session).** Standalone. `Session::open` is the smallest
  user-visible v0 surface and gives us a real E2E test.
- **PR 3 (Metadata).** Standalone. Statement-free path; exercises
  `Session` end-to-end without waiting for the Statement work.
- **PR 4 (Statement + Results).** The hard one — and bundled
  deliberately. `Statement`, `ExecutedStatement`, `ResultStream`,
  `Row<'a>`, `TypedValue`, and `cancel()` are one cohesive
  state-machine. Splitting them creates reviewer confusion (PR for
  `execute()` without rows? PR for cancellation that touches a
  Statement that already exists, but only barely?). They all touch
  `src/statement/*.rs` + `src/result_stream.rs`; bundling means one
  consistent design review.
- **PR 5 (Async Statement).** The async-consumption path is
  superficially similar to PR 4 but has its own polling cadence,
  `StatementStatus` enum semantics, and `await_result` /
  `fetch_result` distinction. Worth its own review pass; carving it
  out also keeps PR 4 from creeping further.
- **PR 6 (C ABI).** One cohesive interface, one header, one set of
  reviewers (likely the ODBC team). Splitting "session-and-error"
  vs "everything else" forces them to review the runtime-management
  story without context for what runs on top of it.
- **PR 7 (Cutover).** Pure deletion + rename. Reviewing this with
  any new behaviour mixed in is exactly the situation we want to
  avoid — it's the one PR where a reviewer *should* be skimming for
  "did anything other than removal sneak in?".

The rest of this section gives the contract for each PR.

### PR 1 — Foundations: `kernel_error` end-to-end + `Transport` rename

**Goal.** Mechanical refactor across the internals. No public-API
changes. Sets the stage so every later PR works against typed errors
and the renamed transport trait.

**What changes.**

- **`kernel_error::Error` end-to-end.** All internal modules
  (`auth/`, `client/`, `reader/`, `metadata/`, `statement/params.rs`,
  `types/`) switch their `Result<T>` alias from the driverbase-backed
  `crate::error::Result` to `crate::kernel_error::Result`. HTTP/SEA-error
  decoding produces `Error { code, message, sql_state, error_code,
  vendor_code, http_status, retryable, query_id, cause }` directly.
  Initial status mapping (refined as later PRs expose new failure
  paths):
  - `429` → `ResourceExhausted`, `retryable=true`
  - `503` → `Unavailable`, `retryable=true`
  - `504` → `Timeout`, `retryable=true`
  - `401` → `Unauthenticated`
  - `403` → `PermissionDenied`
  - `404` → `NotFound`
  - SEA `ServiceError` envelope → `SqlError` with `sql_state`,
    `error_code` (string slug — see spec addendum A9), and
    `query_id` populated from the envelope
  - DNS/TCP/TLS → `NetworkError`
  - Internal panics caught at FFI / `block_on` edge → `Internal`
  - Validation failures (`invalid_argument`, `invalid_state`) →
    `InvalidArgument`
  - `extract_sqlstate_from_message` (today in `src/error.rs`) moves
    into `kernel_error.rs` as a fallback for messages without a
    structured envelope.
- **Legacy ADBC shim.** `src/adbc/*` gets a single
  `kernel_error::Error → adbc_core::error::Error` conversion helper
  that replaces today's `.to_adbc()` on driverbase errors. ADBC
  trait impls keep returning `adbc_core::error::Result`.
- **`Transport` rename.** `src/client/mod.rs::DatabricksClient` is
  renamed `Transport` and moved to `src/client/transport.rs` as
  `pub(crate)`. Same async methods (`create_session`,
  `delete_session`, `execute_statement`, `get_result_chunks`,
  `cancel_statement`, `close_statement`, metadata calls). `SeaClient`
  → `SeaRestTransport`. `Arc<dyn DatabricksClient>` call sites
  become `Arc<dyn Transport>`. `client/mod.rs` keeps re-exporting
  the http client + retry config publicly.

**What does NOT change.**

- `crate::error::DatabricksErrorHelper` and the driverbase dep stay
  for now — they're used only by `src/adbc/` after this PR. Both
  are deleted in PR 7.
- The crate-root `pub use` from `lib.rs` continues to expose
  `kernel_error::{Error, ErrorCode}` (already wired).
- No new transport variants. `TransportMode::SeaRest` is the only
  variant.

**Tests.**

- Existing `crate::error::tests` move/adapt under
  `kernel_error::tests`: SQLSTATE extraction, error display.
- New unit tests for the HTTP-status → `ErrorCode` mapping using
  `wiremock` (already in `dev-dependencies`).
- All `tests/*.rs` E2E tests keep passing — they run against the
  legacy ADBC surface, which converts at the edge.

**Out of scope.** Telemetry-side error categorisation. Refining
`retryable` per-category beyond the table above. Both are PR-local
follow-ups in a later PR.

---

### PR 2 — Session

**Goal.**
`Session::builder().http_path(host, http_path).auth(AuthConfig::Pat{token}).open().await`
returns a working session against a Databricks workspace. First
user-visible piece of the v0 surface; gives us a real E2E test.

**What changes.**

- **`SessionInner` body lands.**
  ```rust
  pub(crate) struct SessionInner {
      transport: Arc<dyn Transport>,
      session_id: String,
      config: SessionConfig,         // kept for re-execute / sub-handles
      runtime_handle: tokio::runtime::Handle,
      log_span: tracing::Span,
  }
  ```
- **`Session::open(config)`** builds `Transport` (auth provider +
  HTTP client + retry config + reader factory) from the typed
  `SessionConfig`, calls `transport.create_session(catalog, schema,
  session_conf)`, stores the `session_id`, returns `Session`.
- **`SessionBuilder::open()`** validates required fields
  (`connection`, `auth`), assembles `SessionConfig`, and forwards.
- **`ConnectionConfig`** grows two ergonomic constructors:
  - `ConnectionConfig::new(host, warehouse_id)` — strict form.
  - `ConnectionConfig::from_http_path(host, http_path)` — parses
    `/sql/1.0/warehouses/abc123?o=12345` for the warehouse-id and
    optional org-id query param. Reuses today's
    `Database::extract_warehouse_id` / `extract_org_id` logic.
  - The `org_id` field becomes part of `ConnectionConfig` so
    multi-tenant routing works (today the legacy code threads it
    through `x-databricks-org-id`).
- **`SessionBuilder::http_path(host, http_path)`** shortcut wraps
  `from_http_path` + sets `self.connection`.
- **`AuthConfig` lands fully typed.** All variants get their final
  field-shape (`Pat { token }`, `OAuthM2M { client_id,
  client_secret, scopes, token_url_override }`, `OAuthU2M {
  client_id, redirect_uri, scopes }`, `OAuthFederation {
  subject_token, subject_token_type, audience }`, `External(Box<dyn
  TokenProvider>)`); bodies delegate to the existing `auth/oauth/*`
  modules. Reasoning: the legacy ADBC layer is the only caller
  doing string-bag parsing today; nothing forces us to expose
  stringly-typed auth on the v0 surface even transiently.
- **`Session::close()`** calls `transport.delete_session(&session_id)`.
  `Drop` spawns a fire-and-forget `delete_session` on the runtime
  if `close()` wasn't called explicitly (matches the pattern PR
  #423 added on Statement).

**What does NOT change.**

- `Statement::*` and `Metadata::*` still `unimplemented!()`.
- `src/adbc/database.rs::Database` continues to drive its own
  configuration path (string-bag `Optionable`); the option-parsing
  logic gets factored into helpers shared with
  `ConnectionConfig::from_http_path` so we don't duplicate it.

**Tests.**

- Unit: `ConnectionConfig::from_http_path` parsing (extracted from
  today's `Database` tests). Builder validation (missing required
  fields → `InvalidArgument`).
- Wiremock: `Session::open` → `create_session` POST → success →
  `session_id` populated. Failure modes: 401 → `Unauthenticated`,
  403 → `PermissionDenied`, 503 → `Unavailable + retryable=true`.
- New E2E: `tests/v0_session_e2e.rs` (gated on env vars per
  CLAUDE.md pattern). PAT + OAuth M2M + OAuth U2M each get a
  smoke-test path against a real workspace.

---

### PR 3 — Metadata

**Goal.** All ten metadata methods on `docs/spec-v0.md` work and
return a `ResultStream`-shaped result. Statement-free path —
exercises `Session` end-to-end without needing PR 4 to land first.

**What changes.**

- **`Identifier::new` / `LikePattern::new`** validation bodies:
  reject NUL bytes, reject backticks in `Identifier` (would break
  escaping at the SQL boundary), enforce length limits per
  Databricks docs.
- Each `Metadata::list_*` / `primary_keys` / `foreign_keys` /
  `functions` / `procedures` / `type_info` / `table_types` calls the
  corresponding `Transport::list_*` (existing) or a new
  `Transport::list_primary_keys` / `list_foreign_keys` /
  `list_functions` / `list_type_info` / `list_table_types` method.
- The three metadata calls `client/sea.rs` doesn't have today
  (`primary_keys`, `foreign_keys`, `functions`) are added as new
  builders in `metadata/sql.rs` (`SHOW KEYS`, `SHOW FOREIGN KEYS`,
  `information_schema.routines`) and wired through `Transport`.
- **The kernel does not reshape the result.** `Metadata::list_*`
  passes the server-shaped Arrow batch (`catalog`, `databaseName`,
  `catalogName`, `namespace`, `tableName`, `col_name`, `keySeq`,
  `constraintName`, …) through to `ResultStream` verbatim. Driver
  wrappers (PySQL, Node) consume the server columns directly today;
  JDBC and ODBC drivers already maintain a rename layer
  (`MetadataResultConstants` / `metadata_transformer.cpp`) and
  continue to use it. Two static methods (`type_info` /
  `table_types`) have no server-native shape and are synthesised
  in-memory using the JDBC `getTypeInfo` / `getTableTypes` schemas;
  see `src/metadata/schemas.rs`.
- A thin `ResultStream::from_reader(Box<dyn ResultReader>)` adapter
  lands here — metadata results today fit in one batch from inline
  Arrow, so this is the smallest `ResultStream` body that works.
  Multi-batch behaviour is verified in PR 4.

**What does NOT change.**

- `Statement::*` still `unimplemented!()`.
- `src/adbc/connection.rs`'s `get_objects` keeps working — it now
  *delegates* to `Metadata::list_*` internally and reshapes the
  result into the ADBC-style nested table that `get_objects` expects.

**Tests.**

- Unit: `Identifier`/`LikePattern` validation. Empty-string
  handling for `LikePattern::all()`.
- Wiremock: each metadata method → assert the SQL the kernel emits
  matches the expected SHOW / `information_schema` form.
- New E2E: `tests/v0_metadata_e2e.rs`. `list_catalogs`,
  `list_schemas`, `list_tables`, `list_columns`,
  `primary_keys` / `foreign_keys` against a known dogfood schema.
- The legacy `tests/metadata_e2e.rs` keeps passing (it runs against
  ADBC's `get_objects`, which now goes through `Metadata::list_*`).

---

### PR 4 — Statement + Results (core)

**Goal.** Sync-execute path works end-to-end:
```rust
let stmt = session.statement();
stmt.with_spec(|s| {
    s.sql("SELECT * FROM t WHERE id = ?")
     .param(1, TypedValue::Int(42));
});
let mut exec = stmt.execute().await?;
while let Some(_batch) = exec.result_stream_mut().next_batch().await? {
    while let Some(row) = exec.result_stream_mut().next_row_in_batch() {
        let id = row.get_i64(0)?;
        // …
    }
}
exec.close().await?;
```

This is the bundle: `Statement` + `ExecutedStatement` +
`ResultStream` (batch + row) + `TypedValue` + cancellation. They
all touch `src/statement/*.rs` and `src/result_stream.rs` and form
one cohesive state machine; splitting them would force reviewers to
re-derive the same design context across multiple PRs.

**What changes.**

- **`StatementInner` + `Statement`.** `StatementInner` is wrapped
  behind `Arc<Mutex<…>>`. `with_spec` takes the lock; `spec_snapshot`
  clones under the lock.
- **`Statement::execute()`**:
  1. Auto-cancel + close any prior `last_statement_id`
     (best-effort, fire-and-forget the cancel; await the close).
  2. Flip the prior validity flag to `false`.
  3. Send `ExecuteStatement` with `wait_timeout=10s`. If the server
     finishes inline, results are ready. If status is `RUNNING`,
     poll `GetStatementStatus` with backoff.
  4. Build a `ResultStream` over the resulting `ResultReader`.
  5. Return `ExecutedStatement` carrying `Arc<SessionInner>`,
     `statement_id`, `Arc<AtomicBool>` validity flag, the
     `ResultStream`.
- **`Statement::cancel()`** reads `last_statement_id` from
  `StatementInner` under the lock; if present, sends
  `CancelStatement` to the server. Concurrent with `execute()`
  because everything is `&self` + interior mutability. The future
  returned by `execute()` records its in-flight `statement_id` into
  the shared slot *before* awaiting, so a sibling `cancel()` task
  can reach it. Drop on the in-flight future installs a
  fire-and-forget `CancelStatement` so dropped queries stop
  charging DBUs.
- **`ResultStream` body**:
  ```rust
  pub struct ResultStream {
      reader: Box<dyn ResultReader + Send>,
      schema: SchemaRef,
      current_batch: Option<RecordBatch>,
      current_row_idx: usize,
  }
  ```
  - `schema(&self) -> &Schema` reads from the cached `SchemaRef`.
  - `next_batch(&mut self).await -> Result<Option<&RecordBatch>>`
    — borrowed (rationale in §6); resets the row cursor.
  - `next_row_in_batch(&mut self) -> Option<Row<'_>>` — sync,
    increments the cursor.
  - `into_rows()` → `impl Stream<Item = Result<Row<'_>>>` driving
    `next_batch` + `next_row_in_batch` internally.
  - `into_owned_batches(self) -> impl Iterator<Item = Result<RecordBatch>>`
    — owned-batch adapter for PyO3 / napi-rs / C-ABI consumers.
    Same shape as the existing `Box<dyn RecordBatchReader + Send + 'static>`
    that PR #423's `Statement::execute_owned` produces.
  - `cancel().await` / `close().await`.
- **`Row<'a>` accessors**: `get_str`, `get_i64`/i32/i16/i8,
  `get_u64`/u32/u16/u8, `get_f64`/f32, `get_bool`, `get_bytes`,
  `get_decimal` (returns `&str` in canonical form for v0),
  `get_date` (`chrono::NaiveDate`), `get_timestamp`
  (`chrono::NaiveDateTime`), `get_interval` (`&str` for v0).
  Null-handling on every accessor; type-mismatch returns
  `InvalidArgument`.
- **`TypedValue` variants**: `Null`, `Bool`, `Int8/16/32/64`,
  `UInt*`, `Float32/64`, `String(String)`, `Binary(Vec<u8>)`,
  `Decimal { value: String, precision, scale }`,
  `Date(NaiveDate)`, `Timestamp(NaiveDateTime)`, `Interval(String)`.
  Mirrors what Databricks SEA accepts.
- **Param wire conversion**: `statement/params.rs` grows
  `typed_value_to_sea_parameter` (and `…_named`) — direct
  conversion to SEA wire form, no round-trip through Arrow.
  Today's `record_batch_to_sea_parameters` stays for the legacy
  ADBC `bind`/`bind_stream` path. `count_parameter_markers` is
  reused by both surfaces for sanity checks.
- **`ExecutedStatement::Drop`** spawns a fire-and-forget
  `close_statement` on the runtime handle (matches PR #423's
  pattern).
- **Validity flag**: `Arc<AtomicBool>` shared between `Statement`
  and `ExecutedStatement`. Stale-handle calls short-circuit to
  `ErrorCode::InvalidStatementHandle` *locally* — no wire round-trip.

**What does NOT change.**

- `Statement::submit` / `ExecutedAsyncStatement::*` still
  `unimplemented!()`. PR 5.

**Tests.**

- Wiremock: success path; `RUNNING` → poll → success; server-error
  envelope → `SqlError` with `sql_state` populated.
- E2E: `tests/v0_execute_e2e.rs`. `SELECT 1`, multi-batch
  CloudFetch-driven `SELECT * FROM range(1_000_000)`,
  parameterised `SELECT ?, ?, ?` of mixed `TypedValue` types,
  error-path (`SELECT * FROM does_not_exist`).
- Re-execute test: call `stmt.execute()` twice; verify the first
  `ExecutedStatement` returns `InvalidStatementHandle` after the
  second.
- Cancellation test: `tokio::select!` race between `execute()` and
  a sibling `cancel()`. Verify a cancel RPC fires.
- E2E cancellation: long query in one task, cancel from another;
  server reports the statement as `Canceled`; kernel returns
  `Cancelled` error.
- Unit: every `TypedValue` variant → expected SEA wire form.
- Unit: per-Arrow-type accessor against synthetic `RecordBatch`es.
  Null handling. Mismatched-type errors.
- E2E: roundtrip every supported type via `SELECT cast(... AS T)`.

---

### PR 5 — Async Statement

**Goal.** Async-consumption path. Used by Node.js's default Promise
flow and a future async PySQL API. Mirrors PR 4's machinery but
with caller-driven polling.

**What changes.**

- **`Statement::submit()`** sends `ExecuteStatement` with
  `wait_timeout=0s`, returns `ExecutedAsyncStatement` carrying
  `statement_id` + validity flag. Same auto-cancel-and-close
  semantics as `execute()`.
- **`ExecutedAsyncStatement::status()`** — one
  `GetStatementStatus` RPC, returns the typed `StatementStatus`.
  `Failed` wraps the server's `SqlError` in
  `StatementStatus::Failed(Error)`.
- **`ExecutedAsyncStatement::await_result()`** — polls with
  backoff until terminal, returns the `ResultStream`.
- **`ExecutedAsyncStatement::fetch_result()`** — returns the
  `ResultStream` immediately if status is `Succeeded`;
  `InvalidState` otherwise.
- Drop / cancel / close mirror `ExecutedStatement`.

**Tests.**

- Wiremock: `submit` → `Pending` → `Running` → `Succeeded` poll
  sequence. `submit` → `Failed` → `await_result` returns `Err`.
- E2E: `tests/v0_submit_e2e.rs` against a real warehouse.

---

### PR 6 — C ABI

**Goal.** ODBC and Go consumers can call into the kernel via a
stable C ABI. One header, one set of `extern "C"` functions, one
review.

**What changes.**

- **`include/databricks_kernel.h`** (hand-written; `cbindgen`
  integration is a v0.1 concern). Defines `KernelStatus`, opaque
  handles (`KernelSession`, `KernelStatement`,
  `KernelExecutedStatement`, `KernelExecutedAsyncStatement`,
  `KernelResultStream`, `KernelError`), `KernelSessionConfig` and
  config sub-structs, and every public function. Every public C
  struct carries a leading `uint32_t version` field so future
  readers can detect older callers (rationale in §6).
- **Process-wide tokio runtime.** `OnceLock<tokio::runtime::Runtime>`
  initialised on first `kernel_session_open`. Same handle shared
  with every session.
- **`kernel_error_*` accessors.** `kernel_error_code`,
  `kernel_error_message`, `kernel_error_sql_state`,
  `kernel_error_vendor_code`, `kernel_error_http_status`,
  `kernel_error_query_id`, `kernel_error_release`. Errors are
  returned from a failing call as a `*mut KernelError` out-param;
  caller releases.
- **`kernel_session_*`.** `_open(*const KernelSessionConfig, *mut
  *mut KernelSession, *mut *mut KernelError) -> KernelStatus`,
  `_close`. Auth in v0: PAT + M2M wired through the C ABI; U2M
  goes through `External(Box<dyn TokenProvider>)` only (Rust-crate
  consumers do U2M directly).
- **`kernel_statement_*`.** `_new`, `_set_sql`, `_bind_*` (mirror
  `TypedValue`), `_execute` (returns `*mut KernelExecutedStatement`),
  `_submit` (returns `*mut KernelExecutedAsyncStatement`),
  `_cancel`, `_close`.
- **`kernel_executed_*`.** `_statement_id`, `_result_stream`,
  `_cancel`, `_close`. Mirror set for `_executed_async_*` covering
  `status` / `await_result` / `fetch_result`.
- **`kernel_result_*`.** `_schema` (Arrow C Data Interface
  `ArrowSchema`), `_next_batch` (Arrow C Data Interface
  `ArrowArray` + `ArrowSchema`), `_next_row` + per-column getters
  mirroring `Row<'a>`, `_cancel`, `_close`.
- **`kernel_metadata_*`.** Thin shims over `Metadata::list_*`.
  Take C strings; convert to `Identifier` / `LikePattern`; return
  `*mut KernelResultStream`.

**What does NOT change.**

- The legacy ADBC `metadata-ffi` C ABI under
  `src/adbc/ffi/metadata.rs` keeps working (separate symbols);
  it's removed in PR 7.

**Tests.**

- Hand-written C tests in `tests/c/` linked against the `cdylib`
  build: `session_smoke.c`, `statement_smoke.c`,
  `result_row_smoke.c`, `metadata_smoke.c`. Compiled and run as
  part of `make check`.
- `make check` grows a C-test step
  (`cc tests/c/*.c -lkernel && ./test`).

---

### PR 7 — Cutover: delete `legacy-adbc`

**Goal.** One concern: removal. No new behaviour.

**What changes.**

- `src/adbc/` deleted.
- `src/error.rs` deleted; `src/kernel_error.rs` renamed to
  `src/error.rs`. The crate-root re-export already points here, so
  no consumer change.
- `src/result_stream.rs` renamed to `src/result.rs` (the legacy
  `src/result/mod.rs` is part of the deletion).
- `src/metadata_v0.rs` renamed to `src/metadata.rs` after the
  legacy `src/metadata/` mod absorbs its parsers and SQL builders
  into a `metadata::sql` / `metadata::parse` submodule layout. The
  v0 `Metadata` struct lives at `metadata::Metadata`.
- `Cargo.toml`:
  - `legacy-adbc`, `ffi`, `metadata-ffi` features removed.
  - `adbc_core`, `adbc_ffi`, `driverbase` deps removed.
  - `arrow` (with `ffi` feature) becomes unconditional — the new
    C ABI uses Arrow C Data Interface.
- `manifest.toml` (ADBC driver-manager descriptor) deleted.
- `include/legacy/databricks_metadata_ffi.h` deleted.
- `lib.rs` cleanup: drop the `#[cfg(feature = "legacy-adbc")]`
  block and the `adbc_ffi::export_driver!` macro call.
- `examples/*.rs` (8 files) migrated to the v0 surface. The
  `[[example]]` blocks lose `required-features = ["legacy-adbc"]`.
- `tests/*.rs` (8 files) migrated. Most have a v0 counterpart from
  PRs 2–5; this PR deletes the legacy version.
- README updated to remove "currently exposed via the ADBC trait
  surface" language.
- `Cargo.toml`: `0.1.0` → `0.2.0`.

**What does NOT change.**

- Nothing additive. Pure removal + rename.

**Tests.**

- `cargo test` (no features) passes.
- `cargo test --no-default-features` passes (no features should be
  needed at all post-cutover).
- `make check` (fmt + clippy + tests + C tests from PR 6) green.

---

### PR 8+ — Wrapper repo cutovers (separate, parallel)

Tracked outside this repo; mentioned here so the kernel-side
timeline is grounded.

- **PyO3 satellite**: move into this repo at
  `databricks-sql-kernel/python/` as a Cargo workspace member
  (renamed `databricks-sql-kernel-py`). Single repo + single
  release cadence avoids the cross-repo tag-coordination problem
  the existing `adbc-drivers/databricks/rust-pyo3` location
  forces. Rewrite against `Session::builder()` / `Statement` /
  `ExecutedStatement::result_stream_mut()` /
  `ResultStream::into_owned_batches()`. Add OAuth M2M/U2M,
  parameter binding via `TypedValue`, metadata via `Metadata`,
  async via `submit()`, cancel via `Statement::cancel()`. Drop the
  `databricks-adbc` crate dep. The old satellite at
  `adbc-drivers/databricks/rust-pyo3` (PR #423) is closed
  unmerged — it served its purpose as a POC.
- **`databricks/databricks-sql-python` (PR #782)**: rename
  `AdbcDatabricksClient` → `KernelDatabricksClient`. Wire metadata
  methods to the kernel's `Metadata`. Wire parameter binding,
  async, cancel. Delete the unreachable `backend/sea/` tree.
- **`databricks/databricks-sql-go`**: new C ABI consumer via cgo.
  Implements the standard `database/sql` driver against the
  kernel's C ABI. Existing Thrift driver stays as a parallel
  backend until the kernel's C ABI proves out.
- **`databricks/databricks-sql-nodejs`**: new napi-rs binding
  modelled after the PyO3 satellite. Consumes the Rust crate
  directly.

The kernel exposes one stable surface; each wrapper is its own
repo / release cadence.


## 5. Cross-cutting concerns

### 5.1 Maintaining the legacy ADBC surface

PRs 1–6 must keep `cargo test --features legacy-adbc` green. The
mechanism:

- `src/adbc/` keeps owning its own `Database`/`Connection`/
  `Statement`. After PR 1 it imports `kernel_error::Error` and
  converts at the ADBC trait boundary (one helper per file); also
  picks up the `Transport` rename trivially since the type was
  always `pub(crate)` in spirit.
- After PR 2, `src/adbc/database.rs` *can* reuse
  `ConnectionConfig::from_http_path` for its own option-parsing —
  optional but reduces duplication.
- After PR 3, `src/adbc/connection.rs::get_objects` keeps using its
  existing pipeline (calls `Transport::list_*` directly and reshapes
  into the ADBC nested `get_objects` table) — `Metadata::list_*`
  doesn't reshape, so delegating through it would just throw away
  work the legacy path then has to redo. Both pipelines share the
  same `Transport` / `metadata::sql` plumbing; the legacy
  `parse_*`/`metadata_builder` family goes away in PR 7 alongside
  the rest of `src/adbc/`.
- After PR 4, `src/adbc/statement.rs::execute` delegates to
  `Statement::execute()` via a thin shim. The legacy `bind` /
  `bind_stream` / `prepare` surface lowers to `TypedValue`s.
- PR 7 is the deletion.

### 5.2 Async runtime

Today `src/adbc/database.rs` creates a `tokio::runtime::Runtime`
per `Database`. The v0 surface assumes the *caller* owns the
runtime when consuming Rust crate APIs (PyO3 / napi-rs do); the C
ABI owns a process-wide `OnceLock<Runtime>`.

PR 2 lands the typed surface and assumes a caller-owned runtime
(`async fn`s — the caller `await`s). PR 6 adds the process-wide
runtime for the C ABI. The legacy ADBC layer keeps its own
per-`Database` runtime through PR 7; this is fine because the
legacy layer is the caller from the Rust side and was always
sync-shaped.

### 5.3 `ResultStream` and `ResultReader`

The kernel today has `pub trait ResultReader { fn schema; fn
next_batch }` (sync) at `src/reader/mod.rs`. The v0
`ResultStream::next_batch` is async. PR 4 adds `async fn
next_batch_async()` to the trait with a default impl that calls
the existing sync `next_batch` under
`tokio::task::spawn_blocking`. The CloudFetch reader overrides
with a real async impl (it already drives async work behind a
`block_on` today; the override removes the bounce). The inline
reader keeps the default — Arrow IPC parse is CPU-only and fast.

### 5.4 Send + Sync assertions

`lib.rs` already asserts `Send + Sync` on every public handle.
PR 2's `transport: Arc<dyn Transport>` field needs the trait
declared `Send + Sync`. `tokio::runtime::Handle` is `Send + Sync`.
`tracing::Span` is `Send + Sync`. `Arc<Mutex<StatementInner>>` is
`Send + Sync` if `StatementInner` is `Send`. All on track.

### 5.5 Logging / spans

The legacy ADBC layer attaches a `session_id` span at `Connection`
creation; every log line within the connection inherits it. The v0
`SessionInner` carries the same `tracing::Span`, entered by every
public method via a `let _g = self.inner.log_span.enter();` guard.
Same UX, ported.

### 5.6 Telemetry

`src/telemetry/` is reused unchanged. `TelemetryConfig` is
`#[non_exhaustive]` empty today; v0.1 fills it in.

### 5.7 Examples and integration tests

`examples/` (8 files) and `tests/` (8 files) are all on the
legacy ADBC surface. Migration strategy:

- PRs 2, 3, 4, 5 each ship at least one new `tests/v0_*_e2e.rs`
  covering the new surface. By the time PR 7 lands, every
  behaviour has a v0 test — PR 7 just deletes the legacy
  version.
- Examples are migrated all at once in PR 7 (small set; one PR
  is cleaner than dribbling).

### 5.8 CI

The kernel CI runs `cargo test`, `cargo fmt`, `cargo clippy`,
`cargo-about`. PR 6 adds a C-test step
(`cc tests/c/*.c -lkernel && ./test`) for the C ABI smoke tests.
PR 7 removes the ADBC RAT-style header check on legacy paths.

## 6. Decisions on questions raised during planning

These were "open questions" in earlier drafts. Captured here as
decisions so future PRs don't relitigate.

- **`ResultStream::next_batch` shape.** Borrowed in spec
  (`Result<Option<&RecordBatch>>`); owned-iterator adapter
  (`into_owned_batches`) lands alongside it in PR 4. The borrowed
  shape is what the row cursor needs to return borrowed `Row<'_>`
  without aliasing tricks; the owned adapter is what the PyO3 /
  napi-rs / C ABI consumers want.
- **`from_http_path` ergonomics.** Ship both
  `ConnectionConfig::from_http_path` *and* a `SessionBuilder::http_path`
  shortcut. Every wrapper translates one DSN field; one builder call
  matches that.
- **`ResultReader` async-ification.** Add `async fn next_batch_async`
  to the trait with a default impl that calls the existing sync
  `next_batch` under `tokio::task::spawn_blocking`. The CloudFetch
  reader overrides with a real async impl (it already drives async
  work behind a `block_on` today; the override removes the bounce).
  The inline reader keeps the default — Arrow IPC parse is CPU-only
  and fast.
- **`TypedValue` decimal/date/timestamp shape.** Typed for date and
  timestamp using `chrono::NaiveDate` / `NaiveDateTime` (chrono is
  already a kernel dep). Decimal carries `String` in canonical form
  for v0 — adding `rust_decimal` is a v0.1 follow-up if any
  wrapper actually needs typed decimal arithmetic before serialising.
- **C ABI version pin.** Hand-versioned `KernelStatus`-and-struct
  layout in `include/databricks_kernel.h`. Bump struct layout
  freely through v0.x (no ABI promise); commit to ABI stability at
  v1.0. Every public C struct carries a leading `uint32_t version`
  field so future readers can detect older callers.
- **OAuth U2M and the C ABI.** PR 6 wires PAT + M2M through the
  C ABI. U2M is Rust-crate-only in v0 (PyO3 / napi-rs consumers can
  do U2M directly; ODBC and Go consumers use PAT or M2M). The
  `External(Box<dyn TokenProvider>)` `AuthConfig` variant is the
  escape hatch for callers that want to drive their own browser
  flow and inject tokens. Revisit in v0.1 if the ODBC team needs
  native U2M.
- **Wrapper-repo cutover sequencing.** Cut once. The wrapper repos
  pin to kernel `0.2.0` after PR 7 lands; intermediate kernel
  versions are not published. Keeping `legacy-adbc` published as a
  feature for an extra release would force every internal change
  to support both surfaces — not worth it. The POC repos already
  prove the kernel internals work; the wrapper rewrites are
  mechanical once the v0 surface lands.

## 7. Follow-ups carried by later PRs

Tracking deferred work that came up during a PR but didn't land in
that PR. Each item names the PR it landed in and the PR that should
clear it.

### From PR 1 (Foundations)

- **~~Drop `#![allow(dead_code)]` at the top of `src/client/transport.rs`.~~**
  Done in PR 2. Removed the file-level allow; the `cloudfetch_config`
  field on `TransportConfig` was actually dead (never read by either
  the legacy or v0 path) and got deleted; remaining `#[allow(dead_code)]`
  attributes on `list_procedures` / `list_procedure_columns` /
  `ChunkLinkFetchResult::end_of_stream` / `next_chunk_index` carry
  per-item `reason = "wired into ... in PR N"` so each is independently
  greppable at cleanup time.
- **~~Triage `Error::io()` call sites.~~** Done in PR 2.
  `client/http.rs`'s response-status arms now call
  `Error::from_http_status(s)` (typed code + `retryable=true` for the
  retryable codes); body-read / TCP failures use
  `Error::network_error()`; URL/header build failures use
  `Error::invalid_argument()`. `client/sea.rs`'s `Error::io()` calls
  triaged similarly: build-request → `invalid_argument`, body-read →
  `network_error`, parse-response → `data_loss`, statement timeout →
  `timeout`. The `Error::io()` constructor stays in `kernel_error`
  for callers that genuinely want "I/O" without a more specific
  category; today nothing inside the kernel uses it.
- **~~Wiremock end-to-end test for the HTTP status → `ErrorCode`
  mapping.~~** Done in PR 2 (`tests/v0_session_wiremock.rs`). 13
  tests cover 200 / 400 / 401 / 403 / 404 / 429 / 500 / 503 / 504
  via `Session::open` end-to-end, plus builder validation
  (missing required fields, malformed http_path).
- **Drop `#[allow(dead_code, …)]` on `SeaChunkLinkFetcher::has_link`
  / `get_link` / `all_links_fetched`.** Originally tagged for PR 2,
  but on closer inspection these public methods aren't reached even
  under PR 2's `ResultReaderFactory` wiring — the *fields* are read
  via direct field access (`self.all_links_fetched.load(...)`), not
  these accessor methods. Either delete the methods (likely) or wire
  them in PR 4's ResultStream surface. Deferred to PR 4.
- **Drop `#[allow(dead_code, …)]` on `SqlCommandBuilder::escape_sql_string`,
  `resolve_catalog_prefix`, `build_get_procedures`,
  `build_get_procedure_columns` in `src/metadata/sql.rs`.** Reached
  only under feature `metadata-ffi` today. PR 3's `Metadata`
  wiring calls these from the v0 path; drop the allows there.
- **Refine the legacy `kernel_error::Error → adbc_core::error::Error`
  status mapping.** PR 1 maps `ResourceExhausted`, `Unavailable`,
  `NetworkError` → `adbc_core::Status::IO` and `SqlError` →
  `Status::Internal` because ADBC's status enum is coarser than
  `ErrorCode`. ADBC consumers see less detail than the v0 surface
  exposes. Acceptable through PR 7 (which deletes the legacy ADBC
  layer entirely); no fix needed before then.
- **`extract_sqlstate_from_message` signature divergence.** PR 1's
  `kernel_error::extract_sqlstate_from_message` returns
  `Option<String>`. The legacy `crate::error::extract_sqlstate_from_message`
  returns `Option<[c_char; 5]>` and is no longer publicly used —
  only `error.rs::From<kernel_error::Error>` reaches its private
  `sqlstate_to_array` helper. Both go away in PR 7.
- **`Error::not_implemented()` maps to `InvalidArgument`.** v0's
  `ErrorCode` has no `NotImplemented` variant. The constructor name
  matches driverbase's parity for the swap, but the underlying code
  is `InvalidArgument`. Addendum A8's variant expansion may add a
  proper `Unimplemented` (or `NotImplemented`) variant; revisit then.
- **Tighten `SeaChunkLinkFetcher` visibility.** PR 1 removed the
  `pub use SeaChunkLinkFetcher` re-export from
  `src/reader/cloudfetch/mod.rs` because the type is now
  `pub(crate)`. No external callers touched it — the symbol was
  only ever surfaced for in-crate convenience. PR 4 may want to
  restore a `pub(crate)` re-export if the v0 wiring grows a need.

### From PR 2 (Session)

- **`SessionBuilder::http_path` defers parse errors to `open()`.** PR 2
  stores a `parse_error: Option<String>` sentinel on the builder
  rather than returning a `Result` from the setter (which would
  break the fluent infallible-builder pattern). The error surfaces
  on `open()`. An alternative would be a fallible builder that
  short-circuits via `Result<SessionBuilder, Error>` chaining, but
  that's a larger ergonomic shift. Reconsider if/when other setters
  grow validation.
- **`AuthConfig::OAuthFederation` and `AuthConfig::External` are
  reserved.** PR 2 defines the field shape but `Session::open`
  rejects both with `InvalidArgument`. OAuthFederation needs the
  actual RFC 8693 token-exchange flow implementation in
  `auth/oauth/`. `External` needs the C ABI to surface a way for
  callers to pass a `dyn TokenProvider` across the FFI boundary.
  Tracked as v0.1 work.
- **`Drop` semantics for `SessionInner` use a `closed` AtomicBool.**
  `Session::close()` flips the flag **after** a successful
  `delete_session` await — on `Err` or future cancellation the
  flag stays `false` so `Drop`'s fire-and-forget retry path picks
  up the slack and the server-side session doesn't leak (per
  gopalldb's PR 6 review). Double-close protection works for the
  common case (close-then-drop) but races could in principle let
  two `delete_session` calls fire if `close()` is racing
  concurrent clones. Acceptable — `delete_session` is idempotent
  server-side; the second call gets a 404 which the SEA impl
  swallows. Worth a closer look in PR 4 if the Statement
  re-execute path stresses the same pattern.
- **HTTP retry config is hardcoded to legacy defaults.** PR 2 wires
  the global `RetryConfig::default()` plus the auth-specific
  30s/3-retries override. The public `HttpConfig` doesn't expose
  retry knobs; if a wrapper needs to tune retries (e.g. a tighter
  ceiling for interactive UIs), they'd need to land later. Captured
  as a follow-up for whichever PR exposes `HttpConfig` retry fields.
- **`Session::statement()` and `Session::metadata()` return handles
  whose methods still `unimplemented!()`.** Those bodies land in PRs
  4 and 3 respectively. Today calling either returns a constructable
  handle that panics on use — wrappers won't hit this in the typical
  flow because they don't call those handles' methods until the
  feature lands.
- **No `tracing::Span::enter` on most `Session` methods yet.**
  `close()` enters the per-session span. Other methods land in PRs
  3-4 and need to enter the span explicitly so log lines tied to a
  session are visibly grouped (matches the legacy ADBC layer's
  `_log_span` pattern).

## 8. Estimated scope

| PR | LoC est | Risk    | Notes                                                                  |
|---:|--------:|---------|------------------------------------------------------------------------|
| 1  | ~800    | Low     | Mechanical: type-alias swap across internals + Transport rename.       |
| 2  | ~800    | Medium  | First real public-API body. PAT + OAuth M2M/U2M, typed config groups.  |
| 3  | ~700    | Medium  | New `information_schema` SQL builders + reshape.                       |
| 4  | ~2300   | High    | Core query path: Statement, ResultStream, Row, TypedValue, cancel.     |
| 5  | ~400    | Medium  | Mirrors PR 4 with caller-driven polling.                               |
| 6  | ~2200   | High    | C ABI: header, opaque handles, runtime mgmt, full surface.             |
| 7  | -2500   | Low     | Pure deletion + rename. Net negative LoC.                              |

Total new code: ~7,200 LoC; deletions in PR 7 bring the net to
~4,700. Roughly six weeks at one PR per ~5 days; PR 4 and PR 6
are the long poles and likely justify two-week reviews each.
