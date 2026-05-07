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
2. **No PR changes more than one concern.** If `Session::open` and
   `Statement::execute` both need `kernel_error::Error` end-to-end,
   `kernel_error` lands first as its own PR.
3. **Test-after-each-step is real.** Every PR ships either a passing
   unit test against the new surface, or a passing E2E test against a
   live warehouse. Skeleton-only PRs (no behaviour) don't count.

The resulting order is not what the breadcrumbs hint:

| PR  | What                                    | Why this order                                                     |
|----:|-----------------------------------------|--------------------------------------------------------------------|
| A   | `kernel_error::Error` end-to-end        | Smallest. Every later PR converts to it; do it once, not N times.  |
| B   | `Transport` trait + `SeaRestTransport`  | Renames `DatabricksClient` in place. No public-API change.         |
| C   | `Session::open` + `SessionConfig` typed | First user-visible v0 surface. Standalone E2E test possible.       |
| D   | `Metadata::list_*` + `Identifier`/`LikePattern` | Statement-free; tests `Session` + reuses metadata SQL builders. |
| E   | `StatementSpec` + `Statement::execute` + `ExecutedStatement` + `ResultStream::next_batch` | Core query path. Replaces ADBC `Statement::execute`. |
| F   | `ResultStream::next_row_in_batch` + `Row<'a>` | Row cursor. ODBC/PySQL/Go consume this.                       |
| G   | `Statement::submit` + `ExecutedAsyncStatement` | Async-consumption path. Used by Node.js + future async PySQL. |
| H   | Cancellation + Drop semantics           | Needs `Statement` and `ExecutedStatement` to exist.                |
| I   | `TypedValue` + parameter binding        | Independent of E–H mechanics; can land in parallel after E.        |
| J   | C ABI: error + session                  | Smallest C surface. Validates the runtime-management story.        |
| K   | C ABI: statement + result + metadata    | Bulk of the FFI; depends on J.                                     |
| L   | Cutover: delete `src/adbc/`, examples & tests on v0 surface | One concern: removal. No new behaviour.   |

Independent track:

| PR  | What                                    | Why                                                                |
|----:|-----------------------------------------|--------------------------------------------------------------------|
| M+  | Wrapper repos cut over                  | Separate repos, separate cadences. See §6.                         |

The rest of this section gives the contract for each PR.

### PR A — `kernel_error::Error` end-to-end

**Goal.** Stop using `driverbase::Error` anywhere except the legacy ADBC
shim. All internal modules return `kernel_error::Result<T>`.

**What changes.**

- Internal modules (`auth/`, `client/`, `reader/`, `metadata/`,
  `statement/params.rs`, `types/`) switch their `Result<T>` alias from
  the driverbase-backed `crate::error::Result` to the new
  `crate::kernel_error::Result`.
- Conversion at the wire boundary: HTTP/SEA-error decoding produces
  `Error { code, message, sql_state, vendor_code, http_status,
  retryable, query_id, cause }` directly. Mapping (initial table —
  refined as PRs land):
  - `429` → `ResourceExhausted`, `retryable=true`
  - `503` → `Unavailable`, `retryable=true`
  - `504` → `Timeout`, `retryable=true`
  - `401` → `Unauthenticated`
  - `403` → `PermissionDenied`
  - `404` → `NotFound`
  - SEA `error_code` envelope → `SqlError` with `sql_state` /
    `vendor_code` populated from the envelope
  - DNS/TCP/TLS → `NetworkError`
  - Internal panics caught at FFI / `block_on` edge → `Internal`
  - Validation failures (`invalid_argument`, `invalid_state`) →
    `InvalidArgument`
- Legacy ADBC shim (`src/adbc/*`) gets a `kernel_error::Error →
  adbc_core::error::Error` conversion replacing today's `.to_adbc()`
  on driverbase errors. ADBC trait impls keep returning
  `adbc_core::error::Result`.
- `extract_sqlstate_from_message` (today in `src/error.rs`) moves into
  `kernel_error.rs` as a fallback for messages that don't carry a
  structured envelope.

**What does NOT change.**

- `crate::error::DatabricksErrorHelper` and the driverbase dep stay
  for now — they're used only by `src/adbc/` after this PR. Both are
  deleted in PR L.
- The crate-root `pub use` from `lib.rs` still exposes
  `kernel_error::{Error, ErrorCode}` (already wired).

**Tests.**

- Existing `crate::error::tests` move/adapt under
  `kernel_error::tests`: SQLSTATE extraction, error display.
- New unit tests for the HTTP-status → `ErrorCode` mapping using
  `wiremock` (this dep is already in `dev-dependencies`).
- All `tests/*.rs` E2E tests keep passing (they run against the legacy
  ADBC surface, which converts at the edge).

**Out of scope.** Telemetry-side error categorisation. Refining
`retryable` per-category. Both PR-local follow-ups.

---

### PR B — `Transport` trait + `SeaRestTransport`

**Goal.** Make the kernel's wire client a `pub(crate) trait Transport`
with one implementation (`SeaRestTransport`). The trait is internal;
wrappers select via `SessionConfig.transport: Option<TransportMode>`.

**What changes.**

- `src/client/mod.rs::DatabricksClient` is renamed `Transport` and
  moved to `src/client/transport.rs` as `pub(crate)`. The trait shape
  doesn't change — same async methods (`create_session`,
  `delete_session`, `execute_statement`, `get_result_chunks`,
  `cancel_statement`, `close_statement`, metadata calls).
- `SeaClient` (`src/client/sea.rs`) is renamed `SeaRestTransport` and
  becomes `impl Transport for SeaRestTransport`.
- `Arc<dyn DatabricksClient>` call sites become `Arc<dyn Transport>`.
  All within `src/`; the type was already `pub(crate)` in spirit.
- `client/mod.rs` keeps re-exporting the http client + retry config
  publicly; only the trait moves to `pub(crate)`.

**What does NOT change.**

- `src/adbc/` keeps compiling because `Database` constructs the
  transport via the renamed type, then hands it to its own
  `Connection`/`Statement` as before.
- No new transport variants. `TransportMode::SeaRest` is the only
  variant.

**Tests.** Rename-only PR. `cargo test` passes unchanged.

---

### PR C — `Session::open` + `SessionConfig` typed groups

**Goal.** `Session::builder().connection(…).auth(…).open().await`
returns a working session against a Databricks workspace. First
user-visible piece of the v0 surface.

**What changes.**

- `SessionInner` (in `src/session.rs`) gets its real body:
  ```rust
  pub(crate) struct SessionInner {
      transport: Arc<dyn Transport>,
      session_id: String,
      config: SessionConfig,         // kept for re-execute / sub-handles
      runtime_handle: tokio::runtime::Handle,
      log_span: tracing::Span,
  }
  ```
- `Session::open(config)` builds the `Transport` (auth provider +
  HTTP client + retry config + reader factory) from the typed
  `SessionConfig`, calls `transport.create_session(catalog, schema,
  session_conf)`, stores the `session_id`, returns `Session`.
- `SessionBuilder::open()` validates required fields (`connection`,
  `auth`), assembles `SessionConfig`, and forwards.
- `ConnectionConfig` grows two ergonomic constructors:
  - `ConnectionConfig::new(host, warehouse_id)` — the simple form.
  - `ConnectionConfig::from_http_path(host, http_path)` — parses
    `/sql/1.0/warehouses/abc123?o=12345` for the warehouse-id and
    optional org-id query param. Reuses today's
    `Database::extract_warehouse_id` / `extract_org_id` logic.
  - The `org_id` field becomes part of `ConnectionConfig` so multi-
    tenant routing works (today the legacy code threads it through
    `x-databricks-org-id`).
- `AuthConfig` lands fully typed in this PR. All variants get
  their final field-shape (`Pat { token }`, `OAuthM2M { client_id,
  client_secret, scopes, token_url_override }`, `OAuthU2M {
  client_id, redirect_uri, scopes }`, `OAuthFederation {
  subject_token, subject_token_type, audience }`, `External(Box<dyn
  TokenProvider>)`); bodies delegate to the existing `auth/oauth/*`
  modules. Reasoning: the legacy ADBC layer is the only caller
  doing string-bag parsing today, and it can keep doing that
  internally; nothing forces us to expose stringly-typed auth on
  the v0 surface even transiently.
- `Session::close()` calls `transport.delete_session(&session_id)`.
  `Drop` spawns a fire-and-forget `delete_session` on the runtime
  if `close()` wasn't called explicitly (matches the pattern PR
  #423 added on Statement).

**What does NOT change.**

- `Statement::*` and `Metadata::*` still `unimplemented!()`. PR D / E
  populate them.
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
- New E2E: `tests/v0_session_e2e.rs` (gated on env vars per CLAUDE.md
  pattern). `Session::builder().connection(...).auth(Pat{...}).open()
  .await?` against a real workspace, then `close().await?`. PAT only
  in this PR.

**Decision: ship the shortcut.** `SessionBuilder` grows
```rust
pub fn http_path(self, host: impl Into<String>, http_path: impl Into<String>) -> Self;
```
…which under the hood calls `ConnectionConfig::from_http_path` and
sets `self.connection`. Every wrapper today translates one DSN field
(`http_path` / `httpPath` / `HTTPPath`) into the connection identity;
forcing them to call a separate parser before the builder is needless
ceremony. The strict `connection(ConnectionConfig)` setter is still
available for callers that already have a `ConnectionConfig`.

---

### PR D — `Metadata` (Identifier/LikePattern + list_*)

**Goal.** All ten metadata methods on `docs/spec-v0.md` work and
return a `ResultStream`-shaped result. Statement-free path —
exercises `Session` end-to-end without needing the Statement work to
land first.

**What changes.**

- `Identifier::new` and `LikePattern::new` get their validation
  bodies: reject NUL bytes, reject backticks in `Identifier` (would
  break escaping at the SQL boundary), enforce length limits per
  Databricks docs.
- Each `Metadata::list_*` / `primary_keys` / `foreign_keys` /
  `functions` / `procedures` / `type_info` / `table_types` calls the
  corresponding `Transport::list_*` (existing) or a new
  `Transport::list_primary_keys` / `list_foreign_keys` /
  `list_functions` / `list_type_info` / `list_table_types` method.
- The two metadata calls that `client/sea.rs` doesn't have today
  (`primary_keys`, `foreign_keys`, `functions`, `type_info`) are
  added as new SQL builders in `metadata/sql.rs` against
  `information_schema.*` and wired through `Transport`.
- The result schema for each method matches the ODBC contract
  (`SQLPrimaryKeys`, `SQLForeignKeys`, …). The schema-fixing logic
  that `src/adbc/metadata_builder.rs` does today (column renaming,
  type adjustments) moves into `metadata/parse.rs` so it's reused
  from both the legacy ADBC layer and the new `Metadata` API.
- `ResultStream` does not need its full body for this PR — metadata
  results today fit in one batch from inline Arrow, so a thin
  `ResultStream::from_reader(Box<dyn ResultReader>)` adapter is
  sufficient. Multi-batch behaviour is verified in PR E.

**What does NOT change.**

- `Statement::*` still `unimplemented!()`.
- `src/adbc/connection.rs`'s `get_objects` keeps working — it now
  *delegates* to `Metadata::list_*` internally and reshapes the
  result into the ADBC-style nested table that `get_objects` expects.

**Tests.**

- Unit: `Identifier`/`LikePattern` validation. Empty-string handling
  for `LikePattern::all()`.
- Wiremock: each metadata method → assert the SQL the kernel emits
  matches the expected SHOW / `information_schema` form.
- New E2E: `tests/v0_metadata_e2e.rs`. `list_catalogs`,
  `list_schemas`, `list_tables`, `list_columns`,
  `primary_keys`/`foreign_keys` against a known dogfood schema.
- The legacy `tests/metadata_e2e.rs` keeps passing (it runs against
  ADBC's `get_objects`, which now goes through `Metadata::list_*`).

---

### PR E — `Statement::execute` + `ExecutedStatement` + `ResultStream::next_batch`

**Goal.** Sync-execute path works end-to-end:
```rust
let stmt = session.statement();
stmt.with_spec(|s| { s.sql("SELECT 1"); });
let mut exec = stmt.execute().await?;
while let Some(batch) = exec.result_stream_mut().next_batch().await? {
    /* batch is &arrow::record_batch::RecordBatch */
}
exec.close().await?;
```

**What changes.**

- `StatementInner` is wrapped behind `Arc<Mutex<…>>`. `with_spec`
  takes the lock; `spec_snapshot` clones under the lock.
- `Statement::execute()`:
  1. Auto-cancel + close any prior `last_statement_id` (best-effort,
     fire-and-forget the cancel; await the close).
  2. Flip the prior validity flag to `false`.
  3. Send `ExecuteStatement` with `wait_timeout=10s`. If the server
     finishes inline, results are ready. If status is `RUNNING`, poll
     `GetStatementStatus` with backoff.
  4. Build a `ResultStream` over the resulting `ResultReader`.
  5. Return `ExecutedStatement` carrying `Arc<SessionInner>`,
     `statement_id`, `Arc<AtomicBool>` validity flag, the `ResultStream`.
- `ResultStream` body lands in this PR (batch-fetch only; row cursor
  in PR F):
  ```rust
  pub struct ResultStream {
      reader: Box<dyn ResultReader + Send>,
      schema: SchemaRef,
      current_batch: Option<RecordBatch>,
  }
  ```
  - `schema(&self) -> &Schema` reads from the cached `SchemaRef`.
  - `next_batch(&mut self).await -> Result<Option<&RecordBatch>>`
    awaits a `Box::pin(reader.next_batch_async())` adapter (today
    `ResultReader::next_batch` is sync; we add an async variant or
    use `runtime_handle.spawn_blocking` — picked in PR E based on
    the actual reader behaviour).
  - `cancel().await` — sends `CancelStatement` against the
    statement_id, marks the stream cancelled.
  - `close().await` — sends `CloseStatement`, drops the reader.
- `ExecutedStatement`'s `Drop` spawns a fire-and-forget
  `close_statement` on the runtime handle (matches the pattern PR
  #423 added).
- The `'static` validity flag short-circuits stale-handle calls to
  `ErrorCode::InvalidStatementHandle` locally — no wire round-trip.

**What does NOT change.**

- `Statement::submit` / `ExecutedAsyncStatement::*` still
  `unimplemented!()`. PR G.
- `Row<'a>` accessors still `unimplemented!()`. PR F.
- Parameter binding still `unimplemented!()`. PR I.

**Tests.**

- Wiremock: success path, `RUNNING` → poll → success, server-error
  envelope → `SqlError` with `sql_state` populated.
- E2E: `tests/v0_execute_e2e.rs`. `SELECT 1`, multi-batch
  CloudFetch-driven `SELECT * FROM range(1_000_000)`, error-path
  (`SELECT * FROM does_not_exist`).
- Re-execute test: call `stmt.execute()` twice; verify the first
  `ExecutedStatement` returns `InvalidStatementHandle` after the
  second.

**Decision: ship borrowed in PR E; add an owned iterator adapter in
PR F.** The spec signature stays
`async fn next_batch(&mut self) -> Result<Option<&RecordBatch>>`
because (a) it's what the row cursor needs to return borrowed `Row<'_>`
without aliasing tricks, and (b) `RecordBatch::clone()` is cheap (the
Arrow buffers are `Arc`-backed) so wrappers that want owned batches
pay only the metadata clone. PR F adds:
```rust
impl ResultStream {
    pub fn into_owned_batches(self) -> impl Iterator<Item = Result<RecordBatch>>;
}
```
which the PyO3 / napi-rs bindings consume — exact same shape as the
existing `Box<dyn RecordBatchReader + Send + 'static>` the POC PR
#423 added to ADBC's `Statement::execute_owned`. Wrappers move from
"call `execute_owned()` once, hold the reader" to "call `execute()`
then `into_owned_batches()`" — semantically identical.

---

### PR F — `Row<'a>` + `next_row_in_batch` + `into_rows`

**Goal.** Per-row consumption with zero allocation in the hot loop.
The ODBC, PySQL row-style, and Go drivers consume this.

**What changes.**

- `ResultStream` grows `current_row_idx: usize`. `next_batch()`
  resets it to 0; `next_row_in_batch()` increments and returns
  `Some(Row { batch: &self.current_batch, row_idx })` or `None` when
  the batch is drained.
- `Row<'a>` accessors materialise: `get_str`, `get_i64`, `get_i32`,
  `get_i16`, `get_i8`, `get_u64`/…, `get_f64`, `get_f32`, `get_bool`,
  `get_bytes`, `get_decimal` (returns a borrowed `&str` in canonical
  form for v0 — full `rust_decimal` typing in v0.1), `get_date`
  (`chrono::NaiveDate`), `get_timestamp` (`chrono::NaiveDateTime`),
  `get_interval` (returns `&str` for v0), null-handling on every
  accessor.
- `get_*` methods type-check the column against the Arrow schema and
  return `InvalidArgument` for mismatches.
- `into_rows()` returns `impl Stream<Item = Result<Row<'_>>>` driving
  `next_batch` + `next_row_in_batch` internally.
- `into_owned_iter()` (sibling to `into_rows`) returns an Arrow
  `RecordBatchReader`-shaped iterator over owned `RecordBatch`es —
  what the PyO3 / napi-rs bindings consume.

**Tests.**

- Unit: per-type accessor against a synthetic `RecordBatch`. Null
  handling. Mismatched-type errors.
- E2E: roundtrip every supported type via `SELECT cast(... AS T)`.

---

### PR G — `Statement::submit` + `ExecutedAsyncStatement`

**Goal.** Async-consumption path. Used by Node.js's default Promise
flow and a future async PySQL API.

**What changes.**

- `Statement::submit()` sends `ExecuteStatement` with
  `wait_timeout=0s`, returns `ExecutedAsyncStatement` carrying
  `statement_id` + validity flag.
- `ExecutedAsyncStatement::status()` issues one
  `GetStatementStatus` RPC, returns the typed `StatementStatus`.
  Failed → wraps the server's `SqlError` in `StatementStatus::Failed(Error)`.
- `ExecutedAsyncStatement::await_result()` polls with backoff until
  terminal, returns the `ResultStream`.
- `ExecutedAsyncStatement::fetch_result()` returns the `ResultStream`
  immediately if status is `Succeeded`; `InvalidState` otherwise.
- Drop / cancel / close mirror `ExecutedStatement`.

**Tests.**

- Wiremock: `submit` → `Pending` → `Running` → `Succeeded` poll
  sequence. `submit` → `Failed` → `await_result` returns `Err`.
- E2E: `tests/v0_submit_e2e.rs` against a real warehouse.

---

### PR H — Cancellation + Drop

**Goal.** `Statement::cancel()` is safe and useful concurrent with
`execute()` / `submit()`. Drop semantics for in-flight futures.

**What changes.**

- `Statement::cancel()` reads `last_statement_id` from `StatementInner`
  under the lock; if present, sends `CancelStatement` to the server.
  Concurrent with `execute()` because everything is `&self` +
  interior mutability.
- The future returned by `execute()` records its in-flight
  `statement_id` into the shared slot *before* awaiting. If the
  future is dropped (e.g. `tokio::select!` losing a race), the
  `Drop` impl on the future installs a fire-and-forget
  `CancelStatement` on the runtime handle — server-side query stops
  charging DBUs.
- Same for `submit()`.

**Tests.**

- Unit: `tokio::select!` race between `execute()` and a sibling
  `cancel()`. Verify a cancel RPC fires.
- E2E: `tests/v0_cancel_e2e.rs`. Run a long query in one task,
  cancel from another, verify the server reports the statement as
  `Canceled` and the kernel returns `Cancelled` error.

---

### PR I — `TypedValue` + parameter binding

**Goal.** `stmt.with_spec(|s| { s.param(1, TypedValue::Int(42)); });`
works on the wire.

**What changes.**

- `TypedValue` variants: `Null`, `Bool`, `Int8/16/32/64`, `UInt*`,
  `Float32/64`, `String(String)`, `Binary(Vec<u8>)`, `Decimal { value:
  String, precision, scale }`, `Date(NaiveDate)`,
  `Timestamp(NaiveDateTime)`, `Interval(String)`. Mirrors what
  Databricks SEA accepts.
- `statement/params.rs` grows `typed_value_to_sea_parameter` (and
  `typed_value_named`) — direct conversion to SEA wire form, no
  round-trip through Arrow. Today's `record_batch_to_sea_parameters`
  stays for the ADBC `bind`/`bind_stream` path.
- `Statement::execute()` reads `spec.positional_params` and
  `spec.named_params` and forwards to `Transport::execute_statement`.
- `count_parameter_markers` (today in `statement/params.rs`) is
  reused by both surfaces for sanity checks.

**Tests.**

- Unit: every `TypedValue` variant → expected SEA wire form.
- E2E: parameterised `SELECT ?, ?, ?` of mixed types; verify
  round-trip equality.

---

### PR J — C ABI: error + session

**Goal.** ODBC and Go consumers can call into the kernel via a
stable C ABI. This PR validates the runtime-management story: one
process-wide tokio runtime, opaque handles via `Box::into_raw`,
status enum returned from every call.

**What changes.**

- Process-wide tokio runtime: `OnceLock<tokio::runtime::Runtime>`
  initialised on first `kernel_session_open` call. Same handle
  shared with every session.
- `kernel_error_*` accessors: `kernel_error_code`,
  `kernel_error_message`, `kernel_error_sql_state`,
  `kernel_error_vendor_code`, `kernel_error_http_status`,
  `kernel_error_query_id`, `kernel_error_release`. The error is
  returned from a failing call as a `*mut KernelError` out-param;
  caller releases.
- `kernel_session_open(*const KernelSessionConfig, *mut *mut
  KernelSession, *mut *mut KernelError) -> KernelStatus`. The
  config is a C struct; the wrapper builds a `SessionConfig` from
  it.
- `kernel_session_close(*mut KernelSession, …)`: `block_on(close())`,
  free the box.
- The legacy ADBC `metadata-ffi` C ABI under
  `src/adbc/ffi/metadata.rs` keeps working (separate symbols); it's
  removed in PR L.

**What does NOT change.**

- The C header (`include/databricks_kernel.h`) is hand-written in
  this PR — `cbindgen` integration is a v0.1 concern.

**Tests.**

- A hand-written C test in `tests/c/session_smoke.c` linked against
  the `cdylib` build of the kernel. Drives `kernel_session_open` →
  `kernel_session_close`. Compiled and run as part of `make check`.

---

### PR K — C ABI: statement + result + metadata

**Goal.** Full ODBC-shaped C surface. Statement execute / result
batch + row consumption / metadata.

**What changes.**

- `kernel_statement_*`: `_new`, `_set_sql`, `_bind_*`,
  `_execute` (returns `*mut KernelExecutedStatement`), `_submit`
  (returns `*mut KernelExecutedAsyncStatement`), `_cancel`, `_close`.
- `kernel_executed_*`: `_statement_id`, `_result_stream`,
  `_cancel`, `_close`.
- `kernel_result_*`: `_schema` (returns Arrow C Data Interface
  struct — same FFI shape today's `metadata-ffi` uses for schema),
  `_next_batch` (Arrow C Data Interface for `RecordBatch`),
  `_next_row` + per-column getters mirroring `Row<'a>`, `_cancel`,
  `_close`.
- `kernel_metadata_*`: thin shims over `Metadata::list_*`. Take C
  strings; convert to `Identifier` / `LikePattern`; return
  `*mut KernelResultStream`.

**Tests.**

- Hand-written C tests for each major flow:
  `tests/c/statement_smoke.c`, `tests/c/result_row_smoke.c`,
  `tests/c/metadata_smoke.c`. Run as part of `make check`.

---

### PR L — Cutover: delete `legacy-adbc`

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
  - `arrow` (with `ffi` feature) becomes unconditional — the new C
    ABI uses Arrow C Data Interface.
- `manifest.toml` (ADBC driver-manager descriptor) deleted.
- `include/legacy/databricks_metadata_ffi.h` deleted; replaced by
  the new `include/databricks_kernel.h` (PR J/K).
- `lib.rs` cleanup: drop the `#[cfg(feature = "legacy-adbc")]` block
  and the `adbc_ffi::export_driver!` macro call.
- `examples/*.rs` migrated to the v0 surface (the eight examples in
  `examples/` today all import the legacy ADBC types). The
  `[[example]]` blocks lose `required-features = ["legacy-adbc"]`.
- `tests/*.rs` (eight integration tests) migrated. Most are E2E
  tests that already have a v0 counterpart from PRs C–H; this PR
  deletes the legacy version.

**What does NOT change.**

- Nothing additive. Pure removal + rename.

**Tests.**

- `cargo test` (no features) passes.
- `cargo test --no-default-features` passes (no features should be
  needed at all post-cutover).
- `make check` (fmt + clippy + tests) green.

**Version bump.** `Cargo.toml`: `0.1.0` → `0.2.0`. README updated to
remove "currently exposed via the ADBC trait surface" language.

---

### PR M+ — Wrapper repo cutovers (separate, parallel)

Tracked outside this repo; mentioned here so the kernel-side timeline
is grounded.

- **PyO3 satellite**: move into this repo at
  `databricks-sql-kernel/python/` as a Cargo workspace member
  (renamed `databricks-sql-kernel-py`). Single repo + single
  release cadence avoids the cross-repo tag-coordination problem
  the existing `adbc-drivers/databricks/rust-pyo3` location forces.
  Rewrite against `Session::builder()` / `Statement` /
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

PRs A–K must keep `cargo test --features legacy-adbc` green. The
mechanism:

- `src/adbc/` keeps owning its own `Database`/`Connection`/
  `Statement`. After PR A it imports `kernel_error::Error` and
  converts at the ADBC trait boundary (one helper per file).
- After PR B, `src/adbc/database.rs` constructs `SeaRestTransport`
  directly (the rename), then hands it down. No structural change.
- After PR C, `src/adbc/database.rs` *can* reuse
  `ConnectionConfig::from_http_path` for its own option-parsing —
  optional but reduces duplication.
- After PR D, `src/adbc/connection.rs::get_objects` delegates to
  `Metadata::list_*` and reshapes the result.
- After PRs E–H, `src/adbc/statement.rs::execute` delegates to
  `Statement::execute()` via a thin shim. The legacy `bind` /
  `bind_stream` / `prepare` surface lowers to `TypedValue`s.
- PR L is the deletion.

### 5.2 Async runtime

Today `src/adbc/database.rs` creates a `tokio::runtime::Runtime`
per `Database`. The v0 surface assumes the *caller* owns the
runtime when consuming Rust crate APIs (PyO3 / napi-rs do); the C
ABI owns a process-wide `OnceLock<Runtime>`.

PR C lands the typed surface and assumes a caller-owned runtime
(`async fn`s — the caller `await`s). PR J adds the process-wide
runtime for the C ABI. The legacy ADBC layer keeps its own
per-`Database` runtime through PR L; this is fine because the
legacy layer is the caller from the Rust side and was always
sync-shaped.

### 5.3 `ResultStream` and `ResultReader`

The kernel today has `pub trait ResultReader { fn schema; fn
next_batch }` (sync) at `src/reader/mod.rs`. The v0
`ResultStream::next_batch` is async. PR E picks one of:

1. Add `async fn next_batch_async()` to `ResultReader` (default
   impl forwards to the sync version).
2. `ResultStream` uses `tokio::task::spawn_blocking` to call the
   sync reader, awaiting the join handle.

(1) is cleaner; (2) keeps the reader implementation untouched. The
choice gets made in PR E after reviewing whether each reader's
`next_batch` blocks on I/O (CloudFetch download) or is purely CPU
(Arrow IPC parse). For v0 we likely want (1) — the CloudFetch
reader already drives async work behind a `block_on` today.

### 5.4 Send + Sync assertions

`lib.rs` already asserts `Send + Sync` on every public handle. PR
4a's `transport: Arc<dyn Transport>` field needs the trait declared
`Send + Sync`. `tokio::runtime::Handle` is `Send + Sync`. `tracing::Span`
is `Send + Sync`. `Arc<Mutex<StatementInner>>` is `Send + Sync` if
`StatementInner` is `Send`. All on track.

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

`examples/` (8 files) and `tests/` (8 files) are all on the legacy
ADBC surface. Migration strategy:

- PRs C, D, E, F, G, H, I each ship at least one new
  `tests/v0_*_e2e.rs` covering the new surface. By the time PR L
  lands, every behaviour has a v0 test — PR L just deletes the
  legacy version.
- Examples are migrated all at once in PR L (small set; one PR is
  cleaner than dribbling).

### 5.8 CI

The kernel CI runs `cargo test`, `cargo fmt`, `cargo clippy`,
`cargo-about`. No CI changes needed through PR K. PR L adds a C
test step (`cc tests/c/*.c -lkernel && ./test`) and removes the
ADBC RAT-style header check on legacy paths.

## 6. Decisions on questions raised during planning

These were "open questions" in earlier drafts. Captured here as
decisions so future PRs don't relitigate.

- **`ResultStream::next_batch` shape.** Borrowed in spec
  (`Result<Option<&RecordBatch>>`); owned-iterator adapter
  (`into_owned_batches`) in PR F. Rationale in PR E.
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
- **OAuth U2M and the C ABI.** PR J/K wires PAT + M2M through the
  C ABI. U2M is Rust-crate-only in v0 (PyO3 / napi-rs consumers can
  do U2M directly; ODBC and Go consumers use PAT or M2M). The
  `External(Box<dyn TokenProvider>)` `AuthConfig` variant is the
  escape hatch for callers that want to drive their own browser
  flow and inject tokens. Revisit in v0.1 if the ODBC team needs
  native U2M.
- **Wrapper-repo cutover sequencing.** Cut once. The wrapper repos
  pin to kernel `0.2.0` after PR L lands; intermediate kernel
  versions are not published. Keeping `legacy-adbc` published as a
  feature for an extra release would force every internal change
  to support both surfaces — not worth it. The POC repos already
  prove the kernel internals work; the wrapper rewrites are
  mechanical once the v0 surface lands.

## 7. Estimated scope

| PR | LoC est | Risk    | Notes                                        |
|----|--------:|---------|----------------------------------------------|
| A  | ~600    | Low     | Mechanical: type alias swap + boundary.      |
| B  | ~200    | Low     | Rename. Mechanical.                          |
| C  | ~800    | Medium  | First real public-API body.                  |
| D  | ~700    | Medium  | New SQL builders + reshape.                  |
| E  | ~900    | High    | Core query path + Statement state machine.   |
| F  | ~600    | Medium  | Per-type accessors; lots of cases, low risk. |
| G  | ~400    | Medium  | Mostly mirrors PR E.                         |
| H  | ~300    | High    | Concurrency edge cases.                      |
| I  | ~500    | Low     | Direct conversion; well-bounded.             |
| J  | ~700    | High    | C ABI; runtime mgmt; opaque handles.         |
| K  | ~1500   | High    | Bulk of the C ABI.                           |
| L  | -2500   | Low     | Pure deletion + rename.                      |

Total new code: ~7,200 LoC; deletions in L bring the net to
~4,700. Roughly two months at one PR per few days.
