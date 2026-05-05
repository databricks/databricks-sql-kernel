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

# Databricks SQL Kernel — v0 Spec

> Snapshot of the [Common Spec for SQL Driver Kernel](https://docs.google.com/document/d/1Wmev0fFMtQYC0OxELMVuS6hmZBvvxCoSfJDMOE5vxgY)
> as of 2026-05-04. The Google Doc is the source of truth; this file is a
> mirror so the kernel-v0 refactor can land discrete PRs against a stable
> reference. Update this file when the spec changes upstream. Image-only
> figures (`![][image1]`, `![][image2]`) are referenced in the source doc
> and not reproduced here.

# Common Spec for SQL Driver Kernel

# Background

As we started OSS ODBC driver development, we took the Kernel-first approach in Rust to avoid building the same driver core for different user facing drivers. We started using ADBC Spec as the Kernel interface. As now we expand the scope of Kernel to support existing OSS Drivers like PySQL, Go Driver and NodeJS driver, we are redesigning the Kernel with an independent Databricks-first spec, completely decoupled from ADBC, making better use of Databricks constructs across different drivers.

More details on this re-alignment:  
[Heilmeier: Rust Kernel Strategy for OSS SQL Drivers](https://docs.google.com/document/d/1ZcsH0RP1fnmXEw3SeOi9e8Yi2PtOhYaBLr4jwwswifY/edit?tab=t.0#heading=h.63gxvivr4xae)

# Requirements 

We want to make sure the new Kernel spec supports following:

1. A lean core spec, which powers all the core functionality of existing drivers  
2. Easily extensible to new features, without cluttering the spec  
3. Should not be a simple wrapper over SEA (or Thrift) interface, but should abstract core driver implementation in the Kernel, e.g. Cloud fetch, inline pagination, Volume ingestion, MST etc  
4. Should be stateful, as drivers are usually stateful, should abstract out common states in the Kernel  
5. Support Databricks specific constructs natively, e.g. SessionConf, StatementConf, also provide a common interface for configuring different options, e.g. Connectivity, Auth, Proxy, Logging, TLS etc

# Spec v0 — what to include

## Design principles

1. **Shape matches what the wrappers expose to end-users**, not what ADBC traits expose. Each operation a wrapper-user calls maps to one kernel call.  
2. **Async-native.** Every operation returns `impl Future`. Sync wrappers (ODBC, Python sync API, Go) use a `block_on` adapter at the FFI edge; async wrappers (Node.js, future Python async API) await directly.  
3. **Map-shaped session configuration is first-class.** A typed `HashMap<String, String>` primitive on `Session`, not a flat `set_option(k, v)` bag.  
4. **Keep the implementation from current Rust driver.** Reuse the existing `client/`, `auth/`, `reader/`, `metadata/` modules unchanged; only the public Rust API and FFI shape change.  
5. **Pluggable transport (kernel-internal).** Wrap the SEA REST client behind a `pub(crate) trait Transport` so future protocol migrations (SEA gRPC, Thrift HiveServer2) don't churn the polling, auth, or CloudFetch layers. The trait is internal — wrappers select transport via `SessionConfig.transport: Option<TransportMode>`, not by providing their own implementation.  
6. **Easy to extend:** While we design for existing driver features, it should also be easily extensible to support future features. In fact **all future features should be implemented once in Kernel**, and other drivers should get it from here.

## Module shape

```
databricks-kernel/
  src/
    lib.rs                  # public API surface
    session.rs              # Session = Database + Connection collapsed
    statement.rs            # Statement primitives
    result.rs               # Result stream (Arrow batches)
    metadata.rs             # Metadata operations (flat per-method)
    config.rs               # SessionConfig + StatementConfig types
    error.rs                # KernelError + status code enum
    ffi/                    # Custom C ABI (not ADBC)
      mod.rs
      session.rs
      statement.rs
      result.rs
      metadata.rs
      error.rs
    # Reused from existing kernel — unchanged
    auth/
    client/
      mod.rs
      transport.rs          # pub(crate) trait Transport — internal only
      sea_rest.rs           # impl Transport for SeaRestTransport (v0)
      # Future: sea_grpc.rs, thrift.rs
      http.rs               # HTTP plumbing shared across transports
      retry.rs
    reader/
    telemetry/
    types/
```

## Architecture

Wrappers consume two distinct entry surfaces depending on language. The kernel itself is one async-core implementation; the FFI layer is what differs.

![][image1]

- **Two FFI paths** — C ABI for ODBC and Go (which can't link Rust crates cleanly); direct Rust-crate dependency for Python and Node.js (PyO3 / napi-rs auto-generate the Rust↔language bridge).  
- **One kernel core** — both FFI paths land on the same `Session` / `Statement` / `Metadata` API.  
- **Result construction is shared** — every public surface that returns rows (statement execution, statement handle, metadata calls) produces a `ResultStream`.  
- **`Transport` is internal** — `pub(crate)` trait. Wrappers never see it; they pick a transport via `SessionConfig.transport: Option<TransportMode>`.

# Public Rust API (v0)

The minimum set that lets any wrapper open a session, execute a parameterized query, fetch results row-by-row or batch-by-batch, and run the metadata calls.

## Session

A session maps to a connection in a SQL driver. It is an entry-point to the driver and creating a session would be the first call to Databricks server, and it initiates a stateful authenticated session to the server. All of the driver operations including SQL execution, metadata, Telemetry logging are linked to a session.

```rust
// === Session ===
// Replaces ADBC Database + Connection — they collapse on Databricks.

pub struct Session { /* ... */ }

impl Session {
    /// Primary entry point. Returns a builder; chain config-group setters
    /// and call .open() to construct the session.
    pub fn builder() -> SessionBuilder;

    /// Lower-level entry point. Used by callers that already have a fully-
    /// formed `SessionConfig` (e.g. deserialized from JSON, cached from a
    /// prior session, or built by the C ABI from a struct).
    pub async fn open(config: SessionConfig) -> Result<Session>;

    /// Build a statement bound to this session.
    pub fn statement(&self) -> Statement<'_>;

    /// Metadata operations — flat, per-method (ODBC-shaped).
    pub fn metadata(&self) -> Metadata<'_>;

    /// Multi-statement transactions. Server honors begin/commit/rollback
    /// end-to-end (JDBC ships this today). Mode is session-scoped — when
    /// autocommit is off, statements execute within an implicit transaction
    /// until the next commit() or rollback(). Maps to SEA `SET AUTOCOMMIT =
    /// FALSE` / `COMMIT` / `ROLLBACK` on the wire.
    ///
    /// Default: autocommit enabled (every statement auto-commits).
    pub async fn set_autocommit(&self, enabled: bool) -> Result<()>;
    pub async fn autocommit(&self) -> Result<bool>;

    /// Commit the active transaction. Returns Err(InvalidState) if
    /// autocommit is on (no active transaction).
    pub async fn commit(&self) -> Result<()>;

    /// Roll back the active transaction. Returns Err(InvalidState) if
    /// autocommit is on.
    pub async fn rollback(&self) -> Result<()>;

    /// Explicit close. Drop also fires-and-forgets cleanup.
    pub async fn close(self) -> Result<()>;
}

/// Fluent builder for Session. Each setter takes a typed config group, not
/// individual fields — keeps the builder surface compact and matches the
/// way wrappers translate connection-string / DSN attributes into config
/// (each ODBC DSN attribute group becomes one setter call).
#[non_exhaustive]
pub struct SessionBuilder { /* fields default to None / empty */ }

impl SessionBuilder {
    /// Connection identity — host + warehouse_id. Required.
    pub fn connection(self, conn: ConnectionConfig) -> Self;

    /// Authentication. Required.
    pub fn auth(self, auth: AuthConfig) -> Self;

    /// Default catalog and schema for the session. Optional.
    pub fn defaults(self, defaults: DefaultOpts) -> Self;

    /// HTTP client config (timeouts, retries, connection pool).
    pub fn http(self, http: HttpConfig) -> Self;

    /// HTTP proxy config. Omit for no proxy.
    pub fn proxy(self, proxy: ProxyConfig) -> Self;

    /// TLS config (cert validation, custom CA, allow self-signed, etc.).
    pub fn tls(self, tls: TlsConfig) -> Self;

    /// CloudFetch tuning (prefetch window, parallelism, merge target).
    pub fn cloudfetch(self, cf: CloudFetchConfig) -> Self;

    /// Logging (level, file destination).
    pub fn logging(self, logging: LoggingConfig) -> Self;

    /// Telemetry export.
    pub fn telemetry(self, telemetry: TelemetryConfig) -> Self;

    /// Server-bound session configuration map.
    /// Maps to SEA `session_confs` / Thrift `configuration`. Caller-controlled
    /// string-keyed map; kernel passes it through unmodified.
    pub fn session_conf(self, conf: HashMap<String, String>) -> Self;

    /// Transport override. Default = SeaRest. Wrappers translate user-facing
    /// flags (UseThriftClient=0/1, transport=...) into this field.
    pub fn transport(self, mode: TransportMode) -> Self;

    /// Validate, assemble a SessionConfig, and open the session.
    /// Returns Err(InvalidArgument) if required fields (connection, auth) are
    /// missing.
    pub async fn open(self) -> Result<Session>;
}

/// Required: identifies the workspace and warehouse to connect to.
pub struct ConnectionConfig {
    pub host: String,
    /// Warehouse ID. Also accepts an `http_path` string like
    /// `/sql/1.0/warehouses/abc123`; the kernel parses out the ID.
    pub warehouse_id: String,
}

/// Optional: per-session defaults applied to statements that don't override.
#[derive(Default)]
pub struct DefaultOpts {
    pub catalog: Option<String>,
    pub schema: Option<String>,
}

pub struct SessionConfig {
    /// Connection identity (matches the existing `databricks.http_path` /
    /// warehouse_id / catalog / schema option group).
    pub connection_config: ConnectionConfig,
    /// Default schema/catalog
    pub default_opts: Option<DefaultOpts>,

    pub auth: AuthConfig,                        // see below

    /// Server-bound session configuration map (SEA `session_confs` /
    /// Thrift `configuration`). Caller-controlled string-keyed map; the
    /// kernel passes it through unmodified.
    pub session_conf: HashMap<String, String>,

    /// Client-side configuration. Each is a typed sub-struct, not a string bag.
    pub http: HttpConfig,                        // timeouts, retries
    pub proxy: Option<ProxyConfig>,              // url, username, password, bypass_hosts
    pub tls: TlsConfig,                          // enabled, allow_self_signed, allow_hostname_mismatch, trusted_cert_path
    pub cloudfetch: CloudFetchConfig,            // prefetch, parallelism, merge target
    pub logging: LoggingConfig,                  // level, file
    pub telemetry: TelemetryConfig,

    /// Transport override. None = kernel picks the default (SEA REST today).
    /// Wrappers translate a user-facing flag (e.g. JDBC's `UseThriftClient`,
    /// Python's `transport=...` kwarg) into this field. The kernel owns the
    /// `Transport` trait and its implementations internally; this enum is
    /// the wrapper's only knob.
    pub transport: Option<TransportMode>,
}

/// Wire-protocol transport override. v0 supports `SeaRest` only. The enum is
/// `#[non_exhaustive]` so v1+ can add SeaGrpc / Thrift without breaking
/// wrappers. The actual `Transport` trait that implements each variant is
/// `pub(crate)` — not exposed across the kernel boundary.
#[non_exhaustive]
pub enum TransportMode {
    /// Statement Execution API over REST. Default.
    SeaRest,
    // Future: SeaGrpc, Thrift
}

pub enum AuthConfig {
    Pat { token: String },
    OAuthM2M { client_id: String, client_secret: String, /* ... */ },
    OAuthU2M { /* browser-flow params */ },
    OAuthFederation { /* RFC 8693 */ },
    External(Box<dyn TokenProvider>),
}
```

### Config hierarchy and where each piece is used

Config is grouped by concern, not flattened into a string bag. Wrappers fluently call `SessionBuilder` setters — one per group — and finalize with `.open()`. The builder assembles a `SessionConfig` internally; the C ABI accepts that struct directly.

![][image2]

## Statement

```rust

// === Statement ===

pub struct Statement<'s> { /* ... */ }

impl<'s> Statement<'s> {
    pub fn sql(self, sql: impl Into<String>) -> Self;

    /// Positional parameter (1-based).
    pub fn param(self, ordinal: usize, value: TypedValue) -> Self;
    /// Named parameter.
    pub fn param_named(self, name: impl Into<String>, value: TypedValue) -> Self;

    /// Per-statement conf overlay (SEA `parameters` / Thrift `confOverlay`).
    /// Same map as session-level but scoped to one statement.
    pub fn statement_conf(self, conf: HashMap<String, String>) -> Self;

    /// Result format. Default is `ResultFormat::Arrow`. v0 only supports
    /// Arrow; passing other variants returns `ErrorCode::InvalidArgument`
    /// at execute time. Wrappers can wire this to a user-facing knob today
    /// — additional variants in v1+ unblock the same wiring without an FFI
    /// shape change.
    pub fn result_format(self, fmt: ResultFormat) -> Self;

    /// Wait-for-result execution. Sends ExecuteStatement with server-side
    /// `wait_timeout=10s` to enable the **direct-results optimization** —
    /// if the server finishes within 10s, results come back inline in the
    /// initial response (no separate fetch RPC). The kernel polls internally
    /// if the server doesn't finish in 10s. Returns when results are ready.
    ///
    /// Used by sync-execute wrappers (Python sync `cursor.execute`,
    /// JDBC `Statement.execute`, ODBC `SQLExecute`, Go `db.QueryContext`)
    /// and by Node.js's default Promise-returning `executeStatement()`.
    pub async fn execute(self) -> Result<ResultStream>;

    // Submit-and-return. wait_timeout=0s; no direct results. After this,                                                                                                                                      
    // status/await_result/fetch_result/cancel are valid until close.
    pub async fn submit(&self) -> Result<()>;
    
    // Post-submit only.
    pub async fn status(&self) -> Result<StatementStatus>;                                                                                                                                                     
    pub async fn await_result(&self) -> Result<ResultStream>;                                                                                                                                                  
    pub async fn fetch_result(&self) -> Result<ResultStream>;
                                                                                                                                                                                                                 
    // Valid at any time.                                            
    pub async fn cancel(&self) -> Result<()>;                                                                                                                                                                  
    pub async fn close(self) -> Result<()>;
}

impl<'s> Drop for Statement<'s> {
   fn drop(&mut self) {                                                                                                                                                                                       
      // Fire-and-forget CloseStatement if a statement_id is still active
      // and close() wasn't called explicitly.                                                                                                                                                               
   }                                                                                                                                                                                                          
}

pub enum StatementStatus { Pending, Running, Succeeded, Failed(Error), Cancelled, Closed }




// === Errors ===

pub struct Error {
    pub code: ErrorCode,
    pub message: String,
    pub sql_state: Option<String>,           // 5-char SQLSTATE
    pub vendor_code: Option<i32>,            // Databricks vendor code
    pub http_status: Option<u16>,            // for transport-layer errors
    pub retryable: bool,
    pub query_id: Option<String>,
    pub cause: Option<Box<Error>>,
}

pub enum ErrorCode {
    InvalidArgument,
    Unauthenticated,
    PermissionDenied,
    NotFound,
    ResourceExhausted,        // 429
    Unavailable,              // 503
    Timeout,
    Cancelled,
    DataLoss,
    Internal,
    InvalidStatementHandle,   // INVALID_HANDLE_STATUS — distinct
    NetworkError,
    SqlError,                 // server-reported execution error
}
```

## ResultStream

A ResultStream is a paginated result returned by the successful execution of a statement. It abstracts out both inline as well as cloud fetch results.  It would include schema for the result, and would support fetching of next batch of results.

```rust
// === Results ===

pub struct ResultStream { /* ... */ }

impl ResultStream {
    pub fn schema(&self) -> &arrow::datatypes::Schema;

    /// Pull the next batch of result data. Drives polling, CloudFetch,
    /// decompression internally. Returns None when stream is exhausted.
    /// In v0 the variant is always `ResultBatch::Arrow`; the enum is
    /// `#[non_exhaustive]` so v1+ formats slot in without breaking wrappers.
    pub async fn next_batch(&mut self) -> Result<Option<ResultBatch>>;

    /// Row-pull adapter for ODBC, Python row-style, Go. Walks the underlying
    /// batches internally and slices one row at a time. Native types per
    /// column. Works regardless of the wire format.
    pub async fn next_row(&mut self) -> Result<Option<Row>>;

    pub async fn cancel(&mut self) -> Result<()>;
    pub async fn close(self) -> Result<()>;
}

// Internal: ResultStream is implemented over a ResultReader trait.
// Wrappers don't see this; it's the polymorphism point that lets one
// public type serve both inline and CloudFetch result delivery.
pub(crate) trait ResultReader: Send {
    fn schema(&self) -> Result<SchemaRef>;
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Format-neutral envelope for one batch of result data. v0 only ever yields
/// `Arrow`. The enum is `#[non_exhaustive]` so wrappers must handle the
/// fallback arm; new variants in v1+ are non-breaking.
#[non_exhaustive]
pub enum ResultBatch {
    Arrow(arrow::record_batch::RecordBatch),
    // Future: ColumnarThrift(...), JsonRows(...)
}

impl ResultBatch {
    /// Convenience for wrappers that only consume Arrow today. Returns
    /// `Err(InvalidArgument)` for any future non-Arrow variant — explicit
    /// failure point lets wrappers find their migration sites when v1 lands.
    pub fn into_arrow(self) -> Result<arrow::record_batch::RecordBatch>;
}

// Implementations of the ResultReader trait (kernel-internal, not exposed):
//   - InlineArrowReader      — inline Arrow attachment in the SEA response.
//                              Used when result.inline_arrow_data is present.
//   - CloudFetchResultReader — chunk download via external presigned URLs.
//                              Used when result.external_links is present.
//   - EmptyReader            — for queries with no rows (terminal state with
//                              no result data); preserves schema from manifest.
```

## Metadata

The Kernel would have explicit spec for metadata operations. Most drivers except for Go have dedicated support for metadata operations. Having explicit metadata spec will help in avoiding redundant metadata code in individual drivers.

```rust
// === Metadata ===

pub struct Metadata<'s> { /* ... */ }

impl<'s> Metadata<'s> {
    pub async fn list_catalogs(&self) -> Result<ResultStream>;
    pub async fn list_schemas(&self, catalog: Option<&str>, schema_pattern: Option<&str>) -> Result<ResultStream>;
    pub async fn list_tables(&self, catalog: Option<&str>, schema: Option<&str>, table_pattern: Option<&str>, table_types: Option<&[&str]>) -> Result<ResultStream>;
    pub async fn list_columns(&self, catalog: Option<&str>, schema: Option<&str>, table: Option<&str>, column_pattern: Option<&str>) -> Result<ResultStream>;
    pub async fn primary_keys(&self, catalog: Option<&str>, schema: Option<&str>, table: &str) -> Result<ResultStream>;
    pub async fn foreign_keys(&self, /* parent + foreign sides */ ) -> Result<ResultStream>;
    pub async fn functions(&self, catalog: Option<&str>, schema_pattern: Option<&str>, name_pattern: Option<&str>) -> Result<ResultStream>;
    pub async fn procedures(&self, catalog: Option<&str>, schema_pattern: Option<&str>, name_pattern: Option<&str>) -> Result<ResultStream>;
    pub async fn type_info(&self) -> Result<ResultStream>;
    pub async fn table_types(&self) -> Result<ResultStream>;
}
```

## Error Handling

---

# Kernel addenda

This section captures decisions that have been agreed in the kernel
implementation but are not yet reflected in the upstream Google Doc. When
upstream is updated, these notes fold back into the relevant section above
and the addendum is removed.

## A1. StatementSpec, Statement, ExecutedStatement, ExecutedAsyncStatement

**Status:** approved 2026-05-05.

Databricks server-side, a statement is immutable once created — it has a
`statement_id` and a single SQL text bound to it. The spec body's single
`Statement<'s>` type collapses three concerns: the *what* (SQL, params,
conf), the *lifecycle owner* (mutable handle that can be re-executed), and
the *executed handle* (immutable, has `statement_id`). This addendum
splits them into four types so the kernel mirrors the server contract and
the executed phase carves cleanly into sync vs async consumption.

Four types:

- `StatementSpec` — plain data. SQL, positional/named params, statement
  conf overlay, result format. No `Session` reference. Constructible in
  isolation; testable; cheaply cloneable.
- `Statement` — lifecycle owner, returned by `Session::statement()`. Owns
  one `StatementSpec` and at most one live executed handle. Exposes the
  spec via `spec(&mut self) -> &mut StatementSpec` for mutation, and
  `execute()` / `submit()` to produce executed handles.
- `ExecutedStatement` — immutable, returned by `Statement::execute()`. The
  kernel has already polled to completion; results are ready to consume.
- `ExecutedAsyncStatement` — immutable, returned by `Statement::submit()`.
  The kernel returned without driving polling; the caller owns polling via
  `status()` / `await_result()`.

`Statement` is reusable. Calling `execute()` or `submit()` again on the
same `Statement` auto-cancels + closes any prior executed handle it
produced. The spec persists; only the executed handle is replaced. The
prior handle becomes invalid; subsequent calls on it return
`ErrorCode::InvalidStatementHandle`.

```rust
// === StatementSpec (plain data, mutable, reusable) ===

#[derive(Default, Clone)]
pub struct StatementSpec {
    pub sql: Option<String>,
    pub positional_params: Vec<TypedValue>,
    pub named_params: HashMap<String, TypedValue>,
    pub statement_conf: HashMap<String, String>,
    pub result_format: ResultFormat,    // Default: Arrow
}

impl StatementSpec {
    pub fn new() -> Self;

    // Fluent setters for ergonomics. All take &mut self.
    pub fn sql(&mut self, sql: impl Into<String>) -> &mut Self;
    pub fn param(&mut self, ordinal: usize, value: TypedValue) -> &mut Self;
    pub fn param_named(&mut self, name: impl Into<String>, value: TypedValue) -> &mut Self;
    pub fn statement_conf(&mut self, conf: HashMap<String, String>) -> &mut Self;
    pub fn result_format(&mut self, fmt: ResultFormat) -> &mut Self;
}


// === Statement (lifecycle owner, mutable, reusable) ===

pub struct Statement<'s> { /* spec: StatementSpec, last_handle, session ref */ }

impl<'s> Statement<'s> {
    /// Mutable access to the spec. Mutate before calling execute/submit.
    pub fn spec(&mut self) -> &mut StatementSpec;

    /// Read-only access (e.g. for logging or assertion).
    pub fn spec_ref(&self) -> &StatementSpec;

    /// Wait-for-result execution. Auto-cancels + closes any prior handle.
    /// Spec is unchanged after the call; can be re-executed.
    pub async fn execute(&mut self) -> Result<ExecutedStatement<'s>>;

    /// Submit-and-return. Auto-cancels + closes any prior handle.
    pub async fn submit(&mut self) -> Result<ExecutedAsyncStatement<'s>>;
}


// === Shared trait for both executed handles ===

pub trait ExecutedStatementHandle {
    fn statement_id(&self) -> &str;
    async fn cancel(&self) -> Result<()>;
    async fn close(self) -> Result<()>;
}


// === ExecutedStatement (sync consumption) ===

pub struct ExecutedStatement<'s> { /* statement_id, ResultStream */ }

impl<'s> ExecutedStatement<'s> {
    pub fn statement_id(&self) -> &str;

    /// Borrow the result stream. Non-consuming — JDBC/ODBC parity, the
    /// caller separates "done iterating" from "done with statement".
    pub fn result_stream(&self) -> &ResultStream;
    pub fn result_stream_mut(&mut self) -> &mut ResultStream;

    pub async fn cancel(&self) -> Result<()>;
    pub async fn close(self) -> Result<()>;
}


// === ExecutedAsyncStatement (async consumption) ===

pub struct ExecutedAsyncStatement<'s> { /* statement_id */ }

impl<'s> ExecutedAsyncStatement<'s> {
    pub fn statement_id(&self) -> &str;

    pub async fn status(&self) -> Result<StatementStatus>;
    pub async fn await_result(&self) -> Result<ResultStream>;
    pub async fn fetch_result(&self) -> Result<ResultStream>;

    pub async fn cancel(&self) -> Result<()>;
    pub async fn close(self) -> Result<()>;
}

// Both ExecutedStatement and ExecutedAsyncStatement fire fire-and-forget
// CloseStatement on Drop if close() wasn't called explicitly.
```

**Typical flow:**

```rust
let mut stmt = session.statement();
stmt.spec()
    .sql("SELECT * FROM t WHERE id = ?")
    .param(1, TypedValue::Int(42));

let exec = stmt.execute().await?;
while let Some(batch) = exec.result_stream_mut().next_batch().await? { ... }
exec.close().await?;

// Re-execute with different params — spec persists, only param changes.
stmt.spec().param(1, TypedValue::Int(43));
let exec = stmt.execute().await?;
// ...
```

**Re-execute semantics.** When `execute()` or `submit()` is called on a
`Statement` that has previously produced an executed handle:

1. If the prior handle's server-side status is `Pending` or `Running`,
   send CancelStatement.
2. Send CloseStatement to release server resources.
3. Send the new ExecuteStatement (or submit) using the current
   `StatementSpec`.

The prior handle, if still held by the caller, returns
`ErrorCode::InvalidStatementHandle` on subsequent calls.

**`Session::statement()`** continues to return `Statement<'_>`; the
docstring is updated to clarify it is the mutable lifecycle owner whose
spec the user mutates before executing.

