

# Kernel Telemetry Design

## Overview

This document proposes the v0 Databricks SQL Kernel telemetry design. Telemetry
becomes a foundational kernel subsystem owned by `SessionInner` and used by
`Session`, `Statement`, `ExecutedStatement`, `ExecutedAsyncStatement`,
`ExecutedStatementHandle`, `Metadata`, and `ResultStream`.

The kernel should implement telemetry once, below all language bindings, and
export through the existing Databricks SQL driver telemetry API shape used by
JDBC and the ADBC implementation. Binding crates and the C ABI configure
telemetry through `TelemetryConfig`; they should not duplicate metric
collection, batching, circuit breaking, or export logic.

## Inputs Reviewed

- `docs/spec-v0.md`, the local mirror of the common kernel design.
- `src/telemetry/mod.rs`, currently a minimal reserved collector.
- `src/config.rs`, where public `TelemetryConfig` is currently empty and
exported at the crate root.
- `src/session.rs`, `src/statement/*`, `src/metadata_v0.rs`, and
`src/result_stream.rs`, which define the session-owned handle graph.
- ADBC implementation under `~/databricks`, especially:
  - `csharp/doc/telemetry-design.md`
  - `docs/designs/PECO-3022-sea-telemetry-integration-design.md`
  - `docs/designs/fix-telemetry-gaps-design.md`
  - `csharp/src/Telemetry/ConnectionTelemetry.cs`
  - `csharp/src/Telemetry/IConnectionTelemetry.cs`
  - `csharp/src/Telemetry/StatementTelemetryContext.cs`
  - `csharp/src/Telemetry/TelemetrySessionContext.cs`
  - `csharp/src/Telemetry/TelemetryClient.cs`
  - `csharp/src/Telemetry/TelemetryClientManager.cs`
  - `csharp/src/Telemetry/DatabricksTelemetryExporter.cs`
  - `csharp/src/Telemetry/CircuitBreakerTelemetryExporter.cs`
  - `csharp/src/FeatureFlagCache.cs`
  - `csharp/src/FeatureFlagContext.cs`
  - `csharp/src/Telemetry/Proto/sql_driver_telemetry.proto`
  - telemetry unit/E2E tests under `csharp/test/*/Telemetry`.
- JDBC implementation under `~/databricks-jdbc` as a secondary reference for
production behavior that ADBC intentionally mirrors: endpoint shape,
feature-flag lineage, batching, circuit breaking, and close-time flush.

The two Google Docs links in the request were not directly accessible from the
shell export path during this pass. The kernel spec is already mirrored in this
repo. The local `~/databricks` checkout is the primary implementation/design
reference for this plan.

## Open Question Answers

### Expected Kernel Telemetry CUJ

The primary customer user journey is:

1. A wrapper opens a kernel `Session` with telemetry configuration.
2. The kernel creates a session telemetry context and a host-scoped telemetry
  client.
3. The user executes statements, polls async statements, fetches result
  streams, calls metadata methods, cancels/closes statement handles, and closes
   the session.
4. The kernel emits telemetry events for terminal operations without exposing
  telemetry mechanics to wrapper users.
5. Telemetry export is batched, non-blocking on hot paths, flushed on close, and
  must never change driver correctness.

The expected v0 event model is "one operation event per terminal kernel
operation", with statement execution as the anchor event:


| CUJ step               | Kernel surface                                                | Telemetry event                                                                                                             |
| ---------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Open connection        | `Session::open`                                               | Session create/open latency, config snapshot, auth type, transport mode (`SeaRest` in v0)                                   |
| Execute sync statement | `Statement::execute` -> `ExecutedStatement` -> `ResultStream` | One statement event including execute latency, poll latency, result format, row/batch/chunk stats, final status/error       |
| Submit async statement | `Statement::submit`                                           | Submit event, followed by terminal event when `await_result`, `fetch_result`, `cancel`, or `close` reaches a terminal state |
| Poll async statement   | `ExecutedAsyncStatement::status` / `await_result`             | Poll count and total/slowest poll latency attached to the statement context                                                 |
| Fetch result           | `ResultStream::next_batch` / row cursor                       | Result-ready latency, consumption latency, row count, batch count, CloudFetch chunk metrics                                 |
| Metadata calls         | `Metadata::{list_catalogs, list_schemas, ...}`                | Metadata operation event with method-specific operation type and result stats                                               |
| Cancel/close handle    | `ExecutedStatementHandle::{cancel, close}`                    | Close/cancel operation latency and terminal disposition                                                                     |
| Close session          | `Session::close` / `Drop`                                     | Best-effort close/delete latency plus telemetry flush-before-close                                                          |


This CUJ is kernel-first: language bindings should only translate user-facing
configuration into `TelemetryConfig` and surface any test hooks needed for
binding-level tests.

### Do ADBC And JDBC Adhere To The Telemetry Design?

ADBC is the better implementation/design to follow for kernel telemetry.

The current ADBC implementation matches the shape the kernel needs:

- `ConnectionTelemetry` owns connection/session-level telemetry and returns a
no-op implementation when telemetry is disabled or initialization fails.
- `TelemetrySessionContext` is created once per connection/session and shared
with statement contexts.
- `StatementTelemetryContext` is populated directly by lifecycle code and builds
the `OssSqlDriverTelemetryLog` proto. It captures statement id, statement
type, operation type, result format, compression, retry count, internal-call
flag, poll stats, first-batch/result-consumption latency, CloudFetch chunk
metrics, and sanitized error info.
- `IConnectionTelemetry.ExecuteWithMetadataTelemetry` wraps metadata calls with
the same statement context shape instead of treating metadata as a wrapper
concern.
- `EmitOperationTelemetry` records discrete operations such as create session,
delete session, close statement, and cancel statement.
- `TelemetryClientManager` shares one telemetry client per host with reference
counting.
- `TelemetryClient` queues events, flushes on batch size or timer, flushes on
close, coordinates concurrent flushes, and swallows telemetry failures.
- `DatabricksTelemetryExporter` serializes `TelemetryRequest { upload_time, proto_logs }` and posts to `/telemetry-ext` or `/telemetry-unauth` with retry.
- `CircuitBreakerTelemetryExporter` protects the telemetry endpoint.
- `FeatureFlagCache` and `FeatureFlagContext` cache all feature flags per host,
use server TTL, support negative caching on fetch failure, and merge remote
flags below local user properties.
- The SEA telemetry design introduces a small observer boundary so statement
classes call lifecycle methods such as `OnExecuteStarted`,
`OnStatementSubmitted`, `OnPollCompleted`, `OnFirstBatchReady`,
`OnConsumed`, `OnChunksDownloaded`, `OnError`, and `OnFinalized` without
depending directly on telemetry proto types.

JDBC broadly adheres to the same backend contract, but should be a secondary
reference. It is useful for validating production semantics, but less suitable
as the kernel model because it relies on a per-connection collector plus some
thread-local attribution paths. The Rust kernel already has explicit
`SessionInner` and handle ownership, so it should follow ADBC's direct
session/context/observer shape and avoid thread-local telemetry state.

Scoping notes for kernel telemetry:


| Area                      | ADBC reference                                                            | Kernel implication                                                                                  |
| ------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Connection abstraction    | `IConnectionTelemetry` with real/no-op implementations                    | Add a `TelemetryRuntime` / no-op runtime on `SessionInner`                                          |
| Statement instrumentation | Observer lifecycle interface over direct `StatementTelemetryContext`      | Add a Rust `StatementOperationObserver` trait so statement code is not coupled to export/proto code |
| Metadata operations       | `ExecuteWithMetadataTelemetry` wraps metadata operations                  | Metadata should create operation contexts in the kernel, not in bindings                            |
| Discrete operations       | `EmitOperationTelemetry` for create/delete/close/cancel                   | Session and handle lifecycle events should use the same event builder                               |
| Result consumption        | Observer/decorator records first batch, consumed, chunk metrics           | `ResultStream` should hold or call an observer context                                              |
| Feature flags             | Per-host cache, local properties win, negative TTL on failure             | Implement host-scoped async cache; never block hot paths once cold-start policy is decided          |
| Export                    | `TelemetryClientManager` + `TelemetryClient` + exporter + circuit breaker | Recreate this pipeline in Rust; do not invent wrapper-specific exporters                            |
| Proto alignment           | Tests assert proto fields and JSON naming                                 | Add Rust serialization/proto alignment tests before enabling export                                 |


## Goals

1. Centralize telemetry in `src/telemetry` so it is a foundational kernel
  subsystem, not a wrapper feature.
2. Wire telemetry through the existing public `TelemetryConfig` API in
  `src/config.rs` and the `SessionBuilder::telemetry(...)` setter.
3. Scope v0 telemetry to the kernel's only current transport: SEA REST
  (`TransportMode::SeaRest`). Keep transport enums non-exhaustive only for
   future protocol additions.
4. Preserve the existing v0 handle model: `SessionInner` shared by `Arc`, public
  handles remain `Send + Sync`, and async public futures remain `Send`.
5. Keep telemetry failure-isolated: collection/export errors are swallowed after
  trace/debug logging and never affect query, metadata, result, close, or drop
   behavior.
6. Avoid PII and query text. Record operation class, result format, status,
  error class, latencies, counts, config booleans, and environment metadata.
7. Make the implementation testable without live telemetry service access.

## Non-Goals

- Do not implement a general OpenTelemetry metrics exporter in v0.
- Do not send SQL text, parameter values, result values, tokens, usernames,
paths containing local secrets, or arbitrary connection strings.
- Do not require wrappers to call telemetry APIs around every kernel call.
- Do not add blocking telemetry work to `ResultStream::next_batch`,
`Statement::execute`, metadata methods, or HTTP retry loops.
- Do not make `Drop` await telemetry export. `Drop` is best effort only.

## Proposed Architecture

```
SessionConfig.telemetry
      |
      v
Session::open
      |
      +--> TelemetryManager::client_for_host(host)
      |        |
      |        +--> TelemetryClient (host-scoped queue, batch flush timer)
      |        +--> TelemetryExporter (HTTP POST)
      |        +--> CircuitBreaker
      |
      +--> TelemetrySessionContext
      |
      v
SessionInner { telemetry: Arc<TelemetryRuntime> }
      |
      +--> Statement / Metadata obtain StatementOperationObserver
      |
      +--> TelemetryObserver owns StatementTelemetryContext
      |
      +--> ExecutedStatement / ExecutedAsyncStatement / ResultStream call observer lifecycle hooks
      |
      v
TelemetryClient.enqueue(TelemetryFrontendLog)
```

### Core Types

Add or replace `src/telemetry/mod.rs` with these components:

```rust
pub struct TelemetryConfig {
    pub enabled: bool,
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub circuit_breaker_enabled: bool,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_timeout: Duration,
}
```

The actual public struct lives in `src/config.rs` today, so the implementation
should either move the richer type there or define the detailed types in
`src/telemetry` and re-export them from `config`.

```rust
pub(crate) struct TelemetryRuntime {
    enabled: bool,
    session: TelemetrySessionContext,
    sink: Arc<dyn TelemetrySink>,
}

pub(crate) struct TelemetrySessionContext {
    session_id: String,
    host_hash_or_workspace: Option<String>,
    driver_name: &'static str,
    driver_version: &'static str,
    os_name: String,
    os_arch: String,
    runtime_name: String,
    auth_type: TelemetryAuthType,
    transport_mode: TelemetryTransportMode, // SeaRest in v0
    connection_params: DriverConnectionParameters,
}

pub(crate) struct StatementTelemetryContext {
    session: Arc<TelemetrySessionContext>,
    statement_id: OnceLock<String>,
    operation_type: OperationType,
    statement_type: StatementType,
    result_format: AtomicResultFormat,
    started_at: Instant,
    execute_latency_ms: AtomicU64,
    poll_stats: LatencyStats,
    result_stats: ResultStats,
    error: Mutex<Option<DriverErrorInfo>>,
    emitted: AtomicBool,
}

pub(crate) trait ConnectionTelemetry: Send + Sync {
    fn session(&self) -> Option<Arc<TelemetrySessionContext>>;
    fn statement_observer(
        &self,
        statement_type: StatementType,
        operation_type: OperationType,
        is_internal_call: bool,
    ) -> Box<dyn StatementOperationObserver>;
    fn emit_operation(
        &self,
        operation_type: OperationType,
        statement_type: StatementType,
        statement_id: Option<&str>,
        elapsed: Duration,
        error: Option<&Error>,
    );
    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(crate) trait StatementOperationObserver: Send {
    fn on_execute_started(&mut self, statement_type: StatementType, operation_type: OperationType);
    fn on_statement_submitted(&mut self, statement_id: &str);
    fn on_poll_completed(
        &mut self,
        count: u32,
        latency: Duration,
        result_format: ExecutionResultFormat,
    );
    fn on_first_batch_ready(&mut self, latency: Duration);
    fn on_consumed(&mut self, latency: Duration);
    fn on_chunks_downloaded(&mut self, metrics: ChunkMetrics);
    fn on_error(&mut self, error: &Error);
    fn on_finalized(&mut self);
}

pub(crate) trait TelemetrySink: Send + Sync {
    fn enqueue(&self, event: TelemetryFrontendLog);
    fn flush<'a>(
        &'a self,
        mode: FlushMode,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn close_best_effort(&self);
}
```

Use a no-op sink when telemetry is disabled, configuration is invalid, feature
flag fetch fails closed, or client creation fails.

The `ConnectionTelemetry` and `StatementOperationObserver` traits mirror ADBC's
`IConnectionTelemetry` and `IStatementOperationObserver`. They keep statement,
metadata, and result-stream code speaking in lifecycle terms instead of proto
terms. The real observer mutates a `StatementTelemetryContext`; the no-op
observer makes disabled telemetry callsites branch-free.

### Ownership

Add to `SessionInner`:

```rust
telemetry: Arc<TelemetryRuntime>,
```

`Session::open` builds the runtime after `create_session` returns, because the
server-issued `session_id` belongs in every event. If session creation fails
before a session ID exists, v0 can skip no-auth failure telemetry unless the
telemetry service contract explicitly requires it. That keeps the first kernel
implementation simpler than JDBC.

`Statement` asks `SessionInner.telemetry` for a `StatementOperationObserver` for
each `execute`, `submit`, or `execute_direct` call. The observer owns the
`StatementTelemetryContext` and is moved into `ExecutedStatement` or
`ExecutedAsyncStatement`.

`ResultStream` should hold an optional observer handle:

```rust
telemetry: Option<Box<dyn StatementOperationObserver>>,
```

`ResultStream::next_batch` records batch count, row count, first-batch latency,
and exhaustion time. `OwnedBatchIter` should record the same stats by driving
the same reader through a telemetry-aware path, or the doc should explicitly
mark owned iteration as using `ResultStream` before conversion.

Metadata calls should use the connection telemetry wrapper pattern from ADBC:
create a metadata observer/context with `statement_type = Metadata` and
`operation_type = ListCatalogs`, `ListSchemas`, etc., execute the operation, and
attach the observer to the returned bare `ResultStream`. Because metadata
returns a bare `ResultStream` with no executed handle, that stream owns
finalization on exhaustion/drop/error.

### Event Emission

Use deterministic, idempotent emission. Every real observer has an emitted flag.
The first terminal path wins; later paths do nothing. This follows ADBC's
`OnFinalized` contract.

Emit statement/result telemetry when:

- `ExecutedStatement::next_batch` reaches end-of-stream and auto-closes.
- `ResultStream::next_batch` reaches `None` for metadata-owned streams.
- `ExecutedAsyncStatement::await_result` or `fetch_result` returns a terminal
stream and that stream is exhausted.
- `ExecutedStatementHandle::cancel` succeeds or returns a terminal error.
- `ExecutedStatementHandle::close` completes.
- A statement operation fails before a result stream is returned.
- `Session::close` flushes the host-scoped telemetry client after emitting
session delete telemetry.

Do not emit on every batch. Batch-level and chunk-level metrics should be
accumulated on the context and included in the terminal event.

### Export Pipeline

Match the ADBC production pattern:

- One telemetry client per host key.
- Reference count or connection UUID set so closing one session does not close
a shared client still used by other sessions.
- Queue events in memory using a `ConcurrentQueue`-equivalent structure and
track an approximate queue count, matching ADBC's `TelemetryClient`.
- Trigger flush when `batch_size` is reached, when `flush_interval` elapses,
and during telemetry client close.
- Export on a background task using a short telemetry HTTP timeout.
- Wrap export in a per-host circuit breaker.
- Swallow all telemetry errors after debug/trace logging.
- Keep enqueue non-blocking and fail-open. In v0, match ADBC and do not add an
explicit max queue size or oldest/newest drop policy.

`Session::close` should:

1. Mark the session closing so no new telemetry client is created for that
  session.
2. Export all pending statement contexts for that session.
3. Await a bounded force flush.
4. Proceed with `delete_session` behavior even if telemetry flush fails or
  times out.

`Drop` paths should call best-effort non-blocking close only.

### Feature Flags

Telemetry enablement should use the ADBC priority:

1. User-specified local configuration.
2. Server feature flags merged into configuration.
3. Driver defaults.

Local configuration wins over remote feature flags. The kernel should keep
ADBC's conservative default: telemetry is disabled unless local config or a
merged feature flag enables it.

The final feature flag endpoint and flag name are product decisions. The kernel
should follow ADBC unless product owners choose a kernel-specific endpoint:

- Endpoint format currently used by ADBC:
`/api/2.0/connector-service/feature-flags/ADBC/{driver_version}`.
- Existing server-side flag:
`databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForAdbc`.

If the kernel needs separate rollout from ADBC, introduce a kernel-specific flag
only after the backend/dashboard owner confirms the split. Do not fall back to
JDBC endpoint or flag names in kernel code.

## Telemetry Data Model

The kernel should mirror the existing SQL driver telemetry envelope:

```text
TelemetryFrontendLog
  workspace_id
  frontend_log_event_id
  context.timestamp_millis
  context.client_context.user_agent [optional]
  entry.sql_driver_log
    session_id
    sql_statement_id
    driver_system_configuration
    driver_connection_parameters
    auth_type
    sql_operation
    driver_error_info
    operation_latency_ms
```

Recommended v0 fields:


| Field group         | Include                                                                                                                                                                                       | Exclude                                                               |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| System config       | driver name/version, OS name/arch/version, Rust version if available, target TLS backend, process/app name if supplied by user-agent config                                                   | usernames, env vars, local paths                                      |
| Connection params   | transport mode (`SeaRest` in v0), auth mechanism enum, proxy enabled booleans, CloudFetch enabled, retry config class, TLS mode booleans, result format preference, metric view metadata flag | host raw URL unless approved, tokens, secrets, custom headers         |
| Statement operation | statement id, statement type, operation type, result format, compression, execute latency, status, number of affected rows when available                                                     | SQL text, params, query tags unless explicitly classified safe        |
| Result stats        | first batch ready latency, consumption latency, batch count, row count, CloudFetch chunk count, slowest/sum/first chunk latencies                                                             | row values, schema names, table names                                 |
| Error info          | kernel `ErrorCode`, SQLSTATE, server error code when present, retryability category                                                                                                           | stack traces containing SQL/paths, raw server message unless scrubbed |


Use enums with an `Unspecified` value and `#[non_exhaustive]` where public.
Serialization should preserve snake_case field names expected by the telemetry
backend and existing driver dashboards.

## Instrumentation Points


| Location                               | Record                                                                       |
| -------------------------------------- | ---------------------------------------------------------------------------- |
| `Session::open`                        | create-session latency, resolved telemetry config, session context           |
| `Session::close`                       | delete-session latency, flush outcome                                        |
| `Statement::execute`                   | execute latency, statement ID, result format, terminal state, initial error  |
| `Statement::submit`                    | submit latency, statement ID, initial state                                  |
| `Statement::execute_direct`            | direct-result completed/running split                                        |
| `ExecutedAsyncStatement::status`       | poll latency/count/status                                                    |
| `ExecutedAsyncStatement::await_result` | total wait latency, terminal status                                          |
| `ExecutedAsyncStatement::fetch_result` | result fetch latency and result format                                       |
| `ExecutedStatementHandle::cancel`      | cancel latency and outcome                                                   |
| `ExecutedStatementHandle::close`       | close latency and outcome                                                    |
| `Metadata::*`                          | metadata method operation type, latency, result stats                        |
| `ResultStream::from_reader`            | schema-ready latency if available                                            |
| `ResultStream::next_batch`             | first batch latency, batch/row counts, exhaustion                            |
| CloudFetch reader/link fetcher         | chunk count, chunk download latency, retry count, chunk errors               |
| HTTP client retry layer                | retry count and final category, attached to current operation where possible |


The first implementation can skip some low-level HTTP retry attribution if the
current operation context cannot be passed cleanly; it should still capture
statement/result lifecycle metrics.

## Public API Changes

Replace the reserved config with a real config:

```rust
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub circuit_breaker_enabled: bool,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_timeout: Duration,
}
```

Defaults:

```rust
enabled = false
batch_size = 100
flush_interval = 5 seconds
max_retries = 3
retry_delay = 100 milliseconds
circuit_breaker_enabled = true
circuit_breaker_threshold = 5
circuit_breaker_timeout = 60 seconds
```

Add test-only injection behind `#[cfg(any(test, feature = "test-utils"))]`:

```rust
pub trait TelemetryExporter: Send + Sync {
    fn export<'a>(
        &'a self,
        request: TelemetryRequest,
    ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>>;
}
```

The public crate root already re-exports `TelemetryConfig`, so bindings can wire
their current user-facing telemetry options into this struct without a new
top-level API.

## Privacy And Safety

Telemetry must use an explicit allowlist. If a field is not named in the data
model, it is not emitted.

Rules:

- Never emit SQL text or parameter values.
- Never emit result values.
- Never emit tokens, client secrets, private keys, auth headers, proxy
credentials, or custom headers.
- Do not emit raw hostnames unless the telemetry contract confirms they are
permitted. Prefer workspace/org IDs from server responses where available, or
omit.
- Scrub error messages before export, or emit only `ErrorCode`, SQLSTATE, and
server error class.
- Treat telemetry service failures, serialization failures, feature flag
failures, and circuit-breaker-open states as non-fatal.

## Rollout Plan

### Phase 1: Proto And Model Alignment

- Add Rust models for the ADBC-compatible telemetry envelope:
`TelemetryFrontendLog`, `FrontendLogContext`, `FrontendLogEntry`,
`TelemetryRequest`, and `OssSqlDriverTelemetryLog`-equivalent event structs.
- Decide whether to generate Rust from
`~/databricks/csharp/src/Telemetry/Proto/sql_driver_telemetry.proto` or keep
serde models aligned to the same field names.
- Add proto/JSON alignment tests equivalent to ADBC's
`TelemetryProtoAlignmentTests`.
- Verify snake_case backend field names and endpoint request shape:
`TelemetryRequest { upload_time, proto_logs }`.

### Phase 2: Core Connection Telemetry

- Replace `src/telemetry/mod.rs` with `ConnectionTelemetry`,
`NoOpConnectionTelemetry`, `TelemetrySessionContext`, config parsing,
event-building helpers, and test exporter injection.
- Expand `TelemetryConfig` in `src/config.rs` with ADBC-compatible defaults.
- Add `TelemetryRuntime` to `SessionInner`.
- Wire `Session::open` to create connection telemetry after `create_session`
returns, using no-op fallback on disabled telemetry, invalid config, or
initialization failure.
- Wire `Session::close` to emit delete-session telemetry and then perform a
bounded flush.

### Phase 3: Observer Boundary And Statement Context

- Add `StatementOperationObserver`, `TelemetryObserver`, `NoOpObserver`, and
optionally `SafeObserver`.
- Add `StatementTelemetryContext` with the ADBC field set: statement id,
statement type, operation type, result format, compression, retry count,
internal-call flag, poll count/latency, first-batch latency, consumption
latency, chunk metrics, and sanitized error info.
- Instrument `Statement::execute`, `submit`, and `execute_direct` using
observer lifecycle calls.
- Instrument `ExecutedStatementHandle::cancel` and `close` using discrete
operation telemetry.
- Ensure observer finalization is idempotent.

### Phase 4: ResultStream And CloudFetch Metrics

- Add optional observer ownership to `ResultStream`.
- Record first-batch-ready and consumed latencies from `ResultStream::next_batch`
and stream exhaustion/drop.
- Add `ChunkMetrics` plumbing from CloudFetch readers/link fetchers into the
observer, matching ADBC's `OnChunksDownloaded` path.
- Decide how `OwnedBatchIter` preserves observer finalization; it should not
bypass result consumption telemetry.

### Phase 5: Metadata Lifecycle

- Mirror ADBC's `ExecuteWithMetadataTelemetry` pattern in Rust.
- Instrument every `Metadata::*` method with `statement_type = Metadata` and the
corresponding operation type.
- Attach the observer to metadata-owned bare `ResultStream`s so finalization
happens on exhaustion/drop/error.

### Phase 6: Export Pipeline

- Implement host-scoped `TelemetryClientManager` with reference counting.
- Implement `TelemetryClient` with a concurrent queue, approximate queue count,
batch-size flush, timer flush, close-time flush, single-flush coordination,
and fail-open behavior, matching ADBC.
- Implement `DatabricksTelemetryExporter` for `/telemetry-ext` and
`/telemetry-unauth`.
- Implement retry with `max_retries` / `retry_delay`.
- Implement per-host circuit breaker with ADBC-compatible threshold/timeout
semantics.

### Phase 7: Feature Flags

- Add `FeatureFlagCache` / `FeatureFlagContext` equivalents.
- Cache all feature flags per host.
- Honor server TTL and short negative TTL on fetch failure.
- Merge remote flags below local configuration.
- Prefer warm-cache non-blocking application during session open; if the cache
is cold, warm it in the background unless product requirements demand a
blocking first fetch.

### Phase 8: Binding Wiring

- C ABI: expose telemetry config fields in the session-open config.
- PyO3/napi: map user-facing options to `TelemetryConfig`.
- Keep binding-specific telemetry collection out of scope unless a binding has
operations that never enter the kernel.

## Testing Strategy

Unit tests:

- `TelemetryConfig` defaults and validation.
- No-op sink behavior when disabled.
- Proto/model alignment with the shared SQL driver telemetry schema.
- `ConnectionTelemetry` creates real/no-op implementations correctly.
- `ConnectionTelemetry::emit_operation` records create session, delete session,
close statement, and cancel statement.
- `StatementOperationObserver` implementations never throw/panic into callers.
- `TelemetryObserver` populates context fields from lifecycle callbacks.
- `TelemetryObserver::on_finalized` enqueues exactly once across success, error,
close, cancel, and drop.
- Result stream records first-batch and consumption latencies.
- CloudFetch chunk metrics merge into `StatementTelemetryContext`.
- Metadata context maps each method to the correct operation type.
- Export client batches on size and interval.
- `TelemetryClientManager` shares clients per host and releases on last close.
- Circuit breaker opens and drops telemetry after repeated failures.
- Feature flag cache honors local-over-remote priority, server TTL, and negative
caching.
- Serialization preserves expected snake_case field names.
- Redaction tests for forbidden fields.

Integration tests:

- Use an in-memory `TelemetryExporter` injected through test-only config, matching
ADBC's `CapturingTelemetryExporter` pattern.
- Execute `SELECT 1` and assert one statement event.
- Execute a query with multiple batches and assert row/batch counts.
- Execute a metadata call and assert metadata operation type.
- Execute a failing SQL statement and assert error category without SQL text.
- Submit async, poll, await, and assert poll metrics.
- Cancel and close statements and assert discrete operation telemetry.
- Close session and assert pending events are flushed.

E2E tests:

- Keep live telemetry service validation optional and ignored by default.
- Prefer local capture tests in CI so telemetry failures do not make kernel CI
flaky.

## Risks And Mitigations


| Risk                                            | Mitigation                                                                                                                                                |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Telemetry work affects query latency            | Use in-memory aggregation and background export only                                                                                                      |
| Telemetry failure affects correctness           | Swallow errors and use no-op fallback                                                                                                                     |
| Events are lost on process exit                 | Bounded flush on explicit `Session::close`; document `Drop` as best effort                                                                                |
| Duplicate events from multiple close/drop paths | `emitted: AtomicBool` on each context                                                                                                                     |
| Host-scoped client leaks after session close    | ADBC-style reference counting in `TelemetryClientManager`                                                                                                 |
| Queue grows during a prolonged telemetry outage | Match ADBC in v0: keep enqueue non-blocking, rely on timer/batch/close flush and circuit breaker; revisit an explicit bound only as a hardening follow-up |
| Sensitive data emitted accidentally             | Allowlist-only event builders and redaction tests                                                                                                         |
| Future transport modes need different fields    | Non-exhaustive transport/result enums and protocol-neutral contexts; v0 records only `SeaRest`                                                            |


## Recommendation

Follow the ADBC implementation in `~/databricks` as the primary design and
implementation model for kernel telemetry.

Concretely:

- Port the ADBC layers into Rust concepts: `ConnectionTelemetry`,
`TelemetrySessionContext`, `StatementTelemetryContext`,
`StatementOperationObserver`, `TelemetryClientManager`, `TelemetryClient`,
`DatabricksTelemetryExporter`, `CircuitBreakerTelemetryExporter`, and
`FeatureFlagCache`.
- Keep statement, metadata, and result-stream code coupled only to lifecycle
observer methods, not to telemetry protobuf/exporter types.
- Preserve ADBC's fail-open behavior: every telemetry component returns no-op or
swallows telemetry errors instead of affecting driver operations.
- Preserve ADBC's event envelope, proto field coverage, endpoint shape,
feature-flag behavior, batching, retry, circuit breaker, and test exporter
strategy.
- Use JDBC only as a secondary compatibility reference for backend expectations,
not as the kernel implementation pattern.

The first implementation should land the ADBC-shaped core, observer boundary,
and captured-exporter tests before enabling live export. Feature flag rollout
and default enablement should follow the ADBC endpoint/flag unless product
owners explicitly split kernel telemetry into a separate server-side flag.
