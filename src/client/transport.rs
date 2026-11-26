// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Wire-protocol abstraction for the kernel.
//!
//! The kernel talks to Databricks over a `Transport`. v0 ships one
//! implementation: [`SeaRestTransport`](super::sea::SeaRestTransport),
//! the Statement Execution API over REST. Future variants
//! (`SeaGrpcTransport`, `ThriftTransport`) plug into the same trait.
//!
//! The trait is `pub(crate)` — wrappers never see it. They select a
//! transport via the `SessionConfig.transport: Option<TransportMode>`
//! enum, which the kernel resolves to a concrete `Transport` impl
//! internally.

use crate::kernel_error::Result;
use crate::reader::ResultReader;
use crate::telemetry::ExecutionResultFormat;
use crate::types::cloudfetch::CloudFetchLink;
use crate::types::sea::{ExecuteParams, ResultManifest, StatementParameterSet, StatementStatus};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub(crate) struct ExecutionParams {
    pub scalar: ExecuteParams,
    pub parameter_sets: Vec<StatementParameterSet>,
}

// ─── Transport-shared config ────────────────────────────────────────

/// Timeout policy for statement execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionTimeout {
    /// Preserve the transport's pre-existing polling ceiling.
    Default,
    /// Skip the transport's polling ceiling.
    Unlimited,
    /// Apply one absolute client deadline.
    Deadline(Instant),
}

impl ExecutionTimeout {
    pub(crate) fn from_explicit_deadline(deadline: Option<Instant>) -> Self {
        deadline.map_or(Self::Unlimited, Self::Deadline)
    }
}

/// Backend-agnostic configuration for [`Transport`] implementations.
///
/// Contains polling settings shared across SEA REST, SEA gRPC, and
/// Thrift backends. CloudFetch tuning lives on
/// [`crate::reader::ResultReaderFactory`] instead — the transport
/// never reads it.
#[derive(Debug, Clone)]
pub(crate) struct TransportConfig {
    /// Default polling ceiling for async and metadata execution (600s).
    pub poll_timeout: Duration,
    /// Interval between status polls (default: 500ms).
    pub poll_interval: Duration,
    /// Whether CloudFetch (cloud-storage-backed result download via
    /// external links) is enabled for this session.
    ///
    /// When false, session creation requests paged inline results without
    /// cloud-storage downloads. The value is fixed for the session lifetime.
    pub cloudfetch_enabled: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            poll_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_millis(500),
            // Mirror `ResultConfig::default().cloudfetch_enabled`.
            cloudfetch_enabled: true,
        }
    }
}

// ─── Trait input/output value types ─────────────────────────────────

/// Whether a statement is known to belong to this transport's session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionAttribution {
    /// The statement was submitted through this session, so its response may advance the session.
    Current,
    /// Only the statement ID is known; its response must not affect this session.
    Detached,
}

/// Session information returned from [`Transport::create_session`].
#[derive(Debug, Clone)]
pub(crate) struct SessionInfo {
    pub session_id: String,
}

/// Result from [`Transport::execute_statement`]. Contains the
/// statement ID (for cancellation/cleanup), a reader over the result
/// data, optionally the SEA manifest for metadata propagation, and
/// the terminal `StatementStatus` so callers can surface
/// `num_modified_rows` / `display_message` / etc. without a second
/// GetStatementStatus round-trip.
pub(crate) struct ExecuteResult {
    pub statement_id: String,
    pub reader: Box<dyn ResultReader + Send>,
    pub manifest: Option<ResultManifest>,
    pub telemetry_result_format: ExecutionResultFormat,
    pub telemetry_is_compressed: bool,
    /// Terminal status the server reported in its final response.
    ///
    /// `status.state == Closed` means the server has already cleaned
    /// up the statement on its end — the kernel must NOT issue a
    /// separate `CloseStatement` for it (the server returns 404
    /// `STATEMENT_NOT_FOUND` on such redundant closes). This is the
    /// common case when the kernel omits `wait_timeout` from the
    /// execute request: the server holds the connection for its
    /// default wait, returns inline data with `state=CLOSED`, and is
    /// done. Setting a concrete `wait_timeout` yields `Succeeded`
    /// instead and requires an explicit close.
    ///
    /// The full status is carried (not just the state) so the
    /// `ExecutedStatement` can expose `num_modified_rows` /
    /// `display_message` / etc. without a second GetStatementStatus
    /// round-trip.
    pub status: StatementStatus,
}

impl std::fmt::Debug for ExecuteResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecuteResult")
            .field("statement_id", &self.statement_id)
            .field("reader", &"<dyn ResultReader>")
            .field("manifest", &self.manifest)
            .field("telemetry_result_format", &self.telemetry_result_format)
            .field("telemetry_is_compressed", &self.telemetry_is_compressed)
            .field("status", &self.status)
            .finish()
    }
}

/// Outcome of a directResults-style execute (`Transport::execute_statement_direct`):
/// the server is given a bounded inline wait (`wait_timeout` + `on_wait_timeout=CONTINUE`)
/// and we return WITHOUT the internal poll loop.
///
/// - `Completed` — the statement reached a terminal-and-succeeded state within
///   the inline wait; the reader is materialised from the inline result (no
///   extra `GetStatementResult` round-trip), exactly like `execute_statement`.
/// - `Running` — the statement is still `Pending`/`Running`; the caller gets the
///   `statement_id` + status and drives polling itself (and can cancel by id).
///
/// Mirrors the Thrift/JDBC directResults shape: fast queries come back inline,
/// slow ones come back as a running handle.
pub(crate) enum DirectOutcome {
    Completed(ExecuteResult),
    Running(ExecuteResponse),
}

/// Unified response from statement execution (internal to client implementations).
#[derive(Debug, Clone)]
pub(crate) struct ExecuteResponse {
    pub(crate) statement_id: String,
    pub(crate) status: StatementStatus,
    pub(crate) manifest: Option<ResultManifest>,
    pub(crate) result: Option<ExecuteResultData>,
}

/// Result data from execution (internal to client implementations).
#[derive(Debug, Clone)]
pub(crate) struct ExecuteResultData {
    pub(crate) next_chunk_internal_link: Option<String>,
    pub(crate) external_links: Option<Vec<CloudFetchLink>>,
    /// Inline Arrow IPC data (decoded from base64).
    /// Present when server returns inline results instead of CloudFetch.
    pub(crate) inline_arrow_data: Option<Vec<u8>>,
    /// Per-chunk metadata for the inline-Arrow streaming path. Only
    /// meaningful when `inline_arrow_data` is `Some(_)`. The
    /// `inline_next_chunk_index` field is the signal: `Some(N)` →
    /// the kernel should construct an [`InlineStreamingProvider`]
    /// (multi-chunk inline); `None` → the result fits in a single
    /// chunk and the legacy [`InlineArrowProvider`] is enough.
    pub(crate) inline_chunk_index: Option<i64>,
    pub(crate) inline_row_offset: Option<i64>,
    pub(crate) inline_row_count: Option<i64>,
    pub(crate) inline_next_chunk_index: Option<i64>,
}

/// Result of fetching chunk links.
#[derive(Debug, Clone)]
pub(crate) struct ChunkLinkFetchResult {
    pub links: Vec<CloudFetchLink>,
    pub has_more: bool,
    /// For chunk-index based backends (SEA).
    #[allow(
        dead_code,
        reason = "consumed by ResultStream chunk pagination in PR 4"
    )]
    pub next_chunk_index: Option<i64>,
    /// For row-offset based backends (Thrift), and as observability /
    /// diagnostic info on the SEA path.
    ///
    /// **Ownership note:** This field is *populated* by `Transport`
    /// implementations from the wire response but is intentionally
    /// **not** consumed by `SeaChunkLinkFetcher` — the fetcher
    /// recomputes its own `next_row_offset` from
    /// `last_link.row_offset + last_link.row_count` because the
    /// bounded SEA API contract designates the per-link metadata as
    /// authoritative. Keeping the field populated (rather than
    /// `None`) gives future Thrift transport wiring a place to land
    /// without churning the `ChunkLinkFetchResult` shape; for SEA
    /// the field is diagnostic only.
    #[allow(
        dead_code,
        reason = "Thrift consumer wiring TBD; SEA path tracks row offset internally"
    )]
    pub next_row_offset: Option<i64>,
}

/// One inline-Arrow chunk fetched via `GetResultData`.
///
/// Returned by [`Transport::get_inline_chunk`] for the SEA inline-
/// Arrow streaming path. Carries the chunk's Arrow IPC bytes
/// inline (no separate cloud download) plus the metadata the kernel
/// needs to walk the chunk chain: `chunk_index`, `row_offset`,
/// `row_count`, and the server-emitted `next_chunk_index`.
#[derive(Debug, Clone)]
pub(crate) struct InlineChunkFetchResult {
    /// Arrow IPC bytes (already base64-decoded by the SEA client).
    pub bytes: Vec<u8>,
    /// Index of this chunk in the result set.
    pub chunk_index: i64,
    /// Row position (0-based) of the first row in this chunk.
    pub row_offset: i64,
    /// Number of rows in this chunk.
    pub row_count: i64,
    /// `chunk_index` to ask for in the next `GetResultData` call.
    /// `None` marks end-of-stream for inline-Arrow link discovery
    /// (per the bounded SEA API contract — same end-of-stream
    /// signal as CloudFetch's per-link `next_chunk_index`).
    pub next_chunk_index: Option<i64>,
}

impl ChunkLinkFetchResult {
    /// Create an empty result indicating end of stream.
    #[allow(
        dead_code,
        reason = "called by Transport impls handling exhausted chunk streams in PR 4"
    )]
    pub fn end_of_stream() -> Self {
        Self {
            links: vec![],
            has_more: false,
            next_chunk_index: None,
            next_row_offset: None,
        }
    }
}

// ─── Transport trait ────────────────────────────────────────────────

/// Wire-protocol abstraction for talking to Databricks.
///
/// Implementations handle the protocol-specific details (REST vs
/// gRPC vs Thrift) of session management, statement execution,
/// CloudFetch chunk fetching, and metadata calls.
///
/// `pub(crate)` — wrappers never reach this trait. They select a
/// transport via `SessionConfig.transport: Option<TransportMode>` and
/// the kernel constructs the appropriate concrete impl internally.
#[async_trait]
pub(crate) trait Transport: Send + Sync + std::fmt::Debug {
    // --- Session Management ---

    /// Create a new session with the given catalog/schema context.
    async fn create_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        session_config: HashMap<String, String>,
    ) -> Result<SessionInfo>;

    /// Close the session created by this transport.
    async fn delete_session(&self) -> Result<()>;

    // --- Statement Execution ---

    /// Execute a SQL statement within a session.
    ///
    /// Handles polling, result format detection, and reader creation internally.
    /// Always returns a ready-to-read `ExecuteResult`.
    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecutionParams,
        timeout: ExecutionTimeout,
    ) -> Result<ExecuteResult>;

    /// Execute a SQL statement relying on the server's default inline wait
    /// (the caller's `ExecutionParams` omit `wait_timeout`/`on_wait_timeout` unless
    /// a `query_timeout_secs` was set), returning WITHOUT the internal poll loop
    /// — the Thrift/JDBC directResults model.
    ///
    /// Returns [`DirectOutcome::Completed`] (terminal + reader materialised
    /// inline) for queries that finish within the wait, or
    /// [`DirectOutcome::Running`] (statement id + status, no reader) for ones
    /// that don't. The caller drives any further polling and owns cancellation.
    async fn execute_statement_direct(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecutionParams,
    ) -> Result<DirectOutcome>;

    /// Submit a SQL statement without polling for completion.
    ///
    /// Returns whatever the server replied with on the initial
    /// `POST /statements` — `PENDING`, `RUNNING`, or terminal — so
    /// the caller (`Statement::submit`) can hand back an
    /// `ExecutedAsyncStatement` immediately. Unlike
    /// [`Transport::execute_statement`], this never polls and never
    /// constructs a reader; callers fetch results later via
    /// [`Transport::completed_result`].
    ///
    /// **The implementation forces `params.wait_timeout = "0s"`**
    /// regardless of what the caller supplied — submit's contract
    /// is "no wait", and honouring a non-zero caller value would
    /// defeat the point (the server would block up to 50s and we
    /// would just be slower about returning a `PENDING` handle).
    async fn submit_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecutionParams,
    ) -> Result<ExecuteResponse>;

    /// Materialise a reader from an observed terminal response.
    ///
    /// Returns:
    /// - `Ok(Some(ExecuteResult))` when the server reports
    ///   `Succeeded` (or `Closed` with inline data, which the SEA
    ///   server emits for one-batch results).
    /// - `Ok(None)` when the server reports `Pending` / `Running` —
    ///   the caller should `status()` again later.
    /// - `Err(...)` on `Failed` / `Cancelled` with the same structured error
    ///   `Transport::execute_statement`'s polling path produces.
    ///
    /// Used by `ExecutedAsyncStatement::fetch_result` and inside the
    /// `await_result` poll loop.
    async fn completed_result(
        &self,
        response: ExecuteResponse,
        session_attribution: SessionAttribution,
    ) -> Result<Option<ExecuteResult>>;

    /// One-shot status check for an in-flight statement. Returns the
    /// full `ExecuteResponse` (status + manifest + initial result, if
    /// any). Used by `ExecutedAsyncStatement::status` and the
    /// `await_result` poll loop, and `fetch_result`.
    async fn statement_status(
        &self,
        statement_id: &str,
        session_attribution: SessionAttribution,
    ) -> Result<ExecuteResponse>;

    // --- Result Fetching (CloudFetch) ---

    /// Fetch chunk links for CloudFetch.
    ///
    /// Different backends use different continuation mechanisms:
    /// - SEA uses `chunk_index`
    /// - Thrift uses `row_offset`
    ///
    /// Both parameters are provided; implementations use the relevant one.
    async fn get_result_chunks(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
        session_attribution: SessionAttribution,
    ) -> Result<ChunkLinkFetchResult>;

    /// Fetch one inline-Arrow chunk by `chunk_index` (+ server-side
    /// `row_offset`).
    ///
    /// Used for paged inline results. The response carries Arrow IPC bytes inline
    /// in `attachment` rather than emitting `external_links`.
    ///
    /// Per the bounded SEA API contract for OSS drivers:
    /// - `chunk_index` is derived from the *previous* chunk's
    ///   `next_chunk_index`. Never synthesised as `prev + 1`.
    /// - `row_offset` is derived from the previous chunk's
    ///   `row_offset + row_count`. Sent on every fetch.
    async fn get_inline_chunk(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
        session_attribution: SessionAttribution,
    ) -> Result<InlineChunkFetchResult>;

    /// Polling cadence + ceiling the kernel should use when driving
    /// `await_result` / `wait_for_completion`. v0 returns the
    /// transport's `TransportConfig` directly; future transports
    /// (gRPC, Thrift) can override.
    fn poll_config(&self) -> TransportConfig;

    // --- Statement Lifecycle ---

    /// Cancel a running statement.
    async fn cancel_statement(&self, statement_id: &str) -> Result<()>;

    /// Close/cleanup a statement (release server resources).
    async fn close_statement(&self, statement_id: &str) -> Result<()>;

    // --- Metadata ---

    /// List all catalogs.
    async fn list_catalogs(&self, session_id: &str) -> Result<ExecuteResult>;

    /// List schemas, optionally filtered by catalog and pattern.
    /// `catalog = None` emits "SHOW SCHEMAS IN ALL CATALOGS"; a `Some`
    /// catalog is scoped verbatim (a literal `%` / `*` / `""` matches no
    /// real catalog → 0 rows, not "all catalogs").
    async fn list_schemas(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    /// List tables, optionally filtered.
    /// `catalog = None` emits "SHOW TABLES IN ALL CATALOGS"; a `Some`
    /// catalog is scoped verbatim (a literal `%` / `*` / `""` matches no
    /// real catalog → 0 rows, not "all catalogs"). The higher-level
    /// `Metadata::list_tables` resolves real catalog wildcards before
    /// this point.
    async fn list_tables(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<ExecuteResult>;

    /// List columns, optionally filtered by catalog (exact) and
    /// schema / table / column patterns. `catalog = None` emits
    /// `SHOW COLUMNS IN ALL CATALOGS`, which returns rows from every
    /// catalog visible to the session; the response's `catalogName`
    /// column attributes each row to its source.
    async fn list_columns(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    /// List procedures, optionally filtered by catalog, schema pattern, and procedure name pattern.
    ///
    /// Uses `information_schema.routines` filtered by `routine_type = 'PROCEDURE'`.
    /// When catalog is None, queries `system.information_schema.routines` (cross-catalog).
    /// When catalog is empty string, returns empty result.
    async fn list_procedures(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    /// List functions, optionally filtered by catalog, schema pattern, and function name pattern.
    ///
    /// Uses `SHOW FUNCTIONS IN CATALOG`, which SEA can route to native `GetFunctions`.
    async fn list_functions(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        function_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    /// List procedure parameter / return columns, optionally filtered by
    /// catalog, schema pattern, procedure name pattern, and column name
    /// pattern.
    ///
    /// Uses `information_schema.parameters` JOINed to
    /// `information_schema.routines` filtered by `routine_type = 'PROCEDURE'`
    /// (function arguments excluded, matching JDBC `getProcedureColumns`).
    /// When catalog is None, queries `system.information_schema`
    /// (cross-catalog). When catalog is empty string, returns empty result.
    async fn list_procedure_columns(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    /// List primary-key columns for the given table. Catalog, schema,
    /// and table are required (Databricks `SHOW KEYS` requires full
    /// qualification).
    async fn list_primary_keys(
        &self,
        session_id: &str,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<ExecuteResult>;

    /// List foreign-key columns referencing the given table. The
    /// foreign side is required; the parent side is optional (matches
    /// "any parent" when omitted).
    #[allow(
        clippy::too_many_arguments,
        reason = "JDBC `getImportedKeys` / ODBC `SQLForeignKeys` mandates this surface"
    )]
    async fn list_foreign_keys(
        &self,
        session_id: &str,
        parent_catalog: Option<&str>,
        parent_schema: Option<&str>,
        parent_table: Option<&str>,
        foreign_catalog: &str,
        foreign_schema: &str,
        foreign_table: &str,
    ) -> Result<ExecuteResult>;

    /// List supported table types (static, no SQL executed).
    fn list_table_types(&self) -> Vec<String>;
}
