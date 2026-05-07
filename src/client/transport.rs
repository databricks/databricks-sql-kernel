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
//!
//! Some methods and value-type fields are unused in the v0 build
//! today (e.g. `end_of_stream()`, `cloudfetch_config`,
//! `next_chunk_index`) but reach back to the legacy ADBC layer in
//! the `metadata-ffi`-gated build. PR 2 wires them into the v0
//! `Session::open` path; PR 7 deletes the legacy callers. The
//! `#![allow(dead_code)]` below is the v0-skeleton convention used
//! by `kernel_error.rs` and the spec skeleton modules.

#![allow(dead_code)]

use crate::kernel_error::Result;
use crate::reader::ResultReader;
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use crate::types::sea::{ExecuteParams, ResultManifest, StatementStatus};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

// ─── Transport-shared config ────────────────────────────────────────

/// Backend-agnostic configuration for [`Transport`] implementations.
///
/// Contains polling and CloudFetch settings shared across SEA REST,
/// SEA gRPC, and Thrift backends.
#[derive(Debug, Clone)]
pub(crate) struct TransportConfig {
    /// Maximum time to wait for statement completion (default: 600s).
    pub poll_timeout: Duration,
    /// Interval between status polls (default: 500ms).
    pub poll_interval: Duration,
    /// CloudFetch configuration.
    pub cloudfetch_config: CloudFetchConfig,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            poll_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_millis(500),
            cloudfetch_config: CloudFetchConfig::default(),
        }
    }
}

// ─── Trait input/output value types ─────────────────────────────────

/// Session information returned from [`Transport::create_session`].
#[derive(Debug, Clone)]
pub(crate) struct SessionInfo {
    pub session_id: String,
}

/// Result from [`Transport::execute_statement`]. Contains the
/// statement ID (for cancellation/cleanup), a reader over the result
/// data, and optionally the SEA manifest for metadata propagation.
pub(crate) struct ExecuteResult {
    pub statement_id: String,
    pub reader: Box<dyn ResultReader + Send>,
    pub manifest: Option<ResultManifest>,
}

impl std::fmt::Debug for ExecuteResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecuteResult")
            .field("statement_id", &self.statement_id)
            .field("reader", &"<dyn ResultReader>")
            .field("manifest", &self.manifest)
            .finish()
    }
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
}

/// Result of fetching chunk links.
#[derive(Debug, Clone)]
pub(crate) struct ChunkLinkFetchResult {
    pub links: Vec<CloudFetchLink>,
    pub has_more: bool,
    /// For chunk-index based backends (SEA).
    pub next_chunk_index: Option<i64>,
    /// For row-offset based backends (Thrift).
    pub next_row_offset: Option<i64>,
}

impl ChunkLinkFetchResult {
    /// Create an empty result indicating end of stream.
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

    /// Delete/close a session.
    async fn delete_session(&self, session_id: &str) -> Result<()>;

    // --- Statement Execution ---

    /// Execute a SQL statement within a session.
    ///
    /// Handles polling, result format detection, and reader creation internally.
    /// Always returns a ready-to-read `ExecuteResult`.
    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResult>;

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
    ) -> Result<ChunkLinkFetchResult>;

    // --- Statement Lifecycle ---

    /// Cancel a running statement.
    async fn cancel_statement(&self, statement_id: &str) -> Result<()>;

    /// Close/cleanup a statement (release server resources).
    async fn close_statement(&self, statement_id: &str) -> Result<()>;

    // --- Metadata ---

    /// List all catalogs.
    async fn list_catalogs(&self, session_id: &str) -> Result<ExecuteResult>;

    /// List schemas, optionally filtered by catalog and pattern.
    /// When catalog is None or wildcard, uses "SHOW SCHEMAS IN ALL CATALOGS".
    async fn list_schemas(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    /// List tables, optionally filtered.
    /// When catalog is None or wildcard, uses "SHOW TABLES IN ALL CATALOGS".
    async fn list_tables(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        table_types: Option<&[&str]>,
    ) -> Result<ExecuteResult>;

    /// List columns for a specific catalog, optionally filtered by patterns.
    /// Catalog is required — `SHOW COLUMNS IN ALL CATALOGS` is not supported.
    async fn list_columns(
        &self,
        session_id: &str,
        catalog: &str,
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

    /// List procedure columns (parameters), optionally filtered.
    ///
    /// Uses `information_schema.parameters` joined with `information_schema.routines`
    /// filtered by `routine_type = 'PROCEDURE'`.
    /// When catalog is None, queries `system.information_schema` (cross-catalog).
    /// When catalog is empty string, returns empty result.
    async fn list_procedure_columns(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<ExecuteResult>;

    /// List supported table types (static, no SQL executed).
    fn list_table_types(&self) -> Vec<String>;
}
