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

//! SEA (Statement Execution API) client implementation.
//!
//! This module implements the `Transport` trait using the Databricks
//! SQL Statement Execution API (REST-based).

use crate::client::retry::RequestType;
use crate::client::{
    ChunkLinkFetchResult, DatabricksHttpClient, DirectOutcome, ExecuteResponse, ExecuteResult,
    ExecuteResultData, InlineChunkFetchResult, SessionInfo, Transport, TransportConfig,
};
use crate::kernel_error::{Error, Result};
use crate::klog::{debug, warn};
use crate::metadata::sql::SqlCommandBuilder;
use crate::reader::ResultReaderFactory;
use crate::types::cloudfetch::CloudFetchLink;
use crate::types::sea::{
    CreateSessionRequest, CreateSessionResponse, ExecuteParams, ExecuteStatementRequest,
    GetChunksResponse, StatementExecutionResponse, StatementState,
};
use async_trait::async_trait;
use reqwest::Method;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Handle as RuntimeHandle;

/// Header telling the server a metadata query may be executed fully
/// synchronously, letting it answer in the initial response instead of
/// going through the asynchronous statement machinery.
///
/// Sent on every synchronous metadata query, unconditionally. It is
/// advisory — a server that does not recognise it ignores it — so there is
/// nothing for a caller to configure.
const SEA_SYNC_METADATA_HEADER: &str = "x-databricks-sea-can-run-fully-sync";

/// Internal terminal-outcome for a single SEA statement response.
///
/// Used by the shared decoder [`SeaRestTransport::terminal_outcome`]
/// so the sync polling loop and the async one-shot
/// [`Transport::fetch_completed_result`] path produce identical
/// outcomes (and identical errors) for `FAILED` / `CANCELLED` responses.
enum StatementOutcome {
    /// Terminal success — `Succeeded` OR `Closed` (with or without inline
    /// data; an empty `Closed` is a valid empty result set, materialised
    /// downstream by `create_reader`'s `EmptyReader`).
    Done(ExecuteResponse),
    /// `Pending` / `Running` — caller polls again.
    NotReady(ExecuteResponse),
    /// Terminal error.
    Err(Error),
}

/// SEA client for the Databricks SQL Statement Execution API.
///
/// This client implements the `Transport` trait using REST endpoints.
/// The `reader_factory` is set after construction via `set_reader_factory()` because
/// `ResultReaderFactory` needs `Arc<dyn Transport>` which requires wrapping
/// `SeaRestTransport` in `Arc` first.
pub(crate) struct SeaRestTransport {
    http_client: Arc<DatabricksHttpClient>,
    host: String,
    warehouse_id: String,
    config: TransportConfig,
    reader_factory: OnceLock<ResultReaderFactory>,
    runtime_handle: OnceLock<RuntimeHandle>,
}

impl std::fmt::Debug for SeaRestTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeaRestTransport")
            .field("host", &self.host)
            .field("warehouse_id", &self.warehouse_id)
            .field("config", &self.config)
            .field(
                "reader_factory",
                &if self.reader_factory.get().is_some() {
                    "<set>"
                } else {
                    "<not set>"
                },
            )
            .finish()
    }
}

impl SeaRestTransport {
    /// Create a new SEA client.
    ///
    /// After construction, call `set_reader_factory()` to enable result reader creation.
    /// This two-step initialization is required because `ResultReaderFactory` needs
    /// `Arc<dyn Transport>`, which is only available after wrapping in `Arc`.
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        host: impl Into<String>,
        warehouse_id: impl Into<String>,
        config: TransportConfig,
    ) -> Self {
        Self {
            http_client,
            host: host.into(),
            warehouse_id: warehouse_id.into(),
            config,
            reader_factory: OnceLock::new(),
            runtime_handle: OnceLock::new(),
        }
    }

    /// Set the reader factory and runtime handle. Must be called once after wrapping in `Arc`.
    pub fn set_reader_factory(
        &self,
        reader_factory: ResultReaderFactory,
        runtime_handle: RuntimeHandle,
    ) {
        let _ = self.reader_factory.set(reader_factory);
        let _ = self.runtime_handle.set(runtime_handle);
    }

    fn reader_factory(&self) -> Result<&ResultReaderFactory> {
        self.reader_factory.get().ok_or_else(|| {
            Error::invalid_state().message("SeaRestTransport reader_factory not initialized")
        })
    }

    /// Build the base URL for API requests.
    fn base_url(&self) -> String {
        format!("{}/api/2.0/sql", self.host.trim_end_matches('/'))
    }

    /// Build the `/result/chunks/{idx}` URL, optionally with the
    /// `row_offset` query parameter set.
    ///
    /// `chunk_index` is a path parameter (the SEA REST contract).
    /// `row_offset` is appended as a query parameter on the inline-
    /// Arrow path — the SEA inline-Arrow design (go/inline-arrow-sea,
    /// "OSS JDBC Driver" section) revives the previously deprecated
    /// `row_offset` field for clients fetching by `chunk_index +
    /// rowOffset` against ThriftServer's `FETCH_ABSOLUTE`-based inline
    /// path. On the CloudFetch path (where the kernel passes
    /// `row_offset = 0`) we omit the parameter entirely so we don't
    /// perturb the existing wire shape that today's SEA accepts.
    ///
    /// Negative `row_offset` is treated the same as `0` (omitted) —
    /// the field is `i64` for arithmetic convenience upstream but the
    /// SEA wire only accepts non-negative offsets. A debug-build
    /// assertion catches the negative case so it surfaces at the
    /// call-site during development rather than as a silent omission
    /// in production. Pathological-but-valid values (`i64::MAX`)
    /// serialise as-is; the server is expected to reject those, and
    /// the kernel does not pre-validate row-position semantics.
    ///
    /// **Invariant** (preserve when calling this helper): the kernel
    /// never *synthesises* a non-zero `row_offset`. Non-zero values
    /// are only ever echoed back from a prior server
    /// `next_row_offset` response. SEA versions that haven't shipped
    /// the inline-Arrow row-offset path return `next_row_offset = None`
    /// in `GetChunksResponse`, so the kernel naturally keeps
    /// `row_offset = 0` and the `?row_offset=N` query parameter
    /// never appears on the wire. If a future change starts
    /// originating row offsets client-side (e.g. for cursor
    /// resumption), it must coordinate with the SEA rollout so a
    /// workspace rollback can't strand the kernel sending
    /// `?row_offset=N` to a server that doesn't recognise it.
    ///
    /// **Future**: if a second optional query parameter ever lands
    /// on this URL, migrate to `reqwest::Url::parse(...).query_pairs_mut()`
    /// — the `format!`-based concatenation only handles one
    /// parameter cleanly and is easy to misuse (`?` vs `&` ordering)
    /// when adding more.
    ///
    /// See: <https://docs.databricks.com/api/workspace/statementexecution>
    fn result_chunks_url(&self, statement_id: &str, chunk_index: i64, row_offset: i64) -> String {
        debug_assert!(
            row_offset >= 0,
            "row_offset must be non-negative on the SEA wire (got {row_offset})",
        );
        if row_offset > 0 {
            format!(
                "{}/statements/{}/result/chunks/{}?row_offset={}",
                self.base_url(),
                statement_id,
                chunk_index,
                row_offset
            )
        } else {
            format!(
                "{}/statements/{}/result/chunks/{}",
                self.base_url(),
                statement_id,
                chunk_index
            )
        }
    }

    /// Convert SEA response to internal ExecuteResultData.
    fn convert_result_data(result: &crate::types::sea::ResultData) -> Result<ExecuteResultData> {
        let external_links = if let Some(ref links) = result.external_links {
            let converted: Result<Vec<CloudFetchLink>> = links
                .iter()
                .map(CloudFetchLink::from_external_link)
                .collect();
            Some(converted?)
        } else {
            None
        };

        Ok(ExecuteResultData {
            next_chunk_internal_link: result.next_chunk_internal_link.clone(),
            external_links,
            inline_arrow_data: result.attachment.clone(),
            inline_chunk_index: result.chunk_index,
            inline_row_offset: result.row_offset,
            inline_row_count: result.row_count,
            inline_next_chunk_index: result.next_chunk_index,
        })
    }

    /// Poll for statement status (private — used by wait_for_completion).
    async fn get_statement_status(&self, statement_id: &str) -> Result<ExecuteResponse> {
        let url = format!("{}/statements/{}", self.base_url(), statement_id);

        debug!("Getting statement status at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::GET, &url)
            .build()
            .map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

        let response = self
            .http_client
            .execute(request, RequestType::GetStatementStatus)
            .await?;
        let body = response
            .text()
            .await
            .map_err(|e| Error::network_error().message(format!("Failed to read response: {e}")))?;

        let sea_response: StatementExecutionResponse =
            serde_json::from_str(&body).map_err(|e| {
                Error::data_loss().message(format!(
                    "Failed to parse status response: {e} - body: {body}"
                ))
            })?;

        debug!(
            "Status response: statement_id={}, status={:?}",
            sea_response.statement_id, sea_response.status.state
        );

        let mut response = Self::convert_response(sea_response)?;
        self.enrich_num_modified_rows(&mut response).await;
        Ok(response)
    }

    /// SEA omits the DML row count from the status envelope — for an
    /// INSERT/UPDATE/DELETE/MERGE it rides in the result set instead (a single
    /// row of `num_*_rows` counters). Thrift, by contrast, reports it on the
    /// operation status. When a terminal GetStatement response carries a
    /// DML-count result inline, derive `num_affected_rows` (the total) and set
    /// it on the status so the extended-status accessors reach Thrift parity —
    /// crucially on the async path, where the count would otherwise require an
    /// explicit result fetch. No extra round-trip: the count rides on the status
    /// poll the caller already made.
    ///
    /// Gated on the manifest's column signature so SELECTs (real columns) never
    /// hit the decode path; a DML-count result is always tiny and inline.
    async fn enrich_num_modified_rows(&self, response: &mut ExecuteResponse) {
        if response.status.num_modified_rows.is_some() {
            return;
        }
        match response.manifest.as_ref() {
            Some(m) if crate::statement::dml_count::manifest_is_dml_count(m) => {}
            _ => return,
        }
        let Ok(factory) = self.reader_factory() else {
            return;
        };
        // Decode the inline count via a throwaway reader; this is independent of
        // any later fetch the caller makes (a fresh GetStatement builds its own
        // reader), so result consumption is unaffected.
        let reader = match factory
            .create_reader(&response.statement_id, response)
            .await
        {
            Ok(r) => r,
            Err(_) => return,
        };
        let mut stream = match crate::result_stream::ResultStream::from_reader(reader).await {
            Ok(s) => s,
            Err(_) => return,
        };
        if let Ok(Some(n)) = crate::statement::dml_count::read_dml_affected_rows(&mut stream).await
        {
            response.status.num_modified_rows = Some(n);
        }
    }

    /// Decode a single SEA `ExecuteResponse` into a terminal outcome.
    ///
    /// Shared by `wait_for_completion` (sync poll loop) and
    /// [`Transport::fetch_completed_result`] (async one-shot fetch)
    /// so both paths produce identical errors for `FAILED` / `CANCELLED`.
    /// `CLOSED` (with or without result data) is a terminal success — JDBC
    /// parity; an empty `CLOSED` materialises an empty result downstream.
    fn terminal_outcome(response: ExecuteResponse) -> StatementOutcome {
        match response.status.state {
            StatementState::Succeeded => StatementOutcome::Done(response),
            StatementState::Failed => {
                let service_error = response.status.error.as_ref();
                let error_msg = service_error
                    .and_then(|e| e.message.clone())
                    .unwrap_or_else(|| "Unknown error".to_string());

                // Server-reported failure → SqlError. The server's
                // `error_code` slug is NOT used to pick the categorical
                // bucket: SEA returns the generic `BAD_REQUEST` slug for
                // genuine SQL errors too (e.g. `TABLE_OR_VIEW_NOT_FOUND`,
                // SQLSTATE 42P01 — verified live), so it is not a reliable
                // InvalidArgument signal. The slug + SQLSTATE are carried
                // below for wrappers that want finer-grained mapping.
                let mut error = Error::sql_error()
                    .message(&error_msg)
                    .with_query_id(&response.statement_id);

                // Reyden and newer DBSQL responses report SQLSTATE on
                // status.sql_state. Message parsing remains as a fallback.
                if let Some(s) = crate::kernel_error::extract_sqlstate_from_status(&response.status)
                {
                    error = error.with_sql_state(s);
                }

                if let Some(err) = service_error {
                    // Databricks-specific error code slug
                    // (`STATEMENT_NOT_FOUND`, `WAREHOUSE_STOPPED`, …).
                    // Wrappers branch on this to map host-language
                    // exception taxonomies.
                    if let Some(ec) = err.error_code.as_deref().filter(|s| !s.is_empty()) {
                        error = error.with_error_code(ec);
                    }
                }

                // Carry the extended status fields onto the Error so
                // wrappers see what Thrift's `TGetOperationStatusResp`
                // surfaces on terminal-error states.
                StatementOutcome::Err(crate::kernel_error::carry_extended_status(
                    error,
                    &response.status,
                ))
            }
            StatementState::Canceled => {
                // `cancelled()` (ErrorCode::Cancelled), not invalid_state:
                // a server-side cancel (admin cancel, warehouse eviction,
                // another client's cancel) is an operational/cancelled
                // condition, not a caller-argument error. The connector
                // maps "Cancelled" -> OperationalError (the DatabaseError
                // family Thrift uses), whereas invalid_state collapses to
                // InvalidArgument -> ProgrammingError (misleading "bad SQL").
                let error = Error::cancelled()
                    .message("Statement was canceled")
                    .with_query_id(&response.statement_id);
                StatementOutcome::Err(crate::kernel_error::carry_extended_status(
                    error,
                    &response.status,
                ))
            }
            StatementState::Closed => {
                // CLOSED is a terminal SUCCESS state — parity with JDBC's
                // `DatabricksSdkClient`, which treats SUCCEEDED and CLOSED
                // identically as success (anything else → handleFailedExecution).
                // It's the server's auto-close-on-completion signal. A CLOSED
                // response with NO result payload is a valid EMPTY result set
                // (common for DDL/DML and no-row queries that auto-close), NOT
                // an error: `create_reader` builds an `EmptyReader` from the
                // manifest schema for exactly this case (see its own CLOSED arm
                // in reader/mod.rs). Gating on `result.is_some()` here was both
                // stricter than JDBC and internally inconsistent with that.
                StatementOutcome::Done(response)
            }
            StatementState::Pending | StatementState::Running => {
                StatementOutcome::NotReady(response)
            }
        }
    }

    /// Wait for statement to complete, polling status.
    async fn wait_for_completion(&self, response: ExecuteResponse) -> Result<ExecuteResponse> {
        let start = std::time::Instant::now();
        let mut current_response = response;

        loop {
            match Self::terminal_outcome(current_response) {
                StatementOutcome::Done(r) => return Ok(r),
                StatementOutcome::Err(e) => return Err(e),
                StatementOutcome::NotReady(r) => {
                    if start.elapsed() > self.config.poll_timeout {
                        return Err(Error::timeout().message("Statement execution timed out"));
                    }

                    tokio::time::sleep(self.config.poll_interval).await;
                    debug!(
                        statement_id = %r.statement_id,
                        state = ?r.status.state,
                        "Polling statement status"
                    );
                    current_response = self.get_statement_status(&r.statement_id).await?;
                }
            }
        }
    }

    /// Call the execute statement API endpoint (without polling or reader creation).
    async fn call_execute_api(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
        request_type: RequestType,
    ) -> Result<ExecuteResponse> {
        let url = format!("{}/statements", self.base_url());

        // Select the result disposition per statement from the
        // connection-level CloudFetch setting, mirroring the reference
        // JDBC driver's SEA path (`DatabricksSdkClient.getRequest`:
        // `cloudFetch ? INLINE_OR_EXTERNAL_LINKS : INLINE`). This is the
        // authoritative mechanism for disabling cloud-fetch on SEA —
        // there is NO `CAN_CLOUD_DOWNLOAD` session conf (the server
        // rejects it at ExecuteStatement with `CONFIG_NOT_AVAILABLE`,
        // issue #196).
        //
        // `INLINE`: the server serves every result inline (Arrow bytes in
        // the response `attachment`), regardless of size. `format` stays
        // `ARROW_STREAM` — the kernel's inline reader consumes Arrow IPC
        // from the attachment (the SEA inline-Arrow path), so unlike JDBC
        // we keep Arrow rather than switching to `JSON_ARRAY`.
        //
        // `INLINE_OR_EXTERNAL_LINKS` (the CloudFetch-enabled default): not
        // a public API disposition but enables returning all chunk links
        // in a single response (requires the JDBC user agent). Once
        // EXTERNAL_LINKS supports multi-link responses, switch back to it.
        let disposition = if self.config.cloudfetch_enabled {
            "INLINE_OR_EXTERNAL_LINKS"
        } else {
            "INLINE"
        };
        let request_body = ExecuteStatementRequest {
            warehouse_id: self.warehouse_id.clone(),
            statement: sql.to_string(),
            session_id: Some(session_id.to_string()),
            catalog: params.catalog.clone(),
            schema: params.schema.clone(),
            disposition: disposition.to_string(),
            format: "ARROW_STREAM".to_string(),
            wait_timeout: params.wait_timeout.clone(),
            on_wait_timeout: params.on_wait_timeout.clone(),
            row_limit: params.row_limit,
            result_compression: Some("LZ4_FRAME".to_string()),
            parameters: params.parameters.clone(),
            query_tags: params.query_tags.clone(),
        };

        debug!("Executing statement at {}: {}", url, sql);

        let mut builder = self
            .http_client
            .inner()
            .request(Method::POST, &url)
            .header("Content-Type", "application/json");

        // Signal that a metadata query may be served fully synchronously, so
        // the server can skip the async statement machinery and answer in the
        // initial response.
        //
        // Sent unconditionally for metadata queries — there is no opt-out knob.
        // The header is advisory: a server that does not recognise it ignores
        // it and the query still succeeds through the normal path, so there is
        // no configuration a caller could need to express. (JDBC exposes an
        // EnableSeaSyncMetadata property for this; we deliberately don't, since
        // a flag whose only settings are "faster" and "same but slower" is a
        // support liability rather than a feature.)
        //
        // The synchronous-only condition comes free from `request_type`: the
        // async path routes through `submit_statement` with
        // `RequestType::ExecuteStatement`, so `ExecuteMetadataQuery` only ever
        // reaches here on the synchronous path.
        if request_type == RequestType::ExecuteMetadataQuery {
            debug!("Adding {SEA_SYNC_METADATA_HEADER} for synchronous metadata query");
            builder = builder.header(SEA_SYNC_METADATA_HEADER, "true");
        }

        let request = builder.json(&request_body).build().map_err(|e| {
            Error::invalid_argument().message(format!("Failed to build request: {e}"))
        })?;

        let response = self.http_client.execute(request, request_type).await?;
        let body = response
            .text()
            .await
            .map_err(|e| Error::network_error().message(format!("Failed to read response: {e}")))?;

        let sea_response: StatementExecutionResponse =
            serde_json::from_str(&body).map_err(|e| {
                Error::data_loss().message(format!(
                    "Failed to parse execute response: {e} - body: {body}"
                ))
            })?;

        debug!(
            "Execute response: statement_id={}, status={:?}",
            sea_response.statement_id, sea_response.status.state
        );

        Self::convert_response(sea_response)
    }

    /// Convert SEA response to internal ExecuteResponse.
    fn convert_response(response: StatementExecutionResponse) -> Result<ExecuteResponse> {
        let result = if let Some(ref result) = response.result {
            Some(Self::convert_result_data(result)?)
        } else {
            None
        };

        Ok(ExecuteResponse {
            statement_id: response.statement_id,
            status: response.status,
            manifest: response.manifest,
            result,
        })
    }

    /// Execute a SQL statement end-to-end: call API → poll → create reader.
    ///
    /// Shared implementation for both user queries (`ExecuteStatement`) and
    /// metadata queries (`ExecuteMetadataQuery`). The `request_type` controls
    /// the retry strategy (idempotent vs non-idempotent).
    async fn execute_and_read(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
        request_type: RequestType,
    ) -> Result<ExecuteResult> {
        // After call_execute_api succeeds, the SEA statement is open
        // on the server. Any subsequent failure before we hand back
        // an ExecuteResult (which wraps it in an ExecutedStatement
        // whose Drop / explicit close() cleans up) would orphan the
        // server-side statement until SQL Gateway's
        // CommandInactivityTimeout (~1h) reclaims it. Two known
        // paths into that gap:
        //
        //   - wait_for_completion times out (`Error::timeout`) while
        //     the server still considers the statement live in
        //     PENDING / RUNNING. (Terminal FAILED / CANCELED is the
        //     server's own cleanup signal — no CloseStatement
        //     needed.)
        //   - create_reader fails. The most likely sub-failure today
        //     is `ResultPostProcessor::new ->
        //     StreamingCloudFetchProvider::get_schema` eagerly
        //     downloading chunk 0 to extract the result schema; a
        //     non-retriable 4xx / 5xx on that GET propagates up.
        //
        // Capture the statement_id from the initial response so we
        // can issue a best-effort close on either path.
        let response = self
            .call_execute_api(session_id, sql, params, request_type)
            .await?;
        let statement_id = response.statement_id.clone();

        // Publish the server statement id to the caller's canceller
        // slot NOW — before polling — so a concurrent
        // `StatementCanceller::cancel()` can target a still-running
        // sync query. (The initial POST returns within the server
        // wait_timeout, ~10s, even for a long query.)
        if let Some(observer) = params.inflight_id_observer.as_ref() {
            if let Ok(mut slot) = observer.lock() {
                *slot = Some(statement_id.clone());
            }
        }

        let response = match self.wait_for_completion(response).await {
            Ok(r) => r,
            Err(e) => {
                self.close_orphaned_statement(&statement_id, "wait_for_completion")
                    .await;
                return Err(e);
            }
        };
        let reader_factory = self.reader_factory()?;
        // `create_reader` wraps in a `ResultPostProcessor`
        // internally, so the reader returned here already carries
        // the server-reported per-column metadata
        // (`databricks.type_name` etc.) on its schema and on every
        // emitted batch. Bindings (PySQL, future ADBC) read those
        // attributes to recover the precise type for columns whose
        // Arrow shape collapses (VARIANT → Utf8, intervals → Interval,
        // GEOMETRY/GEOGRAPHY → Utf8). The post-processor also handles
        // the optional complex-types-as-JSON rewrite in the same pass.
        let reader = match reader_factory
            .create_reader(&response.statement_id, &response)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                self.close_orphaned_statement(&statement_id, "create_reader")
                    .await;
                return Err(e);
            }
        };

        // The full `status` is carried forward so the executed handle
        // can skip a redundant CloseStatement when the server already
        // cleaned up (status.state=Closed) AND expose
        // num_modified_rows / display_message / etc. without a second
        // GetStatementStatus round-trip.
        Ok(ExecuteResult {
            statement_id: response.statement_id,
            reader,
            manifest: response.manifest,
            status: response.status,
        })
    }

    /// Best-effort close for a server-side statement we opened but
    /// can't hand off to an `ExecutedStatement` because of a failure
    /// in `wait_for_completion` or `create_reader`. The trait method
    /// `close_statement` already swallows its own errors (line 797);
    /// the wrapper here is just a logging seam so a future failure
    /// to clean up shows up in traces with the failing-phase
    /// attribution (`wait_for_completion` vs `create_reader`).
    async fn close_orphaned_statement(&self, statement_id: &str, phase: &str) {
        // A synchronously-rejected request (e.g. a `BAD_REQUEST`
        // parameter-binding failure) returns FAILED with no
        // `statement_id` — nothing was opened server-side, so there is
        // nothing to close. Skip rather than issue a CloseStatement for
        // an empty id.
        if statement_id.is_empty() {
            return;
        }
        debug!(
            statement_id = %statement_id,
            phase = %phase,
            "execute_and_read: closing orphaned statement after failure"
        );
        if let Err(e) = <Self as Transport>::close_statement(self, statement_id).await {
            // `close_statement` is documented as best-effort and
            // returns Ok even on inner failure (see body), but the
            // signature is fallible so future implementations can
            // surface real errors. Demote to warn — the caller is
            // already returning the originating error.
            warn!(
                statement_id = %statement_id,
                phase = %phase,
                error = %e,
                "execute_and_read: orphan-close failed; statement may linger \
                 until SQL Gateway CommandInactivityTimeout"
            );
        }
    }
}

/// Convert a parsed `GetChunksResponse` into the kernel-facing
/// `ChunkLinkFetchResult`. Extracted from `get_result_chunks`
/// so the continuation derivation (top-level vs per-link
/// `next_chunk_index`, `next_row_offset` from `.last()`,
/// `has_more` flag) is unit-testable without setting up a
/// wiremock for `/result/chunks/{N}`.
///
/// Continuation derivation:
/// - `next_chunk_index` prefers the top-level field, falling
///   back to the *last* link's `next_chunk_index` if the
///   top-level is absent.
/// - `next_row_offset` comes from the last link's
///   `row_offset + row_count` (saturating).
/// - `has_more = next_chunk_index.is_some()`.
fn derive_chunk_fetch_result(chunks_response: GetChunksResponse) -> Result<ChunkLinkFetchResult> {
    let links = if let Some(ref external_links) = chunks_response.external_links {
        external_links
            .iter()
            .map(CloudFetchLink::from_external_link)
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![]
    };

    // The `next_chunk_index` may be:
    // 1. At the top level of the response, OR
    // 2. Inside the external_link (each link knows its next chunk)
    //
    // Prefer top-level, fall back to the *last* link's
    // `next_chunk_index`. A previous version of this code read
    // `links.first()` — wrong if the batch contains multiple
    // links, since only the last link's continuation pointer is
    // meaningful. Today the SEA chunk-fetch endpoint typically
    // returns a single link per response so the bug was latent,
    // but extracting this into a unit-testable function makes a
    // regression to `.first()` impossible to land silently.
    let next_chunk_index = chunks_response.next_chunk_index.or_else(|| {
        chunks_response
            .external_links
            .as_ref()
            .and_then(|links| links.last())
            .and_then(|link| link.next_chunk_index)
    });

    let has_more = next_chunk_index.is_some();

    // Per the bounded-SEA-API contract for OSS drivers, the
    // server-provided `external_links[].row_offset` is what the
    // next fetch must send back. We propagate the last link's
    // `row_offset + row_count` as the offset for the *next*
    // fetch. `saturating_add` so a malicious/buggy server
    // emitting `i64::MAX` row counts can't trigger an arithmetic
    // panic in debug builds.
    let next_row_offset = if has_more {
        links
            .last()
            .map(|l| l.row_offset.saturating_add(l.row_count))
    } else {
        None
    };

    Ok(ChunkLinkFetchResult {
        links,
        has_more,
        next_chunk_index,
        next_row_offset,
    })
}

#[async_trait]
impl Transport for SeaRestTransport {
    async fn create_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        session_config: HashMap<String, String>,
    ) -> Result<SessionInfo> {
        let url = format!("{}/sessions", self.base_url());

        let request_body = CreateSessionRequest {
            warehouse_id: self.warehouse_id.clone(),
            catalog: catalog.map(|s| s.to_string()),
            schema: schema.map(|s| s.to_string()),
            session_confs: session_config,
        };

        debug!("Creating session at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::POST, &url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .build()
            .map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

        let response = self
            .http_client
            .execute(request, RequestType::CreateSession)
            .await?;
        let body = response
            .text()
            .await
            .map_err(|e| Error::network_error().message(format!("Failed to read response: {e}")))?;

        let session_response: CreateSessionResponse = serde_json::from_str(&body).map_err(|e| {
            Error::data_loss().message(format!(
                "Failed to parse session response: {e} - body: {body}"
            ))
        })?;

        debug!("Created session: {}", session_response.session_id);

        Ok(SessionInfo {
            session_id: session_response.session_id,
        })
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        // SEA requires `warehouse_id` as a query parameter on
        // `DELETE /api/2.0/sql/sessions/{id}`. Without it the server
        // returns HTTP 400 ("Required field is not specified:
        // warehouse_id"), the session never gets cleaned up via the
        // explicit close path, and instead waits for the idle-timeout
        // reaper to drop it.
        let url = format!(
            "{}/sessions/{}?warehouse_id={}",
            self.base_url(),
            session_id,
            self.warehouse_id
        );

        debug!("Deleting session at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::DELETE, &url)
            .build()
            .map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

        // Best-effort cleanup. The HTTP client already retries the
        // global Sea-category ceiling (900s / 5 attempts) before
        // surfacing a final error here; once we're past that, the
        // session was probably already torn down by an idle timeout
        // or the network is hard down. Log so failures aren't
        // invisible, but don't propagate — the caller has no
        // useful recovery path.
        if let Err(e) = self
            .http_client
            .execute(request, RequestType::CloseSession)
            .await
        {
            crate::klog::warn!(
                session_id = %session_id,
                error = %e,
                "delete_session failed after HTTP retries; server-side session will be reclaimed by idle timeout"
            );
        } else {
            debug!("Deleted session: {}", session_id);
        }

        Ok(())
    }

    async fn execute_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResult> {
        self.execute_and_read(session_id, sql, params, RequestType::ExecuteStatement)
            .await
    }

    async fn submit_statement(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<ExecuteResponse> {
        // Single POST /statements; no polling, no reader creation.
        // Force `wait_timeout = "0s"` regardless of what the caller
        // passed — submit's contract is "no wait", and silently
        // honouring a non-zero caller value would defeat the point
        // (the server would block up to 50s and we'd just be slower
        // about handing back a PENDING handle). `RequestType::ExecuteStatement`
        // matches the retry policy of the sync path — submit is also
        // non-idempotent in the worst case (e.g. an INSERT submitted
        // twice).
        let mut params = params.clone();
        params.wait_timeout = Some("0s".to_string());
        params.on_wait_timeout = Some("CONTINUE".to_string());
        self.call_execute_api(session_id, sql, &params, RequestType::ExecuteStatement)
            .await
    }

    async fn execute_statement_direct(
        &self,
        session_id: &str,
        sql: &str,
        params: &ExecuteParams,
    ) -> Result<DirectOutcome> {
        // directResults model: OMIT `wait_timeout` (exactly like the sync
        // `execute` path) so the server applies its default ~10s inline wait
        // AND auto-closes on success (returns `state=CLOSED`). This is the fast
        // path: a quick query / fire-and-forget DDL comes back terminal AND
        // already server-closed, so the handle's `close()` is a true no-op (no
        // cancel+close round-trip that would otherwise discard an unfetched
        // result). Setting `wait_timeout=10s` explicitly instead yields
        // `SUCCEEDED` (statement still alive → needs an explicit cancel+close),
        // which is both slower and breaks fire-and-forget. We just DON'T run the
        // `wait_for_completion` poll loop: a query that outlasts the inline wait
        // comes back `Running` for the caller to poll/cancel.
        let response = self
            .call_execute_api(session_id, sql, params, RequestType::ExecuteStatement)
            .await?;
        let statement_id = response.statement_id.clone();

        // Publish the server statement id to the caller's canceller slot now —
        // a concurrent `StatementCanceller::cancel()` (the Running arm) can
        // target the still-running query immediately, no mid-execute observer
        // race (the id is known from this POST response).
        if let Some(observer) = params.inflight_id_observer.as_ref() {
            if let Ok(mut slot) = observer.lock() {
                *slot = Some(statement_id.clone());
            }
        }

        match Self::terminal_outcome(response) {
            // Terminal + succeeded within the inline wait → materialise the
            // reader from the inline result (no extra GetStatementResult).
            StatementOutcome::Done(r) => {
                let reader_factory = self.reader_factory()?;
                let reader = match reader_factory.create_reader(&r.statement_id, &r).await {
                    Ok(reader) => reader,
                    Err(e) => {
                        self.close_orphaned_statement(&statement_id, "create_reader (direct)")
                            .await;
                        return Err(e);
                    }
                };
                Ok(DirectOutcome::Completed(ExecuteResult {
                    statement_id: r.statement_id,
                    reader,
                    manifest: r.manifest,
                    status: r.status,
                }))
            }
            // Still running after the inline wait → hand back a running handle;
            // the caller polls (and may cancel) via the statement id.
            StatementOutcome::NotReady(r) => Ok(DirectOutcome::Running(r)),
            // Inline terminal failure/cancel → surface it (close the orphan).
            StatementOutcome::Err(e) => {
                self.close_orphaned_statement(&statement_id, "execute_statement_direct")
                    .await;
                Err(e)
            }
        }
    }

    async fn fetch_completed_result(&self, statement_id: &str) -> Result<Option<ExecuteResult>> {
        let response = self.get_statement_status(statement_id).await?;
        match Self::terminal_outcome(response) {
            StatementOutcome::Done(r) => {
                let reader_factory = self.reader_factory()?;
                let reader = reader_factory.create_reader(&r.statement_id, &r).await?;
                Ok(Some(ExecuteResult {
                    statement_id: r.statement_id,
                    reader,
                    manifest: r.manifest,
                    status: r.status,
                }))
            }
            StatementOutcome::NotReady(_) => Ok(None),
            StatementOutcome::Err(e) => Err(e),
        }
    }

    async fn statement_status(&self, statement_id: &str) -> Result<ExecuteResponse> {
        self.get_statement_status(statement_id).await
    }

    async fn get_result_chunks(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<ChunkLinkFetchResult> {
        let url = self.result_chunks_url(statement_id, chunk_index, row_offset);

        debug!("Getting result chunks at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::GET, &url)
            .build()
            .map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

        let response = self
            .http_client
            .execute(request, RequestType::GetResultChunks)
            .await?;
        let body = response
            .text()
            .await
            .map_err(|e| Error::network_error().message(format!("Failed to read response: {e}")))?;

        let chunks_response: GetChunksResponse = serde_json::from_str(&body).map_err(|e| {
            Error::data_loss().message(format!(
                "Failed to parse chunks response: {e} - body: {body}"
            ))
        })?;

        let result = derive_chunk_fetch_result(chunks_response)?;
        debug!(
            "Chunks response: {} links, next_chunk_index={:?}, next_row_offset={:?}, has_more={}",
            result.links.len(),
            result.next_chunk_index,
            result.next_row_offset,
            result.has_more,
        );
        Ok(result)
    }

    async fn get_inline_chunk(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<InlineChunkFetchResult> {
        // Same endpoint as CloudFetch chunk-link fetching, but on
        // the inline-Arrow path the response carries Arrow IPC bytes
        // in `attachment` instead of `external_links`. `row_offset`
        // is sent unconditionally on the inline path — required
        // from drivers per the bounded SEA API contract, and the
        // ThriftServer `FETCH_ABSOLUTE` cursor needs it to find
        // the right row even when row_offset == 0 (the first
        // chunk of a paginated inline result is still a fetch
        // with row_offset = 0; omitting the parameter would be
        // ambiguous from the server's perspective).
        debug_assert!(
            row_offset >= 0,
            "row_offset must be non-negative on the SEA wire (got {row_offset})",
        );
        let row_offset = row_offset.max(0);
        let url = format!(
            "{}/statements/{}/result/chunks/{}?row_offset={}",
            self.base_url(),
            statement_id,
            chunk_index,
            row_offset,
        );

        debug!(
            "Getting inline chunk {} at {} (row_offset={})",
            chunk_index, url, row_offset
        );

        let request = self
            .http_client
            .inner()
            .request(Method::GET, &url)
            .build()
            .map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

        let response = self
            .http_client
            .execute(request, RequestType::GetResultChunks)
            .await?;
        let body = response
            .text()
            .await
            .map_err(|e| Error::network_error().message(format!("Failed to read response: {e}")))?;

        let chunks_response: GetChunksResponse = serde_json::from_str(&body).map_err(|e| {
            Error::data_loss().message(format!(
                "Failed to parse inline chunk response: {e} - body: {body}"
            ))
        })?;

        // Inline-Arrow path: bytes must be present in `attachment`.
        // If the server returned an external_links response on what
        // we expected to be the inline path, surface a clear error
        // (probably the `disposition = INLINE` on the execute request
        // didn't take effect server-side).
        let bytes = chunks_response.attachment.ok_or_else(|| {
            Error::data_loss().message(format!(
                "Inline chunk {} response missing `attachment` field — \
                 server may have ignored the INLINE disposition and \
                 returned a CloudFetch response. Body: {}",
                chunk_index, body
            ))
        })?;

        // Per the bounded SEA API contract, the chunk metadata we
        // need is on the response itself (top-level, not nested).
        //
        // `chunk_index` and `row_offset`: fall back to the values
        // we sent in the request — the server is supposed to echo
        // them back, but a missing field can be reconstructed from
        // the request without ambiguity.
        //
        // `row_count`: cannot be defaulted. A missing `row_count`
        // on a response that DOES carry an attachment is a server
        // contract violation; defaulting it to 0 would silently
        // corrupt the next-fetch row_offset (we compute
        // `row_offset + row_count` for the continuation). Surface
        // as `data_loss` so the consumer sees a typed error rather
        // than silently-dropped rows.
        let chunk_idx = chunks_response.chunk_index.unwrap_or(chunk_index);
        let returned_row_offset = chunks_response.row_offset.unwrap_or(row_offset);
        let row_count = chunks_response.row_count.ok_or_else(|| {
            Error::data_loss().message(format!(
                "Inline chunk {} response carries `attachment` but no `row_count` — \
                 cannot derive next-fetch row_offset safely. Body: {}",
                chunk_idx, body,
            ))
        })?;
        let next_chunk_index = chunks_response.next_chunk_index;

        debug!(
            "Inline chunk {} response: row_offset={}, row_count={}, next_chunk_index={:?}, bytes={}",
            chunk_idx,
            returned_row_offset,
            row_count,
            next_chunk_index,
            bytes.len(),
        );

        Ok(InlineChunkFetchResult {
            bytes,
            chunk_index: chunk_idx,
            row_offset: returned_row_offset,
            row_count,
            next_chunk_index,
        })
    }

    fn poll_config(&self) -> TransportConfig {
        self.config.clone()
    }

    async fn cancel_statement(&self, statement_id: &str) -> Result<()> {
        let url = format!("{}/statements/{}/cancel", self.base_url(), statement_id);

        debug!("Canceling statement at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::POST, &url)
            .build()
            .map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

        self.http_client
            .execute(request, RequestType::CancelStatement)
            .await?;

        debug!("Canceled statement: {}", statement_id);

        Ok(())
    }

    async fn close_statement(&self, statement_id: &str) -> Result<()> {
        let url = format!("{}/statements/{}", self.base_url(), statement_id);

        debug!("Closing statement at {}", url);

        let request = self
            .http_client
            .inner()
            .request(Method::DELETE, &url)
            .build()
            .map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

        // Surface the wire error so the caller's retry-on-failure path can
        // run — `ExecutedStatement::close` / `ExecutedAsyncStatement::close`
        // only flip their `closed` flag on success, so a swallowed error here
        // would make their store-on-success semantics dead code (and a failed
        // explicit close would look like it succeeded). The HTTP client has
        // already applied the SEA retry policy, so callers see only the
        // post-retry result; the `Drop` fire-and-forget fallback close still
        // runs on the kernel side if the explicit close errored.
        if let Err(e) = self
            .http_client
            .execute(request, RequestType::CloseStatement)
            .await
        {
            crate::klog::warn!(
                statement_id = %statement_id,
                error = %e,
                "close_statement failed after HTTP retries; surfacing to caller (Drop fallback close will run)"
            );
            return Err(e);
        }

        debug!("Closed statement: {}", statement_id);

        Ok(())
    }

    // --- Metadata ---

    async fn list_catalogs(&self, session_id: &str) -> Result<ExecuteResult> {
        let sql = SqlCommandBuilder::new().build_show_catalogs();
        debug!("list_catalogs: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_schemas(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
    ) -> Result<ExecuteResult> {
        let sql = SqlCommandBuilder::new()
            .with_catalog(catalog)
            .with_schema_pattern(schema_pattern)
            .build_show_schemas();
        debug!("list_schemas: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_tables(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        _table_types: Option<&[&str]>,
    ) -> Result<ExecuteResult> {
        // Note: table_types filtering is done client-side after fetching
        let sql = SqlCommandBuilder::new()
            .with_catalog(catalog)
            .with_schema_pattern(schema_pattern)
            .with_table_pattern(table_pattern)
            .build_show_tables();
        debug!("list_tables: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_columns(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        table_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<ExecuteResult> {
        let sql = SqlCommandBuilder::new()
            .with_schema_pattern(schema_pattern)
            .with_table_pattern(table_pattern)
            .with_column_pattern(column_pattern)
            .build_show_columns(catalog);
        debug!("list_columns: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_procedures(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
    ) -> Result<ExecuteResult> {
        let sql =
            SqlCommandBuilder::build_get_procedures(catalog, schema_pattern, procedure_pattern);
        debug!("list_procedures: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_functions(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        function_pattern: Option<&str>,
    ) -> Result<ExecuteResult> {
        let sql = SqlCommandBuilder::build_get_functions(catalog, schema_pattern, function_pattern);
        debug!("list_functions: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_procedure_columns(
        &self,
        session_id: &str,
        catalog: Option<&str>,
        schema_pattern: Option<&str>,
        procedure_pattern: Option<&str>,
        column_pattern: Option<&str>,
    ) -> Result<ExecuteResult> {
        let sql = SqlCommandBuilder::build_get_procedure_columns(
            catalog,
            schema_pattern,
            procedure_pattern,
            column_pattern,
        );
        debug!("list_procedure_columns: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_primary_keys(
        &self,
        session_id: &str,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<ExecuteResult> {
        let sql = SqlCommandBuilder::build_show_primary_keys(catalog, schema, table);
        debug!("list_primary_keys: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    async fn list_foreign_keys(
        &self,
        session_id: &str,
        _parent_catalog: Option<&str>,
        _parent_schema: Option<&str>,
        _parent_table: Option<&str>,
        foreign_catalog: &str,
        foreign_schema: &str,
        foreign_table: &str,
    ) -> Result<ExecuteResult> {
        // ODBC `SQLForeignKeys` and JDBC `getImportedKeys` /
        // `getCrossReference` collapse into one Transport call here:
        // the foreign side identifies the table, the parent side (if
        // provided) is a post-fetch filter on the result's `parent*`
        // columns. Databricks `SHOW FOREIGN KEYS IN ...` is
        // server-side single-sided — it queries by the foreign
        // (FK-bearing) table only and returns the parent linkage as
        // result columns. The kernel passes those columns through
        // unchanged; wrappers narrow on `parentCatalogName` /
        // `parentNamespace` / `parentTableName` if the caller
        // provided a parent-side filter.
        let sql = SqlCommandBuilder::build_show_foreign_keys(
            foreign_catalog,
            foreign_schema,
            foreign_table,
        );
        debug!("list_foreign_keys: {}", sql);
        self.execute_and_read(
            session_id,
            &sql,
            &ExecuteParams::default(),
            RequestType::ExecuteMetadataQuery,
        )
        .await
    }

    fn list_table_types(&self) -> Vec<String> {
        vec![
            "SYSTEM TABLE".to_string(),
            "TABLE".to_string(),
            "VIEW".to_string(),
            "METRIC_VIEW".to_string(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::retry::{build_retry_configs, RetryConfig};
    use crate::client::HttpClientConfig;

    fn create_test_client() -> SeaRestTransport {
        let retry_configs = build_retry_configs(&RetryConfig::default(), &HashMap::new());
        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default(), retry_configs).unwrap(),
        );
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        http_client.set_auth_provider(auth);
        SeaRestTransport::new(
            http_client,
            "https://test.databricks.com",
            "warehouse-123",
            TransportConfig::default(),
        )
    }

    #[test]
    fn test_base_url() {
        let client = create_test_client();
        assert_eq!(client.base_url(), "https://test.databricks.com/api/2.0/sql");
    }

    #[test]
    fn terminal_outcome_failed_envelope_without_statement_id_surfaces_full_error() {
        // A synchronously-rejected request (e.g. a parameter-binding
        // `BAD_REQUEST`) comes back FAILED with no statement_id. The
        // envelope must surface as a SqlError carrying the real server
        // message, parsed SQLSTATE, and the `error_code` slug — NOT an
        // opaque DataLoss parse failure. The slug is deliberately NOT
        // remapped to InvalidArgument: SEA returns `BAD_REQUEST` for
        // genuine SQL errors too (TABLE_OR_VIEW_NOT_FOUND etc.), so it's
        // not a reliable caller-error signal. (The mutually-exclusive
        // param case that motivated this is caught client-side at bind
        // time; see `statement::mutable`.)
        use crate::types::sea::{ServiceError, StatementState, StatementStatus};
        let response = ExecuteResponse {
            statement_id: String::new(),
            status: StatementStatus {
                state: StatementState::Failed,
                error: Some(ServiceError {
                    error_code: Some("BAD_REQUEST".to_string()),
                    message: Some(
                        "[INVALID_PARAMETER_MARKER_VALUE.MISSING_NAME] ... SQLSTATE: 22023"
                            .to_string(),
                    ),
                }),
                sql_state: None,
                num_modified_rows: None,
                display_message: None,
                diagnostic_info: None,
                error_details_json: None,
            },
            manifest: None,
            result: None,
        };
        match SeaRestTransport::terminal_outcome(response) {
            StatementOutcome::Err(e) => {
                assert_eq!(e.code, crate::kernel_error::ErrorCode::SqlError);
                assert_eq!(e.error_code.as_deref(), Some("BAD_REQUEST"));
                assert!(e.message.contains("MISSING_NAME"));
                // SQLSTATE parsed from the message text.
                assert_eq!(e.sql_state.as_deref(), Some("22023"));
            }
            _ => panic!("expected Err outcome for a FAILED response"),
        }
    }

    #[test]
    fn test_base_url_strips_trailing_slash() {
        let retry_configs = build_retry_configs(&RetryConfig::default(), &HashMap::new());
        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default(), retry_configs).unwrap(),
        );
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        http_client.set_auth_provider(auth);
        let client = SeaRestTransport::new(
            http_client,
            "https://test.databricks.com/",
            "warehouse-123",
            TransportConfig::default(),
        );
        assert_eq!(client.base_url(), "https://test.databricks.com/api/2.0/sql");
    }

    #[test]
    fn test_convert_result_data_without_external_links() {
        let result = crate::types::sea::ResultData {
            chunk_index: Some(0),
            row_offset: Some(0),
            row_count: Some(100),
            byte_count: Some(5000),
            next_chunk_index: Some(1),
            next_chunk_internal_link: None,
            external_links: None,
            data_array: None,
            attachment: None,
        };

        let converted = SeaRestTransport::convert_result_data(&result).unwrap();
        assert!(converted.external_links.is_none());
        assert!(converted.inline_arrow_data.is_none());
    }

    #[test]
    fn test_convert_result_data_with_inline_arrow_data() {
        let test_data = vec![1u8, 2, 3, 4, 5];
        let result = crate::types::sea::ResultData {
            chunk_index: Some(0),
            row_offset: Some(0),
            row_count: Some(10),
            byte_count: Some(500),
            next_chunk_index: None,
            next_chunk_internal_link: None,
            external_links: None,
            data_array: None,
            attachment: Some(test_data.clone()),
        };

        let converted = SeaRestTransport::convert_result_data(&result).unwrap();
        assert!(converted.inline_arrow_data.is_some());
        assert_eq!(converted.inline_arrow_data.unwrap(), test_data);
    }

    #[test]
    fn test_list_table_types() {
        let client = create_test_client();
        let types = client.list_table_types();
        assert_eq!(types, vec!["SYSTEM TABLE", "TABLE", "VIEW", "METRIC_VIEW"]);
    }

    // ──────────────────────────────────────────────────────────────
    // `derive_chunk_fetch_result` — guards the `.last()`-vs-`.first()`
    // continuation derivation. The bug this protects against:
    // reading `links.first().next_chunk_index` would return the
    // wrong continuation pointer when SEA returns a multi-link
    // batch. Today's SEA chunk-fetch typically returns single-link
    // responses (so the bug was latent), but a future regression
    // to `.first()` would silently truncate or rewind the cursor.
    // ──────────────────────────────────────────────────────────────

    fn test_external_link(
        chunk_index: i64,
        row_offset: i64,
        row_count: i64,
        next_chunk_index: Option<i64>,
    ) -> crate::types::sea::ExternalLink {
        crate::types::sea::ExternalLink {
            external_link: format!("https://storage.example.com/chunk{}", chunk_index),
            expiration: "2099-12-31T23:59:59Z".to_string(),
            chunk_index,
            row_offset,
            row_count,
            byte_count: row_count * 100,
            http_headers: None,
            next_chunk_index,
        }
    }

    #[test]
    fn test_derive_chunk_fetch_result_multi_link_uses_last_for_continuation() {
        // The critical regression guard: response carries 3 links
        // where the *first* link has a `next_chunk_index` (mid-stream)
        // but the *last* link has `None` (end-of-stream). The
        // derived `has_more` must be `false` (matching the last
        // link's signal), and `next_row_offset` must come from
        // the last link's `row_offset + row_count`.
        let response = crate::types::sea::GetChunksResponse {
            statement_id: None,
            chunk_index: None,
            row_offset: None,
            row_count: None,
            // Top-level next_chunk_index absent — forces fallback
            // to per-link inspection.
            next_chunk_index: None,
            external_links: Some(vec![
                test_external_link(0, 0, 1000, Some(1)),
                test_external_link(1, 1000, 1000, Some(2)),
                // Last link signals end-of-stream.
                test_external_link(2, 2000, 1000, None),
            ]),
            attachment: None,
        };

        let result = derive_chunk_fetch_result(response).unwrap();

        assert_eq!(result.links.len(), 3);
        assert!(
            !result.has_more,
            "last-link `next_chunk_index = None` must surface as has_more=false; \
             a regression to `links.first()` would set has_more=true here",
        );
        assert!(result.next_chunk_index.is_none());
        assert_eq!(
            result.next_row_offset, None,
            "next_row_offset is gated by has_more — must be None at EOS",
        );
    }

    #[test]
    fn test_derive_chunk_fetch_result_multi_link_mid_stream_uses_last() {
        // Mid-stream multi-link response: last link points to
        // chunk N. Derived `next_chunk_index` and `next_row_offset`
        // come from the *last* link, not the first.
        let response = crate::types::sea::GetChunksResponse {
            statement_id: None,
            chunk_index: None,
            row_offset: None,
            row_count: None,
            next_chunk_index: None,
            external_links: Some(vec![
                test_external_link(5, 5000, 1000, Some(6)),
                test_external_link(6, 6000, 1000, Some(7)),
                // Last link's next_chunk_index = 8 (and row_offset
                // + row_count = 7000 + 500 = 7500). A `.first()`
                // reader would report next=6 and offset=6000 — wrong.
                test_external_link(7, 7000, 500, Some(8)),
            ]),
            attachment: None,
        };

        let result = derive_chunk_fetch_result(response).unwrap();

        assert!(result.has_more);
        assert_eq!(
            result.next_chunk_index,
            Some(8),
            "must use links.last().next_chunk_index, not links.first()",
        );
        assert_eq!(
            result.next_row_offset,
            Some(7500),
            "next_row_offset must come from last link's row_offset + row_count",
        );
    }

    #[test]
    fn test_derive_chunk_fetch_result_top_level_next_chunk_wins() {
        // When the top-level `next_chunk_index` is present, it
        // takes precedence over per-link signals. (Same precedence
        // rule the production code follows.)
        let response = crate::types::sea::GetChunksResponse {
            statement_id: None,
            chunk_index: None,
            row_offset: None,
            row_count: None,
            next_chunk_index: Some(99),
            external_links: Some(vec![test_external_link(5, 5000, 1000, Some(6))]),
            attachment: None,
        };

        let result = derive_chunk_fetch_result(response).unwrap();
        assert_eq!(
            result.next_chunk_index,
            Some(99),
            "top-level next_chunk_index must win over per-link",
        );
    }

    #[test]
    fn test_derive_chunk_fetch_result_empty_response_no_more() {
        // No links + no top-level signal → end-of-stream.
        let response = crate::types::sea::GetChunksResponse {
            statement_id: None,
            chunk_index: None,
            row_offset: None,
            row_count: None,
            next_chunk_index: None,
            external_links: None,
            attachment: None,
        };

        let result = derive_chunk_fetch_result(response).unwrap();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
        assert!(result.next_chunk_index.is_none());
        assert!(result.next_row_offset.is_none());
    }

    #[test]
    fn result_chunks_url_omits_row_offset_when_zero() {
        let client = create_test_client();
        // CloudFetch path: row_offset=0 is the kernel default and must
        // produce the legacy URL shape (no `?row_offset=...`).
        assert_eq!(
            client.result_chunks_url("01abc", 7, 0),
            "https://test.databricks.com/api/2.0/sql/statements/01abc/result/chunks/7"
        );
    }

    // Note: negative-row_offset behaviour is now gated by a
    // `debug_assert!` in `result_chunks_url`. Debug builds (which is
    // what `cargo test` runs by default) will panic on
    // `row_offset < 0` so an upstream bug surfaces immediately
    // during development. In release builds the assertion is
    // compiled out and the URL builder silently omits the query
    // parameter (treats negative the same as zero); intentional, so
    // a stray negative in production can't produce a malformed URL.

    #[test]
    fn result_chunks_url_includes_row_offset_when_positive() {
        // Inline-Arrow path: row_offset>0 is appended as a query
        // parameter so SEA can FETCH_ABSOLUTE into ThriftServer at
        // the right rowOffset.
        let client = create_test_client();
        assert_eq!(
            client.result_chunks_url("01abc", 3, 12345),
            "https://test.databricks.com/api/2.0/sql/statements/01abc/result/chunks/3?row_offset=12345"
        );
    }

    #[test]
    fn result_chunks_url_appends_row_offset_at_inclusivity_edge() {
        // Branch boundary is `row_offset > 0`. Test `row_offset = 1`
        // explicitly so a refactor that swaps `>` to `>=` (which
        // would still match `12345` and `i64::MAX`) is caught.
        let client = create_test_client();
        assert_eq!(
            client.result_chunks_url("01abc", 3, 1),
            "https://test.databricks.com/api/2.0/sql/statements/01abc/result/chunks/3?row_offset=1"
        );
    }

    #[test]
    fn result_chunks_url_serialises_i64_max_row_offset() {
        // Pathological: i64::MAX is a valid i64 and serialises as
        // its decimal form. The kernel does not pre-validate
        // row-position semantics; the server rejects nonsensical
        // offsets at request-time. The URL builder must not panic
        // or truncate for the legal range.
        let client = create_test_client();
        let url = client.result_chunks_url("01abc", 0, i64::MAX);
        assert!(
            url.ends_with(&format!("?row_offset={}", i64::MAX)),
            "i64::MAX must serialise as decimal: got {url}",
        );
    }

    /// Build a `SeaRestTransport` pointed at a wiremock-controlled
    /// host so we can assert wire-level behaviour rather than just
    /// URL-string output.
    fn create_test_client_against(host: &str) -> SeaRestTransport {
        let retry_configs = build_retry_configs(&RetryConfig::default(), &HashMap::new());
        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default(), retry_configs).unwrap(),
        );
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        http_client.set_auth_provider(auth);
        SeaRestTransport::new(
            http_client,
            host,
            "warehouse-123",
            TransportConfig::default(),
        )
    }

    #[tokio::test]
    async fn get_result_chunks_sends_row_offset_query_param_on_wire() {
        // Pin the wire shape of the inline-Arrow row_offset query
        // parameter against accidental refactors. The unit-test
        // assertions on `result_chunks_url` would all pass even if
        // a refactor short-circuited the helper call from
        // `get_result_chunks` (e.g. shadowing `row_offset` with
        // `0`). Driving through the public `Transport` trait
        // catches that.
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Server contract: `/result/chunks/{idx}?row_offset=N` returns
        // an inline-Arrow result chunk page. Empty external_links is
        // fine — the test only cares about the request shape.
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/01abc/result/chunks/3"))
            .and(query_param("row_offset", "100"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"external_links":[],"next_chunk_index":null}"#),
            )
            .expect(1)
            .mount(&server)
            .await;

        let transport = create_test_client_against(&server.uri());
        let _ = transport
            .get_result_chunks("01abc", 3, 100)
            .await
            .expect("get_result_chunks must succeed against the mock");

        // The `expect(1)` on the Mock + the matcher on
        // `query_param("row_offset", "100")` together assert that
        // exactly one request landed with that exact wire shape.
        // wiremock's drop-time check verifies the expectation; we
        // also assert it explicitly so a failure surfaces in this
        // test's panic message rather than wiremock's.
        server.verify().await;
    }

    #[tokio::test]
    async fn get_result_chunks_omits_row_offset_when_zero_on_wire() {
        // Mirror of the above: when the caller passes
        // `row_offset = 0`, the wire URL must NOT carry the
        // `?row_offset=...` query parameter. Confirms the omission
        // contract end-to-end, not just in URL formatting.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Mount a permissive mock: matches the path but inspects
        // the recorded request afterwards to assert there's no
        // `row_offset` query parameter.
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/01abc/result/chunks/3"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"external_links":[],"next_chunk_index":null}"#),
            )
            .expect(1)
            .mount(&server)
            .await;

        let transport = create_test_client_against(&server.uri());
        let _ = transport
            .get_result_chunks("01abc", 3, 0)
            .await
            .expect("get_result_chunks must succeed against the mock");

        let recorded = server.received_requests().await.unwrap();
        let req = recorded
            .iter()
            .find(|r| r.url.path().ends_with("/result/chunks/3"))
            .expect("chunk-fetch request must be recorded");
        assert!(
            req.url.query().is_none() || !req.url.query().unwrap().contains("row_offset"),
            "row_offset=0 must NOT appear on the wire; got query: {:?}",
            req.url.query(),
        );
    }
}
