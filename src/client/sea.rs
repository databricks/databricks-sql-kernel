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
    ChunkLinkFetchResult, DatabricksHttpClient, ExecuteResponse, ExecuteResult, ExecuteResultData,
    SessionInfo, Transport, TransportConfig,
};
use crate::kernel_error::{Error, Result};
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
use tracing::debug;

/// Internal terminal-outcome for a single SEA statement response.
///
/// Used by the shared decoder [`SeaRestTransport::terminal_outcome`]
/// so the sync polling loop and the async one-shot
/// [`Transport::fetch_completed_result`] path produce identical
/// outcomes (and identical errors) for `FAILED` / `CANCELLED` /
/// `CLOSED-no-data` responses.
enum StatementOutcome {
    /// `Succeeded` (or `Closed` with inline data).
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

        Self::convert_response(sea_response)
    }

    /// Decode a single SEA `ExecuteResponse` into a terminal outcome.
    ///
    /// Shared by `wait_for_completion` (sync poll loop) and
    /// [`Transport::fetch_completed_result`] (async one-shot fetch)
    /// so both paths produce identical errors for `FAILED` /
    /// `CANCELLED` / `CLOSED-no-data`.
    fn terminal_outcome(response: ExecuteResponse) -> StatementOutcome {
        match response.status.state {
            StatementState::Succeeded => StatementOutcome::Done(response),
            StatementState::Failed => {
                let service_error = response.status.error.as_ref();
                let error_msg = service_error
                    .and_then(|e| e.message.clone())
                    .unwrap_or_else(|| "Unknown error".to_string());

                // Server-reported execution error → SqlError with the
                // structured fields populated from the envelope.
                let mut error = Error::sql_error()
                    .message(&error_msg)
                    .with_query_id(&response.statement_id);

                if let Some(err) = service_error {
                    // SQLSTATE: prefer the structured field; fall back
                    // to parsing the message ("SQLSTATE: XXXXX" pattern).
                    let sqlstate = err
                        .sql_state
                        .as_ref()
                        .filter(|s| s.len() == 5)
                        .cloned()
                        .or_else(|| crate::kernel_error::extract_sqlstate_from_message(&error_msg));
                    if let Some(s) = sqlstate {
                        error = error.with_sql_state(s);
                    }

                    // Databricks-specific error code slug
                    // (`STATEMENT_NOT_FOUND`, `WAREHOUSE_STOPPED`, …).
                    // Wrappers branch on this to map host-language
                    // exception taxonomies.
                    if let Some(ec) = err.error_code.as_deref().filter(|s| !s.is_empty()) {
                        error = error.with_error_code(ec);
                    }
                }

                StatementOutcome::Err(error)
            }
            StatementState::Canceled => {
                StatementOutcome::Err(Error::invalid_state().message("Statement was canceled"))
            }
            StatementState::Closed => {
                // Closed with result data is valid for inline results
                // — the server delivers the data and immediately
                // closes the statement since no further fetching is
                // needed.
                if response.result.is_some() {
                    StatementOutcome::Done(response)
                } else {
                    StatementOutcome::Err(Error::invalid_state().message("Statement was closed"))
                }
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
                    debug!("Polling statement status: {}", r.statement_id);
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

        let request_body = ExecuteStatementRequest {
            warehouse_id: self.warehouse_id.clone(),
            statement: sql.to_string(),
            session_id: Some(session_id.to_string()),
            catalog: params.catalog.clone(),
            schema: params.schema.clone(),
            // TODO: INLINE_OR_EXTERNAL_LINKS is not a public API disposition but enables
            // returning all chunk links in a single response (requires JDBC user agent).
            // Once EXTERNAL_LINKS supports multi-link responses, switch back to it.
            disposition: "INLINE_OR_EXTERNAL_LINKS".to_string(),
            format: "ARROW_STREAM".to_string(),
            wait_timeout: params.wait_timeout.clone(),
            on_wait_timeout: params.on_wait_timeout.clone(),
            row_limit: params.row_limit,
            parameters: params.parameters.clone(),
        };

        debug!("Executing statement at {}: {}", url, sql);

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
        let response = self
            .call_execute_api(session_id, sql, params, request_type)
            .await?;
        let response = self.wait_for_completion(response).await?;
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
        let reader = reader_factory.create_reader(&response.statement_id, &response)?;

        // `server_state` carries the terminal state forward so the
        // executed handle can skip a redundant CloseStatement when
        // the server already cleaned up (state=Closed).
        let server_state = response.status.state;
        Ok(ExecuteResult {
            statement_id: response.statement_id,
            reader,
            manifest: response.manifest,
            server_state,
        })
    }
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
            session_configuration: session_config,
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
            tracing::warn!(
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

    async fn fetch_completed_result(&self, statement_id: &str) -> Result<Option<ExecuteResult>> {
        let response = self.get_statement_status(statement_id).await?;
        match Self::terminal_outcome(response) {
            StatementOutcome::Done(r) => {
                let reader_factory = self.reader_factory()?;
                let reader = reader_factory.create_reader(&r.statement_id, &r)?;
                let server_state = r.status.state;
                Ok(Some(ExecuteResult {
                    statement_id: r.statement_id,
                    reader,
                    manifest: r.manifest,
                    server_state,
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
        _row_offset: i64, // Ignored - SEA uses chunk_index
    ) -> Result<ChunkLinkFetchResult> {
        // The chunk_index is a path parameter, not a query parameter
        // See: https://docs.databricks.com/api/workspace/statementexecution
        let url = format!(
            "{}/statements/{}/result/chunks/{}",
            self.base_url(),
            statement_id,
            chunk_index
        );

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

        // Convert external links to CloudFetchLinks
        let links = if let Some(ref external_links) = chunks_response.external_links {
            external_links
                .iter()
                .map(CloudFetchLink::from_external_link)
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };

        // The next_chunk_index may be:
        // 1. At the top level of the response, OR
        // 2. Inside the external_link (each link knows its next chunk)
        // Prefer top-level, fall back to link-level
        let next_chunk_index = chunks_response.next_chunk_index.or_else(|| {
            chunks_response
                .external_links
                .as_ref()
                .and_then(|links| links.first())
                .and_then(|link| link.next_chunk_index)
        });

        let has_more = next_chunk_index.is_some();

        debug!(
            "Chunks response: {} links, next_chunk_index={:?}, has_more={}",
            links.len(),
            next_chunk_index,
            has_more
        );

        Ok(ChunkLinkFetchResult {
            links,
            has_more,
            next_chunk_index,
            next_row_offset: None, // SEA doesn't use row offset
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

        // Ignore errors on statement close (best effort cleanup)
        let _ = self
            .http_client
            .execute(request, RequestType::CloseStatement)
            .await;

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
}
