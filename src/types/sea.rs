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

//! SEA (Statement Execution API) request/response types.
//!
//! These types map directly to the JSON structures used by the Databricks
//! SQL Statement Execution API. They are primarily used by `SeaRestTransport`.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Response from statement execution or status polling.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementExecutionResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    #[serde(default)]
    pub manifest: Option<ResultManifest>,
    #[serde(default)]
    pub result: Option<ResultData>,
}

/// Status of a statement execution.
///
/// `#[non_exhaustive]` — external Rust callers can read fields directly
/// but cannot use a struct literal. Adding new wire-shape fields stays
/// non-breaking for downstream crates (in-crate construction is
/// unaffected).
//
// TODO(pr23-followup): add a `progress` field once the SEA server
// actually emits one. Empirically verified against pecotesting on
// 2026-05-27 — a 37s heavy TPC-H query against a SQL warehouse, polled
// 7 times with `getProgressUpdate=true` on the Thrift path, returned
// `progressUpdateResponse = null` on every poll. Adding the field to
// the kernel before the wire supports it ships an Option<...> that's
// always None, locks us into a shape (`f64`) that doesn't match
// Thrift's `TProgressUpdateResp` struct (headerNames / rows /
// footerSummary / progressedPercentage), and creates user-visible
// API churn when the server finally does grow progress support. Hold
// off until then.
#[non_exhaustive]
#[derive(Clone, Deserialize)]
pub struct StatementStatus {
    pub state: StatementState,
    #[serde(default)]
    pub error: Option<ServiceError>,
    /// Number of rows modified by the statement (UPDATE / INSERT /
    /// DELETE / MERGE). Absent on SELECT and on warehouses that don't
    /// surface the counter.
    #[serde(default)]
    pub num_modified_rows: Option<i64>,
    /// Server-supplied user-facing message (e.g. "Statement was
    /// canceled by the user" or progress hints). Distinct from
    /// `error.message`; populated on non-error states too.
    ///
    /// **PII / sensitive-data note:** server-supplied free-form text
    /// may include fragments of the original SQL or parameter values.
    /// Wrappers forwarding this to centralised logs should redact or
    /// truncate per their own PII policy.
    #[serde(default)]
    pub display_message: Option<String>,
    /// Server-supplied diagnostic detail — multi-line text with
    /// stack frames or operator context. Intended for log / support
    /// surfaces, not user-facing strings.
    ///
    /// **PII / sensitive-data note:** like `display_message`, this is
    /// server-supplied free-form text and may include SQL fragments
    /// or values. Redact before centralised logging.
    #[serde(default)]
    pub diagnostic_info: Option<String>,
    /// Server-supplied JSON blob with extended error details.
    /// Pass-through string — wrappers parse if they need structured
    /// access.
    ///
    /// **Server-side gating:** populated only when the workspace has
    /// `spark.databricks.sql.errorDetailsJson.enabled = true` set on
    /// the underlying SQL cluster. The flag is declared in
    /// `DatabricksSQLConf.scala` as `internal()` with
    /// `createWithDefault(false)`, so it is NOT settable from a
    /// client session-configuration request (verified empirically on
    /// 2026-05-27: pecotesting returns `[CONFIG_NOT_AVAILABLE]
    /// SQLSTATE: 42K0I` when the client attempts to SET it). When the
    /// flag is off the field is `None` even on terminal-error
    /// statements that otherwise populate `display_message` /
    /// `diagnostic_info`. When populated, the content format is
    /// `{"errorClass": "...", "messageTemplate": "..."}`.
    ///
    /// **Unbounded:** the server can return a multi-MB blob. Wrappers
    /// surfacing this should bound size before logging.
    #[serde(default)]
    pub error_details_json: Option<String>,
}

impl std::fmt::Debug for StatementStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Truncate unbounded server-supplied free-form strings so a
        // `?status` in a tracing span doesn't dump multi-MB blobs.
        // Presence + length are the load-bearing signal; the content
        // itself goes through dedicated accessors on `ExecutedStatement`.
        fn summarise(s: &Option<String>) -> Option<String> {
            s.as_ref().map(|v| format!("<{} chars>", v.len()))
        }
        f.debug_struct("StatementStatus")
            .field("state", &self.state)
            .field("error", &self.error)
            .field("num_modified_rows", &self.num_modified_rows)
            .field("display_message", &summarise(&self.display_message))
            .field("diagnostic_info", &summarise(&self.diagnostic_info))
            .field("error_details_json", &summarise(&self.error_details_json))
            .finish()
    }
}

/// Possible states of a statement during execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Closed,
}

/// Error information from the service.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceError {
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    /// SQL state code if provided by server (typically in the message, but may be a separate field)
    #[serde(default)]
    pub sql_state: Option<String>,
}

/// Manifest describing the result set structure.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultManifest {
    pub format: String,
    pub schema: ResultSchema,
    #[serde(default)]
    pub total_chunk_count: Option<i64>,
    #[serde(default)]
    pub total_row_count: Option<i64>,
    #[serde(default)]
    pub total_byte_count: Option<i64>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub chunks: Option<Vec<ChunkInfo>>,
    /// Compression codec used for result data ("LZ4_FRAME" or absent for none)
    #[serde(default)]
    pub result_compression: Option<String>,
}

/// Schema of the result set.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultSchema {
    pub column_count: i32,
    #[serde(default)]
    pub columns: Vec<ColumnInfo>,
}

/// Information about a single column in the result.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub type_text: String,
    pub position: i32,
    #[serde(default)]
    pub type_precision: Option<i64>,
    #[serde(default)]
    pub type_scale: Option<i64>,
    #[serde(default)]
    pub type_interval_type: Option<String>,
}

/// Information about a result chunk (metadata only).
///
/// `byte_count` is `Option<i64>` because the bounded SEA API contract
/// for OSS drivers marks the field as deprecated ("Driver to stop
/// using these soon"). New server versions may omit it on a
/// per-`ChunkInfo` basis; relying on it being a required `i64` would
/// fail deserialisation the moment a partial `chunks` array shipped
/// without the field.
///
/// `chunk_index` / `row_offset` / `row_count` stay required: the
/// bounded API keeps them populated, and they are the load-bearing
/// fields for chunk continuation.
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkInfo {
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    #[serde(default)]
    pub byte_count: Option<i64>,
}

/// Result data from a chunk fetch or initial execution response.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultData {
    #[serde(default)]
    pub chunk_index: Option<i64>,
    #[serde(default)]
    pub row_offset: Option<i64>,
    #[serde(default)]
    pub row_count: Option<i64>,
    #[serde(default)]
    pub byte_count: Option<i64>,
    #[serde(default)]
    pub next_chunk_index: Option<i64>,
    #[serde(default)]
    pub next_chunk_internal_link: Option<String>,
    #[serde(default)]
    pub external_links: Option<Vec<ExternalLink>>,
    /// Inline data for small results in JSON format (not used with Arrow)
    #[serde(default)]
    pub data_array: Option<Vec<Vec<String>>>,
    /// Inline Arrow IPC data (base64-encoded in JSON, decoded by serde).
    /// Present when server returns inline results instead of CloudFetch.
    #[serde(default, deserialize_with = "deserialize_base64_attachment")]
    pub attachment: Option<Vec<u8>>,
}

/// Deserialize base64-encoded attachment field from JSON.
fn deserialize_base64_attachment<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(s) if !s.is_empty() => STANDARD
            .decode(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        _ => Ok(None),
    }
}

/// External link for CloudFetch download.
#[derive(Debug, Clone, Deserialize)]
pub struct ExternalLink {
    pub external_link: String,
    pub expiration: String, // ISO 8601 timestamp
    pub chunk_index: i64,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
    #[serde(default)]
    pub http_headers: Option<HashMap<String, String>>,
    #[serde(default)]
    pub next_chunk_index: Option<i64>,
}

/// A bound parameter for a parameterized SEA statement execution.
///
/// Exactly one of `name` or `ordinal` should be set. `name` is the
/// public, spec-required binding form (canonical proto marks
/// `StatementParameter.name` as `openapi_required=true`); `ordinal`
/// is the `PUBLIC_UNDOCUMENTED` positional form. Server accepts both.
#[derive(Debug, Clone, Serialize)]
pub struct StatementParameter {
    /// Named parameter identifier. When `Some`, the SQL uses
    /// `:name`-style markers and `ordinal` should be `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// 1-based position of the parameter in the SQL statement. When
    /// `Some`, the SQL uses `?` markers and `name` should be `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<i32>,
    /// Databricks SQL type name (e.g., "INT", "STRING", "DOUBLE", "TIMESTAMP").
    #[serde(rename = "type")]
    pub param_type: String,
    /// String representation of the parameter value. None for SQL NULL.
    pub value: Option<String>,
}

/// Request body for statement execution.
#[derive(Debug, Clone, Serialize)]
pub struct ExecuteStatementRequest {
    pub warehouse_id: String,
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub disposition: String, // "EXTERNAL_LINKS" for CloudFetch
    pub format: String,      // "ARROW_STREAM"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_wait_timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<i64>,
    /// Server-side compression for external-link result chunks. Set to
    /// `Some("LZ4_FRAME")` so the cloud-fetch payload is LZ4-compressed
    /// (matches Python/JDBC SEA default). Without it the server returns
    /// uncompressed arrow bytes, doubling network transfer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_compression: Option<String>,
    /// Positional parameters for parameterized queries.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parameters: Vec<StatementParameter>,
}

/// Parameters for statement execution (passed to client methods).
#[derive(Debug, Clone, Default)]
pub struct ExecuteParams {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub wait_timeout: Option<String>,
    pub on_wait_timeout: Option<String>, // "CONTINUE" or "CANCEL"
    pub row_limit: Option<i64>,
    /// Parameters for parameterized query execution.
    pub parameters: Vec<StatementParameter>,
}

/// Compression codec for result data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionCodec {
    #[default]
    None,
    Lz4Frame,
}

impl CompressionCodec {
    /// Parse compression codec from manifest field value.
    pub fn from_manifest(value: Option<&str>) -> Self {
        match value {
            Some("LZ4_FRAME") => Self::Lz4Frame,
            _ => Self::None,
        }
    }
}

/// Request body for session creation.
#[derive(Debug, Clone, Serialize)]
pub struct CreateSessionRequest {
    pub warehouse_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub session_confs: HashMap<String, String>,
}

/// Response from session creation.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateSessionResponse {
    pub session_id: String,
}

/// Response from fetching result chunks.
/// Note: The /result/chunks/{chunk_index} endpoint returns a different format
/// than the main execute response - it doesn't include statement_id.
#[derive(Debug, Clone, Deserialize)]
pub struct GetChunksResponse {
    #[serde(default)]
    pub statement_id: Option<String>,
    #[serde(default)]
    pub chunk_index: Option<i64>,
    #[serde(default)]
    pub row_offset: Option<i64>,
    #[serde(default)]
    pub row_count: Option<i64>,
    #[serde(default)]
    pub next_chunk_index: Option<i64>,
    #[serde(default)]
    pub external_links: Option<Vec<ExternalLink>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_codec_from_manifest() {
        assert_eq!(
            CompressionCodec::from_manifest(Some("LZ4_FRAME")),
            CompressionCodec::Lz4Frame
        );
        assert_eq!(
            CompressionCodec::from_manifest(Some("UNKNOWN")),
            CompressionCodec::None
        );
        assert_eq!(
            CompressionCodec::from_manifest(None),
            CompressionCodec::None
        );
    }

    #[test]
    fn test_statement_state_deserialization() {
        let json = r#""SUCCEEDED""#;
        let state: StatementState = serde_json::from_str(json).unwrap();
        assert_eq!(state, StatementState::Succeeded);

        let json = r#""PENDING""#;
        let state: StatementState = serde_json::from_str(json).unwrap();
        assert_eq!(state, StatementState::Pending);
    }

    #[test]
    fn test_create_session_request_uses_session_confs_wire_name() {
        // SEA v2 deserializer expects `session_confs` (proto field 4).
        // Any other name (e.g. `session_configuration`) is silently
        // dropped and the session opens with default confs.
        let mut confs = HashMap::new();
        confs.insert("USE_CACHED_RESULT".to_string(), "false".to_string());
        let req = CreateSessionRequest {
            warehouse_id: "wh-1".to_string(),
            catalog: None,
            schema: None,
            session_confs: confs,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(
            json.contains("\"session_confs\""),
            "expected `session_confs` on the wire, got: {json}"
        );
    }

    #[test]
    fn test_execute_statement_request_serialization() {
        let req = ExecuteStatementRequest {
            warehouse_id: "abc123".to_string(),
            statement: "SELECT 1".to_string(),
            session_id: Some("session-1".to_string()),
            catalog: None,
            schema: None,
            disposition: "EXTERNAL_LINKS".to_string(),
            format: "ARROW_STREAM".to_string(),
            wait_timeout: Some("30s".to_string()),
            on_wait_timeout: None,
            row_limit: None,
            result_compression: None,
            parameters: vec![],
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"warehouse_id\":\"abc123\""));
        assert!(json.contains("\"session_id\":\"session-1\""));
        assert!(!json.contains("\"catalog\"")); // None should be skipped
    }

    #[test]
    fn test_statement_execution_response_deserialization() {
        let json = r#"{
            "statement_id": "stmt-123",
            "status": {
                "state": "SUCCEEDED"
            },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": 1,
                    "columns": [{"name": "id", "type_name": "INT", "type_text": "INT", "position": 0}]
                },
                "total_chunk_count": 1,
                "total_row_count": 100
            }
        }"#;

        let response: StatementExecutionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.statement_id, "stmt-123");
        assert_eq!(response.status.state, StatementState::Succeeded);
        assert!(response.manifest.is_some());
        let manifest = response.manifest.unwrap();
        assert_eq!(manifest.total_row_count, Some(100));
    }

    #[test]
    fn test_chunk_info_byte_count_optional() {
        // Per the bounded SEA API contract, `byte_count` on
        // ChunkInfo is deprecated and the server may stop emitting
        // it. Verify deserialisation succeeds when the field is
        // absent — without #[serde(default)] this would fail
        // with "missing field `byte_count`".
        let json = r#"{
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": 1000
        }"#;

        let info: ChunkInfo = serde_json::from_str(json).expect("byte_count must be optional");
        assert_eq!(info.chunk_index, 0);
        assert_eq!(info.row_count, 1000);
        assert!(info.byte_count.is_none());

        // Also verify present-and-populated still works.
        let json = r#"{
            "chunk_index": 1,
            "row_offset": 1000,
            "row_count": 500,
            "byte_count": 12345
        }"#;
        let info: ChunkInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.byte_count, Some(12345));
    }

    #[test]
    fn test_external_link_deserialization() {
        let json = r#"{
            "external_link": "https://storage.example.com/chunk0",
            "expiration": "2024-01-01T12:00:00Z",
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": 1000,
            "byte_count": 50000,
            "http_headers": {"x-custom": "value"},
            "next_chunk_index": 1
        }"#;

        let link: ExternalLink = serde_json::from_str(json).unwrap();
        assert_eq!(link.chunk_index, 0);
        assert_eq!(link.next_chunk_index, Some(1));
        assert!(link.http_headers.is_some());
        assert_eq!(
            link.http_headers.unwrap().get("x-custom"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn test_result_data_with_base64_attachment() {
        // "Hello, World!" in base64
        let json = r#"{
            "chunk_index": 0,
            "row_count": 10,
            "attachment": "SGVsbG8sIFdvcmxkIQ=="
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert_eq!(result.chunk_index, Some(0));
        assert!(result.attachment.is_some());
        assert_eq!(result.attachment.unwrap(), b"Hello, World!");
    }

    #[test]
    fn test_result_data_without_attachment() {
        let json = r#"{
            "chunk_index": 0,
            "row_count": 10
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert!(result.attachment.is_none());
    }

    #[test]
    fn test_result_data_with_empty_attachment() {
        let json = r#"{
            "chunk_index": 0,
            "attachment": ""
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert!(result.attachment.is_none());
    }

    #[test]
    fn test_result_data_with_null_attachment() {
        let json = r#"{
            "chunk_index": 0,
            "attachment": null
        }"#;

        let result: ResultData = serde_json::from_str(json).unwrap();
        assert!(result.attachment.is_none());
    }

    #[test]
    fn test_column_info_with_all_optional_fields() {
        let json = r#"{
            "name": "amount",
            "type_name": "DECIMAL",
            "type_text": "DECIMAL(10,2)",
            "position": 0,
            "type_precision": 10,
            "type_scale": 2
        }"#;

        let col: ColumnInfo = serde_json::from_str(json).unwrap();
        assert_eq!(col.name, "amount");
        assert_eq!(col.type_name, "DECIMAL");
        assert_eq!(col.type_text, "DECIMAL(10,2)");
        assert_eq!(col.position, 0);
        assert_eq!(col.type_precision, Some(10));
        assert_eq!(col.type_scale, Some(2));
        assert_eq!(col.type_interval_type, None);
    }

    #[test]
    fn test_column_info_without_optional_fields() {
        let json = r#"{
            "name": "id",
            "type_name": "INT",
            "type_text": "INT",
            "position": 0
        }"#;

        let col: ColumnInfo = serde_json::from_str(json).unwrap();
        assert_eq!(col.name, "id");
        assert_eq!(col.type_precision, None);
        assert_eq!(col.type_scale, None);
        assert_eq!(col.type_interval_type, None);
    }

    #[test]
    fn test_ddl_response_with_empty_schema() {
        // DDL statements (CREATE TABLE, DROP TABLE) return column_count: 0
        // with the columns field omitted entirely.
        let json = r#"{
            "statement_id": "01f12850-3b6a-123f-8e77-57d8d2fd960a",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {"column_count": 0},
                "total_chunk_count": 0,
                "total_row_count": 0,
                "total_byte_count": 0,
                "truncated": false
            },
            "result": {}
        }"#;

        let response: StatementExecutionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            response.statement_id,
            "01f12850-3b6a-123f-8e77-57d8d2fd960a"
        );
        assert_eq!(response.status.state, StatementState::Succeeded);
        let manifest = response.manifest.unwrap();
        assert_eq!(manifest.schema.column_count, 0);
        assert!(manifest.schema.columns.is_empty());
        assert_eq!(manifest.total_row_count, Some(0));
    }

    #[test]
    fn test_statement_status_with_extended_fields() {
        // SEA response with the extended status fields populated. The
        // wire field `progress` was empirically absent on the SQL
        // warehouse path (see the field-level TODO in `StatementStatus`)
        // and has been dropped from the kernel until a server-side
        // shape is settled.
        let json = r#"{
            "statement_id": "stmt-extended",
            "status": {
                "state": "SUCCEEDED",
                "num_modified_rows": 42,
                "display_message": "Statement completed",
                "diagnostic_info": "operator=Scan; rows=42",
                "error_details_json": "{\"trace\":\"...\"}"
            }
        }"#;

        let response: StatementExecutionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status.state, StatementState::Succeeded);
        assert_eq!(response.status.num_modified_rows, Some(42));
        assert_eq!(
            response.status.display_message,
            Some("Statement completed".to_string())
        );
        assert_eq!(
            response.status.diagnostic_info,
            Some("operator=Scan; rows=42".to_string())
        );
        assert_eq!(
            response.status.error_details_json,
            Some(r#"{"trace":"..."}"#.to_string())
        );
    }

    #[test]
    fn test_statement_status_extended_fields_absent() {
        // Minimal SEA response — extended fields are optional and
        // default to None when the server omits them.
        let json = r#"{
            "statement_id": "stmt-min",
            "status": { "state": "SUCCEEDED" }
        }"#;

        let response: StatementExecutionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status.num_modified_rows, None);
        assert_eq!(response.status.display_message, None);
        assert_eq!(response.status.diagnostic_info, None);
        assert_eq!(response.status.error_details_json, None);
    }

    #[test]
    fn test_statement_status_ignores_unknown_progress_field() {
        // Forward-compat: if the SEA server starts emitting a
        // `progress` field, the kernel must not break — it's just an
        // unknown JSON key today and `serde(default)` on the modelled
        // fields means we ignore it. Pin this behaviour so a future
        // strict-mode bump can't silently break the kernel.
        let json = r#"{
            "statement_id": "stmt-progress",
            "status": {
                "state": "RUNNING",
                "progress": 0.5
            }
        }"#;
        let response: StatementExecutionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status.state, StatementState::Running);
    }

    #[test]
    fn test_column_info_with_interval_type() {
        let json = r#"{
            "name": "duration",
            "type_name": "INTERVAL",
            "type_text": "INTERVAL YEAR TO MONTH",
            "position": 0,
            "type_interval_type": "YEAR_MONTH"
        }"#;

        let col: ColumnInfo = serde_json::from_str(json).unwrap();
        assert_eq!(col.type_interval_type, Some("YEAR_MONTH".to_string()));
    }
}
