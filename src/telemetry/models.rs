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

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TelemetryRequest {
    pub upload_time: i64,
    pub proto_logs: Vec<String>,
}

impl TelemetryRequest {
    pub(crate) fn from_logs(logs: &[TelemetryFrontendLog]) -> Result<Self, serde_json::Error> {
        let proto_logs = logs
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            upload_time: unix_time_millis(),
            proto_logs,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct TelemetryFrontendLog {
    pub workspace_id: i64,
    pub frontend_log_event_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<FrontendLogContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry: Option<FrontendLogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FrontendLogContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_context: Option<TelemetryClientContext>,
    pub timestamp_millis: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TelemetryClientContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FrontendLogEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_driver_log: Option<OssSqlDriverTelemetryLog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct OssSqlDriverTelemetryLog {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_statement_id: Option<String>,
    /// Shared with the session and every other event it emits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_configuration: Option<Arc<DriverSystemConfiguration>>,
    /// Shared with the session and every other event it emits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_connection_params: Option<Arc<DriverConnectionParameters>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vol_operation: Option<VolumeOperationEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_operation: Option<SqlExecutionEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_info: Option<DriverErrorInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_latency_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DriverSystemConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_vendor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_arch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_app_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locale_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub char_set_encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct DriverConnectionParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<DriverMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_info: Option<HostDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_proxy: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_mech: Option<DriverAuthMech>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_flow: Option<DriverAuthFlow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_system_proxy: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_proxy_hosts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_cf_proxy: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_host_info: Option<HostDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cf_proxy_host_info: Option<HostDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_mode_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_empty_metadata: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_many_parameters: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_trust_store_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_certificate_revocation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accept_undetermined_certificate_revocation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub google_service_account: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub google_credential_file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_connection_pool_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_sea_hybrid_results: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_complex_datatype_support: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_self_signed_support: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_system_trust_store: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_fetched_per_block: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure_workspace_resource_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure_tenant_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_column_length: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_token_cache: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_arrow: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_direct_results: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_jwt_assertion: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_key_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_algorithm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_timeout: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_volume_ingestion_paths: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub async_poll_interval_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_tags: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_metric_view_metadata: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_commit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_auto_commit_from_server: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_geospatial_support: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HostDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_auth_type: Option<DriverProxyAuth>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DriverErrorInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VolumeOperationEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_operation_type: Option<VolumeOperationType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_file: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct SqlExecutionEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_type: Option<StatementType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_compressed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_result: Option<ExecutionResultFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_details: Option<ChunkDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_latency: Option<ResultLatency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_detail: Option<OperationDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub java_uses_patched_arrow: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct OperationDetail {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n_operation_status_calls: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_status_latency_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_type: Option<OperationType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_internal_call: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResultLatency {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_set_ready_latency_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_set_consumption_latency_millis: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChunkDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_chunk_latency_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slowest_chunk_latency_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_chunks_present: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_chunks_iterated: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sum_chunks_download_time_millis: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DriverMode {
    TypeUnspecified,
    Thrift,
    Sea,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum StatementType {
    TypeUnspecified,
    Query,
    Sql,
    Update,
    Metadata,
    Volume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OperationType {
    TypeUnspecified,
    CreateSession,
    DeleteSession,
    ExecuteStatement,
    ExecuteStatementAsync,
    CloseStatement,
    CancelStatement,
    ListTypeInfo,
    ListCatalogs,
    ListSchemas,
    ListTables,
    ListTableTypes,
    ListColumns,
    ListFunctions,
    ListPrimaryKeys,
    ListImportedKeys,
    ListExportedKeys,
    ListCrossReferences,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ExecutionResultFormat {
    FormatUnspecified,
    InlineArrow,
    InlineJson,
    ExternalLinks,
    ColumnarInline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum VolumeOperationType {
    TypeUnspecified,
    Put,
    Get,
    Delete,
    List,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DriverProxyAuth {
    AuthUnspecified,
    None,
    Basic,
    Spnego,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DriverAuthMech {
    TypeUnspecified,
    Other,
    Pat,
    Oauth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DriverAuthFlow {
    TypeUnspecified,
    TokenPassthrough,
    ClientCredentials,
    BrowserBasedAuthentication,
}

pub(super) fn unix_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::exporter::TelemetryExporter;
    use crate::telemetry::test_support::CapturingTelemetryExporter;
    use serde_json::Value;

    #[test]
    fn telemetry_request_serializes_with_backend_shape() {
        let request = TelemetryRequest {
            upload_time: 42,
            proto_logs: vec!["{\"workspace_id\":1}".to_string()],
        };

        let json = serde_json::to_value(&request).unwrap();

        assert_eq!(json["uploadTime"], 42);
        assert_eq!(json["protoLogs"][0], "{\"workspace_id\":1}");
        assert!(json.get("upload_time").is_none());
        assert!(json.get("proto_logs").is_none());
    }

    #[test]
    fn telemetry_frontend_log_serializes_with_snake_case_fields() {
        let log = sample_frontend_log();

        let json = serde_json::to_value(&log).unwrap();

        assert_eq!(json["workspace_id"], 12345);
        assert_eq!(json["frontend_log_event_id"], "event-1");
        assert_eq!(json["context"]["timestamp_millis"], 1000);
        assert_eq!(
            json["context"]["client_context"]["user_agent"],
            "databricks-sql-kernel/0.2.1"
        );
        assert!(json.get("workspaceId").is_none());
        assert!(json.get("frontendLogEventId").is_none());
    }

    #[test]
    fn telemetry_request_from_logs_serializes_frontend_logs_as_strings() {
        let request = TelemetryRequest::from_logs(&[sample_frontend_log()]).unwrap();

        assert_eq!(request.proto_logs.len(), 1);
        let nested: Value = serde_json::from_str(&request.proto_logs[0]).unwrap();

        assert_eq!(nested["workspace_id"], 12345);
        assert_eq!(nested["entry"]["sql_driver_log"]["session_id"], "session-1");
        assert!(request.upload_time > 0);
    }

    #[test]
    fn oss_sql_driver_telemetry_log_has_proto_field_names() {
        let log = OssSqlDriverTelemetryLog {
            session_id: Some("session-1".to_string()),
            sql_statement_id: Some("statement-1".to_string()),
            system_configuration: Some(Arc::new(DriverSystemConfiguration {
                driver_version: Some("0.2.1".to_string()),
                runtime_name: Some("rust".to_string()),
                runtime_version: Some("1.0".to_string()),
                runtime_vendor: Some("rust-lang".to_string()),
                os_name: Some("linux".to_string()),
                os_version: Some("6.0".to_string()),
                os_arch: Some("x86_64".to_string()),
                driver_name: Some("Databricks SQL Kernel".to_string()),
                client_app_name: Some("test-app".to_string()),
                locale_name: Some("en_US".to_string()),
                char_set_encoding: Some("UTF-8".to_string()),
                process_name: Some("cargo".to_string()),
            })),
            driver_connection_params: Some(Arc::new(DriverConnectionParameters {
                http_path: Some("/sql/1.0/warehouses/abc".to_string()),
                mode: Some(DriverMode::Sea),
                host_info: Some(HostDetails {
                    host_url: Some("https://example.databricks.com".to_string()),
                    port: Some(443),
                    proxy_auth_type: Some(DriverProxyAuth::None),
                }),
                auth_mech: Some(DriverAuthMech::Pat),
                auth_flow: None,
                enable_direct_results: Some(true),
                async_poll_interval_millis: Some(1000),
                auto_commit: Some(true),
                ..Default::default()
            })),
            auth_type: Some("pat".to_string()),
            vol_operation: Some(VolumeOperationEvent {
                volume_operation_type: Some(VolumeOperationType::List),
                volume_path: Some("/Volumes/catalog/schema/volume".to_string()),
                local_file: None,
            }),
            sql_operation: Some(SqlExecutionEvent {
                statement_type: Some(StatementType::Sql),
                execution_result: Some(ExecutionResultFormat::ExternalLinks),
                operation_detail: Some(OperationDetail {
                    n_operation_status_calls: Some(2),
                    operation_status_latency_millis: Some(10),
                    operation_type: Some(OperationType::ExecuteStatement),
                    is_internal_call: Some(false),
                }),
                result_latency: Some(ResultLatency {
                    result_set_ready_latency_millis: Some(5),
                    result_set_consumption_latency_millis: Some(20),
                }),
                chunk_details: Some(ChunkDetails {
                    initial_chunk_latency_millis: Some(1),
                    slowest_chunk_latency_millis: Some(2),
                    total_chunks_present: Some(3),
                    total_chunks_iterated: Some(3),
                    sum_chunks_download_time_millis: Some(6),
                }),
                ..Default::default()
            }),
            error_info: Some(DriverErrorInfo {
                error_name: Some("InvalidState".to_string()),
                stack_trace: Some("sanitized".to_string()),
            }),
            operation_latency_ms: Some(25),
        };

        let json = serde_json::to_value(&log).unwrap();

        for field in [
            "session_id",
            "sql_statement_id",
            "system_configuration",
            "driver_connection_params",
            "auth_type",
            "vol_operation",
            "sql_operation",
            "error_info",
            "operation_latency_ms",
        ] {
            assert!(json.get(field).is_some(), "missing field {field}");
        }
        assert_eq!(json["driver_connection_params"]["mode"], "SEA");
        assert_eq!(json["driver_connection_params"]["auth_mech"], "PAT");
        assert_eq!(json["sql_operation"]["execution_result"], "EXTERNAL_LINKS");
    }

    #[tokio::test]
    async fn capturing_exporter_records_requests() {
        let exporter = CapturingTelemetryExporter::default();
        let request = TelemetryRequest {
            upload_time: 42,
            proto_logs: vec!["{}".to_string()],
        };

        exporter.export(request.clone()).await.unwrap();

        assert_eq!(exporter.requests(), vec![request]);
    }

    fn sample_frontend_log() -> TelemetryFrontendLog {
        TelemetryFrontendLog {
            workspace_id: 12345,
            frontend_log_event_id: "event-1".to_string(),
            context: Some(FrontendLogContext {
                client_context: Some(TelemetryClientContext {
                    user_agent: Some("databricks-sql-kernel/0.2.1".to_string()),
                }),
                timestamp_millis: 1000,
            }),
            entry: Some(FrontendLogEntry {
                sql_driver_log: Some(OssSqlDriverTelemetryLog {
                    session_id: Some("session-1".to_string()),
                    operation_latency_ms: Some(10),
                    ..Default::default()
                }),
            }),
        }
    }
}
