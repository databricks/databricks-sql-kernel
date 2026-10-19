//! Low-level gRPC wire client for the kernel-scoped SEA API.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::codegen::{http, Service};
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status};

use crate::client::grpc_proto as proto;
use crate::client::http::DatabricksHttpClient;
use crate::kernel_error::{Error, Result};
use crate::types::sea;

const EXTERNAL_GRPC_PATH_PREFIX: &str = "/grpc/sql-exec-api";

#[derive(Clone)]
struct ExternalGrpcPath<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for ExternalGrpcPath<S>
where
    S: Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let path = format!("{EXTERNAL_GRPC_PATH_PREFIX}{}", request.uri().path());
        let mut parts = request.uri().clone().into_parts();
        parts.path_and_query = Some(
            path.parse()
                .expect("fixed external gRPC prefix and generated RPC path form a valid URI"),
        );
        *request.uri_mut() = http::Uri::from_parts(parts)
            .expect("replacing only a valid URI's path preserves a valid URI");
        self.inner.call(request)
    }
}

#[derive(Clone)]
pub(crate) struct SeaGrpcClient {
    client: proto::sql_execution_api_service_client::SqlExecutionApiServiceClient<
        ExternalGrpcPath<Channel>,
    >,
    http_client: Arc<DatabricksHttpClient>,
    warehouse_id: String,
}

impl std::fmt::Debug for SeaGrpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeaGrpcClient")
            .field("warehouse_id", &self.warehouse_id)
            .finish_non_exhaustive()
    }
}

impl SeaGrpcClient {
    pub(crate) fn new(
        host: &str,
        warehouse_id: String,
        http_client: Arc<DatabricksHttpClient>,
    ) -> Result<Self> {
        let endpoint = Endpoint::from_shared(host.trim_end_matches('/').to_string())
            .map_err(|e| Error::invalid_argument().message(format!("Invalid gRPC endpoint: {e}")))?
            .connect_timeout(http_client.config().connect_timeout)
            .timeout(http_client.config().read_timeout)
            .user_agent(http_client.config().user_agent.clone())
            .map_err(|e| {
                Error::invalid_argument().message(format!("Invalid gRPC user agent: {e}"))
            })?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| {
                Error::invalid_argument().message(format!("Invalid gRPC TLS configuration: {e}"))
            })?;
        Ok(Self {
            client: proto::sql_execution_api_service_client::SqlExecutionApiServiceClient::new(
                ExternalGrpcPath {
                    inner: endpoint.connect_lazy(),
                },
            ),
            http_client,
            warehouse_id,
        })
    }

    fn request<T>(&self, message: T, extra_headers: &[(&str, &str)]) -> Result<Request<T>> {
        let mut request = Request::new(message);
        let metadata = request.metadata_mut();

        insert_metadata(metadata, "authorization", &self.http_client.auth_header()?)?;
        if let Some(org_id) = &self.http_client.config().org_id {
            insert_metadata(metadata, "x-databricks-org-id", org_id)?;
        }
        for (name, value) in &self.http_client.config().custom_headers {
            insert_metadata(metadata, name, value)?;
        }
        for (name, value) in self.http_client.auth_extra_headers()? {
            insert_metadata(metadata, &name, &value)?;
        }
        for (name, value) in extra_headers {
            insert_metadata(metadata, name, value)?;
        }
        Ok(request)
    }

    pub(crate) async fn create_session(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        session_confs: HashMap<String, String>,
    ) -> Result<proto::CreateSessionResponse> {
        let request = proto::CreateSessionRequestV2 {
            warehouse_id: Some(self.warehouse_id.clone()),
            catalog: catalog.map(str::to_owned),
            schema: schema.map(str::to_owned),
            session_confs,
            execution_mode: Some(proto::SessionExecutionMode::Fast as i32),
        };
        let mut client = self.client.clone();
        client
            .create_session_v2(self.request(request, &[])?)
            .await
            .map(|r| r.into_inner())
            .map_err(map_status)
    }

    pub(crate) async fn terminate_session(&self, session_id: &str) -> Result<()> {
        let request = proto::TerminateSessionRequest {
            session_id: Some(session_id.to_owned()),
            warehouse_id: Some(self.warehouse_id.clone()),
        };
        let mut client = self.client.clone();
        client
            .terminate_session(self.request(request, &[])?)
            .await
            .map(|_| ())
            .map_err(map_status)
    }

    pub(crate) async fn execute_statement(
        &self,
        session_id: &str,
        session_version: Option<i64>,
        sql: &str,
        params: &sea::ExecuteParams,
        cloudfetch_enabled: bool,
        extra_headers: &[(&str, &str)],
    ) -> Result<sea::StatementExecutionResponse> {
        let request = proto::ExecuteStatementRequest {
            statement: Some(sql.to_owned()),
            warehouse_id: Some(self.warehouse_id.clone()),
            catalog: params.catalog.clone(),
            schema: params.schema.clone(),
            row_limit: params.row_limit,
            format: Some(proto::Format::ArrowStream as i32),
            disposition: Some(if cloudfetch_enabled {
                proto::FetchDisposition::InlineOrExternalLinks as i32
            } else {
                proto::FetchDisposition::Inline as i32
            }),
            wait_timeout: params.wait_timeout.clone(),
            on_wait_timeout: params
                .on_wait_timeout
                .as_deref()
                .map(|v| if v == "CANCEL" { proto::TimeoutAction::Cancel } else { proto::TimeoutAction::Continue } as i32),
            session_id: Some(session_id.to_owned()),
            parameters: params.parameters.iter().map(|p| proto::StatementParameter {
                ordinal: p.ordinal,
                name: p.name.clone(),
                value: p.value.clone(),
                r#type: Some(p.param_type.clone()),
            }).collect(),
            result_compression: Some(proto::CompressionCodec::Lz4Frame as i32),
            query_tags: params.query_tags.iter().map(|t| proto::QueryTag {
                key: Some(t.key.clone()),
                value: t.value.clone(),
            }).collect(),
            session_version: session_version.map(|version_id| proto::SessionVersion { version_id: Some(version_id) }),
        };
        let mut client = self.client.clone();
        let response = client
            .execute_statement(self.request(request, extra_headers)?)
            .await
            .map_err(map_status)?
            .into_inner();
        statement_response(response)
    }

    pub(crate) async fn get_statement_result(
        &self,
        statement_id: &str,
    ) -> Result<sea::StatementExecutionResponse> {
        let request = proto::GetStatementResultRequest {
            statement_id: Some(statement_id.to_owned()),
            warehouse_id: Some(self.warehouse_id.clone()),
            session_id: None,
        };
        let mut client = self.client.clone();
        let response = client
            .get_statement_result(self.request(request, &[])?)
            .await
            .map_err(map_status)?
            .into_inner();
        statement_response(response)
    }

    pub(crate) async fn get_result_data(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<sea::GetChunksResponse> {
        let chunk_index = i32::try_from(chunk_index).map_err(|_| {
            Error::invalid_argument().message(format!("chunk index {chunk_index} exceeds i32"))
        })?;
        let request = proto::GetResultDataRequest {
            statement_id: Some(statement_id.to_owned()),
            chunk_index: Some(chunk_index),
            row_offset: Some(row_offset),
            warehouse_id: Some(self.warehouse_id.clone()),
            session_id: None,
        };
        let mut client = self.client.clone();
        let result = client
            .get_result_data(self.request(request, &[])?)
            .await
            .map_err(map_status)?
            .into_inner();
        chunks_response(result)
    }

    pub(crate) async fn cancel_statement(&self, statement_id: &str) -> Result<()> {
        let request = proto::CancelStatementRequest {
            statement_id: Some(statement_id.to_owned()),
        };
        let mut client = self.client.clone();
        client
            .cancel_statement(self.request(request, &[])?)
            .await
            .map(|_| ())
            .map_err(map_status)
    }

    pub(crate) async fn close_statement(&self, statement_id: &str) -> Result<()> {
        let request = proto::CloseStatementRequest {
            statement_id: Some(statement_id.to_owned()),
        };
        let mut client = self.client.clone();
        client
            .close_statement(self.request(request, &[])?)
            .await
            .map(|_| ())
            .map_err(map_status)
    }
}

fn insert_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    name: &str,
    value: &str,
) -> Result<()> {
    let key =
        MetadataKey::<Ascii>::from_bytes(name.to_ascii_lowercase().as_bytes()).map_err(|e| {
            Error::invalid_argument().message(format!("Invalid gRPC metadata name {name:?}: {e}"))
        })?;
    let value = MetadataValue::try_from(value).map_err(|e| {
        Error::invalid_argument().message(format!("Invalid gRPC metadata value for {name:?}: {e}"))
    })?;
    metadata.insert(key, value);
    Ok(())
}

fn map_status(status: Status) -> Error {
    let message = format!("gRPC {}: {}", status.code(), status.message());
    match status.code() {
        Code::Unauthenticated | Code::PermissionDenied => Error::unauthenticated().message(message),
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            Error::invalid_argument().message(message)
        }
        Code::NotFound => Error::not_found().message(message),
        Code::DeadlineExceeded => Error::timeout().message(message),
        Code::Cancelled => Error::cancelled().message(message),
        _ => Error::network_error().message(message),
    }
}

fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| {
        Error::data_loss().message(format!("gRPC response missing required field {field}"))
    })
}

fn statement_response(
    response: proto::StatementResponse,
) -> Result<sea::StatementExecutionResponse> {
    Ok(sea::StatementExecutionResponse {
        statement_id: response.statement_id.unwrap_or_default(),
        status: statement_status(required(response.status, "status")?)?,
        manifest: response.manifest.map(result_manifest).transpose()?,
        result: response.result.map(result_data).transpose()?,
    })
}

fn statement_status(status: proto::StatementStatus) -> Result<sea::StatementStatus> {
    let state = match required(status.state, "status.state")? {
        x if x == proto::statement_status::State::Pending as i32 => sea::StatementState::Pending,
        x if x == proto::statement_status::State::Running as i32 => sea::StatementState::Running,
        x if x == proto::statement_status::State::Succeeded as i32 => {
            sea::StatementState::Succeeded
        }
        x if x == proto::statement_status::State::Failed as i32 => sea::StatementState::Failed,
        x if x == proto::statement_status::State::Canceled as i32 => sea::StatementState::Canceled,
        x if x == proto::statement_status::State::Closed as i32 => sea::StatementState::Closed,
        value => {
            return Err(Error::data_loss().message(format!("Unknown gRPC statement state {value}")))
        }
    };
    Ok(sea::StatementStatus {
        state,
        error: status.error.map(|e| sea::ServiceError {
            error_code: e.error_code.map(service_error_code),
            message: e.message,
            sql_state: None,
        }),
        sql_state: status.sql_state,
        session_version: status.session_version.map(|v| sea::SessionVersion {
            version_id: v.version_id.unwrap_or_default(),
        }),
        num_modified_rows: None,
        display_message: None,
        diagnostic_info: None,
        error_details_json: None,
    })
}

fn service_error_code(value: i32) -> String {
    proto::ErrorCode::try_from(value)
        .map(|code| code.as_str_name().to_owned())
        .unwrap_or_else(|_| value.to_string())
}

fn result_manifest(manifest: proto::ResultManifest) -> Result<sea::ResultManifest> {
    let format = proto::Format::try_from(required(manifest.format, "manifest.format")?)
        .map_err(|_| Error::data_loss().message("Unknown gRPC result format"))?
        .as_str_name()
        .to_owned();
    let schema = required(manifest.schema, "manifest.schema")?;
    Ok(sea::ResultManifest {
        format,
        schema: sea::ResultSchema {
            column_count: required(schema.column_count, "manifest.schema.column_count")?,
            columns: schema
                .columns
                .into_iter()
                .map(column_info)
                .collect::<Result<_>>()?,
        },
        arrow_schema: manifest.arrow_schema,
        total_chunk_count: manifest.total_chunk_count.map(i64::from),
        total_row_count: manifest.total_row_count,
        total_byte_count: manifest.total_byte_count,
        truncated: manifest.truncated.unwrap_or(false),
        chunks: Some(
            manifest
                .chunks
                .into_iter()
                .map(chunk_info)
                .collect::<Result<_>>()?,
        ),
        result_compression: manifest
            .result_compression
            .and_then(|v| proto::CompressionCodec::try_from(v).ok())
            .map(|v| v.as_str_name().to_owned()),
        is_native_metadata_result: manifest.is_native_metadata_result,
    })
}

fn column_info(column: proto::ColumnInfo) -> Result<sea::ColumnInfo> {
    let type_name =
        proto::ColumnTypeName::try_from(required(column.type_name, "column.type_name")?)
            .map_err(|_| Error::data_loss().message("Unknown gRPC column type"))?
            .as_str_name()
            .to_owned();
    Ok(sea::ColumnInfo {
        name: required(column.name, "column.name")?,
        type_name,
        type_text: required(column.type_text, "column.type_text")?,
        position: required(column.position, "column.position")?,
        type_precision: column.type_precision.map(i64::from),
        type_scale: column.type_scale.map(i64::from),
        type_interval_type: column.type_interval_type,
        type_json: column.type_json,
    })
}

fn chunk_info(info: proto::ChunkInfo) -> Result<sea::ChunkInfo> {
    Ok(sea::ChunkInfo {
        chunk_index: i64::from(required(info.chunk_index, "chunk.info.chunk_index")?),
        row_offset: required(info.row_offset, "chunk.info.row_offset")?,
        row_count: required(info.row_count, "chunk.info.row_count")?,
        byte_count: info.byte_count,
    })
}

fn result_data(result: proto::ResultData) -> Result<sea::ResultData> {
    let chunk = result.chunk;
    let info = chunk.as_ref().and_then(|v| v.info.as_ref());
    Ok(sea::ResultData {
        chunk_index: info.and_then(|v| v.chunk_index).map(i64::from),
        row_offset: info.and_then(|v| v.row_offset),
        row_count: info.and_then(|v| v.row_count),
        byte_count: info.and_then(|v| v.byte_count),
        next_chunk_index: info.and_then(|v| v.next_chunk_index).map(i64::from),
        next_chunk_internal_link: info.and_then(|v| v.next_chunk_internal_link.clone()),
        external_links: (!result.external_links.is_empty())
            .then(|| {
                result
                    .external_links
                    .into_iter()
                    .map(external_link)
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?,
        data_array: None,
        attachment: chunk.and_then(|v| v.attachment),
    })
}

fn external_link(link: proto::ExternalLink) -> Result<sea::ExternalLink> {
    let info = required(link.info, "external_link.info")?;
    Ok(sea::ExternalLink {
        external_link: required(link.external_link, "external_link.external_link")?,
        expiration: required(link.expiration, "external_link.expiration")?,
        chunk_index: i64::from(required(
            info.chunk_index,
            "external_link.info.chunk_index",
        )?),
        row_offset: required(info.row_offset, "external_link.info.row_offset")?,
        row_count: required(info.row_count, "external_link.info.row_count")?,
        byte_count: info.byte_count.unwrap_or_default(),
        http_headers: (!link.http_headers.is_empty()).then_some(link.http_headers),
        next_chunk_index: info.next_chunk_index.map(i64::from),
    })
}

fn chunks_response(result: proto::ResultData) -> Result<sea::GetChunksResponse> {
    let converted = result_data(result)?;
    Ok(sea::GetChunksResponse {
        statement_id: None,
        chunk_index: converted.chunk_index,
        row_offset: converted.row_offset,
        row_count: converted.row_count,
        next_chunk_index: converted.next_chunk_index,
        external_links: converted.external_links,
        attachment: converted.attachment,
    })
}

#[cfg(test)]
mod tests {
    use super::service_error_code;

    #[test]
    fn service_error_code_uses_canonical_name() {
        assert_eq!(service_error_code(4), "BAD_REQUEST");
    }

    #[test]
    fn service_error_code_preserves_unknown_value() {
        assert_eq!(service_error_code(123_456), "123456");
    }
}
