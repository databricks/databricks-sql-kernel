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

use super::exporter::{TelemetryExportError, TelemetryExportResult, TelemetryExporter};
use super::models::TelemetryRequest;
use crate::client::retry::RequestType;
use crate::client::DatabricksHttpClient;
use crate::kernel_error::Error as KernelError;
use reqwest::header::{HeaderValue, CONTENT_TYPE};
use reqwest::{Body, Method, Request, Url};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

const AUTHENTICATED_ENDPOINT: &str = "/telemetry-ext";
const UNAUTHENTICATED_ENDPOINT: &str = "/telemetry-unauth";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TelemetryEndpoint {
    Authenticated,
    Unauthenticated,
}

impl TelemetryEndpoint {
    fn path(self) -> &'static str {
        match self {
            Self::Authenticated => AUTHENTICATED_ENDPOINT,
            Self::Unauthenticated => UNAUTHENTICATED_ENDPOINT,
        }
    }
}

pub(super) struct DatabricksTelemetryExporter {
    http_client: Arc<DatabricksHttpClient>,
    endpoint_url: String,
}

impl DatabricksTelemetryExporter {
    pub(super) fn new(
        host: &str,
        http_client: Arc<DatabricksHttpClient>,
        endpoint: TelemetryEndpoint,
    ) -> Self {
        Self {
            http_client,
            endpoint_url: endpoint_url(host, endpoint),
        }
    }
}

impl TelemetryExporter for DatabricksTelemetryExporter {
    fn export<'a>(
        &'a self,
        request: TelemetryRequest,
    ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
        Box::pin(async move {
            let request = build_export_request(&self.endpoint_url, &request)?;

            self.http_client
                .execute(request, RequestType::TelemetryExport)
                .await
                .map(|_| ())
                .map_err(export_error)
        })
    }
}

fn export_error(error: KernelError) -> TelemetryExportError {
    match error.http_status {
        Some(429 | 503) => TelemetryExportError::Throttled(error.to_string()),
        _ => TelemetryExportError::Export(error.to_string()),
    }
}

fn build_export_request(
    endpoint_url: &str,
    request: &TelemetryRequest,
) -> Result<Request, TelemetryExportError> {
    let url = Url::parse(endpoint_url).map_err(|e| TelemetryExportError::Export(e.to_string()))?;
    let body = serde_json::to_vec(request)?;
    let mut request = Request::new(Method::POST, url);
    request
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    *request.body_mut() = Some(Body::from(body));
    Ok(request)
}

fn endpoint_url(host: &str, endpoint: TelemetryEndpoint) -> String {
    let host = host.trim_end_matches('/');
    let host = if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("https://{host}")
    };
    format!("{}{}", host, endpoint.path())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn endpoint_url_normalizes_host_and_endpoint() {
        assert_eq!(
            endpoint_url(
                "example.cloud.databricks.com/",
                TelemetryEndpoint::Authenticated
            ),
            "https://example.cloud.databricks.com/telemetry-ext"
        );
        assert_eq!(
            endpoint_url("http://localhost:8080", TelemetryEndpoint::Unauthenticated),
            "http://localhost:8080/telemetry-unauth"
        );
    }

    #[test]
    fn rate_limit_statuses_classify_as_throttled() {
        for status in [429, 503] {
            assert!(matches!(
                export_error(KernelError::from_http_status(status)),
                TelemetryExportError::Throttled(_)
            ));
        }
        for status in [400, 500] {
            assert!(matches!(
                export_error(KernelError::from_http_status(status)),
                TelemetryExportError::Export(_)
            ));
        }
        assert!(matches!(
            export_error(KernelError::network_error()),
            TelemetryExportError::Export(_)
        ));
    }

    /// Export through a real `DatabricksHttpClient` (with its retry loop)
    /// against a mock `/telemetry-ext` that always answers `status`.
    async fn export_against(status: u16, overall_timeout: Duration) -> TelemetryExportResult {
        use crate::auth::PersonalAccessToken;
        use crate::client::retry::RetryConfigOverrides;
        use crate::client::retry::{build_retry_configs, RequestCategory, RetryConfig};
        use crate::client::HttpClientConfig;
        use std::collections::HashMap;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/telemetry-ext"))
            .respond_with(ResponseTemplate::new(status).insert_header("Retry-After", "1"))
            .mount(&server)
            .await;
        let overrides = HashMap::from([(
            RequestCategory::Telemetry,
            RetryConfigOverrides {
                min_wait: Some(Duration::from_millis(10)),
                max_wait: Some(Duration::from_millis(10)),
                overall_timeout: Some(overall_timeout),
                max_retries: Some(2),
                ..Default::default()
            },
        )]);
        let client = DatabricksHttpClient::new(
            HttpClientConfig::default(),
            build_retry_configs(&RetryConfig::default(), &overrides),
        )
        .unwrap();
        client.set_auth_provider(Arc::new(PersonalAccessToken::new("token".to_string())));
        let exporter = DatabricksTelemetryExporter::new(
            &server.uri(),
            Arc::new(client),
            TelemetryEndpoint::Authenticated,
        );
        exporter
            .export(TelemetryRequest {
                upload_time: 1,
                proto_logs: vec!["{}".to_string()],
            })
            .await
    }

    #[tokio::test]
    async fn rate_limited_export_through_retry_loop_is_throttled() {
        for status in [429, 503] {
            // Retries exhausted, and retry budget exceeded before a retry.
            for budget in [Duration::from_secs(10), Duration::from_millis(1)] {
                let result = export_against(status, budget).await;
                assert!(
                    matches!(result, Err(TelemetryExportError::Throttled(_))),
                    "HTTP {status}, budget {budget:?}: {result:?}"
                );
            }
        }
        let result = export_against(500, Duration::from_secs(10)).await;
        assert!(
            matches!(result, Err(TelemetryExportError::Export(_))),
            "{result:?}"
        );
    }

    #[test]
    fn build_export_request_sets_method_url_json_body_and_content_type() {
        let telemetry_request = TelemetryRequest {
            upload_time: 42,
            proto_logs: vec!["{}".to_string()],
        };

        let request = build_export_request("https://example.com/telemetry-ext", &telemetry_request)
            .expect("valid export request");

        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.url().as_str(), "https://example.com/telemetry-ext");
        assert_eq!(
            request.headers().get(CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
        assert_eq!(
            request.body().and_then(|body| body.as_bytes()),
            Some(br#"{"uploadTime":42,"protoLogs":["{}"]}"#.as_slice())
        );
    }
}
