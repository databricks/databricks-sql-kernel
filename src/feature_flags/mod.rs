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

//! Fail-open feature flags supplied by connector-service.
//!
//! The resolver reuses the kernel's authenticated HTTP client and shares one
//! process-wide cache across sessions. Cache values remain the raw JSON text
//! returned by connector-service, partitioned by workspace. Consumers select
//! one of the six SAFE types through a typed getter; the kernel parses and
//! validates the JSON before returning a native value.

mod cache;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize};

use crate::client::DatabricksHttpClient;
use crate::kernel_error::{Error, Result};
use crate::klog::warn;

use cache::{global_cache, response_ttl, CacheLookup, FeatureFlagCache, WorkspaceKey};

#[cfg(test)]
use crate::kernel_error::ErrorCode;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Driver family understood by the connector-service feature-flag endpoint.
///
/// The wire values intentionally mirror
/// `GetFeatureFlagsRequest.ClientType` in connector-service.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientType {
    Go,
    NodeJs,
    Odbc,
    Python,
}

impl ClientType {
    const fn as_endpoint_segment(self) -> &'static str {
        match self {
            Self::Go => "GOLANG",
            Self::NodeJs => "NODEJS",
            Self::Odbc => "ODBC",
            Self::Python => "PYTHON",
        }
    }
}

impl fmt::Display for ClientType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_endpoint_segment())
    }
}

/// Identity used when evaluating driver feature flags.
///
/// The type and version are sent to connector-service. They are process-level
/// properties of the outer driver and therefore do not participate in the
/// workspace cache key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientIdentity {
    client_type: ClientType,
    version: String,
}

impl ClientIdentity {
    pub fn new(client_type: ClientType, version: impl Into<String>) -> Self {
        let version = version.into();
        Self {
            client_type,
            version: version.trim().to_owned(),
        }
    }

    pub fn client_type(&self) -> ClientType {
        self.client_type
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

/// One cached connector-service response. This is process-cache storage, not a
/// session snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FeatureFlagValues {
    values: Arc<HashMap<String, String>>,
}

impl FeatureFlagValues {
    fn from_entries(entries: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            values: Arc::new(entries.into_iter().collect()),
        }
    }

    fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Resolves allowlisted driver feature flags through connector-service.
///
/// Resolution always fails open: request and response failures are logged and
/// produce defaults (or the last successfully cached values).
#[derive(Clone)]
pub(crate) struct FeatureFlagResolver {
    host: String,
    identity: ClientIdentity,
    workspace_key: WorkspaceKey,
    http_client: Arc<DatabricksHttpClient>,
    cache: Arc<FeatureFlagCache>,
}

impl fmt::Debug for FeatureFlagResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureFlagResolver")
            // Do not recursively render DatabricksHttpClient: its custom
            // headers may contain caller-supplied secrets.
            .field("host", &self.host)
            .field("client_type", &self.identity.client_type())
            .field("workspace_key", &self.workspace_key)
            .finish_non_exhaustive()
    }
}

impl FeatureFlagResolver {
    pub(crate) fn new(
        host: impl Into<String>,
        http_client: Arc<DatabricksHttpClient>,
        identity: ClientIdentity,
    ) -> Self {
        Self::with_cache(host, http_client, identity, global_cache())
    }

    fn with_cache(
        host: impl Into<String>,
        http_client: Arc<DatabricksHttpClient>,
        identity: ClientIdentity,
        cache: Arc<FeatureFlagCache>,
    ) -> Self {
        let host = canonical_host(host.into());
        let workspace_key = WorkspaceKey::new(&host, http_client.config().org_id.clone());
        Self {
            host,
            identity,
            workspace_key,
            http_client,
            cache,
        }
    }

    async fn resolve(&self) -> FeatureFlagValues {
        if self.identity.version().trim().is_empty() {
            return FeatureFlagValues::default();
        }

        let key = self.workspace_key.clone();

        match self.cache.lookup(&key) {
            CacheLookup::Fresh(values) => values,
            CacheLookup::Wait(receiver) => {
                match tokio::time::timeout(self.request_timeout(), receiver).await {
                    Ok(Ok(values)) => values,
                    // A waiter must respect its own connection timeout rather
                    // than inherit a slower leader's network settings.
                    Ok(Err(_)) | Err(_) => FeatureFlagValues::default(),
                }
            }
            CacheLookup::Load => self.fetch_and_complete(key.clone()).await,
            CacheLookup::Stale {
                values,
                start_refresh,
            } => {
                if start_refresh {
                    self.fetch_and_complete(key.clone()).await
                } else {
                    values
                }
            }
        }
    }

    /// Populate or refresh the shared workspace cache before CreateSession.
    /// Failures are deliberately absorbed by [`resolve`](Self::resolve).
    pub(crate) async fn prepare_for_session_open(&self) {
        let _ = self.resolve().await;
    }

    /// Read and validate a Boolean SAFE flag. Missing, malformed, and fetch
    /// failures all fail open as `None`.
    // The readers intentionally precede their first kernel-owned consumer.
    #[allow(dead_code)]
    pub(crate) fn get_bool(&self, name: &str) -> Option<bool> {
        self.get_typed(name, "boolean")
    }

    /// Read and validate a signed 32-bit integer SAFE flag.
    #[allow(dead_code)]
    pub(crate) fn get_int32(&self, name: &str) -> Option<i32> {
        self.get_typed(name, "int32")
    }

    /// Read and validate a signed 64-bit integer SAFE flag.
    #[allow(dead_code)]
    pub(crate) fn get_int64(&self, name: &str) -> Option<i64> {
        self.get_typed(name, "int64")
    }

    /// Read and validate a double-precision SAFE flag.
    #[allow(dead_code)]
    pub(crate) fn get_double(&self, name: &str) -> Option<f64> {
        self.get_typed(name, "double")
    }

    /// Read and validate a string SAFE flag.
    #[allow(dead_code)]
    pub(crate) fn get_string(&self, name: &str) -> Option<String> {
        self.get_typed(name, "string")
    }

    /// Read and validate a string-list SAFE flag.
    #[allow(dead_code)]
    pub(crate) fn get_string_list(&self, name: &str) -> Option<Vec<String>> {
        self.get_typed(name, "string list")
    }

    fn get_typed<T: DeserializeOwned>(&self, name: &str, expected: &str) -> Option<T> {
        let values = self.cache.current(&self.workspace_key)?;
        let raw = values.get(name)?;
        match serde_json::from_str(raw) {
            Ok(value) => Some(value),
            Err(error) => {
                warn!(
                    flag_name = name,
                    expected_type = expected,
                    error = %error,
                    "feature-flag value has the wrong JSON type; using the consumer default"
                );
                None
            }
        }
    }

    async fn fetch_and_complete(&self, key: WorkspaceKey) -> FeatureFlagValues {
        let guard = self.cache.refresh_guard(key);
        match self.fetch().await {
            Ok(result) => guard.success(result.values, response_ttl(result.ttl_seconds)),
            Err(error) => {
                warn!(
                    client_type = %self.identity.client_type(),
                    error = %error,
                    "feature-flag fetch failed; using defaults or stale values"
                );
                guard.failure()
            }
        }
    }

    fn request_timeout(&self) -> Duration {
        REQUEST_TIMEOUT.min(self.http_client.config().read_timeout)
    }

    async fn fetch(&self) -> Result<FetchResult> {
        let url = build_endpoint(&self.host, &self.identity)?;
        let mut request = self
            .http_client
            .inner()
            .get(url)
            .timeout(self.request_timeout());

        // Match the authenticated request header contract without routing this
        // simple GET through the SQL request retry/idempotency machinery.
        for (name, value) in &self.http_client.config().custom_headers {
            let lower_name = name.to_ascii_lowercase();
            if lower_name != "authorization" && lower_name != "x-databricks-org-id" {
                request = request.header(name, value);
            }
        }
        request = request.header("Authorization", self.http_client.auth_header()?);
        for (name, value) in self.http_client.auth_extra_headers()? {
            request = request.header(name, value);
        }
        if let Some(org_id) = &self.http_client.config().org_id {
            request = request.header("x-databricks-org-id", org_id);
        }

        let response = request.send().await.map_err(|error| {
            let kernel_error = if error.is_timeout() {
                Error::timeout()
            } else {
                Error::network_error()
            };
            kernel_error.message(format!(
                "connector-service feature-flag GET failed: {error}"
            ))
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::from_http_status(status.as_u16()).message(format!(
                "connector-service feature-flag GET returned HTTP {}",
                status.as_u16()
            )));
        }
        let response: FeatureFlagsResponse = response.json().await.map_err(|error| {
            let kernel_error = if error.is_timeout() {
                Error::timeout()
            } else {
                Error::data_loss()
            };
            kernel_error.message(format!(
                "failed to decode connector-service feature-flag response: {error}"
            ))
        })?;

        let entries = response.flags.into_iter().filter_map(|entry| {
            let name = entry.name?;
            let value = entry.value?;
            (!name.is_empty()).then_some((name, value))
        });
        Ok(FetchResult {
            values: FeatureFlagValues::from_entries(entries),
            ttl_seconds: response.ttl_seconds,
        })
    }
}

fn canonical_host(host: String) -> String {
    let Ok(mut url) = Url::parse(&host) else {
        return host;
    };
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    url.as_str().trim_end_matches('/').to_string()
}

fn build_endpoint(host: &str, identity: &ClientIdentity) -> Result<Url> {
    let mut url = Url::parse(host).map_err(|error| {
        Error::invalid_argument().message(format!(
            "invalid workspace host for feature-flag request: {error}"
        ))
    })?;
    let mut path = url.path_segments_mut().map_err(|_| {
        Error::invalid_argument()
            .message("workspace host cannot be used as a base URL for feature-flag requests")
    })?;
    path.clear();
    path.extend([
        "api",
        "2.0",
        "connector-service",
        "feature-flags",
        identity.client_type().as_endpoint_segment(),
        identity.version(),
    ]);
    drop(path);
    Ok(url)
}

struct FetchResult {
    values: FeatureFlagValues,
    ttl_seconds: Option<i64>,
}

#[derive(Deserialize)]
struct FeatureFlagsResponse {
    #[serde(default)]
    flags: Vec<FeatureFlagEntry>,
    ttl_seconds: Option<i64>,
}

#[derive(Deserialize)]
struct FeatureFlagEntry {
    name: Option<String>,
    value: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::auth::pat::PersonalAccessToken;
    use crate::client::{build_retry_configs, HttpClientConfig, RetryConfig};

    #[test]
    fn connector_client_types_use_proto_wire_values() {
        assert_eq!(ClientType::Go.to_string(), "GOLANG");
        assert_eq!(ClientType::NodeJs.to_string(), "NODEJS");
        assert_eq!(ClientType::Odbc.to_string(), "ODBC");
        assert_eq!(ClientType::Python.to_string(), "PYTHON");
    }

    #[test]
    fn feature_flag_values_preserve_opaque_values() {
        let values = FeatureFlagValues::from_entries([
            ("boolean-looking".to_string(), " TRUE ".to_string()),
            ("json-looking".to_string(), "{\"enabled\":1}".to_string()),
            ("boolean-looking".to_string(), "last-value".to_string()),
        ]);

        assert_eq!(values.values.len(), 2);
        assert_eq!(values.get("boolean-looking"), Some("last-value"));
        assert_eq!(values.get("json-looking"), Some("{\"enabled\":1}"));
        assert_eq!(values.get("missing"), None);
    }

    #[test]
    fn endpoint_uses_api_prefix_and_encodes_version_as_one_segment() {
        let identity = ClientIdentity::new(ClientType::Go, " 1.2.3/beta\t");
        assert_eq!(identity.version(), "1.2.3/beta");
        let url = build_endpoint("https://workspace.example.com", &identity).unwrap();

        assert_eq!(
            url.as_str(),
            "https://workspace.example.com/api/2.0/connector-service/feature-flags/GOLANG/1.2.3%2Fbeta"
        );
    }

    #[test]
    fn canonicalizes_host_for_endpoint_and_cache_identity() {
        assert_eq!(
            canonical_host("https://workspace.example.com/old?o=42".to_string()),
            "https://workspace.example.com"
        );
    }

    #[test]
    fn request_timeout_respects_a_shorter_client_timeout() {
        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig {
                    read_timeout: Duration::from_millis(250),
                    ..HttpClientConfig::default()
                },
                build_retry_configs(&RetryConfig::default(), &HashMap::new()),
            )
            .unwrap(),
        );
        let resolver = FeatureFlagResolver::with_cache(
            "https://workspace.example.com",
            http_client,
            ClientIdentity::new(ClientType::Go, "1.2.3"),
            Arc::new(FeatureFlagCache::default()),
        );

        assert_eq!(resolver.request_timeout(), Duration::from_millis(250));
    }

    #[tokio::test]
    async fn feature_flag_transport_timeout_preserves_timeout_code() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3",
            ))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)))
            .expect(1)
            .mount(&server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig {
                    read_timeout: Duration::from_millis(20),
                    ..HttpClientConfig::default()
                },
                HashMap::new(),
            )
            .unwrap(),
        );
        http_client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        let resolver = FeatureFlagResolver::with_cache(
            server.uri(),
            http_client,
            ClientIdentity::new(ClientType::Go, "1.2.3"),
            Arc::new(FeatureFlagCache::default()),
        );

        let error = resolver
            .fetch()
            .await
            .err()
            .expect("delayed response should exceed the request timeout");
        assert_eq!(error.code, ErrorCode::Timeout);
    }

    #[tokio::test]
    async fn fetches_all_safe_value_types_once_then_serves_the_process_cache() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3",
            ))
            .and(header("authorization", "Bearer test-token"))
            .and(header("x-databricks-org-id", "42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "flags": [{
                    "name": "example.boolean",
                    "value": "true"
                }, {
                    "name": "example.int32",
                    "value": "-2147483648"
                }, {
                    "name": "example.int64",
                    "value": "9223372036854775807"
                }, {
                    "name": "example.double",
                    "value": "3.141592653589793"
                }, {
                    "name": "example.string",
                    "value": "\"hello\""
                }, {
                    "name": "example.string-list",
                    "value": "[\"a\",\"b\"]"
                }, {
                    "name": "example.opaque-json",
                    "value": "{\"enabled\":true}"
                }, {
                    "name": "",
                    "value": "ignored"
                }, {
                    "name": "example.missing-value"
                }],
                "ttl_seconds": 300
            })))
            .expect(1)
            .mount(&server)
            .await;

        let config = HttpClientConfig {
            org_id: Some("42".to_string()),
            ..HttpClientConfig::default()
        };
        let http_client = Arc::new(
            DatabricksHttpClient::new(
                config,
                build_retry_configs(&RetryConfig::default(), &HashMap::new()),
            )
            .unwrap(),
        );
        http_client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        let resolver = FeatureFlagResolver::with_cache(
            server.uri(),
            http_client,
            ClientIdentity::new(ClientType::Go, "1.2.3"),
            Arc::new(FeatureFlagCache::default()),
        );

        assert_eq!(
            resolver.get_bool("example.boolean"),
            None,
            "typed getters must not trigger connector-service requests"
        );
        resolver.prepare_for_session_open().await;
        for _ in 0..2 {
            assert_eq!(resolver.get_bool("example.boolean"), Some(true));
            assert_eq!(resolver.get_int32("example.int32"), Some(i32::MIN));
            assert_eq!(resolver.get_int64("example.int64"), Some(i64::MAX));
            assert_eq!(
                resolver.get_double("example.double"),
                Some(std::f64::consts::PI)
            );
            assert_eq!(
                resolver.get_string("example.string").as_deref(),
                Some("hello")
            );
            assert_eq!(
                resolver.get_string_list("example.string-list"),
                Some(vec!["a".to_string(), "b".to_string()])
            );
            assert_eq!(
                resolver.get_int32("example.int64"),
                None,
                "int32 validates range even though JSON has no integer type tag"
            );
            assert_eq!(
                resolver.get_bool("example.string"),
                None,
                "typed getters do not coerce JSON strings"
            );
            assert_eq!(resolver.get_bool("example.opaque-json"), None);
            assert_eq!(resolver.get_int32("example.opaque-json"), None);
            assert_eq!(resolver.get_int64("example.opaque-json"), None);
            assert_eq!(resolver.get_double("example.opaque-json"), None);
            assert_eq!(resolver.get_string("example.opaque-json"), None);
            assert_eq!(resolver.get_string_list("example.opaque-json"), None);
        }
    }

    #[tokio::test]
    async fn cache_waiter_respects_its_own_request_timeout() {
        let cache = Arc::new(FeatureFlagCache::default());
        let host = "https://workspace.example.com";
        let key = WorkspaceKey::new(host, None);
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));

        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig {
                    read_timeout: Duration::from_millis(10),
                    ..HttpClientConfig::default()
                },
                HashMap::new(),
            )
            .unwrap(),
        );
        let resolver = FeatureFlagResolver::with_cache(
            host,
            http_client,
            ClientIdentity::new(ClientType::Go, "1.2.3"),
            Arc::clone(&cache),
        );

        let started = std::time::Instant::now();
        resolver.prepare_for_session_open().await;
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(resolver.get_bool("example.flag"), None);

        // Cancel the synthetic leader and clean up the in-flight entry.
        drop(cache.refresh_guard(key));
    }

    #[tokio::test]
    async fn stale_values_remain_readable_while_session_open_refreshes_cache() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(50))
                    .set_body_json(json!({
                        "flags": [{"name": "example.flag", "value": "\"new\""}],
                        "ttl_seconds": 300
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let identity = ClientIdentity::new(ClientType::Go, "1.2.3");
        let cache = Arc::new(FeatureFlagCache::default());
        let key = WorkspaceKey::new(canonical_host(server.uri()), None);
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        cache.refresh_guard(key).success(
            FeatureFlagValues::from_entries([(
                "example.flag".to_string(),
                "\"stale\"".to_string(),
            )]),
            Duration::ZERO,
        );

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default(), HashMap::new()).unwrap(),
        );
        http_client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        let resolver = FeatureFlagResolver::with_cache(server.uri(), http_client, identity, cache);

        let refresh = {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.prepare_for_session_open().await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(
            resolver.get_string("example.flag").as_deref(),
            Some("stale")
        );

        // Typed readers never start network work; while session-open refresh is
        // in flight they keep seeing the last process-cache value.
        let second = resolver.get_string("example.flag");
        let third = resolver.get_string("example.flag");
        assert_eq!(second.as_deref(), Some("stale"));
        assert_eq!(third.as_deref(), Some("stale"));

        refresh.await.expect("session-open refresh task");
        let refreshed = resolver.get_string("example.flag");
        assert_eq!(refreshed.as_deref(), Some("new"));
        server.verify().await;
    }

    #[tokio::test]
    async fn skips_connector_service_when_version_is_empty_or_blank() {
        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig::default(),
                build_retry_configs(&RetryConfig::default(), &HashMap::new()),
            )
            .unwrap(),
        );
        for version in ["", " \t"] {
            let resolver = FeatureFlagResolver::with_cache(
                "not a URL",
                Arc::clone(&http_client),
                ClientIdentity::new(ClientType::Go, version),
                Arc::new(FeatureFlagCache::default()),
            );
            resolver.prepare_for_session_open().await;
            assert_eq!(resolver.get_bool("missing.flag"), None);
        }
    }

    #[tokio::test]
    async fn connector_failure_is_not_cached_and_next_session_open_retries() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3",
            ))
            .respond_with(ResponseTemplate::new(503))
            .expect(2)
            .mount(&server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default(), HashMap::new()).unwrap(),
        );
        http_client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        let resolver = FeatureFlagResolver::with_cache(
            server.uri(),
            http_client,
            ClientIdentity::new(ClientType::Go, "1.2.3"),
            Arc::new(FeatureFlagCache::default()),
        );

        resolver.prepare_for_session_open().await;
        for _ in 0..2 {
            assert_eq!(resolver.get_bool("missing.flag"), None);
        }
        resolver.prepare_for_session_open().await;
    }

    #[tokio::test]
    async fn auth_failure_does_not_poison_a_healthy_session() {
        let server = MockServer::start().await;
        let endpoint = "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3";
        Mock::given(method("GET"))
            .and(path(endpoint))
            .and(header("authorization", "Bearer expired-token"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(endpoint))
            .and(header("authorization", "Bearer healthy-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "flags": [{"name": "example.flag", "value": "\"opaque\""}],
                "ttl_seconds": 300
            })))
            .expect(1)
            .mount(&server)
            .await;

        let cache = Arc::new(FeatureFlagCache::default());
        let resolver = |token: &str| {
            let http_client = Arc::new(
                DatabricksHttpClient::new(HttpClientConfig::default(), HashMap::new()).unwrap(),
            );
            http_client.set_auth_provider(Arc::new(PersonalAccessToken::new(token)));
            FeatureFlagResolver::with_cache(
                server.uri(),
                http_client,
                ClientIdentity::new(ClientType::Go, "1.2.3"),
                Arc::clone(&cache),
            )
        };

        let expired = resolver("expired-token");
        expired.prepare_for_session_open().await;
        assert_eq!(expired.get_string("example.flag"), None);

        let healthy = resolver("healthy-token");
        healthy.prepare_for_session_open().await;
        assert_eq!(
            healthy.get_string("example.flag").as_deref(),
            Some("opaque")
        );
    }

    #[tokio::test]
    async fn malformed_response_does_not_poison_a_healthy_session() {
        let server = MockServer::start().await;
        let endpoint = "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3";
        Mock::given(method("GET"))
            .and(path(endpoint))
            .and(header("authorization", "Bearer proxied-token"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not JSON"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(endpoint))
            .and(header("authorization", "Bearer healthy-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "flags": [{"name": "example.flag", "value": "\"opaque\""}],
                "ttl_seconds": 300
            })))
            .expect(1)
            .mount(&server)
            .await;

        let cache = Arc::new(FeatureFlagCache::default());
        let resolver = |token: &str| {
            let http_client = Arc::new(
                DatabricksHttpClient::new(HttpClientConfig::default(), HashMap::new()).unwrap(),
            );
            http_client.set_auth_provider(Arc::new(PersonalAccessToken::new(token)));
            FeatureFlagResolver::with_cache(
                server.uri(),
                http_client,
                ClientIdentity::new(ClientType::Go, "1.2.3"),
                Arc::clone(&cache),
            )
        };

        let malformed = resolver("proxied-token");
        malformed.prepare_for_session_open().await;
        assert_eq!(malformed.get_string("example.flag"), None);

        let healthy = resolver("healthy-token");
        healthy.prepare_for_session_open().await;
        assert_eq!(
            healthy.get_string("example.flag").as_deref(),
            Some("opaque")
        );
    }
}
