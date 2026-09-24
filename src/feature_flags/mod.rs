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
//! process-wide cache across sessions. A cold miss performs one blocking
//! request; fresh entries make no request; stale entries are returned
//! immediately while one background refresh runs. Values remain opaque until
//! an individual feature explicitly opts in to interpreting one.

mod cache;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Url;
use serde::Deserialize;

use crate::client::DatabricksHttpClient;
use crate::kernel_error::{Error, ErrorCode, Result};
use crate::klog::warn;

use cache::{
    global_cache, response_ttl, CacheKey, CacheLookup, FeatureFlagCache, NEGATIVE_TTL,
    TRANSIENT_NEGATIVE_TTL,
};

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
    Python,
}

impl ClientType {
    const fn as_endpoint_segment(self) -> &'static str {
        match self {
            Self::Go => "GOLANG",
            Self::NodeJs => "NODEJS",
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
/// The version is sent as one URL path segment and is also part of the
/// process-cache key, so different driver releases never share evaluations.
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

/// Immutable name/value snapshot returned by feature-flag resolution.
///
/// Connector-service is responsible for returning only flags that are safe for
/// the identified driver. Each value is the JSON-serialized SAFE value (for
/// example, `true`, `42`, `"value"`, or `["a","b"]`). The kernel deliberately
/// preserves that JSON text without interpreting it; each feature owns its flag
/// name, expected type, parsing, and fallback policy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeatureFlagValues {
    values: Arc<HashMap<String, String>>,
}

impl FeatureFlagValues {
    fn from_entries(entries: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            values: Arc::new(entries.into_iter().collect()),
        }
    }

    /// Return the JSON-serialized SAFE value associated with `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// Iterate over all names and opaque values in this snapshot.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Return the number of flags in this snapshot.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Return whether this snapshot contains no flags.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Resolves allowlisted driver feature flags through connector-service.
///
/// Resolution always fails open: request and response failures are logged and
/// produce defaults (or the last successfully cached values).
#[derive(Clone)]
pub struct FeatureFlagResolver {
    host: String,
    http_client: Arc<DatabricksHttpClient>,
    cache: Arc<FeatureFlagCache>,
    org_id: Option<String>,
}

impl fmt::Debug for FeatureFlagResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureFlagResolver")
            // Do not recursively render DatabricksHttpClient: its custom
            // headers may contain caller-supplied secrets.
            .field("host", &self.host)
            .field("org_id", &self.org_id)
            .finish_non_exhaustive()
    }
}

impl FeatureFlagResolver {
    pub fn new(host: impl Into<String>, http_client: Arc<DatabricksHttpClient>) -> Self {
        Self::with_cache(host, http_client, global_cache())
    }

    fn with_cache(
        host: impl Into<String>,
        http_client: Arc<DatabricksHttpClient>,
        cache: Arc<FeatureFlagCache>,
    ) -> Self {
        let org_id = http_client.config().org_id.clone();
        Self {
            host: canonical_host(host.into()),
            http_client,
            cache,
            org_id,
        }
    }

    pub async fn resolve(&self, identity: &ClientIdentity) -> FeatureFlagValues {
        if identity.version().trim().is_empty() {
            return FeatureFlagValues::default();
        }

        let key = CacheKey::new(&self.host, self.org_id.clone(), identity.clone());

        loop {
            match self.cache.lookup(&key) {
                CacheLookup::Fresh(values) => return values,
                CacheLookup::Wait(receiver) => match receiver.await {
                    Ok(Some(values)) => return values,
                    // The leader failed for caller-local reasons or was
                    // cancelled. Retry lookup so one healthy waiter becomes
                    // the next leader instead of accepting a poisoned default.
                    Ok(None) | Err(_) => continue,
                },
                CacheLookup::Load => {
                    return self.fetch_and_complete(key.clone(), identity.clone()).await;
                }
                CacheLookup::Stale {
                    values,
                    start_refresh,
                } => {
                    if start_refresh {
                        self.refresh_in_background(key.clone(), identity.clone());
                    }
                    return values;
                }
            }
        }
    }

    async fn fetch_and_complete(
        &self,
        key: CacheKey,
        identity: ClientIdentity,
    ) -> FeatureFlagValues {
        let guard = self.cache.refresh_guard(key);
        match self.fetch(&identity).await {
            Ok(result) => guard.success(result.values, response_ttl(result.ttl_seconds)),
            Err(error) => {
                warn!(
                    client_type = %identity.client_type(),
                    error = %error,
                    "feature-flag fetch failed; using defaults or stale values"
                );
                match failure_backoff(&error, self.request_timeout()) {
                    Some(ttl) => guard.backoff_for(ttl),
                    None => guard.retry(),
                }
            }
        }
    }

    fn refresh_in_background(&self, key: CacheKey, identity: ClientIdentity) {
        let resolver = self.clone();
        let guard = self.cache.refresh_guard(key);
        tokio::spawn(async move {
            match resolver.fetch(&identity).await {
                Ok(result) => {
                    guard.success(result.values, response_ttl(result.ttl_seconds));
                }
                Err(error) => {
                    warn!(
                        client_type = %identity.client_type(),
                        error = %error,
                        "background feature-flag refresh failed; retaining stale values"
                    );
                    match failure_backoff(&error, resolver.request_timeout()) {
                        Some(ttl) => {
                            guard.backoff_for(ttl);
                        }
                        None => {
                            guard.retry();
                        }
                    }
                }
            }
        });
    }

    fn request_timeout(&self) -> Duration {
        REQUEST_TIMEOUT.min(self.http_client.config().read_timeout)
    }

    async fn fetch(&self, identity: &ClientIdentity) -> Result<FetchResult> {
        let url = build_endpoint(&self.host, identity)?;
        let request = self
            .http_client
            .inner()
            .get(url)
            .timeout(self.request_timeout())
            .build()
            .map_err(|error| {
                Error::invalid_argument()
                    .message(format!("failed to build feature-flag request: {error}"))
            })?;

        let response = self.http_client.execute_feature_flags(request).await?;
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

/// Endpoint failures receive the full negative-cache interval. Ambiguous
/// transport failures (DNS, TLS, proxy, connection refusal, or a shorter
/// caller timeout) receive a brief interval: this bounds request storms while
/// limiting how long one session's transport problem can affect another.
/// Auth, malformed-response, configuration, and cancellation failures do not
/// publish a cold negative result because they are caller-local. If a prior
/// value exists, the cache retains it with a brief stale-refresh cooldown.
fn failure_backoff(error: &Error, request_timeout: Duration) -> Option<Duration> {
    match error.http_status {
        Some(401 | 403) => None,
        Some(_) => Some(NEGATIVE_TTL),
        None => match error.code {
            ErrorCode::Timeout if request_timeout >= REQUEST_TIMEOUT => Some(NEGATIVE_TTL),
            ErrorCode::Timeout | ErrorCode::NetworkError => Some(TRANSIENT_NEGATIVE_TTL),
            ErrorCode::ResourceExhausted | ErrorCode::Unavailable => Some(NEGATIVE_TTL),
            _ => None,
        },
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
        assert_eq!(ClientType::Python.to_string(), "PYTHON");
    }

    #[test]
    fn feature_flag_values_preserve_opaque_values() {
        let values = FeatureFlagValues::from_entries([
            ("boolean-looking".to_string(), " TRUE ".to_string()),
            ("json-looking".to_string(), "{\"enabled\":1}".to_string()),
            ("boolean-looking".to_string(), "last-value".to_string()),
        ]);

        assert_eq!(values.len(), 2);
        assert_eq!(values.get("boolean-looking"), Some("last-value"));
        assert_eq!(values.get("json-looking"), Some("{\"enabled\":1}"));
        assert_eq!(values.get("missing"), None);
        assert_eq!(values.iter().count(), 2);
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
            Arc::new(FeatureFlagCache::default()),
        );

        assert_eq!(resolver.request_timeout(), Duration::from_millis(250));
    }

    #[test]
    fn failure_backoff_distinguishes_endpoint_and_transport_failures() {
        let timeout = Error::timeout();

        assert_eq!(
            failure_backoff(&timeout, REQUEST_TIMEOUT),
            Some(NEGATIVE_TTL)
        );
        assert_eq!(
            failure_backoff(&timeout, Duration::from_millis(250)),
            Some(TRANSIENT_NEGATIVE_TTL)
        );
        assert_eq!(
            failure_backoff(&Error::network_error(), REQUEST_TIMEOUT),
            Some(TRANSIENT_NEGATIVE_TTL)
        );
        assert_eq!(failure_backoff(&Error::data_loss(), REQUEST_TIMEOUT), None);
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
            Arc::new(FeatureFlagCache::default()),
        );

        let error = resolver
            .fetch(&ClientIdentity::new(ClientType::Go, "1.2.3"))
            .await
            .err()
            .expect("delayed response should exceed the request timeout");
        assert_eq!(error.code, ErrorCode::Timeout);
        assert_eq!(
            failure_backoff(&error, resolver.request_timeout()),
            Some(TRANSIENT_NEGATIVE_TTL)
        );
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
            Arc::new(FeatureFlagCache::default()),
        );
        let identity = ClientIdentity::new(ClientType::Go, "1.2.3");

        for _ in 0..2 {
            let values = resolver.resolve(&identity).await;
            assert_eq!(values.len(), 6);
            assert_eq!(values.get("example.boolean"), Some("true"));
            assert_eq!(values.get("example.int32"), Some("-2147483648"));
            assert_eq!(values.get("example.int64"), Some("9223372036854775807"));
            assert_eq!(values.get("example.double"), Some("3.141592653589793"));
            assert_eq!(values.get("example.string"), Some("\"hello\""));
            assert_eq!(values.get("example.string-list"), Some("[\"a\",\"b\"]"));
        }
    }

    #[tokio::test]
    async fn stale_values_are_served_while_one_background_refresh_updates_cache() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(50))
                    .set_body_json(json!({
                        "flags": [{"name": "example.flag", "value": "new"}],
                        "ttl_seconds": 300
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let identity = ClientIdentity::new(ClientType::Go, "1.2.3");
        let cache = Arc::new(FeatureFlagCache::default());
        let key = CacheKey::new(canonical_host(server.uri()), None, identity.clone());
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        cache.refresh_guard(key).success(
            FeatureFlagValues::from_entries([("example.flag".to_string(), "stale".to_string())]),
            Duration::ZERO,
        );

        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default(), HashMap::new()).unwrap(),
        );
        http_client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        let resolver = FeatureFlagResolver::with_cache(server.uri(), http_client, cache);

        let first = resolver.resolve(&identity).await;
        assert_eq!(first.get("example.flag"), Some("stale"));

        // Readers arriving while the request is in flight keep receiving the
        // stale snapshot without starting more background requests.
        let (second, third) =
            tokio::join!(resolver.resolve(&identity), resolver.resolve(&identity));
        assert_eq!(second.get("example.flag"), Some("stale"));
        assert_eq!(third.get("example.flag"), Some("stale"));

        let refreshed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let values = resolver.resolve(&identity).await;
                if values.get("example.flag") == Some("new") {
                    break values;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background refresh should update the cached value");
        assert_eq!(refreshed.get("example.flag"), Some("new"));
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
        let resolver = FeatureFlagResolver::with_cache(
            "not a URL",
            http_client,
            Arc::new(FeatureFlagCache::default()),
        );

        for version in ["", " \t"] {
            assert!(resolver
                .resolve(&ClientIdentity::new(ClientType::Go, version))
                .await
                .is_empty());
        }
    }

    #[tokio::test]
    async fn connector_failure_is_not_retried_and_fails_open() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/2.0/connector-service/feature-flags/GOLANG/1.2.3",
            ))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;

        // A hand-built map remains safe: the connector-service policy is
        // private and cannot be overridden through the public retry enums.
        let http_client = Arc::new(
            DatabricksHttpClient::new(HttpClientConfig::default(), HashMap::new()).unwrap(),
        );
        http_client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        let resolver = FeatureFlagResolver::with_cache(
            server.uri(),
            http_client,
            Arc::new(FeatureFlagCache::default()),
        );

        for _ in 0..2 {
            assert!(resolver
                .resolve(&ClientIdentity::new(ClientType::Go, "1.2.3"))
                .await
                .is_empty());
        }
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
                "flags": [{"name": "example.flag", "value": "opaque"}],
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
            FeatureFlagResolver::with_cache(server.uri(), http_client, Arc::clone(&cache))
        };
        let identity = ClientIdentity::new(ClientType::Go, "1.2.3");

        assert!(resolver("expired-token")
            .resolve(&identity)
            .await
            .is_empty());
        assert_eq!(
            resolver("healthy-token")
                .resolve(&identity)
                .await
                .get("example.flag"),
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
                "flags": [{"name": "example.flag", "value": "opaque"}],
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
            FeatureFlagResolver::with_cache(server.uri(), http_client, Arc::clone(&cache))
        };
        let identity = ClientIdentity::new(ClientType::Go, "1.2.3");

        assert!(resolver("proxied-token")
            .resolve(&identity)
            .await
            .is_empty());
        assert_eq!(
            resolver("healthy-token")
                .resolve(&identity)
                .await
                .get("example.flag"),
            Some("opaque")
        );
    }
}
