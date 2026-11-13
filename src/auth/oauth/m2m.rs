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

//! M2M (Machine-to-Machine) OAuth authentication using client credentials flow.
//!
//! This module implements the OAuth 2.0 client credentials grant for service
//! principal authentication. The flow:
//!
//! 1. Discover token endpoint via OIDC well-known endpoint
//! 2. Exchange client_id + client_secret for access_token
//! 3. Cache token in TokenStore with refresh lifecycle
//! 4. Re-authenticate using client credentials when token expires (no refresh_token)
//!
//! M2M tokens are not cached to disk because:
//! - Credentials (client_id/secret) are always available for re-authentication
//! - Tokens are typically short-lived
//! - Caching adds security risk without significant benefit

use crate::auth::oauth::oidc::OidcEndpoints;
use crate::auth::oauth::token::OAuthToken;
use crate::auth::oauth::token_store::TokenStore;
use crate::auth::AuthProvider;
use crate::client::http::DatabricksHttpClient;
use crate::client::retry::{is_non_retryable_status, RequestType};
use crate::kernel_error::{Error, ErrorCode, Result};
use crate::util::runtime::block_on_in_runtime;
use futures::future::{BoxFuture, FutureExt, Shared};
use oauth2::basic::BasicClient;
use oauth2::{
    AuthUrl, ClientId, ClientSecret, HttpRequest, HttpResponse, RequestTokenError, Scope,
    TokenResponse, TokenUrl,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::runtime::Handle as RuntimeHandle;

/// `(token_endpoint, authorization_endpoint)` from one OIDC discovery.
type Discovery = Shared<BoxFuture<'static, Result<(String, String)>>>;

/// OIDC endpoints and token store shared by every live provider in the
/// process that uses the same credentials, so N sessions opening at once
/// run one discovery and one token exchange instead of N of each.
struct SharedCredentials {
    key: CredentialsKey,
    discovery: Mutex<Option<Discovery>>,
    token_store: Arc<TokenStore>,
}

impl fmt::Debug for SharedCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedCredentials")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl SharedCredentials {
    /// The discovery to await: the one in flight or done, or a new one if the
    /// last attempt failed. Callers that join an attempt get its result, error
    /// included, instead of each retrying it in turn.
    ///
    /// The request runs as a task on the caller's runtime and only its result
    /// is shared: joiners may be sessions with their own runtimes, so they
    /// must not drive a request bound to another runtime. If that runtime
    /// shuts down first, joiners get `Cancelled` (see [`Self::endpoints`]).
    fn discovery(&self, host: &str, http_client: &Arc<DatabricksHttpClient>) -> Discovery {
        let mut slot = self.discovery.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(discovery) = slot.as_ref() {
            if !matches!(discovery.peek(), Some(Err(_))) {
                return discovery.clone();
            }
        }
        let host = host.to_owned();
        let http_client = Arc::clone(http_client);
        let task = tokio::spawn(async move {
            let endpoints = OidcEndpoints::discover(&host, &http_client).await?;
            Ok((endpoints.token_endpoint, endpoints.authorization_endpoint))
        });
        let discovery = async move {
            task.await.unwrap_or_else(|e| {
                Err(Error::cancelled().message(format!("OIDC discovery did not finish: {e}")))
            })
        }
        .boxed()
        .shared();
        *slot = Some(discovery.clone());
        discovery
    }

    /// Discovered endpoints. An attempt cancelled with the session that
    /// started it is not this caller's failure, so it is retried once.
    async fn endpoints(
        &self,
        host: &str,
        http_client: &Arc<DatabricksHttpClient>,
    ) -> Result<(String, String)> {
        match self.discovery(host, http_client).await {
            Err(e) if e.code == ErrorCode::Cancelled => self.discovery(host, http_client).await,
            result => result,
        }
    }
}

/// Identifies credentials without holding the secret itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CredentialsKey {
    host: String,
    client_id: String,
    secret_sha256: [u8; 32],
    scopes: Vec<String>,
    token_endpoint_override: Option<String>,
    /// Sessions with different TLS, proxy or retry settings never share:
    /// a result fetched through one transport must not stand in for another.
    transport: [u8; 32],
}

type Registry = Mutex<HashMap<CredentialsKey, Weak<SharedCredentials>>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Entries are weak and removed when their last provider drops, so neither
/// the token nor the key outlives the last session using them.
fn shared_credentials(key: CredentialsKey) -> Arc<SharedCredentials> {
    let mut registry = registry().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(shared) = registry.get(&key).and_then(Weak::upgrade) {
        return shared;
    }
    let shared = Arc::new(SharedCredentials {
        key: key.clone(),
        discovery: Mutex::new(None),
        token_store: Arc::new(TokenStore::new()),
    });
    registry.insert(key, Arc::downgrade(&shared));
    shared
}

impl Drop for SharedCredentials {
    fn drop(&mut self) {
        let mut registry = registry().lock().unwrap_or_else(|e| e.into_inner());
        // A new entry may already have replaced this one under the same key.
        if registry
            .get(&self.key)
            .is_some_and(|weak| weak.strong_count() == 0)
        {
            registry.remove(&self.key);
        }
    }
}

/// OAuth provider for M2M (Machine-to-Machine) authentication.
///
/// Uses the OAuth 2.0 client credentials grant to authenticate service principals.
/// The provider discovers the token endpoint via OIDC, then exchanges client
/// credentials for access tokens. Tokens are managed by TokenStore with automatic
/// refresh when they become stale or expire.
///
/// # Token Lifecycle
///
/// - **Initial fetch**: On first `get_auth_header()` call, exchanges credentials for token
/// - **Fresh token**: Returned immediately from TokenStore (no HTTP call)
/// - **Stale token**: Returned immediately, background refresh spawned
/// - **Expired token**: Blocking re-authentication via client credentials exchange
///
/// M2M tokens have no `refresh_token` - re-authentication uses the same client credentials.
///
/// # Example
///
/// ```no_run
/// use databricks_sql_kernel::auth::AuthProvider;
/// use databricks_sql_kernel::auth::oauth::m2m::ClientCredentialsProvider;
/// use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig};
/// use std::sync::Arc;
///
/// # async fn example() -> databricks_sql_kernel::Result<()> {
/// let http_client = Arc::new(DatabricksHttpClient::with_default_retry(HttpClientConfig::default())?);
/// let provider = ClientCredentialsProvider::new(
///     "https://my-workspace.cloud.databricks.com",
///     "my-client-id",
///     "my-client-secret",
///     http_client,
/// ).await?;
///
/// // Get auth header (will trigger initial token fetch)
/// let auth_header = provider.get_auth_header()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ClientCredentialsProvider {
    /// Client ID for OAuth
    client_id: String,
    /// Client secret for OAuth
    client_secret: String,
    /// Discovered OIDC endpoints
    token_endpoint: String,
    auth_endpoint: String,
    /// Thread-safe token store managing the token lifecycle; shared with
    /// other live providers that use the same credentials.
    token_store: Arc<TokenStore>,
    /// Keeps the process-wide entry alive while this provider is.
    _shared: Option<Arc<SharedCredentials>>,
    /// HTTP client for token endpoint requests
    http_client: Arc<DatabricksHttpClient>,
    /// OAuth scopes to request (typically "all-apis"). Empty when `resource`
    /// is set (a v1.0 `resource=` grant).
    scopes: Vec<String>,
    /// Azure AD **v1.0** `resource=` value for the client-credentials grant,
    /// used instead of `scope=`. When `Some`, the token request sends
    /// `resource=<value>` (and no `scope`) to the v1.0 `/oauth2/token`
    /// endpoint, yielding a token whose `aud` claim is that resource verbatim
    /// (e.g. `https://management.core.windows.net/`, with the trailing slash
    /// Databricks' control plane requires). The v2.0 `scope=<resource>/.default`
    /// form drops that trailing slash from `aud`, which the server rejects.
    resource: Option<String>,
    /// Tokio runtime handle captured from the constructing context;
    /// used to drive the async token exchange from the sync
    /// `get_auth_header` trait method safely whether or not the
    /// caller is already inside a runtime (see
    /// `crate::util::runtime::block_on_in_runtime`).
    runtime_handle: RuntimeHandle,
}

impl ClientCredentialsProvider {
    /// Creates a new M2M authentication provider.
    ///
    /// Discovers the token endpoint via OIDC before returning. The provider
    /// is ready to use after construction - the first `get_auth_header()` call
    /// will trigger the initial token exchange.
    ///
    /// # Arguments
    ///
    /// * `host` - Databricks workspace URL (e.g., "https://my-workspace.cloud.databricks.com")
    /// * `client_id` - OAuth client ID for the service principal
    /// * `client_secret` - OAuth client secret for the service principal
    /// * `http_client` - Shared HTTP client for token endpoint requests
    ///
    /// # Returns
    ///
    /// A configured provider ready for authentication, or an error if:
    /// - OIDC discovery fails (network error, invalid response, etc.)
    /// - The runtime handle cannot be obtained
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use databricks_sql_kernel::auth::oauth::m2m::ClientCredentialsProvider;
    /// # use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig};
    /// # use std::sync::Arc;
    /// # async fn example() -> databricks_sql_kernel::Result<()> {
    /// let http_client = Arc::new(DatabricksHttpClient::with_default_retry(HttpClientConfig::default())?);
    /// let provider = ClientCredentialsProvider::new(
    ///     "https://my-workspace.cloud.databricks.com",
    ///     "client-id",
    ///     "client-secret",
    ///     http_client,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        host: &str,
        client_id: &str,
        client_secret: &str,
        http_client: Arc<DatabricksHttpClient>,
    ) -> Result<Self> {
        Self::new_with_scopes(
            host,
            client_id,
            client_secret,
            http_client,
            vec!["all-apis".to_string()],
        )
        .await
    }

    /// Creates a new M2M provider with custom scopes.
    ///
    /// This is useful for testing or when specific scopes are required.
    /// Most users should use `new()` which defaults to "all-apis".
    pub async fn new_with_scopes(
        host: &str,
        client_id: &str,
        client_secret: &str,
        http_client: Arc<DatabricksHttpClient>,
        scopes: Vec<String>,
    ) -> Result<Self> {
        Self::new_with_full_config(host, client_id, client_secret, http_client, scopes, None).await
    }

    /// Creates a new M2M provider with full configuration options.
    ///
    /// Allows overriding the token endpoint URL. If `token_endpoint_override` is `None`,
    /// the endpoint will be discovered via OIDC.
    ///
    /// # Arguments
    ///
    /// * `host` - Databricks workspace URL
    /// * `client_id` - OAuth client ID for the service principal
    /// * `client_secret` - OAuth client secret for the service principal
    /// * `http_client` - Shared HTTP client for token endpoint requests
    /// * `scopes` - OAuth scopes to request
    /// * `token_endpoint_override` - Optional token endpoint URL override
    ///
    /// # Returns
    ///
    /// A configured provider ready for authentication, or an error if:
    /// - OIDC discovery fails (when token_endpoint_override is None)
    /// - The runtime handle cannot be obtained
    pub async fn new_with_full_config(
        host: &str,
        client_id: &str,
        client_secret: &str,
        http_client: Arc<DatabricksHttpClient>,
        scopes: Vec<String>,
        token_endpoint_override: Option<String>,
    ) -> Result<Self> {
        // Discover OIDC endpoints, or skip discovery entirely when the token
        // endpoint is overridden. The client-credentials grant never uses the
        // authorization endpoint, so when a caller pins the token endpoint
        // (e.g. an Azure/Entra endpoint, which is NOT the workspace's) there is
        // no reason to round-trip the workspace OIDC config — and doing so
        // would needlessly depend on it being reachable and describing the
        // right issuer. Reuse the token endpoint as the (unused) auth URL
        // placeholder so the `oauth2` client still constructs.
        let mut key_scopes = scopes.clone();
        key_scopes.sort_unstable();
        key_scopes.dedup();
        let shared = shared_credentials(CredentialsKey {
            host: host.to_string(),
            client_id: client_id.to_string(),
            secret_sha256: Sha256::digest(client_secret.as_bytes()).into(),
            scopes: key_scopes,
            token_endpoint_override: token_endpoint_override.clone(),
            transport: http_client.transport_fingerprint(),
        });
        let (token_endpoint, auth_endpoint) = match token_endpoint_override {
            Some(token_endpoint) => (token_endpoint.clone(), token_endpoint),
            None => shared.endpoints(host, &http_client).await?,
        };

        Ok(Self {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            token_endpoint,
            auth_endpoint,
            token_store: Arc::clone(&shared.token_store),
            _shared: Some(shared),
            http_client,
            scopes,
            resource: None,
            // `new_with_full_config` is `async`, so we're guaranteed
            // to be inside a tokio runtime here. Capture that handle
            // so `get_auth_header` can dispatch correctly later via
            // `block_on_in_runtime`, even when the caller has
            // wrapped the kernel in its own outer `block_on` (e.g.
            // the PyO3 binding) or eventually drives auth from a
            // non-tokio thread (e.g. the C ABI shim).
            runtime_handle: RuntimeHandle::current(),
        })
    }

    /// Construct a provider that mints tokens via the Azure AD **v1.0**
    /// `resource=` client-credentials grant against `token_endpoint` (the v1.0
    /// `{login}/{tenant}/oauth2/token` path), rather than the v2.0
    /// `scope=<resource>/.default` form. The resulting token's `aud` claim is
    /// `resource` verbatim — required for the Azure SP **management token**,
    /// whose `aud` Databricks strict-matches as `https://management.core.windows.net/`
    /// (trailing slash), which the v2.0 `.default` form drops. No OIDC discovery
    /// (the endpoint is always supplied).
    pub async fn new_with_resource_v1(
        client_id: &str,
        client_secret: &str,
        http_client: Arc<DatabricksHttpClient>,
        resource: String,
        token_endpoint: String,
    ) -> Result<Self> {
        Ok(Self {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            auth_endpoint: token_endpoint.clone(),
            token_endpoint,
            token_store: Arc::new(TokenStore::new()),
            _shared: None,
            http_client,
            scopes: Vec::new(),
            resource: Some(resource),
            runtime_handle: RuntimeHandle::current(),
        })
    }
}

impl AuthProvider for ClientCredentialsProvider {
    /// Returns a valid Bearer token for authentication.
    ///
    /// This method implements the token lifecycle state machine via TokenStore:
    /// - **Empty**: Blocks and fetches the initial token
    /// - **Fresh**: Returns cached token immediately (no HTTP call)
    /// - **Stale**: Returns cached token immediately, spawns background refresh
    /// - **Expired**: Blocks and fetches a new token via client credentials exchange
    ///
    /// The method is safe to call from multiple threads concurrently. Only one
    /// token fetch/refresh will execute at a time, coordinated by TokenStore.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The token exchange fails (network error, invalid credentials, etc.)
    /// - The token store lock is poisoned (indicates panic in another thread)
    fn get_auth_header(&self) -> Result<String> {
        // Capture variables for the closure
        let http_client = self.http_client.clone();
        let client_id = self.client_id.clone();
        let client_secret = self.client_secret.clone();
        let token_endpoint = self.token_endpoint.clone();
        let auth_endpoint = self.auth_endpoint.clone();
        let scopes = self.scopes.clone();
        let resource = self.resource.clone();

        // Get or refresh token via TokenStore
        let runtime_handle = self.runtime_handle.clone();
        let token = self.token_store.get_or_refresh(move || {
            // Execute the async token fetch via the kernel's
            // captured runtime handle. `block_on_in_runtime`
            // dispatches on `Handle::try_current()`: it uses
            // `block_in_place` when called from inside a runtime
            // (e.g. driven by the PyO3 wrapper), and falls back to
            // a direct `block_on` when called from a plain thread
            // (e.g. the future C ABI shim). See `crate::util::runtime`.
            block_on_in_runtime(&runtime_handle, async {
                // A v1.0 `resource=` grant (Azure SP management token) needs a
                // lenient response parse — the Azure AD v1.0 token endpoint
                // returns `expires_in` as a JSON *string*, which the `oauth2`
                // crate's strict parser rejects. Fetch it directly instead.
                if let Some(resource) = &resource {
                    return fetch_v1_resource_token(
                        &http_client,
                        &token_endpoint,
                        &client_id,
                        &client_secret,
                        resource,
                    )
                    .await;
                }

                // Build the OAuth client
                let oauth_client = BasicClient::new(ClientId::new(client_id))
                    .set_client_secret(ClientSecret::new(client_secret))
                    .set_auth_uri(AuthUrl::new(auth_endpoint).map_err(|e| {
                        Error::invalid_argument()
                            .message(format!("Invalid authorization endpoint URL: {}", e))
                    })?)
                    .set_token_uri(TokenUrl::new(token_endpoint).map_err(|e| {
                        Error::invalid_argument()
                            .message(format!("Invalid token endpoint URL: {}", e))
                    })?);

                // v2.0 client-credentials grant: send `scope=` per requested scope.
                let mut token_request = oauth_client.exchange_client_credentials();
                for scope in &scopes {
                    token_request = token_request.add_scope(Scope::new(scope.clone()));
                }

                // `oauth2` builds the request; the kernel client sends it so the
                // Auth retry ladder covers 429 / 5xx / network failures.
                let send = |request: HttpRequest| send_token_request(&http_client, request);
                let token_response =
                    token_request
                        .request_async(&send)
                        .await
                        .map_err(|e| match e {
                            RequestTokenError::Request(e) => {
                                token_exchange_error(e, "M2M token exchange")
                            }
                            other => Error::unauthenticated()
                                .message(format!("M2M token exchange failed: {}", other))
                                .context("client credentials grant"),
                        })?;

                // Convert to OAuthToken
                let access_token = token_response.access_token().secret().to_string();
                let token_type = token_response.token_type().as_ref().to_string();
                let expires_in = token_response
                    .expires_in()
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(3600);

                let scopes_result = token_response
                    .scopes()
                    .map(|s| s.iter().map(|scope| scope.to_string()).collect())
                    .unwrap_or_else(|| scopes.clone());

                Ok(OAuthToken::new(
                    access_token,
                    token_type,
                    expires_in,
                    None, // No refresh token for M2M
                    scopes_result,
                ))
            })
        })?;

        // Return Bearer token
        Ok(format!("Bearer {}", token.access_token))
    }
}

/// Send an `oauth2`-built token request through the kernel's retrying client.
/// Plugged into `request_async` so the wire format stays the crate's own.
async fn send_token_request(
    http_client: &DatabricksHttpClient,
    request: HttpRequest,
) -> Result<HttpResponse> {
    let request = reqwest::Request::try_from(request).map_err(|e| {
        Error::invalid_argument().message(format!("failed to build token request: {e}"))
    })?;
    let response = http_client
        .execute_without_auth(request, RequestType::AuthTokenRequest)
        .await?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.map_err(|e| {
        Error::network_error().message(format!("failed to read token response: {e}"))
    })?;
    let mut http_response = HttpResponse::new(body.to_vec());
    *http_response.status_mut() = status;
    *http_response.headers_mut() = headers;
    Ok(http_response)
}

/// Classify a client-credentials token-endpoint failure. A status the retry
/// ladder refuses to retry (e.g. 401 `invalid_client`) is a rejection and
/// becomes `Unauthenticated`; anything it retried and gave up on (429, 5xx,
/// network errors) keeps its own code so it isn't reported as bad credentials.
pub(super) fn token_exchange_error(e: Error, what: &str) -> Error {
    let message = format!("{what} failed: {}", e.message);
    match e.http_status {
        Some(status) if is_non_retryable_status(status) => Error::unauthenticated()
            .message(message)
            .with_http_status(status),
        _ => Error { message, ..e },
    }
    .context("client credentials grant")
}

/// Fetch an Azure AD **v1.0** `resource=` client-credentials token directly,
/// tolerating the v1.0 endpoint's quirk of returning `expires_in` (and the
/// other lifetime fields) as JSON *strings* rather than numbers — which the
/// `oauth2` crate's strict `StandardTokenResponse` parser rejects with "Failed
/// to parse server response".
///
/// Credentials go in the form body (`client_secret_post`); the token's `aud`
/// claim is the requested `resource` verbatim (e.g.
/// `https://management.core.windows.net/`, trailing slash included).
async fn fetch_v1_resource_token(
    http_client: &Arc<DatabricksHttpClient>,
    token_endpoint: &str,
    client_id: &str,
    client_secret: &str,
    resource: &str,
) -> Result<OAuthToken> {
    let request = http_client
        .inner()
        .post(token_endpoint)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("resource", resource),
        ])
        .build()
        .map_err(|e| {
            Error::invalid_argument().message(format!("failed to build token request: {e}"))
        })?;

    // Non-2xx responses come back as `Err` once retries are exhausted.
    let response = http_client
        .execute_without_auth(request, RequestType::AuthTokenRequest)
        .await
        .map_err(|e| token_exchange_error(e, "Azure v1.0 resource token exchange"))?;

    let body = response.text().await.map_err(|e| {
        Error::unauthenticated()
            .message(format!(
                "Azure v1.0 resource token: reading response body: {}",
                e
            ))
            .context("client credentials grant")
    })?;

    // Parse leniently: `expires_in` may be a string ("3599") or a number.
    let json: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
        Error::unauthenticated()
            .message(format!("Azure v1.0 resource token: invalid JSON: {}", e))
            .context("client credentials grant")
    })?;
    let access_token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Error::unauthenticated()
                .message("Azure v1.0 resource token: response missing access_token")
                .context("client credentials grant")
        })?
        .to_string();
    let token_type = json
        .get("token_type")
        .and_then(|v| v.as_str())
        .unwrap_or("Bearer")
        .to_string();
    let expires_in = json
        .get("expires_in")
        .and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| v.as_i64())
        })
        .unwrap_or(3600);

    Ok(OAuthToken::new(
        access_token,
        token_type,
        expires_in,
        None, // No refresh token for M2M
        vec![resource.to_string()],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::http::HttpClientConfig;
    use crate::client::retry::{build_retry_configs, RetryConfig};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// HTTP client whose retry ladder uses millisecond waits.
    fn fast_retry_http_client(max_retries: u32) -> Arc<DatabricksHttpClient> {
        let retry = RetryConfig {
            min_wait: Duration::from_millis(10),
            max_wait: Duration::from_millis(50),
            max_retries,
            ..RetryConfig::default()
        };
        Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig::default(),
                build_retry_configs(&retry, &HashMap::new()),
            )
            .expect("Failed to create HTTP client"),
        )
    }

    /// M2M provider pointed straight at `{server}/oidc/v1/token`, skipping discovery.
    async fn provider_at(
        mock_server: &MockServer,
        http_client: Arc<DatabricksHttpClient>,
    ) -> ClientCredentialsProvider {
        ClientCredentialsProvider::new_with_full_config(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
            vec!["all-apis".to_string()],
            Some(format!("{}/oidc/v1/token", mock_server.uri())),
        )
        .await
        .expect("Failed to create provider")
    }

    /// Helper to create a mock OIDC discovery response
    fn mock_oidc_discovery(mock_server: &MockServer) -> String {
        let token_endpoint = format!("{}/oidc/v1/token", mock_server.uri());
        let _auth_endpoint = format!("{}/oidc/v1/authorize", mock_server.uri());

        // Return the token endpoint for test verification
        token_endpoint
    }

    /// Helper to mock a successful token response
    fn mock_token_response_body() -> serde_json::Value {
        serde_json::json!({
            "access_token": "test-access-token-m2m",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "all-apis"
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_token_exchange() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint - verify grant_type and Basic auth
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .and(header("content-type", "application/x-www-form-urlencoded"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Create HTTP client and provider
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let provider = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await
        .expect("Failed to create provider");

        // Get auth header (should trigger token exchange)
        let auth_header = provider
            .get_auth_header()
            .expect("Failed to get auth header");

        assert_eq!(auth_header, "Bearer test-access-token-m2m");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn v1_resource_grant_tolerates_string_expires_in() {
        // The Azure AD v1.0 `/oauth2/token` endpoint returns `expires_in` (and
        // the other lifetime fields) as JSON *strings*, which the `oauth2`
        // crate's strict parser rejects with "Failed to parse server response".
        // `new_with_resource_v1` fetches directly and parses leniently. It also
        // sends the credentials + `resource=` in the form body (no OIDC
        // discovery, no scope).
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("management.core.windows.net"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token_type": "Bearer",
                "expires_in": "3599",   // v1.0 quirk: string, not number
                "ext_expires_in": "3599",
                "expires_on": "1787294104",
                "not_before": "1787290204",
                "resource": "https://management.core.windows.net/",
                "access_token": "mgmt-access-token",
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );
        let provider = ClientCredentialsProvider::new_with_resource_v1(
            "sp-client-id",
            "sp-client-secret",
            http_client,
            "https://management.core.windows.net/".to_string(),
            format!("{}/tenant-abc/oauth2/token", mock_server.uri()),
        )
        .await
        .expect("provider builds");

        assert_eq!(
            provider.get_auth_header().expect("get_auth_header"),
            "Bearer mgmt-access-token"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_auto_refresh() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Use a counter to return different responses on subsequent calls
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        // Mock token endpoint - will be called twice (initial + refresh)
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(move |_req: &wiremock::Request| {
                let count = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count == 0 {
                    // First call - return short-lived token
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "access_token": "initial-token",
                        "token_type": "Bearer",
                        "expires_in": 1, // Very short expiry to trigger refresh
                        "scope": "all-apis"
                    }))
                } else {
                    // Subsequent calls - return long-lived token
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "access_token": "refreshed-token",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "scope": "all-apis"
                    }))
                }
            })
            .expect(2)
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let provider = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await
        .expect("Failed to create provider");

        // First call - gets initial token
        let auth_header1 = provider
            .get_auth_header()
            .expect("Failed to get initial auth header");
        assert_eq!(auth_header1, "Bearer initial-token");

        // Wait for token to expire (1 second + buffer for expiry check)
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Second call - should trigger refresh due to expiry
        let auth_header2 = provider
            .get_auth_header()
            .expect("Failed to get refreshed auth header");
        assert_eq!(auth_header2, "Bearer refreshed-token");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_oidc_discovery() {
        let mock_server = MockServer::start().await;

        // Mock OIDC discovery with specific endpoints
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": "https://custom.example.com/auth",
                "token_endpoint": "https://custom.example.com/token",
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Provider creation should discover endpoints
        let result = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await;

        assert!(
            result.is_ok(),
            "OIDC discovery should succeed: {:?}",
            result
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_oidc_discovery_failure() {
        let mock_server = MockServer::start().await;

        // Mock OIDC discovery returning 500 error
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal server error"))
            .mount(&mock_server)
            .await;

        // This test covers OIDC error propagation, not retry timing. A
        // production retry ladder turns the one mocked 500 into more than a
        // minute of real backoff sleeps.
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig::default(),
                build_retry_configs(&retry, &HashMap::new()),
            )
            .expect("Failed to create HTTP client"),
        );

        // Provider creation should fail with descriptive error
        let result = ClientCredentialsProvider::new(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
        )
        .await;

        assert!(result.is_err(), "OIDC discovery failure should propagate");
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(
            error_msg.contains("500"),
            "Error should mention HTTP 500: {}",
            error_msg
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_concurrent_token_fetch() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint - should only be called once despite concurrent requests
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .expect(1) // Verify only one token fetch occurs
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let provider = Arc::new(
            ClientCredentialsProvider::new(
                &mock_server.uri(),
                "test-client-id",
                "test-client-secret",
                http_client,
            )
            .await
            .expect("Failed to create provider"),
        );

        // Spawn multiple OS threads (not tokio tasks) to test concurrency.
        // get_auth_header() is sync and uses block_in_place + block_on internally,
        // which would deadlock if called from multiple tokio tasks on the same runtime.
        // Each thread enters the tokio runtime context via Handle::enter().
        let handle = tokio::runtime::Handle::current();
        let mut handles = vec![];
        for _ in 0..10 {
            let provider = provider.clone();
            let handle = handle.clone();
            let thread = std::thread::spawn(move || {
                let _guard = handle.enter();
                provider
                    .get_auth_header()
                    .expect("Failed to get auth header")
            });
            handles.push(thread);
        }

        // Wait for all threads and collect results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.join().expect("Thread panicked"));
        }

        // All should get the same token
        for result in &results {
            assert_eq!(result, "Bearer test-access-token-m2m");
        }

        // Verify mock expectations (only 1 token fetch)
        // This is verified by the .expect(1) on the Mock
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn live_providers_with_the_same_credentials_share_discovery_and_token() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);
        let discovery = Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount_as_scoped(&mock_server)
            .await;
        let token = Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .mount_as_scoped(&mock_server)
            .await;

        let uri = mock_server.uri();
        let providers: Vec<_> = futures::future::join_all(
            (0..8).map(|_| shared_test_provider(&uri, "shared-client", "s1", &["all-apis"])),
        )
        .await;
        for p in &providers {
            assert_eq!(auth_header(p).await, "Bearer test-access-token-m2m");
        }
        assert_eq!(discovery.received_requests().await.len(), 1);
        assert_eq!(token.received_requests().await.len(), 1);
        assert_eq!(registered(&uri), 1);

        drop(providers);
        assert_eq!(registered(&uri), 0, "entry removed with its last provider");
        let after_close = shared_test_provider(&uri, "shared-client", "s1", &["all-apis"]).await;
        auth_header(&after_close).await;
        assert_eq!(
            token.received_requests().await.len(),
            2,
            "token not retained"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn providers_with_different_credentials_never_share() {
        let servers = [MockServer::start().await, MockServer::start().await];
        let mut token_mocks = Vec::new();
        for server in &servers {
            Mock::given(method("GET"))
                .and(path("/oidc/.well-known/oauth-authorization-server"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "authorization_endpoint": format!("{}/oidc/v1/authorize", server.uri()),
                    "token_endpoint": format!("{}/oidc/v1/token", server.uri()),
                })))
                .mount(server)
                .await;
            token_mocks.push(
                Mock::given(method("POST"))
                    .and(path("/oidc/v1/token"))
                    .respond_with(
                        ResponseTemplate::new(200).set_body_json(mock_token_response_body()),
                    )
                    .mount_as_scoped(server)
                    .await,
            );
        }
        let (a, b) = (servers[0].uri(), servers[1].uri());
        let providers = vec![
            shared_test_provider(&a, "client", "secret", &["all-apis"]).await,
            shared_test_provider(&a, "client", "other-secret", &["all-apis"]).await,
            shared_test_provider(&a, "other-client", "secret", &["all-apis"]).await,
            shared_test_provider(&a, "client", "secret", &["sql"]).await,
            shared_test_provider(&b, "client", "secret", &["all-apis"]).await,
            // Scope order doesn't matter.
            shared_test_provider(&b, "client", "secret", &["sql", "all-apis"]).await,
            shared_test_provider(&b, "client", "secret", &["all-apis", "sql"]).await,
        ];
        for p in &providers {
            auth_header(p).await;
        }
        assert_eq!(token_mocks[0].received_requests().await.len(), 4);
        assert_eq!(token_mocks[1].received_requests().await.len(), 2);
        assert_eq!(registered(&a), 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn providers_with_different_transports_never_share() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": format!("{}/oidc/v1/token", mock_server.uri()),
            })))
            .mount(&mock_server)
            .await;
        let token = Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .mount_as_scoped(&mock_server)
            .await;
        let uri = mock_server.uri();
        let strict = shared_test_provider(&uri, "client", "secret", &["all-apis"]).await;
        let mut relaxed_config = HttpClientConfig::default();
        relaxed_config.tls.allow_self_signed = Some(true);
        let relaxed_client = Arc::new(
            DatabricksHttpClient::with_default_retry(relaxed_config)
                .expect("Failed to create HTTP client"),
        );
        let relaxed = Arc::new(
            ClientCredentialsProvider::new(&uri, "client", "secret", relaxed_client)
                .await
                .expect("Failed to create provider"),
        );
        auth_header(&strict).await;
        auth_header(&relaxed).await;
        assert_eq!(token.received_requests().await.len(), 2);
        assert_eq!(registered(&uri), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_opens_share_a_failed_discovery() {
        let mock_server = MockServer::start().await;
        let discovery = Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string("Internal server error")
                    .set_delay(Duration::from_millis(300)),
            )
            .mount_as_scoped(&mock_server)
            .await;
        let uri = mock_server.uri();
        let client = || {
            let retry = RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            };
            Arc::new(
                DatabricksHttpClient::new(
                    HttpClientConfig::default(),
                    build_retry_configs(&retry, &HashMap::new()),
                )
                .expect("Failed to create HTTP client"),
            )
        };
        // Clients first, so every open is in flight before discovery answers.
        let clients: Vec<_> = (0..8).map(|_| client()).collect();
        let results = futures::future::join_all(
            clients
                .into_iter()
                .map(|c| ClientCredentialsProvider::new(&uri, "client", "secret", c)),
        )
        .await;
        for result in results {
            assert!(format!("{:?}", result.unwrap_err()).contains("500"));
        }
        assert_eq!(discovery.received_requests().await.len(), 1);

        // An entry that stays alive across the failure, as one does while
        // other sessions with the same credentials are open, replaces the
        // failed attempt instead of handing it out again.
        let shared = shared_credentials(CredentialsKey {
            host: uri.clone(),
            client_id: "held".to_string(),
            secret_sha256: [0; 32],
            scopes: vec![],
            token_endpoint_override: None,
            transport: [0; 32],
        });
        let c = client();
        assert!(shared.endpoints(&uri, &c).await.is_err());
        assert_eq!(discovery.received_requests().await.len(), 2);
        assert!(shared.endpoints(&uri, &c).await.is_err());
        assert_eq!(
            discovery.received_requests().await.len(),
            3,
            "a failed discovery held by a live entry is retried"
        );
    }

    /// Sessions in the Python binding each own a runtime: an opener that
    /// joins a discovery started on a runtime that then shuts down must still
    /// open, not hang or fail on the abandoned request.
    #[test]
    fn discovery_outlives_the_runtime_that_started_it() {
        let runtime = || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap()
        };
        let (first, second) = (runtime(), runtime());
        let server = second.block_on(MockServer::start());
        let uri = server.uri();
        let body = serde_json::json!({
            "authorization_endpoint": format!("{uri}/oidc/v1/authorize"),
            "token_endpoint": format!("{uri}/oidc/v1/token"),
        });
        second.block_on(async {
            Mock::given(method("GET"))
                .and(path("/oidc/.well-known/oauth-authorization-server"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(body.clone())
                        .set_delay(Duration::from_secs(2)),
                )
                .up_to_n_times(1)
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/oidc/.well-known/oauth-authorization-server"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        });

        // Held here so the second open joins the first one's attempt.
        let shared = shared_credentials(CredentialsKey {
            host: uri.clone(),
            client_id: "client".to_string(),
            secret_sha256: [0; 32],
            scopes: vec![],
            token_endpoint_override: None,
            transport: [0; 32],
        });
        let client = || {
            Arc::new(
                DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                    .expect("Failed to create HTTP client"),
            )
        };
        let (s, u, c) = (Arc::clone(&shared), uri.clone(), client());
        first.spawn(async move {
            let _ = s.endpoints(&u, &c).await;
        });
        // Shut down only once the first open's attempt is in the slot and
        // unresolved, so the second open has to join it.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !shared
            .discovery
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|d| d.peek().is_none())
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the first open never started its discovery"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        first.shutdown_background();

        let joined = second.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), shared.endpoints(&uri, &client())).await
        });
        let endpoints = joined
            .expect("hung on the abandoned discovery")
            .expect("failed on the abandoned discovery");
        assert_eq!(endpoints.0, format!("{uri}/oidc/v1/token"));
    }

    /// Azure SP data tokens go through the token-endpoint override (no
    /// discovery), and share the same way.
    #[tokio::test(flavor = "multi_thread")]
    async fn providers_with_a_token_endpoint_override_share_one_token() {
        let mock_server = MockServer::start().await;
        let token = Mock::given(method("POST"))
            .and(path("/oauth2/v2.0/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(mock_token_response_body()))
            .mount_as_scoped(&mock_server)
            .await;
        let token_url = format!("{}/oauth2/v2.0/token", mock_server.uri());
        let mut providers = Vec::new();
        for _ in 0..4 {
            let http_client = Arc::new(
                DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                    .expect("Failed to create HTTP client"),
            );
            providers.push(Arc::new(
                ClientCredentialsProvider::new_with_full_config(
                    "https://adb-1.azuredatabricks.net",
                    "client",
                    "secret",
                    http_client,
                    vec!["scope/.default".to_string()],
                    Some(token_url.clone()),
                )
                .await
                .expect("Failed to create provider"),
            ));
        }
        for p in &providers {
            auth_header(p).await;
        }
        assert_eq!(token.received_requests().await.len(), 1);
    }

    async fn shared_test_provider(
        host: &str,
        client_id: &str,
        secret: &str,
        scopes: &[&str],
    ) -> Arc<ClientCredentialsProvider> {
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );
        let scopes = scopes.iter().map(|s| s.to_string()).collect();
        Arc::new(
            ClientCredentialsProvider::new_with_scopes(
                host,
                client_id,
                secret,
                http_client,
                scopes,
            )
            .await
            .expect("Failed to create provider"),
        )
    }

    async fn auth_header(p: &Arc<ClientCredentialsProvider>) -> String {
        let p = Arc::clone(p);
        tokio::task::spawn_blocking(move || p.get_auth_header().expect("auth header"))
            .await
            .unwrap()
    }

    fn registered(host: &str) -> usize {
        let registry = registry().lock().unwrap();
        registry.keys().filter(|k| k.host == host).count()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_custom_scopes() {
        let mock_server = MockServer::start().await;
        let token_endpoint = mock_oidc_discovery(&mock_server);

        // Mock OIDC discovery
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": token_endpoint,
            })))
            .mount(&mock_server)
            .await;

        // Mock token endpoint
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "test-token-custom-scopes",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": "custom-scope-1 custom-scope-2"
            })))
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Create provider with custom scopes
        let provider = ClientCredentialsProvider::new_with_scopes(
            &mock_server.uri(),
            "test-client-id",
            "test-client-secret",
            http_client,
            vec!["custom-scope-1".to_string(), "custom-scope-2".to_string()],
        )
        .await
        .expect("Failed to create provider");

        let auth_header = provider
            .get_auth_header()
            .expect("Failed to get auth header");

        assert_eq!(auth_header, "Bearer test-token-custom-scopes");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_m2m_override_skips_discovery() {
        // When a token-endpoint override is set (e.g. an Azure/Entra endpoint,
        // which is NOT the workspace's), the client-credentials grant never
        // uses the authorization endpoint, so OIDC discovery against the
        // workspace must be skipped entirely — no needless round-trip, and no
        // dependency on the workspace OIDC config describing the right issuer.
        let mock_server = MockServer::start().await;

        // Discovery must NOT be hit.
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": format!("{}/oidc/v1/token", mock_server.uri()),
            })))
            .expect(0)
            .mount(&mock_server)
            .await;

        // The exchange lands on the override endpoint with the configured
        // (Azure resource) scope, via HTTP Basic.
        Mock::given(method("POST"))
            .and(path("/azure/tenant/oauth2/v2.0/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("2ff814a6"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "azure-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let token_endpoint = format!("{}/azure/tenant/oauth2/v2.0/token", mock_server.uri());
        let provider = ClientCredentialsProvider::new_with_full_config(
            &mock_server.uri(),
            "azure-client-id",
            "azure-client-secret",
            http_client,
            vec!["2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default".to_string()],
            Some(token_endpoint),
        )
        .await
        .expect("provider creation must not require discovery when overridden");

        let auth_header = provider.get_auth_header().expect("get auth header");
        assert_eq!(auth_header, "Bearer azure-access-token");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2m_token_exchange_retries_transient_failures() {
        use base64::Engine as _;
        let mock_server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();

        // Every attempt must carry the same `oauth2`-encoded request:
        // HTTP Basic client auth plus the client-credentials form body.
        let basic =
            base64::engine::general_purpose::STANDARD.encode("test-client-id:test-client-secret");
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .and(header("authorization", format!("Basic {basic}").as_str()))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("scope=all-apis"))
            .respond_with(move |_req: &wiremock::Request| {
                match counter.fetch_add(1, Ordering::SeqCst) {
                    0 => ResponseTemplate::new(429),
                    1 => ResponseTemplate::new(503),
                    _ => ResponseTemplate::new(200).set_body_json(mock_token_response_body()),
                }
            })
            .mount(&mock_server)
            .await;

        let provider = provider_at(&mock_server, fast_retry_http_client(3)).await;

        assert_eq!(
            provider
                .get_auth_header()
                .expect("retried exchange succeeds"),
            "Bearer test-access-token-m2m"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2m_token_exchange_does_not_retry_rejected_credentials() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "invalid_client",
                "error_description": "Client authentication failed",
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let provider = provider_at(&mock_server, fast_retry_http_client(3)).await;

        let err = provider
            .get_auth_header()
            .expect_err("bad credentials fail");
        assert_eq!(err.code, ErrorCode::Unauthenticated);
        assert_eq!(err.http_status, Some(401));
        assert!(err.message.contains("invalid_client"), "{}", err.message);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2m_token_exchange_exhausted_throttling_is_not_an_auth_error() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(429))
            .expect(3) // 1 attempt + 2 retries
            .mount(&mock_server)
            .await;

        let provider = provider_at(&mock_server, fast_retry_http_client(2)).await;

        let err = provider
            .get_auth_header()
            .expect_err("throttled exchange fails");
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
        assert_eq!(err.http_status, Some(429));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2m_token_exchange_exhausted_retryable_4xx_is_not_an_auth_error() {
        // 408 is a 4xx the retry ladder retries, so it is transient, not a
        // credentials rejection.
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(408))
            .expect(3) // 1 attempt + 2 retries
            .mount(&mock_server)
            .await;

        let provider = provider_at(&mock_server, fast_retry_http_client(2)).await;

        let err = provider
            .get_auth_header()
            .expect_err("timed-out exchange fails");
        assert_ne!(err.code, ErrorCode::Unauthenticated);
        assert_eq!(err.http_status, Some(408));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn v1_resource_grant_retries_transient_failures() {
        let mock_server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("management.core.windows.net"))
            .respond_with(move |_req: &wiremock::Request| {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(503)
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "token_type": "Bearer",
                        "expires_in": "3599",
                        "access_token": "mgmt-access-token",
                    }))
                }
            })
            .mount(&mock_server)
            .await;

        let provider = ClientCredentialsProvider::new_with_resource_v1(
            "sp-client-id",
            "sp-client-secret",
            fast_retry_http_client(3),
            "https://management.core.windows.net/".to_string(),
            format!("{}/tenant-abc/oauth2/token", mock_server.uri()),
        )
        .await
        .expect("provider builds");

        assert_eq!(
            provider
                .get_auth_header()
                .expect("retried exchange succeeds"),
            "Bearer mgmt-access-token"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
