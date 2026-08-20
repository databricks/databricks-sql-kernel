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

//! U2M (User-to-Machine) OAuth authentication using authorization code + PKCE flow.
//!
//! This module implements the OAuth 2.0 authorization code grant with PKCE for
//! interactive browser-based authentication. The flow:
//!
//! 1. Try loading cached token from disk (~/.config/databricks-sql-kernel/oauth/)
//! 2. If cached token has valid refresh_token, use it (refresh if stale/expired)
//! 3. If no cache or refresh token expired, launch browser flow:
//!    a. Generate PKCE challenge/verifier
//!    b. Start local callback server
//!    c. Open browser with authorization URL
//!    d. Wait for callback with authorization code
//!    e. Exchange code for access token
//! 4. Save token to cache after every acquisition/refresh
//!
//! U2M tokens include a `refresh_token` that can be used to obtain new access tokens
//! without re-launching the browser, until the refresh token itself expires.
//!
//! Steps 1 and 4 are skipped entirely when the on-disk cache is disabled
//! (see [`crate::config::TokenCacheConfig`]); the in-memory token store
//! still handles reuse and refresh for the lifetime of the session. When
//! the cache IS enabled the payload is always encrypted under the
//! configured passphrase, because it carries that long-lived refresh token.

use crate::auth::oauth::cache::TokenCache;
use crate::auth::oauth::callback::CallbackServer;
use crate::auth::oauth::oidc::OidcEndpoints;
use crate::auth::oauth::token::OAuthToken;
use crate::auth::oauth::token_store::TokenStore;
use crate::auth::AuthProvider;
use crate::client::http::DatabricksHttpClient;
use crate::kernel_error::{Error, Result};
use crate::util::runtime::block_on_in_runtime;
use oauth2::basic::BasicClient;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, CsrfToken, PkceCodeChallenge, RedirectUrl, RefreshToken,
    Scope, TokenResponse, TokenUrl,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle as RuntimeHandle;

/// Default callback server port for OAuth redirects. Matches the redirect
/// URI registered for the kernel's default U2M OAuth app
/// (`databricks-sql-connector`, `http://localhost:8030`).
const DEFAULT_CALLBACK_PORT: u16 = 8030;

/// Default timeout for waiting for the OAuth callback.
const DEFAULT_CALLBACK_TIMEOUT: Duration = Duration::from_secs(120);

/// Persist `token` to the on-disk cache, or do nothing when disk caching
/// is disabled (`passphrase` is `None`).
///
/// Exists so the two save sites in the token-acquisition flow (post-refresh
/// and post-browser-flow) cannot drift apart, in particular so neither can
/// accidentally write an unencrypted token or write one when the user asked
/// for no cache at all.
fn save_to_cache(
    passphrase: Option<&str>,
    host: &str,
    client_id: &str,
    scopes: &[String],
    token: &OAuthToken,
) {
    match passphrase {
        Some(passphrase) => TokenCache::save(host, client_id, scopes, token, passphrase),
        None => crate::klog::debug!("Token cache disabled; not persisting token to disk"),
    }
}

/// OAuth provider for U2M (User-to-Machine) authentication.
///
/// Uses the OAuth 2.0 authorization code grant with PKCE to authenticate users
/// via browser-based login. The provider:
/// - Attempts to load cached tokens from disk on creation
/// - Falls back to browser flow if no cache or refresh token expired
/// - Uses TokenStore for automatic token refresh lifecycle
/// - Persists tokens to disk after every acquisition/refresh
///
/// # Token Lifecycle
///
/// - **Cache hit with valid refresh_token**: Token loaded from cache, stored in TokenStore
/// - **Cache hit but refresh_token expired**: Falls back to full browser flow
/// - **Cache miss**: Full browser flow (PKCE + authorization code exchange)
/// - **Fresh token**: Returned immediately from TokenStore (no HTTP call)
/// - **Stale token**: Returned immediately, background refresh spawned
/// - **Expired token**: Blocking refresh via refresh_token exchange
///
/// # Example
///
/// ```no_run
/// use databricks_sql_kernel::auth::AuthProvider;
/// use databricks_sql_kernel::auth::oauth::u2m::AuthorizationCodeProvider;
/// use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig};
/// use std::sync::Arc;
///
/// # async fn example() -> databricks_sql_kernel::Result<()> {
/// let http_client = Arc::new(DatabricksHttpClient::with_default_retry(HttpClientConfig::default())?);
/// let provider = AuthorizationCodeProvider::new(
///     "https://my-workspace.cloud.databricks.com",
///     "my-client-id",
///     http_client,
/// ).await?;
///
/// // Get auth header (may launch browser on first call if no cached token)
/// let auth_header = provider.get_auth_header()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct AuthorizationCodeProvider {
    /// Databricks workspace host
    host: String,
    /// OAuth client ID
    client_id: String,
    /// Discovered OIDC endpoints
    token_endpoint: String,
    auth_endpoint: String,
    /// Thread-safe token store managing the token lifecycle
    token_store: TokenStore,
    /// HTTP client for token endpoint requests
    http_client: Arc<DatabricksHttpClient>,
    /// OAuth scopes to request (typically "all-apis offline_access")
    scopes: Vec<String>,
    /// Candidate callback-server ports, tried in order (first free binds).
    callback_ports: Vec<u16>,
    /// Callback timeout
    callback_timeout: Duration,
    /// Whether to open browser automatically (set to false in tests)
    open_browser: bool,
    /// Passphrase for the on-disk token cache, or `None` when disk
    /// caching is disabled. `Some` implies the cache is enabled: the
    /// enabled-without-passphrase combination is rejected upstream in
    /// [`crate::config::TokenCacheConfig::resolve`], so this field carries
    /// both the "should I cache?" and "with what key?" decisions.
    cache_passphrase: Option<String>,
    /// Tokio runtime handle captured from the constructing context;
    /// used to drive the async token exchange from the sync
    /// `get_auth_header` trait method safely whether or not the
    /// caller is already inside a runtime (see
    /// `crate::util::runtime::block_on_in_runtime`).
    runtime_handle: RuntimeHandle,
}

impl AuthorizationCodeProvider {
    /// Creates a new U2M authentication provider.
    ///
    /// Discovers the OIDC endpoints. The first call to `get_auth_header()`
    /// launches the browser flow; the resulting token is then held in memory
    /// and refreshed automatically when stale/expired.
    ///
    /// The on-disk token cache is **disabled** here — persisting a refresh
    /// token requires an encryption passphrase, which this signature cannot
    /// accept. Use [`Self::new_with_full_config`] (as
    /// [`Session::open`](crate::Session::open) does) to enable it.
    ///
    /// # Arguments
    ///
    /// * `host` - Databricks workspace URL (e.g., "https://my-workspace.cloud.databricks.com")
    /// * `client_id` - OAuth client ID for the application
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
    /// # use databricks_sql_kernel::auth::oauth::u2m::AuthorizationCodeProvider;
    /// # use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig};
    /// # use std::sync::Arc;
    /// # async fn example() -> databricks_sql_kernel::Result<()> {
    /// let http_client = Arc::new(DatabricksHttpClient::with_default_retry(HttpClientConfig::default())?);
    /// let provider = AuthorizationCodeProvider::new(
    ///     "https://my-workspace.cloud.databricks.com",
    ///     "client-id",
    ///     http_client,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        host: &str,
        client_id: &str,
        http_client: Arc<DatabricksHttpClient>,
    ) -> Result<Self> {
        // Scopes must match the app registered for `DEFAULT_CALLBACK_PORT`
        // (8030 → `databricks-sql-connector`, whose redirect URI is registered
        // for `sql offline_access`). Keeping these coherent means the `new()`
        // default bundle describes one OAuth app, not two. Callers wiring a
        // different app must use `new_with_config` and override the whole
        // coupled {scopes, port} bundle. See PECOBLR-4039.
        Self::new_with_config(
            host,
            client_id,
            http_client,
            vec!["sql".to_string(), "offline_access".to_string()],
            DEFAULT_CALLBACK_PORT,
            DEFAULT_CALLBACK_TIMEOUT,
        )
        .await
    }

    /// Creates a new U2M provider with custom configuration.
    ///
    /// This is useful for testing or when specific scopes/ports are required.
    /// Most users should use `new()` which provides sensible defaults.
    ///
    /// The on-disk token cache is **disabled** for providers built this way:
    /// encrypting it requires a passphrase, and this signature has no way to
    /// accept one. Use [`Self::new_with_full_config`] to enable it.
    pub async fn new_with_config(
        host: &str,
        client_id: &str,
        http_client: Arc<DatabricksHttpClient>,
        scopes: Vec<String>,
        callback_port: u16,
        callback_timeout: Duration,
    ) -> Result<Self> {
        Self::new_with_full_config(
            host,
            client_id,
            http_client,
            scopes,
            vec![callback_port],
            callback_timeout,
            None, // No token_endpoint override (discover)
            None, // On-disk cache disabled: no passphrase available here
        )
        .await
    }

    /// Creates a new U2M provider with full configuration including optional token endpoint override.
    ///
    /// This is used by Database when a custom token endpoint is configured.
    /// Most users should use `new()` or `new_with_config()` which use OIDC discovery.
    ///
    /// `cache_passphrase` is `Some` to enable the encrypted on-disk token
    /// cache and `None` to disable it; the enabled-but-passphraseless case is
    /// rejected upstream by [`crate::config::TokenCacheConfig::resolve`].
    // The "full config" constructor is a flat parameter list by design — it is
    // the single place every U2M knob is threaded in from `Session::open`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_full_config(
        host: &str,
        client_id: &str,
        http_client: Arc<DatabricksHttpClient>,
        scopes: Vec<String>,
        callback_ports: Vec<u16>,
        callback_timeout: Duration,
        token_endpoint_override: Option<String>,
        cache_passphrase: Option<String>,
    ) -> Result<Self> {
        // Resolve the authorization + token endpoints. When the token endpoint
        // is overridden (only the Azure `azure-oauth` flow does this, pinning
        // the workspace v2.0 `{host}/oidc/oauth2/v2.0/token` path), skip OIDC
        // discovery and derive the authorize endpoint from it by swapping the
        // trailing `/token` for `/authorize`. This avoids a separate authorize
        // override and the discovered `{host}/oidc/v1/authorize` path, which
        // redirects to a malformed Entra URL for the Azure client. Otherwise
        // discover both from the workspace OIDC config.
        let (token_endpoint, auth_endpoint) = match token_endpoint_override {
            Some(token) => {
                let auth = token
                    .strip_suffix("/token")
                    .map(|base| format!("{base}/authorize"))
                    .unwrap_or_else(|| token.clone());
                (token, auth)
            }
            None => {
                let endpoints = OidcEndpoints::discover(host, &http_client).await?;
                (endpoints.token_endpoint, endpoints.authorization_endpoint)
            }
        };

        // Create token store
        let token_store = TokenStore::new();

        // Try loading cached token. A `None` passphrase means the on-disk
        // cache is disabled, so the file is never even read — the in-memory
        // TokenStore above still reuses and refreshes within this process.
        if let Some(cached_token) = cache_passphrase
            .as_deref()
            .and_then(|passphrase| TokenCache::load(host, client_id, &scopes, passphrase))
        {
            // Check if the cached token has a refresh token
            if cached_token.refresh_token.is_some() {
                crate::klog::debug!("Loaded cached token with refresh_token for U2M flow");
                // Store the cached token - TokenStore will handle refresh if needed
                // We need to manually insert it since get_or_refresh expects to call the closure
                // Let's trigger a get_or_refresh with a closure that returns the cached token
                let _ = token_store.get_or_refresh(move || Ok(cached_token.clone()));
            } else {
                crate::klog::debug!("Cached token has no refresh_token, will trigger browser flow");
            }
        } else {
            crate::klog::debug!(
                "No cached token found for U2M flow, will trigger browser flow on first auth"
            );
        }

        Ok(Self {
            host: host.to_string(),
            client_id: client_id.to_string(),
            token_endpoint,
            auth_endpoint,
            token_store,
            http_client,
            scopes,
            callback_ports,
            callback_timeout,
            open_browser: true,
            cache_passphrase,
            // `new_with_full_config` is `async`, so we're guaranteed
            // to be inside a tokio runtime here. Capture that handle
            // so `get_auth_header` can dispatch correctly later via
            // `block_on_in_runtime`.
            runtime_handle: RuntimeHandle::current(),
        })
    }
}

impl AuthorizationCodeProvider {
    /// Disables automatic browser opening during the U2M flow.
    ///
    /// This is primarily useful for testing scenarios where the browser
    /// flow fallback should not launch a real browser.
    #[doc(hidden)]
    pub fn suppress_browser(&mut self) {
        self.open_browser = false;
    }
}

impl AuthProvider for AuthorizationCodeProvider {
    /// Returns a valid Bearer token for authentication.
    ///
    /// This method implements the token lifecycle state machine via TokenStore:
    /// - **Empty (no token)**: Triggers browser flow to obtain initial token
    /// - **Fresh**: Returns cached token immediately (no HTTP call)
    /// - **Stale**: Returns cached token immediately, spawns background refresh
    /// - **Expired**: Blocks and refreshes via refresh_token exchange
    ///
    /// If refresh fails (e.g., refresh token expired), falls back to full browser flow.
    ///
    /// The method is safe to call from multiple threads concurrently. Only one
    /// token fetch/refresh will execute at a time, coordinated by TokenStore.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Browser flow fails (user denies consent, network error, etc.)
    /// - Token refresh fails and browser flow fallback also fails
    /// - The token store lock is poisoned (indicates panic in another thread)
    fn get_auth_header(&self) -> Result<String> {
        // Capture variables for the closure
        let host = self.host.clone();
        let client_id = self.client_id.clone();
        let token_endpoint = self.token_endpoint.clone();
        let auth_endpoint = self.auth_endpoint.clone();
        let scopes = self.scopes.clone();
        let callback_ports = self.callback_ports.clone();
        let callback_timeout = self.callback_timeout;
        let open_browser = self.open_browser;
        let http_client = self.http_client.clone();
        let cache_passphrase = self.cache_passphrase.clone();

        // Get or refresh token via TokenStore
        let runtime_handle = self.runtime_handle.clone();
        let token = self.token_store.get_or_refresh(move || {
            // Execute async token fetch/refresh via the kernel's
            // captured runtime handle. `block_on_in_runtime`
            // dispatches on `Handle::try_current()`: `block_in_place`
            // inside a runtime, direct `block_on` otherwise. See
            // `crate::util::runtime`.
            block_on_in_runtime(&runtime_handle, async {
                    // Try loading from cache first (skipped entirely when
                    // on-disk caching is disabled).
                    if let Some(cached_token) = cache_passphrase
                        .as_deref()
                        .and_then(|p| TokenCache::load(&host, &client_id, &scopes, p))
                    {
                        if let Some(ref refresh_token_str) = cached_token.refresh_token {
                            // Try refreshing the token
                            crate::klog::debug!("Attempting to refresh token via refresh_token");

                            // Build OAuth client
                            let oauth_client = BasicClient::new(ClientId::new(client_id.clone()))
                                .set_auth_uri(AuthUrl::new(auth_endpoint.clone()).map_err(|e| {
                                    Error::invalid_argument()
                                        .message(format!("Invalid authorization endpoint URL: {}", e))
                                })?)
                                .set_token_uri(TokenUrl::new(token_endpoint.clone()).map_err(
                                    |e| {
                                        Error::invalid_argument()
                                            .message(format!("Invalid token endpoint URL: {}", e))
                                    },
                                )?);

                            // Try refresh
                            match oauth_client
                                .exchange_refresh_token(&RefreshToken::new(
                                    refresh_token_str.clone(),
                                ))
                                .request_async(http_client.inner())
                                .await
                            {
                                Ok(token_response) => {
                                    crate::klog::debug!("Token refresh successful");
                                    // Convert to OAuthToken
                                    let access_token =
                                        token_response.access_token().secret().to_string();
                                    let token_type =
                                        token_response.token_type().as_ref().to_string();
                                    let expires_in = token_response
                                        .expires_in()
                                        .map(|d| d.as_secs() as i64)
                                        .unwrap_or(3600);

                                    let refresh_token_result = token_response
                                        .refresh_token()
                                        .map(|rt| rt.secret().to_string())
                                        .or_else(|| Some(refresh_token_str.clone()));

                                    let scopes_result = token_response
                                        .scopes()
                                        .map(|s| {
                                            s.iter().map(|scope| scope.to_string()).collect()
                                        })
                                        .unwrap_or_else(|| scopes.clone());

                                    let token = OAuthToken::new(
                                        access_token,
                                        token_type,
                                        expires_in,
                                        refresh_token_result,
                                        scopes_result,
                                    );

                                    // Save to cache (no-op when disabled)
                                    save_to_cache(
                                        cache_passphrase.as_deref(),
                                        &host,
                                        &client_id,
                                        &scopes,
                                        &token,
                                    );
                                    return Ok(token);
                                }
                                Err(e) => {
                                    // We HAD a refresh token and it failed
                                    // (expired / revoked / invalid_grant).
                                    // Do NOT fall through to the interactive
                                    // browser flow: in a headless / long-lived
                                    // server context (no browser, no human)
                                    // that would block this query thread for
                                    // the full callback timeout (~120s) and
                                    // then fail opaquely. Fail fast with an
                                    // auth error instead — matching the Thrift
                                    // connector, whose mid-session refresh path
                                    // (oauth.py check_and_refresh_access_token)
                                    // raises and never launches a browser. The
                                    // browser flow remains reachable only for a
                                    // genuine first connect (no cached token),
                                    // below.
                                    crate::klog::warn!("OAuth token refresh failed: {}", e);
                                    return Err(Error::unauthenticated().message(format!(
                                        "OAuth token refresh failed (refresh token expired or \
                                         revoked); re-authentication required: {e}"
                                    )));
                                }
                            }
                        }
                    }

                    // No cached token or refresh failed - trigger browser flow
                    crate::klog::debug!("Initiating browser-based authorization code flow");

                    // Generate PKCE challenge
                    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

                    // Start callback server, binding the first free candidate
                    // port (busy-port fallback across the registered range).
                    let callback_server = CallbackServer::new_first_free(&callback_ports).await?;
                    let redirect_uri = callback_server.redirect_uri();

                    // Build OAuth client
                    let oauth_client = BasicClient::new(ClientId::new(client_id.clone()))
                        .set_auth_uri(AuthUrl::new(auth_endpoint.clone()).map_err(|e| {
                            Error::invalid_argument()
                                .message(format!("Invalid authorization endpoint URL: {}", e))
                        })?)
                        .set_token_uri(TokenUrl::new(token_endpoint.clone()).map_err(|e| {
                            Error::invalid_argument()
                                .message(format!("Invalid token endpoint URL: {}", e))
                        })?)
                        .set_redirect_uri(RedirectUrl::new(redirect_uri.clone()).map_err(
                            |e| {
                                Error::invalid_argument()
                                    .message(format!("Invalid redirect URI: {}", e))
                            },
                        )?);

                    // Build authorization URL with PKCE
                    let mut auth_url_builder = oauth_client
                        .authorize_url(CsrfToken::new_random)
                        .set_pkce_challenge(pkce_challenge);

                    for scope in &scopes {
                        auth_url_builder =
                            auth_url_builder.add_scope(Scope::new(scope.clone()));
                    }

                    let (auth_url, csrf_state) = auth_url_builder.url();

                    // Launch browser (unless suppressed for testing)
                    if open_browser {
                        crate::klog::info!("Opening browser for OAuth authorization: {}", auth_url);
                        if let Err(e) = open::that(auth_url.as_str()) {
                            crate::klog::warn!("Failed to automatically open browser: {}. Please manually navigate to: {}", e, auth_url);
                        }
                    } else {
                        crate::klog::debug!("Browser opening suppressed, authorization URL: {}", auth_url);
                    }

                    // Wait for callback
                    let authorization_code = callback_server
                        .wait_for_code(csrf_state.secret(), callback_timeout)
                        .await?;

                    // Exchange code for token
                    let token_response = oauth_client
                        .exchange_code(AuthorizationCode::new(authorization_code))
                        .set_pkce_verifier(pkce_verifier)
                        .request_async(http_client.inner())
                        .await
                        .map_err(|e| {
                            Error::unauthenticated()
                                .message(format!("U2M authorization code exchange failed: {}", e))
                                .context("authorization code grant")
                        })?;

                    // Convert to OAuthToken
                    let access_token = token_response.access_token().secret().to_string();
                    let token_type = token_response.token_type().as_ref().to_string();
                    let expires_in = token_response
                        .expires_in()
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(3600);

                    let refresh_token = token_response
                        .refresh_token()
                        .map(|rt| rt.secret().to_string());

                    let scopes_result = token_response
                        .scopes()
                        .map(|s| s.iter().map(|scope| scope.to_string()).collect())
                        .unwrap_or_else(|| scopes.clone());

                    let token = OAuthToken::new(
                        access_token,
                        token_type,
                        expires_in,
                        refresh_token,
                        scopes_result,
                    );

                    // Save to cache (no-op when disabled)
                    save_to_cache(
                        cache_passphrase.as_deref(),
                        &host,
                        &client_id,
                        &scopes,
                        &token,
                    );

                    Ok(token)
            })
        })?;

        // Return Bearer token
        Ok(format!("Bearer {}", token.access_token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::http::HttpClientConfig;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Passphrase used by the cache-backed tests below. These tests seed the
    /// on-disk cache and then assert the provider picks it up, so they must
    /// build the provider with disk caching ENABLED (via
    /// `new_with_full_config`) and with the same passphrase used to write —
    /// otherwise the load silently misses and the test would pass while
    /// proving nothing.
    const TEST_CACHE_PASSPHRASE: &str = "unit-test-passphrase";

    /// Helper to mock OIDC discovery response
    async fn mock_oidc_discovery(mock_server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": format!("{}/oidc/v1/token", mock_server.uri()),
            })))
            .mount(mock_server)
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_u2m_refresh_token_flow() {
        let mock_server = MockServer::start().await;
        mock_oidc_discovery(&mock_server).await;

        // Mock token endpoint - expects grant_type=refresh_token
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "refreshed-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "new-refresh-token",
                "scope": "all-apis offline_access"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Create a cached token with refresh_token
        let cached_token = OAuthToken::new(
            "old-access-token".to_string(),
            "Bearer".to_string(),
            1, // Expire immediately to trigger refresh
            Some("old-refresh-token".to_string()),
            vec!["all-apis".to_string(), "offline_access".to_string()],
        );

        // Save it to cache
        TokenCache::save(
            &mock_server.uri(),
            "test-client-id",
            &["all-apis".to_string(), "offline_access".to_string()],
            &cached_token,
            TEST_CACHE_PASSPHRASE,
        );

        // Create provider - should load cached token
        let provider = AuthorizationCodeProvider::new_with_full_config(
            &mock_server.uri(),
            "test-client-id",
            http_client,
            vec!["all-apis".to_string(), "offline_access".to_string()],
            vec![8020],
            Duration::from_secs(120),
            None,
            Some(TEST_CACHE_PASSPHRASE.to_string()),
        )
        .await
        .expect("Failed to create provider");

        // Wait a bit to ensure token expires
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Get auth header - should trigger refresh
        let auth_header = provider
            .get_auth_header()
            .expect("Failed to get auth header");

        assert_eq!(auth_header, "Bearer refreshed-access-token");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_u2m_cache_hit() {
        let mock_server = MockServer::start().await;
        mock_oidc_discovery(&mock_server).await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Create a fresh cached token (long expiry)
        let cached_token = OAuthToken::new(
            "cached-access-token".to_string(),
            "Bearer".to_string(),
            3600, // 1 hour - should be FRESH
            Some("cached-refresh-token".to_string()),
            vec!["all-apis".to_string(), "offline_access".to_string()],
        );

        // Save it to cache
        TokenCache::save(
            &mock_server.uri(),
            "test-client-id",
            &["all-apis".to_string(), "offline_access".to_string()],
            &cached_token,
            TEST_CACHE_PASSPHRASE,
        );

        // Create provider - should load cached token
        let provider = AuthorizationCodeProvider::new_with_full_config(
            &mock_server.uri(),
            "test-client-id",
            http_client,
            vec!["all-apis".to_string(), "offline_access".to_string()],
            vec![8020],
            Duration::from_secs(120),
            None,
            Some(TEST_CACHE_PASSPHRASE.to_string()),
        )
        .await
        .expect("Failed to create provider");

        // Get auth header - should return cached token without any HTTP calls
        let auth_header = provider
            .get_auth_header()
            .expect("Failed to get auth header");

        assert_eq!(auth_header, "Bearer cached-access-token");
        // No HTTP mocks for token endpoint - if it tries to refresh, the test will fail
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_u2m_cache_miss_with_expired_refresh() {
        let mock_server = MockServer::start().await;
        mock_oidc_discovery(&mock_server).await;

        // Mock token endpoint for refresh attempt - return error (expired refresh token)
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "Refresh token expired"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Mock token endpoint for authorization code exchange - should not be called in this test
        // (we're not actually launching browser/callback in unit test)

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        // Create an expired cached token with refresh_token
        let cached_token = OAuthToken::new(
            "expired-access-token".to_string(),
            "Bearer".to_string(),
            1, // Expire immediately
            Some("expired-refresh-token".to_string()),
            vec!["all-apis".to_string(), "offline_access".to_string()],
        );

        // Save it to cache
        TokenCache::save(
            &mock_server.uri(),
            "test-client-id",
            &["all-apis".to_string(), "offline_access".to_string()],
            &cached_token,
            TEST_CACHE_PASSPHRASE,
        );

        // Create provider with browser opening suppressed. Must use
        // `new_with_full_config` so the on-disk cache seeded above is
        // actually read — `new_with_config` disables it, and the refresh
        // attempt this test asserts on would never be made.
        let mut provider = AuthorizationCodeProvider::new_with_full_config(
            &mock_server.uri(),
            "test-client-id",
            http_client,
            vec!["all-apis".to_string(), "offline_access".to_string()],
            vec![0],                // Use port 0 to avoid conflicts
            Duration::from_secs(5), // Short timeout for test
            None,
            Some(TEST_CACHE_PASSPHRASE.to_string()),
        )
        .await
        .expect("Failed to create provider");
        provider.suppress_browser();

        // Wait to ensure token expires
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Get auth header - should attempt refresh, fail, then try browser flow
        // Browser flow will fail (no actual browser/callback in test), which is expected
        let result = provider.get_auth_header();

        // Should fail because we can't complete browser flow in unit test
        assert!(
            result.is_err(),
            "Should fail when falling back to browser flow in unit test"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_u2m_token_override_derives_authorize_and_skips_discovery() {
        // The Azure `azure-oauth` flow pins the v2.0 token endpoint (the
        // discovered `{host}/oidc/v1/authorize` redirects to a malformed Entra
        // URL for the Azure client). When the token endpoint is overridden, OIDC
        // discovery must be skipped entirely and the authorize endpoint derived
        // from it (trailing `/token` → `/authorize`).
        let mock_server = MockServer::start().await;

        // Discovery must NOT be hit when the token endpoint is pinned.
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", mock_server.uri()),
                "token_endpoint": format!("{}/oidc/v1/token", mock_server.uri()),
            })))
            .expect(0)
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("Failed to create HTTP client"),
        );

        let token_endpoint = format!("{}/oidc/oauth2/v2.0/token", mock_server.uri());

        let provider = AuthorizationCodeProvider::new_with_full_config(
            &mock_server.uri(),
            "azure-client-id",
            http_client,
            vec!["all-apis".to_string(), "offline_access".to_string()],
            vec![8020],
            Duration::from_secs(120),
            Some(token_endpoint.clone()),
            None, // On-disk cache disabled so no seeded token interferes.
        )
        .await
        .expect("provider creation must not require discovery when the token endpoint is pinned");

        // The pinned token endpoint is carried verbatim; the authorize endpoint
        // is derived from it (discovery skipped).
        assert_eq!(provider.token_endpoint, token_endpoint);
        assert_eq!(
            provider.auth_endpoint,
            format!("{}/oidc/oauth2/v2.0/authorize", mock_server.uri())
        );
        // Discovery `.expect(0)` is verified on mock_server drop.
    }
}
