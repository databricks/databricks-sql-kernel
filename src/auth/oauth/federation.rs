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

//! Mandatory OIDC token exchange (BYOT federation).
//!
//! [`FederationProvider`] wraps another [`AuthProvider`] and, when the inner
//! provider yields a JWT that was **not** issued by the target Databricks
//! workspace (Azure AD, an external IdP, a Bring-Your-Own-Token, or a Workload
//! Identity Federation token), exchanges it for an in-house Databricks OAuth
//! token at the workspace `/oidc/v1/token` endpoint before it is used on any
//! Databricks API.
//!
//! # When the exchange fires
//!
//! Gated entirely by the inner token's `iss` claim vs. the workspace host, so
//! it is free for the common cases:
//!
//! - PAT (`dapi…`) tokens are not JWTs → pass through, no HTTP call.
//! - Same-host M2M / U2M tokens → `iss == host` → pass through, no HTTP call.
//! - Only genuinely foreign-issuer JWTs are exchanged.
//!
//! # Non-breaking fallback
//!
//! If the exchange fails for any reason, the original token is used instead of
//! surfacing an error. This matches the BYOT spec ("in order to not break
//! existing use cases … if any failure happens we will continue with the
//! existing JWT token instead of throwing right away").
//!
//! # Wire contract
//!
//! ```text
//! POST {host}/oidc/v1/token
//! Accept: */*
//! Content-Type: application/x-www-form-urlencoded
//!
//! grant_type = urn:ietf:params:oauth:grant-type:jwt-bearer
//! assertion  = <source JWT>
//! scope      = sql
//! # exactly one of:
//! identity_federation_client_id = <id>            (SP-wide WIF)
//! return_original_token_if_authenticated = true   (BYOT / account-wide WIF)
//! ```

use crate::auth::oauth::jwt;
use crate::auth::oauth::token::{OAuthToken, EXPIRY_BUFFER_SECONDS};
use crate::auth::AuthProvider;
use crate::client::http::DatabricksHttpClient;
use crate::client::retry::RequestType;
use crate::kernel_error::{Error, Result};
use crate::klog::{debug, warn};
use crate::util::runtime::block_on_in_runtime;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;
use tokio::runtime::{Handle as RuntimeHandle, RuntimeFlavor};

const TOKEN_EXCHANGE_PATH: &str = "/oidc/v1/token";
const GRANT_TYPE_JWT_BEARER: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

/// The token-exchange response fields we consume.
#[derive(Debug, Deserialize)]
struct ExchangeResponse {
    access_token: String,
    #[serde(default = "default_token_type")]
    token_type: String,
    /// Lifetime in seconds. Defaults to one hour when the server omits it,
    /// matching the M2M provider's handling.
    #[serde(default = "default_expires_in")]
    expires_in: i64,
}

fn default_token_type() -> String {
    "Bearer".to_string()
}

fn default_expires_in() -> i64 {
    3600
}

/// An [`AuthProvider`] decorator that performs mandatory token exchange for
/// foreign-issuer JWTs. See the [module docs](self).
pub struct FederationProvider {
    /// The wrapped provider that yields the source token.
    inner: Arc<dyn AuthProvider>,
    /// Full token-exchange endpoint URL: `{host}/oidc/v1/token`.
    token_endpoint: String,
    /// The workspace hostname used to decide whether an issuer is in-house.
    workspace_host: String,
    /// SP-wide Workload Identity Federation client id. `None` → BYOT /
    /// account-wide WIF, where the exchange sends
    /// `return_original_token_if_authenticated=true` instead.
    identity_federation_client_id: Option<String>,
    /// Shared HTTP client used for the exchange request. Weak because the
    /// client owns this provider after two-phase auth initialization, avoiding
    /// a new ownership cycle for PAT sessions.
    http_client: Weak<DatabricksHttpClient>,
    /// Runtime handle captured at construction so the sync `get_auth_header`
    /// can drive the async exchange (see [`block_on_in_runtime`]).
    runtime_handle: RuntimeHandle,
    /// The most recent exchange result, as `(source_token, exchanged)`. A
    /// provider tracks exactly one active source token at a time, so a single
    /// slot suffices: when the upstream credential rotates the slot is replaced
    /// (keyed on the source token), and an expired exchanged token is
    /// re-exchanged. Holding the source token lets a cache-hit compare directly
    /// without hashing on the hot path.
    cache: RwLock<Option<CacheEntry>>,
    /// Single-flight leader-election flag. When a cache miss needs an exchange,
    /// exactly one caller flips this `false → true` and performs the network
    /// call; the others poll [`Self::cache`] until it clears (see
    /// `exchanged_or_original`). Mirrors the kernel's `TokenStore` refresh
    /// coordination so concurrent callers on a rotated / expired token don't
    /// each mint a distinct server token (which SP-wide WIF would otherwise do).
    exchanging: AtomicBool,
}

/// How long a follower sleeps between polls while the leader runs the exchange.
/// Matches `TokenStore`'s 1ms refresh-wait cadence.
const FOLLOWER_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Delay before retrying a failed exchange for the same source token.
const FAILED_EXCHANGE_RETRY_DELAY_SECONDS: i64 = 60;

/// A cached exchange outcome: the source token it was derived from, and the
/// resulting in-house token.
struct CacheEntry {
    source_token: String,
    exchanged: OAuthToken,
}

impl std::fmt::Debug for CacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheEntry")
            .field("source_token", &"<redacted>")
            .field("exchanged", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for FederationProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederationProvider")
            .field("inner", &"<redacted>")
            .field("token_endpoint", &self.token_endpoint)
            .field("workspace_host", &self.workspace_host)
            .field(
                "identity_federation_client_id",
                &self.identity_federation_client_id,
            )
            .field("http_client", &"<weak>")
            .field("cache", &"<redacted>")
            .field("exchanging", &self.exchanging.load(Ordering::Relaxed))
            .finish()
    }
}

impl FederationProvider {
    /// Wraps `inner` with mandatory token exchange against `host`.
    ///
    /// `host` is the workspace URL (e.g.
    /// `https://my-workspace.cloud.databricks.com`); it is used to build the
    /// exchange endpoint and to recognize in-house issuers. `async` so it can
    /// capture the current tokio runtime handle, matching the other OAuth
    /// providers. The caller must retain `http_client`; the provider stores a
    /// weak reference so the PAT wrapper does not introduce a cycle after the
    /// client installs the provider.
    pub async fn new(
        inner: Arc<dyn AuthProvider>,
        host: &str,
        identity_federation_client_id: Option<String>,
        http_client: &Arc<DatabricksHttpClient>,
    ) -> Result<Self> {
        let host = host.trim_end_matches('/');
        Ok(Self {
            inner,
            token_endpoint: format!("{host}{TOKEN_EXCHANGE_PATH}"),
            workspace_host: host_of(host).unwrap_or_default(),
            identity_federation_client_id,
            http_client: Arc::downgrade(http_client),
            runtime_handle: RuntimeHandle::current(),
            cache: RwLock::new(None),
            exchanging: AtomicBool::new(false),
        })
    }

    /// Whether a token with issuer `iss` needs exchange (i.e. it is foreign to
    /// this workspace). An unparseable / empty issuer host is treated as "same
    /// host" (no exchange) so malformed values fall back to pass-through.
    fn issuer_is_foreign(&self, iss: &str) -> bool {
        match host_of(iss) {
            Some(issuer_host) => !issuer_host.eq_ignore_ascii_case(&self.workspace_host),
            None => false,
        }
    }

    /// Performs the token exchange and returns the in-house token. Errors here
    /// are turned into the pass-through fallback by the caller.
    async fn exchange(&self, source_token: &str) -> Result<OAuthToken> {
        let http_client = self.http_client.upgrade().ok_or_else(|| {
            Error::invalid_state().message("Token federation HTTP client is no longer available")
        })?;
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", GRANT_TYPE_JWT_BEARER),
            ("assertion", source_token),
            ("scope", "sql"),
        ];
        // `identity_federation_client_id` and
        // `return_original_token_if_authenticated` are mutually exclusive: the
        // server ignores the latter when the former is present.
        match self.identity_federation_client_id.as_deref() {
            Some(client_id) => form.push(("identity_federation_client_id", client_id)),
            None => form.push(("return_original_token_if_authenticated", "true")),
        }

        let request = http_client
            .inner()
            .post(&self.token_endpoint)
            .header("Accept", "*/*")
            .form(&form)
            .build()
            .map_err(|e| {
                Error::invalid_argument()
                    .message(format!("Failed to build token-exchange request: {e}"))
            })?;

        // No auth header: the request authenticates via the `assertion`, not
        // the kernel bearer. `AuthTokenRequest` is idempotent, so the
        // Auth-category ladder retries with backoff on all 5xx / 429 / 408 and
        // timeouts — everything except the fixed non-retryable 4xx set.
        let response = http_client
            .execute_without_auth(request, RequestType::AuthTokenRequest)
            .await?;

        let body: ExchangeResponse = response.json().await.map_err(|e| {
            Error::unauthenticated()
                .message(format!("Failed to parse token-exchange response: {e}"))
        })?;

        Ok(OAuthToken::new(
            body.access_token,
            body.token_type,
            body.expires_in,
            None,
            vec!["sql".to_string()],
        ))
    }

    /// Resolves the token to use: the cached/exchanged in-house token on
    /// success, or `source_token` unchanged on any failure.
    ///
    /// **No lock is ever held across the network call.** A cache hit returns
    /// under a short read lock. On a miss, callers coalesce single-flight
    /// (mirroring the kernel's `TokenStore`): exactly one *leader* wins a
    /// lock-free `AtomicBool` election and runs the exchange, publishing the
    /// outcome to the cache slot; *followers* sleep-poll the slot until the
    /// leader clears the flag, then read the published result. This matters
    /// because on a rotated / expired token a busy session can miss on many
    /// threads at once, and for SP-wide WIF each redundant exchange would mint a
    /// *distinct* server token — coalescing keeps it to one. The Auth-category
    /// retry budget (see `build_retry_configs_for_session`: 30s / 3 retries, not
    /// the global ladder) bounds the leader's latency.
    fn exchanged_or_original(&self, source_token: &str) -> String {
        // Fast path: a fresh cached exchange for this exact source token.
        if let Some(hit) = self.cached_for(source_token) {
            return hit;
        }

        // The exchange bridges sync→async via `block_on_in_runtime`, which uses
        // `block_in_place` when a runtime is current — that panics on a
        // *current-thread* runtime. Every shipped consumer (pyo3, napi, the C
        // ABI) drives the kernel on a multi-thread runtime, but a caller on a
        // current-thread runtime would otherwise crash here. Detect that case
        // and fall back to the original token rather than panicking — the same
        // non-breaking posture as an exchange failure.
        if matches!(
            self.runtime_handle.runtime_flavor(),
            RuntimeFlavor::CurrentThread
        ) {
            warn!(
                "Token federation: skipping exchange on a current-thread runtime \
                 (cannot block safely); using original token"
            );
            return source_token.to_string();
        }

        // Single-flight election. Winner performs the exchange; losers wait.
        if self
            .exchanging
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            // Leader. RAII guard clears the flag even if the exchange panics,
            // so followers can never spin forever.
            let _guard = ExchangeGuard {
                exchanging: &self.exchanging,
            };
            self.lead_exchange(source_token)
        } else {
            // Follower: the wait is synchronous, so tell tokio to move other
            // tasks off this worker while the leader publishes its result.
            match RuntimeHandle::try_current() {
                Ok(handle) if matches!(handle.runtime_flavor(), RuntimeFlavor::MultiThread) => {
                    tokio::task::block_in_place(|| self.follow_exchange(source_token))
                }
                _ => self.follow_exchange(source_token),
            }
        }
    }

    /// Leader path: run the exchange (no lock held) and publish the outcome.
    /// On failure, publish the original token so followers read the same
    /// non-breaking fallback (spec: continue with the existing token rather
    /// than throwing).
    fn lead_exchange(&self, source_token: &str) -> String {
        // Re-check under the flag: a leader that just cleared the flag may have
        // published a result for this exact source token between our miss and
        // our election win.
        if let Some(hit) = self.cached_for(source_token) {
            return hit;
        }

        match block_on_in_runtime(&self.runtime_handle, self.exchange(source_token)) {
            Ok(token) => {
                let access_token = token.access_token.clone();
                self.store(source_token, token);
                debug!("Token federation: exchanged foreign-issuer token for in-house token");
                access_token
            }
            Err(e) => {
                warn!(
                    "Token federation: exchange failed, falling back to original token: {}",
                    e
                );
                // Publish the original (short-lived) so a burst with the same
                // source doesn't hammer the endpoint, and so followers read the
                // same fallback rather than spinning to their timeout.
                self.store(source_token, Self::fallback_token(source_token));
                source_token.to_string()
            }
        }
    }

    /// Follower path: poll until the leader clears the flag, then return the
    /// published result for our source token. If the slot ends up holding a
    /// *different* source token (the upstream credential rotated while we
    /// waited) or nothing usable, fall back to the original token rather than
    /// returning someone else's — correctness over a marginal extra exchange.
    fn follow_exchange(&self, source_token: &str) -> String {
        // Bound the wait so a wedged leader can't hang a follower indefinitely;
        // the leader's own latency is capped by the Auth retry budget (~30s),
        // so a comfortably larger ceiling only trips on a genuine stall.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while self.exchanging.load(Ordering::SeqCst) {
            if std::time::Instant::now() >= deadline {
                warn!(
                    "Token federation: timed out waiting for in-flight exchange; \
                     using original token"
                );
                return source_token.to_string();
            }
            std::thread::sleep(FOLLOWER_POLL_INTERVAL);
        }
        // Leader has finished. Use its published result if it is for our source
        // token; otherwise fall back (don't start a fresh exchange from a
        // follower — keeps this path simple and still non-breaking).
        self.cached_for(source_token)
            .unwrap_or_else(|| source_token.to_string())
    }

    /// Returns the cached access token if the slot holds a non-expired entry
    /// for `source_token`, else `None`. A brief read lock, no network.
    fn cached_for(&self, source_token: &str) -> Option<String> {
        let cache = self.cache.read().ok()?;
        let entry = cache.as_ref()?;
        (entry.source_token == source_token && !entry.exchanged.is_expired())
            .then(|| entry.exchanged.access_token.clone())
    }

    /// Cache the original token until the next failed-exchange retry. Add the
    /// OAuth expiry buffer so it does not shorten the configured retry delay.
    fn fallback_token(source_token: &str) -> OAuthToken {
        OAuthToken::new(
            source_token.to_string(),
            "Bearer".to_string(),
            FAILED_EXCHANGE_RETRY_DELAY_SECONDS + EXPIRY_BUFFER_SECONDS,
            None,
            vec![],
        )
    }

    /// Replace the single cache slot with the outcome for `source_token`.
    /// A brief write lock; never held across a network call.
    fn store(&self, source_token: &str, exchanged: OAuthToken) {
        if let Ok(mut cache) = self.cache.write() {
            *cache = Some(CacheEntry {
                source_token: source_token.to_string(),
                exchanged,
            });
        }
    }
}

/// RAII guard that clears the single-flight flag on drop, so a leader that
/// panics or early-returns can't leave followers polling forever. Mirrors
/// `TokenStore`'s `RefreshGuard`.
struct ExchangeGuard<'a> {
    exchanging: &'a AtomicBool,
}

impl Drop for ExchangeGuard<'_> {
    fn drop(&mut self) {
        self.exchanging.store(false, Ordering::SeqCst);
    }
}

impl AuthProvider for FederationProvider {
    fn get_auth_header(&self) -> Result<String> {
        let header = self.inner.get_auth_header()?;

        // The inner header is `Bearer <token>`. Only bearer JWTs are
        // candidates for exchange; anything else passes through unchanged.
        let Some(source_token) = header.strip_prefix("Bearer ") else {
            return Ok(header);
        };

        // Re-check the issuer on every call (rather than trusting a cached
        // decision) so a fresher in-house token from the inner provider is
        // never shadowed by a stale exchange.
        match jwt::issuer(source_token) {
            Some(iss) if self.issuer_is_foreign(&iss) => {
                let token = self.exchanged_or_original(source_token);
                Ok(format!("Bearer {token}"))
            }
            // Not a JWT, or issued by this workspace: use as-is.
            _ => Ok(header),
        }
    }
}

/// Extracts the hostname from a URL-ish string, lowercased and without its
/// port. Accepts values with or without a scheme and returns `None` if no host
/// can be found.
fn host_of(url: &str) -> Option<String> {
    let normalized;
    let url = if url.contains("://") {
        url
    } else {
        normalized = format!("https://{url}");
        &normalized
    };

    reqwest::Url::parse(url)
        .ok()?
        .host_str()
        .filter(|host| !host.is_empty())
        .map(str::to_ascii_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_of_handles_scheme_and_path() {
        assert_eq!(
            host_of("https://adb-123.4.azuredatabricks.net/oidc").as_deref(),
            Some("adb-123.4.azuredatabricks.net")
        );
        assert_eq!(
            host_of("adb-123.4.azuredatabricks.net").as_deref(),
            Some("adb-123.4.azuredatabricks.net")
        );
        assert_eq!(
            host_of("https://host.example.com:443/path?q=1").as_deref(),
            Some("host.example.com")
        );
        assert_eq!(host_of("").as_deref(), None);
    }

    #[test]
    fn host_of_ignores_ports() {
        assert_eq!(
            host_of("http://host.example.com:80").as_deref(),
            Some("host.example.com")
        );
        assert_eq!(
            host_of("host.example.com:443").as_deref(),
            Some("host.example.com")
        );
        assert_eq!(
            host_of("https://host.example.com:8443").as_deref(),
            Some("host.example.com")
        );
        assert_eq!(
            host_of("http://host.example.com:443").as_deref(),
            Some("host.example.com")
        );
    }

    #[test]
    fn host_of_lowercases() {
        assert_eq!(
            host_of("https://ADB-123.Azuredatabricks.NET/oidc").as_deref(),
            Some("adb-123.azuredatabricks.net")
        );
    }

    #[test]
    fn fallback_cache_retries_after_configured_delay() {
        let before = chrono::Utc::now();
        let fallback = FederationProvider::fallback_token("source-token");
        let after = chrono::Utc::now();
        let retry_at = fallback.expires_at - chrono::Duration::seconds(EXPIRY_BUFFER_SECONDS);

        assert!(
            retry_at >= before + chrono::Duration::seconds(FAILED_EXCHANGE_RETRY_DELAY_SECONDS)
        );
        assert!(retry_at <= after + chrono::Duration::seconds(FAILED_EXCHANGE_RETRY_DELAY_SECONDS));
    }

    /// A provider stub whose `get_auth_header` returns a canned value.
    #[derive(Debug)]
    struct StubProvider(String);
    impl AuthProvider for StubProvider {
        fn get_auth_header(&self) -> Result<String> {
            Ok(self.0.clone())
        }
    }

    fn provider_for(host: &str, inner_header: &str) -> FederationProvider {
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(Default::default()).expect("http client"),
        );
        let host = host.trim_end_matches('/');
        FederationProvider {
            inner: Arc::new(StubProvider(inner_header.to_string())),
            token_endpoint: format!("{host}{TOKEN_EXCHANGE_PATH}"),
            workspace_host: host_of(host).unwrap_or_default(),
            identity_federation_client_id: None,
            http_client: Arc::downgrade(&http_client),
            runtime_handle: RuntimeHandle::current(),
            cache: RwLock::new(None),
            exchanging: AtomicBool::new(false),
        }
    }

    // These use `provider_for`, which calls `RuntimeHandle::current()`, so
    // they must run inside a tokio runtime even though the assertions
    // themselves are synchronous.
    #[tokio::test(flavor = "multi_thread")]
    async fn issuer_same_host_is_not_foreign() {
        let p = provider_for("https://ws.cloud.databricks.com", "Bearer x");
        assert!(!p.issuer_is_foreign("https://ws.cloud.databricks.com/oidc"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn issuer_same_host_ignores_port() {
        let p = provider_for("https://ws.cloud.databricks.com:443", "Bearer x");
        assert!(!p.issuer_is_foreign("https://ws.cloud.databricks.com/oidc"));

        let p = provider_for("https://ws.cloud.databricks.com", "Bearer x");
        assert!(!p.issuer_is_foreign("https://ws.cloud.databricks.com:8443/oidc"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn issuer_other_host_is_foreign() {
        let p = provider_for("https://ws.cloud.databricks.com", "Bearer x");
        assert!(p.issuer_is_foreign("https://login.microsoftonline.com/tenant/v2.0"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_bearer_header_passes_through() {
        let p = provider_for("https://ws.cloud.databricks.com", "Basic abc123");
        assert_eq!(p.get_auth_header().unwrap(), "Basic abc123");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn non_jwt_pat_passes_through() {
        // `dapi…` is not a JWT → no issuer → pass through, no HTTP call.
        let p = provider_for("https://ws.cloud.databricks.com", "Bearer dapiabc123");
        assert_eq!(p.get_auth_header().unwrap(), "Bearer dapiabc123");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pat_wrapper_does_not_retain_http_client() {
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(Default::default()).expect("http client"),
        );
        let weak_http_client = Arc::downgrade(&http_client);
        let provider = Arc::new(
            FederationProvider::new(
                Arc::new(StubProvider("Bearer dapiabc123".to_string())),
                "https://ws.cloud.databricks.com",
                None,
                &http_client,
            )
            .await
            .expect("provider"),
        );
        let weak_provider = Arc::downgrade(&provider);
        http_client.set_auth_provider(provider);

        drop(http_client);

        assert!(weak_http_client.upgrade().is_none());
        assert!(weak_provider.upgrade().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn debug_redacts_all_credentials() {
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(Default::default()).expect("http client"),
        );
        let provider = FederationProvider::new(
            Arc::new(StubProvider("Bearer inner-provider-secret".to_string())),
            "https://ws.cloud.databricks.com",
            Some("federation-client-id".to_string()),
            &http_client,
        )
        .await
        .expect("provider");
        provider.store(
            "source-jwt-secret",
            OAuthToken::new(
                "exchanged-token-secret".to_string(),
                "Bearer".to_string(),
                3600,
                Some("refresh-token-secret".to_string()),
                vec!["sql".to_string()],
            ),
        );

        let provider_debug = format!("{provider:?}");
        let cache_debug = format!("{:?}", provider.cache.read().unwrap().as_ref().unwrap());
        for secret in [
            "inner-provider-secret",
            "source-jwt-secret",
            "exchanged-token-secret",
            "refresh-token-secret",
        ] {
            assert!(
                !provider_debug.contains(secret),
                "secret leaked: {provider_debug}"
            );
            assert!(
                !cache_debug.contains(secret),
                "secret leaked: {cache_debug}"
            );
        }
        assert!(provider_debug.contains("<redacted>"));
        assert!(cache_debug.contains("<redacted>"));
    }
}
