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
use crate::auth::oauth::token::OAuthToken;
use crate::auth::AuthProvider;
use crate::client::http::DatabricksHttpClient;
use crate::client::retry::RequestType;
use crate::kernel_error::{Error, Result};
use crate::klog::{debug, warn};
use crate::util::runtime::block_on_in_runtime;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::runtime::Handle as RuntimeHandle;

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
#[derive(Debug)]
pub struct FederationProvider {
    /// The wrapped provider that yields the source token.
    inner: Arc<dyn AuthProvider>,
    /// Full token-exchange endpoint URL: `{host}/oidc/v1/token`.
    token_endpoint: String,
    /// The workspace host (bare `host[:port]`) used to decide whether an
    /// issuer is in-house.
    workspace_host: String,
    /// SP-wide Workload Identity Federation client id. `None` → BYOT /
    /// account-wide WIF, where the exchange sends
    /// `return_original_token_if_authenticated=true` instead.
    identity_federation_client_id: Option<String>,
    /// Shared HTTP client used for the exchange request.
    http_client: Arc<DatabricksHttpClient>,
    /// Runtime handle captured at construction so the sync `get_auth_header`
    /// can drive the async exchange (see [`block_on_in_runtime`]).
    runtime_handle: RuntimeHandle,
    /// Exchanged tokens keyed by a hash of the *source* token, so a rotated
    /// upstream credential is exchanged fresh and an expired exchanged token
    /// is re-exchanged.
    cache: RwLock<HashMap<String, OAuthToken>>,
    /// Serializes concurrent exchanges (double-checked against `cache`).
    exchange_lock: Mutex<()>,
}

impl FederationProvider {
    /// Wraps `inner` with mandatory token exchange against `host`.
    ///
    /// `host` is the workspace URL (e.g.
    /// `https://my-workspace.cloud.databricks.com`); it is used to build the
    /// exchange endpoint and to recognize in-house issuers. `async` so it can
    /// capture the current tokio runtime handle, matching the other OAuth
    /// providers.
    pub async fn new(
        inner: Arc<dyn AuthProvider>,
        host: &str,
        identity_federation_client_id: Option<String>,
        http_client: Arc<DatabricksHttpClient>,
    ) -> Result<Self> {
        let host = host.trim_end_matches('/');
        Ok(Self {
            inner,
            token_endpoint: format!("{host}{TOKEN_EXCHANGE_PATH}"),
            workspace_host: host_of(host).unwrap_or_default(),
            identity_federation_client_id,
            http_client,
            runtime_handle: RuntimeHandle::current(),
            cache: RwLock::new(HashMap::new()),
            exchange_lock: Mutex::new(()),
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

        let request = self
            .http_client
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
        // the kernel bearer. `AuthTokenRequest` gives it the Auth-category
        // retry ladder (backoff on 408/502/503/504 + timeouts).
        let response = self
            .http_client
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
    fn exchanged_or_original(&self, source_token: &str) -> String {
        let key = cache_key(source_token);

        // Fast path: a fresh cached exchange for this exact source token.
        if let Ok(cache) = self.cache.read() {
            if let Some(tok) = cache.get(&key) {
                if !tok.is_expired() {
                    return tok.access_token.clone();
                }
            }
        }

        // Slow path: serialize so concurrent callers don't all hit the
        // endpoint, then double-check the cache.
        let _guard = self.exchange_lock.lock().unwrap_or_else(|e| e.into_inner());
        if let Ok(cache) = self.cache.read() {
            if let Some(tok) = cache.get(&key) {
                if !tok.is_expired() {
                    return tok.access_token.clone();
                }
            }
        }

        match block_on_in_runtime(&self.runtime_handle, self.exchange(source_token)) {
            Ok(token) => {
                let access_token = token.access_token.clone();
                if let Ok(mut cache) = self.cache.write() {
                    cache.insert(key, token);
                }
                debug!("Token federation: exchanged foreign-issuer token for in-house token");
                access_token
            }
            Err(e) => {
                // Non-breaking fallback: use the original token. Cache the
                // original (short-lived) under the same key so a burst of
                // requests doesn't hammer the endpoint after a failure.
                warn!(
                    "Token federation: exchange failed, falling back to original token: {}",
                    e
                );
                if let Ok(mut cache) = self.cache.write() {
                    cache.insert(
                        key,
                        OAuthToken::new(
                            source_token.to_string(),
                            "Bearer".to_string(),
                            60,
                            None,
                            vec![],
                        ),
                    );
                }
                source_token.to_string()
            }
        }
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

/// SHA-256 hex digest of a token, used as the cache key so raw tokens are not
/// held as map keys.
fn cache_key(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// Extracts the bare `host[:port]` from a URL-ish string, lowercased. Accepts
/// values with or without a scheme (JWT `iss` claims and workspace hosts both
/// occur in both forms). Returns `None` if no host can be found.
///
/// Avoids pulling in a URL-parsing dependency: the kernel already normalizes
/// hosts with plain string ops (see `ConnectionConfig::normalise_host`), and
/// we only need the authority component for a host comparison.
fn host_of(url: &str) -> Option<String> {
    // Strip scheme if present.
    let after_scheme = match url.find("://") {
        Some(idx) => &url[idx + 3..],
        None => url,
    };
    // Authority ends at the first '/', '?', or '#'.
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // Drop userinfo if present (`user:pass@host`).
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    if host_port.is_empty() {
        None
    } else {
        Some(host_port.to_ascii_lowercase())
    }
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
            Some("host.example.com:443")
        );
        assert_eq!(host_of("").as_deref(), None);
    }

    #[test]
    fn host_of_lowercases() {
        assert_eq!(
            host_of("https://ADB-123.Azuredatabricks.NET/oidc").as_deref(),
            Some("adb-123.azuredatabricks.net")
        );
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
            http_client,
            runtime_handle: RuntimeHandle::current(),
            cache: RwLock::new(HashMap::new()),
            exchange_lock: Mutex::new(()),
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
}
