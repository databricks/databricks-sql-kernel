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

//! M2M OAuth using a JWT private-key client assertion.
//!
//! This is the private-key variant of the client-credentials grant (see
//! [`super::m2m`] for the shared-secret variant). Instead of sending a
//! `client_secret`, the provider signs a short-lived JWT with a private
//! key and sends it as an RFC 7523 `client_assertion`. The authorization
//! server verifies the signature against the service principal's
//! registered public key and returns an access token.
//!
//! The OAuth lifecycle — OIDC discovery, in-memory token caching, refresh
//! on expiry — is identical to [`super::m2m::ClientCredentialsProvider`]
//! and reuses the same [`TokenStore`] and [`OidcEndpoints`] machinery. The
//! only substantive differences are:
//!
//! 1. The key is loaded and cached once at construction ([`JwtSigningKey`]).
//! 2. The token request is a manual form POST carrying `client_assertion`
//!    (the `oauth2` crate's `exchange_client_credentials()` helper cannot
//!    send a client-assertion, only a client secret).
//!
//! M2M tokens are not cached to disk (same rationale as [`super::m2m`]):
//! the private key is always available for re-authentication.

use std::sync::{Arc, Weak};

use oauth2::basic::BasicTokenType;
use oauth2::{EmptyExtraTokenFields, StandardTokenResponse, TokenResponse as _};
use tokio::runtime::Handle as RuntimeHandle;

use crate::auth::oauth::jwt_key::JwtSigningKey;
use crate::auth::oauth::m2m::token_exchange_error;
use crate::auth::oauth::oidc::OidcEndpoints;
use crate::auth::oauth::token::OAuthToken;
use crate::auth::oauth::token_store::TokenStore;
use crate::auth::AuthProvider;
use crate::client::http::DatabricksHttpClient;
use crate::client::retry::RequestType;
use crate::kernel_error::{Error, Result};
use crate::util::runtime::block_on_in_runtime;

/// RFC 7523 client-assertion type for a JWT bearer assertion.
const JWT_BEARER_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Lifetime of the signed client-assertion JWT.
///
/// The assertion is given a small positive validity window (`exp` strictly
/// after `iat`); it is single-use and consumed immediately by the token
/// exchange, so a short window is ample.
const ASSERTION_LIFETIME_SECONDS: i64 = 300;

/// Fallback token TTL when the token endpoint omits `expires_in`.
const DEFAULT_TOKEN_TTL_SECONDS: i64 = 3600;

/// Claims for the signed client-assertion JWT.
///
/// Per RFC 7523: `iss` and `sub` are both the client id, `aud` is the
/// token endpoint, `jti` is a unique id (replay protection), and `iat`
/// / `exp` bound the validity window.
#[derive(Debug, serde::Serialize)]
struct AssertionClaims {
    iss: String,
    sub: String,
    aud: String,
    jti: String,
    iat: i64,
    exp: i64,
}

/// The OAuth token endpoint's JSON response, parsed via the `oauth2`
/// crate's standard type so this path shares the same response model as
/// [`super::m2m::ClientCredentialsProvider`] rather than a bespoke struct.
type TokenResponse = StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>;

/// Generate a unique JWT id (`jti`) for replay protection: 128 random
/// bits rendered as hex. Uses the `rand` crate already pulled in for
/// retry jitter, avoiding a `uuid` dependency.
fn new_jti() -> String {
    use rand::RngExt;
    let bytes: [u8; 16] = rand::rng().random();
    hex::encode(bytes)
}

/// OAuth provider for M2M authentication via a JWT private-key client
/// assertion.
///
/// Discovers the token endpoint via OIDC at construction, loads and
/// caches the signing key, then signs a fresh assertion on each token
/// fetch. Tokens are managed by [`TokenStore`] with automatic refresh,
/// exactly like [`super::m2m::ClientCredentialsProvider`].
pub struct JwtClientCredentialsProvider {
    /// OAuth client id (service principal). Also the JWT `iss`/`sub`.
    client_id: String,
    /// Key id written into the JWT header so the server can select the
    /// registered public key.
    jwt_kid: String,
    /// Discovered token endpoint. Also the JWT `aud`.
    token_endpoint: String,
    /// Loaded + cached signing key (parsed once at construction).
    signing_key: Arc<JwtSigningKey>,
    /// Thread-safe token store managing the token lifecycle.
    token_store: TokenStore,
    /// HTTP client for token endpoint requests. Weak; see
    /// [`super::upgrade_http_client`].
    http_client: Weak<DatabricksHttpClient>,
    /// OAuth scopes to request.
    scopes: Vec<String>,
    /// Tokio runtime handle captured at construction so the sync
    /// `get_auth_header` can drive the async token exchange safely.
    /// See [`super::m2m`] and [`crate::util::runtime::block_on_in_runtime`].
    runtime_handle: RuntimeHandle,
}

// `JwtSigningKey` holds opaque signing material and does not implement
// `Debug`; redact it (and never expose client_id-adjacent secrets) in the
// provider's own `Debug`.
impl std::fmt::Debug for JwtClientCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtClientCredentialsProvider")
            .field("client_id", &self.client_id)
            .field("jwt_kid", &self.jwt_kid)
            .field("token_endpoint", &self.token_endpoint)
            .field("signing_key", &"<redacted>")
            .field("scopes", &self.scopes)
            .finish()
    }
}

impl JwtClientCredentialsProvider {
    /// Create a new JWT private-key M2M provider.
    ///
    /// Loads the signing key and discovers the token endpoint (unless
    /// overridden) before returning. The first `get_auth_header()` call
    /// triggers the initial token exchange.
    ///
    /// # Arguments
    ///
    /// * `host` — Databricks workspace URL (for OIDC discovery).
    /// * `client_id` — OAuth client id for the service principal.
    /// * `jwt_key_file` — path to the PEM-encoded private key.
    /// * `jwt_kid` — key id for the JWT header.
    /// * `jwt_passphrase` — passphrase for an encrypted PKCS#8 key, else `None`.
    /// * `jwt_algorithm` — JWT algorithm name (`None` → `RS256`).
    /// * `http_client` — shared HTTP client. The caller must keep it alive;
    ///   the provider holds only a weak reference.
    /// * `scopes` — OAuth scopes to request.
    /// * `token_endpoint_override` — token endpoint URL, or `None` to discover via OIDC.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        host: &str,
        client_id: &str,
        jwt_key_file: &str,
        jwt_kid: &str,
        jwt_passphrase: Option<&str>,
        jwt_algorithm: Option<&str>,
        http_client: &Arc<DatabricksHttpClient>,
        scopes: Vec<String>,
        token_endpoint_override: Option<String>,
    ) -> Result<Self> {
        // Load + validate the key up front so a bad key/algorithm/passphrase
        // fails at session open, not deep inside the first HTTP retry loop.
        let signing_key = Arc::new(JwtSigningKey::load(
            jwt_key_file,
            jwt_passphrase,
            jwt_algorithm,
        )?);

        let token_endpoint = match token_endpoint_override {
            Some(endpoint) => endpoint,
            None => {
                OidcEndpoints::discover(host, http_client)
                    .await?
                    .token_endpoint
            }
        };

        Ok(Self {
            client_id: client_id.to_string(),
            jwt_kid: jwt_kid.to_string(),
            token_endpoint,
            signing_key,
            token_store: TokenStore::new(),
            http_client: Arc::downgrade(http_client),
            scopes,
            runtime_handle: RuntimeHandle::current(),
        })
    }
}

/// Build and sign a fresh client-assertion JWT.
///
/// Free function (not a method) so it can be called from inside the
/// `'static` refresh closure in [`JwtClientCredentialsProvider::get_auth_header`]
/// — the closure captures owned clones (`Arc<JwtSigningKey>` + the claim
/// inputs) rather than borrowing `self`. That keeps signing off the
/// fresh-token fast path: a JWT is only signed when a token fetch/refresh
/// actually runs, not on every request. A fresh assertion per refresh is
/// correct — each carries its own `jti` and `exp`.
fn build_client_assertion(
    signing_key: &JwtSigningKey,
    client_id: &str,
    jwt_kid: &str,
    token_endpoint: &str,
) -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    let claims = AssertionClaims {
        iss: client_id.to_string(),
        sub: client_id.to_string(),
        aud: token_endpoint.to_string(),
        jti: new_jti(),
        iat: now,
        exp: now + ASSERTION_LIFETIME_SECONDS,
    };
    signing_key.sign(jwt_kid, &claims)
}

impl AuthProvider for JwtClientCredentialsProvider {
    /// Returns a valid `Bearer` token, refreshing via a signed
    /// client-assertion exchange when the cached token is empty/expired.
    ///
    /// Lifecycle semantics match [`super::m2m::ClientCredentialsProvider`]
    /// (fresh → cached, stale → background refresh, expired → blocking
    /// refresh), coordinated by [`TokenStore`] and safe across threads.
    fn get_auth_header(&self) -> Result<String> {
        let http_client = self.http_client.clone();
        let token_endpoint = self.token_endpoint.clone();
        let scopes = self.scopes.clone();
        let runtime_handle = self.runtime_handle.clone();
        // Capture cheap owned clones for the `'static` refresh closure so
        // the assertion can be *signed inside it* — signing (RSA/EC crypto)
        // then only happens on the empty/expired/stale-refresh paths, not on
        // every request's fresh-token fast path. `signing_key` is an `Arc`,
        // the rest are `String`s.
        let signing_key = self.signing_key.clone();
        let client_id = self.client_id.clone();
        let jwt_kid = self.jwt_kid.clone();

        let token = self.token_store.get_or_refresh(move || {
            let http_client = super::upgrade_http_client(&http_client)?;
            // Sign a fresh assertion only now that a refresh is actually
            // running (fresh-token calls return before reaching here).
            let client_assertion =
                build_client_assertion(&signing_key, &client_id, &jwt_kid, &token_endpoint)?;
            block_on_in_runtime(&runtime_handle, async {
                let mut form: Vec<(&str, String)> = vec![
                    ("grant_type", "client_credentials".to_string()),
                    (
                        "client_assertion_type",
                        JWT_BEARER_ASSERTION_TYPE.to_string(),
                    ),
                    ("client_assertion", client_assertion),
                ];
                if !scopes.is_empty() {
                    form.push(("scope", scopes.join(" ")));
                }

                let request = http_client
                    .inner()
                    .post(&token_endpoint)
                    .form(&form)
                    .build()
                    .map_err(|e| {
                        Error::invalid_argument()
                            .message(format!("failed to build token request: {e}"))
                    })?;

                // `execute_without_auth` returns `Ok` only for a 2xx
                // response; any non-success status (e.g. a 401 with an
                // `{"error":"invalid_client",…}` body from the token
                // endpoint) is already turned into an `Err` — with the
                // status and parsed server body embedded by `http_error` —
                // and classified here like the client-secret M2M flow. So
                // there is no non-success branch to handle: a returned
                // `response` is a success.
                let response = http_client
                    .execute_without_auth(request, RequestType::AuthTokenRequest)
                    .await
                    .map_err(|e| token_exchange_error(e, "JWT M2M token exchange"))?;

                let parsed: TokenResponse = response.json().await.map_err(|e| {
                    Error::io().message(format!("Failed to retrieve custom M2M token: {e}"))
                })?;

                let expires_in = parsed
                    .expires_in()
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(DEFAULT_TOKEN_TTL_SECONDS);
                // `.scopes()` yields `None` when the endpoint omits `scope`
                // (and never a spurious empty-string scope), so fall back to
                // the requested scopes in that case.
                let granted_scopes = parsed
                    .scopes()
                    .map(|s| s.iter().map(|scope| scope.to_string()).collect())
                    .unwrap_or_else(|| scopes.clone());

                Ok(OAuthToken::new(
                    parsed.access_token().secret().to_string(),
                    parsed.token_type().as_ref().to_string(),
                    expires_in,
                    None, // No refresh token for M2M.
                    granted_scopes,
                ))
            })
        })?;

        Ok(format!("Bearer {}", token.access_token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::http::HttpClientConfig;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::auth::oauth::test_keys;

    fn http_client() -> Arc<DatabricksHttpClient> {
        Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("http client"),
        )
    }

    /// Generate a fresh unencrypted RSA test key, write it to a temp file,
    /// and return its path plus the tempdir guard (dropped by the caller
    /// to clean up). Keys are generated in-process — see
    /// [`crate::auth::oauth::test_keys`].
    fn write_test_key() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("key.pem");
        std::fs::write(&path, test_keys::rsa_pkcs8_pem()).expect("write key");
        let path_str = path.to_str().unwrap().to_string();
        (dir, path_str)
    }

    async fn mock_oidc(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/oidc/.well-known/oauth-authorization-server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "authorization_endpoint": format!("{}/oidc/v1/authorize", server.uri()),
                "token_endpoint": format!("{}/oidc/v1/token", server.uri()),
            })))
            .mount(server)
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_token_exchange_sends_assertion() {
        let server = MockServer::start().await;
        mock_oidc(&server).await;

        // The token endpoint must receive a jwt-bearer client_assertion,
        // not a client secret.
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .and(body_string_contains("client_assertion"))
            .and(body_string_contains(
                "urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "jwt-m2m-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": "all-apis"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (_dir, key_path) = write_test_key();
        let client = http_client();
        let provider = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            &key_path,
            "test-kid",
            None,
            Some("RS256"),
            &client,
            vec!["all-apis".to_string()],
            None,
        )
        .await
        .expect("provider");

        let header = provider.get_auth_header().expect("auth header");
        assert_eq!(header, "Bearer jwt-m2m-access-token");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_assertion_carries_rfc7523_claims() {
        // The signed assertion's sub/iss must be the client id and aud the
        // token endpoint. We assert this end-to-end by capturing the form the
        // token endpoint receives and decoding the client_assertion.
        use std::sync::{Arc as StdArc, Mutex};

        let server = MockServer::start().await;
        mock_oidc(&server).await;
        let token_url = format!("{}/oidc/v1/token", server.uri());

        let captured: StdArc<Mutex<Option<String>>> = StdArc::new(Mutex::new(None));
        let captured_for_mock = captured.clone();
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(move |req: &wiremock::Request| {
                let body = String::from_utf8_lossy(&req.body).to_string();
                *captured_for_mock.lock().unwrap() = Some(body);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "t", "token_type": "Bearer", "expires_in": 3600
                }))
            })
            .mount(&server)
            .await;

        let (_dir, key_path) = write_test_key();
        let client = http_client();
        let provider = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            &key_path,
            "test-kid",
            None,
            Some("RS256"),
            &client,
            vec!["all-apis".to_string()],
            None,
        )
        .await
        .expect("provider");
        provider.get_auth_header().expect("auth header");

        // Extract client_assertion from the urlencoded form body. A JWT is
        // base64url (no '%' escapes) so the raw form value is the assertion
        // as-is; only '=' padding would be percent-encoded, and compact JWS
        // uses no padding. Parse the `k=v&…` pairs directly to avoid adding
        // a url-decoding dependency to the test.
        let body = captured.lock().unwrap().clone().expect("body captured");
        let assertion = body
            .split('&')
            .find_map(|pair| pair.strip_prefix("client_assertion="))
            .expect("client_assertion present")
            .to_string();

        // Verify the assertion's signature against the public key matching
        // the signing key (not just decode the claims): a valid signature is
        // what the authorization server actually checks. `jsonwebtoken::decode`
        // fails if the RS256 signature does not verify.
        use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
        let header = decode_header(&assertion).expect("decode JWS header");
        assert_eq!(header.alg, Algorithm::RS256);
        assert_eq!(header.kid.as_deref(), Some("test-kid"));

        let decoding_key =
            DecodingKey::from_rsa_pem(test_keys::rsa_public_spki_pem().as_bytes()).unwrap();
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[&token_url]);
        let decoded = decode::<serde_json::Value>(&assertion, &decoding_key, &validation)
            .expect("assertion signature must verify against the public key");

        let claims = decoded.claims;
        assert_eq!(claims["sub"], "test-client-id");
        assert_eq!(claims["iss"], "test-client-id");
        assert_eq!(claims["aud"], token_url);
        assert!(claims["jti"].is_string());
        // exp must be strictly after iat (positive validity window).
        assert!(claims["exp"].as_i64().unwrap() > claims["iat"].as_i64().unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_caches_token_across_calls() {
        // A second call returns the cached token without a second exchange.
        // The mock asserts exactly one POST.
        let server = MockServer::start().await;
        mock_oidc(&server).await;
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "cached-token", "token_type": "Bearer", "expires_in": 3600
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (_dir, key_path) = write_test_key();
        let client = http_client();
        let provider = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            &key_path,
            "test-kid",
            None,
            None,
            &client,
            vec!["all-apis".to_string()],
            None,
        )
        .await
        .expect("provider");

        let h1 = provider.get_auth_header().expect("first");
        let h2 = provider.get_auth_header().expect("second");
        assert_eq!(h1, "Bearer cached-token");
        assert_eq!(h2, h1);
        // .expect(1) on the mock verifies only one exchange happened.
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_token_url_override_skips_discovery() {
        // With an explicit token endpoint, no OIDC discovery mock is
        // mounted; the provider must still work.
        let server = MockServer::start().await;
        let token_url = format!("{}/custom/token", server.uri());
        Mock::given(method("POST"))
            .and(path("/custom/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "override-token", "token_type": "Bearer", "expires_in": 3600
            })))
            .mount(&server)
            .await;

        let (_dir, key_path) = write_test_key();
        let client = http_client();
        let provider = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            &key_path,
            "test-kid",
            None,
            None,
            &client,
            vec!["all-apis".to_string()],
            Some(token_url),
        )
        .await
        .expect("provider");

        assert_eq!(
            provider.get_auth_header().expect("header"),
            "Bearer override-token"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_bad_algorithm_fails_at_construction() {
        let server = MockServer::start().await;
        mock_oidc(&server).await;

        let (_dir, key_path) = write_test_key();
        let result = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            &key_path,
            "test-kid",
            None,
            Some("ES512"),
            &http_client(),
            vec!["all-apis".to_string()],
            None,
        )
        .await;

        let err = result.expect_err("ES512 should be rejected");
        assert!(format!("{err:?}").contains("ES512"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_missing_key_file_fails() {
        let server = MockServer::start().await;
        mock_oidc(&server).await;

        let result = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            "/nonexistent/key.pem",
            "test-kid",
            None,
            None,
            &http_client(),
            vec!["all-apis".to_string()],
            None,
        )
        .await;

        assert!(result.is_err(), "missing key file should fail");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_server_rejection_surfaces_error() {
        let server = MockServer::start().await;
        mock_oidc(&server).await;

        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "invalid_client"
            })))
            .mount(&server)
            .await;

        let (_dir, key_path) = write_test_key();
        let client = http_client();
        let provider = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            &key_path,
            "test-kid",
            None,
            None,
            &client,
            vec!["all-apis".to_string()],
            None,
        )
        .await
        .expect("provider");

        let err = provider.get_auth_header().expect_err("should fail on 401");
        let msg = format!("{err:?}");
        // The 401 is turned into an error by `execute_without_auth`'s
        // `http_error`, which folds the server body into the message. Assert
        // BOTH the status and the server-supplied `invalid_client` reason
        // surface — this exercises the real error path (not a manual branch
        // in the provider) and proves the token-endpoint's body reaches the
        // caller.
        assert!(msg.contains("401"), "expected HTTP status in error: {msg}");
        assert!(
            msg.contains("invalid_client"),
            "expected server error body in error: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn jwt_m2m_rejected_assertion_is_unauthenticated() {
        let server = MockServer::start().await;
        mock_oidc(&server).await;

        // RFC 6749 lets the token endpoint reject client auth with a 400.
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_client"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (_dir, key_path) = write_test_key();
        let client = http_client();
        let provider = JwtClientCredentialsProvider::new(
            &server.uri(),
            "test-client-id",
            &key_path,
            "test-kid",
            None,
            None,
            &client,
            vec!["all-apis".to_string()],
            None,
        )
        .await
        .expect("provider");

        let err = provider.get_auth_header().expect_err("should fail on 400");
        assert_eq!(err.code, crate::kernel_error::ErrorCode::Unauthenticated);
        assert_eq!(err.http_status, Some(400));
    }

    /// The HTTP client owns its auth provider, so a provider holding the client
    /// strongly would leak both, plus the provider's tokio runtime handle.
    #[tokio::test(flavor = "multi_thread")]
    async fn provider_does_not_retain_http_client() {
        let (_dir, key_path) = write_test_key();
        let client = http_client();
        let weak_client = Arc::downgrade(&client);
        let provider = Arc::new(
            JwtClientCredentialsProvider::new(
                "https://ws.cloud.databricks.com",
                "test-client-id",
                &key_path,
                "test-kid",
                None,
                None,
                &client,
                vec!["all-apis".to_string()],
                Some("https://ws.cloud.databricks.com/oidc/v1/token".to_string()),
            )
            .await
            .expect("provider"),
        );
        let weak_provider = Arc::downgrade(&provider);
        client.set_auth_provider(provider);

        drop(client);

        assert!(weak_client.upgrade().is_none());
        assert!(weak_provider.upgrade().is_none());
    }
}
