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

//! Integration tests for mandatory OIDC token exchange (BYOT federation).
//!
//! Exercises [`FederationProvider`] end-to-end against a mocked
//! `/oidc/v1/token` endpoint: foreign-issuer JWTs are exchanged, same-host and
//! non-JWT tokens pass through untouched, the WIF-vs-BYOT parameters are sent
//! correctly, and any exchange failure falls back to the original token.

use std::collections::HashMap;
use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use databricks_sql_kernel::auth::oauth::FederationProvider;
use databricks_sql_kernel::auth::{AuthProvider, PersonalAccessToken};
use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig};
use databricks_sql_kernel::client::retry::{build_retry_configs, RetryConfig};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Builds a JWT-shaped `header.payload.signature` string whose payload carries
/// the given `iss`. Header and signature are arbitrary — federation only reads
/// the payload's `iss` claim, without verifying the signature.
fn jwt_with_issuer(iss: &str) -> String {
    let payload = URL_SAFE_NO_PAD.encode(
        serde_json::json!({
            "iss": iss,
            "exp": 4_102_444_800i64, // year 2100
        })
        .to_string(),
    );
    format!("header.{payload}.signature")
}

/// HTTP client with retries disabled so a mocked failure doesn't incur the
/// real backoff ladder (minutes of sleeps).
fn no_retry_http_client() -> Arc<DatabricksHttpClient> {
    let retry = RetryConfig {
        max_retries: 0,
        ..RetryConfig::default()
    };
    Arc::new(
        DatabricksHttpClient::new(
            HttpClientConfig::default(),
            build_retry_configs(&retry, &HashMap::new()),
        )
        .expect("http client"),
    )
}

/// Inner provider that yields `Bearer <token>` — `PersonalAccessToken`
/// formats exactly that, so it stands in for whatever base auth flow produced
/// the token.
fn inner_bearing(token: &str) -> Arc<dyn AuthProvider> {
    Arc::new(PersonalAccessToken::new(token))
}

fn exchange_response(access_token: &str) -> serde_json::Value {
    serde_json::json!({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": 3600,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_issuer_is_exchanged() {
    let server = MockServer::start().await;

    // `:` in the grant_type is percent-encoded by the form encoder, so match
    // on the stable tail plus the always-present `scope=sql`.
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .and(body_string_contains("grant-type%3Ajwt-bearer"))
        .and(body_string_contains("scope=sql"))
        .respond_with(ResponseTemplate::new(200).set_body_json(exchange_response("in-house-token")))
        .expect(1)
        .mount(&server)
        .await;

    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer(
            "https://login.microsoftonline.com/tenant/v2.0",
        )),
        &server.uri(),
        None,
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    let header = provider.get_auth_header().expect("auth header");
    assert_eq!(header, "Bearer in-house-token");
}

#[tokio::test(flavor = "multi_thread")]
async fn same_host_issuer_passes_through() {
    let server = MockServer::start().await;

    // Any hit on the exchange endpoint is a failure for this case.
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(exchange_response("should-not-be-used")),
        )
        .expect(0)
        .mount(&server)
        .await;

    // Issuer host == workspace host → already an in-house token.
    let issuer = format!("{}/oidc", server.uri());
    let inner_token = jwt_with_issuer(&issuer);

    let provider = FederationProvider::new(
        inner_bearing(&inner_token),
        &server.uri(),
        None,
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    let header = provider.get_auth_header().expect("auth header");
    assert_eq!(header, format!("Bearer {inner_token}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn non_jwt_pat_passes_through() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(exchange_response("nope")))
        .expect(0)
        .mount(&server)
        .await;

    let provider = FederationProvider::new(
        inner_bearing("dapiabc123def456"), // opaque PAT, not a JWT
        &server.uri(),
        None,
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    let header = provider.get_auth_header().expect("auth header");
    assert_eq!(header, "Bearer dapiabc123def456");
}

#[tokio::test(flavor = "multi_thread")]
async fn sp_wide_wif_sends_identity_federation_client_id() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .and(body_string_contains(
            "identity_federation_client_id=my-sp-client-id",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(exchange_response("wif-token")))
        .expect(1)
        .mount(&server)
        .await;

    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer("https://external-idp.example.com")),
        &server.uri(),
        Some("my-sp-client-id".to_string()),
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    assert_eq!(provider.get_auth_header().unwrap(), "Bearer wif-token");
}

#[tokio::test(flavor = "multi_thread")]
async fn byot_sends_return_original_token_flag() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .and(body_string_contains(
            "return_original_token_if_authenticated=true",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(exchange_response("byot-token")))
        .expect(1)
        .mount(&server)
        .await;

    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer("https://external-idp.example.com")),
        &server.uri(),
        None, // no client id → BYOT / account-wide WIF
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    assert_eq!(provider.get_auth_header().unwrap(), "Bearer byot-token");
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_http_401_falls_back_to_original() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"error":"invalid_token"}"#))
        .mount(&server)
        .await;

    let inner_token = jwt_with_issuer("https://external-idp.example.com");
    let provider = FederationProvider::new(
        inner_bearing(&inner_token),
        &server.uri(),
        None,
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    // Non-breaking fallback: the original token is used, no error surfaced.
    let header = provider.get_auth_header().expect("auth header");
    assert_eq!(header, format!("Bearer {inner_token}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_http_500_falls_back_to_original() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let inner_token = jwt_with_issuer("https://external-idp.example.com");
    let provider = FederationProvider::new(
        inner_bearing(&inner_token),
        &server.uri(),
        None,
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    let header = provider.get_auth_header().expect("auth header");
    assert_eq!(header, format!("Bearer {inner_token}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn exchanged_token_is_cached_across_calls() {
    let server = MockServer::start().await;

    // Same source token on every call → exactly one exchange, then cache hits.
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(exchange_response("cached-token")))
        .expect(1)
        .mount(&server)
        .await;

    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer("https://external-idp.example.com")),
        &server.uri(),
        None,
        no_retry_http_client(),
    )
    .await
    .expect("provider");

    for _ in 0..3 {
        assert_eq!(provider.get_auth_header().unwrap(), "Bearer cached-token");
    }
}
