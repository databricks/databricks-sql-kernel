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
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use databricks_sql_kernel::auth::oauth::FederationProvider;
use databricks_sql_kernel::auth::{AuthProvider, PersonalAccessToken};
use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig};
use databricks_sql_kernel::client::retry::{build_retry_configs, RetryConfig};
use databricks_sql_kernel::{AuthConfig, ConnectionConfig, HttpConfig, Session};
use wiremock::matchers::{body_string_contains, header, method, path};
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

/// HTTP client with a small-but-real retry budget: enough retries that a
/// failing endpoint takes measurable wall-time per exchange, but bounded so a
/// test finishes quickly. Used to prove that a degraded `/oidc/v1/token` does
/// not serialize concurrent callers behind a shared lock.
fn bounded_retry_http_client() -> Arc<DatabricksHttpClient> {
    let retry = RetryConfig {
        min_wait: Duration::from_millis(200),
        max_wait: Duration::from_millis(200),
        max_retries: 2, // 3 attempts total
        overall_timeout: Duration::from_secs(5),
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

    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer(
            "https://login.microsoftonline.com/tenant/v2.0",
        )),
        &server.uri(),
        None,
        &http_client,
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

    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing(&inner_token),
        &server.uri(),
        None,
        &http_client,
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

    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing("dapiabc123def456"), // opaque PAT, not a JWT
        &server.uri(),
        None,
        &http_client,
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

    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer("https://external-idp.example.com")),
        &server.uri(),
        Some("my-sp-client-id".to_string()),
        &http_client,
    )
    .await
    .expect("provider");

    assert_eq!(provider.get_auth_header().unwrap(), "Bearer wif-token");
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_config_federation_client_id_reaches_exchange() {
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
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .and(header("authorization", "Bearer wif-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "session_id": "session-id",
            "warehouse_id": "warehouse-id",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut http = HttpConfig::default();
    http.retry_max_retries = 0;
    let session = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/warehouse-id")
                .unwrap(),
        )
        .auth(AuthConfig::Pat {
            token: jwt_with_issuer("https://external-idp.example.com"),
            identity_federation_client_id: Some("my-sp-client-id".to_string()),
        })
        .http(http)
        .open()
        .await
        .unwrap();

    assert_eq!(session.session_id(), "session-id");
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

    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer("https://external-idp.example.com")),
        &server.uri(),
        None, // no client id → BYOT / account-wide WIF
        &http_client,
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
    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing(&inner_token),
        &server.uri(),
        None,
        &http_client,
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
    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing(&inner_token),
        &server.uri(),
        None,
        &http_client,
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

    let http_client = no_retry_http_client();
    let provider = FederationProvider::new(
        inner_bearing(&jwt_with_issuer("https://external-idp.example.com")),
        &server.uri(),
        None,
        &http_client,
    )
    .await
    .expect("provider");

    for _ in 0..3 {
        assert_eq!(provider.get_auth_header().unwrap(), "Bearer cached-token");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn representative_databricks_issuer_passes_through() {
    // Non-circular version of the same-host case: use a realistic in-house
    // Databricks issuer (`https://<workspace-host>/oidc`) and a matching
    // workspace host that is NOT the mock URI, proving the host comparison —
    // scheme + `/oidc` path stripped — treats a real Databricks-issued token
    // as in-house and skips the exchange.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(exchange_response("unused")))
        .expect(0)
        .mount(&server)
        .await;

    let workspace = "https://adb-5484724900402942.2.azuredatabricks.net";
    let inner_token = jwt_with_issuer(&format!("{workspace}/oidc"));

    let http_client = no_retry_http_client();
    let provider =
        FederationProvider::new(inner_bearing(&inner_token), workspace, None, &http_client)
            .await
            .expect("provider");

    // Same host (modulo scheme + `/oidc`) ⇒ pass through unchanged, no HTTP.
    assert_eq!(
        provider.get_auth_header().unwrap(),
        format!("Bearer {inner_token}")
    );
}

/// Spawn `n` concurrent `get_auth_header` callers on separate OS threads, each
/// entering the runtime context (get_auth_header is sync and blocks its thread).
/// Returns each caller's result.
fn concurrent_auth_headers(
    provider: Arc<FederationProvider>,
    n: usize,
) -> Vec<databricks_sql_kernel::Result<String>> {
    let handle = tokio::runtime::Handle::current();
    let threads: Vec<_> = (0..n)
        .map(|_| {
            let p = provider.clone();
            let h = handle.clone();
            std::thread::spawn(move || {
                let _guard = h.enter();
                p.get_auth_header()
            })
        })
        .collect();
    threads.into_iter().map(|t| t.join().unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_callers_coalesce_to_one_exchange() {
    // The real single-flight parity check (mirrors the M2M provider's
    // `test_m2m_concurrent_token_fetch` and Go's federation concurrency test):
    // N callers miss the cache simultaneously, but exactly ONE exchange hits
    // the endpoint — the rest wait for the leader and read its published token.
    // `.expect(1)` is what fails a regression that let every caller race; a
    // wall-time bound would pass even if all N stampeded.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(exchange_response("shared-token")))
        .expect(1)
        .mount(&server)
        .await;

    let http_client = no_retry_http_client();
    let provider = Arc::new(
        FederationProvider::new(
            inner_bearing(&jwt_with_issuer("https://external-idp.example.com")),
            &server.uri(),
            None,
            &http_client,
        )
        .await
        .expect("provider"),
    );

    let results = concurrent_auth_headers(provider, 10);

    // Every caller gets the single exchanged token.
    for r in &results {
        assert_eq!(r.as_deref().unwrap(), "Bearer shared-token");
    }
    // The `.expect(1)` on the mock verifies only one exchange occurred.
}

#[tokio::test(flavor = "multi_thread")]
async fn degraded_endpoint_falls_back_for_all_concurrent_callers() {
    // With retries ENABLED and the endpoint always failing, the single-flight
    // leader exhausts its bounded retry budget and publishes the original-token
    // fallback; followers read it. Every concurrent caller still returns the
    // original token (non-breaking), and no caller surfaces an error.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let inner_token = jwt_with_issuer("https://external-idp.example.com");
    let http_client = bounded_retry_http_client();
    let provider = Arc::new(
        FederationProvider::new(
            inner_bearing(&inner_token),
            &server.uri(),
            None,
            &http_client,
        )
        .await
        .expect("provider"),
    );

    let start = Instant::now();
    let results = concurrent_auth_headers(provider, 8);
    let elapsed = start.elapsed();

    for r in &results {
        assert_eq!(r.as_deref().unwrap(), format!("Bearer {inner_token}"));
    }
    // Coalesced: only the leader spends the retry budget (~0.4s); followers
    // sleep-poll and return as soon as it publishes. A generous bound catches a
    // regression where every caller ran the full failing ladder in parallel and
    // still guards against accidental serialization.
    assert!(
        elapsed < Duration::from_secs(2),
        "concurrent degraded-endpoint calls took {elapsed:?}; expected the leader \
         to bound the wait and followers to read its fallback"
    );
}
