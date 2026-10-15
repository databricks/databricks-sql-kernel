// Copyright (c) 2026 Databricks, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databricks_sql_kernel::{AuthConfig, ConnectionConfig, ErrorCode, HttpConfig, Session};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn token() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "access_token": "shared-token", "token_type": "Bearer", "expires_in": 3600,
    }))
}

fn http(retries: u32) -> HttpConfig {
    let mut config = HttpConfig::default();
    config.retry_max_retries = retries;
    config.retry_min_wait = Duration::from_millis(1);
    config.retry_max_wait = Duration::from_millis(10);
    config.overall_timeout = Duration::from_secs(5);
    config
}

async fn mount_session(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .and(header("authorization", "Bearer shared-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "session_id": "m2m-session", "warehouse_id": "test_wh",
        })))
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/m2m-session"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

async fn open(
    identity: &str,
    host: &str,
    endpoint: Option<String>,
    http: HttpConfig,
) -> databricks_sql_kernel::Result<Session> {
    Session::builder()
        .connection(ConnectionConfig::from_http_path(host, "/sql/1.0/warehouses/test_wh").unwrap())
        .auth(AuthConfig::OAuthM2M {
            client_id: identity.into(),
            client_secret: "secret".into(),
            scopes: None,
            token_url_override: endpoint,
            identity_federation_client_id: None,
        })
        .http(http)
        .open()
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_connections_share_discovery_and_token_exchange() {
    let server = MockServer::start().await;
    mount_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/oidc/.well-known/oauth-authorization-server"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "authorization_endpoint": format!("{}/authorize", server.uri()),
                    "token_endpoint": format!("{}/token", server.uri()),
                }))
                .set_delay(Duration::from_millis(100)),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(token().set_delay(Duration::from_millis(100)))
        .expect(1)
        .mount(&server)
        .await;

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let host = server.uri();
        tasks.push(tokio::spawn(async move {
            let session = open(
                "independent_connections_share_discovery_and_token_exchange",
                &host,
                None,
                http(0),
            )
            .await
            .unwrap();
            session.close().await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    // Reopening after all earlier sessions close must keep the cached token.
    let session = open(
        "independent_connections_share_discovery_and_token_exchange",
        &server.uri(),
        None,
        http(0),
    )
    .await
    .unwrap();
    session.close().await.unwrap();
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn token_exchange_retries_rate_limits_and_server_errors() {
    let server = MockServer::start().await;
    mount_session(&server).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(
            move |_: &wiremock::Request| match count.fetch_add(1, Ordering::SeqCst) {
                0 => ResponseTemplate::new(429)
                    .insert_header("Retry-After", "1")
                    .set_body_json(json!({"error": "temporarily_unavailable"})),
                1 => ResponseTemplate::new(503).set_body_json(json!({"error": "server_error"})),
                _ => token(),
            },
        )
        .expect(3)
        .mount(&server)
        .await;
    let mut config = http(2);
    config.retry_max_wait = Duration::from_secs(2);
    let start = Instant::now();
    let session = open(
        "token_exchange_retries_rate_limits_and_server_errors",
        &server.uri(),
        Some(format!("{}/token", server.uri())),
        config,
    )
    .await
    .unwrap();
    assert!(
        start.elapsed() >= Duration::from_secs(1),
        "Retry-After must be honored"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn exhausted_token_retries_preserve_status_and_allow_a_later_connection() {
    let server = MockServer::start().await;
    mount_session(&server).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_: &wiremock::Request| {
            if count.fetch_add(1, Ordering::SeqCst) < 4 {
                ResponseTemplate::new(429)
                    .set_body_json(json!({"error": "temporarily_unavailable"}))
            } else {
                token()
            }
        })
        .expect(5)
        .mount(&server)
        .await;
    let endpoint = Some(format!("{}/token", server.uri()));
    let error = open(
        "exhausted_token_retries_preserve_status_and_allow_a_later_connection",
        &server.uri(),
        endpoint.clone(),
        http(2),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::ResourceExhausted);
    assert_eq!(error.http_status, Some(429));
    assert!(error.retryable);
    // Sessions give Auth its own budget of three retries.
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    let session = open(
        "exhausted_token_retries_preserve_status_and_allow_a_later_connection",
        &server.uri(),
        endpoint,
        http(0),
    )
    .await
    .unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_credentials_are_not_retried() {
    for status in [400, 401, 403] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(
                ResponseTemplate::new(status).set_body_json(json!({"error": "invalid_client"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let error = open(
            "invalid_credentials_are_not_retried",
            &server.uri(),
            Some(format!("{}/token", server.uri())),
            http(3),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Unauthenticated);
        assert_eq!(error.http_status, Some(status));
        assert!(!error.retryable);
        server.verify().await;
    }
}

#[test]
fn shared_token_state_survives_the_original_connection_runtime() {
    let server_runtime = tokio::runtime::Runtime::new().unwrap();
    let server = server_runtime.block_on(async {
        let server = MockServer::start().await;
        mount_session(&server).await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(token())
            .expect(1)
            .mount(&server)
            .await;
        server
    });
    for _ in 0..2 {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let session = open(
                "shared_token_state_survives_the_original_connection_runtime",
                &server.uri(),
                Some(format!("{}/token", server.uri())),
                http(0),
            )
            .await
            .unwrap();
            session.close().await.unwrap();
        });
    }
    server_runtime.block_on(server.verify());
}
