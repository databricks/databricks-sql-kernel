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

//! Wiremock end-to-end tests for `Session::open` HTTP-status mapping.
//!
//! Stubs the SEA `POST /api/2.0/sql/sessions` endpoint with each
//! status code in the migration plan's mapping table and asserts the
//! resulting `kernel_error::Error` carries the right `ErrorCode` and
//! `retryable` flag. This is the §7 follow-up that PR 1 deferred —
//! `Session::open` is the natural place to drive it because it
//! exercises the full HTTP path (auth header injection, retry
//! orchestration, response decoding) end to end.
//!
//! The retry-disabled requests give us a single-shot status check
//! per status code; the three "retryable" responses (429, 503, 504)
//! are stubbed to fire enough times to exhaust the auth retry
//! ceiling so we observe the final error code.

use databricks_sql_kernel::{AuthConfig, ConnectionConfig, ErrorCode, ResultConfig, Session};
use wiremock::matchers::{body_partial_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Open a session against `mock_server` using a PAT auth provider.
/// Returns the `Result` directly so each test can assert the error
/// shape.
async fn open_session_against(server: &MockServer) -> databricks_sql_kernel::Result<Session> {
    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
        })
        .open()
        .await
}

/// Mount a single-response mock for the create-session endpoint.
async fn mock_create_session_returns(server: &MockServer, status: u16, body: &'static str) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(ResponseTemplate::new(status).set_body_string(body))
        .mount(server)
        .await;
}

#[tokio::test]
async fn create_session_401_maps_to_unauthenticated() {
    let server = MockServer::start().await;
    mock_create_session_returns(&server, 401, r#"{"message":"invalid token"}"#).await;

    let err = open_session_against(&server)
        .await
        .expect_err("401 should fail open()");
    assert_eq!(err.code, ErrorCode::Unauthenticated);
    assert!(!err.retryable, "401 must not be retryable");
}

#[tokio::test]
async fn create_session_403_maps_to_permission_denied() {
    let server = MockServer::start().await;
    mock_create_session_returns(&server, 403, r#"{"message":"forbidden"}"#).await;

    let err = open_session_against(&server)
        .await
        .expect_err("403 should fail open()");
    assert_eq!(err.code, ErrorCode::PermissionDenied);
    assert!(!err.retryable);
}

#[tokio::test]
async fn create_session_404_maps_to_not_found() {
    let server = MockServer::start().await;
    mock_create_session_returns(&server, 404, r#"{"message":"warehouse not found"}"#).await;

    let err = open_session_against(&server)
        .await
        .expect_err("404 should fail open()");
    assert_eq!(err.code, ErrorCode::NotFound);
    assert!(!err.retryable);
}

#[tokio::test]
async fn create_session_429_maps_to_resource_exhausted_retryable() {
    // 429 is retryable, so the HTTP client will burn through its
    // retry ceiling before surfacing the error. We stub the same
    // 429 response on every attempt.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let err = open_session_against(&server)
        .await
        .expect_err("429 should fail open()");
    assert_eq!(err.code, ErrorCode::ResourceExhausted);
    assert!(err.retryable, "429 must be retryable");
}

#[tokio::test]
async fn create_session_503_maps_to_unavailable_retryable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(ResponseTemplate::new(503).set_body_string("warehouse cold"))
        .mount(&server)
        .await;

    let err = open_session_against(&server)
        .await
        .expect_err("503 should fail open()");
    assert_eq!(err.code, ErrorCode::Unavailable);
    assert!(err.retryable, "503 must be retryable");
}

#[tokio::test]
async fn create_session_504_maps_to_timeout_retryable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(ResponseTemplate::new(504).set_body_string("upstream timeout"))
        .mount(&server)
        .await;

    let err = open_session_against(&server)
        .await
        .expect_err("504 should fail open()");
    assert_eq!(err.code, ErrorCode::Timeout);
    assert!(err.retryable, "504 must be retryable");
}

#[tokio::test]
async fn create_session_400_maps_to_invalid_argument() {
    // 4xx that isn't one of the well-known codes falls back to
    // InvalidArgument per the plan's mapping table.
    let server = MockServer::start().await;
    mock_create_session_returns(&server, 400, r#"{"message":"bad request"}"#).await;

    let err = open_session_against(&server)
        .await
        .expect_err("400 should fail open()");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(!err.retryable);
}

#[tokio::test]
async fn create_session_500_maps_to_internal() {
    // 5xx that isn't one of the retryable codes falls back to
    // Internal per the plan's mapping table.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("server bug"))
        .mount(&server)
        .await;

    let err = open_session_against(&server)
        .await
        .expect_err("500 should fail open()");
    assert_eq!(err.code, ErrorCode::Internal);
}

#[tokio::test]
async fn create_session_success_returns_session_with_id() {
    let server = MockServer::start().await;
    mock_create_session_returns(
        &server,
        200,
        r#"{"session_id":"sess-abc-123","warehouse_id":"test_wh"}"#,
    )
    .await;

    let session = open_session_against(&server)
        .await
        .expect("success path should yield a Session");
    assert_eq!(session.session_id(), "sess-abc-123");
}

#[tokio::test]
async fn http_status_carries_through_error() {
    // Verify that the http_status field is populated end-to-end —
    // wrappers parsing kernel_error::Error structurally need this.
    let server = MockServer::start().await;
    mock_create_session_returns(&server, 401, "unauthorized").await;

    let err = open_session_against(&server).await.unwrap_err();
    assert_eq!(err.http_status, Some(401));
}

#[tokio::test]
async fn missing_required_fields_returns_invalid_argument() {
    // No connection / no http_path / no auth → InvalidArgument
    // before any wire call.
    let err = Session::builder()
        .open()
        .await
        .expect_err("empty builder must fail");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("connection") || err.message.contains("http_path"));
}

#[tokio::test]
async fn missing_auth_returns_invalid_argument() {
    let err = Session::builder()
        .connection(ConnectionConfig::new("https://h", "wh"))
        .open()
        .await
        .expect_err("missing auth must fail");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("auth"));
}

#[tokio::test]
async fn malformed_http_path_surfaces_at_open() {
    let err = Session::builder()
        .http_path("https://h", "/not/a/sql/path")
        .auth(AuthConfig::Pat { token: "x".into() })
        .open()
        .await
        .expect_err("malformed http_path must fail at open()");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("/not/a/sql/path"));
}

#[tokio::test]
async fn connection_setter_clears_prior_http_path_parse_error() {
    // Footgun guard: a prior failed `.http_path(...)` left a sentinel;
    // a subsequent `.connection(...)` must clear it so `open()`
    // doesn't reject the now-valid override.
    let server = MockServer::start().await;
    mock_create_session_returns(
        &server,
        200,
        r#"{"session_id":"sess-after-override","warehouse_id":"test_wh"}"#,
    )
    .await;

    let session = Session::builder()
        .http_path("https://h", "/garbage")
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("override path"),
        )
        .auth(AuthConfig::Pat { token: "x".into() })
        .open()
        .await
        .expect("override should win — open() must succeed");
    assert_eq!(session.session_id(), "sess-after-override");
}

#[tokio::test]
async fn http_path_setter_clears_prior_parse_error_on_success() {
    // Symmetric: a prior failed `.http_path("/garbage")` followed by
    // a successful `.http_path("/sql/1.0/warehouses/x")` should
    // clear the sentinel.
    let server = MockServer::start().await;
    mock_create_session_returns(
        &server,
        200,
        r#"{"session_id":"sess-recover","warehouse_id":"test_wh"}"#,
    )
    .await;

    let session = Session::builder()
        .http_path("https://h", "/garbage")
        .http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
        .auth(AuthConfig::Pat { token: "x".into() })
        .open()
        .await
        .expect("recovery http_path call should clear the sentinel");
    assert_eq!(session.session_id(), "sess-recover");
}

#[tokio::test]
async fn session_close_sends_warehouse_id_query_param() {
    // SEA requires `warehouse_id` as a query parameter on
    // `DELETE /api/2.0/sql/sessions/{id}`. Without it the server
    // returns HTTP 400 and the session leaks until idle timeout.
    //
    // wiremock's `expect(1)` + the DELETE matcher (path + query_param)
    // together assert (a) the DELETE was hit, (b) at the right path,
    // and (c) with `warehouse_id=test_wh` in the query string. The
    // expectation is verified on `MockServer::drop`.
    let server = MockServer::start().await;
    mock_create_session_returns(
        &server,
        200,
        r#"{"session_id":"sess-close-1","warehouse_id":"test_wh"}"#,
    )
    .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/sess-close-1"))
        .and(query_param("warehouse_id", "test_wh"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let session = open_session_against(&server)
        .await
        .expect("session must open");
    session.close().await.expect("close must succeed");
    // MockServer::drop runs the .expect(1) assertion when `server`
    // goes out of scope at end-of-test.
}

/// Open a session with a custom `ResultConfig`. Used by the
/// `can_cloud_download` tests below — the standard `open_session_against`
/// uses the default config.
async fn open_session_with_result_cfg(
    server: &MockServer,
    result: ResultConfig,
) -> databricks_sql_kernel::Result<Session> {
    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
        })
        .result(result)
        .open()
        .await
}

#[tokio::test]
async fn create_session_with_cloudfetch_disabled_sends_can_cloud_download_false() {
    // Per the SEA inline-Arrow design (go/inline-arrow-sea, DD1), when
    // the kernel-level `cloudfetch_enabled` flag is false we forward
    // `can_cloud_download: "false"` in `session_configuration` so SEA
    // routes the session to the inline-Arrow path.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .and(body_partial_json(serde_json::json!({
            "session_configuration": { "can_cloud_download": "false" }
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-cf-off","warehouse_id":"test_wh"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = open_session_with_result_cfg(&server, result)
        .await
        .expect("session open with cloudfetch_enabled=false");
    assert_eq!(session.session_id(), "sess-cf-off");
}

#[tokio::test]
async fn create_session_respects_user_supplied_can_cloud_download() {
    // If the caller explicitly wired `can_cloud_download` via session
    // conf (e.g., to force inline-Arrow even with CloudFetch enabled
    // at the kernel layer), the kernel must NOT override that value.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .and(body_partial_json(serde_json::json!({
            "session_configuration": { "can_cloud_download": "true" }
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-explicit","warehouse_id":"test_wh"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    // cloudfetch_enabled=false would normally inject
    // can_cloud_download="false". But the user has explicitly set
    // "true" via session_conf; the kernel must yield to the user.
    let mut session_conf = std::collections::HashMap::new();
    session_conf.insert("can_cloud_download".to_string(), "true".to_string());
    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
        })
        .result(result)
        .session_conf(session_conf)
        .open()
        .await
        .expect("session must open with user-supplied conf");
    assert_eq!(session.session_id(), "sess-explicit");
}

#[tokio::test]
async fn create_session_with_cloudfetch_enabled_omits_can_cloud_download() {
    // The default (cloudfetch_enabled=true) path must NOT inject
    // `can_cloud_download` — silently sending it would tell SEA to
    // serve every query inline, which would regress the CloudFetch
    // concurrent download pipeline for the kernel's primary use case.
    //
    // We assert this by requiring the request body to NOT contain a
    // `can_cloud_download` field anywhere under `session_configuration`.
    // wiremock's `body_partial_json` matches if the request's JSON
    // contains the given keys; we therefore mount a NEGATIVE
    // expectation (`.expect(0)`) and require the affirmative
    // can_cloud_download path to never fire.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .and(body_partial_json(serde_json::json!({
            "session_configuration": { "can_cloud_download": "false" }
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    // Also mount a permissive responder so the session-create call
    // actually returns 200 — without this the kernel would surface
    // a 404 and our negative-expect would still pass for the wrong
    // reason.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-cf-on","warehouse_id":"test_wh"}"#),
        )
        .mount(&server)
        .await;

    // Default ResultConfig has cloudfetch_enabled=true.
    let result = ResultConfig::default();
    assert!(
        result.cloudfetch_enabled,
        "ResultConfig::default() must have cloudfetch_enabled=true",
    );
    let session = open_session_with_result_cfg(&server, result)
        .await
        .expect("session open with default ResultConfig");
    assert_eq!(session.session_id(), "sess-cf-on");
}
