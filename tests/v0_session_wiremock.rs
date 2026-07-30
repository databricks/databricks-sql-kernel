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
//! exercises the full HTTP path (auth header injection, request
//! dispatch, response decoding) end to end.
//!
//! Retries are disabled because these tests verify status mapping, not
//! retry orchestration (which has dedicated coverage in the HTTP client
//! tests). This keeps retryable responses from sleeping through the
//! production backoff ladder before their final error is observable.

use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, ErrorCode, HttpConfig, ResultConfig, Session,
};
use wiremock::matchers::{body_partial_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Open a session against `mock_server` using a PAT auth provider.
/// Returns the `Result` directly so each test can assert the error
/// shape.
async fn open_session_against(server: &MockServer) -> databricks_sql_kernel::Result<Session> {
    let mut http = HttpConfig::default();
    http.retry_max_retries = 0;

    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
        })
        .http(http)
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
            "session_confs": { "CAN_CLOUD_DOWNLOAD": "false" }
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
            "session_confs": { "CAN_CLOUD_DOWNLOAD": "true" }
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
async fn create_session_respects_user_value_case_insensitively() {
    // If the caller passes the key with a non-canonical case (e.g.
    // `Can_Cloud_Download`), the kernel must recognise it as an
    // already-set value and NOT inject a second `can_cloud_download`
    // alongside. Otherwise both keys would land in the wire body and
    // the server's last-write-wins behaviour would be a coin flip.
    //
    // Note on key casing: `filter_session_conf` uppercases every
    // allowlisted key (the wire normalisation that PR #80 added), so
    // the key on the wire is `CAN_CLOUD_DOWNLOAD` regardless of what
    // the user supplied — but the user's *value* survives because
    // we detect "already set" case-insensitively before injecting.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-case","warehouse_id":"test_wh"}"#),
        )
        .mount(&server)
        .await;

    let mut session_conf = std::collections::HashMap::new();
    session_conf.insert("Can_Cloud_Download".to_string(), "true".to_string());
    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    Session::builder()
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
        .expect("session must open");

    // Verify the wire request contains exactly one
    // can_cloud_download-shaped key, post-uppercase, carrying the
    // user-supplied value `"true"`. A regression that double-inserts
    // would either produce a second key or override the value to
    // `"false"`.
    let recorded = server.received_requests().await.unwrap();
    let create = recorded
        .iter()
        .find(|r| r.url.path() == "/api/2.0/sql/sessions")
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&create.body).unwrap();
    let session_conf_obj = body
        .get("session_confs")
        .and_then(|v| v.as_object())
        .expect("session_confs present");
    let matches: Vec<(&String, &serde_json::Value)> = session_conf_obj
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("can_cloud_download"))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "exactly one can_cloud_download-shaped key must be present; got {matches:?}",
    );
    let (key, value) = matches[0];
    assert_eq!(
        key, "CAN_CLOUD_DOWNLOAD",
        "the allowlist filter uppercases the key on the wire",
    );
    assert_eq!(
        value.as_str(),
        Some("true"),
        "the user-supplied value must survive the injection check",
    );
}

#[tokio::test]
async fn create_session_empty_user_value_is_overridden() {
    // SEA session-conf semantics for empty-string values are
    // undefined, so the kernel treats an explicit empty value as
    // "not set" and falls back to injecting its canonical
    // `"false"`. This avoids shipping an ambiguous empty string on
    // the wire.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .and(body_partial_json(serde_json::json!({
            "session_confs": { "CAN_CLOUD_DOWNLOAD": "false" }
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-empty","warehouse_id":"test_wh"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut session_conf = std::collections::HashMap::new();
    session_conf.insert("can_cloud_download".to_string(), "".to_string());
    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    Session::builder()
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
        .expect("session must open");
}

#[tokio::test]
async fn create_session_noncanonical_case_with_empty_value_purges_and_injects() {
    // Cross-product cell of the case-insensitive / empty-value
    // matrix: user passes `"Can_Cloud_Download" -> ""` with
    // CloudFetch disabled. The kernel must:
    //   1. Recognise the value is empty (treat it as "not set").
    //   2. Purge the case-variant before injecting the canonical
    //      key — otherwise BOTH `Can_Cloud_Download: ""` and
    //      `can_cloud_download: "false"` ship in the wire body
    //      and the server picks one non-deterministically.
    // Asserts the recorded body has exactly one
    // can_cloud_download-shaped key whose value is `"false"`.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-cross","warehouse_id":"test_wh"}"#),
        )
        .mount(&server)
        .await;

    let mut session_conf = std::collections::HashMap::new();
    session_conf.insert("Can_Cloud_Download".to_string(), "".to_string());
    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    Session::builder()
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
        .expect("session must open");

    let recorded = server.received_requests().await.unwrap();
    let create = recorded
        .iter()
        .find(|r| r.url.path() == "/api/2.0/sql/sessions")
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&create.body).unwrap();
    let session_conf_obj = body
        .get("session_confs")
        .and_then(|v| v.as_object())
        .expect("session_confs present");
    let matches: Vec<(&String, &serde_json::Value)> = session_conf_obj
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("can_cloud_download"))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "exactly one can_cloud_download-shaped key must be present after \
         purge-and-inject; got {matches:?}",
    );
    let (key, value) = matches[0];
    assert_eq!(
        key, "CAN_CLOUD_DOWNLOAD",
        "the allowlist filter uppercases the kernel-injected key on the wire",
    );
    assert_eq!(
        value.as_str(),
        Some("false"),
        "the kernel must have injected the canonical \"false\" value",
    );
}

#[tokio::test]
async fn create_session_with_cloudfetch_enabled_respects_user_inline_request() {
    // Mirror of `create_session_respects_user_supplied_can_cloud_download`.
    // Setup: kernel CloudFetch ENABLED, but the user explicitly wants
    // inline-Arrow (sets `can_cloud_download=false` themselves). The
    // kernel must not strip the user's value — `session_conf` is the
    // user's wire surface and the kernel only ever *injects* on the
    // `!cloudfetch_enabled` branch, never overrides.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .and(body_partial_json(serde_json::json!({
            "session_confs": { "CAN_CLOUD_DOWNLOAD": "false" }
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-mirror","warehouse_id":"test_wh"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut session_conf = std::collections::HashMap::new();
    session_conf.insert("can_cloud_download".to_string(), "false".to_string());
    // cloudfetch_enabled defaults to true; user is overriding to inline.
    let result = ResultConfig::default();
    assert!(
        result.cloudfetch_enabled,
        "test prereq: ResultConfig defaults to cloudfetch_enabled=true",
    );
    Session::builder()
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
        .expect("session must open");
}

#[tokio::test]
async fn create_session_with_cloudfetch_enabled_omits_can_cloud_download() {
    // The default (cloudfetch_enabled=true) path must NOT inject
    // `can_cloud_download` — silently sending it would tell SEA to
    // serve every query inline, which would regress the CloudFetch
    // concurrent download pipeline for the kernel's primary use case.
    //
    // We assert this by inspecting the recorded request body and
    // verifying that `session_configuration` does NOT contain a
    // `can_cloud_download` field with **any** value (not just
    // "false"). A regression that injected `"true"` would be just
    // as wrong, so the previous one-sided `.expect(0)` check on
    // `{"can_cloud_download": "false"}` alone wasn't enough.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-cf-on","warehouse_id":"test_wh"}"#),
        )
        .mount(&server)
        .await;

    let result = ResultConfig::default();
    assert!(
        result.cloudfetch_enabled,
        "ResultConfig::default() must have cloudfetch_enabled=true",
    );
    let session = open_session_with_result_cfg(&server, result)
        .await
        .expect("session open with default ResultConfig");
    assert_eq!(session.session_id(), "sess-cf-on");

    // Inspect the actual recorded request body to confirm the
    // can_cloud_download key is absent regardless of value.
    let recorded = server
        .received_requests()
        .await
        .expect("server must have request log enabled");
    let create_session_req = recorded
        .iter()
        .find(|r| r.url.path() == "/api/2.0/sql/sessions" && r.method.as_str() == "POST")
        .expect("a POST /sessions request was made");
    let body: serde_json::Value = serde_json::from_slice(&create_session_req.body)
        .expect("create-session body must be valid JSON");
    let session_conf = body
        .get("session_confs")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    if let Some(obj) = session_conf.as_object() {
        for key in obj.keys() {
            assert!(
                !key.eq_ignore_ascii_case("can_cloud_download"),
                "session_configuration must not contain a can_cloud_download key on the \
                 default path; found key={key:?} value={:?}",
                obj.get(key),
            );
        }
    }
}
