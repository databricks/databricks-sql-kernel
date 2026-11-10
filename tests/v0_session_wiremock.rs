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
//! Retries are disabled because these tests verify status mapping, not
//! retry orchestration (which has dedicated coverage in the HTTP client
//! tests). This keeps retryable responses from sleeping through the
//! production backoff ladder before their final error is observable.

use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, ErrorCode, GeospatialResultRepresentation, HttpConfig,
    ResultConfig, Session,
};
use wiremock::matchers::{method, path, query_param};
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
            identity_federation_client_id: None,
        })
        .http(http)
        .open()
        .await
}

/// Mount a single-response mock for the create-session endpoint.
async fn mock_create_session_returns(server: &MockServer, status: u16, body: &'static str) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
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
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
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
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
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
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
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
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
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
        .auth(AuthConfig::Pat {
            token: "x".into(),
            identity_federation_client_id: None,
        })
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
        .auth(AuthConfig::Pat {
            token: "x".into(),
            identity_federation_client_id: None,
        })
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
        .auth(AuthConfig::Pat {
            token: "x".into(),
            identity_federation_client_id: None,
        })
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
            identity_federation_client_id: None,
        })
        .result(result)
        .open()
        .await
}

async fn assert_no_can_cloud_download_on_create(server: &MockServer) {
    let recorded = server
        .received_requests()
        .await
        .expect("server must have request log enabled");
    let create = recorded
        .iter()
        .find(|r| {
            r.url.path() == "/api/2.0/sql/warehouses/test_wh/sessions"
                && r.method.as_str() == "POST"
        })
        .expect("a CreateSession request was made");
    let body: serde_json::Value =
        serde_json::from_slice(&create.body).expect("create-session body must be valid JSON");
    if let Some(obj) = body.get("session_confs").and_then(|v| v.as_object()) {
        for key in obj.keys() {
            assert!(
                !key.eq_ignore_ascii_case("can_cloud_download"),
                "unexpected session setting: key={key:?} value={:?}",
                obj.get(key),
            );
        }
    }
}

async fn create_session_body(server: &MockServer) -> serde_json::Value {
    let recorded = server
        .received_requests()
        .await
        .expect("server must have request log enabled");
    let create = recorded
        .iter()
        .find(|r| {
            r.url.path() == "/api/2.0/sql/warehouses/test_wh/sessions"
                && r.method.as_str() == "POST"
        })
        .expect("a CreateSession request was made");
    serde_json::from_slice(&create.body).expect("create-session body must be valid JSON")
}

#[tokio::test]
async fn create_session_with_cloudfetch_disabled_requests_inline_results() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-cf-off","warehouse_id":"test_wh"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh").unwrap(),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .result(result)
        .session_conf(std::collections::HashMap::from([
            ("Can_Cloud_Download".into(), "true".into()),
            ("TIMEZONE".into(), "UTC".into()),
        ]))
        .open()
        .await
        .expect("session open with cloudfetch_enabled=false");
    assert_eq!(session.session_id(), "sess-cf-off");
    let body = create_session_body(&server).await;
    assert!(body["session_confs"].get("Can_Cloud_Download").is_none());
    assert_eq!(body["session_confs"]["TIMEZONE"], "UTC");

    assert_eq!(
        create_session_body(&server)
            .await
            .pointer("/session_confs/can_cloud_download"),
        Some(&serde_json::json!("false")),
    );
}

#[tokio::test]
async fn create_session_forwards_user_supplied_session_conf_unchanged() {
    // Caller-supplied server-bound session_conf is no longer filtered or
    // normalized by the kernel. Even keys the server may reject later pass
    // through unchanged; validation belongs to the server.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-dropped","warehouse_id":"test_wh"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut session_conf = std::collections::HashMap::new();
    session_conf.insert("Can_Cloud_Download".to_string(), "true".to_string());
    session_conf.insert("unknown.MixedCase".to_string(), "value".to_string());
    let session = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .session_conf(session_conf)
        .open()
        .await
        .expect("session must open with caller-supplied session_conf");
    assert_eq!(session.session_id(), "sess-dropped");

    let body = create_session_body(&server).await;
    let obj = body
        .get("session_confs")
        .and_then(|v| v.as_object())
        .expect("session_confs object");
    assert_eq!(
        obj.get("Can_Cloud_Download").and_then(|v| v.as_str()),
        Some("true")
    );
    assert_eq!(
        obj.get("unknown.MixedCase").and_then(|v| v.as_str()),
        Some("value")
    );
}

async fn open_session_with_geospatial_config(
    server: &MockServer,
    representation: Option<GeospatialResultRepresentation>,
    session_conf: std::collections::HashMap<String, String>,
) -> Session {
    let mut builder = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .session_conf(session_conf);
    if let Some(representation) = representation {
        builder = builder.geospatial_result_representation(representation);
    }
    builder.open().await.expect("session must open")
}

async fn mount_geospatial_session(server: &MockServer, session_id: &str) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "session_id": session_id,
            "warehouse_id": "test_wh",
        })))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn create_session_keeps_geospatial_string_mode_client_side() {
    let server = MockServer::start().await;
    mount_geospatial_session(&server, "sess-geo-string").await;

    let session = open_session_with_geospatial_config(
        &server,
        Some(GeospatialResultRepresentation::String),
        Default::default(),
    )
    .await;
    assert_eq!(session.session_id(), "sess-geo-string");

    let body = create_session_body(&server).await;
    assert!(body
        .pointer("/session_confs/EnableGeoSpatialSupport")
        .is_none());
}

#[tokio::test]
async fn create_session_keeps_geospatial_binary_mode_client_side() {
    let server = MockServer::start().await;
    mount_geospatial_session(&server, "sess-geo-binary").await;

    let session = open_session_with_geospatial_config(
        &server,
        Some(GeospatialResultRepresentation::Binary),
        Default::default(),
    )
    .await;
    assert_eq!(session.session_id(), "sess-geo-binary");

    let body = create_session_body(&server).await;
    assert!(body
        .pointer("/session_confs/EnableGeoSpatialSupport")
        .is_none());
}

#[tokio::test]
async fn create_session_omits_geospatial_conf_when_unset() {
    let server = MockServer::start().await;
    mount_geospatial_session(&server, "sess-geo-default").await;

    let session = open_session_with_geospatial_config(&server, None, Default::default()).await;
    assert_eq!(session.session_id(), "sess-geo-default");

    let body = create_session_body(&server).await;
    let has_geospatial_conf = body
        .get("session_confs")
        .and_then(|value| value.as_object())
        .is_some_and(|confs| {
            confs
                .keys()
                .any(|key| key.eq_ignore_ascii_case("EnableGeoSpatialSupport"))
        });
    assert!(
        !has_geospatial_conf,
        "unset mode must not synthesize a geospatial server conf"
    );
}

#[tokio::test]
async fn typed_geospatial_config_does_not_reinterpret_raw_session_conf() {
    let server = MockServer::start().await;
    mount_geospatial_session(&server, "sess-geo-pass-through").await;

    let session_conf = std::collections::HashMap::from([
        ("ENABLEGEOSPATIALSUPPORT".to_string(), "true".to_string()),
        ("EnableGeoSpatialSupport".to_string(), "false".to_string()),
        ("ansi_mode".to_string(), "true".to_string()),
    ]);
    let session = open_session_with_geospatial_config(
        &server,
        Some(GeospatialResultRepresentation::Binary),
        session_conf,
    )
    .await;
    assert_eq!(session.session_id(), "sess-geo-pass-through");

    let body = create_session_body(&server).await;
    let confs = body
        .get("session_confs")
        .and_then(|value| value.as_object())
        .expect("session_confs object");
    assert_eq!(
        confs
            .get("ENABLEGEOSPATIALSUPPORT")
            .and_then(|value| value.as_str()),
        Some("true")
    );
    assert_eq!(
        confs
            .get("EnableGeoSpatialSupport")
            .and_then(|value| value.as_str()),
        Some("false")
    );
    assert_eq!(
        confs.get("ansi_mode").and_then(|value| value.as_str()),
        Some("true")
    );
}

#[tokio::test]
async fn create_session_with_cloudfetch_enabled_omits_can_cloud_download() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
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

    assert_no_can_cloud_download_on_create(&server).await;
}
