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

//! Regression test for issue #213 (CONNPROP-004): a `LoggingConfig`
//! configured on a `SessionBuilder` must actually produce log output on
//! the **direct-Rust** `Session::open` path.
//!
//! Before the fix, `SessionBuilder::logging(LoggingConfig)` was accepted
//! and stored on `SessionConfig::logging` but never consumed — the
//! subscriber installer (`logging::init_logging`) was reachable only
//! through the C ABI's `kernel_init_logging`. So a direct-Rust caller's
//! configured log destination was a silent no-op. `Session::open` now
//! installs the subscriber from `LoggingConfig`, matching the C ABI path.
//!
//! ## Why this lives in its own test binary
//!
//! The tracing subscriber installs at most once per process (`OnceLock`,
//! first-call-wins), and every `Session::open` now calls `init_logging`.
//! If this assertion shared a binary with any other test that opened a
//! session first, that earlier open would install a subscriber with **no**
//! log file and this file-backed install would be a no-op. Isolating it in
//! its own integration-test file guarantees this is the first (and only)
//! `init_logging` call in its process, so the file destination wins.

use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, HttpConfig, LoggingConfig, Session, KERNEL_LOG_TARGET,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mount a successful CreateSession stub.
async fn mock_create_session_ok(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-log-213","warehouse_id":"test_wh"}"#),
        )
        .mount(server)
        .await;
}

/// CONNPROP-004 — a configured log destination actually produces log
/// output on the direct-Rust `Session::open` path.
///
/// Opening a session emits a `debug!` "creating session" record on the
/// kernel's own log target, so a `debug`-level filter plus a file
/// destination must leave a non-empty log file behind.
#[tokio::test]
async fn logging_config_file_destination_is_written() {
    let server = MockServer::start().await;
    mock_create_session_ok(&server).await;

    let log_file = std::env::temp_dir().join(format!(
        "kt213_logging_{}.log",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // `LoggingConfig` is `#[non_exhaustive]`: build from default and assign.
    let mut logging = LoggingConfig::default();
    logging.filter = Some(format!("{KERNEL_LOG_TARGET}=debug"));
    logging.file = Some(log_file.clone());

    let mut http = HttpConfig::default();
    http.retry_max_retries = 0;

    let session = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .http(http)
        .logging(logging)
        .open()
        .await
        .expect("Session::open with a log destination configured must succeed");

    assert_eq!(session.session_id(), "sess-log-213");

    // The subscriber is installed and the "creating session" debug record
    // has been emitted synchronously during open(); the file must exist and
    // be non-empty.
    let size = std::fs::metadata(&log_file).map(|m| m.len()).unwrap_or(0);
    let _ = std::fs::remove_file(&log_file);

    assert!(
        size > 0,
        "issue #213: the configured log destination {log_file:?} was not written (size {size}); \
         Session::open must install a subscriber from LoggingConfig, not leave it inert"
    );

    // Idempotency: a second Session::open in the same process re-invokes
    // init_logging, which must be a harmless no-op (the subscriber installs
    // once per process, first-call-wins) — not an error that fails open().
    // This exercises the once-per-process invariant through the public path.
    let mut logging2 = LoggingConfig::default();
    logging2.filter = Some(format!("{KERNEL_LOG_TARGET}=debug"));
    logging2.file = Some(std::env::temp_dir().join("kt213_second_open_should_be_noop.log"));
    let mut http2 = HttpConfig::default();
    http2.retry_max_retries = 0;
    let session2 = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .http(http2)
        .logging(logging2)
        .open()
        .await
        .expect("a second Session::open must succeed; re-init of logging is a no-op");
    assert_eq!(session2.session_id(), "sess-log-213");
}
