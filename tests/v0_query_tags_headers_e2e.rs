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

//! End-to-end smoke tests for per-statement **query tags** and custom
//! **HTTP headers** against a live Databricks workspace.
//!
//! These are *smoke* tests: they assert the tagged / custom-header
//! request **succeeds** end-to-end. That proves the server accepts the
//! `query_tags` statement-conf and the custom headers — a malformed
//! `query_tags` conf, or a header that breaks the request, surfaces as
//! a failed execute here.
//!
//! Why not assert harder:
//! - **query tags**: `system.query.history` carries a `query_tags`
//!   column, but its ingestion lag is minutes+ (empirically nothing
//!   after 60s), so a synchronous assertion would be flaky/slow. The
//!   exact wire serialisation is pinned by the unit tests in
//!   `src/statement/spec.rs` (`serialize_query_tags_*`).
//! - **headers**: the server doesn't echo arbitrary request headers
//!   back. The exact on-the-wire behaviour (custom header + caller
//!   User-Agent sent; `Authorization` / `x-databricks-org-id` win) is
//!   pinned by the wiremock test in `src/client/http.rs`
//!   (`test_custom_headers_applied_with_auth_winning`).
//!
//! Run with:
//!
//! ```sh
//! set -a && source ~/.databricks/pecotesting-creds && set +a
//! cargo test --test v0_query_tags_headers_e2e -- --ignored
//! ```

use databricks_sql_kernel::{
    AuthConfig, ExecutedStatementHandle, HttpConfig, ResultStream, Session,
};
use std::collections::HashMap;
use std::env;

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty())
}

fn host_path_token() -> (String, String, String) {
    (
        env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST required"),
        env_var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required"),
        env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN required"),
    )
}

async fn live_session() -> Session {
    let (host, path, token) = host_path_token();
    Session::builder()
        .http_path(host, path)
        .auth(AuthConfig::Pat { token })
        .open()
        .await
        .expect("Session::open against live workspace")
}

/// Open a session whose HTTP client sends the given custom headers on
/// every request.
// `HttpConfig` is `#[non_exhaustive]`, so it can't be built with a
// struct literal from this external test crate — the
// default-then-assign form is the only option, and the clippy lint
// that prefers a literal doesn't apply.
#[allow(clippy::field_reassign_with_default)]
async fn live_session_with_headers(headers: Vec<(String, String)>) -> Session {
    let (host, path, token) = host_path_token();
    let mut http = HttpConfig::default();
    http.custom_headers = headers;
    Session::builder()
        .http_path(host, path)
        .auth(AuthConfig::Pat { token })
        .http(http)
        .open()
        .await
        .expect("Session::open against live workspace (custom headers)")
}

/// Drain `SELECT 1` and assert exactly one row == 1.
async fn assert_select_one(stream: &mut ResultStream) {
    stream.next_batch().await.unwrap().expect("one batch");
    let row = stream.next_row_in_batch().expect("one row");
    assert_eq!(row.get_i64(0).unwrap(), Some(1));
}

#[tokio::test]
#[ignore]
async fn query_tags_round_trip_succeeds() {
    // A query carrying per-statement query tags must execute cleanly.
    // The server validates the `query_tags` conf on receipt; a
    // malformed serialisation would fail the execute.
    let session = live_session().await;
    let mut stmt = session.statement();
    let mut tags = HashMap::new();
    tags.insert("team".to_string(), Some("platform".to_string()));
    tags.insert("dashboard".to_string(), Some("abc123".to_string()));
    stmt.spec().sql("SELECT 1 AS n");
    stmt.spec().query_tags(tags).expect("query_tags accepted");

    let mut exec = stmt.execute().await.expect("execute with query tags");
    assert_select_one(exec.result_stream_mut()).await;
    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn query_tags_with_none_value_succeeds() {
    // A `None`-valued tag serialises to a bare key (no colon). Confirm
    // the server accepts that wire shape too.
    let session = live_session().await;
    let mut stmt = session.statement();
    let mut tags = HashMap::new();
    tags.insert("production".to_string(), None);
    tags.insert("team".to_string(), Some("platform".to_string()));
    stmt.spec().sql("SELECT 1 AS n");
    stmt.spec()
        .query_tags(tags)
        .expect("query_tags (with None value) accepted");

    let mut exec = stmt.execute().await.expect("execute with bare-key tag");
    assert_select_one(exec.result_stream_mut()).await;
    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn custom_http_headers_round_trip_succeeds() {
    // A session with custom headers (incl. a custom User-Agent, the
    // way the connector forwards its composed UA) must execute cleanly.
    // The kernel's own Authorization is applied last and wins; the
    // custom headers ride alongside. A header that broke the request
    // would surface as a failed execute here.
    //
    // A caller `User-Agent` REPLACES the kernel's base UA entirely (see
    // `HttpClient::new`). The SEA gateway gates the `INLINE_OR_EXTERNAL_LINKS`
    // result disposition on a `startsWith` match against its allow-list of
    // known driver prefixes, so the caller UA MUST carry a real, allow-listed
    // connector prefix — a made-up prefix is rejected with HTTP 400
    // ("not a supported disposition"). Use the actual Node connector UA
    // (`NodejsDatabricksSqlConnector/...`), the way the NodeJS driver forwards
    // its composed UA over the napi binding.
    let session = live_session_with_headers(vec![
        ("X-Custom-E2E-Header".to_string(), "kernel-e2e".to_string()),
        (
            "User-Agent".to_string(),
            "NodejsDatabricksSqlConnector/1.0 (smoke-test)".to_string(),
        ),
    ])
    .await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let mut exec = stmt.execute().await.expect("execute with custom headers");
    assert_select_one(exec.result_stream_mut()).await;
    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn custom_authorization_header_is_dropped_and_real_auth_wins() {
    // A caller-supplied `Authorization` custom header must be DROPPED —
    // the kernel always applies the real PAT. If the bogus token leaked
    // through (or replaced the real one), the server would reject with
    // 401 and `SELECT 1` would fail. Success proves the reserved-header
    // skip holds against a live server (the wiremock test pins the same
    // behaviour at the unit level).
    let session = live_session_with_headers(vec![(
        "Authorization".to_string(),
        "Bearer obviously-bogus-token".to_string(),
    )])
    .await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let mut exec = stmt
        .execute()
        .await
        .expect("execute must succeed — the real PAT wins, the bogus Authorization is dropped");
    assert_select_one(exec.result_stream_mut()).await;
    exec.close().await.expect("close");
    session.close().await.expect("session close");
}
