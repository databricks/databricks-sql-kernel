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

//! End-to-end TLS tests for the v0 `Session` surface, exercised
//! through an intercepting HTTPS proxy (mitmproxy) against a live
//! Databricks workspace.
//!
//! ## How this works (ported from `adbc-drivers/databricks`'s
//! `tls_e2e.rs`)
//!
//! mitmproxy sits in front of the workspace and re-signs every TLS
//! connection with its own CA. A client that trusts only the system
//! roots therefore sees an *untrusted* certificate and the handshake
//! fails — unless we either (a) add mitmproxy's CA to the trust store
//! via [`TlsConfig::custom_ca_cert`], or (b) disable validation via
//! [`TlsConfig::accept_self_signed`]. The three tests below cover
//! exactly those three outcomes (fail / CA-trusted / self-signed),
//! mirroring the upstream ADBC suite but rewritten against the v0
//! kernel API (`SessionBuilder::tls` + `AuthConfig`).
//!
//! ## Why the proxy comes from `HTTPS_PROXY`, not the v0 API
//!
//! The v0 public `ProxyConfig` is still a reserved empty struct
//! (`src/config.rs`), so `SessionBuilder::proxy` can't yet point the
//! client at mitmproxy. We don't need it to: the kernel only calls
//! `reqwest::ClientBuilder::proxy` when an *explicit* proxy URL is set
//! (`src/client/http.rs`), and otherwise leaves reqwest's default
//! behaviour intact — which honours the `HTTP_PROXY` / `HTTPS_PROXY`
//! environment variables. The CI job sets `HTTPS_PROXY=http://localhost:8080`
//! so all traffic flows through mitmproxy with no code change. When the
//! v0 proxy surface lands, a fourth test should drive the proxy through
//! `SessionBuilder::proxy` directly.
//!
//! ## Running locally
//!
//! ```sh
//! # 1. Start mitmproxy and grab its CA cert.
//! docker run -d --name mitmproxy -p 8080:8080 \
//!   mitmproxy/mitmproxy:12.2.1 mitmdump --set stream_large_bodies=0
//! docker cp mitmproxy:/home/mitmproxy/.mitmproxy/mitmproxy-ca-cert.pem \
//!   /tmp/mitmproxy-ca-cert.pem
//!
//! # 2. Point the tests at it and run.
//! set -a && source ~/.databricks/pecotesting-creds && set +a
//! export MITMPROXY_CA_CERT=/tmp/mitmproxy-ca-cert.pem
//! export HTTPS_PROXY=http://localhost:8080
//! cargo test --test v0_tls_e2e -- --ignored --nocapture
//! ```
//!
//! All tests are `#[ignore]` so the default `cargo test` run (which has
//! no proxy / workspace) skips them.
//!
//! Every test uses `#[tokio::test(flavor = "multi_thread")]`. The OAuth
//! M2M provider's sync `get_auth_header` acquires its token via
//! `block_in_place` + `block_on` (`src/auth/oauth/m2m.rs`), and
//! `block_in_place` panics on a current-thread runtime. Real consumers
//! (the pyo3 / napi wrappers) always drive the kernel on a multi-thread
//! runtime, so the tests match that. The PAT path doesn't make a token
//! call and is flavor-agnostic, but we keep all three uniform so the
//! suite passes regardless of which auth the configured creds select.

use databricks_sql_kernel::{
    AuthConfig, ExecutedStatementHandle, ResultStream, Session, TlsConfig,
};
use std::env;

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty())
}

/// Auth for the live workspace. Prefers OAuth M2M (so the TLS suite
/// doubles as live coverage of the client-credentials flow through the
/// proxy); falls back to PAT when only a token is configured.
fn live_auth() -> AuthConfig {
    if let (Some(client_id), Some(client_secret)) = (
        env_var("DATABRICKS_TEST_CLIENT_ID"),
        env_var("DATABRICKS_TEST_CLIENT_SECRET"),
    ) {
        AuthConfig::OAuthM2M {
            client_id,
            client_secret,
            scopes: None,
            token_url_override: None,
        }
    } else {
        let token = env_var("DATABRICKS_TOKEN")
            .expect("need DATABRICKS_TEST_CLIENT_ID/SECRET (OAuth M2M) or DATABRICKS_TOKEN (PAT)");
        AuthConfig::Pat { token }
    }
}

fn host_and_path() -> (String, String) {
    (
        env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST required"),
        env_var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required"),
    )
}

/// Read the mitmproxy CA cert PEM bytes from `MITMPROXY_CA_CERT`.
fn mitmproxy_ca_pem() -> Vec<u8> {
    let path = env_var("MITMPROXY_CA_CERT")
        .expect("MITMPROXY_CA_CERT must point at the extracted mitmproxy CA cert");
    std::fs::read(&path).unwrap_or_else(|e| panic!("read MITMPROXY_CA_CERT ({path}): {e}"))
}

/// Open a session with the given TLS config against the live
/// workspace (traffic routed through `HTTPS_PROXY`).
async fn open_with_tls(tls: TlsConfig) -> databricks_sql_kernel::Result<Session> {
    let (host, path) = host_and_path();
    Session::builder()
        .http_path(host, path)
        .auth(live_auth())
        .tls(tls)
        .open()
        .await
}

/// Drain a `SELECT 1` and assert it returned exactly one row equal to 1.
async fn assert_select_one(session: &Session) {
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let mut exec = stmt
        .execute()
        .await
        .expect("execute SELECT 1 through proxy");
    let stream: &mut ResultStream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().expect("one batch");
    let row = stream.next_row_in_batch().expect("one row");
    assert_eq!(row.get_i64(0).unwrap(), Some(1), "SELECT 1 should return 1");
    exec.close().await.expect("close");
}

/// Without any TLS relaxation, the mitmproxy-re-signed certificate is
/// untrusted and the handshake must fail. This is the guard that
/// proves the positive tests below are actually validating the chain
/// (and not, say, silently bypassing the proxy).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn tls_fails_without_config_behind_intercepting_proxy() {
    // Default TLS = strict. The proxy's CA is not in the system trust
    // store, so `Session::open` (which makes authenticated calls) must
    // error out rather than complete the handshake.
    let result = open_with_tls(TlsConfig::default()).await;
    let session = match result {
        Err(_) => return, // expected: handshake/connect failure
        Ok(session) => session,
    };
    // Some auth flows defer the first network call until execute, so
    // also exercise a query before concluding the connection "worked".
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let exec = stmt.execute().await;
    assert!(
        exec.is_err(),
        "expected an untrusted-certificate failure behind mitmproxy with default TLS, \
         but the query succeeded — is HTTPS_PROXY actually pointing at mitmproxy?"
    );
}

/// Adding mitmproxy's CA via `custom_ca_cert` makes the re-signed
/// certificate trusted, so the full round-trip succeeds. This is the
/// core positive path: it proves `TlsConfig::custom_ca_cert` is wired
/// all the way through to the reqwest handshake.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn tls_with_trusted_custom_ca_succeeds() {
    let mut tls = TlsConfig::default();
    tls.custom_ca_cert = Some(mitmproxy_ca_pem());
    let session = open_with_tls(tls)
        .await
        .expect("Session::open with mitmproxy CA trusted");
    assert_select_one(&session).await;
    session.close().await.expect("session close");
}

/// `accept_self_signed` disables chain validation entirely, so the
/// round-trip succeeds without supplying the CA. Confirms the
/// dev-only escape hatch is wired through.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn tls_with_accept_self_signed_succeeds() {
    let mut tls = TlsConfig::default();
    tls.accept_self_signed = true;
    let session = open_with_tls(tls)
        .await
        .expect("Session::open with accept_self_signed");
    assert_select_one(&session).await;
    session.close().await.expect("session close");
}
