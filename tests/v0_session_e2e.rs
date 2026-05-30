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

//! End-to-end tests for the v0 `Session::open` against a live
//! Databricks workspace.
//!
//! Marked `#[ignore]` so CI without credentials skips them. Run with
//! the connection variables set per CLAUDE.md:
//!
//! ```sh
//! set -a && source ~/.databricks/pecotesting-creds && set +a
//! cargo test --test v0_session_e2e -- --ignored
//! ```
//!
//! Three smoke tests cover the auth modes the plan calls for: PAT
//! (always run when `DATABRICKS_TOKEN` is set), OAuth M2M (requires
//! `DATABRICKS_TEST_CLIENT_ID` / `DATABRICKS_TEST_CLIENT_SECRET`),
//! and OAuth U2M (requires a human at the keyboard — gated on a
//! `DATABRICKS_RUN_U2M=1` env to avoid surprising automated runs).

use databricks_sql_kernel::{AuthConfig, Session};
use std::env;

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty())
}

fn require_host_and_path() -> (String, String) {
    let host =
        env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST must be set for v0 session E2E tests");
    let path = env_var("DATABRICKS_HTTP_PATH")
        .expect("DATABRICKS_HTTP_PATH must be set for v0 session E2E tests");
    (host, path)
}

#[tokio::test]
#[ignore]
async fn open_with_pat_against_live_warehouse() {
    let (host, http_path) = require_host_and_path();
    let token =
        env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN must be set for the PAT smoke test");

    let session = Session::builder()
        .http_path(host, http_path)
        .auth(AuthConfig::Pat { token })
        .open()
        .await
        .expect("Session::open should succeed against the live warehouse");

    assert!(
        !session.session_id().is_empty(),
        "session_id must be populated"
    );
    session
        .close()
        .await
        .expect("explicit close() should succeed");
}

#[cfg(feature = "oauth")]
#[tokio::test]
#[ignore]
async fn open_with_oauth_m2m_against_live_warehouse() {
    let (host, http_path) = require_host_and_path();
    let client_id = match env_var("DATABRICKS_TEST_CLIENT_ID") {
        Some(v) => v,
        None => {
            eprintln!("skipping M2M test — DATABRICKS_TEST_CLIENT_ID not set");
            return;
        }
    };
    let client_secret = env_var("DATABRICKS_TEST_CLIENT_SECRET")
        .expect("DATABRICKS_TEST_CLIENT_SECRET must accompany DATABRICKS_TEST_CLIENT_ID");

    let session = Session::builder()
        .http_path(host, http_path)
        .auth(AuthConfig::OAuthM2M {
            client_id,
            client_secret,
            scopes: None,
            token_url_override: None,
        })
        .open()
        .await
        .expect("Session::open with OAuthM2M should succeed");

    assert!(!session.session_id().is_empty());
    session.close().await.expect("close()");
}

#[cfg(feature = "oauth")]
#[tokio::test]
#[ignore]
async fn open_with_oauth_u2m_against_live_warehouse() {
    if env_var("DATABRICKS_RUN_U2M").is_none() {
        eprintln!("skipping U2M test — set DATABRICKS_RUN_U2M=1 to run (opens browser)");
        return;
    }
    let (host, http_path) = require_host_and_path();

    let session = Session::builder()
        .http_path(host, http_path)
        .auth(AuthConfig::OAuthU2M {
            client_id: None,
            redirect_port: None,
            scopes: None,
            token_url_override: None,
            callback_timeout: None,
        })
        .open()
        .await
        .expect("Session::open with OAuthU2M should succeed");

    assert!(!session.session_id().is_empty());
    session.close().await.expect("close()");
}

#[tokio::test]
#[ignore]
async fn open_drop_does_not_panic() {
    // Verify Drop fires the fire-and-forget delete_session path
    // without explicit close(). Run this after a regular open with
    // PAT auth and just let the Session drop.
    let (host, http_path) = require_host_and_path();
    let token = env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN");

    let session = Session::builder()
        .http_path(host, http_path)
        .auth(AuthConfig::Pat { token })
        .open()
        .await
        .expect("open");
    let _id = session.session_id().to_owned();
    drop(session);
    // If we reach here without panicking, the Drop path is sane.
    // The actual delete_session runs in the background; we don't
    // wait on it — it's fire-and-forget.
}
