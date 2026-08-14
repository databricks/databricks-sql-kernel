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

//! Direct-API (non-C-ABI) coverage for the shared in-flight-id slot clear that
//! `Statement::{execute, submit, execute_direct}` perform at dispatch entry
//! (`reset_inflight_slot`, src/statement/mutable.rs).
//!
//! The C-ABI wiremock suite only drives `execute` (async `submit` is not exposed
//! over the v0 C ABI — `kernel_statement_submit` is a stub), so the clear added
//! to `submit()` and `execute_direct()` had no test: deleting it from either
//! failed nothing. These tests exercise those two paths through the direct Rust
//! API with a `Statement::canceller()`-derived canceller (which reads the shared
//! `inflight_id` slot — unlike `AsyncStatementCanceller`, which captures its own
//! id at construction and never touches the slot).
//!
//! Discriminator (deterministic, single-threaded): a first dispatch publishes
//! its id into the slot; a second dispatch that FAILS never republishes. With
//! `reset_inflight_slot` in place the second dispatch clears the slot at entry
//! (before its first await), so after the failure the canceller observes `None`.
//! Without the clear the slot still holds the prior id, and a retained canceller
//! would fire a cancel against the already-finished prior query. The submit /
//! execute_direct POST is `RequestType::ExecuteStatement` (non-idempotent), so a
//! 400 on the second call fails fast with no retry backoff.

use databricks_sql_kernel::{AuthConfig, ConnectionConfig, ExecutedStatementHandle, Session};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

// ─── Helpers (mirror tests/v0_async_execute_wiremock.rs) ─────────────

async fn open_session(server: &MockServer) -> Session {
    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .open()
        .await
        .expect("Session::open should succeed")
}

async fn mount_create_session(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-test","warehouse_id":"test_wh"}"#),
        )
        .mount(server)
        .await;
}

/// Catch-all DELETE so teardown's fire-and-forget deletes don't 404 in the log.
async fn mount_close_any(server: &MockServer) {
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(server)
        .await;
}

fn running_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "RUNNING" },
    })
    .to_string()
}

/// Statement POST responder: 200 + `first_body` on the initial call, then 400
/// (non-retryable for the non-idempotent ExecuteStatement) on every later call —
/// so the second dispatch fails fast without republishing an id into the slot.
struct FirstOkThenBadRequest {
    first_body: String,
    calls: AtomicUsize,
}

impl Respond for FirstOkThenBadRequest {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200).set_body_string(self.first_body.clone())
        } else {
            ResponseTemplate::new(400).set_body_string("bad request")
        }
    }
}

#[tokio::test]
async fn submit_clears_inflight_slot_at_dispatch_entry() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_close_any(&server).await;
    // First submit → RUNNING(stmt-A); second submit → 400 (fails, publishes nothing).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(FirstOkThenBadRequest {
            first_body: running_body("stmt-A"),
            calls: AtomicUsize::new(0),
        })
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");

    // Canceller derived from the Statement shares its inflight_id slot.
    let canceller = stmt.canceller();

    // First submit publishes stmt-A into the shared slot.
    let async_exec = stmt.submit().await.expect("first submit should succeed");
    assert_eq!(async_exec.statement_id(), "stmt-A");
    assert_eq!(
        canceller.statement_id().as_deref(),
        Some("stmt-A"),
        "the first submit must publish its id into the shared slot"
    );

    // Second submit fails. reset_inflight_slot ran at its entry (before the
    // await), so the slot was cleared and — the submit having failed — never
    // republished. A retained canceller therefore sees an empty slot, not the
    // finished stmt-A. Without the clear, the slot would still hold stmt-A here.
    let err = stmt.submit().await;
    assert!(err.is_err(), "second submit should fail on the 400");
    assert_eq!(
        canceller.statement_id(),
        None,
        "submit() must clear the shared inflight-id slot at dispatch entry, so a \
         retained canceller never targets the prior finished query"
    );
}

#[tokio::test]
async fn execute_direct_clears_inflight_slot_at_dispatch_entry() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_close_any(&server).await;
    // First execute_direct → RUNNING(stmt-A) (a Running handle, no polling);
    // second → 400 (fails, publishes nothing).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(FirstOkThenBadRequest {
            first_body: running_body("stmt-A"),
            calls: AtomicUsize::new(0),
        })
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");

    let canceller = stmt.canceller();

    // First execute_direct publishes stmt-A into the shared slot.
    let _direct = stmt
        .execute_direct()
        .await
        .expect("first execute_direct should succeed");
    assert_eq!(
        canceller.statement_id().as_deref(),
        Some("stmt-A"),
        "the first execute_direct must publish its id into the shared slot"
    );

    // Second execute_direct fails; reset_inflight_slot cleared the slot at entry
    // and nothing republished it.
    let err = stmt.execute_direct().await;
    assert!(err.is_err(), "second execute_direct should fail on the 400");
    assert_eq!(
        canceller.statement_id(),
        None,
        "execute_direct() must clear the shared inflight-id slot at dispatch entry, \
         so a retained canceller never targets the prior finished query"
    );
}
