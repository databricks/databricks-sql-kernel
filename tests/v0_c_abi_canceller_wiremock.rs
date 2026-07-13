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

//! Wiremock tests for the C ABI's detached statement canceller
//! (`kernel_statement_canceller_new` / `_cancel` / `_free`). The null-guard
//! unit test in `src/c_abi/statement.rs` only pins the bad-pointer paths; these
//! tests drive the entry points against a mock SEA server to cover the behavior
//! that actually matters:
//!
//! - `canceller_cancels_a_running_execute_from_another_thread`: the headline
//!   contract — one thread blocks in `kernel_statement_execute` while another
//!   fires `kernel_statement_canceller_cancel`. The status poll stays RUNNING
//!   until the cancel POST lands, so the execute unwinds because of the cancel.
//! - `cancel_before_execute_is_a_noop_success`: `_cancel` with no observed id
//!   returns `Success` with `dispatched == false`, and is idempotent.
//! - `cancel_rpc_failure_returns_error_and_sets_dispatched_false`: a failing
//!   cancel RPC surfaces a non-Success status with `dispatched` written `false`.
//! - `cancel_after_terminal_dispatches_and_reports_true`: a cancel with an
//!   observed id dispatches and reports `dispatched == true`, even post-terminal.
//! - `canceller_retargets_the_current_execution_on_reuse`: a canceller kept
//!   across a re-execute cancels the second query, not the finished first.
//! - `canceller_outlives_statement_and_session_close`: a canceller stays usable
//!   after its statement AND session are closed — the cancel still dispatches
//!   (`dispatched == true`) and its POST reaches the server, proving the cloned
//!   session `Arc` keeps the client-side session alive past `kernel_session_close`.
//!
//! The C ABI drives its own process-wide tokio runtime inside `block_on_ffi`,
//! so the entry points must be called from plain (non-async) threads — hence
//! `#[test]` + `std::thread::spawn`, never `#[tokio::test]` / `tokio::spawn`
//! (a runtime worker trips `block_on_ffi`'s reentrancy guard). A dedicated
//! runtime hosts the wiremock server and its request polling only.

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_new_statement, kernel_session_open,
    kernel_session_t, KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::statement::{
    kernel_executed_statement_close, kernel_executed_statement_t,
    kernel_statement_canceller_cancel, kernel_statement_canceller_free,
    kernel_statement_canceller_new, kernel_statement_canceller_t, kernel_statement_close,
    kernel_statement_execute, kernel_statement_set_sql, kernel_statement_t,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

// ─── Mock helpers (mirrors tests/v0_execute_wiremock.rs) ─────────────

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

/// Catch-all DELETE so the session/statement teardown's fire-and-forget
/// deletes don't 404 in the log.
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

fn canceled_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "CANCELED" },
    })
    .to_string()
}

/// A terminal SUCCEEDED body with an empty result (`total_chunk_count: 0`), which
/// the reader materialises as an empty result set from the manifest schema — no
/// Arrow attachment needed. Used where a query must finish successfully (so the
/// mid-execute cancel guard defuses and fires no cleanup cancel).
fn succeeded_empty_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": { "column_count": 1, "columns": [
                { "name": "n", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0 }
            ] },
            "total_chunk_count": 0,
            "total_row_count": 0,
            "total_byte_count": 0,
            "truncated": false,
        },
    })
    .to_string()
}

/// GET-status responder that reports `RUNNING` until a cancel is observed, then
/// `CANCELED` — so the blocking execute unwinds *because* the cancel landed, not
/// on a fixed timer. `cancelled` is shared with the cancel POST mock, which sets
/// it when the cancel RPC arrives. A poll cap is a safety valve: if a broken
/// cancel never fires, the responder still flips to `CANCELED` after `max_running`
/// polls so the test fails its causal assertion instead of hanging on the
/// kernel's 600s poll timeout.
struct CancelAwareStatus {
    statement_id: String,
    cancelled: Arc<AtomicBool>,
    polls: AtomicUsize,
    max_running: usize,
}

impl Respond for CancelAwareStatus {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let n = self.polls.fetch_add(1, Ordering::SeqCst);
        let done = self.cancelled.load(Ordering::SeqCst) || n >= self.max_running;
        let body = if done {
            canceled_body(&self.statement_id)
        } else {
            running_body(&self.statement_id)
        };
        ResponseTemplate::new(200).set_body_string(body)
    }
}

/// Cancel POST responder that records arrival by setting a shared flag.
struct RecordingCancel {
    cancelled: Arc<AtomicBool>,
}

impl Respond for RecordingCancel {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        self.cancelled.store(true, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_string("{}")
    }
}

/// Execute POST responder that returns `first` on the initial call and `rest`
/// on every later call — models two executes on one statement issuing distinct
/// server statement ids. Sets `second_started` when the second POST arrives, so
/// a cancelling thread can wait until the second query's id will be the one
/// published before it fires (avoiding a race with the first query's stale id).
struct SequencedExecute {
    first: String,
    rest: String,
    calls: AtomicUsize,
    second_started: Arc<AtomicBool>,
}

impl Respond for SequencedExecute {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let body = if n == 0 {
            &self.first
        } else {
            self.second_started.store(true, Ordering::SeqCst);
            &self.rest
        };
        ResponseTemplate::new(200).set_body_string(body.clone())
    }
}

/// Execute POST responder that returns `first` immediately, then on the second
/// call flags `second_entered` (the moment the second dispatch's POST reaches
/// the server — the transport has NOT yet received the body, so the new id is
/// NOT yet republished into the shared slot) and holds the response open for
/// `hold` before returning `rest`. That open window is where a concurrent
/// cancel observes the slot: with `reset_inflight_slot` in place the slot reads
/// `None` (the second dispatch cleared it at entry, before its first await);
/// without the fix it still holds the prior query's id. This is the
/// discriminating pre-observer window that a POST-arrival gate steps over.
struct HeldSecondExecute {
    first: String,
    rest: String,
    hold: Duration,
    calls: AtomicUsize,
    second_entered: Arc<AtomicBool>,
}

impl Respond for HeldSecondExecute {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200).set_body_string(self.first.clone())
        } else {
            self.second_entered.store(true, Ordering::SeqCst);
            // Hold the response so the cancel thread has a wide, deterministic
            // window to read the slot before this POST returns and the transport
            // republishes the new id.
            ResponseTemplate::new(200)
                .set_body_string(self.rest.clone())
                .set_delay(self.hold)
        }
    }
}

/// Build a config and open a session over the C ABI, returning the raw handle.
/// Mirrors the minting sequence in tests/v0_c_abi_session_close_wiremock.rs.
///
/// # Safety
/// Caller owns the returned handle and must `kernel_session_close` it.
unsafe fn open_session_c_abi(uri: &str) -> *mut kernel_session_t {
    let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
    assert_eq!(
        kernel_session_config_new(&mut config),
        KernelStatusCode::Success
    );
    let host = CString::new(uri).unwrap();
    let http_path = CString::new("/sql/1.0/warehouses/test_wh").unwrap();
    assert_eq!(
        kernel_session_config_set_http_path(config, host.as_ptr(), http_path.as_ptr()),
        KernelStatusCode::Success
    );
    let token = CString::new("dapi_test_token").unwrap();
    assert_eq!(
        kernel_session_config_set_auth_pat(config, token.as_ptr()),
        KernelStatusCode::Success
    );
    let mut session: *mut kernel_session_t = std::ptr::null_mut();
    assert_eq!(
        kernel_session_open(config, &mut session),
        KernelStatusCode::Success
    );
    assert!(!session.is_null());
    session
}

/// A `*mut` handle we hand to another thread. Raw pointers are `!Send`; the C
/// ABI contract is that the handle may cross threads (that is the whole point
/// of a detached canceller), so we assert `Send` explicitly for the move.
/// Extract the pointer via `into_ptr(self)` — consuming the whole wrapper — so
/// edition-2021 disjoint closure capture moves the `Send` wrapper into the
/// spawned closure, not the bare `!Send` field.
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
impl<T> SendPtr<T> {
    fn into_ptr(self) -> *mut T {
        self.0
    }
}

#[test]
fn canceller_cancels_a_running_execute_from_another_thread() {
    // Shared with the mocks: the GET-status responder reports RUNNING until this
    // flips, so execute keeps polling until the cancel POST actually arrives.
    let cancelled = Arc::new(AtomicBool::new(false));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        // POST returns RUNNING carrying the id (published to the canceller's
        // slot before the poll loop).
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_string(running_body("stmt-cancel")))
            .mount(&server)
            .await;
        // GET stays RUNNING until the cancel lands, then reports CANCELED — so
        // execute unwinds because of the cancel, not on a fixed schedule.
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-cancel"))
            .respond_with(CancelAwareStatus {
                statement_id: "stmt-cancel".to_string(),
                cancelled: Arc::clone(&cancelled),
                polls: AtomicUsize::new(0),
                max_running: 100,
            })
            .mount(&server)
            .await;
        // The cancel RPC thread B fires; arrival flips the shared flag.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-cancel/cancel"))
            .respond_with(RecordingCancel {
                cancelled: Arc::clone(&cancelled),
            })
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    let session;
    let stmt;
    let canceller;
    unsafe {
        session = open_session_c_abi(&uri);
        let mut s: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut s),
            KernelStatusCode::Success
        );
        assert!(!s.is_null());
        stmt = s;

        let sql = CString::new("SELECT 1").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );

        // Detached canceller obtained BEFORE execute — the whole point of the API.
        let mut c: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_canceller_new(stmt, &mut c),
            KernelStatusCode::Success
        );
        assert!(!c.is_null());
        canceller = c;
    }

    // Thread B: fire cancel from another (plain OS) thread while thread A is
    // blocked in execute. Loop until `dispatched` flips true — before the POST
    // publishes the id, cancel is a tolerated no-op (Success, dispatched=false).
    let canceller_send = SendPtr(canceller);
    let dispatched_seen = Arc::new(AtomicBool::new(false));
    let dispatched_flag = Arc::clone(&dispatched_seen);
    let canceller_thread = std::thread::spawn(move || {
        let canceller = canceller_send.into_ptr();
        for _ in 0..200 {
            let mut dispatched = false;
            let rc = unsafe { kernel_statement_canceller_cancel(canceller, &mut dispatched) };
            assert_eq!(rc, KernelStatusCode::Success);
            if dispatched {
                dispatched_flag.store(true, Ordering::SeqCst);
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    // Thread A: blocking execute. Returns Err once the GET poll sees CANCELED.
    let stmt_send = SendPtr(stmt);
    let execute_thread = std::thread::spawn(move || {
        let stmt = stmt_send.into_ptr();
        let mut exec: *mut kernel_executed_statement_t = std::ptr::null_mut();
        unsafe { kernel_statement_execute(stmt, &mut exec) }
    });

    let execute_rc = execute_thread.join().expect("execute thread panicked");
    canceller_thread.join().expect("canceller thread panicked");

    // The cancel actually dispatched an RPC (not a silent no-op), and the GET
    // status only turned CANCELED because that cancel landed (it reports RUNNING
    // until then). These two carry the causal claim; assert them before the
    // execute outcome so a broken cancel fails here rather than on the timer.
    assert!(
        dispatched_seen.load(Ordering::SeqCst),
        "the cancel should have dispatched an RPC once the statement id was observed"
    );
    assert!(
        cancelled.load(Ordering::SeqCst),
        "the cancel POST should have reached the server, driving the CANCELED status"
    );
    // Execute unwinds non-Success once its poll sees the cancel-driven CANCELED
    // (the sync path maps CANCELED to Cancelled/InvalidArgument).
    assert_ne!(
        execute_rc,
        KernelStatusCode::Success,
        "execute should fail once the cancel takes effect, not return a result"
    );

    unsafe {
        assert_eq!(
            kernel_statement_canceller_free(canceller),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }

    // The cancel POST must have reached the wire against the in-flight id.
    let saw_cancel = rt.block_on(async {
        for _ in 0..50 {
            let requests = server.received_requests().await.unwrap_or_default();
            if requests.iter().any(|r| {
                r.method.as_str() == "POST"
                    && r.url.path().ends_with("/statements/stmt-cancel/cancel")
            }) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    });
    assert!(
        saw_cancel,
        "kernel_statement_canceller_cancel must POST a cancel for the in-flight statement id"
    );
}

#[test]
fn cancel_before_execute_is_a_noop_success() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        server
    });
    let uri = server.uri();

    unsafe {
        let session = open_session_c_abi(&uri);
        let mut stmt: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut stmt),
            KernelStatusCode::Success
        );

        // A null `out` is rejected rather than written through.
        assert_eq!(
            kernel_statement_canceller_new(stmt, std::ptr::null_mut()),
            KernelStatusCode::InvalidArgument
        );

        let mut canceller: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_canceller_new(stmt, &mut canceller),
            KernelStatusCode::Success
        );

        // No execute has run, so no statement id has been observed: cancel is a
        // no-op that still returns Success with dispatched == false.
        let mut dispatched = true;
        assert_eq!(
            kernel_statement_canceller_cancel(canceller, &mut dispatched),
            KernelStatusCode::Success
        );
        assert!(!dispatched, "no id observed yet → cancel must be a no-op");

        // Idempotent: a second call behaves identically.
        let mut dispatched2 = true;
        assert_eq!(
            kernel_statement_canceller_cancel(canceller, &mut dispatched2),
            KernelStatusCode::Success
        );
        assert!(
            !dispatched2,
            "repeated no-op cancel must stay a no-op Success"
        );

        // A NULL dispatched out-param is tolerated.
        assert_eq!(
            kernel_statement_canceller_cancel(canceller, std::ptr::null_mut()),
            KernelStatusCode::Success
        );

        assert_eq!(
            kernel_statement_canceller_free(canceller),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}

#[test]
fn cancel_rpc_failure_returns_error_and_sets_dispatched_false() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        // Execute runs to a terminal CANCELED state, which leaves the observed
        // statement id in the shared slot (it is cleared only at the next
        // execute's start). A subsequent cancel therefore reads the id and
        // dispatches — but the cancel endpoint fails, so the RPC errors.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_string(running_body("stmt-err")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-err"))
            .respond_with(ResponseTemplate::new(200).set_body_string(canceled_body("stmt-err")))
            .mount(&server)
            .await;
        // 404 is non-retryable for an idempotent cancel, so the RPC fails after
        // one round-trip rather than exhausting the retry ladder (a 5xx would add
        // ~60s of backoff to this test).
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-err/cancel"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    unsafe {
        let session = open_session_c_abi(&uri);
        let mut stmt: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut stmt),
            KernelStatusCode::Success
        );
        let sql = CString::new("SELECT 1").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );
        let mut canceller: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_canceller_new(stmt, &mut canceller),
            KernelStatusCode::Success
        );

        // Run execute to its terminal state (returns non-Success for CANCELED),
        // which publishes and leaves the statement id in the shared slot.
        let mut exec: *mut kernel_executed_statement_t = std::ptr::null_mut();
        let exec_rc = kernel_statement_execute(stmt, &mut exec);
        assert_ne!(exec_rc, KernelStatusCode::Success);

        // Now cancel: an id is observed, so the RPC dispatches — and fails (404).
        // A pre-seeded `true` sentinel confirms the error path still defined the
        // out-param (to false), per the always-write contract.
        let mut dispatched = true;
        let rc = kernel_statement_canceller_cancel(canceller, &mut dispatched);
        assert_ne!(
            rc,
            KernelStatusCode::Success,
            "a failing cancel RPC must surface a non-Success status"
        );
        assert!(
            !dispatched,
            "on the error path, *dispatched is written false (never left undefined)"
        );

        assert_eq!(
            kernel_statement_canceller_free(canceller),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}

#[test]
fn cancel_after_terminal_dispatches_and_reports_true() {
    // Once a statement id has been observed it stays in the shared slot until the
    // next execute. A cancel after the statement is already terminal therefore
    // still dispatches an RPC against that id (the server treats it as its own
    // no-op) and reports dispatched == true — distinct from the never-executed
    // no-op, which reports false.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_string(running_body("stmt-term")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-term"))
            .respond_with(ResponseTemplate::new(200).set_body_string(canceled_body("stmt-term")))
            .mount(&server)
            .await;
        // The cancel succeeds here (unlike the failure test's 404).
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-term/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    unsafe {
        let session = open_session_c_abi(&uri);
        let mut stmt: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut stmt),
            KernelStatusCode::Success
        );
        let sql = CString::new("SELECT 1").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );
        let mut canceller: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_canceller_new(stmt, &mut canceller),
            KernelStatusCode::Success
        );

        // Drive execute to its terminal state, leaving the id in the shared slot.
        let mut exec: *mut kernel_executed_statement_t = std::ptr::null_mut();
        assert_ne!(
            kernel_statement_execute(stmt, &mut exec),
            KernelStatusCode::Success
        );

        // Cancel after terminal: id still observed → RPC dispatched, dispatched == true.
        let mut dispatched = false;
        assert_eq!(
            kernel_statement_canceller_cancel(canceller, &mut dispatched),
            KernelStatusCode::Success
        );
        assert!(
            dispatched,
            "a cancel with an observed id dispatches and reports true, even post-terminal"
        );

        assert_eq!(
            kernel_statement_canceller_free(canceller),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}

#[test]
fn canceller_retargets_the_current_execution_on_reuse() {
    // A canceller shares the statement's in-flight-id slot, which is cleared and
    // republished on each execute. A canceller kept across a re-execute therefore
    // cancels the SECOND query. This drives two executes on one statement and
    // asserts the cancel targets the second statement id, not the first.
    let cancelled = Arc::new(AtomicBool::new(false));
    let second_started = Arc::new(AtomicBool::new(false));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        // Two executes on one statement issue distinct ids: "stmt-A" then
        // "stmt-B". The first runs to SUCCEEDED so its mid-execute cancel guard
        // defuses (no cleanup cancel fires against stmt-A); the point is that the
        // second execute republishes the shared id slot to "stmt-B".
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(SequencedExecute {
                first: running_body("stmt-A"),
                rest: running_body("stmt-B"),
                calls: AtomicUsize::new(0),
                second_started: Arc::clone(&second_started),
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-A"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(succeeded_empty_body("stmt-A")),
            )
            .mount(&server)
            .await;
        // Second execute's status stays RUNNING until its cancel lands.
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-B"))
            .respond_with(CancelAwareStatus {
                statement_id: "stmt-B".to_string(),
                cancelled: Arc::clone(&cancelled),
                polls: AtomicUsize::new(0),
                max_running: 100,
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-B/cancel"))
            .respond_with(RecordingCancel {
                cancelled: Arc::clone(&cancelled),
            })
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    let session;
    let stmt;
    unsafe {
        session = open_session_c_abi(&uri);
        let mut s: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut s),
            KernelStatusCode::Success
        );
        stmt = s;
        let sql = CString::new("SELECT 1").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );

        // Obtain the canceller before the first execute, then keep it across the
        // second. The second execute's start clears the shared id slot to None
        // (never back to stmt-A), so a cancel fired during the second execute
        // can only ever target None (no-op) or stmt-B — never the finished first.
        let mut canceller: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_canceller_new(stmt, &mut canceller),
            KernelStatusCode::Success
        );
        // First execute succeeds, so its cancel guard defuses — nothing hits
        // stmt-A/cancel.
        let mut exec1: *mut kernel_executed_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_execute(stmt, &mut exec1),
            KernelStatusCode::Success
        );
        assert_eq!(
            kernel_executed_statement_close(exec1),
            KernelStatusCode::Success
        );

        // Second execute on the same statement, cancelled from another thread.
        let stmt_send = SendPtr(stmt);
        let execute_thread = std::thread::spawn(move || {
            let stmt = stmt_send.into_ptr();
            let mut exec2: *mut kernel_executed_statement_t = std::ptr::null_mut();
            kernel_statement_execute(stmt, &mut exec2)
        });

        let canceller_send = SendPtr(canceller);
        let second_started_c = Arc::clone(&second_started);
        let canceller_thread = std::thread::spawn(move || {
            let canceller = canceller_send.into_ptr();
            // Wait until the second execute's POST has been issued. By then its
            // start has already cleared the shared id slot (to None), so the
            // slot can only resolve to stmt-B or None — never the stale stmt-A.
            while !second_started_c.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
            for _ in 0..200 {
                let mut dispatched = false;
                let rc = kernel_statement_canceller_cancel(canceller, &mut dispatched);
                assert_eq!(rc, KernelStatusCode::Success);
                if dispatched {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let exec2_rc = execute_thread.join().expect("execute thread panicked");
        canceller_thread.join().expect("canceller thread panicked");
        assert_ne!(
            exec2_rc,
            KernelStatusCode::Success,
            "the second execute should be the one cancelled"
        );

        assert_eq!(
            kernel_statement_canceller_free(canceller),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }

    // The cancel targeted the SECOND statement id, and no cancel ever hit the
    // first — the retained canceller followed the statement to its new query.
    let (saw_b, saw_a) = rt.block_on(async {
        let requests = server.received_requests().await.unwrap_or_default();
        let saw_b = requests.iter().any(|r| {
            r.method.as_str() == "POST" && r.url.path().ends_with("/statements/stmt-B/cancel")
        });
        let saw_a = requests.iter().any(|r| {
            r.method.as_str() == "POST" && r.url.path().ends_with("/statements/stmt-A/cancel")
        });
        (saw_b, saw_a)
    });
    assert!(
        saw_b,
        "cancel must target the second (current) statement id"
    );
    assert!(
        !saw_a,
        "cancel must not target the first (finished) statement id"
    );
}

#[test]
fn canceller_outlives_statement_and_session_close() {
    // The header's motivating claim for the detached canceller is that it holds
    // its OWN session reference (a cloned `Arc<SessionInner>`) and its own clone
    // of the statement's in-flight-id slot, so it stays usable — and keeps the
    // client-side session alive — even after the statement and session it came
    // from are closed. Every other test frees the canceller first, so none
    // exercises this cross-lifetime contract; a refactor that made `_new` borrow
    // (rather than clone) the session, or that cleared the id slot on statement
    // close, would regress this silently while all those tests still passed.
    //
    // Here: execute to a terminal id, then close the executed handle, the
    // statement, AND the session, and only THEN cancel. The cancel must still
    // succeed with dispatched == true and its POST must actually reach the
    // server — proving the cloned Arc kept the client-side session (transport +
    // connection pool) alive past `kernel_session_close`. Free the canceller
    // last, as the header's teardown note allows.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        // POST returns RUNNING carrying the id; GET reports SUCCEEDED so the
        // query finishes cleanly (its mid-execute cancel guard defuses — no
        // cleanup cancel fires) and the id "stmt-outlive" stays in the slot.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_string(running_body("stmt-outlive")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-outlive"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(succeeded_empty_body("stmt-outlive")),
            )
            .mount(&server)
            .await;
        // The post-close cancel POST. If the cloned session Arc did NOT keep the
        // client alive, this endpoint would never be hit.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-outlive/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    unsafe {
        let session = open_session_c_abi(&uri);
        let mut stmt: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut stmt),
            KernelStatusCode::Success
        );
        let sql = CString::new("SELECT 1").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );

        // Canceller obtained before execute (the required order); kept past all
        // three closes below.
        let mut canceller: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_canceller_new(stmt, &mut canceller),
            KernelStatusCode::Success
        );

        // Execute to a terminal SUCCEEDED, leaving "stmt-outlive" in the slot.
        let mut exec: *mut kernel_executed_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_execute(stmt, &mut exec),
            KernelStatusCode::Success
        );

        // Tear down everything the canceller came from BEFORE cancelling: the
        // executed handle, the statement, then the session.
        assert_eq!(
            kernel_executed_statement_close(exec),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);

        // The canceller still holds the observed id and a live session clone, so
        // the cancel dispatches (server treats a cancel of a finished statement
        // as its own no-op) and reports true — proving the client-side session
        // survived `kernel_session_close`.
        let mut dispatched = false;
        assert_eq!(
            kernel_statement_canceller_cancel(canceller, &mut dispatched),
            KernelStatusCode::Success
        );
        assert!(
            dispatched,
            "a canceller must stay usable after statement/session close: its cloned \
             session Arc keeps the client alive and its id slot still holds the observed id"
        );

        assert_eq!(
            kernel_statement_canceller_free(canceller),
            KernelStatusCode::Success
        );
    }

    // The cancel POST actually reached the server — the strongest proof the
    // cloned session Arc kept the transport alive past session close.
    let saw_cancel = rt.block_on(async {
        let requests = server.received_requests().await.unwrap_or_default();
        requests.iter().any(|r| {
            r.method.as_str() == "POST" && r.url.path().ends_with("/statements/stmt-outlive/cancel")
        })
    });
    assert!(
        saw_cancel,
        "the post-close cancel POST must reach the server, proving the canceller's \
         cloned session reference kept the client-side session alive"
    );
}

#[test]
fn retained_canceller_noops_in_the_reexecute_handoff_window() {
    // Guards the headline `reset_inflight_slot()` fix (src/statement/mutable.rs).
    // The prior `canceller_retargets_the_current_execution_on_reuse` test gates
    // its cancel on the second POST *arriving*, which is past the point where the
    // new id is about to be published — so the stale-prior-id window is already
    // closed and that test passes with OR without the fix.
    //
    // This test sits deterministically INSIDE that window: `HeldSecondExecute`
    // flags `second_entered` the instant the second execute's POST reaches the
    // server, then holds the response open. During that hold the second
    // dispatch has already cleared the slot at entry (the fix) but has not yet
    // republished its id, so a cancel fired here must observe an EMPTY slot:
    // `dispatched == false` and no POST against the prior (finished) id. Remove
    // `reset_inflight_slot()` and the slot still holds `stmt-A` in this window —
    // both assertions flip — so this test fails, which the retarget test does not.
    let second_entered = Arc::new(AtomicBool::new(false));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        // First execute → stmt-A immediately; second → stmt-B held open 2s.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(HeldSecondExecute {
                first: running_body("stmt-A"),
                rest: running_body("stmt-B"),
                hold: Duration::from_secs(2),
                calls: AtomicUsize::new(0),
                second_entered: Arc::clone(&second_entered),
            })
            .mount(&server)
            .await;
        // Both executes run to SUCCEEDED so their mid-execute cancel guards
        // defuse — the only cancel that can reach stmt-A/cancel is a buggy stale
        // dispatch from our own cancel call.
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-A"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(succeeded_empty_body("stmt-A")),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-B"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(succeeded_empty_body("stmt-B")),
            )
            .mount(&server)
            .await;
        // Accept a cancel POST on either id so a stale (buggy) dispatch does not
        // 404-hang; received_requests then tells us whether stmt-A/cancel was hit.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-A/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-B/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    let session;
    let stmt;
    let dispatched_in_window = Arc::new(AtomicBool::new(false));
    unsafe {
        session = open_session_c_abi(&uri);
        let mut s: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut s),
            KernelStatusCode::Success
        );
        stmt = s;
        let sql = CString::new("SELECT 1").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );
        let mut canceller: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_canceller_new(stmt, &mut canceller),
            KernelStatusCode::Success
        );

        // First execute runs to terminal, publishing stmt-A into the shared slot.
        let mut exec1: *mut kernel_executed_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_execute(stmt, &mut exec1),
            KernelStatusCode::Success
        );
        assert_eq!(
            kernel_executed_statement_close(exec1),
            KernelStatusCode::Success
        );

        // Second execute (its POST is held open by the mock) on another thread.
        let stmt_send = SendPtr(stmt);
        let execute_thread = std::thread::spawn(move || {
            let stmt = stmt_send.into_ptr();
            let mut exec2: *mut kernel_executed_statement_t = std::ptr::null_mut();
            kernel_statement_execute(stmt, &mut exec2)
        });

        // Cancel thread: fire exactly once inside the held window.
        let canceller_send = SendPtr(canceller);
        let second_entered_c = Arc::clone(&second_entered);
        let dispatched_flag = Arc::clone(&dispatched_in_window);
        let cancel_thread = std::thread::spawn(move || {
            let canceller = canceller_send.into_ptr();
            // Wait until the second dispatch's POST has entered the server (its
            // entry already cleared the slot); the response is now held, so the
            // new id is not yet republished — this is the discriminating window.
            while !second_entered_c.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            let mut dispatched = false;
            let rc = kernel_statement_canceller_cancel(canceller, &mut dispatched);
            assert_eq!(rc, KernelStatusCode::Success);
            dispatched_flag.store(dispatched, Ordering::SeqCst);
        });

        let exec2_rc = execute_thread.join().expect("execute thread panicked");
        cancel_thread.join().expect("cancel thread panicked");
        assert_eq!(
            exec2_rc,
            KernelStatusCode::Success,
            "the second execute should still complete normally"
        );

        assert_eq!(
            kernel_statement_canceller_free(canceller),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }

    // The cancel fired in the handoff window saw an empty slot — the second
    // dispatch cleared it at entry before its first await.
    assert!(
        !dispatched_in_window.load(Ordering::SeqCst),
        "a cancel in the re-execute handoff must no-op (dispatched=false): the slot \
         is cleared at dispatch entry, so it never targets the prior finished id"
    );
    // And the strong post-hoc proof: no cancel was ever POSTed against stmt-A.
    let saw_stmt_a_cancel = rt.block_on(async {
        let requests = server.received_requests().await.unwrap_or_default();
        requests.iter().any(|r| {
            r.method.as_str() == "POST" && r.url.path().ends_with("/statements/stmt-A/cancel")
        })
    });
    assert!(
        !saw_stmt_a_cancel,
        "no cancel may be POSTed against the prior (finished) statement id during \
         the re-execute handoff"
    );
}
