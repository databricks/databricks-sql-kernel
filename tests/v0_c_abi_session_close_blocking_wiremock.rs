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

//! Wiremock test for `kernel_session_close_blocking`: unlike the
//! fire-and-forget `kernel_session_close` (whose DELETE the sibling test
//! has to *poll* for, since it runs on a detached task), the blocking
//! close must have completed the server-side `DeleteSession` **before it
//! returns** — so the DELETE is observable immediately with no polling
//! loop. That synchronous-completion property is exactly what a host wires
//! it up for (parity with the Thrift path's synchronous CloseSession RPC),
//! so it is what this test asserts.
//!
//! Same threading shape as the sibling: the C ABI drives its own
//! process-wide runtime inside `block_on_ffi`, so the entry points run on
//! a plain `#[test]` thread with a dedicated runtime only to host the mock.

use std::ffi::CString;
use std::thread;
use std::time::{Duration, Instant};

use databricks_sql_kernel::c_abi::cancel::{
    kernel_cancel_token_cancel, kernel_cancel_token_free, kernel_cancel_token_new,
    kernel_cancel_token_t,
};
use databricks_sql_kernel::c_abi::session::{
    kernel_session_close_blocking, kernel_session_close_blocking_cancellable,
    kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_open, kernel_session_t,
    KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn session_close_blocking_awaits_delete_before_returning() {
    // A dedicated runtime just to run the async wiremock setup / request
    // inspection; the C ABI calls themselves run on the kernel's own runtime.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"session_id":"sess-c-abi-close-blocking","warehouse_id":"test_wh"}"#,
            ))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-c-abi-close-blocking"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    unsafe {
        let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
        assert_eq!(
            kernel_session_config_new(&mut config),
            KernelStatusCode::Success
        );
        let host = CString::new(uri.as_str()).unwrap();
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

        // Blocking close: this must not return until the DELETE round-trip
        // is done.
        assert_eq!(
            kernel_session_close_blocking(session),
            KernelStatusCode::Success
        );
    }

    // No polling loop: the DELETE must ALREADY be on the wire the instant
    // the blocking close returned. A single synchronous check proves the
    // await happened (vs. the fire-and-forget path, which needs polling).
    let saw_delete = rt.block_on(async {
        let requests = server.received_requests().await.unwrap_or_default();
        requests.iter().any(|r| {
            r.method.as_str() == "DELETE" && r.url.path().ends_with("sess-c-abi-close-blocking")
        })
    });
    assert!(
        saw_delete,
        "kernel_session_close_blocking must complete DeleteSession before returning"
    );
}

/// A null handle is rejected with `InvalidArgument` and a stored last
/// error, never a crash — matching `kernel_session_close`'s null guard.
#[test]
fn session_close_blocking_null_handle_rejected() {
    unsafe {
        assert_eq!(
            kernel_session_close_blocking(std::ptr::null_mut()),
            KernelStatusCode::InvalidArgument
        );
    }
}

/// F3: `kernel_session_close_blocking_cancellable` honours the caller's cancel
/// token — a token fired from another thread while the DELETE is stalled
/// server-side unblocks the close promptly with `Cancelled`, instead of
/// blocking up to the retry ceiling. The session handle is still consumed on
/// the cancel path (the dropped future releases the boxed session), so there is
/// no free/close afterwards.
#[test]
fn session_close_blocking_cancellable_fires_during_stalled_delete() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-close-cancel","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            // DELETE stalls 30s; the test fires cancel after ~200ms and must return
            // long before that would elapse.
            Mock::given(method("DELETE"))
                .and(path("/api/2.0/sql/sessions/sess-close-cancel"))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
                .mount(&server)
                .await;
            server
        });
    let uri = server.uri();

    unsafe {
        let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
        assert_eq!(
            kernel_session_config_new(&mut config),
            KernelStatusCode::Success
        );
        let host = CString::new(uri.as_str()).unwrap();
        let http_path = CString::new("/sql/1.0/warehouses/test_wh").unwrap();
        assert_eq!(
            kernel_session_config_set_http_path(config, host.as_ptr(), http_path.as_ptr()),
            KernelStatusCode::Success
        );
        let auth = CString::new("dapi_test_token").unwrap();
        assert_eq!(
            kernel_session_config_set_auth_pat(config, auth.as_ptr()),
            KernelStatusCode::Success
        );
        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_open(config, &mut session),
            KernelStatusCode::Success
        );
        assert!(!session.is_null());

        let mut tok: *mut kernel_cancel_token_t = std::ptr::null_mut();
        assert_eq!(kernel_cancel_token_new(&mut tok), KernelStatusCode::Success);
        // Fire the token from a watcher thread after a short beat.
        let tok_addr = tok as usize;
        let watcher = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            let t = tok_addr as *mut kernel_cancel_token_t;
            assert_eq!(kernel_cancel_token_cancel(t), KernelStatusCode::Success);
        });

        let start = Instant::now();
        let rc = kernel_session_close_blocking_cancellable(session, tok);
        let elapsed = start.elapsed();
        watcher.join().unwrap();

        assert_eq!(
            rc,
            KernelStatusCode::Cancelled,
            "a token fired during a stalled DELETE must unblock the close as Cancelled"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "cancel must unblock the stalled close promptly, took {elapsed:?}"
        );
        assert_eq!(kernel_cancel_token_free(tok), KernelStatusCode::Success);
        // No further close/free: the session was consumed on the cancel path.
    }
}

/// A NULL token makes `kernel_session_close_blocking_cancellable` behave exactly
/// like the plain `kernel_session_close_blocking`: it awaits the DELETE and
/// returns `Success`, with the DELETE observable immediately (no polling).
#[test]
fn session_close_blocking_cancellable_null_token_awaits_delete() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-close-nulltok","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            Mock::given(method("DELETE"))
                .and(path("/api/2.0/sql/sessions/sess-close-nulltok"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;
            server
        });
    let uri = server.uri();

    unsafe {
        let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
        assert_eq!(
            kernel_session_config_new(&mut config),
            KernelStatusCode::Success
        );
        let host = CString::new(uri.as_str()).unwrap();
        let http_path = CString::new("/sql/1.0/warehouses/test_wh").unwrap();
        assert_eq!(
            kernel_session_config_set_http_path(config, host.as_ptr(), http_path.as_ptr()),
            KernelStatusCode::Success
        );
        let auth = CString::new("dapi_test_token").unwrap();
        assert_eq!(
            kernel_session_config_set_auth_pat(config, auth.as_ptr()),
            KernelStatusCode::Success
        );
        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_open(config, &mut session),
            KernelStatusCode::Success
        );
        assert!(!session.is_null());

        assert_eq!(
            kernel_session_close_blocking_cancellable(session, std::ptr::null()),
            KernelStatusCode::Success
        );
    }

    let saw_delete = rt.block_on(async {
        let requests = server.received_requests().await.unwrap_or_default();
        requests
            .iter()
            .any(|r| r.method.as_str() == "DELETE" && r.url.path().ends_with("sess-close-nulltok"))
    });
    assert!(
        saw_delete,
        "a NULL token must await the DELETE like the plain blocking close"
    );
}
