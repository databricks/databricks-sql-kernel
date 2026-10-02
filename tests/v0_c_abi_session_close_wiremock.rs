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

//! Wiremock test for the C ABI's `kernel_session_close` delete-initiation
//! contract: closing a session must initiate the server-side
//! `DeleteSession` **even while a statement handle from that session is
//! still open**. The statement handle holds its own `Arc<SessionInner>`
//! clone, so if close relied on the last-`Arc`-drop path (`SessionInner::
//! Drop`) the delete would be deferred until the statement is also closed
//! — the bug this pins. `kernel_session_close` instead calls
//! `initiate_delete` eagerly.
//!
//! The C ABI drives its own process-wide tokio runtime inside
//! `block_on_ffi`, so the entry points must be called from a plain
//! (non-async) thread — hence `#[test]`, not `#[tokio::test]`, with a
//! dedicated runtime only to host the wiremock server.

use std::ffi::CString;
use std::time::Duration;

use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_is_open, kernel_session_new_statement,
    kernel_session_open, kernel_session_t, KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::statement::{kernel_statement_close, kernel_statement_t};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn session_close_initiates_delete_with_statement_still_open() {
    // A dedicated runtime just to run the async wiremock setup / polling;
    // the C ABI calls themselves run on the kernel's own runtime.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/warehouses/test_wh/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-c-abi-close","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            // The DELETE we expect close to initiate.
            Mock::given(method("DELETE"))
                .and(path("/api/2.0/sql/sessions/sess-c-abi-close"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;
            server
        });
    let uri = server.uri();

    unsafe {
        // Build config → open session.
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

        // A freshly-opened session reports open (guards against an inverted
        // or hardcoded-false is_open). The false case can't be probed over
        // the C ABI — kernel_session_close frees the handle in the same
        // call — so it's covered by the crate-internal
        // `session::is_open_tests::is_open_flips_true_to_false_on_delete`.
        let mut is_open = false;
        assert_eq!(
            kernel_session_is_open(session, &mut is_open),
            KernelStatusCode::Success
        );
        assert!(is_open, "a freshly-opened session must report open");

        // Mint a statement — it holds an Arc<SessionInner> clone, so the
        // session's inner won't drop when we close the session below.
        let mut stmt: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut stmt),
            KernelStatusCode::Success
        );
        assert!(!stmt.is_null());

        // Close the session while the statement is STILL open. The fix
        // must initiate DeleteSession now rather than deferring it.
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);

        // Close the statement afterwards (orderly teardown of the leftover
        // handle). This must not double-fire the delete.
        assert_eq!(kernel_statement_close(stmt), KernelStatusCode::Success);
    }

    // The DELETE is a detached fire-and-forget task; poll the mock's
    // received requests until it lands (or time out).
    let saw_delete = rt.block_on(async {
        for _ in 0..50 {
            let requests = server.received_requests().await.unwrap_or_default();
            if requests.iter().any(|r| {
                r.method.as_str() == "DELETE" && r.url.path().ends_with("sess-c-abi-close")
            }) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    });
    assert!(
        saw_delete,
        "kernel_session_close must initiate DeleteSession even with a statement handle still open"
    );
}
