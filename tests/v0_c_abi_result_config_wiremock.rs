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

//! C-ABI wiremock tests for the typed result-config surface
//! (`kernel_session_config_set_result_config`).
//!
//! These prove the typed client-side knobs actually reach the wire /
//! kernel behaviour through the C ABI — closing the gap where the C ABI
//! previously could not reach `ResultConfig` at all (only the legacy
//! `cloudfetch_max_chunks_in_memory` magic string). The headline check:
//! opening a session with `cloudfetch_enabled = false` via the typed
//! setter makes every `ExecuteStatement` carry SEA `disposition = INLINE`
//! (see #196 for why that, not a `can_cloud_download` session conf, is the
//! correct mechanism), while the default carries
//! `INLINE_OR_EXTERNAL_LINKS`.

use std::ffi::CString;

use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_config_set_result_config,
    kernel_session_new_statement, kernel_session_open, kernel_session_t, KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::statement::{
    kernel_executed_statement_close, kernel_executed_statement_t, kernel_statement_execute,
    kernel_statement_set_sql, kernel_statement_t,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

/// Catch-all DELETE so teardown's fire-and-forget deletes don't 404.
async fn mount_close_any(server: &MockServer) {
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(server)
        .await;
}

/// A terminal SUCCEEDED body with an empty result (`total_chunk_count: 0`),
/// materialised from the manifest schema with no Arrow attachment — so
/// execute completes without needing chunk data.
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

async fn mount_execute(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(succeeded_empty_body("stmt-rc")))
        .mount(server)
        .await;
}

/// Open a session over the C ABI, applying `set_result_config` first when
/// `cloudfetch_enabled` is `Some`. Returns the raw handle (caller closes).
///
/// # Safety
/// Caller owns the returned handle and must `kernel_session_close` it.
unsafe fn open_session_c_abi(uri: &str, cloudfetch_enabled: Option<bool>) -> *mut kernel_session_t {
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
    if let Some(cf) = cloudfetch_enabled {
        // 0s => keep kernel defaults for the window/chunk knobs.
        assert_eq!(
            kernel_session_config_set_result_config(config, cf, 0, 0, 0),
            KernelStatusCode::Success
        );
    }
    let mut session: *mut kernel_session_t = std::ptr::null_mut();
    assert_eq!(
        kernel_session_open(config, &mut session),
        KernelStatusCode::Success
    );
    assert!(!session.is_null());
    session
}

/// Drive one execute over the C ABI and return the `disposition` string the
/// kernel sent on the recorded POST /statements request.
fn recorded_execute_disposition(cloudfetch_enabled: Option<bool>) -> String {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        mount_execute(&server).await;
        server
    });
    let uri = server.uri();

    unsafe {
        let session = open_session_c_abi(&uri, cloudfetch_enabled);
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
        let mut exec: *mut kernel_executed_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_execute(stmt, &mut exec),
            KernelStatusCode::Success
        );
        kernel_executed_statement_close(exec);
        kernel_session_close(session);
    }

    let recorded = rt.block_on(async { server.received_requests().await.unwrap() });
    let exec = recorded
        .iter()
        .find(|r| r.url.path() == "/api/2.0/sql/statements" && r.method.as_str() == "POST")
        .expect("a POST /statements request was made");
    let body: serde_json::Value =
        serde_json::from_slice(&exec.body).expect("execute body must be valid JSON");
    body.get("disposition")
        .and_then(|v| v.as_str())
        .expect("execute request must carry a disposition")
        .to_string()
}

#[test]
fn c_abi_set_result_config_disabled_sends_inline_disposition() {
    // The headline: a C-ABI host that disables CloudFetch via the typed
    // setter gets SEA `disposition = INLINE` on the wire — proving the
    // typed result config reaches the execute path (previously unreachable
    // from the C ABI). Mirrors the Rust-crate coverage in
    // tests/v0_execute_wiremock.rs.
    assert_eq!(recorded_execute_disposition(Some(false)), "INLINE");
}

#[test]
fn c_abi_set_result_config_enabled_sends_inline_or_external_links_disposition() {
    assert_eq!(
        recorded_execute_disposition(Some(true)),
        "INLINE_OR_EXTERNAL_LINKS"
    );
}

#[test]
fn c_abi_default_result_config_sends_inline_or_external_links_disposition() {
    // No set_result_config call at all → kernel default (CloudFetch on).
    assert_eq!(
        recorded_execute_disposition(None),
        "INLINE_OR_EXTERNAL_LINKS"
    );
}
