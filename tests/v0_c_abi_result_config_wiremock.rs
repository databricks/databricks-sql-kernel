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

//! C-ABI wiremock tests for the client-side result-config surface
//! (`kernel_session_config_set_client_conf`).
//!
//! These prove the client-side knobs actually reach the wire / kernel
//! behaviour through the C ABI — closing the gap where the C ABI
//! previously could not reach `ResultConfig` at all (only the legacy
//! `cloudfetch_max_chunks_in_memory` magic string). The headline check:
//! opening a session with `cloudfetch_enabled = false` via
//! `set_client_conf` makes every `ExecuteStatement` carry SEA
//! `disposition = INLINE` (see #196 for why that, not a
//! `can_cloud_download` session conf, is the correct mechanism), while the
//! default carries `INLINE_OR_EXTERNAL_LINKS`.

use std::ffi::CString;

use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_free, kernel_session_config_new,
    kernel_session_config_set_auth_pat, kernel_session_config_set_client_conf,
    kernel_session_config_set_enable_geospatial_support, kernel_session_config_set_http_path,
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
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
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

/// Open a session over the C ABI, applying `set_client_conf` first when
/// `cloudfetch_enabled` is `Some`. Returns the raw handle (caller closes).
///
/// # Safety
/// Caller owns the returned handle and must `kernel_session_close` it.
unsafe fn open_session_c_abi(
    uri: &str,
    cloudfetch_enabled: Option<bool>,
    enable_geospatial_support: Option<bool>,
) -> *mut kernel_session_t {
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
        // Set the one client knob under test by name via the generic
        // client-conf setter; other knobs keep their kernel defaults.
        let key = CString::new("cloudfetch_enabled").unwrap();
        let value = CString::new(if cf { "true" } else { "false" }).unwrap();
        assert_eq!(
            kernel_session_config_set_client_conf(config, key.as_ptr(), value.as_ptr()),
            KernelStatusCode::Success
        );
    }
    if let Some(enabled) = enable_geospatial_support {
        assert_eq!(
            kernel_session_config_set_enable_geospatial_support(config, enabled),
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
        let session = open_session_c_abi(&uri, cloudfetch_enabled, None);
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

fn recorded_create_session_geospatial_conf(enabled: bool) -> Option<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_close_any(&server).await;
        server
    });
    let uri = server.uri();

    unsafe {
        let session = open_session_c_abi(&uri, None, Some(enabled));
        kernel_session_close(session);
    }

    let recorded = rt.block_on(async { server.received_requests().await.unwrap() });
    let create = recorded
        .iter()
        .find(|request| {
            request.url.path() == "/api/2.0/sql/warehouses/test_wh/sessions"
                && request.method.as_str() == "POST"
        })
        .expect("a POST /sessions request was made");
    let body: serde_json::Value =
        serde_json::from_slice(&create.body).expect("create-session body must be valid JSON");
    body.pointer("/session_confs/EnableGeoSpatialSupport")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

#[test]
fn c_abi_client_conf_cloudfetch_disabled_sends_inline_disposition() {
    // The headline: a C-ABI host that disables CloudFetch via
    // set_client_conf("cloudfetch_enabled","false") gets SEA
    // `disposition = INLINE` on the wire — proving the client-conf channel
    // reaches the execute path (previously unreachable from the C ABI).
    // Mirrors the Rust-crate coverage in tests/v0_execute_wiremock.rs.
    assert_eq!(recorded_execute_disposition(Some(false)), "INLINE");
}

#[test]
fn c_abi_client_conf_cloudfetch_enabled_sends_inline_or_external_links_disposition() {
    assert_eq!(
        recorded_execute_disposition(Some(true)),
        "INLINE_OR_EXTERNAL_LINKS"
    );
}

#[test]
fn c_abi_default_result_config_sends_inline_or_external_links_disposition() {
    // No set_client_conf call at all → kernel default (CloudFetch on).
    assert_eq!(
        recorded_execute_disposition(None),
        "INLINE_OR_EXTERNAL_LINKS"
    );
}

#[test]
fn c_abi_geospatial_mode_stays_client_side() {
    assert_eq!(recorded_create_session_geospatial_conf(true), None);
    assert_eq!(recorded_create_session_geospatial_conf(false), None);
}

#[test]
fn c_abi_set_client_conf_rejects_unknown_key_and_bad_value() {
    use databricks_sql_kernel::c_abi::error::{kernel_get_last_error, KernelError};
    unsafe {
        let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
        assert_eq!(
            kernel_session_config_new(&mut config),
            KernelStatusCode::Success
        );

        // Unknown client key → InvalidArgument (not silently ignored).
        let bad_key = CString::new("cloud_fetch_enabled").unwrap(); // typo
        let val = CString::new("false").unwrap();
        assert_eq!(
            kernel_session_config_set_client_conf(config, bad_key.as_ptr(), val.as_ptr()),
            KernelStatusCode::InvalidArgument
        );

        // A server session-conf key is NOT a client conf → rejected here
        // (it belongs on set_session_conf). This is the channel separation.
        let server_key = CString::new("ANSI_MODE").unwrap();
        let server_val = CString::new("true").unwrap();
        assert_eq!(
            kernel_session_config_set_client_conf(config, server_key.as_ptr(), server_val.as_ptr()),
            KernelStatusCode::InvalidArgument
        );

        // Unparseable value for a known bool key → InvalidArgument.
        let key = CString::new("cloudfetch_enabled").unwrap();
        let bad_val = CString::new("flase").unwrap(); // typo
        assert_eq!(
            kernel_session_config_set_client_conf(config, key.as_ptr(), bad_val.as_ptr()),
            KernelStatusCode::InvalidArgument
        );
        let mut err = std::mem::MaybeUninit::<KernelError>::uninit();
        assert!(
            kernel_get_last_error(err.as_mut_ptr()),
            "an error must be stored after a rejected client conf"
        );
        assert_eq!(
            err.assume_init().code,
            KernelStatusCode::InvalidArgument as i32
        );

        kernel_session_config_free(config);
    }
}
