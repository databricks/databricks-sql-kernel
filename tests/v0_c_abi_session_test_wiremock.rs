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

//! Wiremock tests for the C ABI's `kernel_session_test` connection-probe
//! contract: unlike `kernel_session_is_open` (a local handle-state check),
//! `kernel_session_test` round-trips a `SELECT 1` to the server, so it
//! returns `Success` only when the session is actually reachable and an
//! error status when the round-trip fails. This is the I/O-performing
//! probe an ODBC host wires to `SQL_ATTR_CONNECTION_DEAD` so a
//! server-side-dead session can be reported dead
//! (databricks/databricks-odbc#298).
//!
//! The C ABI drives its own process-wide tokio runtime inside
//! `block_on_ffi`, so the entry points must be called from a plain
//! (non-async) thread — hence `#[test]`, not `#[tokio::test]`, with a
//! dedicated runtime only to host the wiremock server.

use std::ffi::CString;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_open, kernel_session_t,
    kernel_session_test, KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A SEA `SUCCEEDED` inline body for a one-column, one-row `SELECT 1`.
fn select_1_succeeded_body(statement_id: &str) -> String {
    let schema = Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut ipc = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut ipc, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 1,
                "columns": [{ "name": "1", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0 }],
            },
            "total_chunk_count": 1,
            "total_row_count": 1,
            "total_byte_count": ipc.len() as i64,
            "truncated": false,
        },
        "result": {
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": 1,
            "byte_count": ipc.len() as i64,
            "attachment": STANDARD.encode(&ipc),
        },
    })
    .to_string()
}

/// Build a config and open a session against `uri`, returning the raw
/// handle. Panics on any setup failure.
///
/// # Safety
/// Returns an owned `kernel_session_t*` the caller must release with
/// `kernel_session_close`.
unsafe fn open_session(uri: &str) -> *mut kernel_session_t {
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

#[test]
fn session_test_returns_success_when_server_reachable() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/warehouses/test_wh/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-c-abi-test","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string(select_1_succeeded_body("stmt-c-abi-test")),
                )
                .mount(&server)
                .await;
            // Swallow statement + session close DELETEs so teardown is quiet.
            Mock::given(method("DELETE"))
                .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
                .mount(&server)
                .await;
            server
        });
    let uri = server.uri();

    unsafe {
        let session = open_session(&uri);

        // The probe reaches the server and succeeds.
        assert_eq!(
            kernel_session_test(session, 0),
            KernelStatusCode::Success,
            "kernel_session_test must return Success against a reachable server"
        );

        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }

    // The probe must have issued a POST /statements (the SELECT 1), i.e. it
    // is a real round-trip, not a local no-op.
    let saw_execute = rt.block_on(async {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .any(|r| r.method.as_str() == "POST" && r.url.path() == "/api/2.0/sql/statements")
    });
    assert!(
        saw_execute,
        "kernel_session_test must round-trip a SELECT 1 statement"
    );
}

#[test]
fn session_test_returns_error_when_server_unreachable() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Open the session against a live mock (so open succeeds), then tear the
    // server down so the SELECT 1 probe has nowhere to land.
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/warehouses/test_wh/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-c-abi-dead","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            server
        });
    let uri = server.uri();

    unsafe {
        let session = open_session(&uri);

        // Kill the server: the statement round-trip now fails.
        drop(server);

        let rc = kernel_session_test(session, 0);
        assert_ne!(
            rc,
            KernelStatusCode::Success,
            "kernel_session_test must return an error status when the server is unreachable"
        );

        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}

// odbc#347-C: a non-zero `timeout_secs` bounds the whole probe. When the SELECT 1
// round-trip is slower than the deadline, the probe returns Timeout promptly —
// it does not block for the full server delay (nor the session's retry budget).
#[test]
fn session_test_honours_per_call_timeout() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/warehouses/test_wh/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-c-abi-slow","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            // The SELECT 1 statement POST is delayed well beyond the 1s probe
            // deadline, so the probe must give up on its own deadline rather than
            // wait for this response.
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_delay(std::time::Duration::from_secs(20))
                        .set_body_string(select_1_succeeded_body("stmt-slow")),
                )
                .mount(&server)
                .await;
            server
        });
    let uri = server.uri();

    unsafe {
        let session = open_session(&uri);

        // With a 1-second deadline against a 20-second response, the probe must
        // return Timeout within a small multiple of the deadline (not ~20s).
        let start = std::time::Instant::now();
        let rc = kernel_session_test(session, 1);
        let elapsed = start.elapsed();

        assert_eq!(
            rc,
            KernelStatusCode::Timeout,
            "a probe slower than its per-call deadline must return Timeout"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "the probe blocked for {:?}, far longer than its 1s deadline",
            elapsed
        );

        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}
