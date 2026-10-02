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

//! Wiremock test for the C ABI result-stream canceller
//! (`kernel_result_stream_canceller_new` / `_cancel` / `_free`, odbc#355-B).
//!
//! The deterministic in-flight interrupt (a `next_batch` parked inside a
//! blocking fetch, unwound by a cancel from another thread) is covered by the
//! crate-internal unit test `c_abi::result::tests::
//! cancel_interrupts_an_in_flight_next_batch`, which uses a controllable
//! blocking reader. This integration test proves the **exported** canceller
//! symbols link and work against a real session + transport: open a session
//! over wiremock, take a live result stream, create a canceller from it, fire
//! it, and confirm the subsequent `next_batch` reports `Cancelled` and the
//! stream stays permanently aborted (never hangs, never re-drives the reader).
//!
//! Like the other C ABI wiremock tests, the entry points run on the kernel's
//! process-wide runtime inside `block_on_ffi`, so they must be called from a
//! plain (non-async) thread — hence `#[test]`, with a dedicated runtime only
//! to host the wiremock server.

use std::ffi::CString;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use databricks_sql_kernel::c_abi::metadata::kernel_metadata_list_table_types;
use databricks_sql_kernel::c_abi::result::{
    kernel_result_stream_canceller_cancel, kernel_result_stream_canceller_free,
    kernel_result_stream_canceller_new, kernel_result_stream_canceller_t,
    kernel_result_stream_close, kernel_result_stream_next_batch, kernel_result_stream_t,
};
use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_open, kernel_session_t,
    KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Open a real C ABI session against `uri` (PAT auth). Mirrors the session
/// setup shared by the other C ABI wiremock tests.
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

/// Firing an exported result-stream canceller aborts the stream: the next
/// `next_batch` returns `Cancelled` and the stream stays aborted across
/// subsequent calls (never yields the buffered rows, never hangs).
#[test]
fn exported_canceller_aborts_the_stream() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/warehouses/test_wh/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-rs-cancel","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-rs-cancel"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    unsafe {
        let session = open_session(&uri);

        // A live owned result stream (list_table_types is static/no-wire, so
        // it is a well-defined stream regardless of the mock's query surface).
        let mut stream: *mut kernel_result_stream_t = std::ptr::null_mut();
        assert_eq!(
            kernel_metadata_list_table_types(session, &mut stream),
            KernelStatusCode::Success
        );
        assert!(!stream.is_null());

        // Create a canceller from the live stream and fire it.
        let mut canceller: *mut kernel_result_stream_canceller_t = std::ptr::null_mut();
        assert_eq!(
            kernel_result_stream_canceller_new(stream, &mut canceller),
            KernelStatusCode::Success
        );
        assert!(!canceller.is_null());
        assert_eq!(
            kernel_result_stream_canceller_cancel(canceller),
            KernelStatusCode::Success
        );

        // next_batch after the cancel returns Cancelled with no batch, and a
        // repeat call still reports Cancelled (permanent abort — never hangs).
        for _ in 0..2 {
            let mut arr = FFI_ArrowArray::empty();
            let mut sch = FFI_ArrowSchema::empty();
            let rc = kernel_result_stream_next_batch(stream, &mut arr, &mut sch);
            assert_eq!(
                rc,
                KernelStatusCode::Cancelled,
                "next_batch after cancel must report Cancelled"
            );
            assert!(
                arr.is_released(),
                "no batch may be handed out after a cancel"
            );
        }

        assert_eq!(
            kernel_result_stream_canceller_free(canceller),
            KernelStatusCode::Success
        );
        assert_eq!(
            kernel_result_stream_close(stream),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}
