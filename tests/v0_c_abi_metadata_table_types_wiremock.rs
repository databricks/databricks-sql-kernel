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

//! Wiremock test for the C ABI `kernel_metadata_list_table_types` entry
//! point. `table_types` is a static, no-wire call (the set comes from the
//! session's transport), so only a session-open mock is needed: the test
//! opens a real session over the C ABI, drives the raw FFI function, and
//! asserts the stream carries the single `TABLE_TYPE` column (JDBC
//! getTableTypes shape) with at least one row. This pins the FFI wrapper
//! that backs ODBC `SQLTables(table_type = SQL_ALL_TABLE_TYPES)`.
//!
//! Like the other C ABI wiremock tests, the entry points run on the
//! kernel's own process-wide runtime inside `block_on_ffi`, so they must
//! be called from a plain (non-async) thread — hence `#[test]`, with a
//! dedicated runtime only to host the wiremock server.

use std::ffi::CString;

use arrow_array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{RecordBatch, StructArray};
use arrow_schema::Schema;
use databricks_sql_kernel::c_abi::metadata::kernel_metadata_list_table_types;
use databricks_sql_kernel::c_abi::result::{
    kernel_result_stream_close, kernel_result_stream_get_schema, kernel_result_stream_next_batch,
    kernel_result_stream_t,
};
use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_open, kernel_session_t,
    KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn list_table_types_returns_table_type_column() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/warehouses/test_wh/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-table-types","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            Mock::given(method("DELETE"))
                .and(path("/api/2.0/sql/sessions/sess-table-types"))
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

        // Drive the FFI entry point. No further wire calls: the type set
        // is static, sourced from the session transport.
        let mut stream: *mut kernel_result_stream_t = std::ptr::null_mut();
        assert_eq!(
            kernel_metadata_list_table_types(session, &mut stream),
            KernelStatusCode::Success
        );
        assert!(!stream.is_null());

        // The schema must be the single-column JDBC getTableTypes shape.
        let mut ffi_schema = FFI_ArrowSchema::empty();
        assert_eq!(
            kernel_result_stream_get_schema(stream, &mut ffi_schema),
            KernelStatusCode::Success
        );
        let schema = Schema::try_from(&ffi_schema).expect("import schema");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["TABLE_TYPE"], "JDBC getTableTypes shape");

        // Drain batches; expect at least one row (e.g. TABLE / VIEW / …).
        let mut total_rows = 0usize;
        loop {
            let mut arr = FFI_ArrowArray::empty();
            let mut sch = FFI_ArrowSchema::empty();
            assert_eq!(
                kernel_result_stream_next_batch(stream, &mut arr, &mut sch),
                KernelStatusCode::Success
            );
            if arr.is_released() {
                break; // end-of-stream sentinel
            }
            // A batch is exported as a StructArray (the Arrow C Data
            // batch-as-array form); import it back the same way.
            let data = from_ffi(arr, &sch).expect("import array");
            let batch = RecordBatch::from(StructArray::from(data));
            total_rows += batch.num_rows();
        }
        assert!(
            total_rows >= 1,
            "table types must include at least one type, got {total_rows}"
        );

        assert_eq!(
            kernel_result_stream_close(stream),
            KernelStatusCode::Success
        );
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}
