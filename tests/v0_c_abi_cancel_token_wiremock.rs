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

//! Wiremock tests for the cooperative cancel token and the
//! `kernel_session_open_cancellable` connect-side entry point.
//!
//! The behaviour under test is: a blocking C-ABI call that would otherwise sit
//! on a slow/stalled server response can be interrupted by firing a cancel
//! token from another thread, returning `Cancelled` promptly instead of
//! blocking to completion — and, crucially, that a NULL token leaves the exact
//! same call blocking to completion (so cancellation is opt-in and the default
//! path is unchanged).
//!
//! We simulate a stalled server with wiremock's response `set_delay`, fire the
//! token from a spawned OS thread after a short beat, and assert both the
//! `Cancelled` status and that the call returned well before the server delay
//! would have elapsed. The C ABI drives its own process-wide runtime inside
//! `block_on_ffi_cancellable`, so the entry points run on a plain `#[test]`
//! thread; the dedicated tokio runtime here only hosts the mock server.
//!
//! The connect path exercises the exact `block_on_ffi_cancellable` machinery the
//! result-stream fetch path (`kernel_result_stream_next_batch_cancellable`) also
//! routes through. The final test drives the fetch path directly through a real
//! executed statement, proving `next_batch_cancellable` honors a fired token (and
//! that a NULL token yields the batch normally).

use std::ffi::CString;
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use databricks_sql_kernel::c_abi::cancel::{
    kernel_cancel_token_cancel, kernel_cancel_token_free, kernel_cancel_token_new,
    kernel_cancel_token_t,
};
use databricks_sql_kernel::c_abi::result::{
    kernel_result_stream_close, kernel_result_stream_next_batch_cancellable, kernel_result_stream_t,
};
use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_new_statement, kernel_session_open,
    kernel_session_open_cancellable, kernel_session_t, KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::statement::{
    kernel_executed_statement_close, kernel_executed_statement_get_result_stream,
    kernel_executed_statement_t, kernel_statement_execute, kernel_statement_set_sql,
    kernel_statement_t,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Address a config at the given mock server URI with PAT auth. Shared setup.
///
/// # Safety
/// `config` must be a live config handle; the returned CStrings must outlive
/// the setter calls (they do — the caller holds them).
unsafe fn address_config(
    config: *mut KernelSessionConfig,
    uri: &str,
) -> (CString, CString, CString) {
    let host = CString::new(uri).unwrap();
    let http_path = CString::new("/sql/1.0/warehouses/test_wh").unwrap();
    let token = CString::new("dapi_test_token").unwrap();
    assert_eq!(
        kernel_session_config_set_http_path(config, host.as_ptr(), http_path.as_ptr()),
        KernelStatusCode::Success
    );
    assert_eq!(
        kernel_session_config_set_auth_pat(config, token.as_ptr()),
        KernelStatusCode::Success
    );
    (host, http_path, token)
}

/// A token that is ALREADY fired before the call short-circuits connect to
/// `Cancelled` without opening a session — the "ctx already done at call time"
/// case. The `biased` select in `block_on_ffi_cancellable` makes this
/// deterministic (the cancel branch is polled first).
#[test]
fn open_cancellable_prefired_token_short_circuits() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        // A create-session mock that, if ever hit, would succeed — so a failure
        // to short-circuit surfaces as a Success (test fails on the assert).
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-prefired","warehouse_id":"test_wh"}"#),
            )
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
        let _held = address_config(config, &uri);

        let mut tok: *mut kernel_cancel_token_t = std::ptr::null_mut();
        assert_eq!(kernel_cancel_token_new(&mut tok), KernelStatusCode::Success);
        // Fire BEFORE the call.
        assert_eq!(kernel_cancel_token_cancel(tok), KernelStatusCode::Success);

        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        let rc = kernel_session_open_cancellable(config, &mut session, tok);
        assert_eq!(
            rc,
            KernelStatusCode::Cancelled,
            "a pre-fired token must short-circuit connect to Cancelled"
        );
        assert!(session.is_null(), "no session handle on a cancelled open");

        assert_eq!(kernel_cancel_token_free(tok), KernelStatusCode::Success);
    }
}

/// A token fired from another thread DURING a stalled connect interrupts it:
/// the call returns `Cancelled` well before the (long) server delay elapses.
/// This is the connect-side deadline property — a slow warehouse cold-start no
/// longer blocks past the caller's deadline.
#[test]
fn open_cancellable_fires_during_stalled_connect() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Create-session stalls for 10s; the test fires cancel after ~150ms and
    // must return long before the 10s would elapse.
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(10))
                    .set_body_string(r#"{"session_id":"sess-stall","warehouse_id":"test_wh"}"#),
            )
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
        let _held = address_config(config, &uri);

        let mut tok: *mut kernel_cancel_token_t = std::ptr::null_mut();
        assert_eq!(kernel_cancel_token_new(&mut tok), KernelStatusCode::Success);

        // Watcher thread: fire the token after a short beat. `usize` shuttle so
        // the raw pointer crosses the thread boundary (kernel_cancel_token_t is
        // Send-safe to fire from another thread by contract).
        let tok_addr = tok as usize;
        let watcher = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            let t = tok_addr as *mut kernel_cancel_token_t;
            assert_eq!(kernel_cancel_token_cancel(t), KernelStatusCode::Success);
        });

        let start = Instant::now();
        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        let rc = kernel_session_open_cancellable(config, &mut session, tok);
        let elapsed = start.elapsed();

        watcher.join().unwrap();

        assert_eq!(
            rc,
            KernelStatusCode::Cancelled,
            "a token fired mid-connect must interrupt the blocking connect"
        );
        assert!(session.is_null(), "no session handle on a cancelled open");
        assert!(
            elapsed < Duration::from_secs(5),
            "cancel must abort the stalled connect promptly, took {elapsed:?}"
        );

        assert_eq!(kernel_cancel_token_free(tok), KernelStatusCode::Success);
    }
}

/// A NULL token means "no cancellation": a normal (fast) create-session with a
/// NULL token completes normally. This pins that the cancellable entry point is
/// a strict superset of the plain one (the default path is unchanged).
#[test]
fn open_cancellable_null_token_behaves_like_plain_open() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-null-tok","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-null-tok"))
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
        let _held = address_config(config, &uri);

        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        // NULL token → same as kernel_session_open.
        let rc = kernel_session_open_cancellable(config, &mut session, std::ptr::null());
        assert_eq!(rc, KernelStatusCode::Success);
        assert!(!session.is_null());

        // Tidy up the opened session (fire-and-forget close).
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}

// ── mid-fetch cancel (kernel_result_stream_next_batch_cancellable) ────────────

/// A SUCCEEDED statement response carrying one inline-Arrow chunk of BIGINT rows,
/// so a real result stream can be opened and fetched over the C ABI.
fn succeeded_inline_body(statement_id: &str) -> String {
    let batch = RecordBatch::try_from_iter([(
        "n",
        std::sync::Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let mut ipc = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut ipc, &batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    let attachment = STANDARD.encode(&ipc);
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": { "column_count": 1, "columns": [
                { "name": "n", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0 }
            ]},
            "total_chunk_count": 1,
            "total_row_count": 3,
            "total_byte_count": ipc.len() as i64,
            "truncated": false,
        },
        "result": {
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": 3,
            "byte_count": ipc.len() as i64,
            "attachment": attachment,
        },
    })
    .to_string()
}

/// Drive the fetch path directly: open a session, execute a query that returns
/// an inline-Arrow result, obtain the result stream, then call
/// `kernel_result_stream_next_batch_cancellable`. A **pre-fired** token must
/// short-circuit the fetch to `Cancelled` (the biased select honors the token
/// before polling the fetch — the Gap 6 mid-fetch property), while a **NULL**
/// token on a fresh stream yields the batch normally (parity with the plain
/// next_batch). This exercises the actual `next_batch_cancellable` entry point,
/// not just the shared machinery the connect tests cover.
#[test]
fn next_batch_cancellable_prefired_token_short_circuits_fetch() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-fetch","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-fetch"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(succeeded_inline_body("stmt-fetch")),
            )
            .mount(&server)
            .await;
        server
    });
    let uri = server.uri();

    unsafe {
        // Open a session.
        let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
        assert_eq!(
            kernel_session_config_new(&mut config),
            KernelStatusCode::Success
        );
        let _held = address_config(config, &uri);
        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_open(config, &mut session),
            KernelStatusCode::Success
        );
        assert!(!session.is_null());

        // execute a query → executed handle → result stream.
        let mut stmt: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut stmt),
            KernelStatusCode::Success
        );
        let sql = CString::new("SELECT n FROM t").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );
        let mut exec: *mut kernel_executed_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_execute(stmt, &mut exec),
            KernelStatusCode::Success
        );
        let mut stream: *mut kernel_result_stream_t = std::ptr::null_mut();
        assert_eq!(
            kernel_executed_statement_get_result_stream(exec, &mut stream),
            KernelStatusCode::Success
        );
        assert!(!stream.is_null());

        // Pre-fire a token, then fetch: the biased select must honor the fired
        // token and return Cancelled without producing a batch.
        let mut tok: *mut kernel_cancel_token_t = std::ptr::null_mut();
        assert_eq!(kernel_cancel_token_new(&mut tok), KernelStatusCode::Success);
        assert_eq!(kernel_cancel_token_cancel(tok), KernelStatusCode::Success);
        let mut arr = FFI_ArrowArray::empty();
        let mut sch = FFI_ArrowSchema::empty();
        let rc = kernel_result_stream_next_batch_cancellable(stream, &mut arr, &mut sch, tok);
        assert_eq!(
            rc,
            KernelStatusCode::Cancelled,
            "a pre-fired token must short-circuit the fetch to Cancelled"
        );
        assert_eq!(kernel_cancel_token_free(tok), KernelStatusCode::Success);

        // A NULL token on the same stream fetches the batch normally (parity with
        // the plain next_batch): the inline chunk yields a non-released array.
        let mut arr2 = FFI_ArrowArray::empty();
        let mut sch2 = FFI_ArrowSchema::empty();
        let rc = kernel_result_stream_next_batch_cancellable(
            stream,
            &mut arr2,
            &mut sch2,
            std::ptr::null(),
        );
        assert_eq!(
            rc,
            KernelStatusCode::Success,
            "NULL token must fetch normally"
        );

        // Teardown in the documented order: stream → executed → session.
        kernel_result_stream_close(stream);
        kernel_executed_statement_close(exec);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}

/// One chunk's worth of Arrow IPC stream bytes carrying a single Int64 column
/// `n`. Used to build multi-chunk CloudFetch (external-link) results.
fn arrow_chunk_ipc(values: Vec<i64>) -> Vec<u8> {
    let batch = RecordBatch::try_from_iter([(
        "n",
        std::sync::Arc::new(Int64Array::from(values)) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let mut ipc = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut ipc, &batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    ipc
}

/// A SUCCEEDED statement body describing a **two-chunk** CloudFetch result whose
/// chunks are served as external links back at the mock server. Chunk 0 carries
/// its own `next_chunk_index=1` so the reader keeps going after the first batch.
fn succeeded_two_chunk_external_body(
    statement_id: &str,
    chunk0_url: &str,
    chunk1_url: &str,
    chunk0_bytes: i64,
    chunk1_bytes: i64,
) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": { "column_count": 1, "columns": [
                { "name": "n", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0 }
            ]},
            "total_chunk_count": 2,
            "total_row_count": 4,
            "truncated": false,
        },
        "result": {
            "external_links": [
                {
                    "external_link": chunk0_url,
                    "expiration": "2099-01-01T00:00:00.000Z",
                    "chunk_index": 0,
                    "row_offset": 0,
                    "row_count": 2,
                    "byte_count": chunk0_bytes,
                    "next_chunk_index": 1,
                },
                {
                    "external_link": chunk1_url,
                    "expiration": "2099-01-01T00:00:00.000Z",
                    "chunk_index": 1,
                    "row_offset": 2,
                    "row_count": 2,
                    "byte_count": chunk1_bytes,
                },
            ],
        },
    })
    .to_string()
}

/// F4: a token fired WHILE a CloudFetch chunk download is genuinely in flight
/// interrupts the caller's wait promptly. This is the mid-flight fetch property
/// the pre-fired inline test can't reach (there, no download exists).
///
/// Shape: a two-chunk external-link result. Chunk 0's GET returns valid Arrow
/// IPC immediately, so `execute()` + `get_schema()` (which eagerly waits on
/// chunk 0) and the FIRST `next_batch` both complete normally. Chunk 1's GET is
/// STALLED with a long `set_delay`, so the SECOND `next_batch_cancellable` is
/// blocked on an actually-in-flight download. A watcher thread fires the token
/// mid-fetch, and the call must return `Cancelled` well before the stall would
/// elapse — proving cancel interrupts the wait on a live fetch.
///
/// Per the corrected F1 contract, this asserts the caller unblocks promptly; it
/// does NOT assert the background download is torn down (it isn't — the detached
/// task drains to its read-timeout), which is exactly why the header/rows.go
/// docs were reworded from "aborts the download" to "stops waiting".
#[test]
fn next_batch_cancellable_fires_during_stalled_fetch() {
    let chunk0 = arrow_chunk_ipc(vec![1, 2]);
    let chunk0_len = chunk0.len() as i64;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let server =
        rt.block_on(async {
            let server = MockServer::start().await;
            let chunk0_url = format!("{}/cf/stmt-stall/chunk-0.arrow", server.uri());
            let chunk1_url = format!("{}/cf/stmt-stall/chunk-1.arrow", server.uri());

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/sessions"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    r#"{"session_id":"sess-stall-fetch","warehouse_id":"test_wh"}"#,
                ))
                .mount(&server)
                .await;
            Mock::given(method("DELETE"))
                .and(path("/api/2.0/sql/sessions/sess-stall-fetch"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_string(
                    succeeded_two_chunk_external_body(
                        "stmt-stall",
                        &chunk0_url,
                        &chunk1_url,
                        chunk0_len,
                        1024,
                    ),
                ))
                .mount(&server)
                .await;
            // Chunk 0: immediate valid Arrow IPC (drives schema + first batch).
            Mock::given(method("GET"))
                .and(path("/cf/stmt-stall/chunk-0.arrow"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(chunk0.clone()))
                .mount(&server)
                .await;
            // Chunk 1: STALLED — the download the second next_batch blocks on. The
            // 30s delay is far longer than the cancel latency we assert below.
            Mock::given(method("GET"))
                .and(path("/cf/stmt-stall/chunk-1.arrow"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_secs(30))
                        .set_body_bytes(arrow_chunk_ipc(vec![3, 4])),
                )
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
        let _held = address_config(config, &uri);
        let mut session: *mut kernel_session_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_open(config, &mut session),
            KernelStatusCode::Success
        );
        assert!(!session.is_null());

        let mut stmt: *mut kernel_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_session_new_statement(session, &mut stmt),
            KernelStatusCode::Success
        );
        let sql = CString::new("SELECT n FROM t").unwrap();
        assert_eq!(
            kernel_statement_set_sql(stmt, sql.as_ptr()),
            KernelStatusCode::Success
        );
        let mut exec: *mut kernel_executed_statement_t = std::ptr::null_mut();
        assert_eq!(
            kernel_statement_execute(stmt, &mut exec),
            KernelStatusCode::Success
        );
        let mut stream: *mut kernel_result_stream_t = std::ptr::null_mut();
        assert_eq!(
            kernel_executed_statement_get_result_stream(exec, &mut stream),
            KernelStatusCode::Success
        );
        assert!(!stream.is_null());

        // First batch (chunk 0) fetches normally with a NULL token.
        let mut arr0 = FFI_ArrowArray::empty();
        let mut sch0 = FFI_ArrowSchema::empty();
        let rc = kernel_result_stream_next_batch_cancellable(
            stream,
            &mut arr0,
            &mut sch0,
            std::ptr::null(),
        );
        assert_eq!(
            rc,
            KernelStatusCode::Success,
            "chunk 0 must fetch normally before the stalled chunk 1"
        );

        // Second batch (chunk 1) is stalled server-side. Fire the token from a
        // watcher thread after a short beat; the call must return Cancelled well
        // before the 30s stall elapses.
        let mut tok: *mut kernel_cancel_token_t = std::ptr::null_mut();
        assert_eq!(kernel_cancel_token_new(&mut tok), KernelStatusCode::Success);
        let tok_addr = tok as usize;
        let watcher = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            let t = tok_addr as *mut kernel_cancel_token_t;
            assert_eq!(kernel_cancel_token_cancel(t), KernelStatusCode::Success);
        });

        let mut arr1 = FFI_ArrowArray::empty();
        let mut sch1 = FFI_ArrowSchema::empty();
        let start = Instant::now();
        let rc = kernel_result_stream_next_batch_cancellable(stream, &mut arr1, &mut sch1, tok);
        let elapsed = start.elapsed();
        watcher.join().unwrap();

        assert_eq!(
            rc,
            KernelStatusCode::Cancelled,
            "a token fired mid-fetch must interrupt the wait on the stalled chunk"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "cancel must unblock the stalled fetch promptly, took {elapsed:?}"
        );
        assert_eq!(kernel_cancel_token_free(tok), KernelStatusCode::Success);

        kernel_result_stream_close(stream);
        kernel_executed_statement_close(exec);
        assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
    }
}
