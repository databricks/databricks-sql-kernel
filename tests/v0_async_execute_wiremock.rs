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

//! Wiremock tests for the v0 `Statement::submit` / `ExecutedAsyncStatement`
//! path.
//!
//! Each test stubs SEA's `CreateSession`, `POST /statements`, and the
//! `GET /statements/{id}` / `POST /statements/{id}/cancel` /
//! `DELETE /statements/{id}` endpoints as needed, then drives a
//! `Statement::submit()` through the async pipeline:
//! Session::open → Statement::submit → ExecutedAsyncStatement::status /
//! await_result / fetch_result.

use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, ErrorCode, ExecutedStatementHandle, ResultBatch, Session,
    StatementStatus, TypedValue,
};
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ─── Helpers ────────────────────────────────────────────────────────

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
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-test","warehouse_id":"test_wh"}"#),
        )
        .mount(server)
        .await;
}

async fn mount_delete_session(server: &MockServer) {
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/sess-test"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(server)
        .await;
}

/// Stub a successful close on any statement (DELETE
/// /statements/...). Quiets the executed-handle's Drop spawn.
async fn mount_close_any_statement(server: &MockServer) {
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(server)
        .await;
}

fn arrow_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

fn succeeded_inline_body(statement_id: &str, batch: &RecordBatch) -> String {
    let ipc = arrow_ipc_bytes(batch);
    let attachment = STANDARD.encode(&ipc);
    let columns: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            json!({
                "name": f.name(),
                "type_name": match f.data_type() {
                    DataType::Int64 => "BIGINT",
                    DataType::Utf8 => "STRING",
                    _ => "STRING",
                },
                "type_text": match f.data_type() {
                    DataType::Int64 => "BIGINT",
                    DataType::Utf8 => "STRING",
                    _ => "STRING",
                },
                "position": i as i32,
            })
        })
        .collect();
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": batch.num_columns() as i32,
                "columns": columns,
            },
            "total_chunk_count": 1,
            "total_row_count": batch.num_rows() as i64,
            "total_byte_count": ipc.len() as i64,
            "truncated": false,
        },
        "result": {
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": batch.num_rows() as i64,
            "byte_count": ipc.len() as i64,
            "attachment": attachment,
        },
    })
    .to_string()
}

fn file_transfer_body(operation: &str, url: &str, local: &std::path::Path) -> String {
    use arrow_array::StringArray;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "operation",
            Arc::new(StringArray::from(vec![operation])) as arrow_array::ArrayRef,
        ),
        ("url", Arc::new(StringArray::from(vec![url]))),
        ("headers", Arc::new(StringArray::from(vec!["{}"]))),
        (
            "local",
            Arc::new(StringArray::from(vec![local.to_str().unwrap()])),
        ),
    ])
    .unwrap();
    let mut body: serde_json::Value =
        serde_json::from_str(&succeeded_inline_body("transfer", &batch)).unwrap();
    body["manifest"]["is_volume_operation"] = json!(true);
    body.to_string()
}

fn pending_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "PENDING" },
    })
    .to_string()
}

fn running_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "RUNNING" },
    })
    .to_string()
}

fn failed_body(statement_id: &str, message: &str, sql_state: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": {
            "state": "FAILED",
            "sql_state": sql_state,
            "error": {
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": format!("{message}. SQLSTATE: {sql_state}"),
            }
        },
    })
    .to_string()
}

/// `FAILED` body with the three SEA extended status fields populated.
/// Pins the async polling path's `Error.display_message` /
/// `diagnostic_info` / `error_details_json` carry — the async
/// equivalent of the sync `failed_body_with_extended_fields`.
fn failed_body_with_extended_fields(
    statement_id: &str,
    message: &str,
    sql_state: &str,
    display_message: &str,
    diagnostic_info: &str,
    error_details_json: &str,
) -> String {
    json!({
        "statement_id": statement_id,
        "status": {
            "state": "FAILED",
            "sql_state": sql_state,
            "error": {
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": format!("{message}. SQLSTATE: {sql_state}"),
            },
            "display_message": display_message,
            "diagnostic_info": diagnostic_info,
            "error_details_json": error_details_json,
        },
    })
    .to_string()
}

/// `CANCELED` body with the three SEA extended status fields
/// populated. Used to pin the async path's
/// `await_result::StatementStatus::Cancelled` arm carrying the
/// extended fields onto the returned `Error`.
fn cancelled_body_with_extended_fields(
    statement_id: &str,
    display_message: &str,
    diagnostic_info: &str,
) -> String {
    json!({
        "statement_id": statement_id,
        "status": {
            "state": "CANCELED",
            "display_message": display_message,
            "diagnostic_info": diagnostic_info,
        },
    })
    .to_string()
}

fn cancelled_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "CANCELED" },
    })
    .to_string()
}

/// `CLOSED` status body — what the SEA server returns from
/// `GetStatementStatus` after the statement has been closed (verified
/// against a live warehouse: `GET` after `DELETE` → 200 `state: CLOSED`).
fn closed_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "CLOSED" },
    })
    .to_string()
}

/// Like [`succeeded_inline_body`] but reports `status.state == CLOSED`.
/// A statement that auto-closed server-side can surface `CLOSED` (with
/// inline results) from a polled `GetStatementStatus` on the async path;
/// the kernel must NOT then issue a redundant `CloseStatement` (DELETE)
/// on it. See issue #168.
fn closed_inline_body(statement_id: &str, batch: &RecordBatch) -> String {
    succeeded_inline_body(statement_id, batch)
        .replace(r#""state":"SUCCEEDED""#, r#""state":"CLOSED""#)
}

/// Poll `server.received_requests()` until `expected` `DELETE`s to the
/// given statement endpoint are observed, or `timeout` elapses; returns
/// the final observed count. Mirrors the sync suite's helper: a negative
/// expectation (`expected == 0`) still gives an erroneously-spawned close
/// the full window to reach the wire but breaks early the instant one
/// appears, so the negative assertion stays reliably able to fail.
async fn wait_for_close_delete(
    server: &MockServer,
    statement_id: &str,
    expected: usize,
    timeout: std::time::Duration,
) -> usize {
    let delete_path = format!("/api/2.0/sql/statements/{statement_id}");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.method == wiremock::http::Method::DELETE && r.url.path() == delete_path)
            .count();
        if count >= expected.max(1) || tokio::time::Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Mount a single `GetStatementStatus` (GET) response for `attach`'s
/// seeding call.
async fn mount_status_once(server: &MockServer, statement_id: &str, body: String) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{statement_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

/// Mount a `404 NOT_FOUND` for an unknown/expired statement id — the
/// SEA shape verified against a live warehouse for a well-formed but
/// unknown id.
async fn mount_status_not_found(server: &MockServer, statement_id: &str) {
    let body = json!({
        "error_code": "NOT_FOUND",
        "message": format!("The statement {statement_id} was not found."),
    })
    .to_string();
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{statement_id}")))
        .respond_with(ResponseTemplate::new(404).set_body_string(body))
        .mount(server)
        .await;
}

async fn mount_submit(server: &MockServer, body: String) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

async fn mount_status_sequence(server: &MockServer, statement_id: &str, bodies: Vec<String>) {
    // Mount each response as `up_to_n_times(1)` so wiremock serves
    // them in declaration order; subsequent matches fall through to
    // the final mount (which has no n-cap).
    let path_str = format!("/api/2.0/sql/statements/{statement_id}");
    let count = bodies.len();
    for (i, body) in bodies.into_iter().enumerate() {
        let mock = Mock::given(method("GET"))
            .and(path(path_str.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_string(body));
        if i + 1 < count {
            mock.up_to_n_times(1).mount(server).await;
        } else {
            mock.mount(server).await;
        }
    }
}

fn int64_batch(name: &str, values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

async fn drain(stream: &mut databricks_sql_kernel::ResultStream) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    while let Some(batch) = stream.next_batch().await.expect("next_batch") {
        let ResultBatch::Arrow(rb) = batch else {
            panic!("expected Arrow batch in v0");
        };
        out.push(rb.clone());
    }
    out
}

// ─── Tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn submit_returns_pending_handle_without_polling() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Server replies PENDING on the submit; submit() must NOT poll.
    mount_submit(&server, pending_body("stmt-async")).await;
    // Subsequent status() call returns PENDING as well.
    mount_status_sequence(&server, "stmt-async", vec![pending_body("stmt-async")]).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt.submit().await.expect("submit");
    assert_eq!(async_exec.statement_id(), "stmt-async");

    let status = async_exec.status().await.expect("status");
    assert!(matches!(status, StatementStatus::Pending), "{status:?}");
}

#[tokio::test]
async fn owning_async_status_advances_session_version_for_next_execute() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .and(body_json(json!({
            "execution_mode": "FAST",
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "session_id": "sess-test",
            "session_version": { "version_id": 7 },
        })))
        .expect(1)
        .mount(&server)
        .await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_json(json!({
            "warehouse_id": "test_wh",
            "statement": "SELECT 1",
            "session_id": "sess-test",
            "session_version": { "version_id": 7 },
            "disposition": "INLINE_OR_EXTERNAL_LINKS",
            "format": "ARROW_STREAM",
            "wait_timeout": "0s",
            "on_wait_timeout": "CONTINUE",
            "result_compression": "LZ4_FRAME",
        })))
        .respond_with(ResponseTemplate::new(200).set_body_string(pending_body("stmt-first")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-first"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-first",
            "status": {
                "state": "SUCCEEDED",
                "session_version": { "version_id": 9 },
            },
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_json(json!({
            "warehouse_id": "test_wh",
            "statement": "SELECT 2",
            "session_id": "sess-test",
            "session_version": { "version_id": 9 },
            "disposition": "INLINE_OR_EXTERNAL_LINKS",
            "format": "ARROW_STREAM",
            "wait_timeout": "0s",
            "on_wait_timeout": "CONTINUE",
            "result_compression": "LZ4_FRAME",
        })))
        .respond_with(ResponseTemplate::new(200).set_body_string(pending_body("stmt-second")))
        .expect(1)
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut first = session.statement();
    first.spec().sql("SELECT 1");
    let first_execution = first.submit().await.expect("first submit");
    assert!(matches!(
        first_execution.status().await.expect("terminal status"),
        StatementStatus::Succeeded
    ));

    let mut second = session.statement();
    second.spec().sql("SELECT 2");
    second.submit().await.expect("second submit");
}

#[tokio::test]
async fn await_result_polls_until_succeeded() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = int64_batch("n", vec![42]);
    mount_submit(&server, pending_body("stmt-poll")).await;
    mount_status_sequence(
        &server,
        "stmt-poll",
        vec![
            running_body("stmt-poll"),
            // Final response includes inline results.
            succeeded_inline_body("stmt-poll", &batch),
        ],
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 42");
    let async_exec = stmt.submit().await.expect("submit");

    let mut stream = async_exec.await_result().await.expect("await_result");
    let batches = drain(&mut stream).await;
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
}

#[tokio::test]
async fn await_result_propagates_failed_status() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_submit(&server, pending_body("stmt-fail")).await;
    mount_status_sequence(
        &server,
        "stmt-fail",
        vec![failed_body(
            "stmt-fail",
            "syntax error near 'INTO'",
            "42601",
        )],
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELEKT 1");
    let async_exec = stmt.submit().await.expect("submit");

    let res = async_exec.await_result().await;
    let err = match res {
        Ok(_) => panic!("await_result on FAILED should error"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::SqlError);
    assert_eq!(err.sql_state.as_deref(), Some("42601"), "{err:?}");
}

#[tokio::test]
async fn await_result_surfaces_cancelled_as_cancelled_code() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_submit(&server, pending_body("stmt-cancel")).await;
    mount_status_sequence(&server, "stmt-cancel", vec![cancelled_body("stmt-cancel")]).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt.submit().await.expect("submit");

    let err = match async_exec.await_result().await {
        Ok(_) => panic!("await_result on CANCELED should error"),
        Err(e) => e,
    };
    // A server-side CANCELLED is an operational/cancelled condition,
    // not a caller-argument error — `Error::cancelled()` (ErrorCode::
    // Cancelled). The connector maps this to OperationalError, not the
    // misleading ProgrammingError that InvalidArgument would yield.
    assert_eq!(err.code, ErrorCode::Cancelled);
    assert!(err.message.contains("cancel"), "{}", err.message);
}

#[tokio::test]
async fn fetch_result_rejects_non_terminal() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_submit(&server, pending_body("stmt-running")).await;
    // status() returns RUNNING. fetch_result() must surface
    // InvalidState — it does not poll on its own.
    mount_status_sequence(&server, "stmt-running", vec![running_body("stmt-running")]).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt.submit().await.expect("submit");

    let err = match async_exec.fetch_result().await {
        Ok(_) => panic!("fetch_result on RUNNING should error"),
        Err(e) => e,
    };
    // `Error::invalid_state()` currently maps to InvalidArgument
    // until the ErrorCode taxonomy expansion (spec A8) ships
    // `FailedPrecondition` / `InvalidState`.
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("status"), "{}", err.message);
}

#[tokio::test]
async fn fetch_result_after_succeeded_returns_stream() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = int64_batch("n", vec![7]);
    mount_submit(&server, pending_body("stmt-done")).await;
    mount_status_sequence(
        &server,
        "stmt-done",
        vec![succeeded_inline_body("stmt-done", &batch)],
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 7");
    let async_exec = stmt.submit().await.expect("submit");

    let mut stream = async_exec.fetch_result().await.expect("fetch_result");
    let batches = drain(&mut stream).await;
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        7
    );
}

#[tokio::test]
async fn re_submit_invalidates_prior_async_handle() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    // Stub cancel for both statement ids.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-X/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-Y/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(pending_body("stmt-X")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(pending_body("stmt-Y")))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec_x = stmt.submit().await.expect("submit X");
    assert_eq!(async_exec_x.statement_id(), "stmt-X");
    let reader = async_exec_x.reader().expect("reader");

    // Re-submit invalidates async_exec_x.
    let _async_exec_y = stmt.submit().await.expect("submit Y");
    assert_eq!(
        async_exec_x.try_num_modified_rows().unwrap_err().code,
        ErrorCode::InvalidStatementHandle
    );
    assert_eq!(
        reader.num_modified_rows().unwrap_err().code,
        ErrorCode::InvalidStatementHandle
    );
    let err = async_exec_x
        .cancel()
        .await
        .expect_err("stale async handle cancel must fail");
    assert_eq!(err.code, ErrorCode::InvalidStatementHandle);
    assert_eq!(
        reader.status().await.unwrap_err().code,
        ErrorCode::InvalidStatementHandle
    );
    assert_eq!(
        reader.await_result().await.err().unwrap().code,
        ErrorCode::InvalidStatementHandle
    );
    assert_eq!(
        reader.fetch_result().await.err().unwrap().code,
        ErrorCode::InvalidStatementHandle
    );
    assert_eq!(
        async_exec_x.reader().err().unwrap().code,
        ErrorCode::InvalidStatementHandle
    );
}

#[tokio::test]
async fn executed_async_close_is_idempotent() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_submit(&server, pending_body("stmt-close-async")).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt.submit().await.unwrap();
    async_exec.close().await.expect("first close");
    // Second close short-circuits without hitting the wire.
    async_exec.close().await.expect("second close — idempotent");
}

#[tokio::test]
async fn submit_validates_missing_sql() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    let err = match stmt.submit().await {
        Ok(_) => panic!("submit without sql must fail"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("sql"), "{}", err.message);
}

#[tokio::test]
async fn submit_rejects_binary_param_with_unhex_workaround_message() {
    // Same client-side rejection as Statement::execute — Binary is
    // not a SEA-accepted parameter type. Verifies submit shares the
    // spec-validation path.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT ?")
        .param(1, TypedValue::Binary(vec![0xCA, 0xFE]))
        .unwrap();
    let err = match stmt.submit().await {
        Ok(_) => panic!("Binary must be rejected before wire"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("unhex"), "{}", err.message);
}

#[tokio::test]
async fn await_result_drop_fires_cancel_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // The server keeps reporting RUNNING — `await_result` will loop
    // forever, so the test drops the future via tokio::time::timeout
    // and asserts that the drop fires `POST /statements/{id}/cancel`.
    mount_submit(&server, pending_body("stmt-cancel-on-drop")).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-cancel-on-drop"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(running_body("stmt-cancel-on-drop")),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-cancel-on-drop/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt.submit().await.expect("submit");

    // Drop the await_result future via timeout. We expect timeout
    // to win — the server stays in RUNNING forever per the mock.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        async_exec.await_result(),
    )
    .await;
    assert!(outcome.is_err(), "timeout should fire");

    // Give the spawned fire-and-forget cancel a moment to land.
    // 200ms is generous; the cancel task is a single POST on a
    // local mock.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Query wiremock for the recorded requests on the cancel
    // endpoint.
    let cancel_calls = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|req| {
            req.method == "POST"
                && req
                    .url
                    .path()
                    .ends_with("/statements/stmt-cancel-on-drop/cancel")
        })
        .count();
    assert!(
        cancel_calls >= 1,
        "await_result drop should fire at least one cancel_statement; got {cancel_calls}"
    );
}

/// `await_result()` must enforce the client query timeout on its own
/// poll loop (not just the `status()` path). Submitting with a short
/// deadline against an always-RUNNING server drives one of the loop's
/// deadline exits (pre-poll check / mid-poll `timeout_at` / terminal
/// `Pending|Running` arm), each of which defuses the cancel guard and
/// returns `expire_client_query_timeout()`. We assert the owning path
/// surfaces `ErrorCode::Timeout` / `HYT00` carrying the query id, and
/// that the guard still fired a best-effort server-side cancel on exit.
#[tokio::test]
async fn await_result_enforces_client_query_timeout_against_running_server() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Server never leaves RUNNING, so the only way out of the
    // `await_result` loop is the client deadline expiring.
    mount_submit(&server, pending_body("stmt-await-timeout")).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-await-timeout"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(running_body("stmt-await-timeout")),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-await-timeout/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt
        .submit_with_timeout(std::time::Duration::from_millis(25))
        .await
        .expect("submit");

    let err = match async_exec.await_result().await {
        Ok(_) => panic!("await_result against an always-RUNNING server must time out"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::Timeout);
    assert_eq!(err.sql_state.as_deref(), Some("HYT00"));
    assert_eq!(
        err.query_id.as_deref(),
        Some("stmt-await-timeout"),
        "client query timeout must carry the observed statement id"
    );

    // The owning handle defuses the drop-cancel guard on timeout and
    // fires a best-effort server-side cancel so the warehouse stops
    // charging DBUs on the now-abandoned query.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let cancel_calls = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|req| {
            req.method == wiremock::http::Method::POST
                && req
                    .url
                    .path()
                    .ends_with("/statements/stmt-await-timeout/cancel")
        })
        .count();
    assert!(
        cancel_calls >= 1,
        "await_result timeout should fire a best-effort cancel_statement; got {cancel_calls}"
    );
}

/// Companion to `await_result_enforces_client_query_timeout_against_running_server`,
/// which (with an instant-responding mock) only drives the loop-level deadline
/// checks — the pre-poll check and the terminal-arm `Pending|Running` check.
/// This test pins the THIRD, distinct exit: the mid-poll `tokio::time::timeout_at`
/// RPC-expiry branch, which fires only when the status RPC itself hangs past
/// `deadline + CLIENT_QUERY_TIMEOUT_STATUS_GRACE` (5s). We give the client a
/// deadline comfortably longer than the first pre-poll evaluation (so the loop
/// enters the RPC rather than bailing early) and make the `GetStatementStatus`
/// GET hang past the grace window, so `timeout_at` wins. As on the other exits,
/// the owning handle must defuse the cancel guard and surface
/// `ErrorCode::Timeout` / `HYT00` carrying the statement id.
#[tokio::test]
async fn await_result_client_query_timeout_fires_on_hung_status_poll() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Submit returns PENDING instantly, but the status GET hangs for
    // 8s — longer than `deadline + CLIENT_QUERY_TIMEOUT_STATUS_GRACE`
    // (~0.5s + 5s) — so the only way out of the loop is the mid-poll
    // `timeout_at` RPC-expiry branch, NOT the pre-poll / terminal-arm
    // checks.
    mount_submit(&server, pending_body("stmt-hung-poll")).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-hung-poll"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(running_body("stmt-hung-poll"))
                .set_delay(std::time::Duration::from_secs(8)),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-hung-poll/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    // 500ms deadline: long enough that the first pre-poll check passes
    // and we enter the (hung) status RPC, short enough that the test's
    // bound is `deadline + grace` ≈ 5.5s, not the full 8s GET delay.
    let async_exec = stmt
        .submit_with_timeout(std::time::Duration::from_millis(500))
        .await
        .expect("submit");

    let err = match async_exec.await_result().await {
        Ok(_) => panic!("await_result must time out when the status poll hangs past the deadline"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::Timeout);
    assert_eq!(err.sql_state.as_deref(), Some("HYT00"));
    assert_eq!(
        err.query_id.as_deref(),
        Some("stmt-hung-poll"),
        "client query timeout must carry the observed statement id"
    );

    // The mid-poll exit defuses the drop-cancel guard and fires a
    // best-effort server-side cancel, same as the other two exits.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let cancel_calls = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|req| {
            req.method == wiremock::http::Method::POST
                && req
                    .url
                    .path()
                    .ends_with("/statements/stmt-hung-poll/cancel")
        })
        .count();
    assert!(
        cancel_calls >= 1,
        "mid-poll timeout_at exit should fire a best-effort cancel_statement; got {cancel_calls}"
    );
}

/// Companion to `await_result_client_query_timeout_fires_on_hung_status_poll`,
/// pinning the OTHER `tokio::time::timeout_at` exit on the submit path: the
/// mid-*submit* deadline-expiry branch, where the `POST /statements` RPC itself
/// hangs past the deadline. This branch has distinct observable behavior from
/// the status-poll exit: because `cancel_state.observe` runs only after `submit`
/// resolves, a timeout before that leaves the statement id unknown, so the
/// surfaced `Timeout`/`HYT00` error carries a `None` query id and NO server-side
/// cancel/close is scheduled (there is no id to target). The same `set_delay`
/// technique as the status-poll test covers it — delay the `POST /statements`
/// past the deadline so `timeout_at` wins.
#[tokio::test]
async fn submit_with_timeout_client_query_timeout_fires_on_hung_submit() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // The submit POST hangs for 8s — far longer than the 300ms deadline —
    // so the only way out is the mid-submit `timeout_at` RPC-expiry branch.
    // The response body never matters because the deadline fires first.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(pending_body("stmt-hung-submit"))
                .set_delay(std::time::Duration::from_secs(8)),
        )
        .mount(&server)
        .await;
    // Mount a cancel endpoint so that, if the kernel erroneously tried to
    // cancel, the request would be recorded (and the negative assertion
    // below would catch it).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-hung-submit/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let err = match stmt
        .submit_with_timeout(std::time::Duration::from_millis(300))
        .await
    {
        Ok(_) => panic!("submit_with_timeout must time out when the submit POST hangs"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::Timeout);
    assert_eq!(err.sql_state.as_deref(), Some("HYT00"));
    // No statement id was observed before the deadline fired, so the error
    // carries no query id — distinct from the status-poll exit.
    assert_eq!(
        err.query_id, None,
        "mid-submit timeout cannot know the server statement id, so query_id must be None"
    );

    // Because the id is unknown, no best-effort cancel/close can be scheduled.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let cancel_calls = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|req| {
            req.method == wiremock::http::Method::POST && req.url.path().ends_with("/cancel")
        })
        .count();
    assert_eq!(
        cancel_calls, 0,
        "mid-submit timeout has no id to target, so no cancel_statement should fire; got {cancel_calls}"
    );
}

// ─── Extended SEA status fields on the async handle (PR #23) ────────
//
// The accessors `num_modified_rows()` / `display_message()` /
// `diagnostic_info()` / `error_details_json()` on
// `ExecutedAsyncStatement` reflect the most recent
// `StatementStatus` this handle has observed (initial submit
// response, or any subsequent `status()` / `await_result` poll).
// The tests below pin three load-bearing properties:
//
//   1. After `await_result()` resolves to terminal SUCCEEDED, the
//      accessors return the TERMINAL poll's extended fields, not the
//      initial PENDING / RUNNING ones.
//   2. When `submit()` returns immediately with an already-terminal
//      response (server completed the query during the wait window),
//      the accessors expose those fields even without the caller
//      driving a single `status()` poll.
//   3. A `status()` call updates the cached snapshot — a later
//      accessor reflects that poll, not the initial-submit response.

fn succeeded_inline_body_with_status(
    statement_id: &str,
    batch: &RecordBatch,
    num_modified_rows: Option<i64>,
    display_message: Option<&str>,
    diagnostic_info: Option<&str>,
    error_details_json: Option<&str>,
) -> String {
    let ipc = arrow_ipc_bytes(batch);
    let attachment = STANDARD.encode(&ipc);
    let columns: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            json!({
                "name": f.name(),
                "type_name": match f.data_type() {
                    DataType::Int64 => "BIGINT",
                    _ => "STRING",
                },
                "type_text": match f.data_type() {
                    DataType::Int64 => "BIGINT",
                    _ => "STRING",
                },
                "position": i as i32,
            })
        })
        .collect();

    let mut status = json!({ "state": "SUCCEEDED" });
    let status_obj = status.as_object_mut().unwrap();
    if let Some(n) = num_modified_rows {
        status_obj.insert("num_modified_rows".to_string(), json!(n));
    }
    if let Some(m) = display_message {
        status_obj.insert("display_message".to_string(), json!(m));
    }
    if let Some(d) = diagnostic_info {
        status_obj.insert("diagnostic_info".to_string(), json!(d));
    }
    if let Some(e) = error_details_json {
        status_obj.insert("error_details_json".to_string(), json!(e));
    }

    json!({
        "statement_id": statement_id,
        "status": status,
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": batch.num_columns() as i32,
                "columns": columns,
            },
            "total_chunk_count": 1,
            "total_row_count": batch.num_rows() as i64,
            "total_byte_count": ipc.len() as i64,
            "truncated": false,
        },
        "result": {
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": batch.num_rows() as i64,
            "byte_count": ipc.len() as i64,
            "attachment": attachment,
        },
    })
    .to_string()
}

#[tokio::test]
async fn async_await_result_caches_terminal_status_for_accessors() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Initial submit returns PENDING — no extended fields.
    mount_submit(&server, pending_body("stmt-async-status")).await;

    // First poll RUNNING (no fields), then terminal SUCCEEDED with
    // all extended fields populated. The handle must reflect the
    // terminal poll's values, not the initial PENDING snapshot.
    let batch = int64_batch("n", vec![1]);
    mount_status_sequence(
        &server,
        "stmt-async-status",
        vec![
            running_body("stmt-async-status"),
            succeeded_inline_body_with_status(
                "stmt-async-status",
                &batch,
                Some(17),
                Some("All good (terminal)"),
                Some("operator=Scan; stage=Execute"),
                Some(r#"{"errorClass":"NONE"}"#),
            ),
        ],
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("UPDATE t SET x = 1");
    let async_exec = stmt.submit().await.expect("submit");

    // Before any poll: accessors reflect the initial PENDING
    // submit response, which had no extended fields.
    assert_eq!(async_exec.num_modified_rows(), None);
    assert_eq!(async_exec.display_message(), None);

    // Drive polling to completion. The poll loop overwrites the
    // cached status on every wire response — the terminal poll's
    // extended fields must now be visible.
    let _stream = async_exec
        .await_result()
        .await
        .expect("await_result should reach SUCCEEDED");

    assert_eq!(async_exec.num_modified_rows(), Some(17));
    assert_eq!(
        async_exec.display_message().as_deref(),
        Some("All good (terminal)")
    );
    assert_eq!(
        async_exec.diagnostic_info().as_deref(),
        Some("operator=Scan; stage=Execute")
    );
    assert_eq!(
        async_exec.error_details_json().as_deref(),
        Some(r#"{"errorClass":"NONE"}"#)
    );

    async_exec.close().await.expect("close");
}

#[tokio::test]
async fn async_submit_caches_initial_terminal_status() {
    // If the server completes the query inside the
    // `submit_statement` request (returns SUCCEEDED on the initial
    // response with all fields populated), the accessors expose
    // those values immediately — no poll needed. This matches
    // Thrift's behaviour where `executeStatement` with direct
    // results already carries the operation status fields.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = int64_batch("n", vec![1]);
    mount_submit(
        &server,
        succeeded_inline_body_with_status(
            "stmt-async-fast",
            &batch,
            Some(5),
            Some("Done at submit time"),
            None,
            None,
        ),
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("UPDATE t SET x = 1");
    let async_exec = stmt.submit().await.expect("submit");

    // Without a single poll, accessors must already expose what the
    // initial submit response carried.
    assert_eq!(async_exec.num_modified_rows(), Some(5));
    assert_eq!(
        async_exec.display_message().as_deref(),
        Some("Done at submit time")
    );
    assert_eq!(async_exec.diagnostic_info(), None);
    assert_eq!(async_exec.error_details_json(), None);

    async_exec.close().await.expect("close");
}

#[tokio::test]
async fn async_status_call_refreshes_cached_status() {
    // `status()` must refresh the cached snapshot used by the
    // extended-field accessors, not just return the enum.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Submit returns PENDING (no extended fields).
    mount_submit(&server, pending_body("stmt-async-refresh")).await;

    // A status() poll returns SUCCEEDED with extended fields. The
    // status() call itself must overwrite the cached snapshot.
    let batch = int64_batch("n", vec![1]);
    mount_status_sequence(
        &server,
        "stmt-async-refresh",
        vec![succeeded_inline_body_with_status(
            "stmt-async-refresh",
            &batch,
            Some(99),
            Some("Refreshed via status()"),
            None,
            None,
        )],
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("UPDATE t SET x = 1");
    let async_exec = stmt.submit().await.expect("submit");

    // Before status(): accessors reflect PENDING (no fields).
    assert_eq!(async_exec.num_modified_rows(), None);

    let s = async_exec.status().await.expect("status");
    // The terminal status arm of the enum.
    assert!(
        matches!(
            s,
            databricks_sql_kernel::StatementStatus::Succeeded
                | databricks_sql_kernel::StatementStatus::Closed
        ),
        "expected Succeeded/Closed enum variant, got {s:?}"
    );

    // After status(): accessors reflect the status() poll's values.
    assert_eq!(async_exec.num_modified_rows(), Some(99));
    assert_eq!(
        async_exec.display_message().as_deref(),
        Some("Refreshed via status()")
    );

    async_exec.close().await.expect("close");
}

#[tokio::test]
async fn async_await_result_failed_carries_extended_status_fields_onto_error() {
    // Pins the async polling path's terminal-error contract,
    // companion to the sync-path `execute_failed_carries_extended_status_fields_onto_error`
    // in `tests/v0_execute_wiremock.rs`. Regression guard for the
    // `executed_async.rs::from_sea_response::Failed` carry. Prior to
    // the carry being added, the kernel `Error` returned from
    // `await_result()` would have NONE of these three fields populated
    // even when the SEA wire response did — silently breaking the
    // Thrift-parity contract documented at `napi/src/error.rs`.
    use databricks_sql_kernel::ErrorCode;
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_submit(&server, pending_body("stmt-async-fail-ext")).await;
    mount_status_sequence(
        &server,
        "stmt-async-fail-ext",
        vec![failed_body_with_extended_fields(
            "stmt-async-fail-ext",
            "table not found",
            "42P01",
            "User-facing reason from server",
            "operator=Scan; stage=Analyze; full stack ...",
            r#"{"errorClass":"TABLE_OR_VIEW_NOT_FOUND"}"#,
        )],
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT * FROM does_not_exist");
    let async_exec = stmt.submit().await.expect("submit");

    let err = match async_exec.await_result().await {
        Ok(_) => panic!("await_result on FAILED should error"),
        Err(e) => e,
    };

    assert_eq!(err.code, ErrorCode::SqlError);
    assert_eq!(err.sql_state.as_deref(), Some("42P01"));
    assert_eq!(
        err.display_message.as_deref(),
        Some("User-facing reason from server"),
        "async await_result must carry display_message from FAILED wire envelope (Thrift-parity contract)"
    );
    assert_eq!(
        err.diagnostic_info.as_deref(),
        Some("operator=Scan; stage=Analyze; full stack ...")
    );
    assert_eq!(
        err.error_details_json.as_deref(),
        Some(r#"{"errorClass":"TABLE_OR_VIEW_NOT_FOUND"}"#)
    );
}

#[tokio::test]
async fn async_await_result_cancelled_carries_extended_status_fields_onto_error() {
    // Pins the Cancelled arm of `await_result`. The server can
    // include `display_message` (e.g. "Statement was canceled by the
    // user") on a CANCELED response; the kernel must carry it onto
    // the returned Error. Regression guard for the
    // `executed_async.rs::await_result` Cancelled-arm carry.
    use databricks_sql_kernel::ErrorCode;
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_submit(&server, pending_body("stmt-async-cancel-ext")).await;
    mount_status_sequence(
        &server,
        "stmt-async-cancel-ext",
        vec![cancelled_body_with_extended_fields(
            "stmt-async-cancel-ext",
            "Statement was canceled by the user",
            "operator=Scan; cancelled-mid-execute",
        )],
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt.submit().await.expect("submit");

    let err = match async_exec.await_result().await {
        Ok(_) => panic!("await_result on CANCELED should error"),
        Err(e) => e,
    };
    // A server-side CANCELLED surfaces as ErrorCode::Cancelled (the
    // connector maps it to OperationalError, not ProgrammingError).
    assert_eq!(err.code, ErrorCode::Cancelled);
    assert!(
        err.message.contains("cancelled"),
        "message should mention cancellation, got: {}",
        err.message
    );
    assert_eq!(
        err.display_message.as_deref(),
        Some("Statement was canceled by the user"),
        "Cancelled arm must carry display_message from CANCELED wire envelope"
    );
    assert_eq!(
        err.diagnostic_info.as_deref(),
        Some("operator=Scan; cancelled-mid-execute")
    );
    let _ = ErrorCode::SqlError; // silence unused-import warning if the assert above changes
}

#[tokio::test]
async fn async_readers_preserve_session_routes() {
    use wiremock::matchers::{body_partial_json, query_param};

    for (realtime, cloudfetch) in [(false, false), (false, true), (true, false), (true, true)] {
        let server = MockServer::start().await;
        let session_prefix = if realtime {
            "/api/2.0/sql/warehouses/test_wh/sessions/sess-test"
        } else {
            "/api/2.0/sql"
        };
        let mut create_response =
            ResponseTemplate::new(200).set_body_json(json!({ "session_id": "sess-test" }));
        if realtime {
            create_response = create_response.insert_header("x-databricks-rt-session", "true");
        }
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
            .respond_with(create_response)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("{session_prefix}/statements")))
            .and(body_partial_json(json!({ "wait_timeout": "0s" })))
            .respond_with(ResponseTemplate::new(200).set_body_string(pending_body("stmt-result")))
            .expect(1)
            .mount(&server)
            .await;

        let batches = [int64_batch("n", vec![1, 2]), int64_batch("n", vec![3, 4])];
        let mut response: serde_json::Value =
            serde_json::from_str(&succeeded_inline_body("stmt-result", &batches[0])).unwrap();
        response["manifest"]["total_chunk_count"] = json!(2);
        response["manifest"]["total_row_count"] = json!(4);
        response["result"]["next_chunk_index"] = json!(1);
        let mut continuation = json!({
            "chunk_index": 1,
            "row_offset": 2,
            "row_count": 2,
            "attachment": STANDARD.encode(arrow_ipc_bytes(&batches[1])),
        });
        if cloudfetch {
            let mut links = Vec::new();
            for (index, batch) in batches.iter().enumerate() {
                let bytes = arrow_ipc_bytes(batch);
                let chunk_path = format!("/cloud-fetch/chunk-{index}.arrow");
                links.push(json!({
                    "external_link": format!("{}{chunk_path}", server.uri()),
                    "expiration": "2099-01-01T00:00:00.000Z",
                    "chunk_index": index,
                    "row_offset": index * 2,
                    "row_count": 2,
                    "byte_count": bytes.len(),
                    "next_chunk_index": if index == 0 { Some(1) } else { None },
                }));
                Mock::given(method("GET"))
                    .and(path(chunk_path))
                    .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
                    .expect(3)
                    .mount(&server)
                    .await;
            }
            Mock::given(method("GET"))
                .and(path(format!(
                    "{session_prefix}/statements/stmt-result/result/chunks/0"
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "external_links": [links[0].clone()],
                })))
                .expect(3)
                .mount(&server)
                .await;
            let mut expired_link = links[0].clone();
            expired_link["external_link"] = json!(format!("{}/expired-link", server.uri()));
            expired_link["expiration"] = json!("2000-01-01T00:00:00.000Z");
            response["result"] = json!({ "external_links": [expired_link] });
            continuation = json!({ "external_links": [links[1].clone()] });
        }
        Mock::given(method("GET"))
            .and(path(format!("{session_prefix}/statements/stmt-result")))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response))
            .expect(7)
            .mount(&server)
            .await;
        let chunk_mock = Mock::given(method("GET")).and(path(format!(
            "{session_prefix}/statements/stmt-result/result/chunks/1"
        )));
        let chunk_mock = if cloudfetch {
            chunk_mock
        } else {
            chunk_mock.and(query_param("row_offset", "2"))
        };
        chunk_mock
            .respond_with(ResponseTemplate::new(200).set_body_json(&continuation))
            .expect(1..)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(format!("{session_prefix}/statements/stmt-result")))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(if realtime {
                session_prefix.to_string()
            } else {
                format!("{session_prefix}/sessions/sess-test")
            }))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let session = open_session(&server).await;
        let mut statement = session.statement();
        statement.spec().sql("SELECT n FROM range(1, 5) AS t(n)");
        let owned = statement.submit().await.expect("owned submit");
        let owned_stream = owned.await_result().await.expect("owned result");
        let reader = owned.reader().expect("reader");
        let refetch = owned.reader().expect("refetch reader");
        assert_eq!(reader.statement_id(), owned.statement_id());
        let (reader_status, owned_status) = tokio::join!(reader.status(), owned.status());
        assert!(matches!(reader_status.unwrap(), StatementStatus::Succeeded));
        assert!(matches!(owned_status.unwrap(), StatementStatus::Succeeded));
        let (read_stream, refetch_stream) =
            tokio::join!(reader.await_result(), refetch.fetch_result());
        for mut stream in [owned_stream, read_stream.unwrap(), refetch_stream.unwrap()] {
            let fetched = drain(&mut stream).await;
            assert_eq!(fetched.len(), 2);
            let values: Vec<i64> = fetched
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<arrow_array::types::Int64Type>()
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            assert_eq!(values, vec![1, 2, 3, 4]);
        }
        drop(reader);
        drop(refetch);
        owned.close().await.expect("owned close");
        session.close().await.expect("session close");
        server.verify().await;
    }
}

#[tokio::test]
async fn owner_release_invalidates_readers() {
    use std::time::Duration;
    use tokio::sync::Notify;

    for close_owner in [true, false] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_submit(&server, pending_body("stmt-reader")).await;
        let polled = Arc::new(Notify::new());
        let notify = Arc::clone(&polled);
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-reader"))
            .respond_with(move |_: &wiremock::Request| {
                notify.notify_one();
                ResponseTemplate::new(200).set_body_string(running_body("stmt-reader"))
            })
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-reader/cancel"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/statements/stmt-reader"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let session = open_session(&server).await;
        let mut statement = session.statement();
        statement.spec().sql("SELECT 1");
        let mut owner = Some(statement.submit().await.unwrap());
        let reader = owner.as_ref().unwrap().reader().unwrap();
        let release_owner = async {
            polled.notified().await;
            if close_owner {
                let owner = owner.as_mut().unwrap();
                owner.close().await.unwrap();
                assert_eq!(
                    owner.reader().err().unwrap().code,
                    ErrorCode::InvalidStatementHandle
                );
            } else {
                drop(owner.take());
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(reader.await_result(), release_owner)
        })
        .await
        .expect("reader stops polling after owner release");
        assert_eq!(
            result.err().unwrap().code,
            ErrorCode::InvalidStatementHandle
        );
        assert_eq!(
            reader.num_modified_rows().unwrap_err().code,
            ErrorCode::InvalidStatementHandle
        );
        for result in [
            reader.status().await.map(|_| ()),
            reader.await_result().await.map(|_| ()),
            reader.fetch_result().await.map(|_| ()),
        ] {
            assert_eq!(result.unwrap_err().code, ErrorCode::InvalidStatementHandle);
        }
        assert_eq!(
            wait_for_close_delete(&server, "stmt-reader", 1, Duration::from_secs(5)).await,
            1
        );
        statement.close().await.unwrap();
        session.close().await.unwrap();
        server.verify().await;
    }
}

#[tokio::test]
async fn owner_query_timeout_invalidates_readers() {
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_submit(&server, pending_body("stmt-reader-timeout")).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-reader-timeout"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(closed_body("stmt-reader-timeout")),
        )
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-reader-timeout/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut statement = session.statement();
    statement.spec().sql("SELECT 1");
    let owner = statement
        .submit_with_timeout(Duration::from_secs(1))
        .await
        .unwrap();
    let reader = owner.reader().unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;
    let error = owner.status().await.unwrap_err();
    assert_eq!(error.code, ErrorCode::Timeout);
    assert_eq!(error.sql_state.as_deref(), Some("HYT00"));
    assert_eq!(
        owner.reader().err().unwrap().code,
        ErrorCode::InvalidStatementHandle
    );
    for result in [
        reader.status().await.map(|_| ()),
        reader.await_result().await.map(|_| ()),
        reader.fetch_result().await.map(|_| ()),
    ] {
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidStatementHandle);
    }
    let error = owner.await_result().await.err().unwrap();
    assert_eq!(error.code, ErrorCode::Timeout);
    assert_eq!(error.sql_state.as_deref(), Some("HYT00"));
    assert_eq!(
        wait_for_close_delete(&server, "stmt-reader-timeout", 1, Duration::from_secs(5)).await,
        1
    );

    owner.close().await.unwrap();
    statement.close().await.unwrap();
    session.close().await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn reader_timeout_and_drop_leave_statement_owned() {
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_submit(&server, pending_body("stmt-reader")).await;
    mount_status_once(&server, "stmt-reader", running_body("stmt-reader")).await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-reader/cancel"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut statement = session.statement();
    statement.spec().sql("SELECT 1");
    let owned = statement
        .submit_with_timeout(Duration::from_secs(1))
        .await
        .unwrap();
    let reader = owned.reader().unwrap();
    let no_close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-reader"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(matches!(
        reader.status().await.unwrap(),
        StatementStatus::Running
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), reader.await_result())
            .await
            .is_err()
    );
    drop(reader);
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(no_close);

    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-reader"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    owned.close().await.unwrap();
    session.close().await.unwrap();
    server.verify().await;
}

// ─── attach_async_statement (resume-by-id) ──────────────────────────

#[tokio::test]
async fn attach_seeds_status_and_reports_server_state() {
    // attach issues one GetStatementStatus to seed the handle; the
    // seeded state is whatever the server reports for the id.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_status_once(&server, "stmt-attach", running_body("stmt-attach")).await;

    let session = open_session(&server).await;
    let async_exec = session
        .attach_async_statement("stmt-attach")
        .await
        .expect("attach should succeed for a known id");
    assert_eq!(async_exec.statement_id(), "stmt-attach");

    let status = async_exec.status().await.expect("status");
    assert!(matches!(status, StatementStatus::Running), "{status:?}");
}

#[tokio::test]
async fn attached_statement_inline_reads_use_standard_urls() {
    use wiremock::matchers::query_param;

    for realtime in [true, false] {
        let server = MockServer::start().await;
        let mut create_response =
            ResponseTemplate::new(200).set_body_json(json!({ "session_id": "sess-test" }));
        if realtime {
            create_response = create_response.insert_header("x-databricks-rt-session", "true");
        }
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
            .respond_with(create_response)
            .expect(1)
            .mount(&server)
            .await;

        let session_prefix = if realtime {
            "/api/2.0/sql/warehouses/test_wh/sessions/sess-test"
        } else {
            "/api/2.0/sql"
        };
        let first_batch = int64_batch("n", vec![1, 2]);
        let last_batch = int64_batch("n", vec![3, 4]);
        let mut status: serde_json::Value =
            serde_json::from_str(&succeeded_inline_body("stmt-attach", &first_batch)).unwrap();
        status["manifest"]["total_chunk_count"] = json!(2);
        status["manifest"]["total_row_count"] = json!(4);
        status["result"]["next_chunk_index"] = json!(1);
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-attach"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status))
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-attach/result/chunks/1"))
            .and(query_param("row_offset", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "chunk_index": 1,
                "row_offset": 2,
                "row_count": 2,
                "attachment": STANDARD.encode(arrow_ipc_bytes(&last_batch)),
            })))
            .expect(1)
            .mount(&server)
            .await;
        let close_path = if realtime {
            session_prefix.to_string()
        } else {
            format!("{session_prefix}/sessions/sess-test")
        };
        Mock::given(method("DELETE"))
            .and(path(close_path))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("{session_prefix}/statements")))
            .respond_with(ResponseTemplate::new(200).set_body_string(pending_body("stmt-owned")))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("{session_prefix}/statements/stmt-owned")))
            .respond_with(ResponseTemplate::new(200).set_body_string(running_body("stmt-owned")))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(format!("{session_prefix}/statements/stmt-owned")))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let session = open_session(&server).await;
        let attached = session
            .attach_async_statement("stmt-attach")
            .await
            .expect("attach");
        let reader = attached.reader().expect("reader");
        assert!(matches!(
            reader.status().await.expect("status"),
            StatementStatus::Succeeded
        ));
        let mut stream = reader.fetch_result().await.expect("fetch_result");
        let batches = drain(&mut stream).await;
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, vec![1, 2, 3, 4]);
        let mut statement = session.statement();
        statement.spec().sql("SELECT 5");
        let owned = statement.submit().await.expect("owned submit");
        assert!(matches!(
            owned.status().await.expect("owned status"),
            StatementStatus::Running
        ));
        owned.close().await.expect("owned close");
        session.close().await.expect("session close");
        server.verify().await;
    }
}

#[tokio::test]
async fn attached_cloudfetch_pagination_and_refresh_use_standard_urls() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-databricks-rt-session", "true")
                .set_body_json(json!({ "session_id": "session-b" })),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions/session-b"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let batches = [int64_batch("n", vec![1, 2]), int64_batch("n", vec![3, 4])];
    let mut links = Vec::new();
    for (index, batch) in batches.iter().enumerate() {
        let bytes = arrow_ipc_bytes(batch);
        let chunk_path = format!("/cloud-fetch/chunk-{index}.arrow");
        links.push(json!({
            "external_link": format!("{}{chunk_path}", server.uri()),
            "expiration": "2099-01-01T00:00:00.000Z",
            "chunk_index": index,
            "row_offset": index * 2,
            "row_count": 2,
            "byte_count": bytes.len(),
            "next_chunk_index": if index == 0 { Some(1) } else { None },
        }));
        Mock::given(method("GET"))
            .and(path(chunk_path))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/2.0/sql/statements/statement-a/result/chunks/{index}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "external_links": [links[index].clone()],
            })))
            .expect(1..)
            .mount(&server)
            .await;
    }
    let mut expired_link = links[0].clone();
    expired_link["external_link"] = json!(format!("{}/expired-link", server.uri()));
    expired_link["expiration"] = json!("2000-01-01T00:00:00.000Z");
    let mut response: serde_json::Value =
        serde_json::from_str(&succeeded_inline_body("statement-a", &batches[0])).unwrap();
    response["manifest"]["total_chunk_count"] = json!(2);
    response["manifest"]["total_row_count"] = json!(4);
    response["result"] = json!({ "external_links": [expired_link] });
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/statement-a"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(2)
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let attached = session
        .attach_async_statement("statement-a")
        .await
        .expect("attach");
    let reader = attached.reader().expect("reader");
    drop(attached);
    let mut stream = reader.fetch_result().await.expect("fetch_result");
    let fetched = drain(&mut stream).await;
    let values: Vec<i64> = fetched
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![1, 2, 3, 4]);
    session.close().await.expect("session close");
    server.verify().await;
}

#[tokio::test]
async fn attach_reports_closed_for_a_closed_statement() {
    // The connector relies on this: a closed async command must report
    // CLOSED from the *server*, not from connector-side bookkeeping.
    // Verified against a live warehouse — GET after DELETE returns
    // 200 state=CLOSED.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_status_once(&server, "stmt-closed", closed_body("stmt-closed")).await;

    let session = open_session(&server).await;
    let async_exec = session
        .attach_async_statement("stmt-closed")
        .await
        .expect("attach should succeed for a closed (but still known) id");

    let status = async_exec.status().await.expect("status");
    assert!(matches!(status, StatementStatus::Closed), "{status:?}");
}

#[tokio::test]
async fn attach_errors_not_found_for_unknown_id() {
    // An unknown / TTL-expired id returns 404 NOT_FOUND, which the
    // transport maps to ErrorCode::NotFound. The connector keys its
    // "command is gone → CLOSED" decision on this specific code, so
    // assert the mapping holds (and is not, e.g., a generic SqlError).
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_status_not_found(&server, "stmt-gone").await;

    let session = open_session(&server).await;
    let err = match session.attach_async_statement("stmt-gone").await {
        Ok(_) => panic!("attach on an unknown id should error"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::NotFound, "got: {err:?}");
}

#[tokio::test]
async fn attached_handle_drop_does_not_close_statement() {
    // An attached handle is non-owning: dropping it must NOT fire a
    // CloseStatement. This is load-bearing — get_query_state re-attaches
    // on every poll, so a self-closing attached handle would close the
    // statement the poll loop is watching. Assert zero DELETEs reach the
    // server after the handle drops.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_status_once(&server, "stmt-noclose", running_body("stmt-noclose")).await;
    // Scoped expectation: NO DELETE on the statement endpoint.
    let _no_close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-noclose"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let async_exec = session
            .attach_async_statement("stmt-noclose")
            .await
            .expect("attach");
        let _ = async_exec.status().await.expect("status");
        // async_exec drops here.
    }
    // Give any (erroneously) spawned fire-and-forget close a chance to
    // hit the wire before the scoped mock verifies expect(0) on drop.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    // `_no_close` verifies expect(0) when it drops at end of scope.
}

#[tokio::test]
async fn attached_handle_await_result_drop_does_not_cancel_statement() {
    // P1: an attached (non-owning) handle must NOT fire cancel_statement
    // when its await_result() future is dropped mid-poll. The owning
    // submit path *does* (await_result_drop_fires_cancel_statement pins
    // that); attach is a read-only adopt, so a passive observer wrapping
    // await_result() in a timeout must never cancel the owner's query.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    // Server stays RUNNING forever so await_result() loops until dropped.
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-attach-noclancel"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(running_body("stmt-attach-noclancel")),
        )
        .mount(&server)
        .await;
    // Scoped expectation: ZERO cancels on this statement.
    let _no_cancel = Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-attach-noclancel/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    let async_exec = session
        .attach_async_statement("stmt-attach-noclancel")
        .await
        .expect("attach");

    // Drop the await_result future via timeout.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_millis(150),
        async_exec.await_result(),
    )
    .await;
    assert!(outcome.is_err(), "timeout should fire");

    // Give any (erroneously) spawned cancel a chance to hit the wire
    // before the scoped mock verifies expect(0) on drop.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    // `_no_cancel` verifies expect(0) when it drops at end of scope.
}

#[tokio::test]
async fn attached_handle_cancel_errors() {
    // P1: cancel() on an attached (read-only) handle must refuse rather
    // than terminate a statement it doesn't own. No cancel reaches the
    // server.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_status_once(
        &server,
        "stmt-attach-cancel",
        running_body("stmt-attach-cancel"),
    )
    .await;
    let _no_cancel = Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-attach-cancel/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    let async_exec = session
        .attach_async_statement("stmt-attach-cancel")
        .await
        .expect("attach");

    let err = async_exec
        .cancel()
        .await
        .expect_err("cancel() on an attached handle must error");
    assert_eq!(err.code, ErrorCode::InvalidArgument, "got: {err:?}");

    // Same for a canceller derived from the attached handle.
    let canceller = async_exec.canceller();
    let cerr = canceller
        .cancel()
        .await
        .expect_err("canceller from an attached handle must error");
    assert_eq!(cerr.code, ErrorCode::InvalidArgument, "got: {cerr:?}");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

// ─── CloseStatement / status.state==CLOSED handling (issue #168) ───

/// Regression for issue #168 on the ASYNC path: when `await_result()`
/// polls and observes a terminal `status.state == CLOSED` (a statement
/// that auto-closed server-side), the statement is already closed
/// server-side and the kernel must NOT issue a redundant
/// `DELETE /api/2.0/sql/statements/<id>` (CloseStatement) on the handle's
/// `close()` / drop — matching the sync `execute()` path.
///
/// The submit path starts the shared `closed` flag `false` (correct:
/// `wait_timeout=0s` can't return CLOSED on submit), but reaching CLOSED
/// via a later poll must flip it so the fire-and-forget close is skipped.
#[tokio::test]
async fn await_result_closed_status_skips_close_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let batch = int64_batch("n", vec![7]);
    mount_submit(&server, pending_body("stmt-async-closed")).await;
    // await_result polls RUNNING → CLOSED (with inline results). The final
    // CLOSED body is also served to materialise_result's own status GET.
    mount_status_sequence(
        &server,
        "stmt-async-closed",
        vec![
            running_body("stmt-async-closed"),
            closed_inline_body("stmt-async-closed", &batch),
        ],
    )
    .await;

    // Scoped expectation: NO DELETE on the statement endpoint.
    let _no_close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-async-closed"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 7 AS n");
        let async_exec = stmt.submit().await.expect("submit");
        let mut stream = async_exec.await_result().await.expect("await_result");
        let batches = drain(&mut stream).await;
        assert_eq!(
            batches[0]
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            7
        );
        let reader = async_exec.reader().expect("reader after server auto-close");
        let mut refetched = reader.fetch_result().await.expect("refetch closed result");
        assert_eq!(drain(&mut refetched).await, batches);
        // An explicit close() on a CLOSED statement must also be a no-op.
        async_exec.close().await.expect("close");
        // async_exec (and stmt) drop here — a redundant close guard fires now.
    }

    // Give any (erroneously) spawned fire-and-forget close the full window to
    // reach the wire, but break early the instant one appears.
    let closes = wait_for_close_delete(
        &server,
        "stmt-async-closed",
        0,
        std::time::Duration::from_millis(500),
    )
    .await;
    assert_eq!(closes, 0, "CLOSED status must not trigger CloseStatement");
    // `_no_close` also verifies expect(0) when it drops at end of scope.
}

/// Regression-guard for the async CLOSED-skip fix: a SUCCEEDED async
/// result must STILL fire exactly one `CloseStatement` on close/drop —
/// the server still owns the statement. Guards against over-correcting
/// the CLOSED-skip into a server-statement leak.
#[tokio::test]
async fn await_result_succeeded_status_still_closes_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let batch = int64_batch("n", vec![7]);
    mount_submit(&server, pending_body("stmt-async-succ")).await;
    mount_status_sequence(
        &server,
        "stmt-async-succ",
        vec![
            running_body("stmt-async-succ"),
            succeeded_inline_body("stmt-async-succ", &batch),
        ],
    )
    .await;

    // Scoped expectation: exactly one DELETE on the statement endpoint.
    let _close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-async-succ"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 7 AS n");
        let async_exec = stmt.submit().await.expect("submit");
        let mut stream = async_exec.await_result().await.expect("await_result");
        let _ = drain(&mut stream).await;
        async_exec.close().await.expect("close");
        // async_exec drops here — the close guard must fire the DELETE.
    }

    let closes = wait_for_close_delete(
        &server,
        "stmt-async-succ",
        1,
        std::time::Duration::from_millis(2000),
    )
    .await;
    assert_eq!(
        closes, 1,
        "SUCCEEDED status must trigger exactly one CloseStatement"
    );
    // `_close` also verifies expect(1) when it drops at end of scope.
}

#[tokio::test]
async fn file_transfer_completion_is_shared_by_independent_readers() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    Mock::given(method("GET"))
        .and(path("/storage/file"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"download"))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("download");
    let body = file_transfer_body(
        "GET",
        &format!("{}/storage/file", server.uri()),
        &destination,
    );
    mount_submit(&server, pending_body("transfer")).await;
    mount_status_once(&server, "transfer", body).await;
    let session = open_session(&server).await;
    let mut statement = session.statement();
    statement
        .spec()
        .sql("GET '/Volumes/c/s/v/file' TO '/tmp/download'");
    statement.spec().staging_allowed_local_paths = Some(vec![dir.path().to_str().unwrap().into()]);
    let owner = statement.submit().await.unwrap();
    let first = owner.reader().unwrap();
    let second = owner.reader().unwrap();
    let (first, second) = tokio::join!(first.fetch_result(), second.fetch_result());
    for stream in [first, second] {
        assert!(stream.unwrap().next_batch().await.unwrap().is_none());
    }
    assert_eq!(std::fs::read(destination).unwrap(), b"download");
    owner.close().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test]
async fn sync_and_direct_transfers_complete_and_preserve_error_context() {
    use databricks_sql_kernel::DirectStatement;
    for direct in [false, true] {
        for denied in [false, true] {
            let server = MockServer::start().await;
            mount_create_session(&server).await;
            mount_delete_session(&server).await;
            mount_close_any_statement(&server).await;
            Mock::given(method("GET"))
                .and(path("/storage/file"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(b"download"))
                .expect(if denied { 0 } else { 1 })
                .mount(&server)
                .await;
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("download");
            let body = file_transfer_body(
                "GET",
                &format!("{}/storage/file", server.uri()),
                &destination,
            );
            mount_submit(&server, body).await;
            let session = open_session(&server).await;
            let mut statement = session.statement();
            statement
                .spec()
                .sql("GET '/Volumes/c/s/v/file' TO '/tmp/download'");
            statement.spec().staging_allowed_local_paths = Some(if denied {
                vec![]
            } else {
                vec![directory.path().to_str().unwrap().into()]
            });
            let result = if direct {
                match statement.execute_direct().await {
                    Ok(DirectStatement::Completed(executed)) => Ok(executed),
                    Ok(DirectStatement::Running(_)) => panic!("expected inline completion"),
                    Err(error) => Err(error),
                }
            } else {
                statement.execute().await
            };
            if denied {
                let error = result.err().expect("denied transfer");
                assert_eq!(error.error_code.as_deref(), Some("FILE_TRANSFER_ERROR"));
                assert_eq!(error.query_id.as_deref(), Some("transfer"));
                assert!(!destination.exists());
                assert_eq!(
                    wait_for_close_delete(
                        &server,
                        "transfer",
                        1,
                        std::time::Duration::from_secs(1),
                    )
                    .await,
                    1,
                );
            } else {
                let mut executed = result.unwrap();
                assert!(executed
                    .result_stream_mut()
                    .next_batch()
                    .await
                    .unwrap()
                    .is_none());
                assert_eq!(std::fs::read(&destination).unwrap(), b"download");
                executed.close().await.unwrap();
            }
            session.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn cancellation_interrupts_file_transfers() {
    use databricks_sql_kernel::DirectStatement;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for operation in ["PUT", "GET"] {
        for mode in ["sync", "direct", "async-owner", "async-detached"] {
            let server = MockServer::start().await;
            mount_create_session(&server).await;
            mount_delete_session(&server).await;
            mount_close_any_statement(&server).await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements/transfer/cancel"))
                .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
                .expect(1)
                .mount(&server)
                .await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (started, received) = tokio::sync::oneshot::channel();
            let storage = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(socket.read_u8().await.unwrap());
                }
                if operation == "GET" {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial").await.unwrap();
                } else {
                    socket.read_u8().await.unwrap();
                }
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            let directory = tempfile::tempdir().unwrap();
            let local = directory.path().join("file");
            let original = vec![b'a'; 8 * 1024 * 1024];
            std::fs::write(&local, &original).unwrap();
            let body = file_transfer_body(operation, &format!("http://{address}/file"), &local);
            if mode == "sync" || mode == "direct" {
                mount_submit(&server, body).await;
            } else {
                mount_submit(&server, pending_body("transfer")).await;
                mount_status_once(&server, "transfer", body).await;
            }
            let session = open_session(&server).await;
            let mut statement = session.statement();
            let sql = if operation == "PUT" {
                format!(
                    "PUT '{}' INTO '/Volumes/c/s/v/file' OVERWRITE",
                    local.display()
                )
            } else {
                format!("GET '/Volumes/c/s/v/file' TO '{}'", local.display())
            };
            statement.spec().sql(sql);
            statement.spec().staging_allowed_local_paths =
                Some(vec![directory.path().to_str().unwrap().into()]);
            let errors = if mode == "sync" || mode == "direct" {
                let canceller = statement.canceller();
                let transfer = tokio::spawn(async move {
                    if mode == "direct" {
                        match statement.execute_direct().await {
                            Ok(DirectStatement::Completed(executed)) => Ok(executed),
                            Ok(DirectStatement::Running(_)) => panic!("expected inline completion"),
                            Err(error) => Err(error),
                        }
                    } else {
                        statement.execute().await
                    }
                });
                tokio::time::timeout(Duration::from_secs(5), received)
                    .await
                    .unwrap()
                    .unwrap();
                canceller.cancel().await.unwrap();
                let result = tokio::time::timeout(Duration::from_secs(2), transfer)
                    .await
                    .expect("sync/direct cancellation did not stop the transfer")
                    .unwrap();
                vec![result.err().expect("cancelled transfer must fail")]
            } else {
                let owner = statement.submit().await.unwrap();
                let first = owner.reader().unwrap();
                let second = owner.reader().unwrap();
                let first = tokio::spawn(async move { first.fetch_result().await });
                tokio::time::timeout(Duration::from_secs(5), received)
                    .await
                    .unwrap()
                    .unwrap();
                let second = tokio::spawn(async move { second.fetch_result().await });
                if mode == "async-detached" {
                    owner.canceller().cancel().await.unwrap();
                } else {
                    ExecutedStatementHandle::cancel(&owner).await.unwrap();
                }
                let results = tokio::time::timeout(Duration::from_secs(2), async {
                    (first.await.unwrap(), second.await.unwrap())
                })
                .await
                .expect("async cancellation did not stop the transfer");
                let errors = vec![
                    results.0.err().expect("cancelled transfer must fail"),
                    results.1.err().expect("cancelled transfer must fail"),
                ];
                owner.close().await.unwrap();
                errors
            };
            for error in errors {
                assert_eq!(error.code, ErrorCode::Cancelled);
                assert_eq!(error.error_code.as_deref(), Some("FILE_TRANSFER_ERROR"));
                assert_eq!(error.query_id.as_deref(), Some("transfer"));
            }
            if operation == "GET" {
                assert_eq!(std::fs::read(&local).unwrap(), original);
            }
            storage.abort();
            session.close().await.unwrap();
        }
    }
}
