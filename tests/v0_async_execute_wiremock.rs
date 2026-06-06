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
//! Each test stubs SEA's `POST /sessions`, `POST /statements`, and the
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
use wiremock::matchers::{method, path};
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
        })
        .open()
        .await
        .expect("Session::open should succeed")
}

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

    // Re-submit invalidates async_exec_x.
    let _async_exec_y = stmt.submit().await.expect("submit Y");
    let err = async_exec_x
        .cancel()
        .await
        .expect_err("stale async handle cancel must fail");
    assert_eq!(err.code, ErrorCode::InvalidStatementHandle);
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
