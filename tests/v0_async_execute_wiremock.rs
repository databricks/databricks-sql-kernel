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

fn cancelled_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "CANCELED" },
    })
    .to_string()
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
async fn await_result_surfaces_cancelled_as_invalid_state() {
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
    // `Error::invalid_state()` currently maps to InvalidArgument
    // until the ErrorCode taxonomy expansion (spec A8) ships
    // `FailedPrecondition` / `InvalidState`.
    assert_eq!(err.code, ErrorCode::InvalidArgument);
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
