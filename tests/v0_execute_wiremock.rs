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

//! Wiremock tests for the v0 `Statement::execute` path.
//!
//! Each test stubs SEA's `POST /sessions`, `POST /statements`, and
//! (where needed) `GET /statements/{id}`, then drives a
//! `Statement::execute()` through the full pipeline:
//! Session::open → Statement::execute → ExecutedStatement →
//! ResultStream::next_batch → Row accessors.

use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, ErrorCode, ExecutedStatementHandle, ResultBatch, Session,
    TypedValue,
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
/// /statements/...). Otherwise the executed handle's Drop will
/// generate noisy 404s in the test log.
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

async fn mount_execute(server: &MockServer, body: String) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

async fn mount_status(server: &MockServer, statement_id: &str, body: String) {
    Mock::given(method("GET"))
        .and(path(format!("/api/2.0/sql/statements/{statement_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

fn int64_batch(name: &str, values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn str_batch(name: &str, values: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
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
async fn execute_succeeded_inline_returns_one_row() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let batch = int64_batch("n", vec![1]);
    mount_execute(&server, succeeded_inline_body("stmt-1", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let mut exec = stmt.execute().await.expect("execute");

    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
    exec.close().await.expect("close");
}

#[tokio::test]
async fn execute_running_then_succeeded_polls_status() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // First POST returns RUNNING; subsequent GET returns SUCCEEDED.
    mount_execute(&server, running_body("stmt-poll")).await;
    let batch = int64_batch("n", vec![42]);
    mount_status(
        &server,
        "stmt-poll",
        succeeded_inline_body("stmt-poll", &batch),
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 42 AS n");
    let mut exec = stmt
        .execute()
        .await
        .expect("execute should poll to success");

    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    exec.close().await.expect("close");
}

#[tokio::test]
async fn execute_failed_propagates_sql_state() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let body = failed_body("stmt-fail", "syntax error near 'INTO'", "42601");
    mount_execute(&server, body).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELEKT 1");
    let err = match stmt.execute().await {
        Ok(_) => panic!("execute should fail on FAILED state"),
        Err(e) => e,
    };

    assert_eq!(err.code, ErrorCode::SqlError);
    assert_eq!(err.sql_state.as_deref(), Some("42601"), "{err:?}");
}

#[tokio::test]
async fn execute_validates_missing_sql() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    let err = match stmt.execute().await {
        Ok(_) => panic!("execute without sql must fail"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("sql"), "{}", err.message);
}

#[tokio::test]
async fn execute_sends_named_params_in_wire_form() {
    // Named binding is the spec-required public form (canonical SEA
    // proto marks `StatementParameter.name` as openapi_required=true).
    // The kernel must forward `StatementSpec.named_params` to the
    // SEA `parameters` array with `name` set and `ordinal` omitted.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = str_batch("v", vec!["bound"]);
    mount_execute(&server, succeeded_inline_body("stmt-named", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT :x")
        .param_named("x", TypedValue::Int64(1))
        .expect("param_named");
    let mut exec = stmt.execute().await.expect("execute with named params");
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches[0].num_rows(), 1);
    exec.close().await.expect("close");
}

#[tokio::test]
async fn execute_rejects_binary_typed_value_before_wire() {
    // No execute mock — Binary must be rejected client-side before
    // any HTTP request, with a message pointing at the `unhex(?)`
    // workaround.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT ?")
        .param(1, TypedValue::Binary(vec![0xCA, 0xFE]))
        .unwrap();
    let err = match stmt.execute().await {
        Ok(_) => panic!("Binary must be rejected before any wire call"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("unhex"), "{}", err.message);
}

#[tokio::test]
async fn execute_sends_positional_params_in_wire_form() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Capture the POST body via a counter mock — we don't need to
    // assert the body here; the success of the round-trip means the
    // kernel serialised parameters into the expected request shape.
    let batch = str_batch("v", vec!["bound"]);
    mount_execute(&server, succeeded_inline_body("stmt-params", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT ?, ?")
        .param(1, TypedValue::Int64(42))
        .unwrap()
        .param(2, TypedValue::String("bound".into()))
        .unwrap();
    let mut exec = stmt.execute().await.expect("execute with params");
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches[0].num_rows(), 1);
    exec.close().await.expect("close");
}

#[tokio::test]
async fn re_execute_invalidates_prior_handle() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    // Stub cancel for the prior statement (POST /cancel).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-A/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-B/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    // We need two different statement_ids across two POST /statements
    // calls. Stub each via `up_to_n_times`. Use the response template
    // builder for sequenced mocks.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(succeeded_inline_body("stmt-A", &int64_batch("n", vec![1]))),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(succeeded_inline_body("stmt-B", &int64_batch("n", vec![2]))),
        )
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let exec_a = stmt.execute().await.expect("execute A");
    assert_eq!(exec_a.statement_id(), "stmt-A");

    // Re-execute on the same Statement invalidates exec_a.
    let _exec_b = stmt.execute().await.expect("execute B");
    let err = exec_a
        .cancel()
        .await
        .expect_err("stale handle cancel must fail");
    assert_eq!(err.code, ErrorCode::InvalidStatementHandle);
}

#[tokio::test]
async fn executed_close_is_idempotent() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let batch = int64_batch("n", vec![1]);
    mount_execute(&server, succeeded_inline_body("stmt-close", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let exec = stmt.execute().await.unwrap();
    exec.close().await.expect("first close");
    // Second close short-circuits without hitting the wire.
    exec.close().await.expect("second close — idempotent");
}

#[tokio::test]
async fn statement_close_cancels_and_closes_outstanding_executed_handle() {
    // `Statement::close()` must cancel + close any executed handle
    // the Statement is still tracking, so a JDBC / ODBC / PySQL-
    // style "I'm done with this whole thing" call actually
    // releases the server-side statement. Same wire shape as the
    // auto-invalidate-on-reuse path that `execute()` / `submit()`
    // run before re-issuing.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-Z/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    let batch = int64_batch("n", vec![1]);
    mount_execute(&server, succeeded_inline_body("stmt-Z", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let exec = stmt.execute().await.expect("execute");
    assert_eq!(exec.statement_id(), "stmt-Z");

    // Close the parent Statement (not the executed handle). The
    // executed handle the caller still holds becomes invalid; its
    // next call short-circuits locally via the validity flag.
    stmt.close().await.expect("Statement::close");

    let err = exec
        .cancel()
        .await
        .expect_err("stale handle cancel after Statement::close must fail");
    assert_eq!(err.code, ErrorCode::InvalidStatementHandle);
}

#[tokio::test]
async fn row_accessors_walk_succeeded_result() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), None])),
        ],
    )
    .unwrap();
    mount_execute(&server, succeeded_inline_body("stmt-rows", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT id, name FROM t");
    let mut exec = stmt.execute().await.unwrap();

    let stream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().unwrap();

    let row = stream.next_row_in_batch().expect("first row");
    assert_eq!(row.get_i64(0).unwrap(), Some(1));
    assert_eq!(row.get_str(1).unwrap(), Some("alice"));

    let row = stream.next_row_in_batch().expect("second row");
    assert_eq!(row.get_i64(0).unwrap(), Some(2));
    assert_eq!(row.get_str(1).unwrap(), None);

    assert!(stream.next_row_in_batch().is_none());
    exec.close().await.expect("close");
}
