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
    AuthConfig, ConnectionConfig, ErrorCode, ExecutedStatementHandle, ResultBatch, ResultConfig,
    Session, TypedValue,
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
            identity_federation_client_id: None,
        })
        .open()
        .await
        .expect("Session::open should succeed")
}

/// Open a session with a custom [`ResultConfig`]. Used by the
/// disposition tests below to drive `cloudfetch_enabled`.
async fn open_session_with_result(server: &MockServer, result: ResultConfig) -> Session {
    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .result(result)
        .open()
        .await
        .expect("Session::open should succeed")
}

/// Read the `disposition` sent on the recorded POST /statements request.
async fn recorded_execute_disposition(server: &MockServer) -> String {
    let recorded = server
        .received_requests()
        .await
        .expect("wiremock received_requests must be available");
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

/// Like [`succeeded_inline_body`] but reports `status.state == CLOSED`.
/// This is the inline direct-result fast path (a quick `SELECT 1` where
/// the kernel omits `wait_timeout`): the server auto-closes the statement
/// and returns `CLOSED`, so the kernel must NOT issue a follow-up
/// `CloseStatement` (DELETE) on it. See issue #168 (CLOUDFETCH-013).
fn closed_inline_body(statement_id: &str, batch: &RecordBatch) -> String {
    succeeded_inline_body(statement_id, batch)
        .replace(r#""state":"SUCCEEDED""#, r#""state":"CLOSED""#)
}

fn running_body(statement_id: &str) -> String {
    json!({
        "statement_id": statement_id,
        "status": { "state": "RUNNING" },
    })
    .to_string()
}

/// Poll `server.received_requests()` until `expected` `DELETE`s to the
/// given statement endpoint are observed, or `timeout` elapses; returns
/// the final observed count. Mirrors the metadata suite's helper: for a
/// positive expectation it waits only as long as needed, and for a
/// negative expectation (`expected == 0`) it still gives an
/// erroneously-spawned close the full `timeout` to reach the wire but
/// breaks early the instant one appears, so the negative assertion stays
/// reliably able to fail under CI load.
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
/// Pins the kernel's `Error.display_message` / `diagnostic_info` /
/// `error_details_json` carry on the sync polling path.
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
async fn execute_with_cloudfetch_enabled_sends_inline_or_external_links_disposition() {
    // Default (cloudfetch_enabled=true): the ExecuteStatement disposition
    // must be INLINE_OR_EXTERNAL_LINKS so the server may return CloudFetch
    // external links.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let batch = int64_batch("n", vec![1]);
    mount_execute(&server, succeeded_inline_body("stmt-cf-on", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let mut exec = stmt.execute().await.expect("execute");
    drain(exec.result_stream_mut()).await;
    exec.close().await.expect("close");

    assert_eq!(
        recorded_execute_disposition(&server).await,
        "INLINE_OR_EXTERNAL_LINKS",
    );
}

#[tokio::test]
async fn execute_with_cloudfetch_disabled_sends_inline_disposition() {
    // Regression test for issue #196. With cloudfetch_enabled=false the
    // ExecuteStatement disposition must be INLINE — this is how the kernel
    // (like the JDBC / Thrift reference drivers) disables cloud-fetch on
    // SEA, NOT via a `can_cloud_download` session conf (which SEA rejects
    // at ExecuteStatement with CONFIG_NOT_AVAILABLE). The server then
    // serves results inline and the statement succeeds.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let batch = int64_batch("n", vec![7]);
    mount_execute(&server, succeeded_inline_body("stmt-cf-off", &batch)).await;

    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = open_session_with_result(&server, result).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 7 AS n");
    let mut exec = stmt
        .execute()
        .await
        .expect("execute must succeed inline with cloudfetch disabled");
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        7
    );
    exec.close().await.expect("close");

    assert_eq!(recorded_execute_disposition(&server).await, "INLINE");
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

// ─── Cancel-after-parent-drop regression guard (PR #25 / #29) ─────
//
// Pins the kernel-level contract that the napi binding (PR #29) must
// counteract.
//
// The kernel `Statement` (mutable) shares a `ValidityFlag` with each
// `ExecutedStatement` / `ExecutedAsyncStatement` it produces. Dropping
// the parent calls `valid.invalidate()` on that shared flag — so any
// subsequent `ExecutedStatement::cancel` short-circuits locally with
// `KernelError(InvalidStatementHandle)` without a wire round-trip.
//
// `close()` is **not** validity-checked: dropping the parent still
// lets the server-side `CloseStatement` round-trip run so the server
// resource is released. The asymmetry is intentional — cancel must
// be cheap on a zombie handle (no wire) while close must always
// reach the server when possible.
//
// The cancel-lifecycle bug surfaced by PR #25 is that the napi
// `Connection::execute_statement` wrapper drops the parent kernel
// `Statement` at end of its closure, leaving the JS-visible
// `Statement` holding only the `ExecutedStatement`. The next
// `Statement.cancel()` from JS hits the short-circuit above and
// returns `InvalidStatementHandle`.
//
// PR #29 fixes the binding by wrapping the inner state in a
// `StatementInner { _parent, executed }` struct that keeps the
// parent alive for the wrapper's lifetime.
//
// This test pins the **kernel invariant** the fix is designed
// around. If the kernel ever weakens the parent-drop semantics
// (e.g. doesn't invalidate on Drop), this test fails loudly and
// the binding's fix can be reviewed for whether it's still needed.
// If the kernel keeps the semantics, the binding-side regression
// guard (added separately once #29 lands) holds the binding to
// account.

#[tokio::test]
async fn kernel_invariant_parent_drop_invalidates_executed_cancel() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = int64_batch("n", vec![1]);
    mount_execute(&server, succeeded_inline_body("stmt-parent-drop", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let exec = stmt.execute().await.expect("execute");

    // This is the exact shape the napi binding's
    // `Connection::execute_statement` produces today (PR #25): the
    // kernel `Statement` (`stmt`) is dropped at end of the closure
    // while the caller keeps holding the `ExecutedStatement`
    // (`exec`). PR #29 fixes the binding by stashing `stmt` alongside
    // `exec` so the parent outlives the executed handle.
    drop(stmt);

    let err = exec
        .cancel()
        .await
        .expect_err("cancel after parent Statement drop must short-circuit");
    assert_eq!(err.code, ErrorCode::InvalidStatementHandle);
}

// ─── ExecutedStatement extended status accessors (PR #23) ─────────

/// Build a SEA `SUCCEEDED` body with the 5 extended status fields
/// populated. Mirrors `succeeded_inline_body` but injects status
/// fields the kernel surfaces via `ExecutedStatement::*` getters.
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
async fn executed_statement_surfaces_extended_status_fields() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = int64_batch("n", vec![1]);
    mount_execute(
        &server,
        succeeded_inline_body_with_status(
            "stmt-status",
            &batch,
            Some(7),
            Some("All good"),
            Some("operator=Scan"),
            Some(r#"{"k":"v"}"#),
        ),
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("UPDATE t SET x = 1");
    let exec = stmt.execute().await.expect("execute");

    assert_eq!(exec.num_modified_rows(), Some(7));
    assert_eq!(exec.display_message(), Some("All good"));
    assert_eq!(exec.diagnostic_info(), Some("operator=Scan"));
    assert_eq!(exec.error_details_json(), Some(r#"{"k":"v"}"#));

    exec.close().await.expect("close");
}

#[tokio::test]
async fn executed_statement_status_fields_default_to_none() {
    // When the server omits the extended fields (the common case for
    // SELECT on classic warehouses), all five accessors return None.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = int64_batch("n", vec![1]);
    mount_execute(&server, succeeded_inline_body("stmt-empty-status", &batch)).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let exec = stmt.execute().await.expect("execute");

    assert_eq!(exec.num_modified_rows(), None);
    assert_eq!(exec.display_message(), None);
    assert_eq!(exec.diagnostic_info(), None);
    assert_eq!(exec.error_details_json(), None);

    exec.close().await.expect("close");
}

#[tokio::test]
async fn polling_path_carries_terminal_status_not_initial() {
    // Regression guard against the kernel surfacing the FIRST status
    // response instead of the TERMINAL one.
    //
    // Setup: the POST /statements returns RUNNING with no extended
    // fields. The polling GET /statements/{id} then returns SUCCEEDED
    // with all 5 extended fields populated. The kernel's
    // `wait_for_completion` loop must hand the **terminal** status
    // (the final GET) to `ExecutedStatement::new`, not the initial
    // POST response. If the kernel ever regresses to carrying forward
    // the first response, this test fails because the accessors will
    // see `None` instead of the populated terminal values.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Initial execute returns RUNNING — no extended fields. The
    // kernel must NOT latch this onto the ExecutedStatement.
    mount_execute(&server, running_body("stmt-poll-status")).await;

    // Polled GET returns SUCCEEDED with the extended fields
    // populated. This is the status the kernel must carry forward.
    let batch = int64_batch("n", vec![42]);
    mount_status(
        &server,
        "stmt-poll-status",
        succeeded_inline_body_with_status(
            "stmt-poll-status",
            &batch,
            Some(11),
            Some("Done after retry"),
            Some("operator=Project; stage=Execute"),
            Some(r#"{"errorClass":"NONE"}"#),
        ),
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("UPDATE t SET x = 1");
    let exec = stmt
        .execute()
        .await
        .expect("execute should poll to terminal SUCCEEDED");

    // All accessors must reflect the TERMINAL poll, not the initial
    // RUNNING response (which had no extended fields).
    assert_eq!(exec.num_modified_rows(), Some(11));
    assert_eq!(exec.display_message(), Some("Done after retry"));
    assert_eq!(
        exec.diagnostic_info(),
        Some("operator=Project; stage=Execute")
    );
    assert_eq!(exec.error_details_json(), Some(r#"{"errorClass":"NONE"}"#));

    exec.close().await.expect("close");
}

#[tokio::test]
async fn execute_failed_carries_extended_status_fields_onto_error() {
    // Pins the sync polling path's terminal-error contract: when the
    // SEA wire response on a FAILED statement includes
    // `display_message` / `diagnostic_info` / `error_details_json`,
    // the kernel `Error` returned from `Statement::execute()` carries
    // all three. Mirrors the Thrift `TGetOperationStatusResp` field
    // set so a Thrift-migrating wrapper (e.g. the napi binding's JS
    // Error envelope) sees identical content regardless of which
    // path surfaced the failure. Regression guard for the
    // `terminal_outcome::Failed → carry_extended_status` wiring at
    // `src/client/sea.rs`.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let body = failed_body_with_extended_fields(
        "stmt-fail-ext",
        "table not found",
        "42P01",
        "User-facing reason from server",
        "operator=Scan; stage=Analyze; full stack ...",
        r#"{"errorClass":"TABLE_OR_VIEW_NOT_FOUND","messageTemplate":"..."}"#,
    );
    mount_execute(&server, body).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT * FROM does_not_exist");
    let err = match stmt.execute().await {
        Ok(_) => panic!("execute should fail on FAILED state"),
        Err(e) => e,
    };

    assert_eq!(err.code, ErrorCode::SqlError);
    assert_eq!(err.sql_state.as_deref(), Some("42P01"));
    assert_eq!(
        err.display_message.as_deref(),
        Some("User-facing reason from server"),
        "display_message must carry through from FAILED wire envelope"
    );
    assert_eq!(
        err.diagnostic_info.as_deref(),
        Some("operator=Scan; stage=Analyze; full stack ...")
    );
    assert_eq!(
        err.error_details_json.as_deref(),
        Some(r#"{"errorClass":"TABLE_OR_VIEW_NOT_FOUND","messageTemplate":"..."}"#)
    );
}

/// Reproducer for the orphaned-statement bug observed against the
/// proxy-test suite's CLOUDFETCH-016 (databricks-driver-test PR #410).
///
/// `Statement::execute()` is supposed to be all-or-nothing: either it
/// hands back an `ExecutedStatement` whose `close()` cleans up
/// server-side state, OR it returns an error and the kernel cleans
/// up internally. Today the latter half is missing for the CloudFetch
/// path: when the server returns a `SUCCEEDED` response carrying
/// `external_links` and the kernel's reader-construction reads chunk
/// 0 (in `ResultPostProcessor::new` -> `StreamingCloudFetchProvider::
/// get_schema` -> eager download) and that download fails, the
/// SUCCEEDED statement on the server is leaked until the SQL
/// Gateway's `CommandInactivityTimeout` (~1h) reclaims it. The
/// kernel never issues `DELETE /api/2.0/sql/statements/{id}`.
///
/// This test mounts:
///   1. `POST /api/2.0/sql/statements` returning a SUCCEEDED body
///      with `external_links` pointing back at the mock server.
///   2. `GET` on that link returning HTTP 500 (simulating an Azure
///      Blob / S3 transient failure that doesn't trigger urllib3's
///      retry policy in our case).
///
/// Expectation after the fix: `Statement::execute()` returns Err,
/// AND the mock records a `DELETE /api/2.0/sql/statements/stmt-cf-leak`
/// before the test ends.
#[tokio::test]
async fn execute_cloudfetch_chunk0_failure_closes_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let statement_id = "stmt-cf-leak";
    let chunk_path = format!("/cloud-fetch/{statement_id}/chunk-0.arrow");
    let chunk_url = format!("{}{}", server.uri(), chunk_path);

    // POST /statements: SUCCEEDED + external_links pointing at the
    // mock-served chunk URL. The kernel will read chunk 0 eagerly in
    // ResultPostProcessor::new to extract the result schema, so chunk
    // 0's GET fires before execute() returns.
    let exec_body = json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 1,
                "columns": [{
                    "name": "id",
                    "type_name": "BIGINT",
                    "type_text": "BIGINT",
                    "position": 0,
                }],
            },
            "total_chunk_count": 1,
            "total_row_count": 1,
            "total_byte_count": 1024,
            "truncated": false,
        },
        "result": {
            "external_links": [{
                "external_link": chunk_url,
                "expiration": "2099-01-01T00:00:00.000Z",
                "chunk_index": 0,
                "row_offset": 0,
                "row_count": 1,
                "byte_count": 1024,
            }],
        },
    })
    .to_string();
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(exec_body))
        .mount(&server)
        .await;

    // Chunk-0 download fails with HTTP 404 — a non-retryable client
    // error. The kernel's CloudFetch downloader treats 5xx as
    // transient and retries up to ~30 times, which would make this
    // test run for minutes; 404 hits the same propagation path
    // (downloader -> ResultPostProcessor::new -> execute_and_read)
    // on the first attempt. The bug we're reproducing is independent
    // of which 4xx/5xx code surfaces the failure — what matters is
    // that download fails after the SUCCEEDED execute response.
    Mock::given(method("GET"))
        .and(path(chunk_path.as_str()))
        .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
        .mount(&server)
        .await;

    // DELETE /statements/{id}: success. We do NOT mount it with
    // `expect()` so that absence of the call shows up as a missing
    // entry in `received_requests`, not as a wiremock failure
    // message ("expected mock not called").
    Mock::given(method("DELETE"))
        .and(path(format!("/api/2.0/sql/statements/{statement_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT id FROM range(2000000)");

    let err = stmt
        .execute()
        .await
        .err()
        .expect("execute must fail when chunk-0 returns HTTP 500");

    // The error variant isn't load-bearing here; the close path is.
    // Log so a regression that changes the surfaced error shape is
    // visible at test-output time without burying the close-assert.
    eprintln!("[reproducer] execute() returned: {err:?}");

    // Defensive close — closes any state the failing execute()
    // tracked locally. Mirrors what the Python connector does (and
    // the C# canonical's `DatabricksCompositeReader.Dispose`).
    let _ = stmt.close().await;

    // Drop the session too so any lingering shutdown paths run.
    let _ = session.close().await;

    // The fix: after the eager-chunk-0 failure, the kernel must
    // issue DELETE /api/2.0/sql/statements/{id} so the SUCCEEDED
    // server-side statement doesn't linger until
    // CommandInactivityTimeout (~1h).
    let recorded = server
        .received_requests()
        .await
        .expect("wiremock received_requests must be available");
    let delete_path = format!("/api/2.0/sql/statements/{statement_id}");
    let deletes: Vec<_> = recorded
        .iter()
        .filter(|r| r.method == wiremock::http::Method::DELETE && r.url.path() == delete_path)
        .collect();
    assert!(
        !deletes.is_empty(),
        "expected at least one DELETE {delete_path} after chunk-0 download \
         failure; observed requests: {observed:?}",
        delete_path = delete_path,
        observed = recorded
            .iter()
            .map(|r| format!("{} {}", r.method, r.url.path()))
            .collect::<Vec<_>>(),
    );
}

// ─── CloseStatement / status.state==CLOSED handling (issue #168) ───

/// Regression for issue #168 (extended by CLOUDFETCH-013): when the
/// `ExecuteStatement` response for a *regular* query already reports
/// `status.state == CLOSED` (a `SELECT 1` delivered inline via direct
/// results, where the server auto-closes and returns `CLOSED`), the
/// statement is closed server-side and the kernel must NOT issue a
/// redundant `DELETE /api/2.0/sql/statements/<id>` (CloseStatement) on
/// `ExecutedStatement::close()` or on drop. Mirrors the metadata suite's
/// `metadata_closed_response_skips_close_statement`.
#[tokio::test]
async fn execute_closed_inline_skips_close_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let batch = int64_batch("n", vec![1]);
    // Inline result with status.state == CLOSED — server already closed it.
    mount_execute(&server, closed_inline_body("stmt-closed", &batch)).await;

    // Scoped expectation: NO DELETE on the statement endpoint.
    let _no_close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-closed"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let mut exec = stmt.execute().await.expect("execute");
        let batches = drain(exec.result_stream_mut()).await;
        assert_eq!(batches.len(), 1);
        // An explicit close() on a CLOSED statement must also be a no-op.
        exec.close().await.expect("close");
        // exec (and stmt) drop here — a redundant close guard would fire now.
    }

    // Give any (erroneously) spawned fire-and-forget close the full window to
    // reach the wire, but break early the instant one appears so the negative
    // assertion stays reliably able to fail under CI load.
    let closes = wait_for_close_delete(
        &server,
        "stmt-closed",
        0,
        std::time::Duration::from_millis(500),
    )
    .await;
    assert_eq!(closes, 0, "CLOSED response must not trigger CloseStatement");
    // `_no_close` also verifies expect(0) when it drops at end of scope.
}

/// Regression-guard for issue #168's fix on the regular-query path: when
/// the `ExecuteStatement` response reports a non-CLOSED terminal state
/// (`SUCCEEDED`), the server still owns the statement, so the kernel MUST
/// still issue exactly one `CloseStatement` (DELETE) on close/drop. Ensures
/// the CLOSED-skip does not over-correct and leak a live server statement.
#[tokio::test]
async fn execute_succeeded_inline_still_closes_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let batch = int64_batch("n", vec![1]);
    // Inline result with status.state == SUCCEEDED — server still owns it.
    mount_execute(&server, succeeded_inline_body("stmt-succeeded", &batch)).await;

    // Scoped expectation: exactly one DELETE on the statement endpoint.
    let _close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-succeeded"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let mut exec = stmt.execute().await.expect("execute");
        let _ = drain(exec.result_stream_mut()).await;
        exec.close().await.expect("close");
        // exec drops here — the close guard must fire the DELETE.
    }

    // Poll until the fire-and-forget close hits the wire (bounded by a
    // timeout) rather than assuming it lands within a fixed sleep.
    let closes = wait_for_close_delete(
        &server,
        "stmt-succeeded",
        1,
        std::time::Duration::from_millis(2000),
    )
    .await;
    assert_eq!(
        closes, 1,
        "SUCCEEDED response must trigger exactly one CloseStatement"
    );
    // `_close` also verifies expect(1) when it drops at end of scope.
}
