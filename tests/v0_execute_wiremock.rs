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
//! Each test stubs SEA's `CreateSession`, `POST /statements`, and
//! (where needed) `GET /statements/{id}`, then drives a
//! `Statement::execute()` through the full pipeline:
//! Session::open → Statement::execute → ExecutedStatement →
//! ResultStream::next_batch → Row accessors.

use arrow_array::builder::{BinaryBuilder, Int32Builder};
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, LargeStringArray, ListArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Fields, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, ErrorCode, ExecutedStatementHandle,
    GeospatialResultRepresentation, HttpConfig, ResultBatch, ResultConfig, Session, TryNextBatch,
    TypedValue, MAX_CLIENT_QUERY_TIMEOUT,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

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

async fn open_session_with_geospatial(
    server: &MockServer,
    representation: GeospatialResultRepresentation,
) -> Session {
    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .geospatial_result_representation(representation)
        .open()
        .await
        .expect("Session::open should succeed")
}

async fn open_session_with_http(server: &MockServer, http: HttpConfig) -> Session {
    Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .http(http)
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

fn arrow_schema_ipc_bytes(schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema).unwrap();
    writer.finish().unwrap();
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

/// Delay only the first execute response.
struct DelayedFirstExecute {
    first: String,
    later: String,
    first_delay: Duration,
    calls: AtomicUsize,
}

impl Respond for DelayedFirstExecute {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(200)
                .set_body_string(self.first.clone())
                .set_delay(self.first_delay)
        } else {
            ResponseTemplate::new(200).set_body_string(self.later.clone())
        }
    }
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

fn dml_count_batch(affected: i64, inserted: i64, updated: i64, deleted: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("num_affected_rows", DataType::Int64, false),
        Field::new("num_inserted_rows", DataType::Int64, false),
        Field::new("num_updated_rows", DataType::Int64, false),
        Field::new("num_deleted_rows", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![affected])),
            Arc::new(Int64Array::from(vec![inserted])),
            Arc::new(Int64Array::from(vec![updated])),
            Arc::new(Int64Array::from(vec![deleted])),
        ],
    )
    .unwrap()
}

fn str_batch(name: &str, values: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
}

const POINT_1_2_WKB: &[u8] = &[
    1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
];

fn geospatial_type(family: &str, type_srid: i32) -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("srid", DataType::Int32, false),
        Field::new("wkb", DataType::Binary, false).with_metadata(HashMap::from([
            (family.to_string(), "true".to_string()),
            ("srid".to_string(), type_srid.to_string()),
        ])),
    ]))
}

fn untagged_geospatial_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("srid", DataType::Int32, true),
        Field::new("wkb", DataType::Binary, true),
    ]))
}

fn geospatial_array(row_srid: i32) -> Arc<StructArray> {
    let DataType::Struct(fields) = untagged_geospatial_type() else {
        unreachable!()
    };
    let mut srids = Int32Builder::new();
    srids.append_value(row_srid);
    srids.append_value(0);
    let mut wkbs = BinaryBuilder::new();
    wkbs.append_value(POINT_1_2_WKB);
    wkbs.append_value([]);
    Arc::new(StructArray::new(
        fields,
        vec![Arc::new(srids.finish()), Arc::new(wkbs.finish())],
        Some(NullBuffer::from(vec![true, false])),
    ))
}

fn geospatial_batch() -> RecordBatch {
    // Reyden's result projection emits nullable physical children without
    // logical geo tags. Reyder retains the canonical geo identity in the
    // structured manifest schema below.
    let geometry = geospatial_array(3857);
    let geography = geospatial_array(4326);
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_0", geometry.data_type().clone(), true),
        Field::new("col_1", geography.data_type().clone(), true),
    ]));
    RecordBatch::try_new(schema, vec![geometry, geography]).unwrap()
}

fn legacy_geospatial_batch(data_type: DataType) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_0", data_type.clone(), true),
        Field::new("col_1", data_type.clone(), true),
    ]));
    let columns: Vec<ArrayRef> = match data_type {
        DataType::Utf8 => vec![
            Arc::new(StringArray::from(vec![Some("SRID=3857;POINT (1 2)"), None])) as _,
            Arc::new(StringArray::from(vec![Some("SRID=4326;POINT (1 2)"), None])) as _,
        ],
        DataType::LargeUtf8 => vec![
            Arc::new(LargeStringArray::from(vec![
                Some("SRID=3857;POINT (1 2)"),
                None,
            ])) as _,
            Arc::new(LargeStringArray::from(vec![
                Some("SRID=4326;POINT (1 2)"),
                None,
            ])) as _,
        ],
        other => panic!("legacy geospatial text must be Utf8 or LargeUtf8, got {other:?}"),
    };
    RecordBatch::try_new(schema, columns).unwrap()
}

fn nested_geospatial_batch(native: bool) -> RecordBatch {
    let geometry: ArrayRef = if native {
        geospatial_array(4326)
    } else {
        Arc::new(StringArray::from(vec![Some("SRID=4326;POINT (1 2)"), None]))
    };
    let blob: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"\x00\xffblob"[..]),
        Some(&b"still here"[..]),
    ]));
    let nested_fields = Fields::from(vec![
        Field::new("geom", geometry.data_type().clone(), true),
        Field::new("blob", DataType::Binary, true),
    ]);
    let nested: ArrayRef = Arc::new(StructArray::new(nested_fields, vec![geometry, blob], None));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col_0",
        nested.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![nested]).unwrap()
}

fn succeeded_nested_geospatial_body(statement_id: &str, batch: &RecordBatch) -> String {
    let ipc = arrow_ipc_bytes(batch);
    let physical_schema = arrow_schema_ipc_bytes(batch.schema().as_ref());
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 1,
                "columns": [{
                    "name": "mixed",
                    "type_name": "STRUCT",
                    "type_text": "STRUCT<geom:GEOMETRY(4326),blob:BINARY>",
                    "position": 0,
                }],
            },
            "arrow_schema": STANDARD.encode(physical_schema),
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
            "attachment": STANDARD.encode(&ipc),
        },
    })
    .to_string()
}

fn logical_geospatial_schema() -> Schema {
    Schema::new(vec![
        Field::new("geom", geospatial_type("geometry", -1), true),
        Field::new("geog", geospatial_type("geography", 4326), true),
    ])
}

fn succeeded_geospatial_inline_body(statement_id: &str, batch: &RecordBatch) -> String {
    let ipc = arrow_ipc_bytes(batch);
    let logical_schema = arrow_schema_ipc_bytes(&logical_geospatial_schema());
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 2,
                "columns": [
                    {
                        "name": "geom",
                        "type_name": "GEOMETRY",
                        "type_text": "GEOMETRY(ANY)",
                        "position": 0,
                    },
                    {
                        "name": "geog",
                        "type_name": "GEOGRAPHY",
                        "type_text": "GEOGRAPHY(4326)",
                        "position": 1,
                    }
                ],
            },
            "arrow_schema": STANDARD.encode(logical_schema),
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
            "attachment": STANDARD.encode(&ipc),
        },
    })
    .to_string()
}

fn succeeded_empty_geospatial_body(statement_id: &str) -> String {
    let logical_schema = arrow_schema_ipc_bytes(&logical_geospatial_schema());
    json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 2,
                "columns": [
                    {
                        "name": "geom",
                        "type_name": "GEOMETRY",
                        "type_text": "GEOMETRY(ANY)",
                        "position": 0,
                    },
                    {
                        "name": "geog",
                        "type_name": "GEOGRAPHY",
                        "type_text": "GEOGRAPHY(4326)",
                        "position": 1,
                    }
                ],
            },
            "arrow_schema": STANDARD.encode(logical_schema),
            "total_chunk_count": 0,
            "total_row_count": 0,
            "total_byte_count": 0,
            "truncated": false,
        },
    })
    .to_string()
}

fn succeeded_legacy_geospatial_body(
    statement_id: &str,
    batch: &RecordBatch,
    include_rows: bool,
) -> String {
    let ipc = arrow_ipc_bytes(batch);
    let physical_schema = arrow_schema_ipc_bytes(batch.schema().as_ref());
    let total_chunks = i32::from(include_rows);
    let total_rows = if include_rows { batch.num_rows() } else { 0 };
    let mut body = json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 2,
                "columns": [
                    {
                        "name": "geom",
                        "type_name": "GEOMETRY",
                        "type_text": "GEOMETRY(ANY)",
                        "position": 0,
                    },
                    {
                        "name": "geog",
                        "type_name": "GEOGRAPHY",
                        "type_text": "GEOGRAPHY(4326)",
                        "position": 1,
                    }
                ],
            },
            "arrow_schema": STANDARD.encode(physical_schema),
            "total_chunk_count": total_chunks,
            "total_row_count": total_rows,
            "total_byte_count": if include_rows { ipc.len() } else { 0 },
            "truncated": false,
        },
    });
    if include_rows {
        body["result"] = json!({
            "chunk_index": 0,
            "row_offset": 0,
            "row_count": batch.num_rows(),
            "byte_count": ipc.len(),
            "attachment": STANDARD.encode(&ipc),
        });
    }
    body.to_string()
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

fn assert_client_query_timeout(error: &databricks_sql_kernel::Error, query_id: Option<&str>) {
    assert_eq!(error.code, ErrorCode::Timeout);
    assert_eq!(error.message, "Client query timeout expired");
    assert_eq!(error.sql_state.as_deref(), Some("HYT00"));
    assert!(!error.retryable);
    assert_eq!(error.query_id.as_deref(), query_id);
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
async fn execute_uses_manifest_nullability_only_for_empty_results_without_arrow_schema() {
    for mode in [
        "inline",
        "cloudfetch",
        "empty",
        "empty_without_chunk_count",
        "empty_with_arrow_schema",
    ] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_close_any_statement(&server).await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Int64, true),
            Field::new("col_1", DataType::Utf8, false),
            Field::new("col_2", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![42])) as _,
                Arc::new(StringArray::from(vec!["label"])) as _,
                Arc::new(StringArray::from(vec!["legacy"])) as _,
            ],
        )
        .unwrap();
        let mut body: serde_json::Value =
            serde_json::from_str(&succeeded_inline_body("stmt-metadata", &batch)).unwrap();
        body["manifest"]["schema"]["columns"] = json!([
            {"name": "id", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0,
             "nullable": false},
            {"name": "label", "type_name": "STRING", "type_text": "STRING", "position": 1,
             "nullable": true},
            {"name": "legacy", "type_name": "STRING", "type_text": "STRING", "position": 2,
             "nullable": null},
        ]);
        match mode {
            "cloudfetch" => {
                let ipc = arrow_ipc_bytes(&batch);
                body["result"] = json!({"external_links": [{
                    "external_link": format!("{}/chunk.arrow", server.uri()),
                    "expiration": "2099-01-01T00:00:00.000Z",
                    "chunk_index": 0, "row_offset": 0, "row_count": 1,
                    "byte_count": ipc.len(),
                }]});
                Mock::given(method("GET"))
                    .and(path("/chunk.arrow"))
                    .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc))
                    .expect(1)
                    .mount(&server)
                    .await;
            }
            "empty" | "empty_without_chunk_count" | "empty_with_arrow_schema" => {
                body.as_object_mut().unwrap().remove("result");
                body["manifest"]["total_row_count"] = json!(0);
                body["manifest"]["total_chunk_count"] = json!(0);
                body["manifest"]["total_byte_count"] = json!(0);
                if mode == "empty_with_arrow_schema" {
                    body["manifest"]["arrow_schema"] =
                        json!(STANDARD.encode(arrow_schema_ipc_bytes(&schema)));
                } else if mode == "empty_without_chunk_count" {
                    body["manifest"]
                        .as_object_mut()
                        .unwrap()
                        .remove("total_chunk_count");
                }
            }
            _ => {}
        }
        mount_execute(&server, body.to_string()).await;
        let session = open_session(&server).await;
        let mut statement = session.statement();
        statement.spec().sql("SELECT id, label, legacy FROM values");
        let mut executed = statement.execute().await.expect("execute");
        let advertised = executed.result_stream_mut().schema();
        let uses_manifest_nullability = matches!(mode, "empty" | "empty_without_chunk_count");
        assert_eq!(advertised.field(0).name(), "id", "{mode}");
        assert_eq!(
            advertised.field(0).is_nullable(),
            !uses_manifest_nullability,
            "{mode}"
        );
        assert_eq!(
            advertised.field(1).is_nullable(),
            uses_manifest_nullability,
            "{mode}"
        );
        assert_eq!(
            advertised.field(2).is_nullable(),
            uses_manifest_nullability,
            "{mode}"
        );
        let batches = drain(executed.result_stream_mut()).await;
        if mode.starts_with("empty") {
            assert!(batches.is_empty(), "{mode}");
        } else {
            assert_eq!(batches.len(), 1, "{mode}");
            assert_eq!(batches[0].schema(), advertised, "{mode}");
            assert_eq!(batches[0].num_rows(), 1, "{mode}");
            assert_eq!(
                batches[0]
                    .column(0)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .value(0),
                42
            );
        }
        executed.close().await.unwrap();
    }
}

#[tokio::test]
async fn execute_empty_result_string_conversions_keep_nullable_output() {
    for (complex_types_as_json, intervals_as_string) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            json!({
                "statement_id": "stmt-empty-converted",
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "format": "ARROW_STREAM",
                    "schema": {"column_count": 4, "columns": [
                        {"name": "array_value", "type_name": "ARRAY", "type_text": "ARRAY<INT>",
                         "position": 0, "nullable": false},
                        {"name": "struct_value", "type_name": "STRUCT", "type_text": "STRUCT<value:INT>",
                         "position": 1, "nullable": false},
                        {"name": "interval_value", "type_name": "INTERVAL", "type_text": "INTERVAL YEAR TO MONTH",
                         "type_interval_type": "YEAR_MONTH", "position": 2, "nullable": false},
                        {"name": "id", "type_name": "BIGINT", "type_text": "BIGINT",
                         "position": 3, "nullable": false},
                    ]},
                    "total_chunk_count": 0,
                    "total_row_count": 0,
                    "truncated": false,
                },
            })
            .to_string(),
        )
        .await;
        let mut result = ResultConfig::default();
        result.complex_types_as_json = complex_types_as_json;
        result.intervals_as_string = intervals_as_string;
        let session = open_session_with_result(&server, result).await;
        let mut statement = session.statement();
        statement
            .spec()
            .sql("SELECT array_value, struct_value, interval_value, id FROM values WHERE 1=0");
        let mut executed = statement.execute().await.expect("execute");
        let schema = executed.schema();
        for index in [0, 1] {
            assert_eq!(schema.field(index).is_nullable(), complex_types_as_json);
            if complex_types_as_json {
                assert_eq!(schema.field(index).data_type(), &DataType::Utf8);
            }
        }
        if !complex_types_as_json {
            assert!(matches!(schema.field(0).data_type(), DataType::List(_)));
            assert!(matches!(schema.field(1).data_type(), DataType::Struct(_)));
        }
        assert_eq!(schema.field(2).is_nullable(), intervals_as_string);
        assert_eq!(
            schema.field(2).data_type(),
            &if intervals_as_string {
                DataType::Utf8
            } else {
                DataType::Interval(arrow_schema::IntervalUnit::YearMonth)
            }
        );
        assert!(!schema.field(3).is_nullable());
        assert!(drain(executed.result_stream_mut()).await.is_empty());
        executed.close().await.unwrap();
    }
}

#[tokio::test]
async fn execute_inline_streaming_fallback_does_not_apply_manifest_nullability() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "col_0",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![None::<i64>]))],
    )
    .unwrap();
    let mut body: serde_json::Value =
        serde_json::from_str(&succeeded_inline_body("stmt-streaming", &batch)).unwrap();
    body["manifest"]["schema"]["columns"][0]["nullable"] = json!(false);
    body["manifest"]["total_chunk_count"] = json!(2);
    let empty_chunk = arrow_schema_ipc_bytes(&schema);
    body["result"]["attachment"] = json!(STANDARD.encode(&empty_chunk));
    body["result"]["byte_count"] = json!(empty_chunk.len());
    body["result"]["row_count"] = json!(0);
    body["result"]["next_chunk_index"] = json!(1);
    mount_execute(&server, body.to_string()).await;
    Mock::given(method("GET"))
        .and(path(
            "/api/2.0/sql/statements/stmt-streaming/result/chunks/1",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "chunk_index": 1,
            "row_offset": 0,
            "row_count": 1,
            "attachment": STANDARD.encode(arrow_ipc_bytes(&batch)),
        })))
        .expect(1)
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut statement = session.statement();
    statement.spec().sql("SELECT value FROM nullable_values");
    let mut executed = statement.execute().await.expect("execute");
    let advertised = executed.schema();
    let batches = drain(executed.result_stream_mut()).await;
    assert!(advertised.field(0).is_nullable());
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema(), advertised);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].column(0).null_count(), 1);
    executed.close().await.unwrap();
}

#[tokio::test]
async fn execute_geospatial_string_mode_uses_manifest_tags_for_untagged_arrow_ipc() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let batch = geospatial_batch();
    mount_execute(
        &server,
        succeeded_geospatial_inline_body("stmt-geo-string", &batch),
    )
    .await;

    let session =
        open_session_with_geospatial(&server, GeospatialResultRepresentation::String).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT geom, geog FROM geo_values");
    let mut exec = stmt.execute().await.expect("execute");
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches.len(), 1);
    let out = &batches[0];
    assert_eq!(out.schema().field(0).name(), "geom");
    assert_eq!(out.schema().field(1).name(), "geog");
    assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(out.schema().field(1).data_type(), &DataType::Utf8);
    let geometry = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let geography = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(geometry.value(0), "SRID=3857;POINT(1 2)");
    assert_eq!(geography.value(0), "SRID=4326;POINT(1 2)");
    assert!(geometry.is_null(1));
    assert!(geography.is_null(1));
    assert_eq!(
        out.schema()
            .field(0)
            .metadata()
            .get("databricks.type_text")
            .map(String::as_str),
        Some("GEOMETRY(ANY)")
    );
    exec.close().await.expect("close");

    let requests = server.received_requests().await.unwrap();
    let create = requests
        .iter()
        .find(|request| request.url.path() == "/api/2.0/sql/warehouses/test_wh/sessions")
        .expect("create-session request");
    let body: serde_json::Value = serde_json::from_slice(&create.body).unwrap();
    let forwarded_geo_setting = body
        .get("session_confs")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|confs| {
            confs
                .keys()
                .any(|key| key.eq_ignore_ascii_case("EnableGeoSpatialSupport"))
        });
    assert!(
        !forwarded_geo_setting,
        "geospatial representation is client-side and must not reach SEA"
    );
}

#[tokio::test]
async fn execute_default_geospatial_mode_applies_manifest_tags_to_arrow_ipc() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let batch = geospatial_batch();
    mount_execute(
        &server,
        succeeded_geospatial_inline_body("stmt-geo-binary", &batch),
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT geom, geog FROM geo_values");
    let mut exec = stmt.execute().await.expect("execute");
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches.len(), 1);
    let out = &batches[0];

    let geometry = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        geometry
            .column_by_name("srid")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        3857
    );
    assert_eq!(
        geometry
            .column_by_name("wkb")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        POINT_1_2_WKB
    );
    assert!(geometry.is_null(1));

    let schema = out.schema();
    let DataType::Struct(geometry_fields) = schema.field(0).data_type() else {
        panic!("geometry must stay a tagged struct")
    };
    assert_eq!(
        geometry_fields[1]
            .metadata()
            .get("geometry")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        geometry_fields[1]
            .metadata()
            .get("srid")
            .map(String::as_str),
        Some("-1")
    );
    let DataType::Struct(geography_fields) = schema.field(1).data_type() else {
        panic!("geography must stay a tagged struct")
    };
    assert_eq!(
        geography_fields[1]
            .metadata()
            .get("geography")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        geography_fields[1]
            .metadata()
            .get("srid")
            .map(String::as_str),
        Some("4326")
    );
    exec.close().await.expect("close");
}

#[tokio::test]
async fn execute_legacy_geospatial_text_is_preserved() {
    for data_type in [DataType::Utf8, DataType::LargeUtf8] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_close_any_statement(&server).await;
        let batch = legacy_geospatial_batch(data_type.clone());
        mount_execute(
            &server,
            succeeded_legacy_geospatial_body(
                &format!("stmt-legacy-geo-{data_type:?}"),
                &batch,
                true,
            ),
        )
        .await;

        let session =
            open_session_with_geospatial(&server, GeospatialResultRepresentation::String).await;
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT geom, geog FROM legacy_geo_values");
        let mut exec = stmt.execute().await.expect("execute");
        let batches = drain(exec.result_stream_mut()).await;
        assert_eq!(batches.len(), 1);
        let output = &batches[0];
        assert_eq!(output.schema().field(0).data_type(), &data_type);
        assert_eq!(output.schema().field(1).data_type(), &data_type);
        let value = match data_type {
            DataType::Utf8 => output.column(0).as_string::<i32>().value(0),
            DataType::LargeUtf8 => output.column(0).as_string::<i64>().value(0),
            _ => unreachable!("test covers both text representations"),
        };
        assert_eq!(value, "SRID=3857;POINT (1 2)");
        assert!(output.column(0).is_null(1));
        exec.close().await.expect("close");
    }
}

#[tokio::test]
async fn execute_standard_warehouse_geospatial_text_converts_to_binary_mode() {
    for data_type in [DataType::Utf8, DataType::LargeUtf8] {
        for include_rows in [true, false] {
            let server = MockServer::start().await;
            mount_create_session(&server).await;
            mount_delete_session(&server).await;
            mount_close_any_statement(&server).await;
            let batch = legacy_geospatial_batch(data_type.clone());
            mount_execute(
                &server,
                succeeded_legacy_geospatial_body(
                    &format!("stmt-legacy-geo-binary-{data_type:?}-{include_rows}"),
                    &batch,
                    include_rows,
                ),
            )
            .await;

            let session =
                open_session_with_geospatial(&server, GeospatialResultRepresentation::Binary).await;
            let mut stmt = session.statement();
            stmt.spec()
                .sql("SELECT geom, geog FROM standard_geo_values");
            let mut exec = stmt.execute().await.expect("execute");

            let schema = exec.schema();
            for (index, family, type_srid) in [(0, "geometry", "-1"), (1, "geography", "4326")] {
                let DataType::Struct(fields) = schema.field(index).data_type() else {
                    panic!("binary geo field must use the tagged struct encoding")
                };
                assert_eq!(
                    fields[1].metadata().get(family).map(String::as_str),
                    Some("true")
                );
                assert_eq!(
                    fields[1].metadata().get("srid").map(String::as_str),
                    Some(type_srid)
                );
            }

            let batches = drain(exec.result_stream_mut()).await;
            if include_rows {
                assert_eq!(batches.len(), 1);
                let geometry = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let srids = geometry
                    .column_by_name("srid")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let wkbs = geometry
                    .column_by_name("wkb")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                assert_eq!(srids.value(0), 3857);
                assert_eq!(wkbs.value(0), POINT_1_2_WKB);
                assert!(geometry.is_null(1));
            } else {
                assert!(batches.is_empty());
            }
            exec.close().await.expect("close");
        }
    }
}

#[tokio::test]
async fn execute_nested_native_and_text_geospatial_preserve_mixed_binary_field() {
    for native_source in [true, false] {
        for representation in [
            GeospatialResultRepresentation::String,
            GeospatialResultRepresentation::Binary,
        ] {
            let server = MockServer::start().await;
            mount_create_session(&server).await;
            mount_delete_session(&server).await;
            mount_close_any_statement(&server).await;
            let batch = nested_geospatial_batch(native_source);
            let statement_id = format!(
                "stmt-nested-geo-{}-{}",
                if native_source { "native" } else { "text" },
                if representation == GeospatialResultRepresentation::Binary {
                    "binary"
                } else {
                    "string"
                }
            );
            mount_execute(
                &server,
                succeeded_nested_geospatial_body(&statement_id, &batch),
            )
            .await;

            let session = open_session_with_geospatial(&server, representation).await;
            let mut stmt = session.statement();
            stmt.spec()
                .sql("SELECT named_struct('geom', geom, 'blob', blob)");
            let mut exec = stmt.execute().await.expect("execute");
            let batches = drain(exec.result_stream_mut()).await;
            assert_eq!(batches.len(), 1);

            let mixed = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let blob = mixed
                .column_by_name("blob")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            assert_eq!(blob.value(0), b"\x00\xffblob");
            assert_eq!(blob.value(1), b"still here");

            match representation {
                GeospatialResultRepresentation::String => {
                    let geom = mixed
                        .column_by_name("geom")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(
                        geom.value(0),
                        if native_source {
                            "SRID=4326;POINT(1 2)"
                        } else {
                            "SRID=4326;POINT (1 2)"
                        }
                    );
                    assert!(geom.is_null(1));
                }
                GeospatialResultRepresentation::Binary => {
                    let geom = mixed
                        .column_by_name("geom")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap();
                    let DataType::Struct(fields) = geom.data_type() else {
                        unreachable!()
                    };
                    assert_eq!(
                        fields[1].metadata().get("geometry").map(String::as_str),
                        Some("true")
                    );
                    let srids = geom
                        .column_by_name("srid")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let wkbs = geom
                        .column_by_name("wkb")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    assert_eq!(srids.value(0), 4326);
                    assert_eq!(wkbs.value(0), POINT_1_2_WKB);
                    assert!(geom.is_null(1));
                }
                _ => unreachable!("test covers every current representation"),
            }
            exec.close().await.expect("close");
        }
    }
}

#[tokio::test]
async fn execute_cloudfetch_geospatial_reconciles_manifest_and_untagged_payload() {
    for representation in [
        GeospatialResultRepresentation::String,
        GeospatialResultRepresentation::Binary,
    ] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_close_any_statement(&server).await;

        let statement_id = match representation {
            GeospatialResultRepresentation::String => "stmt-cloud-geo-string",
            GeospatialResultRepresentation::Binary => "stmt-cloud-geo-binary",
            _ => unreachable!("test covers every current representation"),
        };
        let chunk_path = format!("/cloud-fetch/{statement_id}/chunk-0.arrow");
        let chunk_url = format!("{}{}", server.uri(), chunk_path);
        let batch = geospatial_batch();
        let ipc = arrow_ipc_bytes(&batch);
        // CloudFetch preserves the physical untagged IPC schema in
        // manifest.arrow_schema. The structured manifest schema remains the
        // logical source of GEOMETRY / GEOGRAPHY identity.
        let physical_schema = arrow_schema_ipc_bytes(batch.schema().as_ref());
        let execute_body = json!({
            "statement_id": statement_id,
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": 2,
                    "columns": [
                        {
                            "name": "geom",
                            "type_name": "GEOMETRY",
                            "type_text": "GEOMETRY(ANY)",
                            "position": 0,
                        },
                        {
                            "name": "geog",
                            "type_name": "GEOGRAPHY",
                            "type_text": "GEOGRAPHY(4326)",
                            "position": 1,
                        }
                    ],
                },
                "arrow_schema": STANDARD.encode(physical_schema),
                "total_chunk_count": 1,
                "total_row_count": batch.num_rows() as i64,
                "total_byte_count": ipc.len() as i64,
                "truncated": false,
            },
            "result": {
                "external_links": [{
                    "external_link": chunk_url,
                    "expiration": "2099-01-01T00:00:00.000Z",
                    "chunk_index": 0,
                    "row_offset": 0,
                    "row_count": batch.num_rows() as i64,
                    "byte_count": ipc.len() as i64,
                }],
            },
        })
        .to_string();
        mount_execute(&server, execute_body).await;
        Mock::given(method("GET"))
            .and(path(chunk_path.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc))
            .expect(1)
            .mount(&server)
            .await;

        let session = open_session_with_geospatial(&server, representation).await;
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT geom, geog FROM geo_values");
        let mut exec = stmt.execute().await.expect("execute");
        let batches = drain(exec.result_stream_mut()).await;
        assert_eq!(batches.len(), 1);
        let schema = batches[0].schema();
        match representation {
            GeospatialResultRepresentation::String => {
                assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
                let geometry = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(geometry.value(0), "SRID=3857;POINT(1 2)");
            }
            GeospatialResultRepresentation::Binary => {
                let DataType::Struct(fields) = schema.field(0).data_type() else {
                    panic!("binary geometry must be a tagged struct")
                };
                assert_eq!(
                    fields[1].metadata().get("geometry").map(String::as_str),
                    Some("true")
                );
                assert_eq!(
                    fields[1].metadata().get("srid").map(String::as_str),
                    Some("-1")
                );
            }
            _ => unreachable!("test covers every current representation"),
        }
        exec.close().await.expect("close");
    }
}

#[tokio::test]
async fn execute_array_child_names_match_for_empty_and_non_empty_results() {
    let array = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("element", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        )
        .unwrap(),
    );
    let nested = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("element", array.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1].into()),
            array.clone(),
            None,
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", array.data_type().clone(), true),
            Field::new("nested", nested.data_type().clone(), true),
        ])),
        vec![array, nested],
    )
    .unwrap();
    let mut schemas = Vec::new();

    for empty in [false, true] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_close_any_statement(&server).await;
        let mut body = json!({
            "statement_id": "stmt-arrays",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": 2,
                    "columns": [
                        {
                            "name": "a",
                            "type_name": "ARRAY",
                            "type_text": "ARRAY<INT>",
                            "position": 0,
                        },
                        {
                            "name": "nested",
                            "type_name": "ARRAY",
                            "type_text": "ARRAY<ARRAY<INT>>",
                            "position": 1,
                        },
                    ],
                },
                "total_chunk_count": if empty { 0 } else { 1 },
                "total_row_count": if empty { 0 } else { 1 },
                "truncated": false,
            },
        });
        if !empty {
            body["result"] = json!({ "attachment": STANDARD.encode(arrow_ipc_bytes(&batch)) });
        }
        mount_execute(&server, body.to_string()).await;

        let session = open_session(&server).await;
        let mut stmt = session.statement();
        stmt.spec().sql(if empty {
            "SELECT a, nested FROM array_values WHERE 1=0"
        } else {
            "SELECT a, nested FROM array_values"
        });
        let mut exec = stmt.execute().await.expect("execute");
        schemas.push(exec.schema());
        let batches = drain(exec.result_stream_mut()).await;
        if empty {
            assert!(batches.is_empty());
        } else {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            for i in 0..batch.num_columns() {
                assert_eq!(batches[0].column(i).to_data(), batch.column(i).to_data());
            }
        }
        exec.close().await.expect("close");
    }

    assert_eq!(schemas[0], schemas[1]);
}

#[tokio::test]
async fn execute_empty_geospatial_result_has_the_requested_client_schema() {
    for representation in [
        GeospatialResultRepresentation::String,
        GeospatialResultRepresentation::Binary,
    ] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_close_any_statement(&server).await;
        mount_execute(&server, succeeded_empty_geospatial_body("stmt-empty-geo")).await;

        let session = open_session_with_geospatial(&server, representation).await;
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT geom, geog FROM empty_geo_values");
        let mut exec = stmt.execute().await.expect("execute");
        let schema = exec.schema();
        assert_eq!(schema.field(0).name(), "geom");
        assert_eq!(schema.field(1).name(), "geog");
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get("databricks.type_text")
                .map(String::as_str),
            Some("GEOMETRY(ANY)")
        );

        match representation {
            GeospatialResultRepresentation::String => {
                assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
                assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
            }
            GeospatialResultRepresentation::Binary => {
                for (index, family, type_srid) in [(0, "geometry", "-1"), (1, "geography", "4326")]
                {
                    let DataType::Struct(fields) = schema.field(index).data_type() else {
                        panic!("binary geo field must use the tagged struct encoding")
                    };
                    assert_eq!(
                        fields[1].metadata().get(family).map(String::as_str),
                        Some("true")
                    );
                    assert_eq!(
                        fields[1].metadata().get("srid").map(String::as_str),
                        Some(type_srid)
                    );
                }
            }
            _ => unreachable!("test covers every current representation"),
        }

        assert!(drain(exec.result_stream_mut()).await.is_empty());
        exec.close().await.expect("close");
    }
}

#[tokio::test]
async fn execute_empty_legacy_geospatial_result_uses_physical_text_schema() {
    for data_type in [DataType::Utf8, DataType::LargeUtf8] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_close_any_statement(&server).await;
        let batch = legacy_geospatial_batch(data_type.clone());
        mount_execute(
            &server,
            succeeded_legacy_geospatial_body(
                &format!("stmt-empty-legacy-geo-{data_type:?}"),
                &batch,
                false,
            ),
        )
        .await;

        let session =
            open_session_with_geospatial(&server, GeospatialResultRepresentation::String).await;
        let mut stmt = session.statement();
        stmt.spec()
            .sql("SELECT geom, geog FROM empty_legacy_geo_values");
        let mut exec = stmt.execute().await.expect("execute");
        assert_eq!(exec.schema().field(0).data_type(), &data_type);
        assert_eq!(exec.schema().field(1).data_type(), &data_type);
        assert!(drain(exec.result_stream_mut()).await.is_empty());
        exec.close().await.expect("close");
    }
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
async fn execute_with_cloudfetch_disabled_supports_paged_arrow_results() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let batch = int64_batch("n", vec![7]);
    let mut body: serde_json::Value =
        serde_json::from_str(&succeeded_inline_body("stmt-cf-off", &batch)).unwrap();
    body["manifest"]
        .as_object_mut()
        .unwrap()
        .remove("total_chunk_count");
    body["manifest"]
        .as_object_mut()
        .unwrap()
        .remove("total_row_count");
    body["result"]["next_chunk_index"] = json!(1);
    mount_execute(&server, body.to_string()).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-cf-off/result/chunks/1"))
        .and(query_param("row_offset", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "chunk_index": 1,
            "row_offset": 1,
            "row_count": 1,
            "attachment": STANDARD.encode(arrow_ipc_bytes(&int64_batch("n", vec![8]))),
        })))
        .expect(1)
        .mount(&server)
        .await;

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
    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        7
    );
    assert_eq!(
        batches[1]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        8
    );
    exec.close().await.expect("close");

    assert_eq!(
        recorded_execute_disposition(&server).await,
        "INLINE_OR_EXTERNAL_LINKS",
    );
}

const RT_STATEMENTS: &str = "/api/2.0/sql/warehouses/test_wh/sessions/rt-sess/statements";

/// A realtime (Reyden) session: the CreateSession response carries the
/// realtime header and statements go to the warehouse-session route.
async fn open_realtime_session(server: &MockServer) -> Session {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-databricks-rt-session", "true")
                .set_body_json(
                    json!({"session_id": "rt-sess", "session_version": {"version_id": 1}}),
                ),
        )
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(server)
        .await;
    open_session_with_result(server, ResultConfig::default()).await
}

async fn recorded_execute_dispositions(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .expect("wiremock received_requests must be available")
        .iter()
        .filter(|r| r.method.as_str() == "POST" && r.url.path().ends_with("/statements"))
        .map(|r| {
            let body: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
            body["disposition"].as_str().unwrap().to_string()
        })
        .collect()
}

async fn run_and_drain(session: &Session, sql: &str) -> databricks_sql_kernel::Result<usize> {
    let mut stmt = session.statement();
    stmt.spec().sql(sql);
    let mut exec = stmt.execute().await?;
    let rows = drain(exec.result_stream_mut())
        .await
        .iter()
        .map(|b| b.num_rows())
        .sum();
    exec.close().await?;
    Ok(rows)
}

#[tokio::test]
async fn realtime_repeat_select_with_small_result_uses_inline() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(RT_STATEMENTS))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(succeeded_inline_body("stmt-1", &int64_batch("n", vec![7]))),
        )
        .mount(&server)
        .await;
    let session = open_realtime_session(&server).await;
    for sql in [
        "SELECT 7 AS n",
        "SELECT 7 AS n",
        " -- comment\n(SELECT 8 AS n)",
        " -- comment\n(SELECT 8 AS n)",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (1)",
    ] {
        assert_eq!(run_and_drain(&session, sql).await.expect(sql), 1);
    }
    assert_eq!(
        recorded_execute_dispositions(&server).await,
        [
            "INLINE_OR_EXTERNAL_LINKS",
            "INLINE",
            "INLINE_OR_EXTERNAL_LINKS",
            "INLINE",
            "INLINE_OR_EXTERNAL_LINKS",
            "INLINE_OR_EXTERNAL_LINKS",
        ]
    );
    session.close().await.expect("close session");
}

#[tokio::test]
async fn realtime_inline_overflow_reruns_with_external_links() {
    use wiremock::matchers::body_string_contains;
    let server = MockServer::start().await;
    let mut overflow: serde_json::Value = serde_json::from_str(&failed_body(
        "stmt-inline",
        "Inline byte limit exceeded",
        "42000",
    ))
    .unwrap();
    overflow["status"]["error"]["error_code"] = json!("BAD_REQUEST");
    Mock::given(method("POST"))
        .and(path(RT_STATEMENTS))
        .and(body_string_contains(r#""disposition":"INLINE""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(overflow))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(RT_STATEMENTS))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(succeeded_inline_body("stmt-1", &int64_batch("n", vec![7]))),
        )
        .with_priority(2)
        .mount(&server)
        .await;
    let session = open_realtime_session(&server).await;
    assert_eq!(run_and_drain(&session, "SELECT 7 AS n").await.unwrap(), 1);
    assert_eq!(
        run_and_drain(&session, "SELECT 7 AS n")
            .await
            .expect("re-run must succeed"),
        1
    );
    assert_eq!(
        recorded_execute_dispositions(&server).await,
        [
            "INLINE_OR_EXTERNAL_LINKS",
            "INLINE",
            "INLINE_OR_EXTERNAL_LINKS"
        ]
    );
    session.close().await.expect("close session");
}

#[tokio::test]
async fn realtime_truncated_inline_result_reruns_with_external_links() {
    use wiremock::matchers::body_string_contains;
    let server = MockServer::start().await;
    let mut truncated: serde_json::Value = serde_json::from_str(&succeeded_inline_body(
        "stmt-inline",
        &int64_batch("n", vec![7]),
    ))
    .unwrap();
    truncated["manifest"]["truncated"] = json!(true);
    Mock::given(method("POST"))
        .and(path(RT_STATEMENTS))
        .and(body_string_contains(r#""disposition":"INLINE""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(truncated))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(RT_STATEMENTS))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(succeeded_inline_body(
                "stmt-1",
                &int64_batch("n", vec![7, 8]),
            )),
        )
        .with_priority(2)
        .mount(&server)
        .await;
    let session = open_realtime_session(&server).await;
    assert_eq!(run_and_drain(&session, "SELECT n FROM t").await.unwrap(), 2);
    assert_eq!(run_and_drain(&session, "SELECT n FROM t").await.unwrap(), 2);
    assert_eq!(
        recorded_execute_dispositions(&server).await,
        [
            "INLINE_OR_EXTERNAL_LINKS",
            "INLINE",
            "INLINE_OR_EXTERNAL_LINKS"
        ]
    );
    session.close().await.expect("close session");
}

#[tokio::test]
async fn realtime_inline_small_results_can_be_disabled_through_session_conf() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-databricks-rt-session", "true")
                .set_body_json(
                    json!({"session_id": "rt-sess", "session_version": {"version_id": 1}}),
                ),
        )
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(RT_STATEMENTS))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(succeeded_inline_body("stmt-1", &int64_batch("n", vec![7]))),
        )
        .mount(&server)
        .await;
    let session = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh").unwrap(),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .session_conf(std::collections::HashMap::from([(
            "realtime_inline_small_results".into(),
            "false".into(),
        )]))
        .open()
        .await
        .expect("session open");
    run_and_drain(&session, "SELECT 7 AS n").await.unwrap();
    run_and_drain(&session, "SELECT 7 AS n").await.unwrap();
    assert_eq!(
        recorded_execute_dispositions(&server).await,
        ["INLINE_OR_EXTERNAL_LINKS", "INLINE_OR_EXTERNAL_LINKS"]
    );
    let create = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.url.path() == "/api/2.0/sql/warehouses/test_wh/sessions")
        .unwrap();
    assert!(!String::from_utf8_lossy(&create.body).contains("realtime_inline_small_results"));
    session.close().await.expect("close session");
}

#[tokio::test]
async fn standard_route_never_uses_inline() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_execute(
        &server,
        succeeded_inline_body("stmt-1", &int64_batch("n", vec![7])),
    )
    .await;
    let session = open_session_with_result(&server, ResultConfig::default()).await;
    run_and_drain(&session, "SELECT 7 AS n").await.unwrap();
    run_and_drain(&session, "SELECT 7 AS n").await.unwrap();
    assert_eq!(
        recorded_execute_dispositions(&server).await,
        ["INLINE_OR_EXTERNAL_LINKS", "INLINE_OR_EXTERNAL_LINKS"]
    );
    session.close().await.expect("close session");
}

#[tokio::test]
async fn cloudfetch_disabled_falls_back_to_inline_when_warehouse_rejects_can_cloud_download() {
    use wiremock::matchers::body_string_contains;
    // Reyden rejects the paged-inline session conf at CreateSession; the
    // session must still open and request INLINE results instead.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .and(body_string_contains("can_cloud_download"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error_code": "INVALID_PARAMETER_VALUE",
            "message": "Invalid session params: [INVALID_CONF_VALUE] The value 'false' in the \
                        config \"can_cloud_download\" is invalid. SQLSTATE: 22022",
        })))
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-test","warehouse_id":"test_wh"}"#),
        )
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_execute(
        &server,
        succeeded_inline_body("stmt-inline", &int64_batch("n", vec![7])),
    )
    .await;

    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = open_session_with_result(&server, result).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 7 AS n");
    let mut exec = stmt.execute().await.expect("inline execute");
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches.len(), 1);
    exec.close().await.expect("close");
    assert_eq!(recorded_execute_disposition(&server).await, "INLINE");
    session.close().await.expect("close session");
}

#[tokio::test]
async fn cloudfetch_disabled_surfaces_other_create_session_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error_code": "INVALID_PARAMETER_VALUE",
            "message": "Invalid session params: [INVALID_CONF_VALUE] TIMEZONE is invalid.",
        })))
        .expect(1)
        .mount(&server)
        .await;
    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let error = match Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh").unwrap(),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .result(result)
        .open()
        .await
    {
        Ok(_) => panic!("unrelated CreateSession rejection must surface"),
        Err(error) => error,
    };
    assert_eq!(error.http_status, Some(400));
    assert!(error.message.contains("TIMEZONE"), "{error}");
}

#[tokio::test]
async fn execute_with_cloudfetch_disabled_rejects_external_links_without_downloading() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-cf-off"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount(&server)
        .await;
    let batch = int64_batch("n", vec![7]);
    let mut body: serde_json::Value =
        serde_json::from_str(&succeeded_inline_body("stmt-cf-off", &batch)).unwrap();
    body["result"] = json!({"external_links": [{
        "external_link": format!("{}/storage/chunk.arrow", server.uri()),
        "chunk_index": 0, "row_offset": 0, "row_count": 1,
        "byte_count": 100, "expiration": "2099-01-01T00:00:00Z",
    }]});
    mount_execute(&server, body.to_string()).await;
    Mock::given(method("GET"))
        .and(path("/storage/chunk.arrow"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = open_session_with_result(&server, result).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 7 AS n");
    let error = match stmt.execute().await {
        Ok(_) => panic!("external links must be rejected when CloudFetch is disabled"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::DataLoss);
    assert!(
        error.to_string().contains("CloudFetch is disabled"),
        "{error}"
    );
    stmt.close().await.expect("close statement");
    session.close().await.expect("close session");
    assert_eq!(
        wait_for_close_delete(&server, "stmt-cf-off", 1, Duration::from_secs(2)).await,
        1,
    );
}

#[tokio::test]
async fn cloudfetch_disabled_rejects_continuation_links_without_exposing_them() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let mut body: serde_json::Value = serde_json::from_str(&succeeded_inline_body(
        "stmt-cf-off",
        &int64_batch("n", vec![7]),
    ))
    .unwrap();
    body["manifest"]
        .as_object_mut()
        .unwrap()
        .remove("total_chunk_count");
    body["manifest"]
        .as_object_mut()
        .unwrap()
        .remove("total_row_count");
    body["result"]["next_chunk_index"] = json!(1);
    mount_execute(&server, body.to_string()).await;
    let storage_link = format!(
        "{}/storage/chunk.arrow?signature=private-value",
        server.uri()
    );
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-cf-off/result/chunks/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "external_links": [{
                "external_link": storage_link,
                "chunk_index": 1, "row_offset": 1, "row_count": 1,
                "byte_count": 100, "expiration": "2099-01-01T00:00:00Z",
            }],
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/storage/chunk.arrow"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = open_session_with_result(&server, result).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 7 AS n");
    let mut exec = stmt.execute().await.expect("initial inline response");
    let error = loop {
        match exec.result_stream_mut().next_batch().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("continuation links must be rejected"),
            Err(error) => break error,
        }
    };
    assert_eq!(error.code, ErrorCode::DataLoss);
    assert!(error.to_string().contains("missing `attachment`"));
    assert!(!error.to_string().contains("private-value"));
    assert!(!error.to_string().contains(&storage_link));
    exec.close().await.expect("close statement");
    session.close().await.expect("close session");
}

#[tokio::test]
async fn cloudfetch_disabled_preserves_unsupported_server_error() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    let mut body: serde_json::Value = serde_json::from_str(&failed_body(
        "stmt-unsupported-inline",
        "[CONFIG_NOT_AVAILABLE] Configuration can_cloud_download is not available",
        "42K0I",
    ))
    .unwrap();
    body["status"]["error"]["error_code"] = json!("CONFIG_NOT_AVAILABLE");
    mount_execute(&server, body.to_string()).await;

    let mut result = ResultConfig::default();
    result.cloudfetch_enabled = false;
    let session = open_session_with_result(&server, result).await;
    let mut statement = session.statement();
    statement.spec().sql("SELECT 1");
    let error = match statement.execute().await {
        Ok(_) => panic!("unsupported inline delivery must fail"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::SqlError);
    assert_eq!(error.sql_state.as_deref(), Some("42K0I"));
    assert!(error.message.contains("CONFIG_NOT_AVAILABLE"), "{error}");
    assert!(error.message.contains("can_cloud_download"), "{error}");
    statement.close().await.expect("close statement");
    session.close().await.expect("close session");
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
    // synchronous poll loop must hand the **terminal** status
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
async fn polling_path_materializes_dml_count_after_terminal_state() {
    // The synchronous poll loop deliberately reads raw status responses. DML
    // count extraction belongs to result materialisation, but the public
    // ExecutedStatement must still expose the same count before this refactor.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_execute(&server, running_body("stmt-polled-dml")).await;
    let batch = dml_count_batch(9, 4, 3, 2);
    mount_status(
        &server,
        "stmt-polled-dml",
        succeeded_inline_body("stmt-polled-dml", &batch),
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("UPDATE t SET x = x + 1");
    let mut exec = stmt
        .execute()
        .await
        .expect("execute should poll and materialize the DML result");

    assert_eq!(exec.num_modified_rows(), Some(9));

    // DML extraction peeks rather than consumes: the summary row remains
    // available through the normal result stream.
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        9
    );

    exec.close().await.expect("close");
}

#[tokio::test]
async fn execute_polled_failure_carries_extended_status_fields_onto_error() {
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
    mount_execute(&server, running_body("stmt-fail-ext")).await;
    mount_status(&server, "stmt-fail-ext", body).await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT * FROM does_not_exist");
    let err = match stmt.execute().await {
        Ok(_) => panic!("execute should fail on FAILED state"),
        Err(e) => e,
    };

    assert_eq!(err.code, ErrorCode::SqlError);
    assert_eq!(err.query_id.as_deref(), Some("stmt-fail-ext"));
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

    let recorded = server
        .received_requests()
        .await
        .expect("wiremock received_requests must be available");
    assert!(
        recorded.iter().any(|request| {
            request.method == wiremock::http::Method::DELETE
                && request.url.path() == "/api/2.0/sql/statements/stmt-fail-ext"
        }),
        "polled terminal failure must preserve the existing orphan-close behavior",
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

/// The non-blocking `ExecutedStatement::try_next_batch` must preserve the same
/// drain-driven auto-close as the blocking `next_batch`: the first time it
/// observes `TryNextBatch::End` it fires exactly one fire-and-forget
/// `CloseStatement` (DELETE) on a `SUCCEEDED` (server-owned) statement. This
/// pins the `State::End => { self.spawn_close_if_open(); ... }` arm, which the
/// doc comment claims "preserves the same drain-driven auto-close behavior" —
/// a claim otherwise covered only on the blocking sibling.
#[tokio::test]
async fn try_next_batch_drain_auto_closes_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let batch = int64_batch("n", vec![1]);
    // SUCCEEDED (not CLOSED): the server still owns the statement, so draining
    // to End must trigger exactly one CloseStatement.
    mount_execute(&server, succeeded_inline_body("stmt-try-drain", &batch)).await;

    let _close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-try-drain"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let mut exec = stmt.execute().await.expect("execute");

        // Drain via the non-blocking API. The inline direct-result payload is
        // already buffered, so try_next_batch never has to report Pending here;
        // a defensive bound keeps a regression from spinning forever.
        let mut saw_ready = false;
        let mut saw_end = false;
        for _ in 0..1000 {
            match exec.try_next_batch().expect("try_next_batch") {
                TryNextBatch::Ready(_) => saw_ready = true,
                TryNextBatch::Pending => {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                TryNextBatch::End => {
                    saw_end = true;
                    break;
                }
                _ => unreachable!("TryNextBatch has only Ready/Pending/End today"),
            }
        }
        assert!(saw_ready, "the one buffered batch must surface as Ready");
        assert!(saw_end, "draining must reach End");
        // No explicit close(): the End-arm auto-close is the only path that can
        // fire the DELETE below. exec/stmt drop at end of scope.
    }

    let closes = wait_for_close_delete(
        &server,
        "stmt-try-drain",
        1,
        std::time::Duration::from_millis(2000),
    )
    .await;
    assert_eq!(
        closes, 1,
        "try_next_batch draining to End must trigger exactly one CloseStatement"
    );
    // `_close` also verifies expect(1) when it drops at end of scope.
}

// ─── Client query timeout ───────────────────────────────────────────

#[tokio::test]
async fn client_timeout_covers_initial_post_and_statement_is_immediately_reusable() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let batch = int64_batch("n", vec![1]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(DelayedFirstExecute {
            first: running_body("stmt-delayed-post"),
            later: closed_inline_body("stmt-reused", &batch),
            first_delay: Duration::from_millis(500),
            calls: AtomicUsize::new(0),
        })
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");

    let started = Instant::now();
    let error = stmt
        .execute_with_timeout(Duration::from_millis(75))
        .await
        .err()
        .expect("the delayed initial POST must consume the client deadline");
    assert_client_query_timeout(&error, None);
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "the 500ms POST must be abandoned near the 75ms deadline"
    );

    for timeout in [Duration::ZERO, Duration::MAX, MAX_CLIENT_QUERY_TIMEOUT] {
        let exec = stmt
            .execute_with_timeout(timeout)
            .await
            .expect("statement must be reusable after an initial-POST timeout");
        exec.close().await.expect("close reused execution");
    }

    let oversized = MAX_CLIENT_QUERY_TIMEOUT
        .checked_add(Duration::from_millis(1))
        .unwrap();
    let error = stmt
        .execute_with_timeout(oversized)
        .await
        .err()
        .expect("a finite timeout above the public maximum must be rejected");
    assert_eq!(error.code, ErrorCode::InvalidArgument);

    let execute_posts = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|request| {
            request.method == wiremock::http::Method::POST
                && request.url.path() == "/api/2.0/sql/statements"
        })
        .count();
    assert_eq!(
        execute_posts, 4,
        "range validation must reject before issuing a fifth execute RPC"
    );
}

#[tokio::test]
async fn client_timeout_covers_poll_sleep_and_cleanup_does_not_delay_return() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let running = Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(running_body("stmt-poll-sleep")))
        .expect(1)
        .mount_as_scoped(&server)
        .await;
    let no_status_poll = Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-poll-sleep"))
        .respond_with(ResponseTemplate::new(200).set_body_string(running_body("stmt-poll-sleep")))
        .expect(0)
        .mount_as_scoped(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-poll-sleep/cancel"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_string("already gone")
                .set_delay(Duration::from_millis(750)),
        )
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-poll-sleep"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");

    let started = Instant::now();
    let error = stmt
        .execute_with_timeout(Duration::from_millis(75))
        .await
        .err()
        .expect("the first 500ms poll sleep must be bounded");
    assert_client_query_timeout(&error, Some("stmt-poll-sleep"));
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "slow cancel/close cleanup must run in the background"
    );
    drop(no_status_poll);
    drop(running);

    let batch = int64_batch("n", vec![2]);
    mount_execute(&server, closed_inline_body("stmt-after-timeout", &batch)).await;
    let exec = stmt
        .execute_with_timeout(Duration::ZERO)
        .await
        .expect("statement must be reusable before background cleanup finishes");
    exec.close().await.expect("close reused execution");

    let closes = wait_for_close_delete(&server, "stmt-poll-sleep", 1, Duration::from_secs(2)).await;
    assert_eq!(closes, 1, "timeout cleanup must eventually close the query");
    let cancel_count = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| {
            request.method == wiremock::http::Method::POST
                && request.url.path() == "/api/2.0/sql/statements/stmt-poll-sleep/cancel"
        })
        .count();
    assert_eq!(cancel_count, 1, "timeout cleanup must not dispatch twice");
}

#[tokio::test]
async fn client_timeout_allows_inflight_terminal_status_to_win_during_grace() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_execute(&server, running_body("stmt-delayed-terminal")).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-delayed-terminal"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(failed_body(
                    "stmt-delayed-terminal",
                    "server failure",
                    "42000",
                ))
                .set_delay(Duration::from_millis(600)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT broken");
    let error = stmt
        .execute_with_timeout(Duration::from_millis(700))
        .await
        .err()
        .expect("the terminal server failure must win during status grace");
    assert_eq!(error.code, ErrorCode::SqlError);
    assert_eq!(error.message, "server failure. SQLSTATE: 42000");
    assert_eq!(error.sql_state.as_deref(), Some("42000"));
    assert_eq!(error.query_id.as_deref(), Some("stmt-delayed-terminal"));

    let closes =
        wait_for_close_delete(&server, "stmt-delayed-terminal", 1, Duration::from_secs(1)).await;
    assert_eq!(closes, 1);
}

#[tokio::test]
async fn client_timeout_does_not_repoll_after_nonterminal_status_during_grace() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_execute(&server, running_body("stmt-running-during-grace")).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-running-during-grace"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(running_body("stmt-running-during-grace"))
                .set_delay(Duration::from_millis(600)),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(
            "/api/2.0/sql/statements/stmt-running-during-grace/cancel",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT still_running");
    let error = stmt
        .execute_with_timeout(Duration::from_millis(700))
        .await
        .err()
        .expect("a non-terminal response during grace must preserve the timeout");
    assert_client_query_timeout(&error, Some("stmt-running-during-grace"));

    let closes = wait_for_close_delete(
        &server,
        "stmt-running-during-grace",
        1,
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(closes, 1);
    server.verify().await;
}

#[tokio::test]
async fn client_timeout_caps_inflight_status_grace() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    mount_execute(&server, running_body("stmt-stalled-status")).await;
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-stalled-status"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(failed_body("stmt-stalled-status", "too late", "42000"))
                .set_delay(Duration::from_secs(10)),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-stalled-status/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT stalled_status");
    let started = Instant::now();
    let error = stmt
        .execute_with_timeout(Duration::from_millis(700))
        .await
        .err()
        .expect("a status request must not outlive the fixed grace window");
    let elapsed = started.elapsed();
    assert_client_query_timeout(&error, Some("stmt-stalled-status"));
    assert!(
        elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(9),
        "timeout returned after {elapsed:?}, expected the five-second status grace"
    );

    let closes =
        wait_for_close_delete(&server, "stmt-stalled-status", 1, Duration::from_secs(1)).await;
    assert_eq!(closes, 1);
    server.verify().await;
}

#[tokio::test]
async fn client_timeout_is_cumulative_across_retry_backoff() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(429).append_header("Retry-After", "0"))
        .mount(&server)
        .await;

    let mut http = HttpConfig::default();
    http.retry_min_wait = Duration::from_millis(250);
    http.retry_max_wait = Duration::from_millis(250);
    http.retry_max_retries = 1;
    let session = open_session_with_http(&server, http).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");

    let error = stmt
        .execute_with_timeout(Duration::from_millis(75))
        .await
        .err()
        .expect("client deadline must interrupt retry backoff");
    assert_client_query_timeout(&error, None);
    let execute_posts = server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|request| {
            request.method == wiremock::http::Method::POST
                && request.url.path() == "/api/2.0/sql/statements"
        })
        .count();
    assert_eq!(execute_posts, 1, "no retry may start after the deadline");
}

#[tokio::test]
async fn status_after_observed_terminal_ignores_client_deadline() {
    // Regression: once `status()` has observed a terminal state, the
    // retained client deadline must stop governing the handle. A later
    // `status()` past the deadline must return the terminal state
    // (not Timeout/HYT00) and must NOT fire a cancel/close against the
    // already-finished statement. The deadline covers only "initial
    // submit through observed terminal status".
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    // Submit returns RUNNING (wait_timeout=0s), so the handle starts
    // pre-terminal with the client deadline armed.
    mount_execute(&server, running_body("stmt-latch-terminal")).await;
    // Every status poll reports SUCCEEDED.
    mount_status(
        &server,
        "stmt-latch-terminal",
        json!({
            "statement_id": "stmt-latch-terminal",
            "status": { "state": "SUCCEEDED" },
        })
        .to_string(),
    )
    .await;
    // A cancel must never fire; mount it so an erroneous POST would be
    // observable (and would otherwise 404).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements/stmt-latch-terminal/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt
        .submit_with_timeout(Duration::from_millis(100))
        .await
        .expect("submit returns a handle");

    // First poll observes the terminal SUCCEEDED (within the deadline).
    let first = async_exec.status().await.expect("first status");
    assert_eq!(first.as_str(), "Succeeded");

    // Let the client deadline elapse, then poll again. The latch must
    // make this return SUCCEEDED rather than the client-query-timeout.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let second = async_exec
        .status()
        .await
        .expect("terminal status must survive the elapsed deadline");
    assert_eq!(second.as_str(), "Succeeded");

    // No cancel RPC may have been dispatched against the finished query.
    let cancels = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| {
            r.method == wiremock::http::Method::POST
                && r.url.path() == "/api/2.0/sql/statements/stmt-latch-terminal/cancel"
        })
        .count();
    assert_eq!(cancels, 0, "a finished statement must not be cancelled");
}

#[tokio::test]
async fn terminal_error_mapping_is_inside_the_client_timeout_phase() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;
    mount_execute(
        &server,
        failed_body("stmt-terminal-error", "bad SQL", "42000"),
    )
    .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT broken");
    let error = stmt
        .execute_with_timeout(Duration::from_secs(1))
        .await
        .err()
        .expect("terminal FAILED must be mapped before materialisation");
    assert_eq!(error.code, ErrorCode::SqlError);
    assert_eq!(error.sql_state.as_deref(), Some("42000"));
    assert_eq!(error.query_id.as_deref(), Some("stmt-terminal-error"));
}

#[tokio::test]
async fn client_timeout_excludes_cloudfetch_materialisation() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_close_any_statement(&server).await;

    let statement_id = "stmt-slow-materialisation";
    let chunk_path = format!("/cloud-fetch/{statement_id}/chunk-0.arrow");
    let chunk_url = format!("{}{}", server.uri(), chunk_path);
    let batch = int64_batch("n", vec![42]);
    let ipc = arrow_ipc_bytes(&batch);
    let execute_body = json!({
        "statement_id": statement_id,
        "status": { "state": "SUCCEEDED" },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "column_count": 1,
                "columns": [{
                    "name": "n",
                    "type_name": "BIGINT",
                    "type_text": "BIGINT",
                    "position": 0,
                }],
            },
            "total_chunk_count": 1,
            "total_row_count": 1,
            "total_byte_count": ipc.len() as i64,
            "truncated": false,
        },
        "result": {
            "external_links": [{
                "external_link": chunk_url,
                "expiration": "2099-01-01T00:00:00.000Z",
                "chunk_index": 0,
                "row_offset": 0,
                "row_count": 1,
                "byte_count": ipc.len() as i64,
            }],
        },
    })
    .to_string();
    mount_execute(&server, execute_body).await;
    Mock::given(method("GET"))
        .and(path(chunk_path.as_str()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(ipc)
                .set_delay(Duration::from_millis(700)),
        )
        .expect(1)
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 42 AS n");
    let started = Instant::now();
    let mut exec = stmt
        .execute_with_timeout(Duration::from_millis(300))
        .await
        .expect("CloudFetch reader materialisation must be outside the deadline");
    assert!(
        started.elapsed() >= Duration::from_millis(650),
        "execute should wait for delayed reader materialisation after execution is terminal"
    );
    let batches = drain(exec.result_stream_mut()).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
    exec.close().await.expect("close");
}
