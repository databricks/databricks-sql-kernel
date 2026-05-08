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

//! Wiremock end-to-end tests for the v0 `Metadata::*` surface.
//!
//! For each `Metadata::*` RPC, we stub the SEA `POST /sessions` and
//! `POST /statements` endpoints, encoding a synthesised raw
//! Databricks Arrow batch as inline base64. We then drive the call
//! through `Session::open` + `Metadata::*` and assert that:
//!
//! 1. The returned `ResultStream`'s schema matches the JDBC contract
//!    (column names + types from `metadata::schemas::*`).
//! 2. The reshape layer produced the expected row content.
//!
//! Static methods (`type_info`, `table_types`) skip the wire entirely
//! and just verify the in-memory reader path.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, Identifier, LikePattern, ResultBatch, Session,
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

/// Stub the create-session endpoint so `Session::open` succeeds.
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

/// Stub the delete-session endpoint so the Drop fire-and-forget
/// path doesn't generate noisy 404s.
async fn mount_delete_session(server: &MockServer) {
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/sess-test"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(server)
        .await;
}

/// Encode a single `RecordBatch` as Arrow IPC stream bytes.
fn arrow_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

/// Build a synthesised SUCCEEDED response body with inline Arrow
/// data (matching what `SeaRestTransport::execute_and_read` consumes).
fn succeeded_inline_response_body(batch: &RecordBatch) -> String {
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
                "type_name": "STRING",
                "type_text": "STRING",
                "position": i as i32,
            })
        })
        .collect();

    json!({
        "statement_id": "stmt-test",
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

/// Stub `POST /statements` to return `body` for any execute call.
async fn mount_execute(server: &MockServer, body: String) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

/// Build a `RecordBatch` from a list of `(name, values)` pairs where
/// values is a `Vec<Option<&str>>`.
fn string_batch(cols: Vec<(&str, Vec<Option<&str>>)>) -> RecordBatch {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
        .collect();
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v)| Arc::new(StringArray::from(v.clone())) as ArrayRef)
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

/// Pull all batches into a single `Vec<RecordBatch>` (one for the
/// metadata path).
async fn drain_stream(stream: &mut databricks_sql_kernel::ResultStream) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    while let Some(batch) = stream.next_batch().await.expect("next_batch") {
        let ResultBatch::Arrow(rb) = batch else {
            panic!("expected Arrow variant in v0");
        };
        out.push(rb.clone());
    }
    out
}

// ─── Tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn list_catalogs_returns_jdbc_table_cat_column() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![(
        "catalog",
        vec![Some("main"), Some("hive_metastore")],
    )]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_catalogs()
        .await
        .expect("list_catalogs");

    let schema = stream.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "TABLE_CAT");

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.len(), 1);
    let names = batches[0].column(0).as_string::<i32>();
    assert_eq!(names.value(0), "main");
    assert_eq!(names.value(1), "hive_metastore");
}

#[tokio::test]
async fn list_schemas_uses_fallback_catalog_when_column_absent() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // SHOW SCHEMAS IN `main` returns only databaseName.
    let raw = string_batch(vec![(
        "databaseName",
        vec![Some("default"), Some("information_schema")],
    )]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_schemas(Some(Identifier::new("main").unwrap()), None)
        .await
        .expect("list_schemas");

    assert_eq!(stream.schema().field(0).name(), "TABLE_SCHEM");
    assert_eq!(stream.schema().field(1).name(), "TABLE_CATALOG");

    let batches = drain_stream(&mut stream).await;
    let cat = batches[0].column(1).as_string::<i32>();
    assert_eq!(cat.value(0), "main");
    assert_eq!(cat.value(1), "main");
}

#[tokio::test]
async fn list_tables_returns_jdbc_columns_with_nulls_for_unknown() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![
        ("catalogName", vec![Some("main")]),
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t1")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(None, None, None, None)
        .await
        .expect("list_tables");

    let schema = stream.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "TABLE_TYPE",
            "REMARKS",
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "SELF_REFERENCING_COL_NAME",
            "REF_GENERATION",
        ]
    );

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
    // TYPE_CAT … REF_GENERATION (positions 5-9) all NULL.
    for i in 5..10 {
        assert!(batches[0].column(i).is_null(0));
    }
}

#[tokio::test]
async fn list_tables_filters_by_table_types() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![
        ("catalogName", vec![Some("main"), Some("main")]),
        ("namespace", vec![Some("d"), Some("d")]),
        ("tableName", vec![Some("t1"), Some("v1")]),
        ("tableType", vec![Some("TABLE"), Some("VIEW")]),
    ]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(None, None, None, Some(&["VIEW"]))
        .await
        .expect("list_tables");

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0].column(2).as_string::<i32>().value(0),
        "v1",
        "filter must keep only VIEW rows"
    );
}

#[tokio::test]
async fn list_columns_translates_data_type_and_nullable() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![
        ("catalogName", vec![Some("main")]),
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("col_name", vec![Some("id")]),
        ("columnType", vec![Some("INT")]),
        ("isNullable", vec![Some("false")]),
    ]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_columns(Some(Identifier::new("main").unwrap()), None, None, None)
        .await
        .expect("list_columns");

    assert_eq!(stream.schema().fields().len(), 24);

    let batches = drain_stream(&mut stream).await;
    let rb = &batches[0];
    let data_type = rb
        .column_by_name("DATA_TYPE")
        .unwrap()
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(data_type.value(0), 4); // JDBC INTEGER
    let nullable = rb
        .column_by_name("NULLABLE")
        .unwrap()
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(nullable.value(0), 0); // columnNoNulls
    let is_nullable = rb.column_by_name("IS_NULLABLE").unwrap().as_string::<i32>();
    assert_eq!(is_nullable.value(0), "NO");
}

#[tokio::test]
async fn list_columns_without_catalog_errors() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let res = session
        .metadata()
        .list_columns(None, None, None, None)
        .await;
    use databricks_sql_kernel::ErrorCode;
    match res {
        Ok(_) => panic!("None catalog must error"),
        Err(err) => assert_eq!(err.code, ErrorCode::InvalidArgument),
    }
}

#[tokio::test]
async fn primary_keys_zero_rows_succeeds() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // Empty result — Databricks returns the schema even with 0 rows.
    // Use the live `SHOW KEYS` shape (camelCase) so the reshape
    // layer's tolerance is exercised end-to-end.
    let fields = vec![
        Field::new("catalogName", DataType::Utf8, true),
        Field::new("namespace", DataType::Utf8, true),
        Field::new("tableName", DataType::Utf8, true),
        Field::new("col_name", DataType::Utf8, true),
        Field::new("keySeq", DataType::Int32, true),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(Int32Array::from(Vec::<i32>::new())),
    ];
    let raw = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .primary_keys(
            Some(Identifier::new("main").unwrap()),
            Some(Identifier::new("d").unwrap()),
            Identifier::new("t").unwrap(),
        )
        .await
        .expect("primary_keys");

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 0);
    assert_eq!(batches[0].schema().fields().len(), 6);
}

#[tokio::test]
async fn primary_keys_without_schema_errors() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let res = session
        .metadata()
        .primary_keys(
            Some(Identifier::new("main").unwrap()),
            None,
            Identifier::new("t").unwrap(),
        )
        .await;
    use databricks_sql_kernel::ErrorCode;
    match res {
        Ok(_) => panic!("None schema must error"),
        Err(err) => assert_eq!(err.code, ErrorCode::InvalidArgument),
    }
}

#[tokio::test]
async fn type_info_returns_static_jdbc_rows_without_wire_call() {
    // Note: no execute mock — we never hit the wire.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session.metadata().type_info().await.expect("type_info");

    let schema = stream.schema();
    assert_eq!(schema.field(0).name(), "TYPE_NAME");
    assert_eq!(schema.field(1).name(), "DATA_TYPE");
    assert_eq!(schema.fields().len(), 19);

    let batches = drain_stream(&mut stream).await;
    assert!(batches[0].num_rows() >= 19);
}

#[tokio::test]
async fn table_types_returns_static_set() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session.metadata().table_types().await.expect("table_types");

    let schema = stream.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "TABLE_TYPE");

    let batches = drain_stream(&mut stream).await;
    let arr = batches[0].column(0).as_string::<i32>();
    let collected: Vec<&str> = (0..arr.len()).map(|i| arr.value(i)).collect();
    assert!(collected.contains(&"TABLE"));
    assert!(collected.contains(&"VIEW"));
}

#[tokio::test]
async fn identifier_validation_propagates_invalid_argument() {
    use databricks_sql_kernel::ErrorCode;

    let err = Identifier::new("").expect_err("empty identifier rejected");
    assert_eq!(err.code, ErrorCode::InvalidArgument);

    let err = LikePattern::new("with\0nul").expect_err("NUL byte rejected");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
}
