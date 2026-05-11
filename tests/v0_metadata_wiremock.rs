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
//! For each wire-driven `Metadata::*` RPC, we stub the SEA
//! `POST /sessions` and `POST /statements` endpoints, encoding a
//! synthesised Databricks Arrow batch as inline base64. We then drive
//! the call through `Session::open` + `Metadata::*` and assert that
//! the returned `ResultStream` exposes the **server-shaped** columns
//! verbatim — the kernel does not rename or filter.
//!
//! Static methods (`type_info`, `table_types`) skip the wire entirely
//! and verify the synthesised JDBC-shape result the kernel
//! constructs locally.
//!
//! Validation errors (empty identifier, `None` catalog on
//! `list_columns`, etc.) are also covered here.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
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

fn arrow_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

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

async fn mount_execute(server: &MockServer, body: String) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

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

fn col_names(schema: &arrow_schema::Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

// ─── Pass-through tests ─────────────────────────────────────────────

#[tokio::test]
async fn list_catalogs_passes_server_columns_through() {
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

    // Server shape: single `catalog` column.
    assert_eq!(col_names(&stream.schema()), vec!["catalog"]);

    let batches = drain_stream(&mut stream).await;
    let names = batches[0].column(0).as_string::<i32>();
    assert_eq!(names.value(0), "main");
    assert_eq!(names.value(1), "hive_metastore");
}

#[tokio::test]
async fn list_schemas_passes_server_columns_through() {
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

    // Server shape: just `databaseName` (no synthesised catalog
    // column — the kernel passes through what the server returned).
    assert_eq!(col_names(&stream.schema()), vec!["databaseName"]);

    let batches = drain_stream(&mut stream).await;
    let arr = batches[0].column(0).as_string::<i32>();
    assert_eq!(arr.value(0), "default");
    assert_eq!(arr.value(1), "information_schema");
}

#[tokio::test]
async fn list_tables_passes_server_columns_through() {
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

    assert_eq!(
        col_names(&stream.schema()),
        vec!["catalogName", "namespace", "tableName", "tableType"]
    );

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column_by_name("tableType")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "TABLE"
    );
}

#[tokio::test]
async fn list_tables_table_types_arg_is_advisory_no_op() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // Both rows are returned even though the caller asked for only
    // VIEW: the kernel does not filter post-fetch.
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
    assert_eq!(batches[0].num_rows(), 2, "kernel passes both rows through");
}

#[tokio::test]
async fn list_columns_passes_server_columns_through() {
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

    assert_eq!(
        col_names(&stream.schema()),
        vec![
            "catalogName",
            "namespace",
            "tableName",
            "col_name",
            "columnType",
            "isNullable",
        ]
    );

    let batches = drain_stream(&mut stream).await;
    assert_eq!(
        batches[0]
            .column_by_name("columnType")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "INT"
    );
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
async fn primary_keys_passes_server_columns_through() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // Live `SHOW KEYS` shape — full 14-column row including the
    // parent linkage columns and constraintType.
    let raw = string_batch(vec![
        ("catalogName", vec![Some("main")]),
        ("namespace", vec![Some("d")]),
        ("tableName", vec![Some("t")]),
        ("col_name", vec![Some("id")]),
        ("constraintName", vec![Some("pk_t")]),
        ("constraintType", vec![Some("PRIMARY KEY")]),
    ]);
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

    assert_eq!(
        col_names(&stream.schema()),
        vec![
            "catalogName",
            "namespace",
            "tableName",
            "col_name",
            "constraintName",
            "constraintType",
        ]
    );

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .column_by_name("constraintType")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "PRIMARY KEY"
    );
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

// ─── Static-method tests (JDBC-shape, synthesised) ──────────────────

#[tokio::test]
async fn type_info_returns_static_jdbc_rows_without_wire_call() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session.metadata().type_info().await.expect("type_info");

    let schema = stream.schema();
    assert_eq!(schema.fields().len(), 19);
    assert_eq!(schema.field(0).name(), "TYPE_NAME");
    assert_eq!(schema.field(1).name(), "DATA_TYPE");

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
    assert_eq!(col_names(&schema), vec!["TABLE_TYPE"]);

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
