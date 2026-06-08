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
//! the returned `ResultStream` exposes the **Thrift-compatible**
//! column shape — the kernel renames server columns
//! (`catalog → TABLE_CAT`, `databaseName → TABLE_SCHEM`, …),
//! synthesises JDBC-only columns where the server omits them, and
//! sorts where the JDBC contract requires. See
//! `src/metadata/thrift_shape.rs`.
//!
//! Static methods (`type_info`, `table_types`) skip the wire entirely
//! and verify the synthesised JDBC-shape result the kernel
//! constructs locally.
//!
//! Validation errors (empty identifier, `None` catalog on
//! `primary_keys`, etc.) are also covered here.

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

/// Mixed-type RecordBatch builder for `list_columns` fixtures, which
/// need `ordinalPosition: Int32` alongside the string columns.
fn columns_batch(
    str_cols: Vec<(&str, Vec<Option<&str>>)>,
    ordinal_positions: Vec<Option<i32>>,
) -> RecordBatch {
    use arrow_array::Int32Array;
    let n = str_cols[0].1.len();
    assert_eq!(ordinal_positions.len(), n);
    let mut fields: Vec<Field> = str_cols
        .iter()
        .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
        .collect();
    fields.push(Field::new("ordinalPosition", DataType::Int32, true));
    let mut arrays: Vec<ArrayRef> = str_cols
        .iter()
        .map(|(_, v)| Arc::new(StringArray::from(v.clone())) as ArrayRef)
        .collect();
    arrays.push(Arc::new(Int32Array::from(ordinal_positions)));
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

// ─── Thrift-shape reshape tests ─────────────────────────────────────

#[tokio::test]
async fn list_catalogs_returns_table_cat_column() {
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

    // Kernel renames the server's `catalog` column to `TABLE_CAT`.
    assert_eq!(col_names(&stream.schema()), vec!["TABLE_CAT"]);

    let batches = drain_stream(&mut stream).await;
    let names = batches[0].column(0).as_string::<i32>();
    assert_eq!(names.value(0), "main");
    assert_eq!(names.value(1), "hive_metastore");
}

#[tokio::test]
async fn list_schemas_returns_jdbc_columns_with_synthesised_catalog() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // `SHOW SCHEMAS IN <catalog>` returns only `databaseName`. The
    // kernel synthesises `TABLE_CATALOG` from the request argument.
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

    assert_eq!(
        col_names(&stream.schema()),
        vec!["TABLE_SCHEM", "TABLE_CATALOG"]
    );

    let batches = drain_stream(&mut stream).await;
    let schem = batches[0].column(0).as_string::<i32>();
    let cat = batches[0].column(1).as_string::<i32>();
    assert_eq!(schem.value(0), "default");
    assert_eq!(schem.value(1), "information_schema");
    // Catalog argument echoed into TABLE_CATALOG for every row.
    assert_eq!(cat.value(0), "main");
    assert_eq!(cat.value(1), "main");
}

#[tokio::test]
async fn list_tables_returns_jdbc_columns_with_synthesised_nulls() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t1")]),
        ("catalogName", vec![Some("main")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(None, None, None, None)
        .await
        .expect("list_tables");

    // JDBC `getTables` 10-column shape.
    assert_eq!(
        col_names(&stream.schema()),
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
    assert_eq!(
        batches[0]
            .column_by_name("TABLE_TYPE")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "TABLE"
    );
    // Synthesised JDBC-only columns are all null.
    for col in [
        "TYPE_CAT",
        "TYPE_SCHEM",
        "TYPE_NAME",
        "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION",
    ] {
        assert!(
            batches[0].column_by_name(col).unwrap().is_null(0),
            "{col} should be null"
        );
    }
}

#[tokio::test]
async fn list_tables_table_types_filters_kernel_side() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // The Databricks server doesn't honour `table_types` on
    // `SHOW TABLES`; the kernel filters on `TABLE_TYPE` after reshape.
    let raw = string_batch(vec![
        ("namespace", vec![Some("d"), Some("d")]),
        ("tableName", vec![Some("t1"), Some("v1")]),
        ("catalogName", vec![Some("main"), Some("main")]),
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
    assert_eq!(batches[0].num_rows(), 1, "kernel filters to VIEW only");
    assert_eq!(
        batches[0]
            .column_by_name("TABLE_NAME")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "v1"
    );
}

#[tokio::test]
async fn list_tables_sorted_by_jdbc_key() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // Rows arrive in deliberately scrambled order — the kernel must
    // sort by (TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME) to
    // match JDBC's `getTables` row order ("matching Thrift mode" per
    // JDBC's `MetadataResultSetBuilder`).
    let raw = string_batch(vec![
        ("namespace", vec![Some("s2"), Some("s1"), Some("s1")]),
        ("tableName", vec![Some("z"), Some("a"), Some("b")]),
        (
            "catalogName",
            vec![Some("main"), Some("main"), Some("main")],
        ),
        (
            "tableType",
            vec![Some("VIEW"), Some("TABLE"), Some("TABLE")],
        ),
    ]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(None, None, None, None)
        .await
        .expect("list_tables");

    let batches = drain_stream(&mut stream).await;
    let types = batches[0]
        .column_by_name("TABLE_TYPE")
        .unwrap()
        .as_string::<i32>();
    let names = batches[0]
        .column_by_name("TABLE_NAME")
        .unwrap()
        .as_string::<i32>();
    // TABLE rows come before VIEW; within TABLE, sorted by name.
    assert_eq!(types.value(0), "TABLE");
    assert_eq!(names.value(0), "a");
    assert_eq!(types.value(1), "TABLE");
    assert_eq!(names.value(1), "b");
    assert_eq!(types.value(2), "VIEW");
    assert_eq!(names.value(2), "z");
}

#[tokio::test]
async fn list_columns_returns_jdbc_columns_with_data_type_derived() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = columns_batch(
        vec![
            ("col_name", vec![Some("id")]),
            ("catalogName", vec![Some("main")]),
            ("namespace", vec![Some("default")]),
            ("tableName", vec![Some("t")]),
            ("columnType", vec![Some("INT")]),
            ("isNullable", vec![Some("false")]),
        ],
        vec![Some(1)],
    );
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_columns(Some(Identifier::new("main").unwrap()), None, None, None)
        .await
        .expect("list_columns");

    // 23-column JDBC `getColumns` shape.
    assert_eq!(stream.schema().fields().len(), 23);
    let names = col_names(&stream.schema());
    assert_eq!(names[0], "TABLE_CAT");
    assert_eq!(names[3], "COLUMN_NAME");
    assert_eq!(names[4], "DATA_TYPE");
    assert_eq!(names[5], "TYPE_NAME");

    let batches = drain_stream(&mut stream).await;
    // TYPE_NAME preserves the server's precise string.
    assert_eq!(
        batches[0]
            .column_by_name("TYPE_NAME")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "INT"
    );
    // DATA_TYPE is the JDBC integer code: INT → 4.
    let data_type = batches[0]
        .column_by_name("DATA_TYPE")
        .unwrap()
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(data_type.value(0), 4);
    // NULLABLE derived from isNullable=false → 0 (columnNoNulls).
    let nullable = batches[0]
        .column_by_name("NULLABLE")
        .unwrap()
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(nullable.value(0), 0);
    // IS_NULLABLE string mirror.
    assert_eq!(
        batches[0]
            .column_by_name("IS_NULLABLE")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "NO"
    );
}

#[tokio::test]
async fn list_columns_without_catalog_emits_all_catalogs_query() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // The kernel maps `catalog=None` to
    // `SHOW COLUMNS IN ALL CATALOGS`; the server response carries
    // `catalogName` so each row's TABLE_CAT is filled from the wire
    // (not synthesised from the request).
    let raw = columns_batch(
        vec![
            ("col_name", vec![Some("id")]),
            ("catalogName", vec![Some("hive_metastore")]),
            ("namespace", vec![Some("d")]),
            ("tableName", vec![Some("t")]),
            ("columnType", vec![Some("BIGINT")]),
            ("isNullable", vec![Some("true")]),
        ],
        vec![Some(1)],
    );
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_columns(None, None, None, None)
        .await
        .expect("list_columns with catalog=None should succeed");

    let batches = drain_stream(&mut stream).await;
    assert_eq!(
        batches[0]
            .column_by_name("TABLE_CAT")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "hive_metastore",
        "TABLE_CAT is populated from the server's catalogName column"
    );
}

#[tokio::test]
async fn primary_keys_reshaped_to_jdbc_columns() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // Live `SHOW KEYS` shape — the kernel reshapes the server's
    // `catalogName / namespace / tableName / col_name / keySeq /
    // constraintName` columns into the JDBC `getPrimaryKeys` set.
    let raw = string_batch(vec![
        ("catalogName", vec![Some("main"), Some("main")]),
        ("namespace", vec![Some("d"), Some("d")]),
        ("tableName", vec![Some("t"), Some("t")]),
        ("col_name", vec![Some("id"), Some("code")]),
        ("keySeq", vec![Some("1"), Some("2")]),
        ("constraintName", vec![Some("pk_t"), Some("pk_t")]),
        ("constraintType", vec![Some("PRIMARY"), Some("PRIMARY")]),
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
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "KEY_SEQ",
            "PK_NAME",
        ]
    );

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(
        batches[0]
            .column_by_name("TABLE_CAT")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "main"
    );
    assert_eq!(
        batches[0]
            .column_by_name("COLUMN_NAME")
            .unwrap()
            .as_string::<i32>()
            .value(1),
        "code"
    );
    let key_seq = batches[0]
        .column_by_name("KEY_SEQ")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("KEY_SEQ is Int32");
    assert_eq!(key_seq.value(0), 1);
    assert_eq!(key_seq.value(1), 2);
    assert_eq!(
        batches[0]
            .column_by_name("PK_NAME")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "pk_t"
    );
}

#[tokio::test]
async fn foreign_keys_reshaped_to_jdbc_columns() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // Live `SHOW FOREIGN KEYS` shape — the FK-bearing side is in
    // `catalogName / namespace / tableName / col_name`, the referenced
    // primary side in the `parent*` columns. The kernel reshapes into
    // the JDBC `getCrossReference` set; UPDATE_RULE / DELETE_RULE /
    // PK_NAME have no server source and come back null.
    let raw = string_batch(vec![
        ("catalogName", vec![Some("main")]),
        ("namespace", vec![Some("d")]),
        ("tableName", vec![Some("child")]),
        ("col_name", vec![Some("pid")]),
        ("keySeq", vec![Some("1")]),
        ("constraintName", vec![Some("fk_child")]),
        ("constraintType", vec![Some("FOREIGN")]),
        ("parentCatalogName", vec![Some("main")]),
        ("parentNamespace", vec![Some("d")]),
        ("parentTableName", vec![Some("parent")]),
        ("parentColName", vec![Some("id")]),
        ("deferrability", vec![Some("7")]),
    ]);
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .foreign_keys(
            None,
            None,
            None,
            Some(Identifier::new("main").unwrap()),
            Some(Identifier::new("d").unwrap()),
            Some(Identifier::new("child").unwrap()),
        )
        .await
        .expect("foreign_keys");

    assert_eq!(
        col_names(&stream.schema()),
        vec![
            "PKTABLE_CAT",
            "PKTABLE_SCHEM",
            "PKTABLE_NAME",
            "PKCOLUMN_NAME",
            "FKTABLE_CAT",
            "FKTABLE_SCHEM",
            "FKTABLE_NAME",
            "FKCOLUMN_NAME",
            "KEY_SEQ",
            "UPDATE_RULE",
            "DELETE_RULE",
            "FK_NAME",
            "PK_NAME",
            "DEFERRABILITY",
        ]
    );

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
    let b = &batches[0];
    assert_eq!(
        b.column_by_name("PKTABLE_NAME")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "parent"
    );
    assert_eq!(
        b.column_by_name("FKTABLE_NAME")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "child"
    );
    assert_eq!(
        b.column_by_name("FK_NAME")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "fk_child"
    );
    // UPDATE_RULE / DELETE_RULE / PK_NAME have no server source → null.
    assert!(b.column_by_name("UPDATE_RULE").unwrap().is_null(0));
    assert!(b.column_by_name("DELETE_RULE").unwrap().is_null(0));
    assert!(b.column_by_name("PK_NAME").unwrap().is_null(0));
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
