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
//! `CreateSession` and `POST /statements` endpoints, encoding a
//! synthesised Databricks Arrow batch as inline base64. We then drive
//! the call through `Session::open` + `Metadata::*` and assert that
//! the returned `ResultStream` exposes the **Thrift-compatible**
//! column shape — the kernel renames server columns
//! (`catalog → TABLE_CAT`, `databaseName → TABLE_SCHEM`, …),
//! synthesises JDBC-only columns where the server omits them, and
//! sorts where the JDBC contract requires. See
//! `src/metadata/thrift_shape.rs`.
//!
//! Static methods (`type_info`, `table_types`) skip the wire entirely.
//! `type_info` verifies the Thrift-compatible result the kernel constructs
//! locally; `table_types` verifies its JDBC shape.
//!
//! Identifier validation and session namespace resolution are also covered here.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, Int16Array, Int32Array, Int8Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use databricks_sql_kernel::metadata_v0::exact_catalog_and_pattern;
use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, DefaultOpts, Identifier, LikePattern, ResultBatch, Session,
};
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{body_partial_json, body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ─── Helpers ────────────────────────────────────────────────────────

async fn open_session(server: &MockServer) -> Session {
    open_session_with_default_catalog(server, None).await
}

async fn open_session_with_default_catalog(server: &MockServer, catalog: Option<&str>) -> Session {
    let builder = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        });
    let builder = match catalog {
        Some(catalog) => {
            let mut defaults = DefaultOpts::default();
            defaults.catalog = Some(catalog.to_owned());
            builder.defaults(defaults)
        }
        None => builder,
    };
    builder.open().await.expect("Session::open should succeed")
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

fn native_inline_response_body(batch: &RecordBatch) -> String {
    let mut body: serde_json::Value =
        serde_json::from_str(&succeeded_inline_response_body(batch)).unwrap();
    body["manifest"]["is_native_metadata_result"] = json!(true);
    body.to_string()
}

/// SEA response body for a FAILED statement with the given SQLSTATE +
/// message — drives the kernel's terminal-error path. A `42704` state
/// is treated as object-not-found (skipped in the fan-out); anything
/// else propagates.
fn failed_response_body(sql_state: &str, message: &str) -> String {
    json!({
        "statement_id": "stmt-test",
        "status": {
            "state": "FAILED",
            "sql_state": sql_state,
            "error": {
                "error_code": "BAD_REQUEST",
                "message": message,
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

/// Mount `/statements` so it only responds when the request body (the
/// SEA statement JSON) contains `sql_substr`. If the kernel emits
/// different SQL, no stub matches and the call fails — so this asserts
/// the exact SQL the routing produced.
async fn mount_execute_expecting_sql(server: &MockServer, sql_substr: &str, body: String) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains(sql_substr))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(server)
        .await;
}

/// Like [`succeeded_inline_response_body`] but reports
/// `status.state == CLOSED`. This is the common inline/direct-result
/// case for metadata `SHOW …` queries: the server auto-closes the
/// statement and returns `CLOSED`, so the kernel must NOT issue a
/// follow-up `CloseStatement` (DELETE) on it.
fn closed_inline_response_body(batch: &RecordBatch) -> String {
    succeeded_inline_response_body(batch).replace(r#""state":"SUCCEEDED""#, r#""state":"CLOSED""#)
}

async fn mount_current_catalog(server: &MockServer, catalog: Option<&str>) {
    let current_catalog = string_batch(vec![(
        "current_catalog()",
        catalog
            .map(|catalog| vec![Some(catalog)])
            .unwrap_or_default(),
    )]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SELECT CURRENT_CATALOG()"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(closed_inline_response_body(&current_catalog)),
        )
        .expect(1)
        .mount(server)
        .await;
}

/// Like [`succeeded_inline_response_body`] but with a caller-chosen
/// `statement_id` and `status.state`. Used by the wildcard fan-out
/// CLOSED-skip test, which stubs several statements in one call and must
/// distinguish the `SHOW CATALOGS` statement from each per-catalog
/// `SHOW TABLES` statement by id when asserting on DELETEs.
fn inline_response_body_with(batch: &RecordBatch, statement_id: &str, state: &str) -> String {
    succeeded_inline_response_body(batch)
        .replace(
            r#""statement_id":"stmt-test""#,
            &format!(r#""statement_id":"{statement_id}""#),
        )
        .replace(r#""state":"SUCCEEDED""#, &format!(r#""state":"{state}""#))
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

fn functions_batch(catalog: &str) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("functionName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("catalogName", DataType::Utf8, true),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("functionType", DataType::Int32, true),
            Field::new("specificName", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![Some("answer")])),
            Arc::new(StringArray::from(vec![Some("default")])),
            Arc::new(StringArray::from(vec![Some(catalog)])),
            Arc::new(StringArray::from(vec![Some("Returns 42")])),
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(StringArray::from(vec![Some("answer")])),
        ],
    )
    .unwrap()
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

fn native_columns_batch(ordinal_position: i32) -> RecordBatch {
    let fields = vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("DATA_TYPE", DataType::Int32, false),
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        Field::new("BUFFER_LENGTH", DataType::Int8, true),
        Field::new("DECIMAL_DIGITS", DataType::Int32, true),
        Field::new("NUM_PREC_RADIX", DataType::Int32, true),
        Field::new("NULLABLE", DataType::Int32, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("COLUMN_DEF", DataType::Utf8, true),
        Field::new("SQL_DATA_TYPE", DataType::Int32, true),
        Field::new("SQL_DATETIME_SUB", DataType::Int32, true),
        Field::new("CHAR_OCTET_LENGTH", DataType::Int32, true),
        Field::new("ORDINAL_POSITION", DataType::Int32, true),
        Field::new("IS_NULLABLE", DataType::Utf8, false),
        Field::new("SCOPE_CATALOG", DataType::Utf8, true),
        Field::new("SCOPE_SCHEMA", DataType::Utf8, true),
        Field::new("SCOPE_TABLE", DataType::Utf8, true),
        Field::new("SOURCE_DATA_TYPE", DataType::Int16, true),
        Field::new("IS_AUTO_INCREMENT", DataType::Utf8, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("main")])),
        Arc::new(StringArray::from(vec![Some("default")])),
        Arc::new(StringArray::from(vec![Some("t")])),
        Arc::new(StringArray::from(vec![Some("id")])),
        Arc::new(Int32Array::from(vec![4])),
        Arc::new(StringArray::from(vec![Some("INT")])),
        Arc::new(Int32Array::from(vec![Some(10)])),
        Arc::new(Int8Array::from(vec![None])),
        Arc::new(Int32Array::from(vec![Some(0)])),
        Arc::new(Int32Array::from(vec![Some(10)])),
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(Int32Array::from(vec![None])),
        Arc::new(Int32Array::from(vec![None])),
        Arc::new(Int32Array::from(vec![None])),
        Arc::new(Int32Array::from(vec![ordinal_position])),
        Arc::new(StringArray::from(vec![Some("YES")])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(Int16Array::from(vec![None])),
        Arc::new(StringArray::from(vec![Some("NO")])),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
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

/// Poll `server.received_requests()` until `expected` `DELETE`s to the
/// `stmt-test` statement endpoint are observed, or `timeout` elapses;
/// returns the final observed count. This replaces a bare `sleep` when
/// asserting on the fire-and-forget `StatementCloseGuard`: for a positive
/// expectation it waits only as long as needed (so a close delayed past a
/// fixed window can't flake the test), and for a negative expectation
/// (`expected == 0`) it still gives an erroneously-spawned close the full
/// `timeout` to reach the wire but breaks early the instant one appears, so
/// the negative assertion stays reliably able to fail under CI load.
async fn wait_for_close_delete(
    server: &MockServer,
    expected: usize,
    timeout: std::time::Duration,
) -> usize {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| {
                r.method.as_str() == "DELETE" && r.url.path() == "/api/2.0/sql/statements/stmt-test"
            })
            .count();
        // Positive expectation met, or a spurious DELETE already appeared for a
        // negative expectation — either way there is nothing more to wait for.
        if count >= expected.max(1) || tokio::time::Instant::now() >= deadline {
            return count;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

// ─── Thrift-shape reshape tests ─────────────────────────────────────

#[tokio::test]
async fn native_catalogs_header_and_result_bypass_show_reshape() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // A native result already uses the Thrift/JDBC column name. Attempting
    // the SHOW CATALOGS reshape would look for `catalog` and fail.
    let native = string_batch(vec![("TABLE_CAT", vec![Some("main")])]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header(
            "x-databricks-metadata-operation-type",
            "GetCatalogs",
        ))
        .and(header(
            "x-databricks-require-thrift-native-metadata",
            "true",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(native_inline_response_body(&native)),
        )
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_catalogs()
        .await
        .expect("native list_catalogs");

    assert_eq!(col_names(&stream.schema()), vec!["TABLE_CAT"]);
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].column(0).as_string::<i32>().value(0), "main");
}

#[tokio::test]
async fn native_columns_zero_based_ordinal_is_one_based() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let native = native_columns_batch(0);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header("x-databricks-metadata-operation-type", "GetColumns"))
        .and(header(
            "x-databricks-require-thrift-native-metadata",
            "true",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(native_inline_response_body(&native)),
        )
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_columns(Some(Identifier::new("main").unwrap()), None, None, None)
        .await
        .expect("native list_columns");
    let batches = drain_stream(&mut stream).await;
    let ordinal = batches[0]
        .column_by_name("ORDINAL_POSITION")
        .unwrap()
        .as_primitive::<arrow_array::types::Int32Type>();

    assert_eq!(ordinal.value(0), 1);
}

#[tokio::test]
async fn native_functions_header_and_result_match_jdbc() {
    for (catalog, name_pattern, expected_sql, expected_catalog) in [
        (
            None,
            None,
            "SHOW FUNCTIONS IN CATALOG `main` LIKE '*'",
            None,
        ),
        (
            Some("main"),
            None,
            "SHOW FUNCTIONS IN CATALOG `main` LIKE '*'",
            Some("main"),
        ),
        (
            Some(""),
            Some("answer"),
            "SHOW FUNCTIONS IN CATALOG `` LIKE 'answer'",
            Some(""),
        ),
    ] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;

        if catalog.is_none() {
            mount_current_catalog(&server, Some("main")).await;
        }

        let native = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("FUNCTION_CAT", DataType::Utf8, true),
                Field::new("FUNCTION_SCHEM", DataType::Utf8, true),
                Field::new("FUNCTION_NAME", DataType::Utf8, false),
                Field::new("REMARKS", DataType::Utf8, true),
                Field::new("FUNCTION_TYPE", DataType::Int32, true),
                Field::new("SPECIFIC_NAME", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("")])),
                Arc::new(StringArray::from(vec![Some("default")])),
                Arc::new(StringArray::from(vec![Some("answer")])),
                Arc::new(StringArray::from(vec![Some("Returns 42")])),
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("answer")])),
            ],
        )
        .unwrap();
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(header(
                "x-databricks-metadata-operation-type",
                "GetFunctions",
            ))
            .and(header(
                "x-databricks-require-thrift-native-metadata",
                "true",
            ))
            .and(body_string_contains(expected_sql))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(native_inline_response_body(&native)),
            )
            .mount(&server)
            .await;

        let session = open_session(&server).await;
        let catalog = catalog.map(|catalog| Identifier::new_allow_empty(catalog).unwrap());
        let name_pattern = name_pattern.map(|pattern| LikePattern::new(pattern).unwrap());
        let mut stream = session
            .metadata()
            .functions(catalog, None, name_pattern)
            .await
            .expect("native list_functions");
        assert_eq!(
            col_names(&stream.schema()),
            vec![
                "FUNCTION_CAT",
                "FUNCTION_SCHEM",
                "FUNCTION_NAME",
                "REMARKS",
                "FUNCTION_TYPE",
                "SPECIFIC_NAME",
            ]
        );

        let batches = drain_stream(&mut stream).await;
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column_by_name("FUNCTION_NAME")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            "answer"
        );
        let function_catalog = batches[0]
            .column_by_name("FUNCTION_CAT")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(
            (!function_catalog.is_null(0)).then(|| function_catalog.value(0)),
            expected_catalog
        );
        let function_type = batches[0]
            .column_by_name("FUNCTION_TYPE")
            .unwrap()
            .as_primitive::<arrow_array::types::Int16Type>();
        assert_eq!(function_type.value(0), 1);
    }
}

#[tokio::test]
async fn functions_sql_fallback_uses_current_catalog() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    mount_current_catalog(&server, Some("main")).await;

    let functions = functions_batch("main");
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header(
            "x-databricks-metadata-operation-type",
            "GetFunctions",
        ))
        .and(body_string_contains(
            "SHOW FUNCTIONS IN CATALOG `main` LIKE '*'",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(succeeded_inline_response_body(&functions)),
        )
        .expect(1)
        .mount(&server)
        .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .functions(None, None, None)
        .await
        .expect("list_functions SQL fallback");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(
        batches[0]
            .column_by_name("FUNCTION_CAT")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "main"
    );
}

#[tokio::test]
async fn functions_falls_back_when_current_catalog_is_unavailable() {
    for (configured_catalog, expected_catalog) in
        [(Some("configured"), "configured"), (None, "hive_metastore")]
    {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;

        mount_current_catalog(&server, None).await;

        let functions = functions_batch(expected_catalog);
        mount_execute_expecting_sql(
            &server,
            &format!("SHOW FUNCTIONS IN CATALOG `{expected_catalog}` LIKE '*'"),
            succeeded_inline_response_body(&functions),
        )
        .await;

        let session = match configured_catalog {
            Some(catalog) => open_session_with_default_catalog(&server, Some(catalog)).await,
            None => open_session(&server).await,
        };
        let mut stream = session
            .metadata()
            .functions(None, None, None)
            .await
            .expect("list_functions catalog fallback");
        let batches = drain_stream(&mut stream).await;
        assert_eq!(
            batches[0]
                .column_by_name("FUNCTION_CAT")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            expected_catalog
        );
    }
}

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
async fn list_schemas_literal_percent_catalog_scopes_verbatim_and_empties() {
    // Conformance (METADATA-036/037): a *literal* catalog named `%`
    // (an exact `Identifier`, not a wildcard — the v0 schemas surface
    // takes an `Identifier`) must scope to that catalog and match
    // nothing, NOT broaden to "all catalogs". The stub only matches
    // `SHOW SCHEMAS IN \`%\``; if the kernel emitted `IN ALL CATALOGS`
    // no stub would match and the call would fail. The server answers
    // CATALOG_NOT_FOUND, which the kernel maps to an empty result.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    mount_execute_expecting_sql(
        &server,
        "SHOW SCHEMAS IN `%`",
        failed_response_body("42704", "CATALOG_NOT_FOUND: no catalog named `%`"),
    )
    .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_schemas(Some(Identifier::new("%").unwrap()), None)
        .await
        .expect("list_schemas literal % catalog maps not-found to empty");
    // Still the JDBC getSchemas shape, zero rows.
    assert_eq!(col_names(&stream.schema())[0], "TABLE_SCHEM");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn list_schemas_empty_catalog_matches_nothing_without_wire_call() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let catalog = Some(String::new());
    let requested_pattern = Some("ignored%".to_owned());
    let (catalog, schema_pattern) =
        exact_catalog_and_pattern(&catalog, &requested_pattern).unwrap();
    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_schemas(catalog, schema_pattern)
        .await
        .expect("empty catalog returns empty schemas");

    assert_eq!(col_names(&stream.schema())[0], "TABLE_SCHEM");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn list_tables_escaped_percent_catalog_scopes_verbatim_and_empties() {
    // Conformance (METADATA-037, escaping ON): an *escaped* catalog
    // pattern `\%` denotes a literal catalog named `%`. It has no active
    // wildcards, so `list_tables` takes the fast exact path, unescapes to
    // `%`, and must scope `SHOW TABLES IN CATALOG \`%\`` — NOT broaden to
    // "all catalogs". (Contrast `list_tables_percent_catalog_uses_single_
    // all_catalogs_query`, where an *unescaped* `%` is genuinely
    // match-all.) The server's not-found maps to an empty result.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    mount_execute_expecting_sql(
        &server,
        "SHOW TABLES IN CATALOG `%`",
        failed_response_body("42704", "CATALOG_NOT_FOUND: no catalog named `%`"),
    )
    .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(Some(LikePattern::new(r"\%").unwrap()), None, None, None)
        .await
        .expect("list_tables escaped % catalog maps not-found to empty");
    assert_eq!(col_names(&stream.schema())[0], "TABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
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
async fn list_tables_literal_catalog_emits_in_catalog_unescaped() {
    // A literal catalog with an escaped underscore (`comp\_tests`) must
    // route to the fast exact path: `SHOW TABLES IN CATALOG comp_tests`
    // (unescaped, backtick-quoted), NOT the all-catalogs path. The
    // body-matched stub only responds if that SQL was emitted.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t1")]),
        ("catalogName", vec![Some("comp_tests")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `comp_tests`",
        succeeded_inline_response_body(&raw),
    )
    .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(
            Some(LikePattern::new(r"comp\_tests").unwrap()),
            None,
            None,
            None,
        )
        .await
        .expect("list_tables literal catalog");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn list_tables_wildcard_catalog_fans_out_per_matching_catalog() {
    // A wildcard catalog (`comp_%`) must NOT scan all catalogs. Instead
    // it enumerates catalogs (`SHOW CATALOGS`), filters names, and issues
    // one `SHOW TABLES IN CATALOG <name>` per match. `comp_%` matches
    // `comp_tests` and `comp-x` (the `_` is one char) but not `other`.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // `SHOW CATALOGS` → three catalogs (server column is `catalog`).
    let catalogs = string_batch(vec![(
        "catalog",
        vec![Some("comp_tests"), Some("comp-x"), Some("other")],
    )]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        succeeded_inline_response_body(&catalogs),
    )
    .await;

    // Per-catalog `SHOW TABLES IN CATALOG <name>`: one stub per matching
    // catalog, each returning its own table so we can assert both were
    // queried and that `other` was not.
    let tables_comp_tests = string_batch(vec![
        ("namespace", vec![Some("s")]),
        ("tableName", vec![Some("t_ct")]),
        ("catalogName", vec![Some("comp_tests")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `comp_tests`",
        succeeded_inline_response_body(&tables_comp_tests),
    )
    .await;
    let tables_comp_x = string_batch(vec![
        ("namespace", vec![Some("s")]),
        ("tableName", vec![Some("t_cx")]),
        ("catalogName", vec![Some("comp-x")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `comp-x`",
        succeeded_inline_response_body(&tables_comp_x),
    )
    .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(Some(LikePattern::new("comp_%").unwrap()), None, None, None)
        .await
        .expect("list_tables wildcard catalog");
    let batches = drain_stream(&mut stream).await;

    // Rows from both matching catalogs, none from `other`. (If the kernel
    // had queried `other`, no stub matches `IN CATALOG \`other\`` and the
    // call would have errored.)
    let mut cats: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let arr = b.column_by_name("TABLE_CAT").unwrap().as_string::<i32>();
            (0..arr.len())
                .map(|i| arr.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    cats.sort();
    assert_eq!(cats, vec!["comp-x".to_owned(), "comp_tests".to_owned()]);
}

#[tokio::test]
async fn list_tables_speculation_preserves_catalog_matches_types_and_values() {
    let cases = [
        (
            "bench_name",
            "bench_name",
            vec!["bench_name"],
            vec!["bench_name"],
            2,
        ),
        (
            "bench%name",
            "bench%name",
            vec!["bench%name"],
            vec!["bench%name"],
            2,
        ),
        (
            "bench_name",
            "bench_name",
            vec!["bench_name", "benchXname"],
            vec!["bench_name", "benchXname"],
            3,
        ),
        (
            "bench_name",
            "bench_name",
            vec!["benchXname", "bench_name"],
            vec!["benchXname", "bench_name"],
            3,
        ),
        (
            "bench_name",
            "bench_name",
            vec!["bench_name", "bench_name"],
            vec!["bench_name", "bench_name"],
            3,
        ),
        (
            "c_",
            "c_",
            vec!["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c_"],
            vec!["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c_"],
            11,
        ),
        (
            "bench_name",
            "bench_name",
            vec!["benchXname"],
            vec!["benchXname"],
            3,
        ),
        ("bench_name", "bench_name", vec!["other"], vec![], 2),
        (
            "BENCH_NAME",
            "BENCH_NAME",
            vec!["bench_name"],
            vec!["bench_name"],
            2,
        ),
        (
            r"bench\_name%",
            "bench_name%",
            vec!["bench_name"],
            vec!["bench_name"],
            2,
        ),
        (
            r"bench\%name_",
            "bench%name_",
            vec!["bench%name_"],
            vec!["bench%name_"],
            2,
        ),
        ("absent_", "absent_", vec!["bench_name"], vec![], 2),
        ("*", "*", vec!["bench_name"], vec![], 1),
    ];
    let filters: [Option<&[&str]>; 5] = [
        None,
        Some(&[]),
        Some(&["TABLE"]),
        Some(&["VIEW"]),
        Some(&["view"]),
    ];
    let columns = [
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
    ];
    for native in [false, true] {
        for (pattern, literal, catalogs, matched, count) in &cases {
            for filter in filters {
                let server = MockServer::start().await;
                mount_create_session(&server).await;
                mount_delete_session(&server).await;
                let catalog_batch = string_batch(vec![(
                    "catalog",
                    catalogs.iter().map(|name| Some(*name)).collect(),
                )]);
                mount_execute_expecting_sql(
                    &server,
                    "SHOW CATALOGS",
                    closed_inline_response_body(&catalog_batch),
                )
                .await;
                let mut scopes = catalogs.clone();
                if !scopes.contains(literal) {
                    scopes.push(literal);
                }
                for scope in scopes {
                    let canonical = catalogs
                        .iter()
                        .find(|name| name.eq_ignore_ascii_case(scope));
                    let body = if let Some(canonical) = canonical {
                        let batch = if native {
                            string_batch(vec![
                                ("TABLE_CAT", vec![Some(*canonical); 2]),
                                ("TABLE_SCHEM", vec![Some("default"); 2]),
                                ("TABLE_NAME", vec![Some("v"), Some("t")]),
                                ("TABLE_TYPE", vec![Some("VIEW"), Some("TABLE")]),
                                ("REMARKS", vec![None, Some("table comment")]),
                                ("TYPE_CAT", vec![None; 2]),
                                ("TYPE_SCHEM", vec![None; 2]),
                                ("TYPE_NAME", vec![None; 2]),
                                ("SELF_REFERENCING_COL_NAME", vec![None; 2]),
                                ("REF_GENERATION", vec![None; 2]),
                            ])
                        } else {
                            string_batch(vec![
                                ("namespace", vec![Some("default"); 2]),
                                ("tableName", vec![Some("v"), Some("t")]),
                                ("tableType", vec![Some("VIEW"), Some("TABLE")]),
                                ("remarks", vec![None, Some("table comment")]),
                            ])
                        };
                        let mut body: serde_json::Value =
                            serde_json::from_str(&closed_inline_response_body(&batch)).unwrap();
                        body["manifest"]["is_native_metadata_result"] = json!(native);
                        body.to_string()
                    } else {
                        failed_response_body("42704", "CATALOG_NOT_FOUND")
                    };
                    mount_execute_expecting_sql(&server, &format!("IN CATALOG `{scope}`"), body)
                        .await;
                }
                Mock::given(method("DELETE"))
                    .and(path("/api/2.0/sql/statements/stmt-test"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                    .mount(&server)
                    .await;
                let session = open_session(&server).await;
                let mut stream = session
                    .metadata()
                    .list_tables(
                        Some(LikePattern::new(pattern).unwrap()),
                        Some(LikePattern::new("default").unwrap()),
                        Some(LikePattern::new("%").unwrap()),
                        filter,
                    )
                    .await
                    .unwrap();
                assert_eq!(col_names(&stream.schema()), columns);
                let batches = drain_stream(&mut stream).await;
                let rows: Vec<Vec<Option<String>>> = batches
                    .iter()
                    .flat_map(|batch| {
                        (0..batch.num_rows()).map(|row| {
                            batch
                                .columns()
                                .iter()
                                .map(|column| {
                                    let values = column.as_string::<i32>();
                                    (!values.is_null(row)).then(|| values.value(row).to_owned())
                                })
                                .collect()
                        })
                    })
                    .collect();
                let mut matched = matched.clone();
                matched.sort();
                let mut expected = Vec::new();
                for table_type in ["TABLE", "VIEW"] {
                    if filter.is_some_and(|types| !types.contains(&table_type)) {
                        continue;
                    }
                    for catalog in &matched {
                        expected.push(vec![
                            Some((*catalog).to_owned()),
                            Some("default".to_owned()),
                            Some(if table_type == "TABLE" { "t" } else { "v" }.to_owned()),
                            Some(table_type.to_owned()),
                            if table_type == "TABLE" {
                                Some("table comment".to_owned())
                            } else if native {
                                None
                            } else {
                                Some(String::new())
                            },
                            None,
                            None,
                            None,
                            None,
                            None,
                        ]);
                    }
                }
                assert_eq!(
                    rows, expected,
                    "pattern={pattern}, native={native}, types={filter:?}"
                );
                // Unmatched speculation may finish after the typed empty result.
                let statements = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    loop {
                        let statements = server
                            .received_requests()
                            .await
                            .unwrap()
                            .iter()
                            .filter(|request| {
                                request.method == "POST"
                                    && request.url.path() == "/api/2.0/sql/statements"
                            })
                            .count();
                        if statements >= *count {
                            break statements;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("all expected catalog executes must eventually be submitted");
                assert_eq!(
                    statements, *count,
                    "pattern={pattern}, matched={matched:?}, native={native}, types={filter:?}"
                );
                if *count == matched.len() + 2 {
                    assert_eq!(
                        wait_for_close_delete(&server, 1, std::time::Duration::from_secs(2)).await,
                        1,
                        "unused failed speculation must close before fixture teardown"
                    );
                }
                session.close().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn list_tables_speculation_starts_before_catalog_lookup_finishes() {
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    let catalogs = string_batch(vec![("catalog", vec![Some("bench_name")])]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW CATALOGS"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(closed_inline_response_body(&catalogs))
                .set_delay(Duration::from_secs(1)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let table = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("IN CATALOG `bench_name`"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(closed_inline_response_body(&table)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let session = open_session(&server).await;
    let metadata = session.metadata();
    let future = metadata.list_tables(
        Some(LikePattern::new("bench_name").unwrap()),
        None,
        None,
        None,
    );
    tokio::pin!(future);
    let both_started = async {
        loop {
            let count = server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| {
                    request.method == "POST" && request.url.path() == "/api/2.0/sql/statements"
                })
                .count();
            if count == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        _ = &mut future => panic!("metadata completed before delayed catalog lookup"),
        result = tokio::time::timeout(Duration::from_millis(500), both_started) => {
            result.expect("table request must overlap delayed catalog lookup");
        }
    }
    let mut stream = tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .unwrap()
        .unwrap();
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn list_tables_not_found_speculation_retries_after_catalog_confirmation() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let sql_state = "42704";
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    let catalogs = string_batch(vec![("catalog", vec![Some("bench_name")])]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        closed_inline_response_body(&catalogs),
    )
    .await;
    let table = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    let success = closed_inline_response_body(&table);
    let failure = failed_response_body(sql_state, "speculative failure");
    let executions = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&executions);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("IN CATALOG `bench_name`"))
        .respond_with(move |_: &wiremock::Request| {
            let body = if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                &failure
            } else {
                &success
            };
            ResponseTemplate::new(200).set_body_string(body)
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(
            Some(LikePattern::new("bench_name").unwrap()),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        drain_stream(&mut stream)
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    session.close().await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn list_tables_read_phase_not_found_errors_propagate_without_retry() {
    for native in [false, true] {
        for (pattern, catalog) in [("c%", "c2"), ("c_", "c_")] {
            for error_code in [
                "INVALID_PARAMETER_VALUE",
                "SCHEMA_NOT_FOUND",
                "TABLE_OR_VIEW_NOT_FOUND",
                "NO_SUCH_CATALOG_EXCEPTION",
            ] {
                let server = MockServer::start().await;
                mount_create_session(&server).await;
                mount_delete_session(&server).await;
                let catalogs = string_batch(vec![("catalog", vec![Some("c1"), Some(catalog)])]);
                mount_execute_expecting_sql(
                    &server,
                    "SHOW CATALOGS",
                    closed_inline_response_body(&catalogs),
                )
                .await;
                let table = string_batch(vec![
                    ("namespace", vec![Some("default")]),
                    ("tableName", vec![Some("t")]),
                    ("tableType", vec![Some("TABLE")]),
                ]);
                mount_execute_expecting_sql(
                    &server,
                    "IN CATALOG `c1`",
                    closed_inline_response_body(&table),
                )
                .await;
                let batch = if native {
                    string_batch(vec![
                        ("TABLE_CAT", vec![Some(catalog)]),
                        ("TABLE_SCHEM", vec![Some("default")]),
                        ("TABLE_NAME", vec![Some("t")]),
                        ("TABLE_TYPE", vec![Some("TABLE")]),
                        ("REMARKS", vec![None]),
                        ("TYPE_CAT", vec![None]),
                        ("TYPE_SCHEM", vec![None]),
                        ("TYPE_NAME", vec![None]),
                        ("SELF_REFERENCING_COL_NAME", vec![None]),
                        ("REF_GENERATION", vec![None]),
                    ])
                } else {
                    table
                };
                let mut body: serde_json::Value =
                    serde_json::from_str(&succeeded_inline_response_body(&batch)).unwrap();
                body["statement_id"] = json!("read-error");
                body["manifest"]["is_native_metadata_result"] = json!(native);
                body["manifest"]["total_chunk_count"] = json!(2);
                body["manifest"]["total_row_count"] = json!(2);
                body["result"]["next_chunk_index"] = json!(1);
                Mock::given(method("POST"))
                    .and(path("/api/2.0/sql/statements"))
                    .and(body_string_contains(format!("IN CATALOG `{catalog}`")))
                    .respond_with(ResponseTemplate::new(200).set_body_json(body))
                    .expect(1)
                    .mount(&server)
                    .await;
                Mock::given(method("GET"))
                    .and(path("/api/2.0/sql/statements/read-error/result/chunks/1"))
                    .respond_with(
                        ResponseTemplate::new(400).set_body_json(json!({"error_code": error_code})),
                    )
                    .expect(1)
                    .mount(&server)
                    .await;
                Mock::given(method("DELETE"))
                    .and(path("/api/2.0/sql/statements/read-error"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                    .expect(1)
                    .mount(&server)
                    .await;
                let session = open_session(&server).await;
                let error = session
                    .metadata()
                    .list_tables(Some(LikePattern::new(pattern).unwrap()), None, None, None)
                    .await
                    .err()
                    .expect("a read error must not return silently partial catalog rows");
                assert!(error.to_string().contains(error_code), "{error}");
                session.close().await.unwrap();
                server.verify().await;
            }
        }
    }
}

#[tokio::test]
async fn list_tables_confirmed_speculative_errors_do_not_repeat_the_query() {
    for sql_state in ["42501", "HY000"] {
        for catalogs in [vec!["bench_name"], vec!["benchXname", "bench_name"]] {
            let server = MockServer::start().await;
            mount_create_session(&server).await;
            mount_delete_session(&server).await;
            let catalogs =
                string_batch(vec![("catalog", catalogs.into_iter().map(Some).collect())]);
            mount_execute_expecting_sql(
                &server,
                "SHOW CATALOGS",
                closed_inline_response_body(&catalogs),
            )
            .await;
            let other_table = string_batch(vec![
                ("namespace", vec![Some("default")]),
                ("tableName", vec![Some("other")]),
                ("tableType", vec![Some("TABLE")]),
            ]);
            mount_execute_expecting_sql(
                &server,
                "IN CATALOG `benchXname`",
                closed_inline_response_body(&other_table),
            )
            .await;
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .and(body_string_contains("IN CATALOG `bench_name`"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_string(failed_response_body(
                        sql_state,
                        "confirmed speculative error",
                    )),
                )
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("DELETE"))
                .and(path("/api/2.0/sql/statements/stmt-test"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                .expect(1)
                .mount(&server)
                .await;
            let session = open_session(&server).await;
            let error = session
                .metadata()
                .list_tables(
                    Some(LikePattern::new("bench_name").unwrap()),
                    None,
                    None,
                    None,
                )
                .await
                .err()
                .expect("confirmed error must propagate");
            assert_eq!(error.sql_state.as_deref(), Some(sql_state));
            assert!(error.to_string().contains("confirmed speculative error"));
            session.close().await.unwrap();
            server.verify().await;
        }
    }
}

#[tokio::test]
async fn list_tables_unmatched_speculative_error_does_not_fail_other_catalogs() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    let catalogs = string_batch(vec![("catalog", vec![Some("benchXname")])]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        closed_inline_response_body(&catalogs),
    )
    .await;
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `bench_name`",
        failed_response_body("42501", "unmatched speculative error"),
    )
    .await;
    let table = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("catalogName", vec![Some("benchXname")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `benchXname`",
        closed_inline_response_body(&table),
    )
    .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(
            Some(LikePattern::new("bench_name").unwrap()),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(0),
        "benchXname"
    );
    session.close().await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn list_tables_ineligible_patterns_skip_slow_unusable_speculation() {
    use std::time::Duration;

    let cases = [
        ("%ai%", "%ai%", "main"),
        (
            "metadata_benchmark%",
            "metadata_benchmark%",
            "metadata_benchmarking",
        ),
        (
            r"%\_benchmarking%",
            "%_benchmarking%",
            "metadata_benchmarking",
        ),
        (r"\%%", "%%", "%real"),
        (r"bench\%name_", "bench%name_", "bench%nameX"),
        ("bench*_name", "bench*_name", "bench*Xname"),
        ("BENCH_NAME", "BENCH_NAME", "bench_name"),
        ("Bench_name", "Bench_name", "bench_name"),
    ];
    for (pattern, literal, matched) in cases {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        let catalogs = string_batch(vec![("catalog", vec![Some(matched)])]);
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains("SHOW CATALOGS"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(closed_inline_response_body(&catalogs))
                    .set_delay(Duration::from_millis(50)),
            )
            .mount(&server)
            .await;
        let table = string_batch(vec![
            ("namespace", vec![Some("default")]),
            ("tableName", vec![Some("t")]),
            ("catalogName", vec![Some(matched)]),
            ("tableType", vec![Some("TABLE")]),
        ]);
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(format!("IN CATALOG `{literal}`")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(closed_inline_response_body(&table))
                    .set_delay(Duration::from_secs(2)),
            )
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(format!("IN CATALOG `{matched}`")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(closed_inline_response_body(&table))
                    .set_delay(Duration::from_millis(50)),
            )
            .expect(1)
            .mount(&server)
            .await;
        let session = open_session(&server).await;
        let metadata = session.metadata();
        let mut stream = tokio::time::timeout(
            Duration::from_secs(1),
            metadata.list_tables(Some(LikePattern::new(pattern).unwrap()), None, None, None),
        )
        .await
        .expect("broad patterns must not wait for an unusable speculative scan")
        .unwrap();
        let batches = drain_stream(&mut stream).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let statements = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| {
                request.method == "POST" && request.url.path() == "/api/2.0/sql/statements"
            })
            .count();
        assert_eq!(statements, 2, "pattern={pattern}");
        session.close().await.unwrap();
        server.verify().await;
    }
}

#[tokio::test]
async fn list_tables_slow_literal_in_multi_match_is_fetched_only_once() {
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    let catalogs = string_batch(vec![("catalog", vec![Some("c-t"), Some("c_t")])]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW CATALOGS"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(closed_inline_response_body(&catalogs))
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&server)
        .await;
    for catalog in ["c-t", "c_t"] {
        let table = string_batch(vec![
            ("namespace", vec![Some("default")]),
            ("tableName", vec![Some("t")]),
            ("catalogName", vec![Some(catalog)]),
            ("tableType", vec![Some("TABLE")]),
        ]);
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(format!("IN CATALOG `{catalog}`")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(closed_inline_response_body(&table))
                    .set_delay(Duration::from_millis(500)),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    let session = open_session(&server).await;
    let metadata = session.metadata();
    let mut stream = tokio::time::timeout(
        Duration::from_millis(800),
        metadata.list_tables(Some(LikePattern::new("c_t").unwrap()), None, None, None),
    )
    .await
    .expect("other matched catalogs must not wait for the speculative listing")
    .unwrap();
    let batches = drain_stream(&mut stream).await;
    let catalogs: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|value| value.unwrap().to_owned())
        })
        .collect();
    assert_eq!(catalogs, vec!["c-t", "c_t"]);
    session.close().await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn list_tables_speculation_closes_result_when_catalog_lookup_fails() {
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW CATALOGS"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(
                    failed_response_body("42501", "catalog lookup failure")
                        .replace("stmt-test", "failed-catalog-lookup"),
                )
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&server)
        .await;
    let table = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("IN CATALOG `bench_name`"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(
                    succeeded_inline_response_body(&table)
                        .replace("stmt-test", "unused-speculation"),
                )
                .set_delay(Duration::from_secs(2)),
        )
        .expect(1)
        .mount(&server)
        .await;
    for statement_id in ["failed-catalog-lookup", "unused-speculation"] {
        Mock::given(method("DELETE"))
            .and(path(format!("/api/2.0/sql/statements/{statement_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
    }
    let session = open_session(&server).await;
    let metadata = session.metadata();
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        metadata.list_tables(
            Some(LikePattern::new("bench_name").unwrap()),
            None,
            None,
            None,
        ),
    )
    .await
    .expect("lookup failure must not wait for speculative execution");
    let error = result.err().expect("catalog lookup failure must propagate");
    assert!(error.to_string().contains("catalog lookup failure"));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| {
                    request.method == "DELETE"
                        && request.url.path() == "/api/2.0/sql/statements/unused-speculation"
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached execution must eventually close its known statement");
    server.verify().await;
    session.close().await.unwrap();
}

#[tokio::test]
async fn list_tables_unused_execution_is_released_before_other_catalogs_finish() {
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    let catalogs = string_batch(vec![("catalog", vec![Some("benchXname")])]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW CATALOGS"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(closed_inline_response_body(&catalogs))
                .set_delay(Duration::from_millis(50)),
        )
        .mount(&server)
        .await;
    let table = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `bench_name`",
        succeeded_inline_response_body(&table),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("IN CATALOG `benchXname`"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(closed_inline_response_body(&table))
                .set_delay(Duration::from_millis(500)),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let session = open_session(&server).await;
    let metadata = session.metadata();
    let mut future = Box::pin(metadata.list_tables(
        Some(LikePattern::new("bench_name").unwrap()),
        None,
        None,
        None,
    ));
    tokio::select! {
        _ = &mut future => panic!("the other catalog must still be pending"),
        count = wait_for_close_delete(&server, 1, Duration::from_millis(300)) => {
            assert_eq!(count, 1, "unused execution must be released after discovery");
        }
    }
    let mut stream = future.await.unwrap();
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0].column(0).as_string::<i32>().value(0),
        "benchXname"
    );
    session.close().await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn list_tables_canceled_pending_speculation_closes_after_execute() {
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    let catalogs = string_batch(vec![("catalog", vec![Some("bench_name")])]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW CATALOGS"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(closed_inline_response_body(&catalogs))
                .set_delay(Duration::from_secs(2)),
        )
        .mount(&server)
        .await;
    let table = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("IN CATALOG `bench_name`"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(succeeded_inline_response_body(&table))
                .set_delay(Duration::from_millis(200)),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let session = open_session(&server).await;
    let metadata = session.metadata();
    let mut future = Box::pin(metadata.list_tables(
        Some(LikePattern::new("bench_name").unwrap()),
        None,
        None,
        None,
    ));
    let executing = async {
        loop {
            if server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| {
                    String::from_utf8_lossy(&request.body).contains("IN CATALOG `bench_name`")
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        _ = &mut future => panic!("discovery must still be pending"),
        result = tokio::time::timeout(Duration::from_secs(1), executing) => result.unwrap(),
    }
    drop(future);
    assert_eq!(
        wait_for_close_delete(&server, 1, Duration::from_secs(2)).await,
        1
    );
    session.close().await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn list_tables_canceled_speculation_completes_one_close() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    let catalogs = string_batch(vec![("catalog", vec![Some("bench_name")])]);
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW CATALOGS"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(closed_inline_response_body(&catalogs)),
        )
        .mount(&server)
        .await;
    let table = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `bench_name`",
        succeeded_inline_response_body(&table).replace("stmt-test", "canceled-speculation"),
    )
    .await;
    let closes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&closes);
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/canceled-speculation"))
        .respond_with(move |_: &wiremock::Request| {
            let response = ResponseTemplate::new(200).set_body_json(json!({}));
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                response.set_delay(Duration::from_millis(150))
            } else {
                ResponseTemplate::new(404)
                    .set_body_json(json!({ "error_code": "STATEMENT_NOT_FOUND" }))
            }
        })
        .expect(1)
        .mount(&server)
        .await;
    let session = open_session(&server).await;
    let metadata = session.metadata();
    let mut future = Box::pin(metadata.list_tables(
        Some(LikePattern::new("bench_name").unwrap()),
        None,
        None,
        None,
    ));
    let closing = async {
        while closes.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        _ = &mut future => panic!("delayed statement close must still be pending"),
        result = tokio::time::timeout(Duration::from_secs(2), closing) => {
            result.expect("speculation must start closing its statement");
        }
    }
    drop(future);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    server.verify().await;
    session.close().await.unwrap();
}

#[tokio::test]
async fn list_tables_percent_catalog_uses_single_all_catalogs_query() {
    // `%` matches every catalog and is semantically identical to `None`,
    // so it must take the single `SHOW TABLES IN ALL CATALOGS` path, NOT
    // enumerate + fan out. The stub only matches `IN ALL CATALOGS`; if the
    // kernel instead issued `SHOW CATALOGS` + per-catalog queries, no stub
    // would match and the call would fail.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![
        ("namespace", vec![Some("s")]),
        ("tableName", vec![Some("t1")]),
        ("catalogName", vec![Some("anycat")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN ALL CATALOGS",
        succeeded_inline_response_body(&raw),
    )
    .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(Some(LikePattern::new("%").unwrap()), None, None, None)
        .await
        .expect("list_tables %% catalog");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn list_tables_wildcard_skips_catalog_dropped_mid_enumeration() {
    // Resilience contract: a catalog that vanishes between SHOW CATALOGS
    // and its SHOW TABLES (object-not-found) contributes zero rows rather
    // than failing the whole call.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let catalogs = string_batch(vec![("catalog", vec![Some("comp_a"), Some("comp_b")])]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        succeeded_inline_response_body(&catalogs),
    )
    .await;

    // comp_a returns a table; comp_b was dropped → 42704 not-found.
    let tables_a = string_batch(vec![
        ("namespace", vec![Some("s")]),
        ("tableName", vec![Some("t_a")]),
        ("catalogName", vec![Some("comp_a")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `comp_a`",
        succeeded_inline_response_body(&tables_a),
    )
    .await;
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `comp_b`",
        failed_response_body("42704", "catalog not found"),
    )
    .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(Some(LikePattern::new("comp_%").unwrap()), None, None, None)
        .await
        .expect("list_tables tolerates a dropped catalog");
    let batches = drain_stream(&mut stream).await;
    let cats: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let arr = b.column_by_name("TABLE_CAT").unwrap().as_string::<i32>();
            (0..arr.len())
                .map(|i| arr.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(cats, vec!["comp_a".to_owned()]);
}

#[tokio::test]
async fn list_tables_wildcard_propagates_non_not_found_error() {
    // A real (non-not-found) per-catalog error must abort the whole call —
    // NOT silently return partial results.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let catalogs = string_batch(vec![("catalog", vec![Some("comp_a")])]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        succeeded_inline_response_body(&catalogs),
    )
    .await;
    // Permission error (42501), not object-not-found.
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `comp_a`",
        failed_response_body("42501", "insufficient permissions"),
    )
    .await;

    let session = open_session(&server).await;
    let result = session
        .metadata()
        .list_tables(Some(LikePattern::new("comp_%").unwrap()), None, None, None)
        .await;
    match result {
        Err(e) => assert!(
            e.message.contains("insufficient permissions")
                || e.sql_state.as_deref() == Some("42501"),
            "expected the permission error to propagate, got: {} / {:?}",
            e.message,
            e.sql_state
        ),
        Ok(mut s) => {
            // A stream may resolve lazily; draining must surface the error.
            let err = async {
                while s.next_batch().await?.is_some() {}
                Ok::<_, databricks_sql_kernel::Error>(())
            }
            .await
            .expect_err("non-not-found per-catalog error must propagate");
            assert!(
                err.message.contains("insufficient permissions")
                    || err.sql_state.as_deref() == Some("42501")
            );
        }
    }
}

#[tokio::test]
async fn list_tables_wildcard_zero_catalogs_match_returns_empty() {
    // A wildcard pattern that matches no catalog returns a typed empty
    // result and issues NO per-catalog query (no `IN CATALOG` stub is
    // mounted, so any such call would fail).
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let catalogs = string_batch(vec![("catalog", vec![Some("main"), Some("other")])]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        succeeded_inline_response_body(&catalogs),
    )
    .await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(Some(LikePattern::new("zzz%").unwrap()), None, None, None)
        .await
        .expect("list_tables zero-match");
    assert_eq!(col_names(&stream.schema())[0], "TABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn list_tables_wildcard_fans_out_beyond_inflight_cap() {
    // >8 matching catalogs exercises the bounded-parallel refill loop: all
    // N must be queried and their rows returned, not just the first 8.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let names: Vec<String> = (0..12).map(|i| format!("cat_{i:02}")).collect();
    let name_refs: Vec<Option<&str>> = names.iter().map(|s| Some(s.as_str())).collect();
    let catalogs = string_batch(vec![("catalog", name_refs)]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        succeeded_inline_response_body(&catalogs),
    )
    .await;
    // One per-catalog stub each, keyed on its exact IN CATALOG SQL.
    for name in &names {
        let tables = string_batch(vec![
            ("namespace", vec![Some("s")]),
            ("tableName", vec![Some("t")]),
            ("catalogName", vec![Some(name.as_str())]),
            ("tableType", vec![Some("TABLE")]),
        ]);
        mount_execute_expecting_sql(
            &server,
            &format!("IN CATALOG `{name}`"),
            succeeded_inline_response_body(&tables),
        )
        .await;
    }

    let session = open_session(&server).await;
    // `cat_%` matches all 12 (but is not a match-all `%`, so it fans out).
    let mut stream = session
        .metadata()
        .list_tables(Some(LikePattern::new("cat%").unwrap()), None, None, None)
        .await
        .expect("list_tables >8 fan-out");
    let batches = drain_stream(&mut stream).await;
    let mut cats: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let arr = b.column_by_name("TABLE_CAT").unwrap().as_string::<i32>();
            (0..arr.len())
                .map(|i| arr.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    cats.sort();
    assert_eq!(cats, names, "all 12 catalogs must be queried and returned");
}

#[tokio::test]
async fn list_tables_empty_catalog_matches_nothing_without_wire_call() {
    // An empty catalog pattern is "match nothing": no `/statements` stub
    // is mounted, so if the kernel made a wire call the test would hang /
    // fail. It must instead return an empty result locally.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_tables(Some(LikePattern::new("").unwrap()), None, None, None)
        .await
        .expect("list_tables empty catalog");
    // Still the JDBC getTables shape, zero rows.
    assert_eq!(col_names(&stream.schema())[0], "TABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0);
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
    let ordinal = batches[0]
        .column_by_name("ORDINAL_POSITION")
        .unwrap()
        .as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(ordinal.value(0), 1);
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
async fn list_columns_empty_catalog_matches_nothing_without_wire_call() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let catalog = Some(String::new());
    let requested_pattern = Some("ignored%".to_owned());
    let (catalog, schema_pattern) =
        exact_catalog_and_pattern(&catalog, &requested_pattern).unwrap();
    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_columns(catalog, schema_pattern, None, None)
        .await
        .expect("empty catalog returns empty columns");

    assert_eq!(col_names(&stream.schema())[0], "TABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn list_columns_empty_table_pattern_matches_nothing_without_wire_call() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_columns(
            Some(Identifier::new("main").unwrap()),
            None,
            Some(LikePattern::new("").unwrap()),
            None,
        )
        .await
        .expect("empty table pattern returns empty columns");

    assert_eq!(col_names(&stream.schema())[0], "TABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn list_columns_empty_column_pattern_matches_nothing_without_wire_call() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .list_columns(
            Some(Identifier::new("main").unwrap()),
            None,
            None,
            Some(LikePattern::new("").unwrap()),
        )
        .await
        .expect("empty column pattern returns empty columns");

    assert_eq!(col_names(&stream.schema())[0], "TABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
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
    // the JDBC `getCrossReference` set. This older response omits
    // rule columns, so they remain null along with PK_NAME.
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
    // Absent rule fields and PK_NAME remain null.
    assert!(b.column_by_name("UPDATE_RULE").unwrap().is_null(0));
    assert!(b.column_by_name("DELETE_RULE").unwrap().is_null(0));
    assert!(b.column_by_name("PK_NAME").unwrap().is_null(0));
}

#[tokio::test]
async fn foreign_keys_preserves_rules_for_show_and_native_responses() {
    for native in [false, true] {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;

        let names = if native {
            [
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
                "DEFERRABILITY",
            ]
        } else {
            [
                "parentCatalogName",
                "parentNamespace",
                "parentTableName",
                "parentColName",
                "catalogName",
                "namespace",
                "tableName",
                "col_name",
                "keySeq",
                "updateRule",
                "deleteRule",
                "constraintName",
                "deferrability",
            ]
        };
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["main"; 3])),
            Arc::new(StringArray::from(vec!["d"; 3])),
            Arc::new(StringArray::from(vec!["parent"; 3])),
            Arc::new(StringArray::from(vec!["id"; 3])),
            Arc::new(StringArray::from(vec!["main"; 3])),
            Arc::new(StringArray::from(vec!["d"; 3])),
            Arc::new(StringArray::from(vec!["child"; 3])),
            Arc::new(StringArray::from(vec!["pid"; 3])),
            Arc::new(Int32Array::from(vec![1; 3])),
            Arc::new(Int32Array::from(vec![Some(3), Some(0), None])),
            Arc::new(Int32Array::from(vec![Some(3), Some(2), Some(4)])),
            Arc::new(StringArray::from(vec!["fk_child"; 3])),
            Arc::new(Int32Array::from(vec![7; 3])),
        ];
        let mut fields: Vec<_> = names
            .into_iter()
            .zip(&columns)
            .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
            .collect();
        if native {
            fields.insert(12, Field::new("PK_NAME", DataType::Utf8, true));
            columns.insert(12, Arc::new(StringArray::from(vec![None::<&str>; 3])));
        }
        let raw = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let body = if native {
            native_inline_response_body(&raw)
        } else {
            succeeded_inline_response_body(&raw)
        };
        mount_execute_expecting_sql(&server, "SHOW FOREIGN KEYS", body).await;

        let session = open_session(&server).await;
        let mut stream = session
            .metadata()
            .foreign_keys(
                Some(Identifier::new("main").unwrap()),
                Some(Identifier::new("d").unwrap()),
                Some(Identifier::new("parent").unwrap()),
                Some(Identifier::new("main").unwrap()),
                Some(Identifier::new("d").unwrap()),
                Some(Identifier::new("child").unwrap()),
            )
            .await
            .expect("foreign_keys");
        let batches = drain_stream(&mut stream).await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        for (name, expected) in [
            ("UPDATE_RULE", vec![Some(3), Some(0), None]),
            ("DELETE_RULE", vec![Some(3), Some(2), Some(4)]),
        ] {
            let values = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("rule column is Int16");
            assert_eq!(values, &Int16Array::from(expected), "native={native}");
        }
        assert_eq!(batch.column_by_name("PK_NAME").unwrap().null_count(), 3);
    }
}

#[tokio::test]
async fn foreign_keys_null_foreign_table_returns_empty_without_wire_call() {
    // Conformance (METADATA-037): the PK (parent) side is fully
    // specified but the FK (foreign) table is None. Per the JDBC SEA
    // `listCrossReferences` contract, an unspecified foreign table is
    // "nothing to look up" → empty result, and NO `SHOW FOREIGN KEYS`
    // (no `/statements` call). No `/statements` stub is mounted, so a
    // wire call would fail the test.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .foreign_keys(
            Some(Identifier::new("main").unwrap()),   // parent_catalog
            Some(Identifier::new("d").unwrap()),      // parent_schema
            Some(Identifier::new("parent").unwrap()), // parent_table (PK side set)
            Some(Identifier::new("main").unwrap()),   // foreign_catalog
            Some(Identifier::new("d").unwrap()),      // foreign_schema
            None,                                     // foreign_table = None → empty
        )
        .await
        .expect("foreign_keys null foreign table must not error");

    // Correct JDBC getCrossReference shape, zero rows.
    assert_eq!(col_names(&stream.schema())[0], "PKTABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    // No ExecuteStatement was issued.
    let calls = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/api/2.0/sql/statements")
        .count();
    assert_eq!(calls, 0, "no SHOW FOREIGN KEYS should be issued");
}

#[tokio::test]
async fn foreign_keys_both_tables_null_returns_empty_without_wire_call() {
    // Conformance (METADATA-038): both parent and foreign tables are
    // None. On SEA this follows the JDBC SEA `listCrossReferences`
    // contract (keys off the foreign table only) → empty result, no
    // wire call — rather than Thrift's both-tables-null 42000 rejection.
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session
        .metadata()
        .foreign_keys(
            None,                                   // parent_catalog
            None,                                   // parent_schema
            None,                                   // parent_table
            Some(Identifier::new("main").unwrap()), // foreign_catalog
            Some(Identifier::new("d").unwrap()),    // foreign_schema
            None,                                   // foreign_table = None
        )
        .await
        .expect("foreign_keys both tables null must not error");

    assert_eq!(col_names(&stream.schema())[0], "PKTABLE_CAT");
    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    let calls = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/api/2.0/sql/statements")
        .count();
    assert_eq!(calls, 0, "no SHOW FOREIGN KEYS should be issued");
}

#[tokio::test]
async fn primary_keys_resolves_missing_names_and_preserves_explicit_names() {
    for catalog in [None, Some("explicit_catalog"), Some("")] {
        for schema in [None, Some("explicit_schema"), Some("")] {
            let server = MockServer::start().await;
            mount_create_session(&server).await;
            mount_delete_session(&server).await;
            let needs_lookup = catalog.is_none() || schema.is_none();
            let current = string_batch(vec![
                ("current_catalog()", vec![Some("current_catalog")]),
                ("current_schema()", vec![Some("current_schema")]),
            ]);
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .and(body_partial_json(json!({
                    "statement": "SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()",
                    "session_id": "sess-test",
                })))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_string(closed_inline_response_body(&current)),
                )
                .expect(u64::from(needs_lookup))
                .mount(&server)
                .await;

            let expected_catalog = catalog.unwrap_or("current_catalog");
            let expected_schema = schema.unwrap_or("current_schema");
            let sql = format!(
                "SHOW KEYS IN CATALOG `{expected_catalog}` IN SCHEMA `{expected_schema}` IN TABLE `t`"
            );
            let raw = string_batch(vec![
                ("catalogName", vec![Some(expected_catalog)]),
                ("namespace", vec![Some(expected_schema)]),
                ("tableName", vec![Some("t")]),
                ("col_name", vec![Some("id")]),
                ("keySeq", vec![Some("1")]),
                ("constraintName", vec![Some("pk_t")]),
                ("constraintType", vec![Some("PRIMARY")]),
            ]);
            let has_empty_name = catalog == Some("") || schema == Some("");
            let body = if has_empty_name {
                failed_response_body("42704", "SCHEMA_NOT_FOUND")
            } else {
                closed_inline_response_body(&raw)
            };
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .and(body_partial_json(json!({
                    "statement": sql,
                    "session_id": "sess-test",
                })))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .expect(1)
                .mount(&server)
                .await;

            let session = open_session(&server).await;
            let mut stream = session
                .metadata()
                .primary_keys(
                    catalog.map(|value| Identifier::new_allow_empty(value).unwrap()),
                    schema.map(|value| Identifier::new_allow_empty(value).unwrap()),
                    Identifier::new("t").unwrap(),
                )
                .await
                .expect("primary_keys resolves missing namespace components");
            assert_eq!(col_names(&stream.schema())[0], "TABLE_CAT");
            let batches = drain_stream(&mut stream).await;
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                usize::from(!has_empty_name)
            );
            if !has_empty_name {
                assert_eq!(
                    batches[0].column(0).as_string::<i32>().value(0),
                    expected_catalog
                );
                assert_eq!(
                    batches[0].column(1).as_string::<i32>().value(0),
                    expected_schema
                );
            }
            session.close().await.unwrap();
            server.verify().await;
        }
    }
}

#[tokio::test]
async fn primary_keys_rejects_invalid_current_namespace_and_closes_lookup() {
    use databricks_sql_kernel::ErrorCode;

    let invalid_batches = [
        (
            ErrorCode::DataLoss,
            string_batch(vec![
                ("catalog", vec![None]),
                ("schema", vec![Some("default")]),
            ]),
        ),
        (
            ErrorCode::DataLoss,
            string_batch(vec![
                ("catalog", vec![Some("main")]),
                ("schema", vec![None]),
            ]),
        ),
        (
            ErrorCode::InvalidArgument,
            string_batch(vec![("catalog", vec![]), ("schema", vec![])]),
        ),
        (
            ErrorCode::DataLoss,
            string_batch(vec![("catalog", vec![Some("main")])]),
        ),
        (
            ErrorCode::InvalidArgument,
            string_batch(vec![
                ("catalog", vec![Some("")]),
                ("schema", vec![Some("default")]),
            ]),
        ),
        (
            ErrorCode::InvalidArgument,
            string_batch(vec![
                ("catalog", vec![Some("main")]),
                ("schema", vec![Some("")]),
            ]),
        ),
    ];
    for (index, (expected_code, batch)) in invalid_batches.iter().enumerate() {
        let server = MockServer::start().await;
        mount_create_session(&server).await;
        mount_delete_session(&server).await;
        mount_execute_expecting_sql(
            &server,
            "SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()",
            succeeded_inline_response_body(batch),
        )
        .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/statements/stmt-test"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .expect(1)
            .mount(&server)
            .await;

        let session = open_session(&server).await;
        let result = session
            .metadata()
            .primary_keys(None, None, Identifier::new("t").unwrap())
            .await;
        let error = match result {
            Ok(_) => panic!("invalid namespace must fail"),
            Err(error) => error,
        };
        assert_eq!(
            &error.code, expected_code,
            "namespace case {index}: {error}"
        );
        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method.as_str() == "POST"
                    && request.url.path() == "/api/2.0/sql/statements")
                .count(),
            1,
            "invalid namespace must not issue SHOW KEYS"
        );
        session.close().await.unwrap();
        server.verify().await;
    }
}

#[tokio::test]
async fn primary_keys_propagates_current_namespace_lookup_errors() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;
    mount_execute_expecting_sql(
        &server,
        "SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()",
        failed_response_body("42501", "PERMISSION_DENIED"),
    )
    .await;

    let session = open_session(&server).await;
    let error = match session
        .metadata()
        .primary_keys(None, None, Identifier::new("t").unwrap())
        .await
    {
        Ok(_) => panic!("lookup failure must propagate"),
        Err(error) => error,
    };
    assert!(error.message.contains("PERMISSION_DENIED"));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_str() == "POST"
                && request.url.path() == "/api/2.0/sql/statements")
            .count(),
        1
    );
    session.close().await.unwrap();
}

// ─── Static-method tests (synthesised) ─────────────────────────────

#[tokio::test]
async fn type_info_returns_static_thrift_rows_without_wire_call() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let session = open_session(&server).await;
    let mut stream = session.metadata().type_info().await.expect("type_info");

    let schema = stream.schema();
    let expected = [
        ("TYPE_NAME", DataType::Utf8),
        ("DATA_TYPE", DataType::Int32),
        ("PRECISION", DataType::Int32),
        ("LITERAL_PREFIX", DataType::Utf8),
        ("LITERAL_SUFFIX", DataType::Utf8),
        ("CREATE_PARAMS", DataType::Utf8),
        ("NULLABLE", DataType::Int16),
        ("CASE_SENSITIVE", DataType::Boolean),
        ("SEARCHABLE", DataType::Int16),
        ("UNSIGNED_ATTRIBUTE", DataType::Boolean),
        ("FIXED_PREC_SCALE", DataType::Boolean),
        ("AUTO_INCREMENT", DataType::Boolean),
        ("LOCAL_TYPE_NAME", DataType::Utf8),
        ("MINIMUM_SCALE", DataType::Int16),
        ("MAXIMUM_SCALE", DataType::Int16),
        ("SQL_DATA_TYPE", DataType::Int32),
        ("SQL_DATETIME_SUB", DataType::Int32),
        ("NUM_PREC_RADIX", DataType::Int32),
    ];
    assert_eq!(schema.fields().len(), expected.len());
    for (field, (name, data_type)) in schema.fields().iter().zip(expected) {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), &data_type);
        assert!(field.is_nullable(), "{name} must be nullable");
    }

    let batches = drain_stream(&mut stream).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 20);
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

// ─── CloseStatement / status.state==CLOSED handling (issue #168) ───

/// Regression for issue #168: when the `ExecuteStatement` response for a
/// metadata query already reports `status.state == CLOSED`, the statement
/// is closed server-side and the kernel must NOT issue a redundant
/// `DELETE /api/2.0/sql/statements/<id>` (CloseStatement) on it — matching
/// the regular-query path. Before the fix, the metadata path always
/// installs a `StatementCloseGuard`, firing the redundant DELETE on stream
/// drop.
#[tokio::test]
async fn metadata_closed_response_skips_close_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![("catalog", vec![Some("main")])]);
    // Inline result with status.state == CLOSED — server already closed it.
    mount_execute(&server, closed_inline_response_body(&raw)).await;

    // Scoped expectation: NO DELETE on the statement endpoint.
    let _no_close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stream = session
            .metadata()
            .list_catalogs()
            .await
            .expect("list_catalogs");
        let _ = drain_stream(&mut stream).await;
        // stream drops here — a redundant close guard would fire now.
    }
    // Give any (erroneously) spawned fire-and-forget close the full window to
    // reach the wire, but break early the instant one appears so the negative
    // assertion stays reliably able to fail under CI load.
    let closes = wait_for_close_delete(&server, 0, std::time::Duration::from_millis(500)).await;
    assert_eq!(closes, 0, "CLOSED response must not trigger CloseStatement");
    // `_no_close` also verifies expect(0) when it drops at end of scope.
}

/// Regression-guard for issue #168's fix: when the metadata
/// `ExecuteStatement` response reports a non-CLOSED terminal state
/// (`SUCCEEDED`), the server still owns the statement, so the kernel MUST
/// still issue exactly one `CloseStatement` (DELETE) on stream drop. This
/// ensures the CLOSED-skip does not over-correct and reintroduce the
/// server-statement leak that `from_reader_with_close` guards against.
#[tokio::test]
async fn metadata_succeeded_response_still_closes_statement() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![("catalog", vec![Some("main")])]);
    // Inline result with status.state == SUCCEEDED — server still owns it.
    mount_execute(&server, succeeded_inline_response_body(&raw)).await;

    // Scoped expectation: exactly one DELETE on the statement endpoint.
    let _close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stream = session
            .metadata()
            .list_catalogs()
            .await
            .expect("list_catalogs");
        let _ = drain_stream(&mut stream).await;
        // stream drops here — the close guard must fire the DELETE.
    }
    // Poll until the fire-and-forget close hits the wire (bounded by a
    // timeout) rather than assuming it lands within a fixed sleep — a close
    // delayed under CI load must not flake this positive assertion.
    let closes = wait_for_close_delete(&server, 1, std::time::Duration::from_millis(2000)).await;
    assert_eq!(
        closes, 1,
        "SUCCEEDED response must trigger exactly one CloseStatement"
    );
    // `_close` also verifies expect(1) when it drops at end of scope.
}

/// Same CLOSED-skip guarantee as
/// [`metadata_closed_response_skips_close_statement`], but exercised
/// through `list_tables()` rather than `list_catalogs()`. Unlike
/// `list_catalogs` (which takes the no-error early-`?` path), the other
/// metadata methods route the transport result through a
/// `match { Ok(r) => r, Err(e) => return not_found_or(...) }` before
/// reaching the shared `close_id` reshape. This test guards that more
/// common code shape so a future refactor that reorders the
/// `close_id`/reshape sequence on a `match`-branch method still gets
/// caught.
#[tokio::test]
async fn metadata_closed_response_skips_close_statement_on_match_branch() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    let raw = string_batch(vec![
        ("namespace", vec![Some("default")]),
        ("tableName", vec![Some("t1")]),
        ("catalogName", vec![Some("main")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    // Inline result with status.state == CLOSED — server already closed it.
    mount_execute(&server, closed_inline_response_body(&raw)).await;

    // Scoped expectation: NO DELETE on the statement endpoint.
    let _no_close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-test"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stream = session
            .metadata()
            .list_tables(None, None, None, None)
            .await
            .expect("list_tables");
        let _ = drain_stream(&mut stream).await;
        // stream drops here — a redundant close guard would fire now.
    }
    // Give any (erroneously) spawned fire-and-forget close the full window to
    // reach the wire, but break early the instant one appears so the negative
    // assertion stays reliably able to fail under CI load.
    let closes = wait_for_close_delete(&server, 0, std::time::Duration::from_millis(500)).await;
    assert_eq!(closes, 0, "CLOSED response must not trigger CloseStatement");
    // `_no_close` also verifies expect(0) when it drops at end of scope.
}

/// Same CLOSED-skip guarantee as
/// [`metadata_closed_response_skips_close_statement`], but for the
/// wildcard-catalog fan-out path
/// (`list_tables_matching_catalogs` → `fetch_tables_for_catalog`). That
/// site does NOT delegate to `from_reader_with_close`; it opens one
/// statement per catalog, computes `close_id(&raw)` (empty when the
/// server already reported `CLOSED`), and only fires an explicit
/// `close_statement()` when the id is non-empty. A wildcard pattern
/// (`comp_%`) is required to reach it — the other CLOSED-skip tests drive
/// `list_catalogs()` / `list_tables(None, …)`, which never enter the
/// fan-out. Stub `SHOW CATALOGS` + a per-catalog `SHOW TABLES` that both
/// return `state == CLOSED`, and assert zero DELETEs on the per-catalog
/// statement, so a future refactor of the bespoke `if !stmt_id.is_empty()`
/// guard that reintroduces the redundant close is caught here.
#[tokio::test]
async fn metadata_closed_response_skips_close_on_wildcard_fanout() {
    let server = MockServer::start().await;
    mount_create_session(&server).await;
    mount_delete_session(&server).await;

    // `SHOW CATALOGS` → one matching catalog, returned CLOSED.
    let catalogs = string_batch(vec![("catalog", vec![Some("comp_a")])]);
    mount_execute_expecting_sql(
        &server,
        "SHOW CATALOGS",
        inline_response_body_with(&catalogs, "stmt-catalogs", "CLOSED"),
    )
    .await;

    // Per-catalog `SHOW TABLES IN CATALOG comp_a`, also returned CLOSED so
    // the fan-out's `close_id` yields an empty id and the explicit
    // `close_statement()` must be skipped.
    let tables = string_batch(vec![
        ("namespace", vec![Some("s")]),
        ("tableName", vec![Some("t_a")]),
        ("catalogName", vec![Some("comp_a")]),
        ("tableType", vec![Some("TABLE")]),
    ]);
    mount_execute_expecting_sql(
        &server,
        "IN CATALOG `comp_a`",
        inline_response_body_with(&tables, "stmt-tables-comp-a", "CLOSED"),
    )
    .await;

    // Scoped expectation: NO DELETE on the per-catalog statement endpoint.
    // This is the statement with the distinct close logic; the bespoke
    // guard must not fire a redundant CloseStatement on it.
    let _no_close = Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/statements/stmt-tables-comp-a"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(0)
        .mount_as_scoped(&server)
        .await;

    let session = open_session(&server).await;
    {
        let mut stream = session
            .metadata()
            .list_tables(Some(LikePattern::new("comp_%").unwrap()), None, None, None)
            .await
            .expect("list_tables wildcard catalog");
        let batches = drain_stream(&mut stream).await;
        // Sanity: the fan-out actually produced the comp_a row (i.e. we
        // exercised `fetch_tables_for_catalog`, not an empty short-circuit).
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    // Give any (erroneously) spawned fire-and-forget close the full window
    // to reach the wire, but break early the instant one appears.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    loop {
        let closes = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| {
                r.method.as_str() == "DELETE"
                    && r.url.path() == "/api/2.0/sql/statements/stmt-tables-comp-a"
            })
            .count();
        assert_eq!(
            closes, 0,
            "CLOSED per-catalog statement must not trigger CloseStatement"
        );
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // `_no_close` also verifies expect(0) when it drops at end of scope.
}
