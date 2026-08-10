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
use wiremock::matchers::{body_string_contains, method, path};
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

/// SEA response body for a FAILED statement with the given SQLSTATE +
/// message — drives the kernel's terminal-error path. A `42704` state
/// is treated as object-not-found (skipped in the fan-out); anything
/// else propagates.
fn failed_response_body(sql_state: &str, message: &str) -> String {
    json!({
        "statement_id": "stmt-test",
        "status": {
            "state": "FAILED",
            "error": {
                "error_code": "BAD_REQUEST",
                "message": message,
                "sql_state": sql_state,
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
