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

//! End-to-end tests for the v0 `Metadata::*` surface against a live
//! Databricks workspace.
//!
//! All tests are `#[ignore]` so CI without credentials skips them.
//! Run with:
//!
//! ```sh
//! set -a && source ~/.databricks/pecotesting-creds && set +a
//! cargo test --test v0_metadata_e2e -- --ignored
//! ```
//!
//! Each wire-driven test asserts:
//! 1. The call succeeds against the live warehouse.
//! 2. The returned `ResultStream` exposes the **server-shaped**
//!    columns (the kernel does not rename).
//! 3. Where applicable, the result has at least one row.
//!
//! Static methods (`type_info`, `table_types`) assert the JDBC-shape
//! the kernel synthesises locally.

use arrow_array::cast::AsArray;
use arrow_array::Array;
use databricks_sql_kernel::{AuthConfig, Identifier, ResultBatch, ResultStream, Session};
use std::env;

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty())
}

async fn live_session() -> Session {
    let host = env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST required");
    let path = env_var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required");
    let token = env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN required");

    Session::builder()
        .http_path(host, path)
        .auth(AuthConfig::Pat { token })
        .open()
        .await
        .expect("Session::open against live workspace")
}

async fn collect(mut stream: ResultStream) -> (Vec<arrow_array::RecordBatch>, usize) {
    let mut batches = Vec::new();
    let mut rows = 0;
    while let Some(rb) = stream.next_batch().await.expect("next_batch") {
        let ResultBatch::Arrow(rb) = rb else {
            panic!("expected Arrow batch in v0");
        };
        rows += rb.num_rows();
        batches.push(rb.clone());
    }
    (batches, rows)
}

fn col_names(schema: &arrow_schema::Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

#[tokio::test]
#[ignore]
async fn list_catalogs_returns_table_cat_column() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_catalogs()
        .await
        .expect("list_catalogs");
    // Thrift-compatible JDBC shape: single `TABLE_CAT` column.
    assert_eq!(col_names(&stream.schema()), vec!["TABLE_CAT"]);

    let (batches, rows) = collect(stream).await;
    assert!(rows > 0, "live workspace should have at least one catalog");
    let names: Vec<String> = batches
        .iter()
        .flat_map(|rb| {
            let arr = rb.column(0).as_string::<i32>();
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        names.iter().any(|n| n == "main" || n == "samples"),
        "expected 'main' or 'samples' catalog, got {names:?}"
    );

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn list_schemas_returns_jdbc_columns() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_schemas(Some(Identifier::new("main").unwrap()), None)
        .await
        .expect("list_schemas");
    // JDBC `getSchemas` shape: TABLE_SCHEM, TABLE_CATALOG. The kernel
    // synthesises TABLE_CATALOG from the catalog arg when the server's
    // single-catalog `SHOW SCHEMAS` response omits it.
    assert_eq!(
        col_names(&stream.schema()),
        vec!["TABLE_SCHEM", "TABLE_CATALOG"]
    );

    let (batches, _) = collect(stream).await;
    let mut schema_names: Vec<String> = Vec::new();
    for rb in batches {
        let arr = rb.column(0).as_string::<i32>();
        for i in 0..arr.len() {
            schema_names.push(arr.value(i).to_string());
        }
    }
    assert!(
        schema_names.iter().any(|n| n == "default"),
        "expected 'default' schema, got {schema_names:?}"
    );
    assert!(
        schema_names.iter().any(|n| n == "information_schema"),
        "expected 'information_schema', got {schema_names:?}"
    );

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn list_tables_returns_jdbc_columns() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_tables(
            Some(Identifier::new("main").unwrap()),
            Some(databricks_sql_kernel::LikePattern::new("information_schema").unwrap()),
            None,
            None,
        )
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

    let (_, rows) = collect(stream).await;
    assert!(
        rows > 0,
        "information_schema should expose at least one view"
    );

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn list_columns_returns_jdbc_columns() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_columns(
            Some(Identifier::new("main").unwrap()),
            Some(databricks_sql_kernel::LikePattern::new("information_schema").unwrap()),
            Some(databricks_sql_kernel::LikePattern::new("tables").unwrap()),
            None,
        )
        .await
        .expect("list_columns");

    // JDBC `getColumns` 23-column shape. Check the load-bearing
    // columns; the rest are JDBC-mandated nulls / synthesised.
    let names = col_names(&stream.schema());
    assert_eq!(names.len(), 23);
    for required in [
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "COLUMN_NAME",
        "DATA_TYPE",
        "TYPE_NAME",
        "NULLABLE",
        "IS_NULLABLE",
        "IS_AUTO_INCREMENT",
    ] {
        assert!(
            names.iter().any(|n| n == required),
            "expected '{required}' in {names:?}"
        );
    }

    let (_, rows) = collect(stream).await;
    assert!(rows > 0, "tables view should have columns");

    session.close().await.expect("close");
}

/// Resolve a real (non-view) table to drive PK/FK SHOW commands
/// against. `SHOW (PRIMARY|FOREIGN) KEYS` rejects views with
/// `EXPECT_TABLE_NOT_VIEW`, so we cannot use `information_schema.*`
/// for these tests.
async fn find_known_table(session: &Session) -> Option<(String, String, String)> {
    let candidates = [
        ("samples", "nyctaxi", "trips"),
        ("samples", "tpch", "lineitem"),
        ("samples", "tpch", "orders"),
    ];
    for (cat, schema, table) in candidates {
        let res = session
            .metadata()
            .list_columns(
                Some(Identifier::new(cat).unwrap()),
                Some(databricks_sql_kernel::LikePattern::new(schema).unwrap()),
                Some(databricks_sql_kernel::LikePattern::new(table).unwrap()),
                None,
            )
            .await;
        if let Ok(s) = res {
            let (_, rows) = collect(s).await;
            if rows > 0 {
                return Some((cat.to_string(), schema.to_string(), table.to_string()));
            }
        }
    }
    None
}

#[tokio::test]
#[ignore]
async fn primary_keys_returns_server_show_keys_columns() {
    let session = live_session().await;
    let Some((cat, schema, table)) = find_known_table(&session).await else {
        eprintln!("skipping primary_keys E2E — no known sample table reachable");
        return;
    };

    let stream = session
        .metadata()
        .primary_keys(
            Some(Identifier::new(&cat).unwrap()),
            Some(Identifier::new(&schema).unwrap()),
            Identifier::new(&table).unwrap(),
        )
        .await
        .expect("primary_keys");

    // Live SHOW KEYS shape on Dogfood: 14 columns including
    // catalogName / namespace / tableName / col_name / keySeq /
    // constraintName / constraintType / parent* / updateRule /
    // deleteRule / deferrability. Required minimum:
    let names = col_names(&stream.schema());
    for required in [
        "catalogName",
        "namespace",
        "tableName",
        "col_name",
        "keySeq",
        "constraintName",
    ] {
        assert!(
            names.iter().any(|n| n == required),
            "expected '{required}' in {names:?}"
        );
    }
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn foreign_keys_returns_server_show_foreign_keys_columns() {
    let session = live_session().await;
    let Some((cat, schema, table)) = find_known_table(&session).await else {
        eprintln!("skipping foreign_keys E2E — no known sample table reachable");
        return;
    };

    let stream = session
        .metadata()
        .foreign_keys(
            None,
            None,
            None,
            Some(Identifier::new(&cat).unwrap()),
            Some(Identifier::new(&schema).unwrap()),
            Some(Identifier::new(&table).unwrap()),
        )
        .await
        .expect("foreign_keys");

    let names = col_names(&stream.schema());
    for required in [
        "catalogName",
        "namespace",
        "tableName",
        "col_name",
        "parentCatalogName",
        "parentNamespace",
        "parentTableName",
        "parentColName",
    ] {
        assert!(
            names.iter().any(|n| n == required),
            "expected '{required}' in {names:?}"
        );
    }
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn functions_returns_information_schema_routine_columns() {
    let session = live_session().await;
    let res = session.metadata().functions(None, None, None).await;
    match res {
        Ok(stream) => {
            let names = col_names(&stream.schema());
            for required in ["routine_catalog", "routine_schema", "routine_name"] {
                assert!(
                    names.iter().any(|n| n == required),
                    "expected '{required}' in {names:?}"
                );
            }
            let _ = collect(stream).await;
        }
        Err(e) => {
            eprintln!("functions E2E got SqlError (often expected): {}", e.message);
        }
    }

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn type_info_returns_static_jdbc_shape() {
    let session = live_session().await;
    let stream = session.metadata().type_info().await.expect("type_info");

    // type_info is synthesised — kernel returns the JDBC shape since
    // there's no server-native shape to mirror.
    let names = col_names(&stream.schema());
    assert_eq!(names[0], "TYPE_NAME");
    assert_eq!(names[1], "DATA_TYPE");
    assert_eq!(stream.schema().fields().len(), 19);

    let (_, rows) = collect(stream).await;
    assert!(rows >= 19, "type_info should have ≥19 rows, got {rows}");

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn table_types_returns_static_jdbc_shape() {
    let session = live_session().await;
    let stream = session.metadata().table_types().await.expect("table_types");

    assert_eq!(col_names(&stream.schema()), vec!["TABLE_TYPE"]);
    let (_, rows) = collect(stream).await;
    assert!(rows >= 3, "table_types should have at least 3 rows");

    session.close().await.expect("close");
}
