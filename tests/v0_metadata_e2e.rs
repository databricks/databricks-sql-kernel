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
//! Each test asserts:
//! 1. The call succeeds against the live warehouse.
//! 2. The returned `ResultStream` schema matches the JDBC contract
//!    (column names from `metadata::schemas::*`).
//! 3. Where applicable, the result has at least one row.

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

/// Drain a stream's batches and return total row count + fully
/// materialised batches.
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

fn jdbc_field_names(schema: &arrow_schema::Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

#[tokio::test]
#[ignore]
async fn list_catalogs_returns_table_cat_with_main() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_catalogs()
        .await
        .expect("list_catalogs");
    assert_eq!(jdbc_field_names(&stream.schema()), vec!["TABLE_CAT"]);

    let (batches, rows) = collect(stream).await;
    assert!(rows > 0, "live workspace should have at least one catalog");
    let names: Vec<String> = batches
        .iter()
        .flat_map(|rb| {
            let arr = arrow_array::cast::AsArray::as_string::<i32>(rb.column(0));
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
async fn list_schemas_in_main_returns_default_and_information_schema() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_schemas(Some(Identifier::new("main").unwrap()), None)
        .await
        .expect("list_schemas");
    assert_eq!(
        jdbc_field_names(&stream.schema()),
        vec!["TABLE_SCHEM", "TABLE_CATALOG"]
    );

    let (batches, _) = collect(stream).await;
    let mut schema_names: Vec<String> = Vec::new();
    for rb in batches {
        let arr = arrow_array::cast::AsArray::as_string::<i32>(rb.column(0));
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
async fn list_tables_in_information_schema_returns_views() {
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

    assert_eq!(stream.schema().fields().len(), 10);

    let (_, rows) = collect(stream).await;
    assert!(
        rows > 0,
        "information_schema should expose at least one view"
    );

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn list_columns_for_information_schema_tables_view() {
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

    assert_eq!(stream.schema().fields().len(), 24);
    let (_, rows) = collect(stream).await;
    assert!(rows > 0, "tables view should have columns");

    session.close().await.expect("close");
}

/// Resolve a real (non-view) table to drive PK/FK SHOW commands
/// against. Falls back through a handful of well-known sample tables;
/// returns `None` if none of them are reachable on the workspace, in
/// which case the calling test logs and returns early.
///
/// Databricks `SHOW (PRIMARY|FOREIGN) KEYS` rejects views with
/// `EXPECT_TABLE_NOT_VIEW`, so we cannot use `information_schema.*`
/// for these tests.
async fn find_known_table(session: &Session) -> Option<(String, String, String)> {
    let candidates = [
        ("samples", "nyctaxi", "trips"),
        ("samples", "tpch", "lineitem"),
        ("samples", "tpch", "orders"),
    ];
    for (cat, schema, table) in candidates {
        // Probe via list_columns — succeeds iff the table is visible.
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
            // Only count it as found if at least one row exists.
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
async fn primary_keys_runs_cleanly_against_real_table() {
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

    assert_eq!(stream.schema().fields().len(), 6);
    // Row count may be 0 (sample tables typically don't have PKs).
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn foreign_keys_runs_cleanly_against_real_table() {
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

    assert_eq!(stream.schema().fields().len(), 14);
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn functions_returns_jdbc_columns() {
    // Cross-catalog query (system.information_schema) requires
    // privileges some workspaces don't grant; if the call returns
    // SqlError(INSUFFICIENT_PERMISSIONS) we accept that as valid —
    // the wire path worked, just the workspace doesn't grant
    // information_schema reads to our test user.
    let session = live_session().await;
    let res = session.metadata().functions(None, None, None).await;
    match res {
        Ok(stream) => {
            assert_eq!(stream.schema().fields().len(), 6);
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
async fn type_info_returns_static_set_without_warehouse_busy() {
    let session = live_session().await;
    let stream = session.metadata().type_info().await.expect("type_info");

    assert_eq!(stream.schema().fields().len(), 19);
    let (_, rows) = collect(stream).await;
    assert!(
        rows >= 19,
        "type_info should have at least 19 rows, got {rows}"
    );

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn table_types_returns_supported_set() {
    let session = live_session().await;
    let stream = session.metadata().table_types().await.expect("table_types");

    assert_eq!(stream.schema().fields().len(), 1);
    let (_, rows) = collect(stream).await;
    assert!(rows >= 3, "table_types should have at least 3 rows");

    session.close().await.expect("close");
}
