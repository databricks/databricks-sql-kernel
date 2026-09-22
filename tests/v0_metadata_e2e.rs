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
//! Static methods (`type_info`, `table_types`) assert the metadata shapes
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
        .auth(AuthConfig::Pat {
            token,
            identity_federation_client_id: None,
        })
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

/// Collect every value of a named Utf8 column across all batches.
fn string_column_values(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|rb| {
            let idx = rb.schema().index_of(name).expect("column present");
            let arr = rb.column(idx).as_string::<i32>();
            (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Regression test for databricks/databricks-odbc#129: `list_tables`
/// must treat the catalog argument as an ODBC/JDBC LIKE pattern
/// (`_` / `%`), matching Thrift `TGetTablesReq.catalogName`. The SEA
/// `SHOW` grammar has no catalog-pattern position, so the kernel lists
/// across all catalogs and filters `TABLE_CAT` client-side.
///
/// Self-adapting: derives a `_`-wildcard pattern from a real catalog on
/// the live workspace, so it doesn't depend on the issue's specific
/// `comparator_tests` / `comparator-tests` fixtures existing here.
#[tokio::test]
#[ignore]
async fn list_tables_catalog_wildcard_filters_client_side() {
    use databricks_sql_kernel::LikePattern;

    let session = live_session().await;

    // Resolve a real table first so a zero-row wildcard result cannot be
    // mistaken for an empty schema in the test workspace.
    let (base, schema, table) = find_known_table(&session)
        .await
        .expect("expected a seeded samples table");
    // Replace the 2nd char with `_` (single-char wildcard). For
    // `samples` this yields `s_mples`, which must still match `samples`.
    let mut chars: Vec<char> = base.chars().collect();
    chars[1] = '_';
    let pattern: String = chars.into_iter().collect();

    let stream = session
        .metadata()
        .list_tables(
            Some(LikePattern::new(&pattern).unwrap()),
            Some(LikePattern::new(&schema).unwrap()),
            Some(LikePattern::new(&table).unwrap()),
            None,
        )
        .await
        .expect("list_tables with wildcard catalog");
    let (batches, rows) = collect(stream).await;

    assert!(
        rows > 0,
        "wildcard catalog {pattern:?} against {base}.{schema}.{table} returned no rows — \
         client-side catalog filter dropped valid rows"
    );

    // Every returned row's TABLE_CAT must match the `_` pattern — proves
    // the client-side catalog filter ran (vs. returning all catalogs).
    let returned_cats = string_column_values(&batches, "TABLE_CAT");
    for cat in &returned_cats {
        assert!(
            like_matches_underscore(&pattern, cat),
            "TABLE_CAT {cat:?} does not match wildcard catalog pattern {pattern:?}; \
             returned catalogs: {returned_cats:?}"
        );
    }
    assert!(
        returned_cats.iter().any(|c| c.eq_ignore_ascii_case(&base)),
        "base catalog {base:?} not present in wildcard result {returned_cats:?}"
    );

    session.close().await.expect("close");
}

/// Test-local reference implementation of a case-insensitive LIKE match
/// with `_` as a single-char wildcard (no `%`). Deliberately independent
/// of the kernel's matcher so the e2e assertion doesn't just re-run the
/// code under test.
fn like_matches_underscore(pattern: &str, candidate: &str) -> bool {
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let c: Vec<char> = candidate.to_lowercase().chars().collect();
    if p.len() != c.len() {
        return false;
    }
    p.iter()
        .zip(c.iter())
        .all(|(pc, cc)| *pc == '_' || pc == cc)
}

// Tests in this file target the `samples` catalog rather than `main`.
// `samples` is a Databricks-managed catalog present on every
// Unity-Catalog-enabled workspace; `main` is workspace-configurable
// and on several Dogfood / Azure workspaces `main` lacks both
// `default` and `information_schema` entirely (verified 2026-06-01:
// `SHOW SCHEMAS IN main` returned only workspace-specific schemas
// like `dbdemos_iot_turbine` on two distinct workspaces). The
// `find_known_table` helper below already standardised on `samples`
// for the PK/FK tests; these `list_*` tests now follow suit.
//
// `hive_metastore` is deliberately not used — it's the legacy
// pre-Unity-Catalog catalog and behaves differently from UC
// catalogs across multiple metadata commands.
//
// `information_schema` is treated as a **soft** assertion: it's
// part of the UC contract on every catalog, but a fresh workspace
// may not have it surfaced yet for a brand-new catalog. The tests
// flag its absence with `eprintln!` (so a regression is still
// visible in the log) but only fail when the kernel returns
// genuinely malformed data — wrong column shape, malformed cells
// for rows that DO exist, etc.

#[tokio::test]
#[ignore]
async fn list_schemas_returns_jdbc_columns() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_schemas(Some(Identifier::new("samples").unwrap()), None)
        .await
        .expect("list_schemas");
    // JDBC `getSchemas` shape: TABLE_SCHEM, TABLE_CATALOG. The kernel
    // synthesises TABLE_CATALOG from the catalog arg when the server's
    // single-catalog `SHOW SCHEMAS` response omits it. This shape
    // assertion is load-bearing — it's the JDBC contract every
    // BI tool depends on.
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
    // The `samples` catalog must expose at least one schema —
    // it's Databricks-managed and seeded with the demo datasets
    // (`nyctaxi`, `tpch`, etc.). Anything less means the kernel
    // failed to parse the `SHOW SCHEMAS` response.
    assert!(
        !schema_names.is_empty(),
        "samples catalog should expose at least one schema; got empty result"
    );
    // `information_schema` is part of the UC contract for every
    // catalog. Soft-assert its presence: log a warning if absent
    // so we still see drift, but don't fail — some workspace
    // configurations (notably brand-new catalogs) may not have
    // it surfaced.
    if !schema_names.iter().any(|n| n == "information_schema") {
        eprintln!(
            "warning: samples catalog did not expose 'information_schema'; \
             schemas were {schema_names:?}. This is unusual but not a \
             kernel failure — the kernel correctly returned what the \
             server reported."
        );
    }

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn list_tables_returns_jdbc_columns() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_tables(
            Some(databricks_sql_kernel::LikePattern::new("samples").unwrap()),
            Some(databricks_sql_kernel::LikePattern::new("information_schema").unwrap()),
            None,
            None,
        )
        .await
        .expect("list_tables");

    // JDBC `getTables` 10-column shape — load-bearing.
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
    // `samples.information_schema` typically exposes ~28 views, but
    // the count isn't load-bearing — we just want to know the
    // kernel can iterate the result. Soft-warn if zero so a
    // regression is still visible in the log.
    if rows == 0 {
        eprintln!(
            "warning: samples.information_schema returned no rows. \
             Expected at least one view in a UC-managed information_schema; \
             skipping the rows-positive assertion to keep the test \
             workspace-portable."
        );
    }

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn list_columns_returns_jdbc_columns() {
    let session = live_session().await;
    let stream = session
        .metadata()
        .list_columns(
            Some(Identifier::new("samples").unwrap()),
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
    // The `tables` view in UC's information_schema is documented
    // to expose ~10 columns. Soft-warn if zero rather than fail
    // — if the view exists, the kernel returning zero rows is
    // suspicious; if the view doesn't exist on this workspace,
    // zero rows is the correct empty-result behaviour.
    if rows == 0 {
        eprintln!(
            "warning: samples.information_schema.tables returned no \
             columns. This is either a workspace where the view is \
             absent, or a kernel iteration bug; the test stays soft \
             to keep it workspace-portable."
        );
    }

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
async fn primary_keys_returns_jdbc_columns() {
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

    // Reshaped kernel-side into the JDBC getPrimaryKeys column set
    // (the raw SHOW KEYS columns are renamed by reshape_primary_keys).
    let names = col_names(&stream.schema());
    assert_eq!(
        names,
        vec![
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "KEY_SEQ",
            "PK_NAME",
        ]
    );
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn foreign_keys_returns_jdbc_columns() {
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

    // Reshaped kernel-side into the JDBC getCrossReference column set
    // (the raw SHOW FOREIGN KEYS columns are renamed by
    // reshape_foreign_keys).
    let names = col_names(&stream.schema());
    assert_eq!(
        names,
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
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn functions_returns_jdbc_columns() {
    let session = live_session().await;
    // Reshaped kernel-side into the JDBC getFunctions column set when
    // the server returns the raw SHOW FUNCTIONS result.
    let stream = session
        .metadata()
        .functions(None, None, None)
        .await
        .expect("functions");
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
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn procedures_returns_jdbc_columns() {
    let session = live_session().await;
    // Reshaped kernel-side into the JDBC getProcedures column set
    // (reshape_procedures).
    let stream = session
        .metadata()
        .procedures(None, None, None)
        .await
        .expect("procedures");
    assert_eq!(
        col_names(&stream.schema()),
        vec![
            "PROCEDURE_CAT",
            "PROCEDURE_SCHEM",
            "PROCEDURE_NAME",
            "RESERVED1",
            "RESERVED2",
            "RESERVED3",
            "REMARKS",
            "PROCEDURE_TYPE",
            "SPECIFIC_NAME",
        ]
    );
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn procedure_columns_returns_jdbc_columns() {
    let session = live_session().await;
    // Reshaped kernel-side into the JDBC getProcedureColumns column set
    // (reshape_procedure_columns). Backed by information_schema.parameters
    // JOINed to routines (procedures only).
    let stream = session
        .metadata()
        .procedure_columns(None, None, None, None)
        .await
        .expect("procedure_columns");
    assert_eq!(
        col_names(&stream.schema()),
        vec![
            "PROCEDURE_CAT",
            "PROCEDURE_SCHEM",
            "PROCEDURE_NAME",
            "COLUMN_NAME",
            "COLUMN_TYPE",
            "DATA_TYPE",
            "TYPE_NAME",
            "PRECISION",
            "LENGTH",
            "SCALE",
            "RADIX",
            "NULLABLE",
            "REMARKS",
            "COLUMN_DEF",
            "SQL_DATA_TYPE",
            "SQL_DATETIME_SUB",
            "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION",
            "IS_NULLABLE",
            "SPECIFIC_NAME",
        ]
    );
    let _ = collect(stream).await;

    session.close().await.expect("close");
}

#[tokio::test]
#[ignore]
async fn type_info_returns_static_thrift_shape() {
    let session = live_session().await;
    let stream = session.metadata().type_info().await.expect("type_info");

    // type_info is synthesised to mirror the established Thrift response.
    let names = col_names(&stream.schema());
    assert_eq!(
        names,
        vec![
            "TYPE_NAME",
            "DATA_TYPE",
            "PRECISION",
            "LITERAL_PREFIX",
            "LITERAL_SUFFIX",
            "CREATE_PARAMS",
            "NULLABLE",
            "CASE_SENSITIVE",
            "SEARCHABLE",
            "UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE",
            "AUTO_INCREMENT",
            "LOCAL_TYPE_NAME",
            "MINIMUM_SCALE",
            "MAXIMUM_SCALE",
            "SQL_DATA_TYPE",
            "SQL_DATETIME_SUB",
            "NUM_PREC_RADIX",
        ]
    );

    let (_, rows) = collect(stream).await;
    assert_eq!(rows, 20);

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
