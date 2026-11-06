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

use std::{env, fs};

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Optionable as _, Statement as _};
use arrow_array::{Array as _, Int8Array, ListArray, StringArray};
use databricks_adbc::{Connection, Driver};

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set for ignored live tests"))
}

fn live_connection() -> Connection {
    live_connection_with_options([])
}

fn live_connection_with_options(
    options: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
) -> Connection {
    let host = required_env("DATABRICKS_HOST");
    let http_path = required_env("DATABRICKS_HTTP_PATH");
    let token_file = required_env("DATABRICKS_TOKEN_FILE");
    let token = fs::read_to_string(token_file)
        .expect("DATABRICKS_TOKEN_FILE must be readable")
        .trim()
        .to_owned();
    assert!(!token.is_empty(), "DATABRICKS_TOKEN_FILE must not be empty");

    let mut driver = Driver;
    let database = driver
        .new_database_with_opts(
            [
                (OptionDatabase::Uri, OptionValue::String(host)),
                (
                    OptionDatabase::Other("adbc.databricks.http_path".to_owned()),
                    OptionValue::String(http_path),
                ),
                (
                    OptionDatabase::Other("adbc.databricks.auth.token".to_owned()),
                    OptionValue::String(token),
                ),
            ]
            .into_iter()
            .chain(options),
        )
        .expect("database configuration should be valid");
    database
        .new_connection()
        .expect("live connection should open")
}

#[test]
#[ignore = "requires an explicitly configured live Databricks warehouse"]
fn database_session_aliases_and_complex_types_apply_end_to_end() {
    let mut connection = live_connection_with_options([
        (
            OptionDatabase::Other("adbc.databricks.enable_complex_datatype_support".into()),
            OptionValue::String("true".into()),
        ),
        (
            OptionDatabase::Other("adbc.databricks.ssp_ansi_mode".into()),
            OptionValue::String("false".into()),
        ),
        (
            OptionDatabase::Other("adbc.databricks.ssp_timezone".into()),
            OptionValue::String("America/Los_Angeles".into()),
        ),
    ]);
    let mut statement = connection.new_statement().expect("create statement");
    statement
        .set_sql_query(
            "SELECT current_timezone() AS timezone, CAST(128 AS TINYINT) AS wrapped, array(1, 2) AS nested",
        )
        .expect("set SQL");

    let batches = statement
        .execute()
        .expect("execute option validation query")
        .collect::<Result<Vec<_>, _>>()
        .expect("fetch option validation result");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    let timezone = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("timezone must be UTF-8");
    assert_eq!(timezone.value(0), "America/Los_Angeles");
    let wrapped = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int8Array>()
        .expect("non-ANSI TINYINT must retain its numeric Arrow type");
    assert_eq!(wrapped.value(0), -128);
    let nested = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("enabled complex datatype support must return a native Arrow list");
    assert_eq!(nested.value_length(0), 2);
}

#[test]
#[ignore = "requires an explicitly configured live Databricks warehouse"]
fn select_one_fetches_the_complete_result() {
    let mut connection = live_connection();
    let mut statement = connection
        .new_statement()
        .expect("statement should be created");
    statement
        .set_sql_query("SELECT 1 AS value")
        .expect("query should be accepted");

    let reader = statement.execute().expect("query should execute");
    let mut batches = 0;
    let mut rows = 0;
    for batch in reader {
        let batch = batch.expect("every result batch should decode");
        assert_eq!(batch.num_columns(), 1);
        batches += 1;
        rows += batch.num_rows();
    }
    assert!(batches >= 1, "SELECT 1 should return a batch");
    assert_eq!(rows, 1, "SELECT 1 should return exactly one row");
}

#[test]
#[ignore = "requires an explicitly configured live Databricks warehouse"]
fn namespace_getters_reflect_direct_use_statements() {
    let catalog = env::var("DATABRICKS_TEST_CATALOG").unwrap_or_else(|_| "samples".to_owned());
    let schema = env::var("DATABRICKS_TEST_SCHEMA").unwrap_or_else(|_| "tpch".to_owned());
    let mut connection = live_connection();
    let mut statement = connection.new_statement().expect("create statement");
    statement
        .set_sql_query(format!("USE CATALOG `{}`", catalog.replace('`', "``")))
        .expect("set catalog SQL");
    statement.execute_update().expect("change catalog");
    statement
        .set_sql_query(format!("USE SCHEMA `{}`", schema.replace('`', "``")))
        .expect("set schema SQL");
    statement.execute_update().expect("change schema");
    drop(statement);

    assert_eq!(
        connection
            .get_option_string(OptionConnection::CurrentCatalog)
            .expect("read current catalog"),
        catalog
    );
    assert_eq!(
        connection
            .get_option_string(OptionConnection::CurrentSchema)
            .expect("read current schema"),
        schema
    );
}

#[test]
#[ignore = "requires an explicitly configured live Databricks warehouse"]
fn direct_results_option_round_trips_and_fetches_synchronously() {
    let mut connection = live_connection();
    let mut statement = connection.new_statement().expect("create statement");
    let option = OptionStatement::Other("adbc.databricks.enable_direct_results".into());
    assert_eq!(
        statement
            .get_option_string(option.clone())
            .expect("read default option"),
        "true"
    );
    statement
        .set_option(option.clone(), OptionValue::String("false".into()))
        .expect("disable direct results");
    assert_eq!(
        statement
            .get_option_string(option.clone())
            .expect("read disabled option"),
        "false"
    );
    let invalid = statement
        .set_option(option.clone(), OptionValue::String("invalid".into()))
        .expect_err("reject invalid boolean");
    assert_eq!(invalid.status, adbc_core::error::Status::InvalidArguments);
    assert_eq!(
        statement
            .get_option_string(option)
            .expect("invalid update must preserve the prior value"),
        "false"
    );
    statement.set_sql_query("SELECT 1").expect("set SQL");
    let rows = statement
        .execute()
        .expect("execute through async kernel path")
        .map(|batch| batch.expect("decode result").num_rows())
        .sum::<usize>();
    assert_eq!(rows, 1);

    statement
        .set_sql_query("SELECT * FROM range(10)")
        .expect("set partial-fetch SQL");
    let mut partial = statement.execute().expect("execute partial-fetch query");
    assert!(partial.next().is_some(), "partial result has a first batch");
    drop(partial);
    statement
        .set_sql_query("SELECT 2")
        .expect("reuse statement");
    let reused_rows = statement
        .execute()
        .expect("execute reused statement")
        .map(|batch| batch.expect("decode reused result").num_rows())
        .sum::<usize>();
    assert_eq!(reused_rows, 1);
}

#[test]
#[ignore = "requires an explicitly configured live Databricks warehouse"]
fn metadata_statement_commands_return_canonical_jdbc_shapes() {
    let mut connection = live_connection();
    let metadata_catalog =
        env::var("DATABRICKS_TEST_CATALOG").unwrap_or_else(|_| "samples".to_owned());
    let cases = [
        ("GetCatalogs", vec!["TABLE_CAT"]),
        ("GetSchemas", vec!["TABLE_SCHEM", "TABLE_CATALOG"]),
        (
            "GetTables",
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
            ],
        ),
        (
            "GetColumns",
            vec![
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "TYPE_NAME",
                "COLUMN_SIZE",
                "BUFFER_LENGTH",
                "DECIMAL_DIGITS",
                "NUM_PREC_RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SCOPE_CATALOG",
                "SCOPE_SCHEMA",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTO_INCREMENT",
                "BASE_TYPE_NAME",
            ],
        ),
    ];

    for (command, expected_fields) in cases {
        let mut statement = connection.new_statement().expect("create statement");
        statement
            .set_option(
                OptionStatement::Other("adbc.apache.statement.is_metadata_command".into()),
                OptionValue::String("true".into()),
            )
            .expect("enable metadata command");
        if command != "GetCatalogs" {
            statement
                .set_option(
                    OptionStatement::Other("adbc.get_metadata.target_catalog".into()),
                    OptionValue::String(metadata_catalog.clone()),
                )
                .expect("set metadata catalog");
        }
        statement.set_sql_query(command).expect("set command");
        let reader = statement.execute().expect("execute metadata command");
        assert_eq!(
            reader
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            expected_fields,
            "{command} schema"
        );
    }
}

#[test]
#[ignore = "requires an explicitly configured live Databricks warehouse"]
fn metadata_statement_catalog_underscore_escaping_is_configurable() {
    let catalog = required_env("DATABRICKS_TEST_CATALOG");
    assert!(
        catalog.contains('_'),
        "DATABRICKS_TEST_CATALOG must contain an underscore"
    );

    for (escape_catalog_name, expected_catalogs) in [
        (true, vec![catalog.as_str()]),
        (false, vec!["comparator-tests", catalog.as_str()]),
    ] {
        let mut connection = live_connection_with_options([(
            OptionDatabase::Other("adbc.databricks.metadata.escape_catalog_name".into()),
            OptionValue::String(escape_catalog_name.to_string()),
        )]);
        let mut statement = connection.new_statement().expect("create statement");
        statement
            .set_option(
                OptionStatement::Other("adbc.apache.statement.is_metadata_command".into()),
                OptionValue::String("true".into()),
            )
            .expect("enable metadata command");
        statement
            .set_option(
                OptionStatement::Other("adbc.get_metadata.target_catalog".into()),
                OptionValue::String(catalog.clone()),
            )
            .expect("set metadata catalog");
        statement.set_sql_query("GetTables").expect("set command");

        let batches = statement
            .execute()
            .expect("execute metadata command")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch metadata result");
        let mut actual_catalogs = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("TABLE_CAT must be UTF-8")
                    .iter()
                    .flatten()
                    .filter(|value| !value.is_empty())
            })
            .collect::<Vec<_>>();
        actual_catalogs.sort_unstable();
        actual_catalogs.dedup();
        assert_eq!(actual_catalogs, expected_catalogs);
    }
}
