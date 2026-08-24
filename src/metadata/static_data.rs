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

//! In-memory data sources for static [`Metadata`](crate::Metadata)
//! methods — `type_info` and `table_types`.
//!
//! Neither method has a Statement Execution API response to reshape, so the
//! kernel synthesises a `RecordBatch` locally. `type_info` reproduces the
//! established Thrift/HiveServer2 response's 18-column schema, 20-row type
//! corpus, and every cell value. This lets a driver switch from Thrift to the
//! kernel without changing its public `getTypeInfo()` result.

use crate::metadata::schemas::{TABLE_TYPES_SCHEMA, TYPE_INFO_SCHEMA};
use crate::reader::{InMemoryReader, ResultReader};
use arrow_array::{ArrayRef, BooleanArray, Int16Array, Int32Array, RecordBatch, StringArray};
use std::sync::Arc;

use crate::metadata::type_mapping as tm;

// `java.sql.Types.NULL`. The general Databricks column mapper intentionally
// maps VOID query columns to VARCHAR, but Thrift's getTypeInfo catalogue uses
// the Type enum's NULL code for its VOID row.
const JDBC_NULL: i32 = 0;
const JDBC_TINYINT: i32 = tm::JDBC_TINYINT as i32;
const JDBC_SMALLINT: i32 = tm::JDBC_SMALLINT as i32;
const JDBC_INTEGER: i32 = tm::JDBC_INTEGER as i32;
const JDBC_BIGINT: i32 = tm::JDBC_BIGINT as i32;
const JDBC_FLOAT: i32 = tm::JDBC_FLOAT as i32;
const JDBC_DOUBLE: i32 = tm::JDBC_DOUBLE as i32;
const JDBC_DECIMAL: i32 = tm::JDBC_DECIMAL as i32;
const JDBC_VARCHAR: i32 = tm::JDBC_VARCHAR as i32;
const JDBC_CHAR: i32 = tm::JDBC_CHAR as i32;
const JDBC_BINARY: i32 = tm::JDBC_BINARY as i32;
const JDBC_DATE: i32 = tm::JDBC_DATE as i32;
const JDBC_TIMESTAMP: i32 = tm::JDBC_TIMESTAMP as i32;
const JDBC_OTHER: i32 = tm::JDBC_OTHER as i32;
const JDBC_JAVA_OBJECT: i32 = tm::JDBC_JAVA_OBJECT as i32;
const JDBC_STRUCT: i32 = tm::JDBC_STRUCT as i32;
const JDBC_ARRAY: i32 = tm::JDBC_ARRAY as i32;
const JDBC_BOOLEAN: i32 = tm::JDBC_BOOLEAN as i32;

const TYPE_NULLABLE: i16 = 1;
const TYPE_PRED_NONE: i16 = 0;
const TYPE_SEARCHABLE: i16 = 3;

/// The values that vary across rows in Hive's `Type` enum. All other
/// `getTypeInfo` cells are derived from these properties in
/// [`type_info_reader`].
struct TypeInfoRow {
    type_name: &'static str,
    data_type: i32,
    precision: Option<i32>,
    numeric: bool,
    case_sensitive: bool,
    searchable: i16,
}

/// Exact order used by `SparkGetTypeInfoUtil.supportedType`.
fn type_info_rows() -> [TypeInfoRow; 20] {
    use TypeInfoRow as R;

    [
        R {
            type_name: "VOID",
            data_type: JDBC_NULL,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "BOOLEAN",
            data_type: JDBC_BOOLEAN,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "STRING",
            data_type: JDBC_VARCHAR,
            precision: None,
            numeric: false,
            case_sensitive: true,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "BINARY",
            data_type: JDBC_BINARY,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "TINYINT",
            data_type: JDBC_TINYINT,
            precision: Some(3),
            numeric: true,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "SMALLINT",
            data_type: JDBC_SMALLINT,
            precision: Some(5),
            numeric: true,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "INT",
            data_type: JDBC_INTEGER,
            precision: Some(10),
            numeric: true,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "BIGINT",
            data_type: JDBC_BIGINT,
            precision: Some(19),
            numeric: true,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "FLOAT",
            data_type: JDBC_FLOAT,
            precision: Some(7),
            numeric: true,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "DOUBLE",
            data_type: JDBC_DOUBLE,
            precision: Some(15),
            numeric: true,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "DECIMAL",
            data_type: JDBC_DECIMAL,
            precision: Some(38),
            numeric: true,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "DATE",
            data_type: JDBC_DATE,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "TIMESTAMP",
            data_type: JDBC_TIMESTAMP,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "ARRAY",
            data_type: JDBC_ARRAY,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_PRED_NONE,
        },
        R {
            type_name: "MAP",
            data_type: JDBC_JAVA_OBJECT,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_PRED_NONE,
        },
        R {
            type_name: "STRUCT",
            data_type: JDBC_STRUCT,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_PRED_NONE,
        },
        R {
            type_name: "CHAR",
            data_type: JDBC_CHAR,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "VARCHAR",
            data_type: JDBC_VARCHAR,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "INTERVAL_YEAR_MONTH",
            data_type: JDBC_OTHER,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
        R {
            type_name: "INTERVAL_DAY_TIME",
            data_type: JDBC_OTHER,
            precision: None,
            numeric: false,
            case_sensitive: false,
            searchable: TYPE_SEARCHABLE,
        },
    ]
}

/// Build the exact Thrift `getTypeInfo` batch and return a reader yielding it
/// once.
pub(crate) fn type_info_reader() -> Box<dyn ResultReader + Send> {
    let rows = type_info_rows();
    let schema = TYPE_INFO_SCHEMA.clone();

    let type_name: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|row| row.type_name),
    ));
    let data_type: ArrayRef = Arc::new(Int32Array::from_iter_values(
        rows.iter().map(|row| row.data_type),
    ));
    let precision: ArrayRef = Arc::new(Int32Array::from_iter(rows.iter().map(|row| row.precision)));
    let literal_prefix: ArrayRef =
        Arc::new(StringArray::from_iter(rows.iter().map(|_| None::<&str>)));
    let literal_suffix: ArrayRef =
        Arc::new(StringArray::from_iter(rows.iter().map(|_| None::<&str>)));
    let create_params: ArrayRef =
        Arc::new(StringArray::from_iter(rows.iter().map(|_| None::<&str>)));
    let nullable: ArrayRef = Arc::new(Int16Array::from_iter_values(
        rows.iter().map(|_| TYPE_NULLABLE),
    ));
    let case_sensitive: ArrayRef = Arc::new(BooleanArray::from_iter(
        rows.iter().map(|row| Some(row.case_sensitive)),
    ));
    let searchable: ArrayRef = Arc::new(Int16Array::from_iter_values(
        rows.iter().map(|row| row.searchable),
    ));
    let unsigned_attribute: ArrayRef = Arc::new(BooleanArray::from_iter(
        rows.iter().map(|row| Some(!row.numeric)),
    ));
    let fixed_prec_scale: ArrayRef =
        Arc::new(BooleanArray::from_iter(rows.iter().map(|_| Some(false))));
    let auto_increment: ArrayRef =
        Arc::new(BooleanArray::from_iter(rows.iter().map(|_| Some(false))));
    let local_type_name: ArrayRef =
        Arc::new(StringArray::from_iter(rows.iter().map(|_| None::<&str>)));
    let minimum_scale: ArrayRef = Arc::new(Int16Array::from_iter_values(rows.iter().map(|_| 0)));
    let maximum_scale: ArrayRef = Arc::new(Int16Array::from_iter_values(rows.iter().map(|_| 0)));
    let sql_data_type: ArrayRef = Arc::new(Int32Array::from_iter(rows.iter().map(|_| None::<i32>)));
    let sql_datetime_sub: ArrayRef =
        Arc::new(Int32Array::from_iter(rows.iter().map(|_| None::<i32>)));
    let num_prec_radix: ArrayRef = Arc::new(Int32Array::from_iter(
        rows.iter().map(|row| row.numeric.then_some(10)),
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            type_name,
            data_type,
            precision,
            literal_prefix,
            literal_suffix,
            create_params,
            nullable,
            case_sensitive,
            searchable,
            unsigned_attribute,
            fixed_prec_scale,
            auto_increment,
            local_type_name,
            minimum_scale,
            maximum_scale,
            sql_data_type,
            sql_datetime_sub,
            num_prec_radix,
        ],
    )
    .expect("type_info batch construction must match its static schema");

    Box::new(InMemoryReader::new(schema, vec![batch]))
}

/// Build the `table_types` `RecordBatch` from a list of supported types
/// sourced from the session transport.
pub(crate) fn table_types_reader(types: &[String]) -> Box<dyn ResultReader + Send> {
    let schema = TABLE_TYPES_SCHEMA.clone();
    let array: ArrayRef = Arc::new(StringArray::from_iter_values(types.iter()));
    let batch = RecordBatch::try_new(schema.clone(), vec![array])
        .expect("table_types batch construction must succeed");
    Box::new(InMemoryReader::new(schema, vec![batch]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[tokio::test]
    async fn type_info_reader_matches_thrift_rows() {
        let mut reader = type_info_reader();
        let batch = reader.next_batch().await.unwrap().expect("one batch");

        assert_eq!(batch.schema(), *TYPE_INFO_SCHEMA);
        assert_eq!(batch.num_columns(), 18);
        assert_eq!(batch.num_rows(), 20);
        assert!(reader.next_batch().await.unwrap().is_none());

        let names = batch
            .column_by_name("TYPE_NAME")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let actual_names: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert_eq!(
            actual_names,
            vec![
                "VOID",
                "BOOLEAN",
                "STRING",
                "BINARY",
                "TINYINT",
                "SMALLINT",
                "INT",
                "BIGINT",
                "FLOAT",
                "DOUBLE",
                "DECIMAL",
                "DATE",
                "TIMESTAMP",
                "ARRAY",
                "MAP",
                "STRUCT",
                "CHAR",
                "VARCHAR",
                "INTERVAL_YEAR_MONTH",
                "INTERVAL_DAY_TIME",
            ]
        );

        let data_types = batch
            .column_by_name("DATA_TYPE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual_data_types: Vec<i32> =
            (0..data_types.len()).map(|i| data_types.value(i)).collect();
        assert_eq!(
            actual_data_types,
            vec![
                0, 16, 12, -2, -6, 5, 4, -5, 6, 8, 3, 91, 93, 2003, 2000, 2002, 1, 12, 1111, 1111,
            ]
        );

        let precision = batch
            .column_by_name("PRECISION")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual_precision: Vec<Option<i32>> = (0..precision.len())
            .map(|i| {
                if precision.is_null(i) {
                    None
                } else {
                    Some(precision.value(i))
                }
            })
            .collect();
        assert_eq!(
            actual_precision,
            vec![
                None,
                None,
                None,
                None,
                Some(3),
                Some(5),
                Some(10),
                Some(19),
                Some(7),
                Some(15),
                Some(38),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ]
        );

        for column_name in [
            "LITERAL_PREFIX",
            "LITERAL_SUFFIX",
            "CREATE_PARAMS",
            "LOCAL_TYPE_NAME",
        ] {
            let column = batch
                .column_by_name(column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(column.null_count(), 20, "{column_name}");
        }
        for column_name in ["SQL_DATA_TYPE", "SQL_DATETIME_SUB"] {
            let column = batch
                .column_by_name(column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(column.null_count(), 20, "{column_name}");
        }
        for column_name in [
            "TYPE_NAME",
            "DATA_TYPE",
            "NULLABLE",
            "CASE_SENSITIVE",
            "SEARCHABLE",
            "UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE",
            "AUTO_INCREMENT",
            "MINIMUM_SCALE",
            "MAXIMUM_SCALE",
        ] {
            assert_eq!(
                batch.column_by_name(column_name).unwrap().null_count(),
                0,
                "{column_name}"
            );
        }

        let nullable = batch
            .column_by_name("NULLABLE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        let case_sensitive = batch
            .column_by_name("CASE_SENSITIVE")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let searchable = batch
            .column_by_name("SEARCHABLE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        let unsigned_attribute = batch
            .column_by_name("UNSIGNED_ATTRIBUTE")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let fixed_prec_scale = batch
            .column_by_name("FIXED_PREC_SCALE")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let auto_increment = batch
            .column_by_name("AUTO_INCREMENT")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let minimum_scale = batch
            .column_by_name("MINIMUM_SCALE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        let maximum_scale = batch
            .column_by_name("MAXIMUM_SCALE")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        let num_prec_radix = batch
            .column_by_name("NUM_PREC_RADIX")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let numeric = (4..=10).contains(&i);
            let complex = (13..=15).contains(&i);

            assert_eq!(nullable.value(i), 1, "NULLABLE row {i}");
            assert_eq!(case_sensitive.value(i), i == 2, "CASE_SENSITIVE row {i}");
            assert_eq!(
                searchable.value(i),
                if complex { 0 } else { 3 },
                "SEARCHABLE row {i}"
            );
            assert_eq!(
                unsigned_attribute.value(i),
                !numeric,
                "UNSIGNED_ATTRIBUTE row {i}"
            );
            assert!(!fixed_prec_scale.value(i), "FIXED_PREC_SCALE row {i}");
            assert!(!auto_increment.value(i), "AUTO_INCREMENT row {i}");
            assert_eq!(minimum_scale.value(i), 0, "MINIMUM_SCALE row {i}");
            assert_eq!(maximum_scale.value(i), 0, "MAXIMUM_SCALE row {i}");
            if numeric {
                assert!(!num_prec_radix.is_null(i), "NUM_PREC_RADIX row {i}");
                assert_eq!(num_prec_radix.value(i), 10, "NUM_PREC_RADIX row {i}");
            } else {
                assert!(num_prec_radix.is_null(i), "NUM_PREC_RADIX row {i}");
            }
        }
    }

    #[test]
    fn type_info_void_code_is_distinct_from_column_mapping() {
        use crate::metadata::type_mapping::databricks_type_to_xdbc;

        assert_eq!(type_info_rows()[0].data_type, JDBC_NULL);
        assert_eq!(databricks_type_to_xdbc("VOID") as i32, JDBC_VARCHAR);
    }

    #[tokio::test]
    async fn table_types_reader_yields_provided_types() {
        let types = vec!["TABLE".to_string(), "VIEW".to_string()];
        let mut reader = table_types_reader(&types);
        let batch = reader.next_batch().await.unwrap().expect("one batch");
        assert_eq!(batch.num_rows(), 2);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "TABLE");
        assert_eq!(values.value(1), "VIEW");
        assert!(reader.next_batch().await.unwrap().is_none());
    }
}
