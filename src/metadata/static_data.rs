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
//! Both methods return a fixed answer that doesn't change between
//! calls or between workspaces, so the kernel synthesises a
//! `RecordBatch` once and serves it through an
//! [`InMemoryReader`](crate::reader::InMemoryReader). No wire call.
//!
//! `type_info` rows are derived from the Databricks SQL type
//! catalogue (matches the JDBC driver's `getTypeInfo()`); `JDBC_TYPE`
//! codes come from [`java.sql.Types`](https://docs.oracle.com/javase/8/docs/api/java/sql/Types.html)
//! and stay aligned with [`crate::metadata::type_mapping::databricks_type_to_xdbc`].

use crate::metadata::schemas::{TABLE_TYPES_SCHEMA, TYPE_INFO_SCHEMA};
use crate::reader::{InMemoryReader, ResultReader};
use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use std::sync::Arc;

// ─── JDBC `Types` integer codes used in TYPE_INFO ───────────────────

const JDBC_BIT: i32 = -7; // BOOLEAN
const JDBC_TINYINT: i32 = -6;
const JDBC_BIGINT: i32 = -5;
const JDBC_LONGVARBINARY: i32 = -4;
const JDBC_VARBINARY: i32 = -3;
const JDBC_BINARY: i32 = -2;
const JDBC_LONGVARCHAR: i32 = -1; // STRING
const JDBC_CHAR: i32 = 1;
const JDBC_NUMERIC: i32 = 2;
const JDBC_DECIMAL: i32 = 3;
const JDBC_INTEGER: i32 = 4;
const JDBC_SMALLINT: i32 = 5;
const JDBC_REAL: i32 = 7;
const JDBC_DOUBLE: i32 = 8;
const JDBC_VARCHAR: i32 = 12;
const JDBC_DATE: i32 = 91;
const JDBC_TIMESTAMP: i32 = 93;
const JDBC_OTHER: i32 = 1111;
const JDBC_JAVA_OBJECT: i32 = 2000;
const JDBC_STRUCT: i32 = 2002;
const JDBC_ARRAY: i32 = 2003;

// JDBC nullability codes for TYPE_INFO.NULLABLE
const TYPE_NULLABLE_UNKNOWN: i32 = 2;

// JDBC searchability for TYPE_INFO.SEARCHABLE
const TYPE_PRED_NONE: i32 = 0;
#[allow(dead_code)]
const TYPE_PRED_CHAR: i32 = 1;
const TYPE_PRED_BASIC: i32 = 2;
const TYPE_SEARCHABLE: i32 = 3;

/// One row of the JDBC `getTypeInfo` result. Authored for readability;
/// converted into Arrow columns by [`type_info_reader`].
struct TypeInfoRow {
    type_name: &'static str,
    data_type: i32,
    column_size: Option<i32>,
    literal_prefix: Option<&'static str>,
    literal_suffix: Option<&'static str>,
    create_params: Option<&'static str>,
    nullable: i32,
    case_sensitive: bool,
    searchable: i32,
    unsigned_attribute: Option<bool>,
    fixed_prec_scale: bool,
    auto_unique_value: Option<bool>,
    local_type_name: Option<&'static str>,
    minimum_scale: Option<i32>,
    maximum_scale: Option<i32>,
    sql_data_type: Option<i32>,
    sql_datetime_sub: Option<i32>,
    num_prec_radix: Option<i32>,
}

/// Static catalogue of Databricks SQL types as JDBC `getTypeInfo`
/// rows.
///
/// Values follow the conventions used by other JDBC drivers:
///
/// - `NULLABLE = 2` (unknown) — Databricks doesn't surface a per-type
///   nullable hint at this layer.
/// - `CASE_SENSITIVE` — true for character types only.
/// - `SEARCHABLE` — `3` for primitives that support `LIKE`,
///   `2` (basic) for non-character types, `0` for unsearchable
///   complex types (`ARRAY`/`MAP`/`STRUCT`).
/// - `UNSIGNED_ATTRIBUTE = false` for numerics (Databricks has no
///   unsigned types), `NULL` for non-numeric.
///
/// `LOCAL_TYPE_NAME` mirrors `TYPE_NAME` — Databricks doesn't have a
/// localised name distinct from the canonical SQL identifier.
fn type_info_rows() -> Vec<TypeInfoRow> {
    use TypeInfoRow as R;
    vec![
        R {
            type_name: "BOOLEAN",
            data_type: JDBC_BIT,
            column_size: Some(1),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("BOOLEAN"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_BIT),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "TINYINT",
            data_type: JDBC_TINYINT,
            column_size: Some(3),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("TINYINT"),
            minimum_scale: Some(0),
            maximum_scale: Some(0),
            sql_data_type: Some(JDBC_TINYINT),
            sql_datetime_sub: None,
            num_prec_radix: Some(10),
        },
        R {
            type_name: "SMALLINT",
            data_type: JDBC_SMALLINT,
            column_size: Some(5),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("SMALLINT"),
            minimum_scale: Some(0),
            maximum_scale: Some(0),
            sql_data_type: Some(JDBC_SMALLINT),
            sql_datetime_sub: None,
            num_prec_radix: Some(10),
        },
        R {
            type_name: "INT",
            data_type: JDBC_INTEGER,
            column_size: Some(10),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("INT"),
            minimum_scale: Some(0),
            maximum_scale: Some(0),
            sql_data_type: Some(JDBC_INTEGER),
            sql_datetime_sub: None,
            num_prec_radix: Some(10),
        },
        R {
            type_name: "BIGINT",
            data_type: JDBC_BIGINT,
            column_size: Some(19),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("BIGINT"),
            minimum_scale: Some(0),
            maximum_scale: Some(0),
            sql_data_type: Some(JDBC_BIGINT),
            sql_datetime_sub: None,
            num_prec_radix: Some(10),
        },
        R {
            type_name: "FLOAT",
            data_type: JDBC_REAL,
            column_size: Some(7),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("FLOAT"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_REAL),
            sql_datetime_sub: None,
            num_prec_radix: Some(2),
        },
        R {
            type_name: "DOUBLE",
            data_type: JDBC_DOUBLE,
            column_size: Some(15),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("DOUBLE"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_DOUBLE),
            sql_datetime_sub: None,
            num_prec_radix: Some(2),
        },
        R {
            type_name: "DECIMAL",
            data_type: JDBC_DECIMAL,
            column_size: Some(38),
            literal_prefix: None,
            literal_suffix: None,
            create_params: Some("precision,scale"),
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("DECIMAL"),
            minimum_scale: Some(0),
            maximum_scale: Some(38),
            sql_data_type: Some(JDBC_DECIMAL),
            sql_datetime_sub: None,
            num_prec_radix: Some(10),
        },
        R {
            type_name: "STRING",
            data_type: JDBC_LONGVARCHAR,
            column_size: Some(i32::MAX),
            literal_prefix: Some("'"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: true,
            searchable: TYPE_SEARCHABLE,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("STRING"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_LONGVARCHAR),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "VARCHAR",
            data_type: JDBC_VARCHAR,
            column_size: Some(i32::MAX),
            literal_prefix: Some("'"),
            literal_suffix: Some("'"),
            create_params: Some("length"),
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: true,
            searchable: TYPE_SEARCHABLE,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("VARCHAR"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_VARCHAR),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "CHAR",
            data_type: JDBC_CHAR,
            column_size: Some(255),
            literal_prefix: Some("'"),
            literal_suffix: Some("'"),
            create_params: Some("length"),
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: true,
            searchable: TYPE_SEARCHABLE,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("CHAR"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_CHAR),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "BINARY",
            data_type: JDBC_BINARY,
            column_size: Some(i32::MAX),
            literal_prefix: Some("X'"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("BINARY"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_BINARY),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "DATE",
            data_type: JDBC_DATE,
            column_size: Some(10),
            literal_prefix: Some("DATE '"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("DATE"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_DATE),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "TIMESTAMP",
            data_type: JDBC_TIMESTAMP,
            column_size: Some(29),
            literal_prefix: Some("TIMESTAMP '"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("TIMESTAMP"),
            minimum_scale: Some(0),
            maximum_scale: Some(9),
            sql_data_type: Some(JDBC_TIMESTAMP),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "TIMESTAMP_NTZ",
            data_type: JDBC_TIMESTAMP,
            column_size: Some(29),
            literal_prefix: Some("TIMESTAMP_NTZ '"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("TIMESTAMP_NTZ"),
            minimum_scale: Some(0),
            maximum_scale: Some(9),
            sql_data_type: Some(JDBC_TIMESTAMP),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "INTERVAL DAY TO SECOND",
            data_type: JDBC_OTHER,
            column_size: None,
            literal_prefix: Some("INTERVAL '"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("INTERVAL DAY TO SECOND"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_OTHER),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "INTERVAL YEAR TO MONTH",
            data_type: JDBC_OTHER,
            column_size: None,
            literal_prefix: Some("INTERVAL '"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("INTERVAL YEAR TO MONTH"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_OTHER),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "ARRAY",
            data_type: JDBC_ARRAY,
            column_size: None,
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_NONE,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("ARRAY"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_ARRAY),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "MAP",
            data_type: JDBC_JAVA_OBJECT,
            column_size: None,
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_NONE,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("MAP"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_JAVA_OBJECT),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "STRUCT",
            data_type: JDBC_STRUCT,
            column_size: None,
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_NONE,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("STRUCT"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_STRUCT),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "VARIANT",
            data_type: JDBC_OTHER,
            column_size: None,
            literal_prefix: Some("'"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: true,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("VARIANT"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_OTHER),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "VARBINARY",
            data_type: JDBC_VARBINARY,
            column_size: Some(i32::MAX),
            literal_prefix: Some("X'"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("VARBINARY"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_VARBINARY),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "LONGVARBINARY",
            data_type: JDBC_LONGVARBINARY,
            column_size: Some(i32::MAX),
            literal_prefix: Some("X'"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("LONGVARBINARY"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: Some(JDBC_LONGVARBINARY),
            sql_datetime_sub: None,
            num_prec_radix: None,
        },
        R {
            type_name: "NUMERIC",
            data_type: JDBC_NUMERIC,
            column_size: Some(38),
            literal_prefix: None,
            literal_suffix: None,
            create_params: Some("precision,scale"),
            nullable: TYPE_NULLABLE_UNKNOWN,
            case_sensitive: false,
            searchable: TYPE_PRED_BASIC,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_unique_value: Some(false),
            local_type_name: Some("NUMERIC"),
            minimum_scale: Some(0),
            maximum_scale: Some(38),
            sql_data_type: Some(JDBC_NUMERIC),
            sql_datetime_sub: None,
            num_prec_radix: Some(10),
        },
    ]
}

/// Build the `type_info` `RecordBatch` and return a reader yielding
/// it once. Public to the crate via [`type_info_reader`]; the batch
/// is constructed fresh per call so we don't have to keep a
/// shared-mutable handle alive.
pub(crate) fn type_info_reader() -> Box<dyn ResultReader + Send> {
    let rows = type_info_rows();
    let schema = TYPE_INFO_SCHEMA.clone();

    let type_name: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.type_name),
    ));
    let data_type: ArrayRef = Arc::new(Int32Array::from_iter_values(
        rows.iter().map(|r| r.data_type),
    ));
    let column_size: ArrayRef = Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.column_size)));
    let literal_prefix: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.literal_prefix),
    ));
    let literal_suffix: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.literal_suffix),
    ));
    let create_params: ArrayRef =
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.create_params)));
    let nullable: ArrayRef = Arc::new(Int32Array::from_iter_values(
        rows.iter().map(|r| r.nullable),
    ));
    let case_sensitive: ArrayRef = Arc::new(BooleanArray::from_iter(
        rows.iter().map(|r| Some(r.case_sensitive)),
    ));
    let searchable: ArrayRef = Arc::new(Int32Array::from_iter_values(
        rows.iter().map(|r| r.searchable),
    ));
    let unsigned_attribute: ArrayRef = Arc::new(BooleanArray::from_iter(
        rows.iter().map(|r| r.unsigned_attribute),
    ));
    let fixed_prec_scale: ArrayRef = Arc::new(BooleanArray::from_iter(
        rows.iter().map(|r| Some(r.fixed_prec_scale)),
    ));
    let auto_unique_value: ArrayRef = Arc::new(BooleanArray::from_iter(
        rows.iter().map(|r| r.auto_unique_value),
    ));
    let local_type_name: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.local_type_name),
    ));
    let minimum_scale: ArrayRef =
        Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.minimum_scale)));
    let maximum_scale: ArrayRef =
        Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.maximum_scale)));
    let sql_data_type: ArrayRef =
        Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.sql_data_type)));
    let sql_datetime_sub: ArrayRef = Arc::new(Int32Array::from_iter(
        rows.iter().map(|r| r.sql_datetime_sub),
    ));
    let num_prec_radix: ArrayRef =
        Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.num_prec_radix)));
    let interval_precision: ArrayRef =
        Arc::new(Int32Array::from_iter(rows.iter().map(|_| None::<i32>)));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            type_name,
            data_type,
            column_size,
            literal_prefix,
            literal_suffix,
            create_params,
            nullable,
            case_sensitive,
            searchable,
            unsigned_attribute,
            fixed_prec_scale,
            auto_unique_value,
            local_type_name,
            minimum_scale,
            maximum_scale,
            sql_data_type,
            sql_datetime_sub,
            num_prec_radix,
            interval_precision,
        ],
    )
    .expect("type_info batch construction must succeed (static schema + arrays)");

    Box::new(InMemoryReader::new(schema, vec![batch]))
}

/// Build the `table_types` `RecordBatch` from a list of supported
/// types (sourced from the session's transport), and return a
/// reader yielding it once.
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

    #[test]
    fn type_info_reader_yields_expected_shape() {
        let mut r = type_info_reader();
        let rb = r.next_batch().unwrap().expect("one batch");
        assert_eq!(rb.schema(), *TYPE_INFO_SCHEMA);
        assert!(rb.num_rows() >= 19, "{} rows", rb.num_rows());
        // Stream is exhausted after the first batch.
        assert!(r.next_batch().unwrap().is_none());
    }

    #[test]
    fn type_info_includes_required_databricks_types() {
        let mut r = type_info_reader();
        let rb = r.next_batch().unwrap().unwrap();
        let names = rb
            .column_by_name("TYPE_NAME")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let collected: std::collections::HashSet<&str> =
            (0..names.len()).map(|i| names.value(i)).collect();
        for required in [
            "BOOLEAN",
            "INT",
            "BIGINT",
            "DOUBLE",
            "DECIMAL",
            "STRING",
            "BINARY",
            "DATE",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "ARRAY",
            "MAP",
            "STRUCT",
            "VARIANT",
        ] {
            assert!(collected.contains(required), "missing {required}");
        }
    }

    #[test]
    fn table_types_reader_yields_provided_types() {
        let types = vec!["TABLE".to_string(), "VIEW".to_string()];
        let mut r = table_types_reader(&types);
        let rb = r.next_batch().unwrap().expect("one batch");
        assert_eq!(rb.num_rows(), 2);
        let arr = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "TABLE");
        assert_eq!(arr.value(1), "VIEW");
        assert!(r.next_batch().unwrap().is_none());
    }
}
