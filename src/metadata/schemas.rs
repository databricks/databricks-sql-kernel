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

//! JDBC/ODBC-conforming output schemas for `Metadata::*`.
//!
//! Single source of truth for the column order, names, types, and
//! nullability of every metadata `ResultStream`. The reshape layer
//! ([`crate::metadata::shape`]) and the static-data layer
//! ([`crate::metadata::static_data`]) both clone from these.
//!
//! Field types match the JDBC `DatabaseMetaData` contract (and the
//! ODBC `SQLTables`/`SQLColumns`/etc. shape since they share the same
//! lineage). For columns Databricks has no value for (e.g.
//! `BUFFER_LENGTH`, `SQL_DATETIME_SUB`), the schemas declare the
//! field nullable and the reshape layer emits NULL.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// JDBC `getCatalogs` — one column.
pub(crate) static CATALOGS_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| schema(vec![Field::new("TABLE_CAT", DataType::Utf8, false)]));

/// JDBC `getSchemas` — `TABLE_SCHEM`, `TABLE_CATALOG`. Note that JDBC
/// names the catalog column `TABLE_CATALOG` here (not `TABLE_CAT`)
/// for historical reasons — `getSchemas` predates `getTables`.
pub(crate) static SCHEMAS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("TABLE_SCHEM", DataType::Utf8, false),
        Field::new("TABLE_CATALOG", DataType::Utf8, true),
    ])
});

/// JDBC `getTables` — 10 columns. Last 5 are NULL for Databricks.
pub(crate) static TABLES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("TABLE_TYPE", DataType::Utf8, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("TYPE_CAT", DataType::Utf8, true),
        Field::new("TYPE_SCHEM", DataType::Utf8, true),
        Field::new("TYPE_NAME", DataType::Utf8, true),
        Field::new("SELF_REFERENCING_COL_NAME", DataType::Utf8, true),
        Field::new("REF_GENERATION", DataType::Utf8, true),
    ])
});

/// JDBC `getColumns` — 24 columns.
pub(crate) static COLUMNS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("DATA_TYPE", DataType::Int32, false),
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        Field::new("BUFFER_LENGTH", DataType::Int32, true),
        Field::new("DECIMAL_DIGITS", DataType::Int32, true),
        Field::new("NUM_PREC_RADIX", DataType::Int32, true),
        Field::new("NULLABLE", DataType::Int32, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("COLUMN_DEF", DataType::Utf8, true),
        Field::new("SQL_DATA_TYPE", DataType::Int32, true),
        Field::new("SQL_DATETIME_SUB", DataType::Int32, true),
        Field::new("CHAR_OCTET_LENGTH", DataType::Int32, true),
        Field::new("ORDINAL_POSITION", DataType::Int32, false),
        Field::new("IS_NULLABLE", DataType::Utf8, false),
        Field::new("SCOPE_CATALOG", DataType::Utf8, true),
        Field::new("SCOPE_SCHEMA", DataType::Utf8, true),
        Field::new("SCOPE_TABLE", DataType::Utf8, true),
        Field::new("SOURCE_DATA_TYPE", DataType::Int32, true),
        Field::new("IS_AUTOINCREMENT", DataType::Utf8, false),
        Field::new("IS_GENERATEDCOLUMN", DataType::Utf8, false),
    ])
});

/// JDBC `getPrimaryKeys` — 6 columns.
pub(crate) static PRIMARY_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("KEY_SEQ", DataType::Int32, false),
        Field::new("PK_NAME", DataType::Utf8, true),
    ])
});

/// JDBC `getImportedKeys` / `getExportedKeys` — 14 columns.
pub(crate) static FOREIGN_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("PKTABLE_CAT", DataType::Utf8, true),
        Field::new("PKTABLE_SCHEM", DataType::Utf8, true),
        Field::new("PKTABLE_NAME", DataType::Utf8, false),
        Field::new("PKCOLUMN_NAME", DataType::Utf8, false),
        Field::new("FKTABLE_CAT", DataType::Utf8, true),
        Field::new("FKTABLE_SCHEM", DataType::Utf8, true),
        Field::new("FKTABLE_NAME", DataType::Utf8, false),
        Field::new("FKCOLUMN_NAME", DataType::Utf8, false),
        Field::new("KEY_SEQ", DataType::Int32, false),
        Field::new("UPDATE_RULE", DataType::Int32, true),
        Field::new("DELETE_RULE", DataType::Int32, true),
        Field::new("FK_NAME", DataType::Utf8, true),
        Field::new("PK_NAME", DataType::Utf8, true),
        Field::new("DEFERRABILITY", DataType::Int32, true),
    ])
});

/// JDBC `getProcedures` — 9 columns.
pub(crate) static PROCEDURES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("PROCEDURE_CAT", DataType::Utf8, true),
        Field::new("PROCEDURE_SCHEM", DataType::Utf8, true),
        Field::new("PROCEDURE_NAME", DataType::Utf8, false),
        Field::new("RESERVED1", DataType::Utf8, true),
        Field::new("RESERVED2", DataType::Utf8, true),
        Field::new("RESERVED3", DataType::Utf8, true),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("PROCEDURE_TYPE", DataType::Int32, false),
        Field::new("SPECIFIC_NAME", DataType::Utf8, true),
    ])
});

/// JDBC `getFunctions` — 6 columns.
pub(crate) static FUNCTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("FUNCTION_CAT", DataType::Utf8, true),
        Field::new("FUNCTION_SCHEM", DataType::Utf8, true),
        Field::new("FUNCTION_NAME", DataType::Utf8, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("FUNCTION_TYPE", DataType::Int32, false),
        Field::new("SPECIFIC_NAME", DataType::Utf8, true),
    ])
});

/// JDBC `getTypeInfo` — 19 columns.
pub(crate) static TYPE_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("DATA_TYPE", DataType::Int32, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        Field::new("LITERAL_PREFIX", DataType::Utf8, true),
        Field::new("LITERAL_SUFFIX", DataType::Utf8, true),
        Field::new("CREATE_PARAMS", DataType::Utf8, true),
        Field::new("NULLABLE", DataType::Int32, false),
        Field::new("CASE_SENSITIVE", DataType::Boolean, false),
        Field::new("SEARCHABLE", DataType::Int32, false),
        Field::new("UNSIGNED_ATTRIBUTE", DataType::Boolean, true),
        Field::new("FIXED_PREC_SCALE", DataType::Boolean, false),
        Field::new("AUTO_UNIQUE_VALUE", DataType::Boolean, true),
        Field::new("LOCAL_TYPE_NAME", DataType::Utf8, true),
        Field::new("MINIMUM_SCALE", DataType::Int32, true),
        Field::new("MAXIMUM_SCALE", DataType::Int32, true),
        Field::new("SQL_DATA_TYPE", DataType::Int32, true),
        Field::new("SQL_DATETIME_SUB", DataType::Int32, true),
        Field::new("NUM_PREC_RADIX", DataType::Int32, true),
        Field::new("INTERVAL_PRECISION", DataType::Int32, true),
    ])
});

/// JDBC `getTableTypes` — single column.
pub(crate) static TABLE_TYPES_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| schema(vec![Field::new("TABLE_TYPE", DataType::Utf8, false)]));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogs_schema_has_one_column() {
        assert_eq!(CATALOGS_SCHEMA.fields().len(), 1);
        assert_eq!(CATALOGS_SCHEMA.field(0).name(), "TABLE_CAT");
    }

    #[test]
    fn tables_schema_has_ten_columns_in_order() {
        let names: Vec<&str> = TABLES_SCHEMA
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
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
    }

    #[test]
    fn columns_schema_has_24_columns() {
        assert_eq!(COLUMNS_SCHEMA.fields().len(), 24);
    }

    #[test]
    fn foreign_keys_schema_has_14_columns() {
        assert_eq!(FOREIGN_KEYS_SCHEMA.fields().len(), 14);
    }

    #[test]
    fn type_info_schema_has_19_columns() {
        assert_eq!(TYPE_INFO_SCHEMA.fields().len(), 19);
    }

    #[test]
    fn primary_keys_required_columns_not_nullable() {
        let s = &*PRIMARY_KEYS_SCHEMA;
        assert!(!s.field(2).is_nullable(), "TABLE_NAME must be NOT NULL");
        assert!(!s.field(3).is_nullable(), "COLUMN_NAME must be NOT NULL");
        assert!(!s.field(4).is_nullable(), "KEY_SEQ must be NOT NULL");
    }
}
