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

//! Output schemas for the static `Metadata::type_info` and
//! `Metadata::table_types` methods.
//!
//! Schemas for the four wire-driven core methods (`list_catalogs`,
//! `list_schemas`, `list_tables`, `list_columns`) live in
//! [`crate::metadata::jdbc_shape`] alongside the reshape logic —
//! they form a single JDBC-shape contract that's the kernel's
//! responsibility. `primary_keys` / `foreign_keys` / `procedures` /
//! `functions` still pass server shape through; reshape for those is
//! deferred.
//!
//! Static methods (`type_info`, `table_types`) have no server-native
//! shape to mirror — there's no `SHOW TYPE_INFO` command — so the
//! kernel synthesises a result. The schemas live here so the
//! synthesiser sees one source of truth. We use the **JDBC
//! `getTypeInfo` / `getTableTypes`** column contract for the same
//! reason as the four core methods: it's the contract JDBC, ODBC,
//! and PySQL all agree on. ADBC restructures from this shape.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// JDBC `getTypeInfo` — 19 columns. See the module-level docs for
/// why we pick the JDBC contract here even though the kernel is
/// otherwise pass-through.
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
    fn type_info_schema_has_19_columns() {
        assert_eq!(TYPE_INFO_SCHEMA.fields().len(), 19);
    }

    #[test]
    fn table_types_schema_has_single_column_named_table_type() {
        assert_eq!(TABLE_TYPES_SCHEMA.fields().len(), 1);
        assert_eq!(TABLE_TYPES_SCHEMA.field(0).name(), "TABLE_TYPE");
    }
}
