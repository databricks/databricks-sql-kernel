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
//! [`crate::metadata::thrift_shape`] alongside the reshape logic —
//! they form a single JDBC-shape contract that's the kernel's
//! responsibility. `primary_keys` / `foreign_keys` / `procedures` /
//! `functions` still pass server shape through; reshape for those is
//! deferred.
//!
//! Static methods (`type_info`, `table_types`) have no SEA response to
//! reshape, so the kernel synthesises their results. `type_info` mirrors
//! the established Thrift/HiveServer2 `getTypeInfo` column contract;
//! wrappers can therefore switch transports without changing the schema
//! or row corpus. `table_types` uses the JDBC `getTableTypes` contract.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

fn thrift_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

/// Thrift/HiveServer2 `getTypeInfo` — 18 columns.
///
/// Every field is nullable because the Node Thrift driver's
/// `hiveSchemaToArrowSchema` adapter represents the server schema that way.
pub(crate) static TYPE_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    schema(vec![
        thrift_field("TYPE_NAME", DataType::Utf8),
        thrift_field("DATA_TYPE", DataType::Int32),
        thrift_field("PRECISION", DataType::Int32),
        thrift_field("LITERAL_PREFIX", DataType::Utf8),
        thrift_field("LITERAL_SUFFIX", DataType::Utf8),
        thrift_field("CREATE_PARAMS", DataType::Utf8),
        thrift_field("NULLABLE", DataType::Int16),
        thrift_field("CASE_SENSITIVE", DataType::Boolean),
        thrift_field("SEARCHABLE", DataType::Int16),
        thrift_field("UNSIGNED_ATTRIBUTE", DataType::Boolean),
        thrift_field("FIXED_PREC_SCALE", DataType::Boolean),
        thrift_field("AUTO_INCREMENT", DataType::Boolean),
        thrift_field("LOCAL_TYPE_NAME", DataType::Utf8),
        thrift_field("MINIMUM_SCALE", DataType::Int16),
        thrift_field("MAXIMUM_SCALE", DataType::Int16),
        thrift_field("SQL_DATA_TYPE", DataType::Int32),
        thrift_field("SQL_DATETIME_SUB", DataType::Int32),
        thrift_field("NUM_PREC_RADIX", DataType::Int32),
    ])
});

/// JDBC `getTableTypes` — single column.
pub(crate) static TABLE_TYPES_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| schema(vec![Field::new("TABLE_TYPE", DataType::Utf8, false)]));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_info_schema_matches_thrift() {
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

        assert_eq!(TYPE_INFO_SCHEMA.fields().len(), expected.len());
        for (field, (name, data_type)) in TYPE_INFO_SCHEMA.fields().iter().zip(expected) {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), &data_type);
            assert!(field.is_nullable(), "{name} must be nullable");
        }
    }

    #[test]
    fn table_types_schema_has_single_column_named_table_type() {
        assert_eq!(TABLE_TYPES_SCHEMA.fields().len(), 1);
        assert_eq!(TABLE_TYPES_SCHEMA.field(0).name(), "TABLE_TYPE");
    }
}
