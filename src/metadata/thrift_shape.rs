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

//! Thrift-compatible reshape for wire-driven metadata RPCs.
//!
//! The server's `SHOW CATALOGS / SHOW SCHEMAS / SHOW TABLES / SHOW COLUMNS`
//! commands return columns named in the Databricks dialect (`catalog`,
//! `namespace`, `tableName`, `columnType`, …). The existing Thrift
//! backend in the Python connector — and equivalently JDBC, ODBC, and
//! the Node driver — surface a different, stable column-name contract
//! (`TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `DATA_TYPE`, …) to their
//! users. Those names originate in the JDBC `DatabaseMetaData` spec;
//! Thrift inherited them via HiveServer2, and every Databricks driver
//! since has matched. Pushing the rename + synthesis into the kernel
//! keeps every binding free of duplicated reshape logic and gives the
//! kernel backend a drop-in shape match with Thrift.
//!
//! ADBC `GetObjects` is the one outlier — it returns a deeply nested
//! Arrow struct rather than a flat result set. The ADBC binding
//! restructures from these rows; "raw" vs "Thrift-compatible" as
//! input is roughly equal effort, so we standardise on the latter.
//!
//! ## What this module owns
//!
//! - The canonical column list per operation (single source of truth;
//!   kept aligned with what the Thrift backend returns, which in turn
//!   tracks `java.sql.DatabaseMetaData`).
//! - Functions to reshape a server-shaped Arrow `RecordBatch` into
//!   the Thrift-compatible `RecordBatch`: rename source columns,
//!   synthesize missing ones (mostly nullable `Utf8`), filter when
//!   ODBC requires it (`table_types`), and sort rows per the JDBC
//!   ordering Thrift inherited.
//!
//! ## What this module deliberately does NOT do
//!
//! - **Type-name normalisation** (`timestamp_ntz` → `timestamp`).
//!   The kernel preserves the server's precise type names so ADBC /
//!   future native consumers keep access to them. JDBC and ODBC
//!   wrappers that want the lossy mapping perform it client-side; the
//!   precise `column_type` string is always present.
//! - **`DESCRIBE TABLE EXTENDED` enrichment.** `SHOW COLUMNS IN
//!   CATALOG` already returns the same enriched column set JDBC uses
//!   (`columnSize`, `decimalDigits`, `radix`, `isNullable`,
//!   `ordinalPosition`, `isAutoIncrement`, `isGenerated`, `remarks`).

use crate::kernel_error::{Error, Result};
use crate::metadata::type_mapping::databricks_type_to_xdbc;
use crate::reader::ResultReader;
use arrow_array::{Array, ArrayRef, Int16Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};

/// Detect "object not found" server errors that JDBC's `DatabaseMetaData`
/// contract translates into an empty result rather than a thrown
/// exception. Mirrors the JDBC driver's
/// `MetadataResultConstants.isObjectNotFoundException` (catalog,
/// schema, or table not found).
///
/// We match on:
/// - SQL state `42704` — the SQL standard "undefined object" state
///   the Databricks server emits for missing catalog / schema / table.
/// - Error-message substrings for older server builds that don't
///   populate `sqlstate` reliably (`NO_SUCH_CATALOG_EXCEPTION`,
///   `TABLE_OR_VIEW_NOT_FOUND`, `SCHEMA_NOT_FOUND`,
///   `INVALID_PARAMETER_VALUE`).
pub(crate) fn is_object_not_found(err: &Error) -> bool {
    if err.sql_state.as_deref() == Some("42704") {
        return true;
    }
    let msg = &err.message;
    msg.contains("NO_SUCH_CATALOG_EXCEPTION")
        || msg.contains("TABLE_OR_VIEW_NOT_FOUND")
        || msg.contains("SCHEMA_NOT_FOUND")
        || msg.contains("INVALID_PARAMETER_VALUE")
}

// ─── JDBC schemas ──────────────────────────────────────────────────

/// JDBC `getCatalogs` — single column.
pub(crate) static GET_CATALOGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "TABLE_CAT",
        DataType::Utf8,
        true,
    )]))
});

/// JDBC `getSchemas` — `TABLE_SCHEM`, `TABLE_CATALOG`.
pub(crate) static GET_SCHEMAS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_SCHEM", DataType::Utf8, false),
        Field::new("TABLE_CATALOG", DataType::Utf8, true),
    ]))
});

/// JDBC `getTables` — 10 columns. Matches `java.sql.DatabaseMetaData.getTables`.
pub(crate) static GET_TABLES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
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
    ]))
});

/// JDBC `getColumns` — 23 columns. Matches `java.sql.DatabaseMetaData.getColumns`.
pub(crate) static GET_COLUMNS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        Field::new("DATA_TYPE", DataType::Int32, false),
        Field::new("TYPE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_SIZE", DataType::Int32, true),
        // JDBC spec says `Types.INTEGER` (Int32) for BUFFER_LENGTH,
        // but Thrift's `GetColumns` RPC ships it as TINYINT — that's
        // what existing PySQL/JDBC users see today. Match Thrift's
        // wire type so the kernel backend is a drop-in replacement.
        // The value is always null (no server-side source), so the
        // narrower range has no practical effect.
        Field::new("BUFFER_LENGTH", DataType::Int8, true),
        Field::new("DECIMAL_DIGITS", DataType::Int32, true),
        Field::new("NUM_PREC_RADIX", DataType::Int32, true),
        Field::new("NULLABLE", DataType::Int32, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("COLUMN_DEF", DataType::Utf8, true),
        Field::new("SQL_DATA_TYPE", DataType::Int32, true),
        Field::new("SQL_DATETIME_SUB", DataType::Int32, true),
        Field::new("CHAR_OCTET_LENGTH", DataType::Int32, true),
        // JDBC spec says non-null, but older DBR builds may omit
        // `ordinalPosition` from the `SHOW COLUMNS` response. Declare
        // nullable so the reshape doesn't fail on those rows; modern
        // DBR populates it and the value flows through.
        Field::new("ORDINAL_POSITION", DataType::Int32, true),
        Field::new("IS_NULLABLE", DataType::Utf8, false),
        Field::new("SCOPE_CATALOG", DataType::Utf8, true),
        Field::new("SCOPE_SCHEMA", DataType::Utf8, true),
        Field::new("SCOPE_TABLE", DataType::Utf8, true),
        // JDBC spec says `Types.SMALLINT` (Int16) for SOURCE_DATA_TYPE.
        // Thrift's `GetColumns` ships it as SMALLINT and that's what
        // PySQL/JDBC users see today; we previously emitted Int32 to
        // align with Int32 elsewhere in the schema, but Thrift's
        // narrower type is the user-facing contract.
        Field::new("SOURCE_DATA_TYPE", DataType::Int16, true),
        // JDBC `DatabaseMetaData.getColumns` Javadoc names this
        // `IS_AUTOINCREMENT` (one word). Thrift's wire response uses
        // `IS_AUTO_INCREMENT` (with the underscore), and that's what
        // existing PySQL/JDBC users on Databricks see today. Match
        // Thrift's spelling so the kernel backend is a drop-in
        // replacement; the column meaning is identical.
        Field::new("IS_AUTO_INCREMENT", DataType::Utf8, false),
    ]))
});

/// JDBC `getPrimaryKeys` — 6 columns. Matches
/// `java.sql.DatabaseMetaData.getPrimaryKeys` (and the Databricks
/// Thrift `TGetPrimaryKeysResp` column set). `KEY_SEQ` follows the
/// Thrift wire type (`INT` / Int32), not JDBC's `short`, so the kernel
/// backend is a drop-in replacement for existing PySQL / JDBC users.
pub(crate) static GET_PRIMARY_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("TABLE_CAT", DataType::Utf8, true),
        Field::new("TABLE_SCHEM", DataType::Utf8, true),
        Field::new("TABLE_NAME", DataType::Utf8, false),
        Field::new("COLUMN_NAME", DataType::Utf8, false),
        // JDBC spec says non-null, but declare nullable so the reshape
        // doesn't fail on the rare server response that omits `keySeq`;
        // modern DBR always populates it and the value flows through.
        Field::new("KEY_SEQ", DataType::Int32, true),
        Field::new("PK_NAME", DataType::Utf8, true),
    ]))
});

/// JDBC `getCrossReference` / `getImportedKeys` — 14 columns. Matches
/// `java.sql.DatabaseMetaData.getCrossReference` (and the Databricks
/// Thrift `TGetCrossReferenceResp` column set). `KEY_SEQ` is `INT`
/// (Int32) and `UPDATE_RULE` / `DELETE_RULE` / `DEFERRABILITY` are
/// `SMALLINT` (Int16), matching the Thrift wire types. `UPDATE_RULE` /
/// `DELETE_RULE` / `PK_NAME` have no source in the Databricks
/// `SHOW FOREIGN KEYS` response, so they are emitted as typed nulls
/// (no value is synthesized).
pub(crate) static GET_CROSS_REFERENCE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("PKTABLE_CAT", DataType::Utf8, true),
        Field::new("PKTABLE_SCHEM", DataType::Utf8, true),
        Field::new("PKTABLE_NAME", DataType::Utf8, false),
        Field::new("PKCOLUMN_NAME", DataType::Utf8, false),
        Field::new("FKTABLE_CAT", DataType::Utf8, true),
        Field::new("FKTABLE_SCHEM", DataType::Utf8, true),
        Field::new("FKTABLE_NAME", DataType::Utf8, false),
        Field::new("FKCOLUMN_NAME", DataType::Utf8, false),
        // Nullable for the same reason as `getPrimaryKeys` KEY_SEQ.
        Field::new("KEY_SEQ", DataType::Int32, true),
        Field::new("UPDATE_RULE", DataType::Int16, true),
        Field::new("DELETE_RULE", DataType::Int16, true),
        Field::new("FK_NAME", DataType::Utf8, true),
        Field::new("PK_NAME", DataType::Utf8, true),
        Field::new("DEFERRABILITY", DataType::Int16, true),
    ]))
});

/// JDBC `getProcedures` — 9 columns. Matches
/// `java.sql.DatabaseMetaData.getProcedures`. Columns 4-6 are reserved
/// by the JDBC spec ("reserved for future use") and emitted as null.
/// `PROCEDURE_TYPE` is `SMALLINT` (Int16) per the spec.
pub(crate) static GET_PROCEDURES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("PROCEDURE_CAT", DataType::Utf8, true),
        Field::new("PROCEDURE_SCHEM", DataType::Utf8, true),
        Field::new("PROCEDURE_NAME", DataType::Utf8, false),
        // JDBC "reserved for future use" columns (4, 5, 6).
        Field::new("RESERVED1", DataType::Utf8, true),
        Field::new("RESERVED2", DataType::Utf8, true),
        Field::new("RESERVED3", DataType::Utf8, true),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("PROCEDURE_TYPE", DataType::Int16, true),
        Field::new("SPECIFIC_NAME", DataType::Utf8, true),
    ]))
});

/// JDBC `getFunctions` — 6 columns. Matches
/// `java.sql.DatabaseMetaData.getFunctions`. `FUNCTION_TYPE` is
/// `SMALLINT` (Int16) per the spec.
pub(crate) static GET_FUNCTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("FUNCTION_CAT", DataType::Utf8, true),
        Field::new("FUNCTION_SCHEM", DataType::Utf8, true),
        // Classic SHOW FUNCTIONS can omit the modern `functionName` field.
        // Keep this nullable so one malformed row does not discard the result.
        Field::new("FUNCTION_NAME", DataType::Utf8, true),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("FUNCTION_TYPE", DataType::Int16, true),
        Field::new("SPECIFIC_NAME", DataType::Utf8, true),
    ]))
});

/// JDBC `getProcedureColumns` — 20 columns. Matches
/// `java.sql.DatabaseMetaData.getProcedureColumns`. `COLUMN_TYPE`,
/// `SCALE`, and `RADIX` are `SMALLINT` (Int16); `DATA_TYPE`,
/// `PRECISION`, `LENGTH`, `NULLABLE`, `SQL_DATA_TYPE`,
/// `SQL_DATETIME_SUB`, `CHAR_OCTET_LENGTH`, and `ORDINAL_POSITION` are
/// `INTEGER` (Int32), per the JDBC spec. `LENGTH`, `SQL_DATA_TYPE`,
/// `SQL_DATETIME_SUB`, and `CHAR_OCTET_LENGTH` have no
/// `information_schema.parameters` source and are emitted as null; the
/// ODBC driver derives its equivalents from the other columns.
pub(crate) static GET_PROCEDURE_COLUMNS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("PROCEDURE_CAT", DataType::Utf8, true),
        Field::new("PROCEDURE_SCHEM", DataType::Utf8, true),
        Field::new("PROCEDURE_NAME", DataType::Utf8, false),
        // JDBC spec types COLUMN_NAME / TYPE_NAME non-null, but
        // information_schema.parameters can carry a NULL parameter_name (the
        // return-value row, is_result='YES', and unnamed/positional params), so
        // declare them nullable — otherwise a NULL there fails RecordBatch::try_new
        // (data_loss) and drops the whole result. Matches the defensive nullable
        // declarations of GET_COLUMNS_SCHEMA / GET_PRIMARY_KEYS_SCHEMA above.
        Field::new("COLUMN_NAME", DataType::Utf8, true),
        Field::new("COLUMN_TYPE", DataType::Int16, false),
        Field::new("DATA_TYPE", DataType::Int32, false),
        Field::new("TYPE_NAME", DataType::Utf8, true),
        Field::new("PRECISION", DataType::Int32, true),
        Field::new("LENGTH", DataType::Int32, true),
        Field::new("SCALE", DataType::Int16, true),
        Field::new("RADIX", DataType::Int16, true),
        Field::new("NULLABLE", DataType::Int32, false),
        Field::new("REMARKS", DataType::Utf8, true),
        Field::new("COLUMN_DEF", DataType::Utf8, true),
        Field::new("SQL_DATA_TYPE", DataType::Int32, true),
        Field::new("SQL_DATETIME_SUB", DataType::Int32, true),
        Field::new("CHAR_OCTET_LENGTH", DataType::Int32, true),
        Field::new("ORDINAL_POSITION", DataType::Int32, false),
        Field::new("IS_NULLABLE", DataType::Utf8, true),
        Field::new("SPECIFIC_NAME", DataType::Utf8, true),
    ]))
});

// ─── Helpers ───────────────────────────────────────────────────────

/// Find the index of a column by name in the source schema, returning
/// `None` if absent. Server-shaped results often omit columns that an
/// older DBR didn't emit — callers synthesize a null replacement when
/// the lookup misses.
fn column_index(schema: &Schema, name: &str) -> Option<usize> {
    schema.fields().iter().position(|f| f.name() == name)
}

/// Return the named source column as a `Utf8` array, casting from a
/// related string type (`LargeUtf8`, dictionary-encoded Utf8, …) if
/// necessary. Returns a fresh all-null array when the column is
/// absent from the source schema — older DBR builds omit columns
/// modern DBR includes, and the synthesised null lets the reshape
/// proceed.
///
/// A cast failure is surfaced as `Error::data_loss` rather than
/// silently producing nulls. Identifier columns in `SHOW *` responses
/// have always been `Utf8`; if a future DBR returns a genuinely
/// incompatible type, the kernel should fail loudly rather than emit
/// rows the JDBC schema then rejects at `RecordBatch::try_new` with a
/// confusing nullability-violation error.
fn utf8_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    match column_index(batch.schema_ref(), name) {
        Some(idx) => {
            let col = batch.column(idx);
            if col.data_type() == &DataType::Utf8 {
                Ok(Arc::clone(col))
            } else {
                arrow_cast::cast(col.as_ref(), &DataType::Utf8).map_err(|e| {
                    Error::data_loss().message(format!(
                        "unexpected non-Utf8 type for SHOW response column {name:?}: {e}"
                    ))
                })
            }
        }
        None => Ok(Arc::new(StringArray::from(vec![
            None::<&str>;
            batch.num_rows()
        ]))),
    }
}

/// Like `utf8_column`, but substitutes `""` for null entries instead
/// of preserving them as Arrow nulls. Used for JDBC string fields
/// where the spec allows null but downstream consumers (Java
/// `String`-handling code, SQLAlchemy reflection, etc.) routinely
/// crash on null and the Thrift backend has historically returned
/// `""` for the same fields. Today only `REMARKS`.
fn utf8_column_or_empty(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let n = batch.num_rows();
    let Some(idx) = column_index(batch.schema_ref(), name) else {
        return Ok(Arc::new(StringArray::from(vec![Some(""); n])));
    };
    let col = batch.column(idx);
    let casted = if col.data_type() == &DataType::Utf8 {
        Arc::clone(col)
    } else {
        arrow_cast::cast(col.as_ref(), &DataType::Utf8).map_err(|e| {
            Error::data_loss().message(format!(
                "unexpected non-Utf8 type for SHOW response column {name:?}: {e}"
            ))
        })?
    };
    let strs = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            Error::data_loss().message(format!(
                "utf8_column_or_empty: column {name:?} cast did not yield StringArray"
            ))
        })?;
    let values: Vec<&str> = (0..n)
        .map(|i| if strs.is_null(i) { "" } else { strs.value(i) })
        .collect();
    Ok(Arc::new(StringArray::from(values)))
}

/// Build an all-null `Utf8` column of `len` rows. Used for JDBC columns
/// that have no server-side source (`TYPE_CAT`, `SELF_REFERENCING_COL_NAME`,
/// `COLUMN_DEF`, …).
fn null_utf8(len: usize) -> ArrayRef {
    Arc::new(StringArray::from(vec![None::<&str>; len]))
}

/// Build a constant `Utf8` column of `len` rows. Used to synthesize
/// columns whose value comes from the request args (e.g. `TABLE_CATALOG`
/// in `list_schemas` when the input catalog wasn't echoed by the server).
fn constant_utf8(len: usize, value: Option<&str>) -> ArrayRef {
    let v = value.map(|s| s.to_owned());
    Arc::new(StringArray::from(vec![v; len]))
}

/// Return the named source column as an `Int32` array, casting from a
/// related integer type if necessary. Returns an all-null `Int32`
/// array when the column is absent — symmetric with [`utf8_column`].
/// A cast failure is surfaced as `Error::data_loss` rather than
/// silently producing nulls.
fn int32_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    match column_index(batch.schema_ref(), name) {
        Some(idx) => {
            let col = batch.column(idx);
            if col.data_type() == &DataType::Int32 {
                Ok(Arc::clone(col))
            } else {
                arrow_cast::cast(col.as_ref(), &DataType::Int32).map_err(|e| {
                    Error::data_loss().message(format!(
                        "unexpected non-Int32 type for SHOW response column {name:?}: {e}"
                    ))
                })
            }
        }
        None => Ok(Arc::new(Int32Array::from(vec![
            None::<i32>;
            batch.num_rows()
        ]))),
    }
}

fn null_int32(len: usize) -> ArrayRef {
    Arc::new(Int32Array::from(vec![None::<i32>; len]))
}

fn null_int16(len: usize) -> ArrayRef {
    Arc::new(arrow_array::Int16Array::from(vec![None::<i16>; len]))
}

/// Return the named source column as an `Int16` array, casting from a
/// related integer type if necessary. Returns an all-null `Int16`
/// array when the column is absent — symmetric with [`int32_column`].
/// A cast failure is surfaced as `Error::data_loss` rather than
/// silently producing nulls. Used for JDBC `SMALLINT` columns
/// (`DEFERRABILITY`) whose server source ships as a wider integer.
fn int16_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    match column_index(batch.schema_ref(), name) {
        Some(idx) => {
            let col = batch.column(idx);
            if col.data_type() == &DataType::Int16 {
                Ok(Arc::clone(col))
            } else {
                arrow_cast::cast(col.as_ref(), &DataType::Int16).map_err(|e| {
                    Error::data_loss().message(format!(
                        "unexpected non-Int16 type for SHOW response column {name:?}: {e}"
                    ))
                })
            }
        }
        None => Ok(Arc::new(arrow_array::Int16Array::from(vec![
            None::<i16>;
            batch
                .num_rows()
        ]))),
    }
}

fn null_int8(len: usize) -> ArrayRef {
    Arc::new(arrow_array::Int8Array::from(vec![None::<i8>; len]))
}

/// Map a server-side `"true"` / `"false"` string column to JDBC's
/// `"YES"` / `"NO"` / `""` convention. Used for `IS_NULLABLE` —
/// when the server doesn't give a definitive boolean we fall back
/// to `""` (JDBC's "unknown" sentinel). For `IS_AUTO_INCREMENT` use
/// [`bool_string_to_yn_with_default`] with `"NO"` instead, since
/// Databricks tables don't support auto-increment and "unknown" is
/// strictly less informative than the known-correct answer.
fn bool_string_to_yn(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    bool_string_to_yn_with_default(batch, name, "")
}

/// Variant of [`bool_string_to_yn`] where the caller picks the fallback
/// value used when the server returns null, a non-`"true"`/`"false"`
/// string, or omits the source column entirely. Server-provided
/// `"true"` / `"false"` always win.
fn bool_string_to_yn_with_default(
    batch: &RecordBatch,
    name: &str,
    default: &str,
) -> Result<ArrayRef> {
    let n = batch.num_rows();
    let Some(idx) = column_index(batch.schema_ref(), name) else {
        return Ok(Arc::new(StringArray::from(vec![Some(default); n])));
    };
    let arr = batch.column(idx);
    let strs = arrow_cast::cast(arr.as_ref(), &DataType::Utf8).map_err(|e| {
        Error::data_loss().message(format!(
            "unexpected non-Utf8 type for SHOW response column {name:?}: {e}"
        ))
    })?;
    let strs = strs.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        Error::data_loss().message(format!("{name} cast did not yield StringArray"))
    })?;
    let labels: Vec<&str> = (0..n)
        .map(|i| {
            if strs.is_null(i) {
                default
            } else {
                match strs.value(i) {
                    "true" => "YES",
                    "false" => "NO",
                    _ => default,
                }
            }
        })
        .collect();
    Ok(Arc::new(StringArray::from(labels)))
}

// ─── Per-operation reshape ─────────────────────────────────────────

/// Reshape `SHOW CATALOGS` output. Server returns `catalog: Utf8`.
pub(crate) fn reshape_catalogs(batch: &RecordBatch) -> Result<RecordBatch> {
    let table_cat = utf8_column(batch, "catalog")?;
    RecordBatch::try_new(GET_CATALOGS_SCHEMA.clone(), vec![table_cat])
        .map_err(|e| Error::data_loss().message(format!("reshape catalogs: {e}")))
}

/// Reshape `SHOW SCHEMAS` output.
///
/// Server shape:
/// - `SHOW SCHEMAS IN <catalog>` returns `databaseName: Utf8` only.
/// - `SHOW SCHEMAS IN ALL CATALOGS` returns `databaseName, catalog`.
///
/// `requested_catalog` is the catalog argument passed by the caller;
/// when the server omits the `catalog` column (single-catalog form),
/// we fill `TABLE_CATALOG` from that value so the result is always
/// fully populated.
pub(crate) fn reshape_schemas(
    batch: &RecordBatch,
    requested_catalog: Option<&str>,
) -> Result<RecordBatch> {
    let table_schem = utf8_column(batch, "databaseName")?;
    let table_catalog = if column_index(batch.schema_ref(), "catalog").is_some() {
        utf8_column(batch, "catalog")?
    } else {
        constant_utf8(batch.num_rows(), requested_catalog)
    };
    RecordBatch::try_new(GET_SCHEMAS_SCHEMA.clone(), vec![table_schem, table_catalog])
        .map_err(|e| Error::data_loss().message(format!("reshape schemas: {e}")))
}

/// Reshape `SHOW TABLES IN CATALOG` output.
///
/// Server shape: `namespace, tableName, isTemporary, information,
/// catalogName, tableType, remarks`. Pre-DBR-16 versions may omit
/// `catalogName` / `tableType` / `remarks`; we synthesize defaults
/// when they're missing.
///
/// `requested_catalog` fills `TABLE_CAT` rows for server responses
/// that omit it (single-catalog form). `table_types`, when `Some` with
/// at least one entry, filters rows to those whose `TABLE_TYPE` appears
/// in the slice (matches JDBC `DatabaseMetaData.getTables` behaviour).
/// A `Some(&[])` (explicit empty slice) matches NO table types and
/// yields an empty result — it is NOT treated like `None`. See the
/// inline note in the filter block below (METADATA-035 conformance /
/// hiveserver2 #83, which reversed the earlier empty==null reading).
///
/// Catalog *matching* is not done here: a wildcard catalog is resolved
/// in `list_tables` by enumerating catalogs and issuing one scoped
/// `SHOW TABLES IN CATALOG <name>` per match, so every batch reaching
/// this function is already attributed to a known catalog (either the
/// `catalogName` column or `requested_catalog`).
pub(crate) fn reshape_tables(
    batch: &RecordBatch,
    requested_catalog: Option<&str>,
    table_types: Option<&[&str]>,
) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let table_cat = if column_index(batch.schema_ref(), "catalogName").is_some() {
        utf8_column(batch, "catalogName")?
    } else {
        constant_utf8(n, requested_catalog)
    };
    let table_schem = utf8_column(batch, "namespace")?;
    let table_name = utf8_column(batch, "tableName")?;
    let table_type = if column_index(batch.schema_ref(), "tableType").is_some() {
        utf8_column(batch, "tableType")?
    } else {
        // Old DBR fallback: best-effort default. Matches the JDBC
        // driver's behaviour when SHOW TABLES doesn't expose tableType.
        Arc::new(StringArray::from(vec![Some("TABLE"); n]))
    };
    // REMARKS is contractually nullable per the JDBC spec, but the Thrift
    // backend has historically emitted `""` for tables without a comment
    // and most string-handling consumers expect a `String`, not `null`.
    // `utf8_column_or_empty` covers both branches: column present with
    // null entries → `""`; column absent → all `""`.
    let remarks = utf8_column_or_empty(batch, "remarks")?;
    let reshaped = RecordBatch::try_new(
        GET_TABLES_SCHEMA.clone(),
        vec![
            table_cat,
            table_schem,
            table_name,
            table_type,
            remarks,
            null_utf8(n), // TYPE_CAT
            null_utf8(n), // TYPE_SCHEM
            null_utf8(n), // TYPE_NAME
            null_utf8(n), // SELF_REFERENCING_COL_NAME
            null_utf8(n), // REF_GENERATION
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape tables: {e}")))?;

    // Apply table_types filter, if requested. The JDBC `DatabaseMetaData.
    // getTables(...)` Javadoc specifies that `null` types returns all
    // types, but is silent on what an *explicit empty array* means.
    // Per the multi-language conformance spec (METADATA-035) and the C#
    // ADBC / JDBC drivers, an explicit *empty* array matches NO table
    // types — the result is empty, NOT equivalent to `null`. Only `None`
    // means "all types". This mirrors hiveserver2 #83, which reversed the
    // earlier #72 reading (empty == null) after establishing that
    // databricks-jdbc's `listTables` has always short-circuited an empty
    // array to "return nothing". An empty `Some(&[])` therefore flows
    // through `filter_tables_by_type`, whose `types.iter().any(..)` is
    // `false` for every row → an empty (correctly-typed) batch.
    let filtered = match table_types {
        None => reshaped,
        Some(types) => filter_tables_by_type(&reshaped, types)?,
    };
    // Sort to match the JDBC driver's `getTables` row order
    // (`TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME`). JDBC's
    // `MetadataResultSetBuilder.getTablesResult` does the same sort
    // with a comment marking it as the Thrift-mode order, so
    // consumers that compare kernel output against the existing
    // Thrift backend see matching row sequence.
    sort_tables(&filtered)
}

/// Lex-sort a JDBC-shape getTables batch by `TABLE_TYPE, TABLE_CAT,
/// TABLE_SCHEM, TABLE_NAME` to match JDBC / Thrift row ordering.
///
/// Exposed as [`sort_tables_batch`] so the per-catalog fan-out path in
/// `list_tables` can re-sort the *merged* result across catalogs (each
/// per-catalog `reshape_tables` sorts only its own rows).
pub(crate) fn sort_tables_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    sort_tables(batch)
}

/// Lex-sort a JDBC-shape getTables batch by `TABLE_TYPE, TABLE_CAT,
/// TABLE_SCHEM, TABLE_NAME` to match JDBC / Thrift row ordering.
fn sort_tables(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_rows() <= 1 {
        return Ok(batch.clone());
    }
    // Schema positions: 0=TABLE_CAT, 1=TABLE_SCHEM, 2=TABLE_NAME,
    // 3=TABLE_TYPE. Sort key order is TABLE_TYPE first.
    let sort_columns = [
        arrow_ord::sort::SortColumn {
            values: Arc::clone(batch.column(3)),
            options: None,
        },
        arrow_ord::sort::SortColumn {
            values: Arc::clone(batch.column(0)),
            options: None,
        },
        arrow_ord::sort::SortColumn {
            values: Arc::clone(batch.column(1)),
            options: None,
        },
        arrow_ord::sort::SortColumn {
            values: Arc::clone(batch.column(2)),
            options: None,
        },
    ];
    let indices = arrow_ord::sort::lexsort_to_indices(&sort_columns, None)
        .map_err(|e| Error::data_loss().message(format!("sort_tables lexsort: {e}")))?;
    let cols: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|c| {
            arrow_select::take::take(c.as_ref(), &indices, None)
                .map_err(|e| Error::data_loss().message(format!("sort_tables take: {e}")))
        })
        .collect();
    RecordBatch::try_new(batch.schema(), cols?)
        .map_err(|e| Error::data_loss().message(format!("sort_tables build: {e}")))
}

/// Reshape `SHOW COLUMNS IN CATALOG` output.
///
/// Server shape: `col_name, catalogName, namespace, tableName,
/// columnType, columnSize, decimalDigits, radix, isNullable, remarks,
/// ordinalPosition, isAutoIncrement, isGenerated`.
///
/// `requested_catalog` fills `TABLE_CAT` when the server omits it.
/// `DATA_TYPE` (JDBC integer type code) is derived from the
/// `columnType` string via [`databricks_type_to_xdbc`].
///
/// JDBC columns that have no server-side analogue
/// (`BUFFER_LENGTH`, `SQL_DATA_TYPE`, `SQL_DATETIME_SUB`,
/// `CHAR_OCTET_LENGTH`, `COLUMN_DEF`, `SCOPE_*`,
/// `SOURCE_DATA_TYPE`) are emitted as nullable nulls per JDBC spec.
pub(crate) fn reshape_columns(
    batch: &RecordBatch,
    requested_catalog: Option<&str>,
) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let table_cat = if column_index(batch.schema_ref(), "catalogName").is_some() {
        utf8_column(batch, "catalogName")?
    } else {
        constant_utf8(n, requested_catalog)
    };
    let table_schem = utf8_column(batch, "namespace")?;
    let table_name = utf8_column(batch, "tableName")?;
    let column_name = utf8_column(batch, "col_name")?;
    let column_type_arr = utf8_column(batch, "columnType")?;

    // DATA_TYPE: JDBC integer type code derived per row from columnType.
    let column_type_strs = column_type_arr.as_any().downcast_ref::<StringArray>();
    let data_type_arr: ArrayRef = match column_type_strs {
        Some(s) => Arc::new(Int32Array::from(
            (0..n)
                .map(|i| {
                    if s.is_null(i) {
                        // JDBC `OTHER` per `java.sql.Types`. Same
                        // fallback we use in `static_data.rs` for
                        // type_info.
                        1111
                    } else {
                        databricks_type_to_xdbc(s.value(i)) as i32
                    }
                })
                .collect::<Vec<_>>(),
        )),
        // columnType column missing entirely — fall back to OTHER.
        None => Arc::new(Int32Array::from(vec![1111_i32; n])),
    };

    // TYPE_NAME: the precise server-reported type string, preserved.
    let type_name = column_type_arr;

    let column_size = int32_column(batch, "columnSize")?;
    let decimal_digits = int32_column(batch, "decimalDigits")?;
    let num_prec_radix = int32_column(batch, "radix")?;
    // NULLABLE: JDBC integer code. SHOW COLUMNS returns "true"/"false"
    // as a string. Map: true → 1 (nullable), false → 0 (no nulls),
    // missing → 2 (unknown).
    let nullable = match column_index(batch.schema_ref(), "isNullable") {
        Some(idx) => {
            let arr = batch.column(idx);
            let strs = arrow_cast::cast(arr.as_ref(), &DataType::Utf8)
                .map_err(|e| Error::data_loss().message(format!("cast isNullable: {e}")))?;
            let strs = strs.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                Error::data_loss().message("isNullable cast did not yield StringArray")
            })?;
            let codes: Vec<i32> = (0..n)
                .map(|i| {
                    if strs.is_null(i) {
                        2
                    } else {
                        match strs.value(i) {
                            "true" => 1,
                            "false" => 0,
                            _ => 2,
                        }
                    }
                })
                .collect();
            Arc::new(Int32Array::from(codes)) as ArrayRef
        }
        None => Arc::new(Int32Array::from(vec![2_i32; n])),
    };
    // REMARKS: JDBC allows null, but the Thrift backend has historically
    // returned "" and string-handling consumers expect a `String`. Map
    // null → "" at the kernel layer.
    let remarks = utf8_column_or_empty(batch, "remarks")?;
    let ordinal_position = int32_column(batch, "ordinalPosition")?;
    // IS_NULLABLE: server emits "true"/"false"; JDBC contract is
    // "YES"/"NO"/"" (empty when unknown). The "unknown" sentinel is
    // appropriate here — the server doesn't always know nullability for
    // computed columns / views.
    let is_nullable_str = bool_string_to_yn(batch, "isNullable")?;
    // IS_AUTO_INCREMENT: Databricks tables don't support auto-increment
    // columns, so when the server omits this field (or returns null /
    // an unrecognised value) the correct JDBC answer is "NO", not the
    // "unknown" sentinel "". Server-provided "true" / "false" still
    // wins — if Databricks ever ships identity columns, this honours
    // them. Code comment + grep makes the assumption findable.
    let is_autoincrement = bool_string_to_yn_with_default(batch, "isAutoIncrement", "NO")?;

    RecordBatch::try_new(
        GET_COLUMNS_SCHEMA.clone(),
        vec![
            table_cat,
            table_schem,
            table_name,
            column_name,
            data_type_arr,
            type_name,
            column_size,
            null_int8(n), // BUFFER_LENGTH (Thrift wire type: TINYINT)
            decimal_digits,
            num_prec_radix,
            nullable,
            remarks,
            null_utf8(n),  // COLUMN_DEF
            null_int32(n), // SQL_DATA_TYPE
            null_int32(n), // SQL_DATETIME_SUB
            null_int32(n), // CHAR_OCTET_LENGTH
            ordinal_position,
            is_nullable_str,
            null_utf8(n),  // SCOPE_CATALOG
            null_utf8(n),  // SCOPE_SCHEMA
            null_utf8(n),  // SCOPE_TABLE
            null_int16(n), // SOURCE_DATA_TYPE (Thrift wire type: SMALLINT)
            is_autoincrement,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape columns: {e}")))
}

/// Reshape the `build_get_procedures` result
/// (`routine_catalog, routine_schema, routine_name, comment, specific_name`)
/// into the JDBC `getProcedures` shape.
///
/// Columns 4-6 (`RESERVED*`) are spec-reserved and emitted null.
/// `PROCEDURE_TYPE` is constant `procedureNoResult` (1) — Databricks stored
/// procedures do not return a result set through this metadata call. (JDBC:
/// `procedureResultUnknown`=0, `procedureNoResult`=1, `procedureReturnsResult`=2.)
pub(crate) fn reshape_procedures(batch: &RecordBatch) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let procedure_cat = utf8_column(batch, "routine_catalog")?;
    let procedure_schem = utf8_column(batch, "routine_schema")?;
    let procedure_name = utf8_column(batch, "routine_name")?;
    let remarks = utf8_column(batch, "comment")?;
    let specific_name = utf8_column(batch, "specific_name")?;
    let procedure_type = Arc::new(Int16Array::from(vec![Some(1_i16); n])) as ArrayRef;
    RecordBatch::try_new(
        GET_PROCEDURES_SCHEMA.clone(),
        vec![
            procedure_cat,
            procedure_schem,
            procedure_name,
            null_utf8(n), // RESERVED1
            null_utf8(n), // RESERVED2
            null_utf8(n), // RESERVED3
            remarks,
            procedure_type,
            specific_name,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape procedures: {e}")))
}

/// Reshape the raw `SHOW FUNCTIONS` result into the JDBC `getFunctions` shape.
pub(crate) fn reshape_functions(
    batch: &RecordBatch,
    resolved_catalog: &str,
) -> Result<RecordBatch> {
    let function_cat = constant_utf8(batch.num_rows(), Some(resolved_catalog));
    let function_schem = utf8_column(batch, "namespace")?;
    let function_name = if column_index(batch.schema_ref(), "functionName").is_some() {
        utf8_column(batch, "functionName")?
    } else {
        // Classic SHOW FUNCTIONS exposes a single column named `function`.
        utf8_column(batch, "function")?
    };
    let remarks = utf8_column(batch, "remarks")?;
    let function_type = int16_column(batch, "functionType")?;
    let specific_name = utf8_column(batch, "specificName")?;
    RecordBatch::try_new(
        GET_FUNCTIONS_SCHEMA.clone(),
        vec![
            function_cat,
            function_schem,
            function_name,
            remarks,
            function_type,
            specific_name,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape functions: {e}")))
}

/// Reshape the `build_get_procedure_columns` result (raw
/// `information_schema.parameters` columns) into the JDBC
/// `getProcedureColumns` shape.
///
/// - `COLUMN_TYPE` is derived from `parameter_mode` / `is_result`:
///   `IN`→procedureColumnIn(1), `INOUT`→procedureColumnInOut(2),
///   `OUT`→procedureColumnOut(4), a return value (`is_result = 'YES'`)→
///   procedureColumnReturn(5), else procedureColumnUnknown(0).
/// - `DATA_TYPE` is the JDBC type code from `databricks_type_to_xdbc(data_type)`
///   (the same per-row mapper `getColumns` uses); `TYPE_NAME` is the raw
///   `data_type` string.
/// - `PRECISION` / `SCALE` / `RADIX` / `CHAR_OCTET_LENGTH` pass the
///   `information_schema` numeric metadata through (they arrive as strings and
///   are cast). `NULLABLE` is constant `procedureNullableUnknown` (2) —
///   `information_schema.parameters` carries no nullability for parameters.
/// - `LENGTH` / `SQL_DATA_TYPE` / `SQL_DATETIME_SUB` / `COLUMN_DEF` have no
///   source and are null; the ODBC driver derives its own equivalents.
/// - `IS_NULLABLE` is the empty-string "unknown" sentinel.
pub(crate) fn reshape_procedure_columns(batch: &RecordBatch) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let procedure_cat = utf8_column(batch, "specific_catalog")?;
    let procedure_schem = utf8_column(batch, "specific_schema")?;
    let procedure_name = utf8_column(batch, "specific_name")?;
    let column_name = utf8_column(batch, "parameter_name")?;
    let specific_name = utf8_column(batch, "specific_name")?;

    // COLUMN_TYPE from parameter_mode / is_result.
    let mode_arr = utf8_column(batch, "parameter_mode")?;
    let modes = mode_arr.as_any().downcast_ref::<StringArray>();
    let is_result_arr = utf8_column(batch, "is_result")?;
    let is_results = is_result_arr.as_any().downcast_ref::<StringArray>();
    let column_type = Arc::new(Int16Array::from(
        (0..n)
            .map(|i| {
                let is_return = is_results
                    .map(|a| !a.is_null(i) && a.value(i).eq_ignore_ascii_case("YES"))
                    .unwrap_or(false);
                if is_return {
                    return 5_i16; // procedureColumnReturn
                }
                match modes.map(|a| if a.is_null(i) { "" } else { a.value(i) }) {
                    Some("IN") => 1,    // procedureColumnIn
                    Some("INOUT") => 2, // procedureColumnInOut
                    Some("OUT") => 4,   // procedureColumnOut
                    _ => 0,             // procedureColumnUnknown
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef;

    // DATA_TYPE from the data_type name; TYPE_NAME is the raw string.
    let type_name_arr = utf8_column(batch, "data_type")?;
    let type_name_strs = type_name_arr.as_any().downcast_ref::<StringArray>();
    let data_type = Arc::new(Int32Array::from(match type_name_strs {
        Some(s) => (0..n)
            .map(|i| {
                if s.is_null(i) {
                    1111 // java.sql.Types.OTHER
                } else {
                    databricks_type_to_xdbc(s.value(i)) as i32
                }
            })
            .collect::<Vec<_>>(),
        None => vec![1111_i32; n],
    })) as ArrayRef;

    let precision = int32_column(batch, "numeric_precision")?;
    let scale = int16_column(batch, "numeric_scale")?;
    // RADIX is derived from the type, not passed through from
    // `information_schema.numeric_precision_radix`: that column is
    // inconsistent for integers (it reports 2 for some INT parameters and
    // NULL for others). JDBC `getProcedureColumns` reports RADIX only for
    // types where it is meaningful — 10 for exact decimals, 2 for the
    // approximate (FLOAT / REAL / DOUBLE) types — and NULL otherwise
    // (integers included), matching the reference driver.
    let radix = Arc::new(Int16Array::from(match type_name_strs {
        Some(s) => (0..n)
            .map(|i| {
                if s.is_null(i) {
                    return None;
                }
                let t = s.value(i).to_ascii_uppercase();
                // Strip any parameterisation, e.g. "DECIMAL(10,2)" → "DECIMAL".
                let base = t.split(['(', ' ']).next().unwrap_or(&t);
                match base {
                    "DECIMAL" | "DEC" | "NUMERIC" => Some(10),
                    "FLOAT" | "REAL" | "DOUBLE" => Some(2),
                    _ => None,
                }
            })
            .collect::<Vec<Option<i16>>>(),
        None => vec![None; n],
    })) as ArrayRef;
    let char_octet_length = int32_column(batch, "character_octet_length")?;
    let ordinal_position = int32_column(batch, "ordinal_position")?;
    let remarks = utf8_column(batch, "comment")?;
    // NULLABLE: procedureNullableUnknown (2) — information_schema.parameters
    // does not expose parameter nullability.
    let nullable = Arc::new(Int32Array::from(vec![2_i32; n])) as ArrayRef;
    // IS_NULLABLE: JDBC allows "" for unknown.
    let is_nullable = Arc::new(StringArray::from(vec![Some(""); n])) as ArrayRef;

    RecordBatch::try_new(
        GET_PROCEDURE_COLUMNS_SCHEMA.clone(),
        vec![
            procedure_cat,
            procedure_schem,
            procedure_name,
            column_name,
            column_type,
            data_type,
            type_name_arr,
            precision,
            null_int32(n), // LENGTH (no information_schema source)
            scale,
            radix,
            nullable,
            remarks,
            null_utf8(n),  // COLUMN_DEF
            null_int32(n), // SQL_DATA_TYPE
            null_int32(n), // SQL_DATETIME_SUB
            char_octet_length,
            ordinal_position,
            is_nullable,
            specific_name,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape procedure_columns: {e}")))
}

/// Filter a JDBC-shape getTables batch by `TABLE_TYPE`. Used to apply
/// the client-side `table_types` argument JDBC supports but the
/// server's `SHOW TABLES` doesn't honour.
fn filter_tables_by_type(batch: &RecordBatch, types: &[&str]) -> Result<RecordBatch> {
    let type_idx = batch
        .schema_ref()
        .fields()
        .iter()
        .position(|f| f.name() == "TABLE_TYPE")
        .ok_or_else(|| Error::data_loss().message("filter_tables_by_type: missing TABLE_TYPE"))?;
    let types_arr = batch
        .column(type_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::data_loss().message("TABLE_TYPE is not Utf8"))?;
    let keep: Vec<bool> = (0..batch.num_rows())
        .map(|i| {
            if types_arr.is_null(i) {
                false
            } else {
                let t = types_arr.value(i);
                // Case-insensitive match: callers may pass `"view"` /
                // `"Table"` (JDBC `getTables` allows any case) while the
                // warehouse reports canonical uppercase (`TABLE` / `VIEW`
                // / `SYSTEM_TABLE` / `METRIC_VIEW`). An exact match would
                // silently drop all rows for a mixed-case request.
                types.iter().any(|req| req.eq_ignore_ascii_case(t))
            }
        })
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| Error::data_loss().message(format!("filter_tables_by_type: {e}")))
}

/// Apply the JDBC filters that the native metadata backend does not reliably
/// enforce, then restore the established Thrift row order.
pub(crate) fn process_native_tables(
    batch: &RecordBatch,
    catalog: Option<&str>,
    table_types: Option<&[&str]>,
) -> Result<RecordBatch> {
    let catalog_filtered = if let Some(catalog) = catalog {
        let catalog_idx = column_index(batch.schema_ref(), "TABLE_CAT").ok_or_else(|| {
            Error::data_loss().message("process_native_tables: missing TABLE_CAT")
        })?;
        let catalogs = batch
            .column(catalog_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::data_loss().message("TABLE_CAT is not Utf8"))?;
        let keep = arrow_array::BooleanArray::from(
            (0..batch.num_rows())
                .map(|i| !catalogs.is_null(i) && catalogs.value(i).eq_ignore_ascii_case(catalog))
                .collect::<Vec<_>>(),
        );
        arrow_select::filter::filter_record_batch(batch, &keep).map_err(|e| {
            Error::data_loss().message(format!("process_native_tables catalog filter: {e}"))
        })?
    } else {
        batch.clone()
    };

    let type_filtered = match table_types {
        Some(types) => filter_tables_by_type(&catalog_filtered, types)?,
        None => catalog_filtered,
    };
    sort_tables(&type_filtered)
}

/// Native GetCrossReference narrows only the foreign side. Apply any parent
/// identifiers supplied by the caller to the already canonical Thrift shape.
pub(crate) fn filter_cross_reference_by_parent(
    batch: &RecordBatch,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: Option<&str>,
) -> Result<RecordBatch> {
    if catalog.is_none() && schema.is_none() && table.is_none() {
        return Ok(batch.clone());
    }

    let string_column = |name: &str| -> Result<&StringArray> {
        let idx = column_index(batch.schema_ref(), name).ok_or_else(|| {
            Error::data_loss().message(format!("filter_cross_reference: missing {name}"))
        })?;
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::data_loss().message(format!("{name} is not Utf8")))
    };
    let catalogs = string_column("PKTABLE_CAT")?;
    let schemas = string_column("PKTABLE_SCHEM")?;
    let tables = string_column("PKTABLE_NAME")?;
    let matches = |values: &StringArray, i: usize, expected: Option<&str>| {
        expected.is_none_or(|expected| {
            !values.is_null(i) && values.value(i).eq_ignore_ascii_case(expected)
        })
    };
    let keep = arrow_array::BooleanArray::from(
        (0..batch.num_rows())
            .map(|i| {
                matches(catalogs, i, catalog)
                    && matches(schemas, i, schema)
                    && matches(tables, i, table)
            })
            .collect::<Vec<_>>(),
    );
    arrow_select::filter::filter_record_batch(batch, &keep)
        .map_err(|e| Error::data_loss().message(format!("filter_cross_reference by parent: {e}")))
}

/// Reshape Databricks `SHOW KEYS` output into the JDBC
/// `getPrimaryKeys` shape.
///
/// Server shape (`SHOW KEYS IN ... TABLE ...`): `catalogName,
/// namespace, tableName, col_name, keySeq, constraintName,
/// constraintType, parentCatalogName, parentNamespace,
/// parentTableName, parentColName, updateRule, deleteRule,
/// deferrability`. For a primary-key lookup the server returns only
/// the table's own primary-key rows, so this is a pure column
/// rename + projection onto the six JDBC columns — no filtering and
/// no value synthesis.
pub(crate) fn reshape_primary_keys(batch: &RecordBatch) -> Result<RecordBatch> {
    let table_cat = utf8_column(batch, "catalogName")?;
    let table_schem = utf8_column(batch, "namespace")?;
    let table_name = utf8_column(batch, "tableName")?;
    let column_name = utf8_column(batch, "col_name")?;
    let key_seq = int32_column(batch, "keySeq")?;
    let pk_name = utf8_column(batch, "constraintName")?;
    RecordBatch::try_new(
        GET_PRIMARY_KEYS_SCHEMA.clone(),
        vec![
            table_cat,
            table_schem,
            table_name,
            column_name,
            key_seq,
            pk_name,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape primary_keys: {e}")))
}

/// Reshape Databricks `SHOW FOREIGN KEYS` output into the JDBC
/// `getCrossReference` shape.
///
/// Server shape (same columns as `SHOW KEYS`): the foreign
/// (FK-bearing) side is in `catalogName / namespace / tableName /
/// col_name`, and the referenced primary side is in the `parent*`
/// columns. This is a pure column rename + projection onto the 14
/// JDBC columns. `UPDATE_RULE`, `DELETE_RULE`, and `PK_NAME` have no
/// source in the server response — they are emitted as typed nulls
/// rather than synthesizing a value (e.g. the kernel does not fabricate
/// a JDBC `importedKeyNoAction` rule code).
pub(crate) fn reshape_foreign_keys(batch: &RecordBatch) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let pktable_cat = utf8_column(batch, "parentCatalogName")?;
    let pktable_schem = utf8_column(batch, "parentNamespace")?;
    let pktable_name = utf8_column(batch, "parentTableName")?;
    let pkcolumn_name = utf8_column(batch, "parentColName")?;
    let fktable_cat = utf8_column(batch, "catalogName")?;
    let fktable_schem = utf8_column(batch, "namespace")?;
    let fktable_name = utf8_column(batch, "tableName")?;
    let fkcolumn_name = utf8_column(batch, "col_name")?;
    let key_seq = int32_column(batch, "keySeq")?;
    let fk_name = utf8_column(batch, "constraintName")?;
    let deferrability = int16_column(batch, "deferrability")?;
    RecordBatch::try_new(
        GET_CROSS_REFERENCE_SCHEMA.clone(),
        vec![
            pktable_cat,
            pktable_schem,
            pktable_name,
            pkcolumn_name,
            fktable_cat,
            fktable_schem,
            fktable_name,
            fkcolumn_name,
            key_seq,
            null_int16(n), // UPDATE_RULE — no server source; not synthesized
            null_int16(n), // DELETE_RULE — no server source; not synthesized
            fk_name,
            null_utf8(n), // PK_NAME — no server source; not synthesized
            deferrability,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape foreign_keys: {e}")))
}

// ─── Streaming reshape reader ──────────────────────────────────────

/// Per-batch reshape closure used by [`ThriftReshapingReader`]. Held
/// as a boxed `Fn` so each `list_*` method can capture its own
/// arguments (requested catalog, table-type filter, …) into the
/// closure without leaking them into the reader's type.
pub(crate) type ReshapeFn = dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync;

/// Streaming JDBC-shape reshape over an upstream `ResultReader`.
///
/// `Metadata::list_*` wires its raw transport reader into one of
/// these. Each call to `next_batch` pulls one server-shape batch from
/// the upstream and applies the chosen reshape closure before
/// returning. Schema is fixed at construction (the JDBC-shape schema
/// for the operation) so callers see consistent column descriptors
/// even across an empty stream.
pub(crate) struct ThriftReshapingReader {
    upstream: Box<dyn ResultReader + Send>,
    schema: SchemaRef,
    reshape: Box<ReshapeFn>,
}

impl ThriftReshapingReader {
    pub(crate) fn new(
        upstream: Box<dyn ResultReader + Send>,
        schema: SchemaRef,
        reshape: Box<ReshapeFn>,
    ) -> Self {
        Self {
            upstream,
            schema,
            reshape,
        }
    }
}

#[async_trait::async_trait]
impl ResultReader for ThriftReshapingReader {
    async fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.upstream.next_batch().await? {
            Some(b) => Ok(Some((self.reshape)(&b)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, RecordBatch};

    // ─── is_object_not_found tests ─────────────────────────────────

    fn err(state: Option<&str>, message: &str) -> Error {
        let mut e = Error::sql_error().message(message);
        if let Some(s) = state {
            e = e.with_sql_state(s);
        }
        e
    }

    #[test]
    fn is_object_not_found_matches_sqlstate_42704() {
        assert!(is_object_not_found(&err(Some("42704"), "anything goes")));
    }

    #[test]
    fn is_object_not_found_matches_known_message_substrings() {
        for substring in [
            "NO_SUCH_CATALOG_EXCEPTION",
            "TABLE_OR_VIEW_NOT_FOUND",
            "SCHEMA_NOT_FOUND",
            "INVALID_PARAMETER_VALUE",
        ] {
            let msg = format!("[{substring}] details follow");
            assert!(
                is_object_not_found(&err(None, &msg)),
                "expected {substring} to be detected"
            );
        }
    }

    #[test]
    fn is_object_not_found_rejects_unrelated_errors() {
        // Authn error — different SQL state, different message.
        let auth_err = err(Some("28000"), "Invalid credentials");
        assert!(!is_object_not_found(&auth_err));
        // Runtime error with no SQL state and a non-matching message.
        let runtime_err = err(None, "Server-internal failure");
        assert!(!is_object_not_found(&runtime_err));
    }

    // ─── reshape tests ─────────────────────────────────────────────

    fn server_catalogs(rows: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(rows.iter().map(|s| Some(*s)).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn catalogs_rename_only() {
        let b = server_catalogs(&["main", "samples"]);
        let out = reshape_catalogs(&b).unwrap();
        assert_eq!(out.schema().field(0).name(), "TABLE_CAT");
        assert_eq!(out.num_rows(), 2);
        let arr = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "main");
        assert_eq!(arr.value(1), "samples");
    }

    #[test]
    fn schemas_synthesises_table_catalog_from_arg() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "databaseName",
            DataType::Utf8,
            false,
        )]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["default", "public"]))],
        )
        .unwrap();
        let out = reshape_schemas(&b, Some("main")).unwrap();
        assert_eq!(out.schema().field(0).name(), "TABLE_SCHEM");
        assert_eq!(out.schema().field(1).name(), "TABLE_CATALOG");
        let cat = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cat.value(0), "main");
        assert_eq!(cat.value(1), "main");
    }

    #[test]
    fn schemas_uses_server_catalog_column_when_present() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("databaseName", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, false),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["s1", "s2"])),
                Arc::new(StringArray::from(vec!["c1", "c2"])),
            ],
        )
        .unwrap();
        let out = reshape_schemas(&b, Some("ignored")).unwrap();
        let cat = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cat.value(0), "c1");
        assert_eq!(cat.value(1), "c2");
    }

    fn server_tables_full() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
            Field::new("information", DataType::Utf8, true),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, false),
            Field::new("remarks", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["s1", "s1", "s2"])),
                Arc::new(StringArray::from(vec!["t1", "v1", "t2"])),
                Arc::new(BooleanArray::from(vec![false, false, false])),
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some("")])),
                Arc::new(StringArray::from(vec!["main", "main", "main"])),
                Arc::new(StringArray::from(vec!["TABLE", "VIEW", "TABLE"])),
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some("")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn tables_renames_and_pads_with_nulls() {
        let b = server_tables_full();
        let out = reshape_tables(&b, Some("main"), None).unwrap();
        assert_eq!(out.schema().fields().len(), 10);
        assert_eq!(out.schema().field(0).name(), "TABLE_CAT");
        assert_eq!(out.schema().field(3).name(), "TABLE_TYPE");
        assert_eq!(out.schema().field(5).name(), "TYPE_CAT");
        // synthesized columns null
        let type_cat = out
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(type_cat.is_null(0));
    }

    #[test]
    fn tables_filter_by_type_keeps_only_matching_rows() {
        let b = server_tables_full();
        let out = reshape_tables(&b, Some("main"), Some(&["TABLE"])).unwrap();
        assert_eq!(out.num_rows(), 2);
        let table_name = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table_name.value(0), "t1");
        assert_eq!(table_name.value(1), "t2");
    }

    #[test]
    fn tables_empty_types_matches_none() {
        // Per the JDBC/ADBC contract (spec METADATA-035 / hiveserver2 #83),
        // an explicit empty `table_types` slice matches NO table types —
        // the result is empty, NOT equivalent to `None`. Contrast with
        // `tables_none_types_passes_all`, where `None` returns all rows.
        let b = server_tables_full();
        let out = reshape_tables(&b, Some("main"), Some(&[])).unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn tables_none_types_passes_all() {
        // The all-types path — distinct from the explicit-empty path
        // above (which now matches none). Locks the contract so future
        // refactors don't quietly re-merge the two.
        let b = server_tables_full();
        let out = reshape_tables(&b, Some("main"), None).unwrap();
        assert_eq!(out.num_rows(), 3);
    }

    #[test]
    fn native_tables_apply_case_insensitive_catalog_and_type_filters() {
        let native = reshape_tables(&server_tables_full(), Some("main"), None).unwrap();

        let views = process_native_tables(&native, Some("MAIN"), Some(&["view"])).unwrap();
        assert_eq!(views.num_rows(), 1);
        assert_eq!(
            views
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "v1"
        );
        assert_eq!(
            process_native_tables(&native, Some("other"), None)
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn tables_fallback_table_type_when_missing() {
        // Old DBR: SHOW TABLES returns just namespace, tableName, isTemporary
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["s"])),
                Arc::new(StringArray::from(vec!["t"])),
                Arc::new(BooleanArray::from(vec![false])),
            ],
        )
        .unwrap();
        let out = reshape_tables(&b, Some("c"), None).unwrap();
        let type_arr = out
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(type_arr.value(0), "TABLE");
        let cat_arr = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cat_arr.value(0), "c");
    }

    /// Multi-catalog `SHOW TABLES IN ALL CATALOGS` shape, mirroring the
    /// issue-#129 repro: two catalogs differing only by `_` vs `-`.
    fn server_tables_multi_catalog() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
            Field::new("information", DataType::Utf8, true),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, true),
            Field::new("remarks", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["s", "s", "s"])),
                Arc::new(StringArray::from(vec!["t1", "t2", "t3"])),
                Arc::new(BooleanArray::from(vec![false, false, false])),
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some("")])),
                Arc::new(StringArray::from(vec![
                    "comparator_tests",
                    "comparator-tests",
                    "unrelated",
                ])),
                Arc::new(StringArray::from(vec!["TABLE", "TABLE", "TABLE"])),
                Arc::new(StringArray::from(vec![Some(""), Some(""), Some("")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn tables_multi_catalog_reshape_preserves_all_rows() {
        // A cross-catalog `SHOW TABLES IN ALL CATALOGS` response reshapes
        // all rows and attributes each to its own `catalogName` (catalog
        // *filtering* is now done by the fan-out in `list_tables`, not
        // here).
        let b = server_tables_multi_catalog();
        let out = reshape_tables(&b, None, None).unwrap();
        assert_eq!(out.num_rows(), 3);
        let cats = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..out.num_rows()).map(|i| cats.value(i)).collect();
        assert!(got.contains(&"comparator_tests"));
        assert!(got.contains(&"comparator-tests"));
        assert!(got.contains(&"unrelated"));
    }

    #[test]
    fn sort_tables_batch_merges_across_catalogs_in_jdbc_order() {
        // The fan-out concatenates per-catalog batches then re-sorts the
        // merged set by `TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME`.
        // Feed rows deliberately out of order and assert the JDBC order.
        let cat_b = reshape_tables(
            &single_row_server_table("b_catalog", "s", "t", "TABLE"),
            Some("b_catalog"),
            None,
        )
        .unwrap();
        let cat_a = reshape_tables(
            &single_row_server_table("a_catalog", "s", "t", "TABLE"),
            Some("a_catalog"),
            None,
        )
        .unwrap();
        let merged =
            arrow_select::concat::concat_batches(&GET_TABLES_SCHEMA, &[cat_b, cat_a]).unwrap();
        let sorted = sort_tables_batch(&merged).unwrap();
        let cats = sorted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Same TABLE_TYPE, so TABLE_CAT breaks the tie: a_catalog first.
        assert_eq!(cats.value(0), "a_catalog");
        assert_eq!(cats.value(1), "b_catalog");
    }

    /// Single-row `SHOW TABLES IN CATALOG` server shape for fan-out tests.
    fn single_row_server_table(
        catalog: &str,
        namespace: &str,
        table: &str,
        table_type: &str,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![namespace.to_owned()])),
                Arc::new(StringArray::from(vec![table.to_owned()])),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(StringArray::from(vec![catalog.to_owned()])),
                Arc::new(StringArray::from(vec![Some(table_type.to_owned())])),
            ],
        )
        .unwrap()
    }

    fn server_columns_full() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
            Field::new("columnSize", DataType::Int32, true),
            Field::new("decimalDigits", DataType::Int32, true),
            Field::new("radix", DataType::Int32, true),
            Field::new("isNullable", DataType::Utf8, true),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("ordinalPosition", DataType::Int32, true),
            Field::new("isAutoIncrement", DataType::Utf8, true),
            Field::new("isGenerated", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id", "name"])),
                Arc::new(StringArray::from(vec!["main", "main"])),
                Arc::new(StringArray::from(vec!["s", "s"])),
                Arc::new(StringArray::from(vec!["t", "t"])),
                Arc::new(StringArray::from(vec!["INT", "STRING"])),
                Arc::new(Int32Array::from(vec![Some(4), None])),
                Arc::new(Int32Array::from(vec![Some(0), None])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
                Arc::new(StringArray::from(vec![Some("false"), Some("true")])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("false"), Some("false")])),
                Arc::new(StringArray::from(vec![Some("false"), Some("false")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn columns_renames_and_derives_data_type() {
        let b = server_columns_full();
        let out = reshape_columns(&b, Some("main")).unwrap();
        assert_eq!(out.schema().fields().len(), 23);
        assert_eq!(out.schema().field(0).name(), "TABLE_CAT");
        assert_eq!(out.schema().field(4).name(), "DATA_TYPE");
        assert_eq!(out.schema().field(5).name(), "TYPE_NAME");
        let data_type = out.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
        // INT → java.sql.Types.INTEGER = 4
        assert_eq!(data_type.value(0), 4);
        // STRING → java.sql.Types.VARCHAR = 12 (matches Thrift's
        // released `getColumns` contract; prior kernel value was
        // LONGVARCHAR = -1, which diverged from Thrift in 3,242
        // cells of the comparator run).
        assert_eq!(data_type.value(1), 12);
    }

    #[test]
    fn columns_maps_is_nullable_to_jdbc_codes() {
        let b = server_columns_full();
        let out = reshape_columns(&b, Some("main")).unwrap();
        let nullable = out
            .column(10)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(nullable.value(0), 0); // "false" → 0 (no nulls)
        assert_eq!(nullable.value(1), 1); // "true"  → 1 (nullable)
        let is_nullable = out
            .column(17)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(is_nullable.value(0), "NO");
        assert_eq!(is_nullable.value(1), "YES");
    }

    #[test]
    fn columns_remarks_null_becomes_empty_string() {
        // `server_columns_full` builds REMARKS as `[None, None]`. After
        // reshape, both rows must be `""` (not null), matching Thrift's
        // historical behaviour and downstream consumers that expect a
        // `String`. REMARKS is JDBC column index 11.
        let b = server_columns_full();
        let out = reshape_columns(&b, Some("main")).unwrap();
        let remarks = out
            .column(11)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!remarks.is_null(0), "REMARKS row 0 should be '', not null");
        assert!(!remarks.is_null(1), "REMARKS row 1 should be '', not null");
        assert_eq!(remarks.value(0), "");
        assert_eq!(remarks.value(1), "");
    }

    #[test]
    fn columns_is_auto_increment_defaults_to_no() {
        // Databricks doesn't support auto-increment, so when the server
        // returns "false" (or omits the column entirely) the JDBC value
        // is "NO". IS_AUTO_INCREMENT is JDBC column index 22.
        let b = server_columns_full();
        let out = reshape_columns(&b, Some("main")).unwrap();
        let is_ai = out
            .column(22)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(is_ai.value(0), "NO");
        assert_eq!(is_ai.value(1), "NO");
    }

    #[test]
    fn columns_is_auto_increment_honors_server_true() {
        // The `"NO"` default in `bool_string_to_yn_with_default` is a
        // fallback for the server-silent case; if Databricks ever ships
        // identity columns and the server emits `"true"`, the kernel
        // honours it. Synthetic input with explicit `"true"` values.
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
            Field::new("isNullable", DataType::Utf8, true),
            Field::new("ordinalPosition", DataType::Int32, true),
            Field::new("isAutoIncrement", DataType::Utf8, true),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["s"])),
                Arc::new(StringArray::from(vec!["t"])),
                Arc::new(StringArray::from(vec!["INT"])),
                Arc::new(StringArray::from(vec![Some("false")])),
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("true")])),
            ],
        )
        .unwrap();
        let out = reshape_columns(&b, Some("main")).unwrap();
        let is_ai = out
            .column(22)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(is_ai.value(0), "YES");
    }

    #[test]
    fn columns_is_auto_increment_no_when_column_missing() {
        // If the server doesn't include `isAutoIncrement` at all (e.g.
        // an older DBR's SHOW COLUMNS variant), the kernel still returns
        // "NO" rather than "" — same domain reasoning.
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
            Field::new("isNullable", DataType::Utf8, true),
            Field::new("ordinalPosition", DataType::Int32, true),
            // isAutoIncrement intentionally absent
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["s"])),
                Arc::new(StringArray::from(vec!["t"])),
                Arc::new(StringArray::from(vec!["INT"])),
                Arc::new(StringArray::from(vec![Some("false")])),
                Arc::new(Int32Array::from(vec![Some(1)])),
            ],
        )
        .unwrap();
        let out = reshape_columns(&b, Some("main")).unwrap();
        let is_ai = out
            .column(22)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(is_ai.value(0), "NO");
    }

    // ─── primary / foreign keys reshape ────────────────────────────

    fn server_show_keys() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("col_name", DataType::Utf8, false),
            Field::new("keySeq", DataType::Int32, true),
            Field::new("constraintName", DataType::Utf8, true),
            Field::new("constraintType", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["main", "main"])),
                Arc::new(StringArray::from(vec!["s", "s"])),
                Arc::new(StringArray::from(vec!["t", "t"])),
                Arc::new(StringArray::from(vec!["id", "code"])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec!["pk_t", "pk_t"])),
                Arc::new(StringArray::from(vec!["PRIMARY", "PRIMARY"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn primary_keys_renames_to_jdbc_columns() {
        let out = reshape_primary_keys(&server_show_keys()).unwrap();
        assert_eq!(
            out.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "KEY_SEQ",
                "PK_NAME",
            ]
        );
        assert_eq!(out.num_rows(), 2);
        let key_seq = out.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(key_seq.value(0), 1);
        assert_eq!(key_seq.value(1), 2);
        let pk_name = out
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(pk_name.value(0), "pk_t");
    }

    fn server_show_foreign_keys() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("col_name", DataType::Utf8, false),
            Field::new("keySeq", DataType::Int32, true),
            Field::new("constraintName", DataType::Utf8, true),
            Field::new("parentCatalogName", DataType::Utf8, true),
            Field::new("parentNamespace", DataType::Utf8, true),
            Field::new("parentTableName", DataType::Utf8, true),
            Field::new("parentColName", DataType::Utf8, true),
            Field::new("deferrability", DataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["s"])),
                Arc::new(StringArray::from(vec!["child"])),
                Arc::new(StringArray::from(vec!["pid"])),
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec!["fk_child"])),
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["s"])),
                Arc::new(StringArray::from(vec!["parent"])),
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(Int32Array::from(vec![Some(7)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn foreign_keys_renames_to_jdbc_columns_and_nulls_unsourced() {
        let out = reshape_foreign_keys(&server_show_foreign_keys()).unwrap();
        assert_eq!(
            out.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
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
        assert_eq!(out.num_rows(), 1);
        // Parent (PK) side and foreign (FK) side land in the right columns.
        let pk_table = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(pk_table.value(0), "parent");
        let fk_table = out
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(fk_table.value(0), "child");
        // DEFERRABILITY is cast through to SMALLINT (Int16).
        let deferr = out
            .column(13)
            .as_any()
            .downcast_ref::<arrow_array::Int16Array>()
            .unwrap();
        assert_eq!(deferr.value(0), 7);
        // No server source → typed nulls, not synthesized values.
        assert!(out.column(9).is_null(0)); // UPDATE_RULE
        assert!(out.column(10).is_null(0)); // DELETE_RULE
        assert!(out.column(12).is_null(0)); // PK_NAME
    }

    #[test]
    fn native_cross_reference_filters_parent_case_insensitively() {
        let native = reshape_foreign_keys(&server_show_foreign_keys()).unwrap();
        assert_eq!(
            filter_cross_reference_by_parent(&native, Some("MAIN"), Some("S"), Some("PARENT"))
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            filter_cross_reference_by_parent(&native, None, None, Some("other"))
                .unwrap()
                .num_rows(),
            0
        );
    }
    // ─── filter_tables_by_type tests ───────────────────────────────

    fn tables_with_types(types: &[&str]) -> RecordBatch {
        // Minimal batch carrying only the TABLE_TYPE column that
        // `filter_tables_by_type` inspects.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "TABLE_TYPE",
            DataType::Utf8,
            true,
        )]));
        let arr = StringArray::from(types.iter().map(|s| Some(*s)).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn kept_types(batch: &RecordBatch) -> Vec<String> {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| arr.value(i).to_owned())
            .collect()
    }

    #[test]
    fn filter_tables_by_type_is_case_insensitive() {
        // Warehouse reports canonical uppercase; a lowercase / mixed-case
        // request must still match (JDBC getTables allows any case).
        let batch = tables_with_types(&["TABLE", "VIEW", "SYSTEM_TABLE"]);
        let out = filter_tables_by_type(&batch, &["view", "Table"]).unwrap();
        assert_eq!(kept_types(&out), vec!["TABLE", "VIEW"]);
    }

    #[test]
    fn filter_tables_by_type_drops_non_matching_and_nulls() {
        let batch = tables_with_types(&["TABLE", "VIEW"]);
        let out = filter_tables_by_type(&batch, &["MANAGED"]).unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // ─── procedures / functions / procedure-columns reshape ───────────

    /// One-row `build_get_procedures` server shape.
    fn server_routine_row(comment: Option<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("routine_catalog", DataType::Utf8, true),
            Field::new("routine_schema", DataType::Utf8, true),
            Field::new("routine_name", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
            Field::new("specific_name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("main")])),
                Arc::new(StringArray::from(vec![Some("sales")])),
                Arc::new(StringArray::from(vec![Some("sp_report")])),
                Arc::new(StringArray::from(vec![comment])),
                Arc::new(StringArray::from(vec![Some("sp_report")])),
            ],
        )
        .unwrap()
    }

    fn server_function_row() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("functionName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, true),
            Field::new("catalogName", DataType::Utf8, true),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("functionType", DataType::Int32, true),
            Field::new("specificName", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("sales_total")])),
                Arc::new(StringArray::from(vec![Some("sales")])),
                Arc::new(StringArray::from(vec![Some("main")])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("sales_total")])),
            ],
        )
        .unwrap()
    }

    fn col_names(batch: &RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[test]
    fn reshape_procedures_produces_jdbc_get_procedures_shape() {
        let out = reshape_procedures(&server_routine_row(Some("nightly report"))).unwrap();
        assert_eq!(
            col_names(&out),
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
        assert_eq!(out.num_rows(), 1);
        // Reserved columns are null.
        assert!(out.column(3).is_null(0));
        // PROCEDURE_TYPE = procedureNoResult (1).
        let ptype = out.column(7).as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(ptype.value(0), 1);
    }

    #[test]
    fn reshape_functions_produces_jdbc_get_functions_shape() {
        let out = reshape_functions(&server_function_row(), "main").unwrap();
        assert_eq!(
            col_names(&out),
            vec![
                "FUNCTION_CAT",
                "FUNCTION_SCHEM",
                "FUNCTION_NAME",
                "REMARKS",
                "FUNCTION_TYPE",
                "SPECIFIC_NAME",
            ]
        );
        assert_eq!(
            out.column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "sales_total"
        );
        // The wider server integer is narrowed to JDBC SMALLINT.
        let ftype = out.column(4).as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(ftype.value(0), 1);
    }

    #[test]
    fn reshape_functions_supports_classic_single_column_shape() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "function",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![
                Some("system.builtin.abs"),
                None,
            ]))],
        )
        .unwrap();

        let out = reshape_functions(&source, "main").unwrap();
        let names = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "system.builtin.abs");
        assert!(names.is_null(1));
        assert!(out.column(1).is_null(0));
        assert!(out.column(3).is_null(0));
    }

    /// `build_get_procedure_columns` raw server shape (information_schema
    /// numeric metadata arrives as strings over the wire).
    fn server_parameter_rows() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("specific_catalog", DataType::Utf8, true),
            Field::new("specific_schema", DataType::Utf8, true),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("parameter_name", DataType::Utf8, true),
            Field::new("parameter_mode", DataType::Utf8, true),
            Field::new("is_result", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, true),
            Field::new("numeric_precision", DataType::Utf8, true),
            Field::new("numeric_precision_radix", DataType::Utf8, true),
            Field::new("numeric_scale", DataType::Utf8, true),
            Field::new("character_octet_length", DataType::Utf8, true),
            Field::new("ordinal_position", DataType::Utf8, true),
            Field::new("comment", DataType::Utf8, true),
        ]));
        // Row 0: IN INT param; Row 1: OUT DECIMAL(10,2); Row 2: INOUT; Row 3:
        // return value (is_result='YES') with a NULL parameter_name — exercises
        // the nullable COLUMN_NAME path (a non-nullable schema would fail here).
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("main"); 4])),
                Arc::new(StringArray::from(vec![Some("sales"); 4])),
                Arc::new(StringArray::from(vec![Some("sp_report"); 4])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("IN"),
                    Some("OUT"),
                    Some("INOUT"),
                    Some("OUT"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("NO"),
                    Some("NO"),
                    Some("NO"),
                    Some("YES"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("INT"),
                    Some("DECIMAL"),
                    Some("INT"),
                    Some("INT"),
                ])),
                Arc::new(StringArray::from(vec![None, Some("10"), None, None])),
                Arc::new(StringArray::from(vec![None, Some("10"), None, None])),
                Arc::new(StringArray::from(vec![None, Some("2"), None, None])),
                Arc::new(StringArray::from(vec![None::<&str>; 4])),
                Arc::new(StringArray::from(vec![
                    Some("0"),
                    Some("1"),
                    Some("2"),
                    Some("3"),
                ])),
                Arc::new(StringArray::from(vec![None::<&str>; 4])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn reshape_procedure_columns_produces_jdbc_shape_and_mapping() {
        let out = reshape_procedure_columns(&server_parameter_rows()).unwrap();
        assert_eq!(
            col_names(&out),
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
        assert_eq!(out.num_rows(), 4);

        // COLUMN_TYPE: IN=1, OUT=4, INOUT=2, return(is_result=YES)=5.
        let ct = out.column(4).as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(
            (0..4).map(|i| ct.value(i)).collect::<Vec<_>>(),
            vec![1, 4, 2, 5]
        );

        // COLUMN_NAME is nullable: the return-value row (row 3) has a NULL
        // parameter_name and reshaping must produce a null cell, not error.
        let col_name = out
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!col_name.is_null(0));
        assert!(
            col_name.is_null(3),
            "NULL parameter_name must pass through as a null COLUMN_NAME"
        );

        // DATA_TYPE from data_type name: INT → java.sql.Types.INTEGER (4).
        let dt = out.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(dt.value(0), 4);

        // TYPE_NAME preserves the raw data_type string.
        let tn = out
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tn.value(0), "INT");
        assert_eq!(tn.value(1), "DECIMAL");

        // PRECISION / SCALE pass information_schema through (NULL for the
        // INT row, 10 / 2 for the DECIMAL row).
        let prec = out.column(7).as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(prec.is_null(0));
        assert_eq!(prec.value(1), 10);
        let scale = out.column(9).as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(scale.value(1), 2);

        // RADIX is derived from the type, not passed through: NULL for
        // integers (even though the fixture supplies numeric_precision_radix=2
        // for the INT row), 10 for DECIMAL.
        let radix = out
            .column(10)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert!(
            radix.is_null(0),
            "INT radix must be NULL, not passed through"
        );
        assert_eq!(radix.value(1), 10);

        // NULLABLE is procedureNullableUnknown (2) for every row.
        let nul = out
            .column(11)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(nul.value(0), 2);

        // ORDINAL_POSITION passes information_schema (0-based) through.
        let ord = out
            .column(17)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..4).map(|i| ord.value(i)).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }
}
