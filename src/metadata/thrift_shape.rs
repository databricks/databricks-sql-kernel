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
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
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
        Field::new("BUFFER_LENGTH", DataType::Int32, true),
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
        Field::new("SOURCE_DATA_TYPE", DataType::Int32, true),
        // JDBC `DatabaseMetaData.getColumns` Javadoc names this
        // `IS_AUTOINCREMENT` (one word). Thrift's wire response uses
        // `IS_AUTO_INCREMENT` (with the underscore), and that's what
        // existing PySQL/JDBC users on Databricks see today. Match
        // Thrift's spelling so the kernel backend is a drop-in
        // replacement; the column meaning is identical.
        Field::new("IS_AUTO_INCREMENT", DataType::Utf8, false),
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

/// Map a server-side `"true"` / `"false"` string column to JDBC's
/// `"YES"` / `"NO"` / `""` convention. Used for `IS_NULLABLE` and
/// `IS_AUTO_INCREMENT` in `reshape_columns`; both follow the same
/// rule (JDBC defines an empty string when the value is "unknown",
/// which we use for null / unrecognised inputs). Missing source
/// column → all-empty string array.
fn bool_string_to_yn(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let n = batch.num_rows();
    let Some(idx) = column_index(batch.schema_ref(), name) else {
        return Ok(Arc::new(StringArray::from(vec![Some(""); n])));
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
                ""
            } else {
                match strs.value(i) {
                    "true" => "YES",
                    "false" => "NO",
                    _ => "",
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
/// that omit it (single-catalog form). `table_types`, when `Some`,
/// filters rows to those whose `TABLE_TYPE` appears in the slice
/// (matches JDBC `DatabaseMetaData.getTables` behaviour).
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
    let remarks = if column_index(batch.schema_ref(), "remarks").is_some() {
        utf8_column(batch, "remarks")?
    } else {
        Arc::new(StringArray::from(vec![Some(""); n]))
    };
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

    // Apply table_types filter, if requested. JDBC's contract:
    // null/empty = no filter; otherwise keep only rows whose
    // TABLE_TYPE is in the list.
    let filtered = match table_types {
        Some(types) if !types.is_empty() => filter_tables_by_type(&reshaped, types)?,
        _ => reshaped,
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
    let remarks = utf8_column(batch, "remarks")?;
    let ordinal_position = int32_column(batch, "ordinalPosition")?;
    // IS_NULLABLE / IS_AUTO_INCREMENT: server emits "true"/"false";
    // JDBC contract is "YES"/"NO"/"" (empty for unknown).
    let is_nullable_str = bool_string_to_yn(batch, "isNullable")?;
    let is_autoincrement = bool_string_to_yn(batch, "isAutoIncrement")?;

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
            null_int32(n), // BUFFER_LENGTH
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
            null_int32(n), // SOURCE_DATA_TYPE
            is_autoincrement,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("reshape columns: {e}")))
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
                types.contains(&t)
            }
        })
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| Error::data_loss().message(format!("filter_tables_by_type: {e}")))
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

impl ResultReader for ThriftReshapingReader {
    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.upstream.next_batch()? {
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
    fn tables_empty_types_filter_passes_everything() {
        let b = server_tables_full();
        let out = reshape_tables(&b, Some("main"), Some(&[])).unwrap();
        assert_eq!(out.num_rows(), 3);
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
        // STRING → java.sql.Types.LONGVARCHAR = -1
        assert_eq!(data_type.value(1), -1);
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
}
