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

//! Reshape Databricks `SHOW`/`SELECT information_schema.*` results
//! into JDBC/ODBC-conforming `RecordBatch`es.
//!
//! One `reshape_*` function per [`Metadata`](crate::Metadata) RPC.
//! Each takes the raw Arrow `RecordBatch` returned by
//! [`crate::client::Transport`] and rebuilds it with the JDBC schema
//! from [`crate::metadata::schemas`].
//!
//! ## Tolerance to source schema drift
//!
//! Databricks' `SHOW` commands occasionally rename or reorder
//! columns across DBR versions. The reshape layer looks columns up
//! **by name**, accepting a small set of historically-used aliases
//! per JDBC field, and emits NULL for any source column that's
//! genuinely missing rather than throwing. Required JDBC columns
//! (`TABLE_NAME`, `COLUMN_NAME`, `KEY_SEQ`, …) are an exception —
//! their absence in the source is a `data_loss` error so we don't
//! produce silently-broken output.

use crate::kernel_error::{Error, Result};
use crate::metadata::schemas::{
    CATALOGS_SCHEMA, COLUMNS_SCHEMA, FOREIGN_KEYS_SCHEMA, FUNCTIONS_SCHEMA, PRIMARY_KEYS_SCHEMA,
    PROCEDURES_SCHEMA, SCHEMAS_SCHEMA, TABLES_SCHEMA,
};
use crate::metadata::type_mapping::databricks_type_to_xdbc;
use arrow_array::{cast::AsArray, Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use std::sync::Arc;

// JDBC column-NULLABLE codes
const JDBC_COLUMN_NO_NULLS: i32 = 0;
const JDBC_COLUMN_NULLABLE: i32 = 1;
#[allow(dead_code)]
const JDBC_COLUMN_NULLABLE_UNKNOWN: i32 = 2;

// JDBC PROCEDURE_TYPE / FUNCTION_TYPE codes
const PROCEDURE_RESULT_UNKNOWN: i32 = 0;
const FUNCTION_RESULT_UNKNOWN: i32 = 0;

// ─── Helpers ─────────────────────────────────────────────────────────

/// Look up a column index by case-insensitive name. Returns `None`
/// if the column is absent.
fn col_idx(rb: &RecordBatch, name: &str) -> Option<usize> {
    rb.schema()
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(name))
}

/// Look up a column index using a list of aliases. Returns the
/// first match or `None`.
fn col_idx_any(rb: &RecordBatch, names: &[&str]) -> Option<usize> {
    names.iter().find_map(|n| col_idx(rb, n))
}

/// Pull a UTF-8 column out of `rb` by name, returning a `StringArray`
/// if present (whether `Utf8` or `LargeUtf8`).
fn string_column(rb: &RecordBatch, idx: usize) -> Result<StringArray> {
    let arr = rb.column(idx);
    match arr.data_type() {
        DataType::Utf8 => Ok(arr.as_string::<i32>().clone()),
        DataType::LargeUtf8 => {
            let large = arr.as_string::<i64>();
            // Re-pack into a Utf8 StringArray to match JDBC schema.
            let values: Vec<Option<&str>> = (0..large.len())
                .map(|i| {
                    if large.is_null(i) {
                        None
                    } else {
                        Some(large.value(i))
                    }
                })
                .collect();
            Ok(StringArray::from(values))
        }
        other => Err(Error::data_loss().message(format!(
            "metadata column {} expected Utf8, got {:?}",
            rb.schema().field(idx).name(),
            other
        ))),
    }
}

/// Required UTF-8 column matched against any of the given aliases.
fn required_string_any(rb: &RecordBatch, names: &[&str]) -> Result<StringArray> {
    let idx = col_idx_any(rb, names).ok_or_else(|| {
        Error::data_loss().message(format!(
            "expected one of {names:?} in Databricks metadata response"
        ))
    })?;
    string_column(rb, idx)
}

/// Optional UTF-8 column with aliases.
fn optional_string_any(rb: &RecordBatch, names: &[&str]) -> Result<Option<StringArray>> {
    match col_idx_any(rb, names) {
        Some(idx) => Ok(Some(string_column(rb, idx)?)),
        None => Ok(None),
    }
}

/// Constant UTF-8 column of the given length, all rows the same value.
fn const_string(value: &str, len: usize) -> ArrayRef {
    let arr: StringArray = (0..len).map(|_| Some(value)).collect();
    Arc::new(arr)
}

/// All-NULL UTF-8 column of the given length.
fn null_string(len: usize) -> ArrayRef {
    let arr: StringArray = (0..len).map(|_| None::<&str>).collect();
    Arc::new(arr)
}

/// All-NULL Int32 column of the given length.
fn null_i32(len: usize) -> ArrayRef {
    let arr: Int32Array = (0..len).map(|_| None::<i32>).collect();
    Arc::new(arr)
}

/// Constant Int32 column of the given length.
fn const_i32(value: i32, len: usize) -> ArrayRef {
    let arr: Int32Array = (0..len).map(|_| Some(value)).collect();
    Arc::new(arr)
}

/// Pull an i32 column tolerantly: accept `Int32` natively, `Int64`
/// with a checked downcast, or string parses. Returns `None` per row
/// when the source row is NULL or unparseable.
fn optional_i32(rb: &RecordBatch, name: &str) -> Result<Option<Int32Array>> {
    let Some(idx) = col_idx(rb, name) else {
        return Ok(None);
    };
    let arr = rb.column(idx);
    let n = arr.len();
    let out: Int32Array = match arr.data_type() {
        DataType::Int32 => arr.as_primitive::<arrow_array::types::Int32Type>().clone(),
        DataType::Int64 => {
            let src = arr.as_primitive::<arrow_array::types::Int64Type>();
            (0..n)
                .map(|i| {
                    if src.is_null(i) {
                        None
                    } else {
                        i32::try_from(src.value(i)).ok()
                    }
                })
                .collect()
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let src = string_column(rb, idx)?;
            (0..n)
                .map(|i| {
                    if src.is_null(i) {
                        None
                    } else {
                        src.value(i).parse::<i32>().ok()
                    }
                })
                .collect()
        }
        other => {
            return Err(Error::data_loss().message(format!(
                "metadata column '{name}' expected integer-like, got {other:?}"
            )));
        }
    };
    Ok(Some(out))
}

// ─── Reshape: catalogs ───────────────────────────────────────────────

/// Reshape `SHOW CATALOGS` (`catalog: Utf8`) into JDBC `getCatalogs`
/// (`TABLE_CAT`).
pub(crate) fn reshape_catalogs(raw: RecordBatch) -> Result<RecordBatch> {
    let catalogs = required_string_any(&raw, &["catalog", "TABLE_CAT", "catalog_name"])?;
    RecordBatch::try_new(CATALOGS_SCHEMA.clone(), vec![Arc::new(catalogs)]).map_err(|e| {
        Error::data_loss().message(format!("Failed to assemble JDBC catalogs batch: {e}"))
    })
}

// ─── Reshape: schemas ────────────────────────────────────────────────

/// Reshape `SHOW SCHEMAS` into JDBC `getSchemas` (`TABLE_SCHEM`,
/// `TABLE_CATALOG`).
///
/// `SHOW SCHEMAS IN ALL CATALOGS` returns both `databaseName` and
/// `catalog`; `SHOW SCHEMAS IN <catalog>` returns only
/// `databaseName`. For the second case, `fallback_catalog` (the
/// catalog the caller filtered on) is used to populate
/// `TABLE_CATALOG`.
pub(crate) fn reshape_schemas(
    raw: RecordBatch,
    fallback_catalog: Option<&str>,
) -> Result<RecordBatch> {
    let schemas = required_string_any(&raw, &["databaseName", "namespace", "TABLE_SCHEM"])?;
    let n = schemas.len();
    let catalog = match optional_string_any(&raw, &["catalog", "catalog_name", "TABLE_CATALOG"])? {
        Some(c) => Arc::new(c) as ArrayRef,
        None => match fallback_catalog {
            Some(s) => const_string(s, n),
            None => null_string(n),
        },
    };
    RecordBatch::try_new(SCHEMAS_SCHEMA.clone(), vec![Arc::new(schemas), catalog]).map_err(|e| {
        Error::data_loss().message(format!("Failed to assemble JDBC schemas batch: {e}"))
    })
}

// ─── Reshape: tables ─────────────────────────────────────────────────

/// Reshape `SHOW TABLES` into JDBC `getTables`.
///
/// Source columns (per legacy parse): `namespace`, `tableName`,
/// `isTemporary`, `information?`, `catalogName`, `tableType`,
/// `remarks?`. The 5 trailing JDBC columns (`TYPE_CAT` … `REF_GENERATION`)
/// have no Databricks equivalent and are emitted as NULL.
///
/// `table_type_filter` narrows the output rows to just the requested
/// types when the caller passed `Some(&["TABLE", "VIEW"])`.
pub(crate) fn reshape_tables(
    raw: RecordBatch,
    table_type_filter: Option<&[String]>,
) -> Result<RecordBatch> {
    let cat = required_string_any(&raw, &["catalogName", "TABLE_CAT", "catalog_name"])?;
    let schem = required_string_any(&raw, &["namespace", "TABLE_SCHEM", "schema_name"])?;
    let name = required_string_any(&raw, &["tableName", "TABLE_NAME", "table_name"])?;
    let typ = required_string_any(&raw, &["tableType", "TABLE_TYPE", "table_type"])?;
    let remarks = optional_string_any(&raw, &["remarks", "REMARKS", "comment"])?;

    // Apply the post-fetch table_types filter, if any.
    let keep: Vec<bool> = if let Some(filter) = table_type_filter {
        // Empty filter slice → keep nothing (caller asked for "no
        // types" — atypical but consistent).
        if filter.is_empty() {
            vec![false; typ.len()]
        } else {
            (0..typ.len())
                .map(|i| {
                    if typ.is_null(i) {
                        false
                    } else {
                        let v = typ.value(i);
                        filter.iter().any(|t| t.eq_ignore_ascii_case(v))
                    }
                })
                .collect()
        }
    } else {
        vec![true; typ.len()]
    };

    let n_out = keep.iter().filter(|b| **b).count();

    // Helper to filter a StringArray.
    let filter_str = |src: &StringArray| -> ArrayRef {
        let v: StringArray = keep
            .iter()
            .enumerate()
            .filter_map(|(i, k)| {
                if !*k {
                    None
                } else if src.is_null(i) {
                    Some(None)
                } else {
                    Some(Some(src.value(i)))
                }
            })
            .collect();
        Arc::new(v)
    };

    let cat_arr = filter_str(&cat);
    let schem_arr = filter_str(&schem);
    let name_arr = filter_str(&name);
    let typ_arr = filter_str(&typ);
    let remarks_arr = match remarks {
        Some(r) => filter_str(&r),
        None => null_string(n_out),
    };

    RecordBatch::try_new(
        TABLES_SCHEMA.clone(),
        vec![
            cat_arr,
            schem_arr,
            name_arr,
            typ_arr,
            remarks_arr,
            null_string(n_out), // TYPE_CAT
            null_string(n_out), // TYPE_SCHEM
            null_string(n_out), // TYPE_NAME
            null_string(n_out), // SELF_REFERENCING_COL_NAME
            null_string(n_out), // REF_GENERATION
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("Failed to assemble JDBC tables batch: {e}")))
}

// ─── Reshape: columns ────────────────────────────────────────────────

/// Reshape `SHOW COLUMNS` into JDBC `getColumns`.
pub(crate) fn reshape_columns(raw: RecordBatch) -> Result<RecordBatch> {
    let cat = required_string_any(&raw, &["catalogName", "TABLE_CAT", "catalog_name"])?;
    let schem = required_string_any(&raw, &["namespace", "TABLE_SCHEM", "schema_name"])?;
    let tbl = required_string_any(&raw, &["tableName", "TABLE_NAME", "table_name"])?;
    let name = required_string_any(&raw, &["col_name", "COLUMN_NAME", "column_name"])?;
    let type_text = required_string_any(&raw, &["columnType", "TYPE_NAME", "data_type"])?;
    let n = name.len();

    // DATA_TYPE = JDBC Types code derived from the Databricks type name.
    let data_type: Int32Array = (0..n)
        .map(|i| {
            if type_text.is_null(i) {
                Some(databricks_type_to_xdbc("") as i32)
            } else {
                Some(databricks_type_to_xdbc(type_text.value(i)) as i32)
            }
        })
        .collect();

    let column_size = optional_i32(&raw, "columnSize")?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_i32(n));
    let decimal_digits = optional_i32(&raw, "decimalDigits")?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_i32(n));
    let radix = optional_i32(&raw, "radix")?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_i32(n));
    let ordinal = optional_i32(&raw, "ordinalPosition")?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| {
            // ORDINAL_POSITION is NOT NULL in JDBC; fall back to row
            // index + 1 (1-based) when the source omits it.
            let arr: Int32Array = (0..n).map(|i| Some(i as i32 + 1)).collect();
            Arc::new(arr)
        });

    // is_nullable: source can be "true"/"false"/"YES"/"NO".
    let is_nullable_src = optional_string_any(&raw, &["isNullable", "is_nullable", "nullable"])?;
    let mut nullable_int = Vec::with_capacity(n);
    let mut is_nullable_out = Vec::with_capacity(n);
    for i in 0..n {
        let nullable = match is_nullable_src.as_ref() {
            Some(arr) if !arr.is_null(i) => {
                let v = arr.value(i);
                matches!(v, "true" | "TRUE" | "YES" | "yes" | "Yes")
                    || !matches!(v, "false" | "FALSE" | "NO" | "no" | "No")
            }
            _ => true,
        };
        nullable_int.push(if nullable {
            JDBC_COLUMN_NULLABLE
        } else {
            JDBC_COLUMN_NO_NULLS
        });
        is_nullable_out.push(if nullable { "YES" } else { "NO" });
    }
    let nullable_arr: ArrayRef = Arc::new(Int32Array::from(nullable_int));
    let is_nullable_arr: ArrayRef = Arc::new(StringArray::from_iter_values(is_nullable_out));

    let remarks = optional_string_any(&raw, &["remarks", "REMARKS", "comment"])?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_string(n));
    let auto_inc = optional_string_any(&raw, &["isAutoIncrement", "is_autoincrement"])?
        .map(|a| {
            // Normalise to YES/NO.
            let arr: StringArray = (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        Some("NO")
                    } else {
                        match a.value(i) {
                            "true" | "TRUE" | "YES" | "yes" | "Yes" => Some("YES"),
                            _ => Some("NO"),
                        }
                    }
                })
                .collect();
            Arc::new(arr) as ArrayRef
        })
        .unwrap_or_else(|| const_string("NO", n));
    let generated = optional_string_any(&raw, &["isGenerated", "is_generated"])?
        .map(|a| {
            let arr: StringArray = (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        Some("NO")
                    } else {
                        match a.value(i) {
                            "true" | "TRUE" | "YES" | "yes" | "Yes" => Some("YES"),
                            _ => Some("NO"),
                        }
                    }
                })
                .collect();
            Arc::new(arr) as ArrayRef
        })
        .unwrap_or_else(|| const_string("NO", n));

    RecordBatch::try_new(
        COLUMNS_SCHEMA.clone(),
        vec![
            Arc::new(cat),
            Arc::new(schem),
            Arc::new(tbl),
            Arc::new(name),
            Arc::new(data_type),
            Arc::new(type_text),
            column_size,
            null_i32(n), // BUFFER_LENGTH
            decimal_digits,
            radix,
            nullable_arr,
            remarks,
            null_string(n), // COLUMN_DEF
            null_i32(n),    // SQL_DATA_TYPE
            null_i32(n),    // SQL_DATETIME_SUB
            null_i32(n),    // CHAR_OCTET_LENGTH
            ordinal,
            is_nullable_arr,
            null_string(n), // SCOPE_CATALOG
            null_string(n), // SCOPE_SCHEMA
            null_string(n), // SCOPE_TABLE
            null_i32(n),    // SOURCE_DATA_TYPE
            auto_inc,
            generated,
        ],
    )
    .map_err(|e| Error::data_loss().message(format!("Failed to assemble JDBC columns batch: {e}")))
}

// ─── Reshape: primary keys ───────────────────────────────────────────

/// Reshape `SHOW KEYS IN TABLE` into JDBC `getPrimaryKeys`.
///
/// Source columns observed against Dogfood (DBR 16+):
/// `catalogName, namespace, tableName, col_name, keySeq,
/// constraintName, constraintType, parentCatalogName,
/// parentNamespace, parentTableName, parentColName, updateRule,
/// deleteRule, deferrability`. Older spellings are accepted as
/// aliases.
pub(crate) fn reshape_primary_keys(raw: RecordBatch) -> Result<RecordBatch> {
    let cat = required_string_any(
        &raw,
        &["TABLE_CAT", "catalogName", "catalog_name", "catalog"],
    )?;
    let schem = required_string_any(
        &raw,
        &["TABLE_SCHEM", "namespace", "schema_name", "schemaName"],
    )?;
    let tbl = required_string_any(&raw, &["TABLE_NAME", "tableName", "table_name"])?;
    let col_name = required_string_any(&raw, &["COLUMN_NAME", "col_name", "column_name"])?;
    let key_seq = optional_i32(&raw, "KEY_SEQ")?
        .or(optional_i32(&raw, "keySeq")?)
        .or(optional_i32(&raw, "key_seq")?)
        .or(optional_i32(&raw, "key_sequence")?)
        .ok_or_else(|| {
            Error::data_loss().message("expected 'keySeq' / 'KEY_SEQ' in SHOW KEYS response")
        })?;
    let pk_name = optional_string_any(&raw, &["PK_NAME", "constraintName", "pk_name", "key_name"])?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_string(cat.len()));

    RecordBatch::try_new(
        PRIMARY_KEYS_SCHEMA.clone(),
        vec![
            Arc::new(cat),
            Arc::new(schem),
            Arc::new(tbl),
            Arc::new(col_name),
            Arc::new(key_seq),
            pk_name,
        ],
    )
    .map_err(|e| {
        Error::data_loss().message(format!("Failed to assemble JDBC primary_keys batch: {e}"))
    })
}

// ─── Reshape: foreign keys ───────────────────────────────────────────

/// Reshape `SHOW FOREIGN KEYS IN TABLE` into JDBC `getImportedKeys`.
///
/// Source columns observed against Dogfood (DBR 16+, single-sided
/// SHOW FOREIGN KEYS): `catalogName, namespace, tableName, col_name`
/// (foreign-side identity), `keySeq, constraintName, constraintType`,
/// `parentCatalogName, parentNamespace, parentTableName,
/// parentColName` (parent side referenced by this FK), `updateRule,
/// deleteRule, deferrability`. The mapping into the JDBC
/// `getImportedKeys` shape is straightforward.
pub(crate) fn reshape_foreign_keys(raw: RecordBatch) -> Result<RecordBatch> {
    // Parent side ("PK" columns in JDBC) — the table being referenced.
    let pk_cat = required_string_any(
        &raw,
        &[
            "PKTABLE_CAT",
            "parentCatalogName",
            "parent_catalog",
            "parent_catalog_name",
        ],
    )?;
    let pk_schem = required_string_any(
        &raw,
        &[
            "PKTABLE_SCHEM",
            "parentNamespace",
            "parent_schema",
            "parent_schema_name",
        ],
    )?;
    let pk_tbl = required_string_any(
        &raw,
        &[
            "PKTABLE_NAME",
            "parentTableName",
            "parent_table",
            "parent_table_name",
        ],
    )?;
    let pk_col = required_string_any(
        &raw,
        &[
            "PKCOLUMN_NAME",
            "parentColName",
            "parent_column",
            "parent_column_name",
        ],
    )?;
    // Foreign side ("FK" columns in JDBC) — the table holding the FK.
    let fk_cat = required_string_any(
        &raw,
        &["FKTABLE_CAT", "catalogName", "catalog_name", "catalog"],
    )?;
    let fk_schem = required_string_any(&raw, &["FKTABLE_SCHEM", "namespace", "schema_name"])?;
    let fk_tbl = required_string_any(&raw, &["FKTABLE_NAME", "tableName", "table_name"])?;
    let fk_col = required_string_any(&raw, &["FKCOLUMN_NAME", "col_name", "column_name"])?;
    let key_seq = optional_i32(&raw, "KEY_SEQ")?
        .or(optional_i32(&raw, "keySeq")?)
        .or(optional_i32(&raw, "key_seq")?)
        .or(optional_i32(&raw, "key_sequence")?)
        .ok_or_else(|| {
            Error::data_loss().message("expected 'keySeq' in SHOW FOREIGN KEYS response")
        })?;
    let n = pk_cat.len();
    let update_rule = optional_i32(&raw, "UPDATE_RULE")?
        .or(optional_i32(&raw, "updateRule")?)
        .or(optional_i32(&raw, "update_rule")?)
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_i32(n));
    let delete_rule = optional_i32(&raw, "DELETE_RULE")?
        .or(optional_i32(&raw, "deleteRule")?)
        .or(optional_i32(&raw, "delete_rule")?)
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_i32(n));
    let fk_name = optional_string_any(
        &raw,
        &[
            "FK_NAME",
            "constraintName",
            "fk_name",
            "from_constraint_name",
        ],
    )?
    .map(|a| Arc::new(a) as ArrayRef)
    .unwrap_or_else(|| null_string(n));
    // Databricks doesn't surface the parent-side constraint name on
    // SHOW FOREIGN KEYS — emit NULL for PK_NAME.
    let pk_name = optional_string_any(&raw, &["PK_NAME", "pk_name"])?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_string(n));
    let deferrability = optional_i32(&raw, "DEFERRABILITY")?
        .or(optional_i32(&raw, "deferrability")?)
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_i32(n));

    RecordBatch::try_new(
        FOREIGN_KEYS_SCHEMA.clone(),
        vec![
            Arc::new(pk_cat),
            Arc::new(pk_schem),
            Arc::new(pk_tbl),
            Arc::new(pk_col),
            Arc::new(fk_cat),
            Arc::new(fk_schem),
            Arc::new(fk_tbl),
            Arc::new(fk_col),
            Arc::new(key_seq),
            update_rule,
            delete_rule,
            fk_name,
            pk_name,
            deferrability,
        ],
    )
    .map_err(|e| {
        Error::data_loss().message(format!("Failed to assemble JDBC foreign_keys batch: {e}"))
    })
}

// ─── Reshape: procedures ─────────────────────────────────────────────

/// Reshape `SELECT … information_schema.routines (PROCEDURE)` into
/// JDBC `getProcedures`.
pub(crate) fn reshape_procedures(raw: RecordBatch) -> Result<RecordBatch> {
    let cat = required_string_any(&raw, &["routine_catalog", "PROCEDURE_CAT"])?;
    let schem = required_string_any(&raw, &["routine_schema", "PROCEDURE_SCHEM"])?;
    let name = required_string_any(&raw, &["routine_name", "PROCEDURE_NAME"])?;
    let comment = optional_string_any(&raw, &["comment", "REMARKS"])?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_string(cat.len()));
    let specific = optional_string_any(&raw, &["specific_name", "SPECIFIC_NAME"])?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_string(cat.len()));
    let n = cat.len();

    RecordBatch::try_new(
        PROCEDURES_SCHEMA.clone(),
        vec![
            Arc::new(cat),
            Arc::new(schem),
            Arc::new(name),
            null_string(n), // RESERVED1
            null_string(n), // RESERVED2
            null_string(n), // RESERVED3
            comment,
            const_i32(PROCEDURE_RESULT_UNKNOWN, n),
            specific,
        ],
    )
    .map_err(|e| {
        Error::data_loss().message(format!("Failed to assemble JDBC procedures batch: {e}"))
    })
}

// ─── Reshape: functions ──────────────────────────────────────────────

/// Reshape `SELECT … information_schema.routines (FUNCTION)` into
/// JDBC `getFunctions`.
pub(crate) fn reshape_functions(raw: RecordBatch) -> Result<RecordBatch> {
    let cat = required_string_any(&raw, &["routine_catalog", "FUNCTION_CAT"])?;
    let schem = required_string_any(&raw, &["routine_schema", "FUNCTION_SCHEM"])?;
    let name = required_string_any(&raw, &["routine_name", "FUNCTION_NAME"])?;
    let comment = optional_string_any(&raw, &["comment", "REMARKS"])?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_string(cat.len()));
    let specific = optional_string_any(&raw, &["specific_name", "SPECIFIC_NAME"])?
        .map(|a| Arc::new(a) as ArrayRef)
        .unwrap_or_else(|| null_string(cat.len()));
    let n = cat.len();

    RecordBatch::try_new(
        FUNCTIONS_SCHEMA.clone(),
        vec![
            Arc::new(cat),
            Arc::new(schem),
            Arc::new(name),
            comment,
            const_i32(FUNCTION_RESULT_UNKNOWN, n),
            specific,
        ],
    )
    .map_err(|e| {
        Error::data_loss().message(format!("Failed to assemble JDBC functions batch: {e}"))
    })
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int32Array};
    use arrow_schema::{Field, Schema};

    fn schema_for(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn reshape_catalogs_renames_column() {
        let s = schema_for(vec![Field::new("catalog", DataType::Utf8, false)]);
        let raw = RecordBatch::try_new(
            s,
            vec![Arc::new(StringArray::from(vec!["main", "hive_metastore"]))],
        )
        .unwrap();

        let out = reshape_catalogs(raw).unwrap();
        assert_eq!(out.schema(), *CATALOGS_SCHEMA);
        let col = out.column(0).as_string::<i32>();
        assert_eq!(col.value(0), "main");
        assert_eq!(col.value(1), "hive_metastore");
    }

    #[test]
    fn reshape_catalogs_errors_on_missing_column() {
        let s = schema_for(vec![Field::new("nope", DataType::Utf8, false)]);
        let raw = RecordBatch::try_new(s, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();
        let err = reshape_catalogs(raw).unwrap_err();
        assert!(err.message.contains("catalog"), "{}", err.message);
    }

    #[test]
    fn reshape_schemas_with_explicit_catalog_column() {
        let s = schema_for(vec![
            Field::new("databaseName", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, false),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["default", "information_schema"])),
                Arc::new(StringArray::from(vec!["main", "main"])),
            ],
        )
        .unwrap();
        let out = reshape_schemas(raw, None).unwrap();
        assert_eq!(out.schema(), *SCHEMAS_SCHEMA);
        let s_col = out.column(0).as_string::<i32>();
        let c_col = out.column(1).as_string::<i32>();
        assert_eq!(s_col.value(0), "default");
        assert_eq!(c_col.value(0), "main");
    }

    #[test]
    fn reshape_schemas_uses_fallback_catalog() {
        let s = schema_for(vec![Field::new("databaseName", DataType::Utf8, false)]);
        let raw =
            RecordBatch::try_new(s, vec![Arc::new(StringArray::from(vec!["default"]))]).unwrap();
        let out = reshape_schemas(raw, Some("main")).unwrap();
        let c_col = out.column(1).as_string::<i32>();
        assert_eq!(c_col.value(0), "main");
    }

    #[test]
    fn reshape_tables_emits_jdbc_columns_with_nulls_for_unknown() {
        let s = schema_for(vec![
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, false),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["default"])),
                Arc::new(StringArray::from(vec!["t1"])),
                Arc::new(StringArray::from(vec!["TABLE"])),
            ],
        )
        .unwrap();
        let out = reshape_tables(raw, None).unwrap();
        assert_eq!(out.schema(), *TABLES_SCHEMA);
        assert_eq!(out.num_rows(), 1);
        // REMARKS missing → all NULL.
        assert!(out.column(4).is_null(0));
        // TYPE_CAT … REF_GENERATION all NULL.
        for i in 5..10 {
            assert!(out.column(i).is_null(0), "field {} should be NULL", i);
        }
    }

    #[test]
    fn reshape_tables_filters_by_type() {
        let s = schema_for(vec![
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, false),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main", "main"])),
                Arc::new(StringArray::from(vec!["d", "d"])),
                Arc::new(StringArray::from(vec!["t1", "v1"])),
                Arc::new(StringArray::from(vec!["TABLE", "VIEW"])),
            ],
        )
        .unwrap();
        let only_views = vec!["VIEW".to_string()];
        let out = reshape_tables(raw, Some(&only_views)).unwrap();
        assert_eq!(out.num_rows(), 1);
        let names = out.column(2).as_string::<i32>();
        assert_eq!(names.value(0), "v1");
    }

    #[test]
    fn reshape_columns_translates_data_type_and_nullable() {
        let s = schema_for(vec![
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("col_name", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
            Field::new("isNullable", DataType::Utf8, true),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["default"])),
                Arc::new(StringArray::from(vec!["t"])),
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(StringArray::from(vec!["INT"])),
                Arc::new(StringArray::from(vec!["false"])),
            ],
        )
        .unwrap();
        let out = reshape_columns(raw).unwrap();
        assert_eq!(out.schema(), *COLUMNS_SCHEMA);
        let data_type = out
            .column_by_name("DATA_TYPE")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(data_type.value(0), 4); // JDBC INTEGER

        let nullable = out
            .column_by_name("NULLABLE")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(nullable.value(0), JDBC_COLUMN_NO_NULLS);

        let is_nullable = out
            .column_by_name("IS_NULLABLE")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(is_nullable.value(0), "NO");

        // ORDINAL_POSITION fallback to row+1.
        let ordinal = out
            .column_by_name("ORDINAL_POSITION")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(ordinal.value(0), 1);
    }

    #[test]
    fn reshape_primary_keys_accepts_lower_snake_case() {
        let s = schema_for(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("key_sequence", DataType::Int32, false),
            Field::new("key_name", DataType::Utf8, true),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["d"])),
                Arc::new(StringArray::from(vec!["t"])),
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("pk_t")])),
            ],
        )
        .unwrap();
        let out = reshape_primary_keys(raw).unwrap();
        assert_eq!(out.schema(), *PRIMARY_KEYS_SCHEMA);
        let names = out
            .column_by_name("COLUMN_NAME")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(names.value(0), "id");
        let key_seq = out
            .column_by_name("KEY_SEQ")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(key_seq.value(0), 1);
        let pk_name = out.column_by_name("PK_NAME").unwrap().as_string::<i32>();
        assert_eq!(pk_name.value(0), "pk_t");
    }

    #[test]
    fn reshape_primary_keys_zero_rows_is_ok() {
        let s = schema_for(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("key_sequence", DataType::Int32, false),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
            ],
        )
        .unwrap();
        let out = reshape_primary_keys(raw).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.schema(), *PRIMARY_KEYS_SCHEMA);
    }

    #[test]
    fn reshape_foreign_keys_pairs_from_to_columns() {
        // Synthesised input shaped like the live SHOW FOREIGN KEYS
        // response on Dogfood: foreign-side identity columns first,
        // then `parent*` columns for the referenced table.
        let s = schema_for(vec![
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("col_name", DataType::Utf8, false),
            Field::new("parentCatalogName", DataType::Utf8, false),
            Field::new("parentNamespace", DataType::Utf8, false),
            Field::new("parentTableName", DataType::Utf8, false),
            Field::new("parentColName", DataType::Utf8, false),
            Field::new("keySeq", DataType::Int32, false),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["d"])),
                Arc::new(StringArray::from(vec!["child"])),
                Arc::new(StringArray::from(vec!["pid"])),
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["d"])),
                Arc::new(StringArray::from(vec!["parent"])),
                Arc::new(StringArray::from(vec!["id"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let out = reshape_foreign_keys(raw).unwrap();
        assert_eq!(out.schema(), *FOREIGN_KEYS_SCHEMA);
        let pk_tbl = out
            .column_by_name("PKTABLE_NAME")
            .unwrap()
            .as_string::<i32>();
        let fk_tbl = out
            .column_by_name("FKTABLE_NAME")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(pk_tbl.value(0), "parent");
        assert_eq!(fk_tbl.value(0), "child");
    }

    #[test]
    fn reshape_procedures_renames_columns() {
        let s = schema_for(vec![
            Field::new("routine_catalog", DataType::Utf8, false),
            Field::new("routine_schema", DataType::Utf8, false),
            Field::new("routine_name", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
            Field::new("specific_name", DataType::Utf8, true),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["d"])),
                Arc::new(StringArray::from(vec!["sp"])),
                Arc::new(StringArray::from(vec![Some("desc")])),
                Arc::new(StringArray::from(vec![Some("sp__1")])),
            ],
        )
        .unwrap();
        let out = reshape_procedures(raw).unwrap();
        assert_eq!(out.schema(), *PROCEDURES_SCHEMA);
        let name = out
            .column_by_name("PROCEDURE_NAME")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(name.value(0), "sp");
        let proc_type = out
            .column_by_name("PROCEDURE_TYPE")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(proc_type.value(0), PROCEDURE_RESULT_UNKNOWN);
    }

    #[test]
    fn reshape_functions_renames_columns() {
        let s = schema_for(vec![
            Field::new("routine_catalog", DataType::Utf8, false),
            Field::new("routine_schema", DataType::Utf8, false),
            Field::new("routine_name", DataType::Utf8, false),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["d"])),
                Arc::new(StringArray::from(vec!["fn"])),
            ],
        )
        .unwrap();
        let out = reshape_functions(raw).unwrap();
        assert_eq!(out.schema(), *FUNCTIONS_SCHEMA);
        let name = out
            .column_by_name("FUNCTION_NAME")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(name.value(0), "fn");
    }

    #[test]
    fn reshape_columns_handles_missing_optional_columns() {
        // Just the required columns.
        let s = schema_for(vec![
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("col_name", DataType::Utf8, false),
            Field::new("columnType", DataType::Utf8, false),
        ]);
        let raw = RecordBatch::try_new(
            s,
            vec![
                Arc::new(StringArray::from(vec!["main"])),
                Arc::new(StringArray::from(vec!["d"])),
                Arc::new(StringArray::from(vec!["t"])),
                Arc::new(StringArray::from(vec!["c"])),
                Arc::new(StringArray::from(vec!["STRING"])),
            ],
        )
        .unwrap();
        let out = reshape_columns(raw).unwrap();
        assert_eq!(out.num_rows(), 1);
        // Default to nullable when isNullable column is absent.
        let nullable = out
            .column_by_name("NULLABLE")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(nullable.value(0), JDBC_COLUMN_NULLABLE);
        // is_autoincrement / is_generated default to "NO".
        let auto_inc = out
            .column_by_name("IS_AUTOINCREMENT")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(auto_inc.value(0), "NO");
    }

    #[test]
    fn const_string_and_null_helpers_produce_expected_lengths() {
        let s = const_string("X", 3);
        let s_str = s.as_string::<i32>();
        assert_eq!(s_str.len(), 3);
        for i in 0..3 {
            assert_eq!(s_str.value(i), "X");
        }

        let n = null_i32(2);
        assert_eq!(n.len(), 2);
        assert!(n.is_null(0) && n.is_null(1));
    }

    #[test]
    #[allow(dead_code)]
    fn boolean_helper_compiles() {
        // Sanity that BooleanArray builds — used by static_data; ensures
        // the import is genuinely live in this file.
        let _b = BooleanArray::from(vec![true, false]);
    }
}
