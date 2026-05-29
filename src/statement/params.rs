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

//! Parameter handling utilities for prepared statements.
//!
//! This module provides:
//! - SQL parameter marker (`?`) parsing and transformation
//! - Arrow RecordBatch to SEA statement parameter conversion
//! - Databricks type name to/from Arrow DataType mapping
//! - `DESCRIBE QUERY` result parsing for `execute_schema()`

use crate::kernel_error::{Error, Result};
use crate::metadata::type_mapping::{arrow_type_to_databricks, databricks_type_to_arrow};
use crate::statement::spec::{geo_wire, TypedValue};
use crate::types::sea::StatementParameter;
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

// ---------------------------------------------------------------------------
// SQL parameter marker parsing
// ---------------------------------------------------------------------------

/// States for the SQL parser state machine.
#[derive(Clone, Copy, PartialEq)]
enum SqlParserState {
    Normal,
    InSingleQuote,
    InDoubleQuote,
    InBacktickQuote,
    InLineComment,
    InBlockComment,
}

/// Walk a SQL string with a state machine, calling `on_marker` for each `?`
/// found in normal context (outside string literals and comments).
fn walk_sql(sql: &str, mut on_marker: impl FnMut(usize)) {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut state = SqlParserState::Normal;

    while i < len {
        match state {
            SqlParserState::Normal => {
                if bytes[i] == b'?' {
                    on_marker(i);
                    i += 1;
                } else if bytes[i] == b'\'' {
                    state = SqlParserState::InSingleQuote;
                    i += 1;
                } else if bytes[i] == b'"' {
                    state = SqlParserState::InDoubleQuote;
                    i += 1;
                } else if bytes[i] == b'`' {
                    state = SqlParserState::InBacktickQuote;
                    i += 1;
                } else if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    state = SqlParserState::InLineComment;
                    i += 2;
                } else if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    state = SqlParserState::InBlockComment;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            SqlParserState::InSingleQuote => {
                if i + 1 < len && bytes[i] == b'\'' && bytes[i + 1] == b'\'' {
                    // Escaped single quote ('')
                    i += 2;
                } else if bytes[i] == b'\'' {
                    state = SqlParserState::Normal;
                    i += 1;
                } else {
                    i += 1;
                }
            }
            SqlParserState::InDoubleQuote => {
                if i + 1 < len && bytes[i] == b'"' && bytes[i + 1] == b'"' {
                    // Escaped double quote ("")
                    i += 2;
                } else if bytes[i] == b'"' {
                    state = SqlParserState::Normal;
                    i += 1;
                } else {
                    i += 1;
                }
            }
            SqlParserState::InBacktickQuote => {
                if bytes[i] == b'`' {
                    state = SqlParserState::Normal;
                    i += 1;
                } else {
                    i += 1;
                }
            }
            SqlParserState::InLineComment => {
                if bytes[i] == b'\n' {
                    state = SqlParserState::Normal;
                }
                i += 1;
            }
            SqlParserState::InBlockComment => {
                if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    state = SqlParserState::Normal;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
    }
}

/// Count parameter markers (`?`) in a SQL string, ignoring those inside
/// string literals (`'...'`) and comments (`--`, `/* */`).
pub fn count_parameter_markers(sql: &str) -> usize {
    let mut count = 0;
    walk_sql(sql, |_| count += 1);
    count
}

/// Replace parameter markers (`?`) with string literals (`'?'`) for use
/// with `DESCRIBE QUERY`. Markers inside string literals and comments are
/// left untouched.
///
/// Reference: JDBC's `DatabricksPreparedStatement.surroundPlaceholdersWithQuotes()`.
pub fn replace_parameter_markers_with_literals(sql: &str) -> String {
    // Collect marker positions
    let mut positions = Vec::new();
    walk_sql(sql, |pos| positions.push(pos));

    if positions.is_empty() {
        return sql.to_string();
    }

    // Build result string, replacing each `?` at marker positions with `'?'`
    let mut result = String::with_capacity(sql.len() + positions.len() * 2);
    let mut last = 0;
    for pos in positions {
        result.push_str(&sql[last..pos]);
        result.push_str("'?'");
        last = pos + 1;
    }
    result.push_str(&sql[last..]);
    result
}

// ---------------------------------------------------------------------------
// Arrow → SEA parameter conversion
// ---------------------------------------------------------------------------

/// Convert a single-row Arrow RecordBatch into SEA statement parameters.
///
/// Each column in the batch becomes one positional parameter. Arrow types are
/// mapped to Databricks SQL type names and values are serialized to strings.
/// Null values are sent with `value: None` (JSON null).
pub fn record_batch_to_sea_parameters(batch: &RecordBatch) -> Result<Vec<StatementParameter>> {
    if batch.num_rows() != 1 {
        return Err(Error::invalid_argument().message(format!(
            "bind() expects a single-row RecordBatch, got {} rows",
            batch.num_rows()
        )));
    }

    let mut params = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        let param_type = arrow_type_to_databricks(column.data_type());
        let value = if column.is_null(0) {
            None
        } else {
            Some(arrow_value_to_string(column, 0)?)
        };

        params.push(StatementParameter {
            name: None,
            ordinal: Some((i + 1) as i32),
            param_type,
            value,
        });
    }

    Ok(params)
}

/// Extract a scalar value from an Arrow array at the given index as a string.
fn arrow_value_to_string(array: &dyn Array, index: usize) -> Result<String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_boolean();
            Ok(arr.value(index).to_string())
        }
        DataType::Int8 => Ok(array.as_primitive::<Int8Type>().value(index).to_string()),
        DataType::Int16 => Ok(array.as_primitive::<Int16Type>().value(index).to_string()),
        DataType::Int32 => Ok(array.as_primitive::<Int32Type>().value(index).to_string()),
        DataType::Int64 => Ok(array.as_primitive::<Int64Type>().value(index).to_string()),
        DataType::UInt8 => Ok(array.as_primitive::<UInt8Type>().value(index).to_string()),
        DataType::UInt16 => Ok(array.as_primitive::<UInt16Type>().value(index).to_string()),
        DataType::UInt32 => Ok(array.as_primitive::<UInt32Type>().value(index).to_string()),
        DataType::UInt64 => Ok(array.as_primitive::<UInt64Type>().value(index).to_string()),
        DataType::Float16 => Ok(array
            .as_primitive::<Float16Type>()
            .value(index)
            .to_f64()
            .to_string()),
        DataType::Float32 => Ok(array.as_primitive::<Float32Type>().value(index).to_string()),
        DataType::Float64 => Ok(array.as_primitive::<Float64Type>().value(index).to_string()),
        DataType::Decimal128(_, scale) => {
            let arr = array.as_primitive::<Decimal128Type>();
            let raw = arr.value(index);
            Ok(format_decimal128(raw, *scale))
        }
        DataType::Utf8 => Ok(array.as_string::<i32>().value(index).to_string()),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(index).to_string()),
        DataType::Binary => {
            let arr = array.as_binary::<i32>();
            Ok(hex::encode(arr.value(index)))
        }
        DataType::LargeBinary => {
            let arr = array.as_binary::<i64>();
            Ok(hex::encode(arr.value(index)))
        }
        DataType::Date32 => {
            let arr = array.as_primitive::<Date32Type>();
            let date = arr.value_as_date(index).unwrap_or(chrono::NaiveDate::MIN);
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let arr = array.as_primitive::<Date64Type>();
            let date = arr.value_as_date(index).unwrap_or(chrono::NaiveDate::MIN);
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(unit, _tz) => {
            let dt = match unit {
                TimeUnit::Second => array
                    .as_primitive::<TimestampSecondType>()
                    .value_as_datetime(index),
                TimeUnit::Millisecond => array
                    .as_primitive::<TimestampMillisecondType>()
                    .value_as_datetime(index),
                TimeUnit::Microsecond => array
                    .as_primitive::<TimestampMicrosecondType>()
                    .value_as_datetime(index),
                TimeUnit::Nanosecond => array
                    .as_primitive::<TimestampNanosecondType>()
                    .value_as_datetime(index),
            };
            let dt = dt.unwrap_or(chrono::DateTime::UNIX_EPOCH.naive_utc());
            Ok(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        }
        _ => {
            // Fallback: use Debug representation
            Err(Error::invalid_argument().message(format!(
                "Unsupported Arrow type for parameter binding: {:?}",
                array.data_type()
            )))
        }
    }
}

/// Format a Decimal128 raw value with the given scale.
fn format_decimal128(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        return raw.to_string();
    }
    let scale = scale as u32;
    let divisor = 10_i128.pow(scale);
    let sign = if raw < 0 { "-" } else { "" };
    let abs_raw = raw.unsigned_abs();
    let whole = abs_raw / divisor as u128;
    let frac = abs_raw % divisor as u128;
    format!("{sign}{whole}.{frac:0>width$}", width = scale as usize)
}

// ---------------------------------------------------------------------------
// TypedValue → SEA parameter
// ---------------------------------------------------------------------------

/// SEA wire payload for a single bound parameter: the
/// Databricks-side type name (which the server interprets to coerce
/// the value), and the canonical string form of the value (`None`
/// for SQL NULL).
struct SeaPayload {
    type_name: String,
    value: Option<String>,
}

/// Convert a [`TypedValue`] to its SEA wire form. Returns
/// `Err(InvalidArgument)` for variants the SEA wire protocol does
/// not accept as a bound parameter type (today: `Binary` — see the
/// variant arm for the workaround).
fn typed_value_to_sea_payload(value: &TypedValue) -> Result<SeaPayload> {
    fn ok(type_name: impl Into<String>, value: Option<String>) -> Result<SeaPayload> {
        Ok(SeaPayload {
            type_name: type_name.into(),
            value,
        })
    }
    match value {
        TypedValue::Null => ok("STRING", None),
        TypedValue::Bool(b) => ok("BOOLEAN", Some(b.to_string())),
        TypedValue::Int8(v) => ok("TINYINT", Some(v.to_string())),
        TypedValue::Int16(v) => ok("SMALLINT", Some(v.to_string())),
        TypedValue::Int32(v) => ok("INT", Some(v.to_string())),
        TypedValue::Int64(v) => ok("BIGINT", Some(v.to_string())),
        // Unsigned types are promoted to the next wider signed type
        // — Databricks SQL has no unsigned numerics. Matches the
        // policy in `arrow_type_to_databricks`.
        TypedValue::UInt8(v) => ok("SMALLINT", Some(v.to_string())),
        TypedValue::UInt16(v) => ok("INT", Some(v.to_string())),
        TypedValue::UInt32(v) => ok("BIGINT", Some(v.to_string())),
        TypedValue::UInt64(v) => ok("BIGINT", Some(v.to_string())),
        TypedValue::Float32(v) => ok("FLOAT", Some(v.to_string())),
        TypedValue::Float64(v) => ok("DOUBLE", Some(v.to_string())),
        TypedValue::String(s) => ok("STRING", Some(s.clone())),
        // BINARY is a real Databricks SQL type
        // (https://docs.databricks.com/aws/en/sql/language-manual/data-types/binary-type).
        // Everywhere BINARY appears in SQL text — `X'cafe'`
        // literals, `CAST(... AS BINARY)`, BINARY result columns —
        // works through the kernel today (the
        // `select_returns_all_types_correctly` E2E test proves it).
        //
        // The narrow gap is **the SEA parameter-array bind layer**:
        // when the kernel sends `{"type": "BINARY", "value": ...}`
        // in the SEA `parameters` field, the server rejects with
        // `INVALID_PARAMETER_MARKER_VALUE.INVALID_DATA_TYPE`
        // regardless of value encoding (verified empirically;
        // PySQL independently confirms by listing `BINARY` in its
        // `DatabricksSupportedType` enum but shipping no
        // `BinaryParameter` class).
        //
        // JDBC works around this by SQL-text interpolation
        // (`X'hex'` literal, gated on a `supportManyParameters`
        // connection flag); the kernel doesn't do SQL rewriting in
        // v0. Callers needing to bind raw bytes hex-encode them,
        // pass as `TypedValue::String`, and wrap the marker with
        // `unhex(?)` in their SQL (e.g.
        // `INSERT INTO t VALUES (unhex(?))`). Tracked as a v0.1
        // follow-up: native binary param binding once SEA grows
        // wire support (or auto-rewrite to `unhex(?)`).
        TypedValue::Binary(_) => Err(Error::invalid_argument().message(
            "TypedValue::Binary cannot currently be bound through SEA's parameters array \
             (server-side limitation; the BINARY type itself works everywhere else — `X'cafe'` \
             literals, CAST(... AS BINARY), BINARY result columns). Workaround: hex-encode the \
             bytes, bind them as TypedValue::String, and wrap the marker with unhex(?) in your \
             SQL (e.g. `INSERT INTO t VALUES (unhex(?))`).",
        )),
        TypedValue::Decimal {
            value,
            precision,
            scale,
        } => {
            // The parameter type name must carry (precision, scale)
            // so the server doesn't default to `DECIMAL(10, 0)` and
            // round the fractional digits away — observed against
            // Dogfood with `value="-123.45"` defaulting to
            // `"-123.00"`. JDBC sends the same form.
            ok(format!("DECIMAL({precision},{scale})"), Some(value.clone()))
        }
        TypedValue::Date(d) => ok("DATE", Some(d.format("%Y-%m-%d").to_string())),
        TypedValue::Timestamp(ts) => ok(
            "TIMESTAMP",
            Some(ts.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        ),
        // `TIMESTAMP_NTZ` carries the same naive datetime as `TIMESTAMP`
        // but flags the type-name on the wire so the server treats it
        // as explicitly no-timezone. Avoids the session-timezone
        // interpretation `TIMESTAMP` is subject to in some DBR
        // versions.
        TypedValue::TimestampNtz(ts) => ok(
            "TIMESTAMP_NTZ",
            Some(ts.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        ),
        // `TIMESTAMP_LTZ` is tz-aware. The wire format is UTC; SEA
        // accepts an ISO-8601 string with a `Z` suffix as the
        // canonical form. We render with `%.f` so sub-second precision
        // (microseconds) round-trips; the format omits it when the
        // value has no fractional second.
        TypedValue::TimestampLtz(ts) => ok(
            "TIMESTAMP_LTZ",
            Some(ts.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()),
        ),
        TypedValue::Interval(s) => ok("INTERVAL", Some(s.clone())),
        // `VARIANT` — pass-through. The kernel doesn't validate the
        // JSON; the server parses on receipt and rejects malformed
        // input with its own error. The connector or caller is
        // expected to have produced valid JSON.
        TypedValue::Variant(s) => ok("VARIANT", Some(s.clone())),
        // `GEOGRAPHY` and `GEOMETRY` — bind as EWKT today. The
        // `param_type` carries the SRID decorator when present
        // (e.g. `GEOGRAPHY(4326)`), matching the form the server
        // emits in column metadata. The `value` is EWKT (`SRID=N;
        // WKT`) when SRID is set; bare WKT when not.
        //
        // Binding via WKB is not yet wired — when SEA grows the
        // binary geo input path, this arm gets a branch on
        // `wkb.is_some()` to send the binary form instead. The
        // variant's optional `wkb` field is already in place to
        // make that change additive.
        TypedValue::Geography { wkt, srid, .. } => ok(
            geo_param_type(geo_wire::GEOGRAPHY, *srid),
            Some(geo_param_value(wkt, *srid)),
        ),
        TypedValue::Geometry { wkt, srid, .. } => ok(
            geo_param_type(geo_wire::GEOMETRY, *srid),
            Some(geo_param_value(wkt, *srid)),
        ),
    }
}

/// Format the SEA `param_type` for a geospatial value. Appends an
/// `(SRID)` decorator when the SRID is set, matching the form the
/// server emits in column metadata (e.g. `GEOGRAPHY(4326)`).
fn geo_param_type(family: &str, srid: Option<i32>) -> String {
    match srid {
        Some(s) => format!("{family}({s})"),
        None => family.to_owned(),
    }
}

/// Format the SEA `value` field for a geospatial binding. EWKT
/// (`SRID=N;WKT`) when an SRID is present; bare WKT otherwise.
fn geo_param_value(wkt: &str, srid: Option<i32>) -> String {
    match srid {
        Some(s) => format!("{}{};{}", geo_wire::EWKT_SRID_PREFIX, s, wkt),
        None => wkt.to_owned(),
    }
}

/// Convert a positional [`TypedValue`] binding to a SEA
/// [`StatementParameter`].
///
/// `ordinal` is 1-based on the wire. Direct conversion — no
/// Arrow round-trip. Returns `Err(InvalidArgument)` for variants
/// the SEA wire protocol does not accept (today: `Binary`).
pub(crate) fn typed_value_to_sea_parameter(
    ordinal: i32,
    value: &TypedValue,
) -> Result<StatementParameter> {
    let payload = typed_value_to_sea_payload(value)?;
    Ok(StatementParameter {
        name: None,
        ordinal: Some(ordinal),
        param_type: payload.type_name,
        value: payload.value,
    })
}

/// Convert a named [`TypedValue`] binding to a SEA
/// [`StatementParameter`]. The canonical SEA proto marks
/// `StatementParameter.name` as `openapi_required=true` — named
/// binding is the spec-required public form; positional is
/// `PUBLIC_UNDOCUMENTED`. Returns `Err(InvalidArgument)` for variants
/// the SEA wire protocol does not accept (today: `Binary`).
pub(crate) fn named_typed_value_to_sea_parameter(
    name: impl Into<String>,
    value: &TypedValue,
) -> Result<StatementParameter> {
    let payload = typed_value_to_sea_payload(value)?;
    Ok(StatementParameter {
        name: Some(name.into()),
        ordinal: None,
        param_type: payload.type_name,
        value: payload.value,
    })
}

// ---------------------------------------------------------------------------
// DESCRIBE QUERY result → Arrow Schema
// ---------------------------------------------------------------------------

/// Convert a `DESCRIBE QUERY` result into an Arrow Schema.
///
/// `DESCRIBE QUERY` returns rows with columns `col_name` and `data_type`.
/// Each row becomes a field in the output schema.
pub fn describe_result_to_schema(
    reader: impl arrow_array::RecordBatchReader,
) -> crate::kernel_error::Result<Schema> {
    let mut fields = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            Error::io().message(format!("Error reading DESCRIBE QUERY result: {e}"))
        })?;

        // DESCRIBE QUERY returns at least 2 columns: col_name, data_type
        if batch.num_columns() < 2 {
            return Err(Error::io().message(format!(
                "DESCRIBE QUERY returned {} columns, expected at least 2",
                batch.num_columns()
            )));
        }

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                Error::io().message("DESCRIBE QUERY col_name column is not a StringArray")
            })?;

        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                Error::io().message("DESCRIBE QUERY data_type column is not a StringArray")
            })?;

        for row in 0..batch.num_rows() {
            let col_name = names.value(row);
            let data_type_str = types.value(row);
            let arrow_type = databricks_type_to_arrow(data_type_str);
            fields.push(Field::new(col_name, arrow_type, true));
        }
    }

    Ok(Schema::new(fields))
}

/// Encode bytes as a hex string. Avoids adding a `hex` crate dependency.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // --- SQL parser tests ---

    #[test]
    fn test_count_simple() {
        assert_eq!(count_parameter_markers("SELECT * FROM t WHERE id = ?"), 1);
    }

    #[test]
    fn test_count_multiple() {
        assert_eq!(count_parameter_markers("INSERT INTO t VALUES (?, ?, ?)"), 3);
    }

    #[test]
    fn test_count_in_string_literal() {
        assert_eq!(count_parameter_markers("SELECT '?' FROM t"), 0);
    }

    #[test]
    fn test_count_in_line_comment() {
        assert_eq!(
            count_parameter_markers("SELECT -- ? comment\n * FROM t WHERE id = ?"),
            1
        );
    }

    #[test]
    fn test_count_in_block_comment() {
        assert_eq!(
            count_parameter_markers("SELECT /* ? */ * FROM t WHERE id = ?"),
            1
        );
    }

    #[test]
    fn test_count_escaped_quote() {
        assert_eq!(
            count_parameter_markers("SELECT * FROM t WHERE name = '?''s value' AND id = ?"),
            1
        );
    }

    #[test]
    fn test_count_in_double_quote() {
        assert_eq!(
            count_parameter_markers(r#"SELECT * FROM t WHERE "col?" = ?"#),
            1
        );
    }

    #[test]
    fn test_count_in_backtick_quote() {
        assert_eq!(
            count_parameter_markers("SELECT * FROM `col?` WHERE id = ?"),
            1
        );
    }

    #[test]
    fn test_count_escaped_double_quote() {
        assert_eq!(
            count_parameter_markers(r#"SELECT * FROM t WHERE "a""?" = ?"#),
            1
        );
    }

    #[test]
    fn test_count_no_markers() {
        assert_eq!(count_parameter_markers("SELECT 1"), 0);
    }

    #[test]
    fn test_count_empty() {
        assert_eq!(count_parameter_markers(""), 0);
    }

    // --- replace markers tests ---

    #[test]
    fn test_replace_simple() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT * FROM t WHERE id = ?"),
            "SELECT * FROM t WHERE id = '?'"
        );
    }

    #[test]
    fn test_replace_multiple() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT * FROM t WHERE a = ? AND b = ?"),
            "SELECT * FROM t WHERE a = '?' AND b = '?'"
        );
    }

    #[test]
    fn test_replace_in_string_literal_untouched() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT '?' FROM t WHERE id = ?"),
            "SELECT '?' FROM t WHERE id = '?'"
        );
    }

    #[test]
    fn test_replace_in_double_quote_untouched() {
        assert_eq!(
            replace_parameter_markers_with_literals(r#"SELECT "col?" FROM t WHERE id = ?"#),
            r#"SELECT "col?" FROM t WHERE id = '?'"#
        );
    }

    #[test]
    fn test_replace_in_backtick_untouched() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT `col?` FROM t WHERE id = ?"),
            "SELECT `col?` FROM t WHERE id = '?'"
        );
    }

    #[test]
    fn test_replace_no_markers() {
        assert_eq!(
            replace_parameter_markers_with_literals("SELECT 1"),
            "SELECT 1"
        );
    }

    // --- Arrow → SEA parameter conversion tests ---

    fn make_single_row_batch(fields: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let schema_fields: Vec<Field> = fields
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(schema_fields));
        let arrays: Vec<Arc<dyn Array>> = fields.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn test_convert_int32() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(Int32Array::from(vec![42])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].ordinal, Some(1));
        assert_eq!(params[0].param_type, "INT");
        assert_eq!(params[0].value, Some("42".to_string()));
    }

    #[test]
    fn test_convert_string() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(StringArray::from(vec!["hello"])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "STRING");
        assert_eq!(params[0].value, Some("hello".to_string()));
    }

    #[test]
    fn test_convert_null_value() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(StringArray::from(vec![None::<&str>])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "STRING");
        assert_eq!(params[0].value, None);
    }

    #[test]
    fn test_convert_multi_column() {
        let batch = make_single_row_batch(vec![
            (
                "p1",
                Arc::new(Int32Array::from(vec![100])) as Arc<dyn Array>,
            ),
            (
                "p2",
                Arc::new(StringArray::from(vec!["alice"])) as Arc<dyn Array>,
            ),
            (
                "p3",
                Arc::new(Float64Array::from(vec![3.25])) as Arc<dyn Array>,
            ),
        ]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].ordinal, Some(1));
        assert_eq!(params[0].param_type, "INT");
        assert_eq!(params[1].ordinal, Some(2));
        assert_eq!(params[1].param_type, "STRING");
        assert_eq!(params[2].ordinal, Some(3));
        assert_eq!(params[2].param_type, "DOUBLE");
    }

    #[test]
    fn test_convert_multi_row_error() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>],
        )
        .unwrap();
        assert!(record_batch_to_sea_parameters(&batch).is_err());
    }

    #[test]
    fn test_convert_boolean() {
        let batch = make_single_row_batch(vec![(
            "p1",
            Arc::new(BooleanArray::from(vec![true])) as Arc<dyn Array>,
        )]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "BOOLEAN");
        assert_eq!(params[0].value, Some("true".to_string()));
    }

    #[test]
    fn test_convert_decimal128() {
        let arr = arrow_array::Decimal128Array::from(vec![12345])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = make_single_row_batch(vec![("p1", Arc::new(arr) as Arc<dyn Array>)]);
        let params = record_batch_to_sea_parameters(&batch).unwrap();
        assert_eq!(params[0].param_type, "DECIMAL(10,2)");
        assert_eq!(params[0].value, Some("123.45".to_string()));
    }

    // --- Decimal formatting tests ---

    #[test]
    fn test_format_decimal128() {
        assert_eq!(format_decimal128(12345, 2), "123.45");
        assert_eq!(format_decimal128(100, 2), "1.00");
        assert_eq!(format_decimal128(5, 3), "0.005");
        assert_eq!(format_decimal128(-12345, 2), "-123.45");
        assert_eq!(format_decimal128(-5, 2), "-0.05");
        assert_eq!(format_decimal128(-1, 3), "-0.001");
        assert_eq!(format_decimal128(42, 0), "42");
        assert_eq!(format_decimal128(0, 2), "0.00");
    }

    // --- DESCRIBE QUERY result parsing tests ---

    #[test]
    fn test_describe_result_to_schema() {
        let names = StringArray::from(vec!["id", "name", "amount"]);
        let types = StringArray::from(vec!["INT", "STRING", "DECIMAL(10,2)"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(names) as Arc<dyn Array>, Arc::new(types)],
        )
        .unwrap();

        let reader = arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            Arc::new(Schema::new(vec![
                Field::new("col_name", DataType::Utf8, false),
                Field::new("data_type", DataType::Utf8, false),
            ])),
        );

        let result_schema = describe_result_to_schema(reader).unwrap();
        assert_eq!(result_schema.fields().len(), 3);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(result_schema.field(1).name(), "name");
        assert_eq!(result_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(result_schema.field(2).name(), "amount");
        assert_eq!(
            result_schema.field(2).data_type(),
            &DataType::Decimal128(10, 2)
        );
    }

    // --- StatementParameter serialization test ---

    #[test]
    fn test_statement_parameter_serialization() {
        let params = vec![
            StatementParameter {
                name: None,
                ordinal: Some(1),
                param_type: "INT".to_string(),
                value: Some("100".to_string()),
            },
            StatementParameter {
                name: None,
                ordinal: Some(2),
                param_type: "STRING".to_string(),
                value: None,
            },
        ];

        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains(r#""ordinal":1"#));
        assert!(json.contains(r#""type":"INT""#));
        assert!(json.contains(r#""value":"100""#));
        assert!(json.contains(r#""value":null"#));
        // Positional params must not emit a `name` field — the
        // canonical SEA proto distinguishes named vs positional by
        // which field is set.
        assert!(!json.contains(r#""name""#), "json={json}");
    }

    #[test]
    fn test_named_statement_parameter_serialization() {
        // Named binding is the spec-required public form (canonical
        // proto marks `name` as openapi_required=true). When `name`
        // is set, `ordinal` must be omitted from the wire payload.
        let params = vec![StatementParameter {
            name: Some("n".to_string()),
            ordinal: None,
            param_type: "INT".to_string(),
            value: Some("42".to_string()),
        }];

        let json = serde_json::to_string(&params).unwrap();
        assert!(json.contains(r#""name":"n""#), "json={json}");
        assert!(json.contains(r#""type":"INT""#));
        assert!(json.contains(r#""value":"42""#));
        assert!(!json.contains(r#""ordinal""#), "json={json}");
    }

    // ─── TypedValue → SEA tests ─────────────────────────────────────

    #[test]
    fn typed_value_null_emits_string_type_with_no_value() {
        let p = typed_value_to_sea_parameter(1, &TypedValue::Null).unwrap();
        assert_eq!(p.ordinal, Some(1));
        assert_eq!(p.name, None);
        assert_eq!(p.param_type, "STRING");
        assert_eq!(p.value, None);
    }

    #[test]
    fn typed_value_primitives_emit_canonical_wire_form() {
        let cases = [
            (TypedValue::Bool(true), "BOOLEAN", "true"),
            (TypedValue::Int8(-1), "TINYINT", "-1"),
            (TypedValue::Int16(2), "SMALLINT", "2"),
            (TypedValue::Int32(3), "INT", "3"),
            (TypedValue::Int64(4), "BIGINT", "4"),
            (TypedValue::Float32(1.5), "FLOAT", "1.5"),
            (TypedValue::Float64(2.5), "DOUBLE", "2.5"),
            (TypedValue::String("hi".into()), "STRING", "hi"),
            (
                TypedValue::Interval("INTERVAL '1' DAY".into()),
                "INTERVAL",
                "INTERVAL '1' DAY",
            ),
        ];
        for (v, expect_type, expect_value) in cases {
            let p = typed_value_to_sea_parameter(7, &v).unwrap();
            assert_eq!(p.ordinal, Some(7));
            assert_eq!(p.param_type, expect_type, "type for {v:?}");
            assert_eq!(p.value.as_deref(), Some(expect_value), "value for {v:?}");
        }
    }

    #[test]
    fn typed_value_unsigned_promotes_to_wider_signed() {
        let cases = [
            (TypedValue::UInt8(255), "SMALLINT", "255"),
            (TypedValue::UInt16(65535), "INT", "65535"),
            (TypedValue::UInt32(4_000_000_000), "BIGINT", "4000000000"),
            (TypedValue::UInt64(42), "BIGINT", "42"),
        ];
        for (v, expect_type, expect_value) in cases {
            let p = typed_value_to_sea_parameter(1, &v).unwrap();
            assert_eq!(p.param_type, expect_type, "type for {v:?}");
            assert_eq!(p.value.as_deref(), Some(expect_value), "value for {v:?}");
        }
    }

    #[test]
    fn typed_value_binary_is_rejected_with_workaround_message() {
        use crate::kernel_error::ErrorCode;
        let err =
            typed_value_to_sea_parameter(1, &TypedValue::Binary(vec![0xCA, 0xFE])).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        // The error message must mention the workaround (hex+unhex)
        // so callers can fix their query without reading source.
        assert!(err.message.contains("unhex"), "{}", err.message);
    }

    #[test]
    fn named_typed_value_sets_name_and_clears_ordinal() {
        let p = named_typed_value_to_sea_parameter("n", &TypedValue::Int32(42)).unwrap();
        assert_eq!(p.name.as_deref(), Some("n"));
        assert_eq!(p.ordinal, None);
        assert_eq!(p.param_type, "INT");
        assert_eq!(p.value.as_deref(), Some("42"));
    }

    #[test]
    fn named_typed_value_binary_is_rejected() {
        use crate::kernel_error::ErrorCode;
        let err = named_typed_value_to_sea_parameter("b", &TypedValue::Binary(vec![0xCA, 0xFE]))
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("unhex"), "{}", err.message);
    }

    #[test]
    fn typed_value_decimal_emits_value_string_with_precision_scale() {
        // Precision and scale must be in the type name; otherwise
        // SEA defaults to DECIMAL(10,0) and rounds fractional
        // digits away.
        let p = typed_value_to_sea_parameter(
            1,
            &TypedValue::Decimal {
                value: "-123.45".into(),
                precision: 10,
                scale: 2,
            },
        )
        .unwrap();
        assert_eq!(p.param_type, "DECIMAL(10,2)");
        assert_eq!(p.value.as_deref(), Some("-123.45"));
    }

    #[test]
    fn typed_value_date_emits_yyyy_mm_dd() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 5, 7).unwrap();
        let p = typed_value_to_sea_parameter(1, &TypedValue::Date(d)).unwrap();
        assert_eq!(p.param_type, "DATE");
        assert_eq!(p.value.as_deref(), Some("2026-05-07"));
    }

    #[test]
    fn typed_value_timestamp_emits_iso8601_seconds_with_fraction() {
        let ts = chrono::NaiveDate::from_ymd_opt(2026, 5, 7)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let p = typed_value_to_sea_parameter(1, &TypedValue::Timestamp(ts)).unwrap();
        assert_eq!(p.param_type, "TIMESTAMP");
        assert_eq!(p.value.as_deref(), Some("2026-05-07 12:00:00"));
    }

    #[test]
    fn typed_value_timestamp_ntz_emits_distinct_type_name() {
        let ts = chrono::NaiveDate::from_ymd_opt(2026, 5, 7)
            .unwrap()
            .and_hms_micro_opt(12, 0, 0, 123_456)
            .unwrap();
        let p = typed_value_to_sea_parameter(1, &TypedValue::TimestampNtz(ts)).unwrap();
        assert_eq!(p.param_type, "TIMESTAMP_NTZ");
        // Sub-second precision round-trips with `%.f`.
        assert_eq!(p.value.as_deref(), Some("2026-05-07 12:00:00.123456"));
    }

    #[test]
    fn typed_value_timestamp_ltz_emits_utc_iso8601() {
        let ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            chrono::NaiveDate::from_ymd_opt(2026, 5, 7)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
            chrono::Utc,
        );
        let p = typed_value_to_sea_parameter(1, &TypedValue::TimestampLtz(ts)).unwrap();
        assert_eq!(p.param_type, "TIMESTAMP_LTZ");
        // `Z` suffix; ISO-8601 `T` separator (LTZ convention).
        assert_eq!(p.value.as_deref(), Some("2026-05-07T12:00:00Z"));
    }

    #[test]
    fn typed_value_variant_emits_json_pass_through() {
        // Object form.
        let json = r#"{"k":1,"nested":{"a":[true,null,"x"]}}"#;
        let p = typed_value_to_sea_parameter(1, &TypedValue::Variant(json.into())).unwrap();
        assert_eq!(p.param_type, "VARIANT");
        assert_eq!(p.value.as_deref(), Some(json));

        // Scalar JSON values are also valid VARIANT contents.
        for raw in [r#""hello""#, "42", "true", "null"] {
            let p = typed_value_to_sea_parameter(1, &TypedValue::Variant(raw.into())).unwrap();
            assert_eq!(p.param_type, "VARIANT");
            assert_eq!(p.value.as_deref(), Some(raw));
        }
    }

    #[test]
    fn typed_value_geography_with_srid_emits_ewkt_and_decorated_type() {
        let p = typed_value_to_sea_parameter(
            1,
            &TypedValue::Geography {
                wkt: "POINT(1 2)".into(),
                srid: Some(4326),
                wkb: None,
            },
        )
        .unwrap();
        assert_eq!(p.param_type, "GEOGRAPHY(4326)");
        assert_eq!(p.value.as_deref(), Some("SRID=4326;POINT(1 2)"));
    }

    #[test]
    fn typed_value_geography_without_srid_emits_bare_wkt() {
        let p = typed_value_to_sea_parameter(
            1,
            &TypedValue::Geography {
                wkt: "POINT(1 2)".into(),
                srid: None,
                wkb: None,
            },
        )
        .unwrap();
        assert_eq!(p.param_type, "GEOGRAPHY");
        assert_eq!(p.value.as_deref(), Some("POINT(1 2)"));
    }

    #[test]
    fn typed_value_geometry_with_srid_emits_ewkt() {
        let p = typed_value_to_sea_parameter(
            1,
            &TypedValue::Geometry {
                wkt: "LINESTRING(0 0,1 1)".into(),
                srid: Some(0),
                wkb: None,
            },
        )
        .unwrap();
        assert_eq!(p.param_type, "GEOMETRY(0)");
        assert_eq!(p.value.as_deref(), Some("SRID=0;LINESTRING(0 0,1 1)"));
    }

    #[test]
    fn typed_value_geometry_without_srid_emits_bare_wkt() {
        let p = typed_value_to_sea_parameter(
            1,
            &TypedValue::Geometry {
                wkt: "LINESTRING(0 0,1 1)".into(),
                srid: None,
                wkb: None,
            },
        )
        .unwrap();
        assert_eq!(p.param_type, "GEOMETRY");
        assert_eq!(p.value.as_deref(), Some("LINESTRING(0 0,1 1)"));
    }
}
