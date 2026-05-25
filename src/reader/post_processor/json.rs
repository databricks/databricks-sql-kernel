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

//! Compact JSON serialization of Arrow row values for complex types.
//!
//! The output matches what the Databricks server emits on the Thrift
//! wire when `complexTypesAsArrow=false` (and what the JDBC driver
//! produces from `DatabricksArray.toString()` / `DatabricksMap.toString()` /
//! `DatabricksStruct.toString()`):
//!
//! - no whitespace between separators (equivalent to
//!   `json.dumps(obj, separators=(',', ':'))`);
//! - SQL NULL → JSON `null` (bare, not the string `"null"`);
//! - strings double-quoted with standard JSON escaping;
//! - map keys stringified and always rendered quoted;
//! - VARIANT / GEOMETRY / GEOGRAPHY values are server-emitted text
//!   already, so they pass through verbatim.
//!
//! The entry point used by the post-processor is
//! [`append_json`], which writes one row's JSON form into an
//! existing `String` (so the outer column builder can append straight
//! into a `StringBuilder` without an intermediate allocation).

use crate::kernel_error::{Error, Result};
use crate::reader::metadata_keys;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType,
    IntervalYearMonthType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::Array;
use arrow_schema::{DataType, Field, TimeUnit};
use base64::Engine;
use std::fmt::Write as _;

/// Write the JSON form of `array[index]` into `out`.
///
/// `field` carries Databricks-specific metadata (VARIANT / GEOMETRY /
/// GEOGRAPHY hints) under `databricks.type_name`; we honour that to
/// decide whether a `Utf8` value should be quoted as a string or
/// passed through as already-JSON.
///
/// On unsupported Arrow types, returns an `Error::not_implemented`.
/// We never panic on the hot path — a typed error is propagated up to
/// the result reader, which surfaces to the caller.
pub(crate) fn append_json(
    out: &mut String,
    array: &dyn Array,
    index: usize,
    field: &Field,
) -> Result<()> {
    if array.is_null(index) {
        out.push_str("null");
        return Ok(());
    }

    match array.data_type() {
        DataType::Null => out.push_str("null"),
        DataType::Boolean => {
            let v = array.as_boolean().value(index);
            out.push_str(if v { "true" } else { "false" });
        }
        DataType::Int8 => write!(out, "{}", array.as_primitive::<Int8Type>().value(index)).unwrap(),
        DataType::Int16 => {
            write!(out, "{}", array.as_primitive::<Int16Type>().value(index)).unwrap()
        }
        DataType::Int32 => {
            write!(out, "{}", array.as_primitive::<Int32Type>().value(index)).unwrap()
        }
        DataType::Int64 => {
            write!(out, "{}", array.as_primitive::<Int64Type>().value(index)).unwrap()
        }
        DataType::UInt8 => {
            write!(out, "{}", array.as_primitive::<UInt8Type>().value(index)).unwrap()
        }
        DataType::UInt16 => {
            write!(out, "{}", array.as_primitive::<UInt16Type>().value(index)).unwrap()
        }
        DataType::UInt32 => {
            write!(out, "{}", array.as_primitive::<UInt32Type>().value(index)).unwrap()
        }
        DataType::UInt64 => {
            write!(out, "{}", array.as_primitive::<UInt64Type>().value(index)).unwrap()
        }
        DataType::Float32 => {
            let v = array.as_primitive::<Float32Type>().value(index);
            append_float(out, v as f64);
        }
        DataType::Float64 => {
            let v = array.as_primitive::<Float64Type>().value(index);
            append_float(out, v);
        }
        DataType::Decimal128(_, scale) => {
            let raw = array.as_primitive::<Decimal128Type>().value(index);
            out.push_str(&format_decimal128(raw, *scale));
        }
        DataType::Decimal256(_, scale) => {
            let raw = array.as_primitive::<Decimal256Type>().value(index);
            out.push_str(&format_decimal256(raw, *scale));
        }
        DataType::Utf8 => {
            let s = array.as_string::<i32>().value(index);
            append_utf8_value(out, s, field);
        }
        DataType::LargeUtf8 => {
            let s = array.as_string::<i64>().value(index);
            append_utf8_value(out, s, field);
        }
        DataType::Binary => {
            let bytes = array.as_binary::<i32>().value(index);
            append_quoted(out, &base64_encode(bytes));
        }
        DataType::LargeBinary => {
            let bytes = array.as_binary::<i64>().value(index);
            append_quoted(out, &base64_encode(bytes));
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    Error::internal().message("FixedSizeBinary downcast failed in json encoder")
                })?;
            append_quoted(out, &base64_encode(arr.value(index)));
        }
        DataType::Date32 => {
            let arr = array.as_primitive::<Date32Type>();
            let date = arr.value_as_date(index).unwrap_or(chrono::NaiveDate::MIN);
            append_quoted(out, &date.format("%Y-%m-%d").to_string());
        }
        DataType::Date64 => {
            let arr = array.as_primitive::<Date64Type>();
            let date = arr.value_as_date(index).unwrap_or(chrono::NaiveDate::MIN);
            append_quoted(out, &date.format("%Y-%m-%d").to_string());
        }
        DataType::Timestamp(unit, tz) => {
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
            // Render at the Arrow column's native subsecond precision
            // so we don't silently truncate microseconds/nanoseconds —
            // matches JDBC's `DatabricksArray.toString()` chain, which
            // honours the underlying type's precision.
            //
            // TIMESTAMP carries a tz on the Arrow side; TIMESTAMP_NTZ
            // does not. Match Thrift: tz'd → trailing 'Z', NTZ → no
            // offset suffix.
            let base = match unit {
                TimeUnit::Second => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
                TimeUnit::Millisecond => dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
                TimeUnit::Microsecond => dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
                TimeUnit::Nanosecond => dt.format("%Y-%m-%dT%H:%M:%S%.9f").to_string(),
            };
            let s = if tz.is_some() {
                format!("{base}Z")
            } else {
                base
            };
            append_quoted(out, &s);
        }
        DataType::Interval(_) => {
            // INTERVAL columns surface as either YEAR_MONTH or
            // DAY_TIME variants depending on the SQL kind. We render
            // the Databricks text form, matching JDBC's
            // `Period`/`Duration`-style output.
            let s = format_interval(array, index)?;
            append_quoted(out, &s);
        }
        DataType::Duration(_) => {
            let s = format_duration(array, index)?;
            append_quoted(out, &s);
        }
        DataType::List(inner_field) => {
            let arr = array.as_list::<i32>();
            let values = arr.value(index);
            append_array_json(out, values.as_ref(), inner_field)?;
        }
        DataType::LargeList(inner_field) => {
            let arr = array.as_list::<i64>();
            let values = arr.value(index);
            append_array_json(out, values.as_ref(), inner_field)?;
        }
        DataType::FixedSizeList(inner_field, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeListArray>()
                .ok_or_else(|| {
                    Error::internal().message("FixedSizeList downcast failed in json encoder")
                })?;
            let values = arr.value(index);
            append_array_json(out, values.as_ref(), inner_field)?;
        }
        DataType::Map(entries_field, _) => {
            let arr = array.as_map();
            let entries = arr.value(index);
            append_map_json(out, &entries, entries_field)?;
        }
        DataType::Struct(fields) => {
            let arr = array.as_struct();
            append_struct_json(out, arr, index, fields)?;
        }
        other => {
            return Err(Error::not_implemented().message(format!(
                "JSON stringification not implemented for Arrow type {other:?}"
            )));
        }
    }

    Ok(())
}

/// Decide if `field` says the column is one of the precise Databricks
/// types whose `Utf8` payload is **already** correctly formatted
/// (VARIANT carries JSON text, GEOMETRY/GEOGRAPHY carry WKT). In that
/// case we pass through verbatim, including the surrounding quoting
/// the server emitted; otherwise we treat it as a normal string and
/// JSON-escape it.
///
/// Known limitation: the manifest only attaches `databricks.type_name`
/// to top-level Arrow fields, so inner fields of `ARRAY<VARIANT>` /
/// `MAP<…, VARIANT>` / `STRUCT<v: VARIANT>` lack the metadata and the
/// nested element is treated as a plain `Utf8` and JSON-escaped
/// (`"\"{\\\"a\\\":1}\""` instead of `{"a":1}`). Same for nested
/// `GEOMETRY` / `GEOGRAPHY`. Producing the JDBC-matching form for
/// nested VARIANTs requires parsing the manifest's `type_text`
/// (e.g. `ARRAY<VARIANT>`) and propagating into inner Arrow fields;
/// tracked separately, see `tests::nested_variant_inside_list_is_escaped_known_limitation`.
fn append_utf8_value(out: &mut String, value: &str, field: &Field) {
    if let Some(type_name) = field.metadata().get(metadata_keys::TYPE_NAME) {
        match type_name.as_str() {
            // VARIANT is a JSON value already (e.g. `{"a":1}`,
            // `[1,2]`, `"foo"`, `null`); inserting another layer of
            // quoting would corrupt it.
            "VARIANT" => {
                out.push_str(value);
                return;
            }
            // GEOMETRY / GEOGRAPHY render as quoted WKT text. The
            // server delivers the raw WKT (without quotes); we add
            // the JSON quoting here.
            "GEOMETRY" | "GEOGRAPHY" => {
                append_quoted(out, value);
                return;
            }
            _ => {}
        }
    }
    append_quoted(out, value);
}

fn append_array_json(out: &mut String, values: &dyn Array, inner_field: &Field) -> Result<()> {
    out.push('[');
    let len = values.len();
    for i in 0..len {
        if i > 0 {
            out.push(',');
        }
        append_json(out, values, i, inner_field)?;
    }
    out.push(']');
    Ok(())
}

fn append_map_json(
    out: &mut String,
    entries: &arrow_array::StructArray,
    entries_field: &Field,
) -> Result<()> {
    out.push('{');
    let (key_field, value_field) = match entries_field.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => (fields[0].clone(), fields[1].clone()),
        other => {
            return Err(Error::internal()
                .message(format!("Map entries field has unexpected shape: {other:?}")));
        }
    };
    let keys = entries.column(0);
    let values = entries.column(1);
    for i in 0..entries.len() {
        if i > 0 {
            out.push(',');
        }
        // Map keys: render the value via the same rules and force
        // quoted-string output. For string keys this is a no-op; for
        // numeric keys it produces `"42":...` (matches JDBC).
        if keys.is_null(i) {
            // Null map keys are a server-side anomaly. Match JDBC,
            // which writes the literal string "null" (quoted) rather
            // than corrupting the JSON shape.
            out.push_str("\"null\"");
        } else {
            let mut key_buf = String::new();
            append_json(&mut key_buf, keys.as_ref(), i, &key_field)?;
            if key_buf.starts_with('"') {
                out.push_str(&key_buf);
            } else if key_buf == "null" {
                // A non-null key whose value rendered as bare `null`
                // can only come from `append_float` collapsing
                // NaN / ±Inf. Disambiguate from the actual null-key
                // case above by emitting the standard textual form.
                let f = float_key_value(keys.as_ref(), i);
                let text = match f {
                    Some(v) if v.is_nan() => "NaN",
                    Some(v) if v.is_sign_negative() => "-Infinity",
                    Some(_) => "Infinity",
                    None => "null",
                };
                append_quoted(out, text);
            } else {
                // Non-string key — wrap the rendered value in quotes.
                append_quoted(out, &key_buf);
            }
        }
        out.push(':');
        append_json(out, values.as_ref(), i, &value_field)?;
    }
    out.push('}');
    Ok(())
}

fn append_struct_json(
    out: &mut String,
    arr: &arrow_array::StructArray,
    row: usize,
    fields: &arrow_schema::Fields,
) -> Result<()> {
    out.push('{');
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        append_quoted(out, field.name());
        out.push(':');
        let column = arr.column(i);
        append_json(out, column.as_ref(), row, field)?;
    }
    out.push('}');
    Ok(())
}

fn append_quoted(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                // Other control chars → \uXXXX.
                write!(out, "\\u{:04x}", c as u32).unwrap();
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Return the float value at `index` for Float32/Float64 arrays, used
/// to disambiguate map keys whose `append_json` form collapsed to bare
/// `null` (NaN / ±Inf). Returns `None` for any other array type.
fn float_key_value(array: &dyn Array, index: usize) -> Option<f64> {
    match array.data_type() {
        DataType::Float32 => Some(array.as_primitive::<Float32Type>().value(index) as f64),
        DataType::Float64 => Some(array.as_primitive::<Float64Type>().value(index)),
        _ => None,
    }
}

fn append_float(out: &mut String, v: f64) {
    if v.is_nan() || v.is_infinite() {
        // NaN / ±Inf are not valid JSON; emit `null` to stay parseable.
        out.push_str("null");
        return;
    }
    // Rust's `{}` formatter for f64 uses the shortest round-trippable
    // representation, matching Java's `Double.toString`.
    write!(out, "{v}").unwrap();
}

fn format_decimal128(raw: i128, scale: i8) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    if scale < 0 {
        // Negative scale means the stored integer represents
        // `raw * 10^|scale|`. Databricks doesn't emit these in
        // practice, but if it ever does we render the magnified
        // integer with trailing zeros rather than silently dropping
        // the multiplier.
        let zeros = "0".repeat((-(scale as i32)) as usize);
        return format!("{raw}{zeros}");
    }
    let scale = scale as u32;
    let divisor = 10_i128.pow(scale);
    let sign = if raw < 0 { "-" } else { "" };
    let abs_raw = raw.unsigned_abs();
    let whole = abs_raw / divisor as u128;
    let frac = abs_raw % divisor as u128;
    format!("{sign}{whole}.{frac:0>width$}", width = scale as usize)
}

fn format_decimal256(raw: arrow_buffer::i256, scale: i8) -> String {
    // Defer to the arrow formatter — Decimal256 isn't trivially
    // splittable with native arithmetic.
    let s = raw.to_string();
    if scale == 0 {
        return s;
    }
    if scale < 0 {
        // Same multiplier-bearing rendering as Decimal128 negative
        // scale. Dormant in practice on Databricks.
        let zeros = "0".repeat((-(scale as i32)) as usize);
        return format!("{s}{zeros}");
    }
    let scale = scale as usize;
    // Insert a decimal point `scale` digits from the right of the
    // unsigned representation.
    let (sign, digits) = if let Some(rest) = s.strip_prefix('-') {
        ("-", rest.to_string())
    } else {
        ("", s)
    };
    if digits.len() <= scale {
        let pad = "0".repeat(scale - digits.len());
        format!("{sign}0.{pad}{digits}")
    } else {
        let split = digits.len() - scale;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    }
}

/// Base64-encode binary payloads inside JSON arrays/maps/structs.
///
/// Uses RFC 4648 standard encoding (with `=` padding) to match the
/// JDBC reference, whose `DatabricksArray.toString()` defers to
/// `java.util.Base64.getEncoder()` — that encoder is the standard
/// padded one. Switching to the URL-safe or no-padding variant here
/// would byte-mismatch the comparator.
fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn format_interval(array: &dyn Array, index: usize) -> Result<String> {
    match array.data_type() {
        DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => {
            let months = array.as_primitive::<IntervalYearMonthType>().value(index);
            let sign = if months < 0 { "-" } else { "" };
            let m = months.unsigned_abs();
            Ok(format!("{sign}{}-{}", m / 12, m % 12))
        }
        DataType::Interval(arrow_schema::IntervalUnit::DayTime) => {
            let v = array.as_primitive::<IntervalDayTimeType>().value(index);
            // arrow_buffer::IntervalDayTime fields: days (i32), milliseconds (i32).
            let days = v.days as i64;
            let millis = v.milliseconds as i64;
            Ok(format_day_time_interval(days, millis * 1_000_000))
        }
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => {
            let v = array
                .as_primitive::<IntervalMonthDayNanoType>()
                .value(index);
            // Arrow's MonthDayNano can carry all three components;
            // Databricks INTERVAL columns only populate one variant
            // at a time, so a non-zero `months` field unambiguously
            // identifies a YEAR_MONTH interval. The all-zero case
            // `(0, 0, 0)` is ambiguous between zero YEAR_MONTH and
            // zero DAY_TIME — we resolve it to DAY_TIME form
            // (`"0 00:00:00.000000000"`) since Databricks emits
            // explicit interval subtypes upstream (the dedicated
            // YearMonth Arrow type for YEAR_MONTH columns), so a
            // MonthDayNano column reaching here is in DAY_TIME
            // semantics by construction.
            if v.months != 0 && v.days == 0 && v.nanoseconds == 0 {
                let months = v.months;
                let sign = if months < 0 { "-" } else { "" };
                let m = months.unsigned_abs();
                Ok(format!("{sign}{}-{}", m / 12, m % 12))
            } else {
                Ok(format_day_time_interval(v.days as i64, v.nanoseconds))
            }
        }
        other => Err(Error::internal().message(format!("not an interval type: {other:?}"))),
    }
}

fn format_day_time_interval(days: i64, nanos: i64) -> String {
    let total_nanos: i128 = (days as i128) * 86_400 * 1_000_000_000 + nanos as i128;
    let negative = total_nanos < 0;
    let abs_nanos = total_nanos.unsigned_abs();
    let nanos_per_sec: u128 = 1_000_000_000;
    let total_secs = (abs_nanos / nanos_per_sec) as u64;
    let nanos_rem = (abs_nanos % nanos_per_sec) as u32;
    let d = total_secs / 86_400;
    let rem = total_secs % 86_400;
    let h = rem / 3_600;
    let m = (rem % 3_600) / 60;
    let s = rem % 60;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{d} {h:02}:{m:02}:{s:02}.{nanos_rem:09}")
}

fn format_duration(array: &dyn Array, index: usize) -> Result<String> {
    let nanos: i128 = match array.data_type() {
        DataType::Duration(TimeUnit::Second) => {
            (array.as_primitive::<DurationSecondType>().value(index) as i128) * 1_000_000_000
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            (array.as_primitive::<DurationMillisecondType>().value(index) as i128) * 1_000_000
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            (array.as_primitive::<DurationMicrosecondType>().value(index) as i128) * 1_000
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            array.as_primitive::<DurationNanosecondType>().value(index) as i128
        }
        other => {
            return Err(Error::internal().message(format!("not a duration type: {other:?}")));
        }
    };
    let total_nanos = nanos;
    let days = (total_nanos / (86_400 * 1_000_000_000)) as i64;
    let remainder = total_nanos - (days as i128) * 86_400 * 1_000_000_000;
    Ok(format_day_time_interval(days, remainder as i64))
}

/// Returns `true` when `dt` is an Arrow type that the JSON path
/// should stringify into a `Utf8` column. Primitive columns and
/// `Utf8`/`Binary` columns are pass-through unless their field
/// metadata marks them as VARIANT/GEOMETRY/GEOGRAPHY — which the
/// post-processor already keeps as `Utf8` either way (we only
/// stringify nested structure).
pub(crate) fn is_complex_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::{Int32Array, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Fields};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn field_of(dt: DataType) -> Field {
        Field::new("x", dt, true)
    }

    fn render(arr: &dyn Array, idx: usize, field: &Field) -> String {
        let mut s = String::new();
        append_json(&mut s, arr, idx, field).unwrap();
        s
    }

    #[test]
    fn primitives_render_bare() {
        assert_eq!(
            render(&Int32Array::from(vec![42]), 0, &field_of(DataType::Int32),),
            "42"
        );
        assert_eq!(
            render(
                &arrow_array::Float64Array::from(vec![2.5]),
                0,
                &field_of(DataType::Float64),
            ),
            "2.5"
        );
        assert_eq!(
            render(
                &arrow_array::BooleanArray::from(vec![true]),
                0,
                &field_of(DataType::Boolean),
            ),
            "true"
        );
    }

    #[test]
    fn null_renders_bare_null() {
        let arr = Int32Array::from(vec![None]);
        assert_eq!(render(&arr, 0, &field_of(DataType::Int32)), "null");
    }

    #[test]
    fn string_is_quoted_and_escaped() {
        let arr = StringArray::from(vec!["he said \"hi\"\n\t"]);
        assert_eq!(
            render(&arr, 0, &field_of(DataType::Utf8)),
            "\"he said \\\"hi\\\"\\n\\t\""
        );
    }

    #[test]
    fn float_nan_inf_become_null() {
        let arr = arrow_array::Float64Array::from(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
        for i in 0..3 {
            assert_eq!(render(&arr, i, &field_of(DataType::Float64)), "null");
        }
    }

    #[test]
    fn list_of_strings_renders_as_array() {
        let mut b = ListBuilder::new(StringBuilder::new());
        b.values().append_value("a");
        b.values().append_value("b");
        b.values().append_value("c");
        b.append(true);
        let arr = b.finish();
        let field = Field::new(
            "x",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );
        assert_eq!(render(&arr, 0, &field), "[\"a\",\"b\",\"c\"]");
    }

    #[test]
    fn empty_list_renders_as_empty_brackets() {
        let mut b = ListBuilder::new(StringBuilder::new());
        b.append(true);
        let arr = b.finish();
        let field = Field::new(
            "x",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );
        assert_eq!(render(&arr, 0, &field), "[]");
    }

    #[test]
    fn null_inside_list_renders_bare() {
        let mut b = ListBuilder::new(Int32Builder::new());
        b.values().append_value(1);
        b.values().append_null();
        b.values().append_value(3);
        b.append(true);
        let arr = b.finish();
        let field = Field::new(
            "x",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        );
        assert_eq!(render(&arr, 0, &field), "[1,null,3]");
    }

    #[test]
    fn struct_renders_with_field_order_and_quoting() {
        let fields: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]
        .into();
        let arr = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["foo"])) as _,
                Arc::new(Int32Array::from(vec![1])) as _,
            ],
            None,
        );
        let field = Field::new("x", DataType::Struct(fields), true);
        assert_eq!(render(&arr, 0, &field), "{\"a\":\"foo\",\"b\":1}");
    }

    #[test]
    fn map_string_string_renders() {
        let mut b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        b.keys().append_value("k1");
        b.values().append_value("v1");
        b.keys().append_value("k2");
        b.values().append_value("v2");
        b.append(true).unwrap();
        let arr = b.finish();
        let entries_field = match arr.data_type() {
            DataType::Map(ef, _) => (**ef).clone(),
            _ => unreachable!(),
        };
        let field = Field::new("x", DataType::Map(Arc::new(entries_field), false), true);
        assert_eq!(render(&arr, 0, &field), "{\"k1\":\"v1\",\"k2\":\"v2\"}");
    }

    #[test]
    fn variant_pass_through_no_quoting() {
        let arr = StringArray::from(vec!["{\"a\":1}"]);
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert(metadata_keys::TYPE_NAME.to_string(), "VARIANT".to_string());
        let field = Field::new("x", DataType::Utf8, true).with_metadata(metadata);
        assert_eq!(render(&arr, 0, &field), "{\"a\":1}");
    }

    #[test]
    fn geometry_quoted_as_wkt() {
        let arr = StringArray::from(vec!["POINT (1.5 2.5)"]);
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert(metadata_keys::TYPE_NAME.to_string(), "GEOMETRY".to_string());
        let field = Field::new("x", DataType::Utf8, true).with_metadata(metadata);
        assert_eq!(render(&arr, 0, &field), "\"POINT (1.5 2.5)\"");
    }

    #[test]
    fn decimal128_renders_with_scale() {
        let arr = arrow_array::Decimal128Array::from(vec![9999_i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let field = field_of(DataType::Decimal128(10, 2));
        assert_eq!(render(&arr, 0, &field), "99.99");
    }

    #[test]
    fn binary_renders_base64() {
        let arr = arrow_array::BinaryArray::from(vec![b"hello".as_ref()]);
        let field = field_of(DataType::Binary);
        assert_eq!(render(&arr, 0, &field), "\"aGVsbG8=\"");
    }

    #[test]
    fn date32_renders_iso() {
        let arr = arrow_array::Date32Array::from(vec![19737]); // 2024-01-15
        let field = field_of(DataType::Date32);
        assert_eq!(render(&arr, 0, &field), "\"2024-01-15\"");
    }

    #[test]
    fn struct_field_null_renders_json_null() {
        let fields: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]
        .into();
        let arr = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["foo"])) as _,
                Arc::new(Int32Array::from(vec![None])) as _,
            ],
            None,
        );
        let field = Field::new("x", DataType::Struct(fields), true);
        assert_eq!(render(&arr, 0, &field), "{\"a\":\"foo\",\"b\":null}");
    }

    #[test]
    fn timestamp_microsecond_precision_preserved() {
        let arr = arrow_array::TimestampMicrosecondArray::from(vec![1_705_314_600_123_456_i64]);
        let field = field_of(DataType::Timestamp(TimeUnit::Microsecond, None));
        // 2024-01-15T10:30:00.123456 (NTZ → no Z).
        assert_eq!(render(&arr, 0, &field), "\"2024-01-15T10:30:00.123456\"");
    }

    #[test]
    fn timestamp_nanosecond_with_tz_keeps_nine_digits_and_z() {
        let arr = arrow_array::TimestampNanosecondArray::from(vec![1_705_314_600_123_456_789_i64])
            .with_timezone("UTC");
        let field = field_of(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        ));
        assert_eq!(
            render(&arr, 0, &field),
            "\"2024-01-15T10:30:00.123456789Z\""
        );
    }

    #[test]
    fn map_float_key_nan_renders_as_nan_not_null() {
        // MAP<DOUBLE, INT32> with NaN / +Inf / -Inf keys. Before the
        // fix, `append_float` collapsed each to bare `null` and the
        // map-key wrapper turned them into `"null"`, indistinguishable
        // from an actual null-key entry. The fix detects the
        // non-finite numeric key in the wrapper and emits the standard
        // textual form.
        use arrow_array::builder::Int32Builder;
        use arrow_array::builder::MapBuilder;
        let mut b = MapBuilder::new(
            None,
            arrow_array::builder::Float64Builder::new(),
            Int32Builder::new(),
        );
        b.keys().append_value(f64::NAN);
        b.values().append_value(1);
        b.keys().append_value(f64::INFINITY);
        b.values().append_value(2);
        b.keys().append_value(f64::NEG_INFINITY);
        b.values().append_value(3);
        b.append(true).unwrap();
        let arr = b.finish();
        let entries_field = match arr.data_type() {
            DataType::Map(ef, _) => (**ef).clone(),
            _ => unreachable!(),
        };
        let field = Field::new("x", DataType::Map(Arc::new(entries_field), false), true);
        assert_eq!(
            render(&arr, 0, &field),
            "{\"NaN\":1,\"Infinity\":2,\"-Infinity\":3}"
        );
    }

    #[test]
    fn nested_variant_inside_list_is_escaped_known_limitation() {
        // Known limitation pinned: ARRAY<VARIANT> inner fields don't
        // carry `databricks.type_name=VARIANT` because the manifest
        // augmenter only attaches it to top-level fields. Until we
        // propagate metadata into inner Arrow fields (would require
        // parsing the manifest's `type_text`), nested VARIANT elements
        // are JSON-escaped as plain strings rather than passed
        // through. This test exists to surface a behavior change
        // loudly if we ever fix the propagation.
        let mut b = ListBuilder::new(StringBuilder::new());
        b.values().append_value("{\"a\":1}");
        b.append(true);
        let arr = b.finish();
        // Mark the OUTER field as VARIANT — has no effect on inner
        // rendering, which is the bug.
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert(metadata_keys::TYPE_NAME.to_string(), "VARIANT".to_string());
        let field = Field::new(
            "x",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )
        .with_metadata(metadata);
        assert_eq!(render(&arr, 0, &field), "[\"{\\\"a\\\":1}\"]");
    }
}
