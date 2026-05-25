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

//! Databricks ↔ Arrow type mapping (single source of truth).
//!
//! This module is the canonical location for all Databricks SQL type name ↔ Arrow
//! DataType conversions. Both directions must be kept in sync:
//!
//! - `databricks_type_to_arrow()` — Databricks string → Arrow (for parsing server responses)
//! - `arrow_type_to_databricks()` — Arrow → Databricks string (for serializing parameters)
//! - `databricks_type_to_xdbc()` — Databricks string → XDBC/JDBC type code
//!
//! When adding a new type, update **both** mapping functions.

use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use std::sync::Arc;

/// Map a Databricks SQL type spec to an Arrow `DataType`.
///
/// Accepts both bare names (e.g. `"INT"`) and full type-text shapes
/// emitted by SEA's `ColumnInfo.type_text` (e.g. `"DECIMAL(10,2)"`,
/// `"ARRAY<INT>"`, `"MAP<STRING,INT>"`, `"STRUCT<a:INT,b:STRING>"`).
/// The latter is the form the kernel uses to synthesise an Arrow schema
/// for empty-result responses where no Arrow IPC payload is available
/// — see `reader::ResultReaderFactory::extract_schema_from_manifest`.
///
/// Unrecognised input falls back to `DataType::Utf8` rather than
/// panicking; the kernel always returns *some* schema even when the
/// type is unknown.
pub fn databricks_type_to_arrow(type_text: &str) -> DataType {
    databricks_type_to_arrow_with_interval(type_text, None)
}

/// Like [`databricks_type_to_arrow`] but accepts the manifest's
/// `type_interval_type` hint (`"YEAR_MONTH"` / `"DAY_TIME"`) to
/// disambiguate the Arrow `IntervalUnit`. Use this when calling
/// from the SEA manifest path; the hint resolves the YEAR-MONTH
/// vs DAY-TIME distinction that the wire payload otherwise encodes.
///
/// Caller passes `type_interval_type` from `ColumnInfo`; absent
/// callers (the public `databricks_type_to_arrow`) pass `None` and
/// the parser falls back to the `type_text` itself, which for
/// real SEA columns is `"INTERVAL YEAR TO MONTH"` /
/// `"INTERVAL DAY TO SECOND"`.
pub fn databricks_type_to_arrow_with_interval(
    type_text: &str,
    type_interval_type: Option<&str>,
) -> DataType {
    parse_type(type_text, type_interval_type, 0).unwrap_or(DataType::Utf8)
}

/// Maximum depth of nested composite types we'll parse before
/// falling back to `Utf8`. `type_text` is server-controlled input;
/// without a guard a deeply-nested ARRAY/MAP/STRUCT could
/// stack-overflow the kernel. 64 is well past any real-world type.
const MAX_PARSE_DEPTH: usize = 64;

/// Recursive entry point. Returns `None` only when parsing genuinely
/// fails (malformed composite text); callers convert that to
/// `DataType::Utf8` at the top level so the public function never
/// returns `None`.
///
/// `type_interval_type` is propagated through recursive calls but
/// only consulted at the `INTERVAL` arm — it has no meaning for
/// nested composites, so passing it down is harmless and lets the
/// caller specify it once.
fn parse_type(
    s: &str,
    type_interval_type: Option<&str>,
    depth: usize,
) -> Option<DataType> {
    if depth >= MAX_PARSE_DEPTH {
        return None;
    }
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Split base name from the parameter / composite tail. The base
    // ends at the first of:
    //   - `<` (composite: ARRAY<INT>, MAP<K,V>, STRUCT<a:T>)
    //   - `(` (parameterised primitive: DECIMAL(p,s))
    //   - whitespace (qualified primitive:
    //     "INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND" —
    //     the canonical SEA `type_text` for INTERVAL columns)
    // Whichever appears first wins.
    let split_at = trimmed
        .char_indices()
        .find(|(_, c)| matches!(c, '<' | '(' | ' ' | '\t'))
        .map(|(i, _)| i);
    let base = match split_at {
        Some(i) => trimmed[..i].trim(),
        None => trimmed,
    };
    let base_upper = base.to_uppercase();

    Some(match base_upper.as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TINYINT" | "BYTE" => DataType::Int8,
        "SMALLINT" | "SHORT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" | "LONG" => DataType::Int64,
        "FLOAT" | "REAL" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "DECIMAL" | "DEC" | "NUMERIC" => {
            let (precision, scale) = parse_decimal_params(trimmed);
            DataType::Decimal128(precision, scale)
        }
        "STRING" | "VARCHAR" | "CHAR" | "TEXT" => DataType::Utf8,
        "BINARY" | "VARBINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        // Databricks `TIMESTAMP` is tz-aware (logically `TIMESTAMP_LTZ`).
        // The non-empty wire schema (Arrow IPC from SEA) reports
        // `Timestamp(Microsecond, Some("Etc/UTC"))` for plain TIMESTAMP
        // columns; matching that here keeps the empty-result schema in
        // sync. `TIMESTAMP_NTZ` is the only no-timezone variant.
        "TIMESTAMP" | "TIMESTAMP_LTZ" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("Etc/UTC".into()))
        }
        "TIMESTAMP_NTZ" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "INTERVAL" => DataType::Interval(resolve_interval_unit(trimmed, type_interval_type)),
        "ARRAY" => parse_array(trimmed, type_interval_type, depth)?,
        "MAP" => parse_map(trimmed, type_interval_type, depth)?,
        "STRUCT" => parse_struct(trimmed, type_interval_type, depth)?,
        "VOID" | "NULL" => DataType::Null,
        _ => DataType::Utf8,
    })
}

/// Pick the Arrow `IntervalUnit` for an INTERVAL column. Prefers
/// the manifest's `type_interval_type` (canonical) and falls back
/// to keyword-matching the `type_text` ("YEAR" / "MONTH" → YearMonth;
/// anything else → DayTime). Bare `"INTERVAL"` with no qualifier
/// keeps the historical DayTime default.
fn resolve_interval_unit(type_text: &str, type_interval_type: Option<&str>) -> IntervalUnit {
    if let Some(hint) = type_interval_type {
        let h = hint.to_uppercase();
        if h.contains("YEAR") || h.contains("MONTH") {
            return IntervalUnit::YearMonth;
        }
        if h.contains("DAY") || h.contains("HOUR") || h.contains("MINUTE") || h.contains("SECOND") {
            return IntervalUnit::DayTime;
        }
    }
    // Fall back to type_text keywords. "INTERVAL YEAR TO MONTH" /
    // "INTERVAL DAY TO SECOND" are the canonical SEA forms.
    let up = type_text.to_uppercase();
    if up.contains("YEAR") || up.contains("MONTH") {
        IntervalUnit::YearMonth
    } else {
        IntervalUnit::DayTime
    }
}

/// Slice out the content between the outer `<...>` of a composite type
/// spec. Returns `None` if the brackets are missing or unbalanced.
fn inside_angle_brackets(s: &str) -> Option<&str> {
    let start = s.find('<')?;
    // Walk from the end to find the matching close — composite values
    // can nest, but the kernel's input is single-rooted so the outermost
    // `<...>` is everything from the first `<` to the last `>`.
    let end = s.rfind('>')?;
    if end <= start {
        return None;
    }
    Some(s[start + 1..end].trim())
}

/// Split a comma-separated list at depth-0 commas. `STRUCT<a:MAP<X,Y>,b:INT>`
/// has two top-level fields, not three; the inner `MAP<X,Y>` comma must
/// not split.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = Vec::new();
    let mut depth: i32 = 0;
    let mut last = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(s[last..i].trim());
                last = i + 1;
            }
            _ => {}
        }
    }
    parts.push(s[last..].trim());
    parts
}

/// Parse `ARRAY<inner>` into a `List(Field("item", inner, true))`.
/// Field name `"item"` matches arrow-rs's canonical default
/// (`Field::new_list_field`) and every other call site in this
/// kernel (`reader/post_processor/`). pyarrow's
/// `list_(t)` default also uses `"item"`. Using anything else would
/// make the empty-result schema fail field-level equality against
/// the non-empty Arrow IPC payload.
fn parse_array(
    s: &str,
    type_interval_type: Option<&str>,
    depth: usize,
) -> Option<DataType> {
    let inner = inside_angle_brackets(s)?;
    let inner_dt = parse_type(inner, type_interval_type, depth + 1)?;
    Some(DataType::List(Arc::new(Field::new_list_field(
        inner_dt, true,
    ))))
}

/// Parse `MAP<K, V>` into Arrow's canonical `Map(entries: Struct{key, value})`.
/// Field names `"entries"` / `"key"` / `"value"` match pyarrow.map_(k, v).
fn parse_map(
    s: &str,
    type_interval_type: Option<&str>,
    depth: usize,
) -> Option<DataType> {
    let inner = inside_angle_brackets(s)?;
    let parts = split_top_level_commas(inner);
    if parts.len() != 2 {
        return None;
    }
    let key_dt = parse_type(parts[0], type_interval_type, depth + 1)?;
    let val_dt = parse_type(parts[1], type_interval_type, depth + 1)?;
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", key_dt, false),
            Field::new("value", val_dt, true),
        ])),
        false,
    );
    Some(DataType::Map(Arc::new(entries), false))
}

/// Parse `STRUCT<name:type, name:type, ...>` into `Struct(Fields::from([Field; N]))`.
/// Each child field is nullable (SEA's manifest doesn't carry per-field
/// nullability) — same convention used elsewhere in the kernel.
fn parse_struct(
    s: &str,
    type_interval_type: Option<&str>,
    depth: usize,
) -> Option<DataType> {
    let inner = inside_angle_brackets(s)?;
    if inner.is_empty() {
        return Some(DataType::Struct(Fields::empty()));
    }
    let mut fields: Vec<Field> = Vec::new();
    for part in split_top_level_commas(inner) {
        // `name:type` — split on the first colon at depth 0. Names are
        // unquoted identifiers; types may contain colons inside
        // composites (none today, but be defensive).
        let colon_idx = depth_zero_colon(part)?;
        let name = part[..colon_idx].trim();
        let type_text = part[colon_idx + 1..].trim();
        if name.is_empty() {
            return None;
        }
        let dt = parse_type(type_text, type_interval_type, depth + 1)?;
        fields.push(Field::new(name, dt, true));
    }
    Some(DataType::Struct(Fields::from(fields)))
}

/// Find the first `:` at angle/paren depth 0 in a STRUCT field text.
fn depth_zero_colon(s: &str) -> Option<usize> {
    let mut depth: i32 = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ':' if depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Map Arrow DataType to Databricks SQL type name.
///
/// This is the reverse of `databricks_type_to_arrow()`. It produces the canonical
/// Databricks type name for a given Arrow type. Unsigned integer types are promoted
/// to the next wider signed type since Databricks SQL has no unsigned integers.
///
/// Used for serializing Arrow parameter values into SEA API requests.
pub fn arrow_type_to_databricks(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        // Unsigned types promoted to next wider signed type
        DataType::UInt8 => "SMALLINT".to_string(),
        DataType::UInt16 => "INT".to_string(),
        DataType::UInt32 => "BIGINT".to_string(),
        DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 => "FLOAT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Binary | DataType::LargeBinary => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::Null => "STRING".to_string(),
        _ => "STRING".to_string(),
    }
}

/// Map Databricks type to XDBC/JDBC type code.
pub fn databricks_type_to_xdbc(type_name: &str) -> i16 {
    let type_upper = type_name.to_uppercase();
    let base_type = type_upper.split('(').next().unwrap_or(&type_upper).trim();

    match base_type {
        "BOOLEAN" | "BOOL" => -7,                              // JDBC BIT
        "TINYINT" | "BYTE" => -6,                              // JDBC TINYINT
        "SMALLINT" | "SHORT" => 5,                             // JDBC SMALLINT
        "INT" | "INTEGER" => 4,                                // JDBC INTEGER
        "BIGINT" | "LONG" => -5,                               // JDBC BIGINT
        "FLOAT" | "REAL" => 6,                                 // JDBC FLOAT
        "DOUBLE" => 8,                                         // JDBC DOUBLE
        "DECIMAL" | "DEC" | "NUMERIC" => 3,                    // JDBC DECIMAL
        "STRING" | "TEXT" => -1,                               // JDBC LONGVARCHAR
        "VARCHAR" => 12,                                       // JDBC VARCHAR
        "CHAR" => 1,                                           // JDBC CHAR
        "BINARY" | "VARBINARY" => -3,                          // JDBC VARBINARY
        "DATE" => 91,                                          // JDBC DATE
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => 93, // JDBC TIMESTAMP
        "ARRAY" => 2003,                                       // JDBC ARRAY
        "MAP" => 2000,                                         // JDBC JAVA_OBJECT
        "STRUCT" => 2002,                                      // JDBC STRUCT
        "INTERVAL" => 12,                                      // JDBC VARCHAR (matches JDBC driver)
        _ => 12,                                               // Default to VARCHAR
    }
}

/// Parse precision and scale from DECIMAL(p,s) type string.
/// Defaults to DECIMAL(38,18) if not specified (Databricks default).
fn parse_decimal_params(type_name: &str) -> (u8, i8) {
    let default_precision = 38u8;
    let default_scale = 18i8;

    let Some(start) = type_name.find('(') else {
        return (default_precision, default_scale);
    };
    let Some(end) = type_name.find(')') else {
        return (default_precision, default_scale);
    };

    let params = &type_name[start + 1..end];
    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();

    let precision = parts
        .first()
        .and_then(|p| p.parse::<u8>().ok())
        .unwrap_or(default_precision);
    let scale = parts
        .get(1)
        .and_then(|s| s.parse::<i8>().ok())
        .unwrap_or(default_scale);

    // Clamp to legal `Decimal128` range. Arrow requires precision in
    // [1, 38] and scale in [-38, 38]; pathological server input
    // (`DECIMAL(0, 0)`, `DECIMAL(99, 50)`) would otherwise produce an
    // invalid Arrow type that panics in downstream array builders.
    let precision = precision.clamp(1, 38);
    let scale = scale.clamp(-38, 38);

    (precision, scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_databricks_type_to_arrow_primitives() {
        assert_eq!(databricks_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("BOOL"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("TINYINT"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("BYTE"), DataType::Int8);
        assert_eq!(databricks_type_to_arrow("SMALLINT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("SHORT"), DataType::Int16);
        assert_eq!(databricks_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("LONG"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("FLOAT"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("REAL"), DataType::Float32);
        assert_eq!(databricks_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(databricks_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("CHAR"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("BINARY"), DataType::Binary);
        assert_eq!(databricks_type_to_arrow("DATE"), DataType::Date32);
    }

    #[test]
    fn test_databricks_type_to_arrow_timestamps() {
        // Plain TIMESTAMP is tz-aware in Databricks (logical
        // TIMESTAMP_LTZ); matches what the non-empty wire path emits
        // via Arrow IPC. The tz string is "Etc/UTC" to match what SEA
        // produces — not bare "UTC" — so empty-result and non-empty
        // schemas are byte-equal.
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("Etc/UTC".into()))
        );
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP_LTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("Etc/UTC".into()))
        );
        // TIMESTAMP_NTZ is the only no-timezone variant.
        assert_eq!(
            databricks_type_to_arrow("TIMESTAMP_NTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_databricks_type_to_arrow_decimal() {
        assert_eq!(
            databricks_type_to_arrow("DECIMAL"),
            DataType::Decimal128(38, 18)
        );
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            databricks_type_to_arrow("DECIMAL(38, 0)"),
            DataType::Decimal128(38, 0)
        );
        assert_eq!(
            databricks_type_to_arrow("NUMERIC(5,3)"),
            DataType::Decimal128(5, 3)
        );
    }

    #[test]
    fn test_databricks_type_to_arrow_complex() {
        // Bare composite names (no `<...>` payload) fall back to Utf8.
        // This is the legacy behavior for callers that pass only
        // ColumnInfo.type_name; the structured composite shape only
        // arrives via type_text and gets parsed below.
        assert_eq!(databricks_type_to_arrow("ARRAY"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("MAP"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("STRUCT"), DataType::Utf8);
    }

    #[test]
    fn test_databricks_type_to_arrow_array_nested() {
        // ARRAY<INT> → List<Int32>. Field name is "item" — the
        // arrow-rs canonical default; matches the non-empty wire
        // payload and every other call site in this kernel.
        assert_eq!(
            databricks_type_to_arrow("ARRAY<INT>"),
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
        );
        // ARRAY<STRING>
        assert_eq!(
            databricks_type_to_arrow("ARRAY<STRING>"),
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
        );
        // Nested: ARRAY<ARRAY<INT>>
        assert_eq!(
            databricks_type_to_arrow("ARRAY<ARRAY<INT>>"),
            DataType::List(Arc::new(Field::new_list_field(
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            )))
        );
    }

    #[test]
    fn test_array_field_name_is_item_not_element() {
        // Lock in the field-name convention. The non-empty-result
        // schema (wire payload) uses "item"; if this regresses,
        // empty-result and non-empty-result schemas stop being
        // field-equal — directly defeats the empty-result schema-fix
        // goal of bucket A/E in the comparator parity tasklist.
        let dt = databricks_type_to_arrow("ARRAY<INT>");
        let DataType::List(field) = dt else {
            panic!("expected List, got {dt:?}");
        };
        assert_eq!(field.name(), "item", "list field name must be 'item'");
    }

    #[test]
    fn test_databricks_type_to_arrow_map() {
        // MAP<STRING, INT> → Map<Struct{key: Utf8, value: Int32}>
        let dt = databricks_type_to_arrow("MAP<STRING, INT>");
        match dt {
            DataType::Map(entries, sorted) => {
                assert!(!sorted);
                assert_eq!(entries.name(), "entries");
                let DataType::Struct(fields) = entries.data_type() else {
                    panic!("expected Struct inside Map");
                };
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "key");
                assert_eq!(fields[0].data_type(), &DataType::Utf8);
                assert!(!fields[0].is_nullable());
                assert_eq!(fields[1].name(), "value");
                assert_eq!(fields[1].data_type(), &DataType::Int32);
                assert!(fields[1].is_nullable());
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn test_databricks_type_to_arrow_map_nested() {
        // MAP<STRING, MAP<STRING, INT>> — top-level comma split must
        // ignore inner commas. Asserts the comma-depth tracking in
        // `split_top_level_commas` works.
        let dt = databricks_type_to_arrow("MAP<STRING, MAP<STRING, INT>>");
        let DataType::Map(entries, _) = dt else {
            panic!("expected Map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected Struct");
        };
        // The value field is itself a Map.
        let DataType::Map(_, _) = fields[1].data_type() else {
            panic!("expected inner Map");
        };
    }

    #[test]
    fn test_databricks_type_to_arrow_struct() {
        // STRUCT<x:INT,y:STRING>
        let dt = databricks_type_to_arrow("STRUCT<x:INT,y:STRING>");
        let DataType::Struct(fields) = dt else {
            panic!("expected Struct");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "x");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert!(fields[0].is_nullable());
        assert_eq!(fields[1].name(), "y");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_databricks_type_to_arrow_struct_whitespace_and_complex_fields() {
        // Whitespace tolerance + a STRUCT field whose type is itself a
        // composite. Confirms `depth_zero_colon` doesn't trip on inner
        // colons (there aren't any today, but be defensive) and that
        // whitespace inside <...> is trimmed.
        let dt = databricks_type_to_arrow("STRUCT< a : ARRAY<INT> , b : MAP<STRING, INT> >");
        let DataType::Struct(fields) = dt else {
            panic!("expected Struct");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "a");
        assert!(matches!(fields[0].data_type(), DataType::List(_)));
        assert_eq!(fields[1].name(), "b");
        assert!(matches!(fields[1].data_type(), DataType::Map(_, _)));
    }

    #[test]
    fn test_databricks_type_to_arrow_array_of_struct() {
        // ARRAY<STRUCT<x:INT,y:INT>> — mirrors the corpus
        // `array_of_structs_column`. Asserts the kernel produces the
        // same nested Arrow type the connector and Thrift use.
        let dt = databricks_type_to_arrow("ARRAY<STRUCT<x:INT,y:INT>>");
        let DataType::List(field) = dt else {
            panic!("expected List");
        };
        let DataType::Struct(fields) = field.data_type() else {
            panic!("expected Struct inside List");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "x");
        assert_eq!(fields[1].name(), "y");
    }

    #[test]
    fn test_databricks_type_to_arrow_struct_with_decimal_field() {
        // STRUCT<amt:DECIMAL(10,2)> — confirms the parser correctly
        // delegates back to the DECIMAL handler when a struct field's
        // type carries parameters.
        let dt = databricks_type_to_arrow("STRUCT<amt:DECIMAL(10,2)>");
        let DataType::Struct(fields) = dt else {
            panic!("expected Struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_databricks_type_to_arrow_malformed_falls_back_to_utf8() {
        // Unbalanced or partial composite text falls back to Utf8 rather
        // than panicking — the kernel must always produce *some* schema.
        assert_eq!(databricks_type_to_arrow("ARRAY<"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("MAP<STRING>"), DataType::Utf8);
        assert_eq!(
            databricks_type_to_arrow("STRUCT<missing_colon>"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_databricks_type_to_arrow_special() {
        assert_eq!(databricks_type_to_arrow("VOID"), DataType::Null);
        assert_eq!(databricks_type_to_arrow("NULL"), DataType::Null);
        assert_eq!(
            databricks_type_to_arrow("INTERVAL"),
            DataType::Interval(IntervalUnit::DayTime)
        );
    }

    #[test]
    fn test_databricks_type_to_arrow_unknown() {
        assert_eq!(databricks_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8);
    }

    #[test]
    fn test_databricks_type_to_arrow_case_insensitive() {
        assert_eq!(databricks_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("Int"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("String"), DataType::Utf8);
    }

    #[test]
    fn test_databricks_type_to_xdbc() {
        assert_eq!(databricks_type_to_xdbc("BOOLEAN"), -7);
        assert_eq!(databricks_type_to_xdbc("TINYINT"), -6);
        assert_eq!(databricks_type_to_xdbc("SMALLINT"), 5);
        assert_eq!(databricks_type_to_xdbc("INT"), 4);
        assert_eq!(databricks_type_to_xdbc("BIGINT"), -5);
        assert_eq!(databricks_type_to_xdbc("FLOAT"), 6);
        assert_eq!(databricks_type_to_xdbc("DOUBLE"), 8);
        assert_eq!(databricks_type_to_xdbc("DECIMAL"), 3);
        assert_eq!(databricks_type_to_xdbc("STRING"), -1);
        assert_eq!(databricks_type_to_xdbc("VARCHAR"), 12);
        assert_eq!(databricks_type_to_xdbc("CHAR"), 1);
        assert_eq!(databricks_type_to_xdbc("BINARY"), -3);
        assert_eq!(databricks_type_to_xdbc("DATE"), 91);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP"), 93);
        assert_eq!(databricks_type_to_xdbc("ARRAY"), 2003);
        assert_eq!(databricks_type_to_xdbc("MAP"), 2000);
        assert_eq!(databricks_type_to_xdbc("STRUCT"), 2002);
    }

    #[test]
    fn test_databricks_type_to_xdbc_with_params() {
        assert_eq!(databricks_type_to_xdbc("DECIMAL(10,2)"), 3);
        assert_eq!(databricks_type_to_xdbc("VARCHAR(255)"), 12);
    }

    #[test]
    fn test_parse_decimal_params() {
        assert_eq!(parse_decimal_params("DECIMAL(10,2)"), (10, 2));
        assert_eq!(parse_decimal_params("DECIMAL(38, 0)"), (38, 0));
        assert_eq!(parse_decimal_params("DECIMAL"), (38, 18));
        assert_eq!(parse_decimal_params("NUMERIC(5,3)"), (5, 3));
    }

    #[test]
    fn test_arrow_to_databricks_primitives() {
        assert_eq!(arrow_type_to_databricks(&DataType::Boolean), "BOOLEAN");
        assert_eq!(arrow_type_to_databricks(&DataType::Int8), "TINYINT");
        assert_eq!(arrow_type_to_databricks(&DataType::Int16), "SMALLINT");
        assert_eq!(arrow_type_to_databricks(&DataType::Int32), "INT");
        assert_eq!(arrow_type_to_databricks(&DataType::Int64), "BIGINT");
        assert_eq!(arrow_type_to_databricks(&DataType::Float32), "FLOAT");
        assert_eq!(arrow_type_to_databricks(&DataType::Float64), "DOUBLE");
        assert_eq!(arrow_type_to_databricks(&DataType::Utf8), "STRING");
        assert_eq!(arrow_type_to_databricks(&DataType::LargeUtf8), "STRING");
        assert_eq!(arrow_type_to_databricks(&DataType::Binary), "BINARY");
        assert_eq!(arrow_type_to_databricks(&DataType::LargeBinary), "BINARY");
        assert_eq!(arrow_type_to_databricks(&DataType::Date32), "DATE");
        assert_eq!(arrow_type_to_databricks(&DataType::Date64), "DATE");
        assert_eq!(
            arrow_type_to_databricks(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_type_to_databricks(&DataType::Interval(IntervalUnit::DayTime)),
            "INTERVAL"
        );
        assert_eq!(
            arrow_type_to_databricks(&DataType::Decimal128(10, 2)),
            "DECIMAL(10,2)"
        );
        assert_eq!(arrow_type_to_databricks(&DataType::Null), "STRING");
    }

    #[test]
    fn test_arrow_to_databricks_unsigned_promotion() {
        assert_eq!(arrow_type_to_databricks(&DataType::UInt8), "SMALLINT");
        assert_eq!(arrow_type_to_databricks(&DataType::UInt16), "INT");
        assert_eq!(arrow_type_to_databricks(&DataType::UInt32), "BIGINT");
        assert_eq!(arrow_type_to_databricks(&DataType::UInt64), "BIGINT");
    }

    // ── INTERVAL type_text + type_interval_type plumbing ──────────────

    #[test]
    fn test_interval_real_wire_strings_resolve_to_correct_unit() {
        // Production SEA `type_text` is `"INTERVAL YEAR TO MONTH"` /
        // `"INTERVAL DAY TO SECOND"`. The parser must split on
        // whitespace to find the `INTERVAL` base.
        assert_eq!(
            databricks_type_to_arrow("INTERVAL YEAR TO MONTH"),
            DataType::Interval(IntervalUnit::YearMonth)
        );
        assert_eq!(
            databricks_type_to_arrow("INTERVAL DAY TO SECOND"),
            DataType::Interval(IntervalUnit::DayTime)
        );
        // Other DAY-TIME variants.
        assert_eq!(
            databricks_type_to_arrow("INTERVAL HOUR TO MINUTE"),
            DataType::Interval(IntervalUnit::DayTime)
        );
        assert_eq!(
            databricks_type_to_arrow("INTERVAL MINUTE TO SECOND"),
            DataType::Interval(IntervalUnit::DayTime)
        );
    }

    #[test]
    fn test_interval_type_interval_type_hint_wins() {
        // `type_interval_type` from the manifest is canonical and
        // overrides any keyword guessing from `type_text`. Mismatch is
        // hypothetical (server is consistent in practice), but the
        // hint precedence is the contract.
        assert_eq!(
            databricks_type_to_arrow_with_interval("INTERVAL", Some("YEAR_MONTH")),
            DataType::Interval(IntervalUnit::YearMonth)
        );
        assert_eq!(
            databricks_type_to_arrow_with_interval("INTERVAL", Some("DAY_TIME")),
            DataType::Interval(IntervalUnit::DayTime)
        );
        // Hint substring matching tolerates the qualifier-style form
        // (`"YEAR TO MONTH"`, `"DAY TO SECOND"`) too.
        assert_eq!(
            databricks_type_to_arrow_with_interval("INTERVAL", Some("YEAR TO MONTH")),
            DataType::Interval(IntervalUnit::YearMonth)
        );
    }

    #[test]
    fn test_interval_bare_falls_back_to_day_time() {
        // Backward-compat: bare `"INTERVAL"` with no hint and no
        // qualifier maps to DayTime (the historical default).
        assert_eq!(
            databricks_type_to_arrow("INTERVAL"),
            DataType::Interval(IntervalUnit::DayTime)
        );
    }

    // ── parse_type depth guard + DECIMAL clamps ──────────────────────

    #[test]
    fn test_parse_type_deeply_nested_falls_back_to_utf8() {
        // Server-controlled `type_text` could nest arbitrarily;
        // without a depth guard the recursive parser would blow the
        // stack. 64 levels of nesting is well past anything real.
        let deep = "ARRAY<".repeat(MAX_PARSE_DEPTH + 1)
            + "INT"
            + &">".repeat(MAX_PARSE_DEPTH + 1);
        assert_eq!(databricks_type_to_arrow(&deep), DataType::Utf8);
    }

    #[test]
    fn test_decimal_clamps_invalid_precision_scale() {
        // `Decimal128` requires precision in [1, 38] and scale in
        // [-38, 38]. Pathological server input (DECIMAL(0,0),
        // DECIMAL(99,50)) would otherwise produce an invalid Arrow
        // type that panics later. Clamp to legal range.
        let dt = databricks_type_to_arrow("DECIMAL(0,0)");
        match dt {
            DataType::Decimal128(p, s) => {
                assert!(p >= 1, "precision must clamp to >= 1, got {p}");
                assert!((-38..=38).contains(&s), "scale must be in [-38,38], got {s}");
            }
            other => panic!("expected Decimal128, got {other:?}"),
        }
        let dt = databricks_type_to_arrow("DECIMAL(99,50)");
        match dt {
            DataType::Decimal128(p, s) => {
                assert!(p <= 38, "precision must clamp to <= 38, got {p}");
                assert!(s <= 38, "scale must clamp to <= 38, got {s}");
            }
            other => panic!("expected Decimal128, got {other:?}"),
        }
    }
}
