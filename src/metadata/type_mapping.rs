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

use crate::config::GeospatialResultRepresentation;
use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

// ─── JDBC `Types` integer codes ─────────────────────────────────────
//
// Single source of truth for Databricks query-column mappings. The static
// `getTypeInfo` catalogue reuses these constants where its Thrift contract
// agrees. One intentional exception is VOID: query columns surface as
// VARCHAR (12), while Hive's type catalogue reports `java.sql.Types.NULL`
// (0). The two metadata operations describe different contracts, so do not
// force structural equality between them.
//
// Values are `i16` because JDBC `Types` codes fit in `short`; the
// `getTypeInfo` Arrow column happens to be `i32` so callers there
// cast at the use site.
//
// Mapping policy for the subset that diverges between Thrift and
// kernel: **match the Thrift backend's released behaviour** so the
// kernel can be a drop-in replacement without breaking existing
// SQLAlchemy / pandas-read-sql consumers. Where Thrift and kernel
// happen to agree (most types), this is a no-op restatement; where
// they differ, the comment notes the policy choice.

/// JDBC `BIT` (-7). Predates `JDBC_BOOLEAN`; kept for completeness
/// of the reference table. Databricks `BOOLEAN` maps to
/// [`JDBC_BOOLEAN`], not to `BIT` — see the mapper for the parity
/// rationale.
#[allow(dead_code)]
pub(crate) const JDBC_BIT: i16 = -7;
pub(crate) const JDBC_TINYINT: i16 = -6;
pub(crate) const JDBC_BIGINT: i16 = -5;
/// JDBC `LONGVARBINARY` (-4). Databricks has no `LONGVARBINARY`
/// type; kept here for the reference table only.
#[allow(dead_code)]
pub(crate) const JDBC_LONGVARBINARY: i16 = -4;
/// JDBC `VARBINARY` (-3). Databricks `VARBINARY` (treated as an
/// alias for `BINARY` in the SQL parser) maps to [`JDBC_BINARY`],
/// matching Thrift. Kept here for the reference table.
#[allow(dead_code)]
pub(crate) const JDBC_VARBINARY: i16 = -3;
pub(crate) const JDBC_BINARY: i16 = -2;
/// JDBC `LONGVARCHAR` (-1). Databricks `STRING` is unbounded so this
/// is technically the more accurate JDBC code, but Thrift emits
/// `JDBC_VARCHAR` (12) and existing Python clients consume that.
/// Kept here for completeness; the mapper uses `JDBC_VARCHAR`.
#[allow(dead_code)]
pub(crate) const JDBC_LONGVARCHAR: i16 = -1;
pub(crate) const JDBC_CHAR: i16 = 1;
/// JDBC `NUMERIC` (2). Databricks `NUMERIC` is an alias for
/// `DECIMAL`; both surfaces emit [`JDBC_DECIMAL`]. Kept for the
/// reference table.
#[allow(dead_code)]
pub(crate) const JDBC_NUMERIC: i16 = 2;
pub(crate) const JDBC_DECIMAL: i16 = 3;
pub(crate) const JDBC_INTEGER: i16 = 4;
pub(crate) const JDBC_SMALLINT: i16 = 5;
pub(crate) const JDBC_FLOAT: i16 = 6;
#[allow(dead_code)]
pub(crate) const JDBC_REAL: i16 = 7;
pub(crate) const JDBC_DOUBLE: i16 = 8;
pub(crate) const JDBC_VARCHAR: i16 = 12;
/// JDBC `BOOLEAN` (4.0). Databricks `BOOLEAN` maps here; the older
/// `JDBC_BIT` (-7) is supported for legacy callers that ask for it
/// explicitly but is **not** the value `getColumns` / `getTypeInfo`
/// surface for a Databricks `BOOLEAN` column — Thrift returns 16,
/// and we match.
pub(crate) const JDBC_BOOLEAN: i16 = 16;
pub(crate) const JDBC_DATE: i16 = 91;
pub(crate) const JDBC_TIMESTAMP: i16 = 93;
pub(crate) const JDBC_OTHER: i16 = 1111;
pub(crate) const JDBC_JAVA_OBJECT: i16 = 2000;
pub(crate) const JDBC_STRUCT: i16 = 2002;
pub(crate) const JDBC_ARRAY: i16 = 2003;

/// Map a Databricks SQL type spec to an Arrow `DataType`.
///
/// Accepts both bare names (e.g. `"INT"`) and full type-text shapes
/// emitted by SEA's `ColumnInfo.type_text` (e.g. `"DECIMAL(10,2)"`,
/// `"ARRAY<INT>"`, `"MAP<STRING,INT>"`, `"STRUCT<a:INT,b:STRING>"`).
/// The latter is the form the kernel uses to synthesise an Arrow schema
/// for empty-result responses where no Arrow IPC payload is available
/// — see `reader::ResultReaderFactory::extract_logical_schema_from_manifest`.
///
/// Unrecognised input falls back to `DataType::Utf8` rather than
/// panicking; the kernel always returns *some* schema even when the
/// type is unknown.
///
/// `GEOMETRY` / `GEOGRAPHY` map to `DataType::Utf8` in this legacy public
/// helper. The result reader uses the internal representation-aware mapper
/// below to apply the session's native/binary default or explicit string mode.
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
///
/// Like [`databricks_type_to_arrow`], this legacy public helper maps
/// geospatial leaves to the EWKT `DataType::Utf8` representation.
pub fn databricks_type_to_arrow_with_interval(
    type_text: &str,
    type_interval_type: Option<&str>,
) -> DataType {
    databricks_type_to_arrow_with_options(
        type_text,
        type_interval_type,
        GeospatialResultRepresentation::String,
    )
}

/// Manifest-aware mapping with an explicit physical representation for
/// `GEOMETRY` / `GEOGRAPHY` leaves.
///
/// SEA's structured manifest reports the logical geospatial type. This mapper
/// reconstructs its tagged `struct<srid,wkb>` schema when binary output is
/// requested, including for attachments whose IPC schema omits the tags. The
/// choice is applied recursively through ARRAY / MAP / STRUCT type text.
pub(crate) fn databricks_type_to_arrow_with_options(
    type_text: &str,
    type_interval_type: Option<&str>,
    geospatial_representation: GeospatialResultRepresentation,
) -> DataType {
    parse_type(type_text, type_interval_type, geospatial_representation, 0)
        .unwrap_or(DataType::Utf8)
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
    geospatial_representation: GeospatialResultRepresentation,
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
        "ARRAY" => parse_array(
            trimmed,
            type_interval_type,
            geospatial_representation,
            depth,
        )?,
        "MAP" => parse_map(
            trimmed,
            type_interval_type,
            geospatial_representation,
            depth,
        )?,
        "STRUCT" => parse_struct(
            trimmed,
            type_interval_type,
            geospatial_representation,
            depth,
        )?,
        "GEOMETRY" | "GEOGRAPHY" => match geospatial_representation {
            GeospatialResultRepresentation::String => DataType::Utf8,
            GeospatialResultRepresentation::Binary => geospatial_binary_arrow_type(trimmed)?,
        },
        // Untyped NULL (`SELECT NULL` / VOID) maps to `Utf8`, not
        // `DataType::Null`: the server reports it as STRING (and the
        // Thrift / JDBC reference exposes it as STRING), so an
        // empty-result VOID column reports the same type as a
        // `CAST(NULL AS STRING)` sibling — consistent with the live-data
        // path, which coerces the Arrow `Null` column to `Utf8` in the
        // result post-processor. See issue #203 (DATATYPE-041).
        "VOID" | "NULL" => DataType::Utf8,
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
    geospatial_representation: GeospatialResultRepresentation,
    depth: usize,
) -> Option<DataType> {
    let inner = inside_angle_brackets(s)?;
    let inner_dt = parse_type(
        inner,
        type_interval_type,
        geospatial_representation,
        depth + 1,
    )?;
    Some(DataType::List(Arc::new(Field::new_list_field(
        inner_dt, true,
    ))))
}

/// Parse `MAP<K, V>` into Arrow's canonical `Map(entries: Struct{key, value})`.
/// Field names `"entries"` / `"key"` / `"value"` match pyarrow.map_(k, v).
fn parse_map(
    s: &str,
    type_interval_type: Option<&str>,
    geospatial_representation: GeospatialResultRepresentation,
    depth: usize,
) -> Option<DataType> {
    let inner = inside_angle_brackets(s)?;
    let parts = split_top_level_commas(inner);
    if parts.len() != 2 {
        return None;
    }
    let key_dt = parse_type(
        parts[0],
        type_interval_type,
        geospatial_representation,
        depth + 1,
    )?;
    let val_dt = parse_type(
        parts[1],
        type_interval_type,
        geospatial_representation,
        depth + 1,
    )?;
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
/// Fields are nullable unless SEA's type text includes a `NOT NULL` qualifier.
fn parse_struct(
    s: &str,
    type_interval_type: Option<&str>,
    geospatial_representation: GeospatialResultRepresentation,
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
        let (type_text, nullable) = struct_field_nullability(part[colon_idx + 1..].trim());
        if name.is_empty() {
            return None;
        }
        let dt = parse_type(
            type_text,
            type_interval_type,
            geospatial_representation,
            depth + 1,
        )?;
        fields.push(Field::new(name, dt, nullable));
    }
    Some(DataType::Struct(Fields::from(fields)))
}

/// Remove only a complete, whitespace-delimited `NOT NULL` field suffix.
/// In particular, leave malformed/trailing geospatial qualifiers for the strict
/// type parser to reject rather than assigning them canonical Arrow tags.
fn struct_field_nullability(type_text: &str) -> (&str, bool) {
    if let Some((prefix, null)) = type_text.rsplit_once(char::is_whitespace) {
        if null.eq_ignore_ascii_case("NULL") {
            if let Some((sql_type, not)) = prefix.trim_end().rsplit_once(char::is_whitespace) {
                if not.eq_ignore_ascii_case("NOT") {
                    return (sql_type.trim_end(), false);
                }
            }
        }
    }
    (type_text, true)
}

/// Native Arrow representation emitted by DBR and Reyden.
///
/// Besides the `struct<srid,wkb>` layout, the `wkb` child carries the exact
/// family and type-level SRID tags that geo-aware Arrow consumers use to
/// distinguish this encoding from an ordinary user struct. `ANY` is tagged
/// with Reyden's `-1` sentinel; bare types use their SQL defaults.
pub(crate) fn geospatial_binary_arrow_type(type_text: &str) -> Option<DataType> {
    let upper = type_text.trim().to_ascii_uppercase();
    let (family, default_srid, qualifier) = if let Some(rest) = upper.strip_prefix("GEOGRAPHY") {
        ("geography", 4326, rest)
    } else {
        let rest = upper.strip_prefix("GEOMETRY")?;
        ("geometry", 0, rest)
    };
    let qualifier = qualifier.trim();
    let srid = if qualifier.is_empty() {
        default_srid
    } else {
        let value = qualifier.strip_prefix('(')?.strip_suffix(')')?.trim();
        if value == "ANY" {
            -1
        } else {
            value.parse::<i32>().ok()?
        }
    };
    let wkb_metadata = HashMap::from([
        (family.to_string(), "true".to_string()),
        ("srid".to_string(), srid.to_string()),
    ]);

    Some(DataType::Struct(Fields::from(vec![
        Field::new("srid", DataType::Int32, false),
        Field::new("wkb", DataType::Binary, false).with_metadata(wkb_metadata),
    ])))
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

/// Map a Databricks SQL type spec to the JDBC `Types` integer code,
/// matching the Thrift backend's `getColumns` surface for parity with
/// existing SQLAlchemy / pandas-read-sql consumers.
///
/// Accepts both bare names (e.g. `"INT"`) and the parameterised /
/// composite forms emitted by the server (`"DECIMAL(10,2)"`,
/// `"VARCHAR(255)"`, `"ARRAY<INT>"`, `"MAP<STRING,INT>"`,
/// `"STRUCT<a:INT>"`). The splitter strips the parameter or composite
/// tail to find the base name. **Whitespace is intentionally not a
/// delimiter** — qualified `"INTERVAL YEAR TO MONTH"` should not match
/// the bare-`"INTERVAL"` arm; it falls through to `JDBC_OTHER`, which
/// is what Thrift returns for the same input.
///
/// Unknown types fall back to [`JDBC_OTHER`] (1111) — Thrift's
/// behaviour. Earlier kernel versions claimed VARCHAR (12) for
/// unknowns, but that's silent misinformation.
pub fn databricks_type_to_xdbc(type_name: &str) -> i16 {
    let type_upper = type_name.to_uppercase();

    // Cut the base name at the first `<` (composite tail) or `(`
    // (parameter tail). Whitespace is *not* split — see fn-level doc.
    let split_at = type_upper
        .char_indices()
        .find(|(_, c)| matches!(c, '<' | '('))
        .map(|(i, _)| i);
    let base_type = match split_at {
        Some(i) => type_upper[..i].trim(),
        None => type_upper.trim(),
    };

    match base_type {
        // ── Boolean ──────────────────────────────────────────────
        // Thrift returns JDBC `BOOLEAN` (16). The older `BIT` (-7) is
        // semantically equivalent but predates JDBC 4.0; Thrift uses
        // `BOOLEAN` and that's the released contract consumers expect.
        "BOOLEAN" | "BOOL" => JDBC_BOOLEAN,
        // ── Integers ─────────────────────────────────────────────
        "TINYINT" | "BYTE" => JDBC_TINYINT,
        "SMALLINT" | "SHORT" => JDBC_SMALLINT,
        "INT" | "INTEGER" => JDBC_INTEGER,
        "BIGINT" | "LONG" => JDBC_BIGINT,
        // ── Floating point ───────────────────────────────────────
        // Thrift returns JDBC `FLOAT` (6) for Databricks `FLOAT`, not
        // `REAL` (7). Match it. `JDBC_REAL` stays a public const so
        // any consumer that explicitly asks for it can still find it.
        "FLOAT" | "REAL" => JDBC_FLOAT,
        "DOUBLE" => JDBC_DOUBLE,
        "DECIMAL" | "DEC" | "NUMERIC" => JDBC_DECIMAL,
        // ── Character ────────────────────────────────────────────
        // Databricks `STRING` is unbounded so `LONGVARCHAR` (-1) is
        // technically more accurate, but Thrift emits `VARCHAR` (12)
        // — and existing Python clients (SQLAlchemy column
        // introspection, pandas-read-sql) have been built against
        // that. Match Thrift for drop-in parity.
        "STRING" | "TEXT" => JDBC_VARCHAR,
        "VARCHAR" => JDBC_VARCHAR,
        "CHAR" => JDBC_CHAR,
        // ── Binary ───────────────────────────────────────────────
        // Thrift returns `BINARY` (-2), not `VARBINARY` (-3). Match.
        "BINARY" | "VARBINARY" => JDBC_BINARY,
        // ── Date / time ──────────────────────────────────────────
        "DATE" => JDBC_DATE,
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => JDBC_TIMESTAMP,
        // ── Composites ───────────────────────────────────────────
        "ARRAY" => JDBC_ARRAY,
        "MAP" => JDBC_JAVA_OBJECT,
        "STRUCT" => JDBC_STRUCT,
        // ── Interval ─────────────────────────────────────────────
        // **Bare** `"INTERVAL"` only — qualified forms like
        // `"INTERVAL YEAR TO MONTH"` deliberately don't match (the
        // splitter doesn't cut on whitespace) and fall through to
        // `JDBC_OTHER`, matching Thrift's behaviour for those.
        "INTERVAL" => JDBC_VARCHAR,
        // ── Untyped NULL / VOID ──────────────────────────────────
        // The server reports untyped NULL (`SELECT NULL`) as STRING and
        // the JDBC reference maps a NULL-named column to `Types.VARCHAR`
        // (DatabricksTypeUtil: `case NULL: return Types.VARCHAR`), so
        // report VARCHAR rather than the `JDBC_OTHER` sentinel — keeping
        // this surface consistent with the Arrow type (`Utf8`). See
        // issue #203 (DATATYPE-041).
        "VOID" | "NULL" => JDBC_VARCHAR,
        // ── Unknown ──────────────────────────────────────────────
        // `JDBC_OTHER` (1111) is the JDBC sentinel "I don't know what
        // this is". Earlier kernel versions returned `VARCHAR` here
        // which silently misrepresented variants / geo / qualified
        // intervals / future types as strings.
        _ => JDBC_OTHER,
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
    fn test_legacy_geospatial_type_mapping_uses_ewkt_string() {
        for type_text in [
            "GEOGRAPHY",
            "GEOGRAPHY(4326)",
            "GEOMETRY(ANY)",
            "geometry(3857)",
        ] {
            assert_eq!(
                databricks_type_to_arrow(type_text),
                DataType::Utf8,
                "{type_text}"
            );
        }
    }

    #[test]
    fn test_geospatial_binary_types_use_srid_wkb_struct() {
        for type_text in ["GEOGRAPHY(4326)", "GEOMETRY(ANY)", "geometry(3857)"] {
            let expected = geospatial_binary_arrow_type(type_text).unwrap();
            assert_eq!(
                databricks_type_to_arrow_with_options(
                    type_text,
                    None,
                    GeospatialResultRepresentation::Binary,
                ),
                expected,
                "{type_text}"
            );
        }

        let expected = geospatial_binary_arrow_type("GEOMETRY(ANY)").unwrap();
        let DataType::Struct(fields) = expected else {
            unreachable!("geo binary helper must return a struct")
        };
        assert_eq!(fields[0].name(), "srid");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].name(), "wkb");
        assert_eq!(fields[1].data_type(), &DataType::Binary);
        assert!(!fields[1].is_nullable());
        assert_eq!(
            fields[1].metadata().get("geometry").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            fields[1].metadata().get("srid").map(String::as_str),
            Some("-1")
        );

        let DataType::Struct(geography_fields) =
            geospatial_binary_arrow_type("GEOGRAPHY(4326)").unwrap()
        else {
            unreachable!("geo binary helper must return a struct")
        };
        assert_eq!(
            geography_fields[1]
                .metadata()
                .get("geography")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            geography_fields[1]
                .metadata()
                .get("srid")
                .map(String::as_str),
            Some("4326")
        );

        for malformed in [
            "GEOMETRY(garbage)",
            "GEOGRAPHY()",
            "GEOMETRY(4326) trailing",
        ] {
            assert_eq!(
                geospatial_binary_arrow_type(malformed),
                None,
                "a malformed logical type must not be assigned canonical geo tags"
            );
            assert_eq!(databricks_type_to_arrow(malformed), DataType::Utf8);
        }
    }

    #[test]
    fn test_geospatial_representation_applies_to_nested_type_text() {
        let binary = databricks_type_to_arrow_with_options(
            "STRUCT<geom:GEOMETRY(ANY), geogs:ARRAY<GEOGRAPHY(4326)>>",
            None,
            GeospatialResultRepresentation::Binary,
        );
        let string = databricks_type_to_arrow_with_options(
            "STRUCT<geom:GEOMETRY(ANY), geogs:ARRAY<GEOGRAPHY(4326)>>",
            None,
            GeospatialResultRepresentation::String,
        );

        let DataType::Struct(binary_fields) = binary else {
            panic!("expected outer struct")
        };
        assert_eq!(
            binary_fields[0].data_type(),
            &geospatial_binary_arrow_type("GEOMETRY(ANY)").unwrap()
        );
        let DataType::List(binary_items) = binary_fields[1].data_type() else {
            panic!("expected geography list")
        };
        assert_eq!(
            binary_items.data_type(),
            &geospatial_binary_arrow_type("GEOGRAPHY(4326)").unwrap()
        );

        let DataType::Struct(string_fields) = string else {
            panic!("expected outer struct")
        };
        assert_eq!(string_fields[0].data_type(), &DataType::Utf8);
        let DataType::List(string_items) = string_fields[1].data_type() else {
            panic!("expected geography list")
        };
        assert_eq!(string_items.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_nested_geospatial_fields_with_not_null_qualifiers() {
        for representation in [
            GeospatialResultRepresentation::Binary,
            GeospatialResultRepresentation::String,
        ] {
            let geo =
                |sql_type| databricks_type_to_arrow_with_options(sql_type, None, representation);
            let expected = DataType::Struct(Fields::from(vec![
                Field::new("geom", geo("GEOMETRY(ANY)"), false),
                Field::new(
                    "arr",
                    DataType::List(Arc::new(Field::new_list_field(
                        DataType::Struct(Fields::from(vec![Field::new(
                            "g",
                            geo("GEOGRAPHY(4326)"),
                            false,
                        )])),
                        true,
                    ))),
                    false,
                ),
                Field::new(
                    "map",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(Fields::from(vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new(
                                    "value",
                                    DataType::Struct(Fields::from(vec![Field::new(
                                        "g",
                                        geo("GEOMETRY(0)"),
                                        false,
                                    )])),
                                    true,
                                ),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
                Field::new("blob", DataType::Binary, false),
                Field::new("amount", DataType::Decimal128(10, 2), true),
            ]));
            assert_eq!(
                geo("STRUCT<geom: GEOMETRY(ANY) NOT NULL, \
                     arr: ARRAY<STRUCT<g: GEOGRAPHY(4326) not\t null>> NOT NULL, \
                     map: MAP<STRING,STRUCT<g: GEOMETRY(0) NOT NULL>>, \
                     blob: BINARY NOT NULL, amount: DECIMAL(10,2)>"),
                expected,
            );
        }

        for invalid in [
            "GEOMETRY(0) NOTNULL",
            "GEOMETRY(0)NOT NULL",
            "GEOMETRY(0) NOT NOT NULL",
            "GEOMETRY(0) NOT NULL trailing",
            "GEOMETRY(garbage) NOT NULL",
        ] {
            assert_eq!(
                databricks_type_to_arrow_with_options(
                    &format!("STRUCT<g:{invalid}>"),
                    None,
                    GeospatialResultRepresentation::Binary,
                ),
                DataType::Utf8,
                "malformed field type must not acquire geospatial tags: {invalid}",
            );
        }
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
        // Untyped NULL / VOID surfaces as the server-reported STRING
        // (`Utf8`), matching a `CAST(NULL AS STRING)` sibling and the
        // Thrift / JDBC reference — see issue #203 (DATATYPE-041).
        assert_eq!(databricks_type_to_arrow("VOID"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("NULL"), DataType::Utf8);
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
        // Codes here must match Thrift's `getColumns` surface for
        // parity. See the docstring on `databricks_type_to_xdbc` for
        // the policy choices that differ from a naive JDBC mapping
        // (BOOLEAN → 16 not -7; STRING → 12 not -1; BINARY → -2 not
        // -3; FLOAT → 6 not 7).
        assert_eq!(databricks_type_to_xdbc("BOOLEAN"), JDBC_BOOLEAN);
        assert_eq!(databricks_type_to_xdbc("BOOL"), JDBC_BOOLEAN);
        assert_eq!(databricks_type_to_xdbc("TINYINT"), JDBC_TINYINT);
        assert_eq!(databricks_type_to_xdbc("BYTE"), JDBC_TINYINT);
        assert_eq!(databricks_type_to_xdbc("SMALLINT"), JDBC_SMALLINT);
        assert_eq!(databricks_type_to_xdbc("SHORT"), JDBC_SMALLINT);
        assert_eq!(databricks_type_to_xdbc("INT"), JDBC_INTEGER);
        assert_eq!(databricks_type_to_xdbc("INTEGER"), JDBC_INTEGER);
        assert_eq!(databricks_type_to_xdbc("BIGINT"), JDBC_BIGINT);
        assert_eq!(databricks_type_to_xdbc("LONG"), JDBC_BIGINT);
        assert_eq!(databricks_type_to_xdbc("FLOAT"), JDBC_FLOAT);
        assert_eq!(databricks_type_to_xdbc("REAL"), JDBC_FLOAT);
        assert_eq!(databricks_type_to_xdbc("DOUBLE"), JDBC_DOUBLE);
        assert_eq!(databricks_type_to_xdbc("DECIMAL"), JDBC_DECIMAL);
        assert_eq!(databricks_type_to_xdbc("DEC"), JDBC_DECIMAL);
        assert_eq!(databricks_type_to_xdbc("NUMERIC"), JDBC_DECIMAL);
        assert_eq!(databricks_type_to_xdbc("STRING"), JDBC_VARCHAR);
        assert_eq!(databricks_type_to_xdbc("TEXT"), JDBC_VARCHAR);
        assert_eq!(databricks_type_to_xdbc("VARCHAR"), JDBC_VARCHAR);
        assert_eq!(databricks_type_to_xdbc("CHAR"), JDBC_CHAR);
        assert_eq!(databricks_type_to_xdbc("BINARY"), JDBC_BINARY);
        assert_eq!(databricks_type_to_xdbc("VARBINARY"), JDBC_BINARY);
        assert_eq!(databricks_type_to_xdbc("DATE"), JDBC_DATE);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP"), JDBC_TIMESTAMP);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP_NTZ"), JDBC_TIMESTAMP);
        assert_eq!(databricks_type_to_xdbc("TIMESTAMP_LTZ"), JDBC_TIMESTAMP);
        assert_eq!(databricks_type_to_xdbc("ARRAY"), JDBC_ARRAY);
        assert_eq!(databricks_type_to_xdbc("MAP"), JDBC_JAVA_OBJECT);
        assert_eq!(databricks_type_to_xdbc("STRUCT"), JDBC_STRUCT);
    }

    #[test]
    fn test_databricks_type_to_xdbc_with_params() {
        // Parameter-style tail (DECIMAL precision/scale, VARCHAR
        // length): splitter cuts on `(` to find the base name.
        assert_eq!(databricks_type_to_xdbc("DECIMAL(10,2)"), JDBC_DECIMAL);
        assert_eq!(databricks_type_to_xdbc("DECIMAL(38, 0)"), JDBC_DECIMAL);
        assert_eq!(databricks_type_to_xdbc("VARCHAR(255)"), JDBC_VARCHAR);
        assert_eq!(databricks_type_to_xdbc("CHAR(10)"), JDBC_CHAR);
    }

    #[test]
    fn test_databricks_type_to_xdbc_with_composite_brackets() {
        // Composite tail `<...>`: splitter cuts on `<` to find the
        // base name. Pre-fix the splitter only handled `(` so these
        // all fell through to the default code, hiding real
        // composite columns under "OTHER".
        assert_eq!(databricks_type_to_xdbc("ARRAY<INT>"), JDBC_ARRAY);
        assert_eq!(databricks_type_to_xdbc("ARRAY<STRING>"), JDBC_ARRAY);
        assert_eq!(databricks_type_to_xdbc("ARRAY<ARRAY<INT>>"), JDBC_ARRAY);
        assert_eq!(databricks_type_to_xdbc("MAP<STRING,INT>"), JDBC_JAVA_OBJECT);
        assert_eq!(
            databricks_type_to_xdbc("MAP<STRING, STRING>"),
            JDBC_JAVA_OBJECT
        );
        assert_eq!(databricks_type_to_xdbc("STRUCT<a:INT>"), JDBC_STRUCT);
        assert_eq!(
            databricks_type_to_xdbc("STRUCT<a:INT, b:STRING>"),
            JDBC_STRUCT
        );
    }

    #[test]
    fn test_databricks_type_to_xdbc_interval_handling() {
        // Bare `"INTERVAL"` maps to VARCHAR (12), matching the
        // Databricks JDBC driver's longstanding behaviour.
        assert_eq!(databricks_type_to_xdbc("INTERVAL"), JDBC_VARCHAR);
        // Qualified intervals (`INTERVAL YEAR TO MONTH`, `INTERVAL
        // DAY TO SECOND`) deliberately do NOT match the bare branch
        // — the splitter doesn't cut on whitespace. They fall
        // through to `JDBC_OTHER` (1111), matching Thrift. This used
        // to silently emit 12 here while Thrift reported 1111 —
        // 128 cell diffs in the comparator run that drove this fix.
        assert_eq!(
            databricks_type_to_xdbc("INTERVAL YEAR TO MONTH"),
            JDBC_OTHER
        );
        assert_eq!(
            databricks_type_to_xdbc("INTERVAL DAY TO SECOND"),
            JDBC_OTHER
        );
        assert_eq!(
            databricks_type_to_xdbc("INTERVAL HOUR TO MINUTE"),
            JDBC_OTHER
        );
    }

    #[test]
    fn test_databricks_type_to_xdbc_void_null_is_varchar() {
        // Untyped NULL / VOID reports VARCHAR (12), matching the server's
        // STRING type and the JDBC reference (`case NULL: Types.VARCHAR`)
        // — not the `JDBC_OTHER` sentinel. Issue #203 (DATATYPE-041).
        assert_eq!(databricks_type_to_xdbc("VOID"), JDBC_VARCHAR);
        assert_eq!(databricks_type_to_xdbc("NULL"), JDBC_VARCHAR);
    }

    #[test]
    fn test_databricks_type_to_xdbc_unknown_falls_back_to_other() {
        // The default arm now returns `JDBC_OTHER` (1111). Prior
        // behaviour claimed unknown types were VARCHAR (12), silently
        // misrepresenting variants, geo types, and future Databricks
        // types as strings. Matches Thrift.
        assert_eq!(databricks_type_to_xdbc("GEOGRAPHY"), JDBC_OTHER);
        assert_eq!(databricks_type_to_xdbc("GEOMETRY"), JDBC_OTHER);
        assert_eq!(databricks_type_to_xdbc("VARIANT"), JDBC_OTHER);
        assert_eq!(databricks_type_to_xdbc("OBJECT"), JDBC_OTHER);
        assert_eq!(databricks_type_to_xdbc("UNKNOWN_FUTURE_TYPE"), JDBC_OTHER);
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
        let deep = "ARRAY<".repeat(MAX_PARSE_DEPTH + 1) + "INT" + &">".repeat(MAX_PARSE_DEPTH + 1);
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
                assert!(
                    (-38..=38).contains(&s),
                    "scale must be in [-38,38], got {s}"
                );
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
