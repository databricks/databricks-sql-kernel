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

//! Parse a connector-style `(sql_type, value_str)` pair into a
//! kernel `TypedValue`.
//!
//! The PySQL connector hands us parameters as
//! ``TSparkParameter(type="INT", value=stringValue("42"))`` —
//! the value is always a string, and the SQL type carries the
//! intent. To avoid a Python ↔ Rust round-trip through native
//! Python types, the PyO3 surface takes the string form directly
//! and we parse it here.
//!
//! Compound types (``ARRAY`` / ``MAP`` / ``STRUCT``) and the
//! never-used named-binding mode are explicitly rejected — the
//! connector backend surfaces those as ``NotSupportedError``.

use chrono::{NaiveDate, NaiveDateTime};
use databricks_sql_kernel::{Error, Result, TypedValue};

/// Parse a Databricks SQL type name + string-encoded value into a
/// kernel ``TypedValue``.
///
/// ``value_str = None`` always produces ``TypedValue::Null``,
/// regardless of ``sql_type`` — matches the connector's
/// ``VoidParameter`` shape.
///
/// ``sql_type`` is the canonical SQL type name the connector
/// produces (``"INT"``, ``"BIGINT"``, ``"STRING"``, etc.). For
/// DECIMAL, the form is ``"DECIMAL(precision,scale)"``; we parse
/// the parenthesized form to populate ``TypedValue::Decimal``'s
/// precision/scale fields so the kernel doesn't have to default
/// them to ``DECIMAL(10, 0)`` on the wire (which loses fractional
/// digits — see `statement/params.rs` for the kernel-side
/// rationale).
///
/// Errors are ``Error::invalid_argument()`` so they surface to the
/// Python caller as ``KernelError(code='InvalidArgument', ...)``.
pub(crate) fn parse_typed_value(sql_type: &str, value_str: Option<&str>) -> Result<TypedValue> {
    let s = match value_str {
        None => return Ok(TypedValue::Null),
        Some(s) => s,
    };

    // Normalise to uppercase for the simple variants; DECIMAL keeps
    // its case-sensitive parenthesised form so we can extract
    // precision/scale.
    let upper = sql_type.to_ascii_uppercase();
    match upper.as_str() {
        "VOID" => Ok(TypedValue::Null),
        "BOOLEAN" | "BOOL" => parse_bool(s),
        "TINYINT" => parse_i8(s),
        "SMALLINT" => parse_i16(s),
        "INT" | "INTEGER" => parse_i32(s),
        "BIGINT" | "LONG" => parse_i64(s),
        "FLOAT" => parse_f32(s),
        "DOUBLE" => parse_f64(s),
        "STRING" | "VARCHAR" | "CHAR" => Ok(TypedValue::String(s.to_owned())),
        "BINARY" => Err(invalid(
            "BINARY parameters are not supported by the SEA wire today",
        )),
        "DATE" => parse_date(s),
        // The three timestamp variants map to three distinct TypedValue
        // arms so the SEA wire payload carries the exact type-name
        // string the server expects. `TIMESTAMP` and `TIMESTAMP_NTZ`
        // share `parse_timestamp` for the naive-datetime parsing; the
        // NTZ arm re-wraps as `TimestampNtz` so the SEA payload mapper
        // emits the explicit type-name. `TIMESTAMP_LTZ` has its own
        // parser since it carries a timezone offset.
        "TIMESTAMP" => parse_timestamp(s),
        "TIMESTAMP_NTZ" => parse_timestamp(s).map(|v| match v {
            TypedValue::Timestamp(ts) => TypedValue::TimestampNtz(ts),
            other => other,
        }),
        "TIMESTAMP_LTZ" => parse_timestamp_ltz(s),
        "INTERVAL" => Ok(TypedValue::Interval(s.to_owned())),
        // VARIANT — pass the JSON text through verbatim. We don't
        // validate parseability: the connector is expected to have
        // produced valid JSON (Python's `json.dumps`, JS's
        // `JSON.stringify`, etc.), and on a malformed payload the
        // server returns a typed error which the kernel surfaces
        // back to the caller.
        //
        // Note on the JSON-null vs SQL-NULL asymmetry: a caller
        // binding a JSON `null` value sends `value_str=Some("null")`
        // and lands here as `TypedValue::Variant("null")` — a VARIANT
        // column containing the JSON-null value. The SQL-NULL path is
        // distinct: `value_str=None` short-circuits at the top of
        // this function to `TypedValue::Null` regardless of
        // `sql_type`. Both are correct and intentionally different.
        // Don't "fix" this by collapsing `Variant("null")` into
        // `TypedValue::Null` — JSON-null is a meaningful VARIANT
        // payload that the server stores and reads back as a JSON
        // `null` literal, not as SQL NULL.
        "VARIANT" => Ok(TypedValue::Variant(s.to_owned())),
        _ if upper.starts_with("DECIMAL(") => parse_decimal(sql_type, s),
        _ if is_compound_type_name(&upper) => Err(invalid(
            "compound parameter types (ARRAY/MAP/STRUCT) are not yet supported",
        )),
        _ => Err(invalid_msg(format!(
            "unsupported parameter SQL type: {sql_type:?}"
        ))),
    }
}

/// True iff `upper` is one of ARRAY/MAP/STRUCT exactly, or one of
/// those followed by `<` (generic) or `(` (function-style). This is
/// tighter than a `starts_with("ARRAY")` test, which would also
/// match unrelated names like `ARRAYBUFFER` or `STRUCTURE`.
fn is_compound_type_name(upper: &str) -> bool {
    for prefix in ["ARRAY", "MAP", "STRUCT"] {
        if upper == prefix {
            return true;
        }
        if let Some(rest) = upper.strip_prefix(prefix) {
            if rest.starts_with('<') || rest.starts_with('(') {
                return true;
            }
        }
    }
    false
}

fn invalid(msg: &'static str) -> Error {
    Error::invalid_argument().message(msg)
}

fn invalid_msg(msg: String) -> Error {
    Error::invalid_argument().message(msg)
}

fn parse_bool(s: &str) -> Result<TypedValue> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(TypedValue::Bool(true)),
        "false" | "0" => Ok(TypedValue::Bool(false)),
        _ => Err(invalid_msg(format!("invalid BOOLEAN literal: {s:?}"))),
    }
}

fn parse_i8(s: &str) -> Result<TypedValue> {
    s.parse::<i8>()
        .map(TypedValue::Int8)
        .map_err(|e| invalid_msg(format!("invalid TINYINT literal {s:?}: {e}")))
}

fn parse_i16(s: &str) -> Result<TypedValue> {
    s.parse::<i16>()
        .map(TypedValue::Int16)
        .map_err(|e| invalid_msg(format!("invalid SMALLINT literal {s:?}: {e}")))
}

fn parse_i32(s: &str) -> Result<TypedValue> {
    s.parse::<i32>()
        .map(TypedValue::Int32)
        .map_err(|e| invalid_msg(format!("invalid INT literal {s:?}: {e}")))
}

fn parse_i64(s: &str) -> Result<TypedValue> {
    s.parse::<i64>()
        .map(TypedValue::Int64)
        .map_err(|e| invalid_msg(format!("invalid BIGINT literal {s:?}: {e}")))
}

fn parse_f32(s: &str) -> Result<TypedValue> {
    let v = s
        .parse::<f32>()
        .map_err(|e| invalid_msg(format!("invalid FLOAT literal {s:?}: {e}")))?;
    // `str::parse::<f32>()` returns ±inf for overflow ("1e500") and
    // NaN for the literals "nan"/"NaN", both of which the SEA wire
    // can't represent. Reject so bad literals fail at bind time
    // rather than producing a misleading row at execute time.
    if !v.is_finite() {
        return Err(invalid_msg(format!("non-finite FLOAT literal {s:?}")));
    }
    Ok(TypedValue::Float32(v))
}

fn parse_f64(s: &str) -> Result<TypedValue> {
    let v = s
        .parse::<f64>()
        .map_err(|e| invalid_msg(format!("invalid DOUBLE literal {s:?}: {e}")))?;
    if !v.is_finite() {
        return Err(invalid_msg(format!("non-finite DOUBLE literal {s:?}")));
    }
    Ok(TypedValue::Float64(v))
}

fn parse_date(s: &str) -> Result<TypedValue> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(TypedValue::Date)
        .map_err(|e| invalid_msg(format!("invalid DATE literal {s:?}: {e}")))
}

fn parse_timestamp(s: &str) -> Result<TypedValue> {
    // The connector's TimestampParameter stringifies via str(),
    // which on a Python datetime produces ``YYYY-MM-DD HH:MM:SS``
    // optionally with ``.microseconds``. Accept both with and
    // without sub-second precision.
    //
    // Also accept the trailing-``Z`` UTC marker that JavaScript's
    // ``Date.prototype.toISOString()`` always emits — strip it
    // before parsing since ``TypedValue::Timestamp`` is a naive
    // ``NaiveDateTime`` (the SEA wire encodes timestamps in UTC).
    let body = s.strip_suffix('Z').unwrap_or(s);
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for fmt in formats {
        if let Ok(ts) = NaiveDateTime::parse_from_str(body, fmt) {
            return Ok(TypedValue::Timestamp(ts));
        }
    }
    Err(invalid_msg(format!("invalid TIMESTAMP literal {s:?}")))
}

/// Parse a `TIMESTAMP_LTZ` literal into a tz-aware `DateTime<Utc>`.
///
/// Accepts three input shapes, all interpreted as UTC on the wire:
///
/// 1. Naive form (`"2026-05-15 12:30:45[.f]"` or T-separator) — the
///    value is taken as UTC. This matches what Python's
///    `datetime.utcnow().isoformat()` produces.
/// 2. Trailing `Z` (`"2026-05-15T12:30:45Z"`) — explicit UTC marker;
///    same semantics as #1 after stripping `Z`.
/// 3. Explicit offset (`"2026-05-15T12:30:45+05:30"`) — parsed and
///    converted to UTC, preserving the wall-clock-to-instant
///    relationship.
///
/// The naive form is what most connector code emits today; the
/// offset form covers callers using Python `datetime.astimezone(tz).isoformat()`.
fn parse_timestamp_ltz(s: &str) -> Result<TypedValue> {
    use chrono::{DateTime, FixedOffset, Utc};

    // Try explicit-offset (RFC 3339) form first — covers `+HH:MM` and
    // `Z` suffix in one call.
    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(s) {
        return Ok(TypedValue::TimestampLtz(dt.with_timezone(&Utc)));
    }
    // Also try the space-separated equivalent (`%Y-%m-%d %H:%M:%S%.f%:z`)
    // which Python's `datetime.astimezone().isoformat(sep=' ')`
    // happens to emit.
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
        return Ok(TypedValue::TimestampLtz(dt.with_timezone(&Utc)));
    }
    // Fall back to the naive forms `parse_timestamp` accepts, treating
    // them as UTC. Strip trailing `Z` first.
    let body = s.strip_suffix('Z').unwrap_or(s);
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for fmt in formats {
        if let Ok(naive) = NaiveDateTime::parse_from_str(body, fmt) {
            return Ok(TypedValue::TimestampLtz(
                DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc),
            ));
        }
    }
    Err(invalid_msg(format!("invalid TIMESTAMP_LTZ literal {s:?}")))
}

fn parse_decimal(sql_type: &str, value: &str) -> Result<TypedValue> {
    // Extract precision/scale from "DECIMAL(p,s)" / "DECIMAL(p)" /
    // "Decimal(p, s)".
    let (precision, scale) = extract_decimal_precision_scale(sql_type)?;
    // Validate the value itself parses as a finite decimal number,
    // so bad literals like ``bind_param(1, "abc", "DECIMAL(10,2)")``
    // fail at bind time rather than at execute. The kernel's
    // ``TypedValue::Decimal`` carries the value as a string and
    // doesn't re-validate.
    validate_decimal_literal(value)?;
    Ok(TypedValue::Decimal {
        value: value.to_owned(),
        precision,
        scale,
    })
}

/// Reject DECIMAL value strings that don't represent a finite
/// number. Accepts an optional sign followed by an integer part
/// (digits) and, optionally, a dot and a fractional part — but
/// requires at least one digit on each side of the dot when a dot
/// is present (``-123.45``, ``0``, ``123``). Forms like ``42.``,
/// ``.5`` and the bare sign ``-`` are rejected to match
/// Databricks' DECIMAL literal parser — they'd otherwise pass
/// bind-time validation here and surface as a server error at
/// execute, defeating the headline "bad literal fails at bind"
/// contract.
///
/// Scientific notation (``1e5``) is disallowed because it changes
/// the implicit precision/scale and the connector never emits it.
fn validate_decimal_literal(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return Err(invalid_msg(format!("invalid DECIMAL literal {value:?}")));
    }
    let mut i = 0;
    if matches!(bytes[i], b'+' | b'-') {
        i += 1;
    }
    let int_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    let int_digits = i - int_start;
    let mut has_dot = false;
    let mut frac_digits = 0;
    if i < bytes.len() && bytes[i] == b'.' {
        has_dot = true;
        i += 1;
        let frac_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        frac_digits = i - frac_start;
    }
    let well_formed = i == bytes.len() && int_digits >= 1 && (!has_dot || frac_digits >= 1);
    if !well_formed {
        return Err(invalid_msg(format!("invalid DECIMAL literal {value:?}")));
    }
    Ok(())
}

fn extract_decimal_precision_scale(sql_type: &str) -> Result<(u8, i8)> {
    // sql_type looks like "DECIMAL(10,2)", "DECIMAL(10, 2)",
    // "DECIMAL(38)" (scale defaults to 0), or "Decimal(10,2)"
    // depending on casing.
    let lparen = sql_type.find('(').ok_or_else(|| {
        invalid_msg(format!(
            "DECIMAL type missing precision/scale: {sql_type:?}"
        ))
    })?;
    let rparen = sql_type
        .rfind(')')
        .ok_or_else(|| invalid_msg(format!("DECIMAL type missing closing paren: {sql_type:?}")))?;
    if rparen <= lparen {
        return Err(invalid_msg(format!(
            "DECIMAL type has malformed parens: {sql_type:?}"
        )));
    }
    let inner = &sql_type[lparen + 1..rparen];
    let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
    let (precision_str, scale_str) = match parts.as_slice() {
        [p] => (*p, "0"),
        [p, s] => (*p, *s),
        _ => {
            return Err(invalid_msg(format!(
                "DECIMAL type must have one or two parameters: {sql_type:?}"
            )));
        }
    };
    let precision = precision_str
        .parse::<u8>()
        .map_err(|e| invalid_msg(format!("invalid DECIMAL precision {sql_type:?}: {e}")))?;
    let scale = scale_str
        .parse::<i8>()
        .map_err(|e| invalid_msg(format!("invalid DECIMAL scale {sql_type:?}: {e}")))?;
    // Kernel-side bind constraint, intentionally tighter than the
    // Databricks server spec. The server accepts precision in
    // ``1..=38`` and scale in ``-(38 - precision)..=precision``
    // (negative scale rounds to multiples of ``10^|s|``), but no
    // current Databricks connector emits negative-scale DECIMAL
    // parameters and the kernel's ``TypedValue::Decimal`` doesn't
    // round-trip them cleanly. Reject here so obvious junk like
    // ``DECIMAL(0,0)`` or ``DECIMAL(255,127)`` fails at bind time;
    // when a negative-scale caller materialises, widen this check
    // and the ``i8`` scale parse together.
    if !(1..=38).contains(&precision) {
        return Err(invalid_msg(format!(
            "DECIMAL precision must be in 1..=38: {sql_type:?}"
        )));
    }
    if scale < 0 || (scale as i16) > (precision as i16) {
        return Err(invalid_msg(format!(
            "DECIMAL scale must be in 0..=precision: {sql_type:?}"
        )));
    }
    Ok((precision, scale))
}

#[cfg(test)]
mod tests {
    use super::*;
    use databricks_sql_kernel::ErrorCode;

    // None → Null regardless of SQL type
    #[test]
    fn none_value_is_null() {
        assert!(matches!(
            parse_typed_value("INT", None).unwrap(),
            TypedValue::Null
        ));
        assert!(matches!(
            parse_typed_value("STRING", None).unwrap(),
            TypedValue::Null
        ));
        // Even a bad SQL type is fine when value is None — VOID-style.
        assert!(matches!(
            parse_typed_value("NOPE", None).unwrap(),
            TypedValue::Null
        ));
    }

    #[test]
    fn void_type_is_null() {
        assert!(matches!(
            parse_typed_value("VOID", Some("anything")).unwrap(),
            TypedValue::Null
        ));
    }

    #[test]
    fn boolean_parses_truthy_falsey() {
        assert!(matches!(
            parse_typed_value("BOOLEAN", Some("true")).unwrap(),
            TypedValue::Bool(true)
        ));
        assert!(matches!(
            parse_typed_value("BOOLEAN", Some("False")).unwrap(),
            TypedValue::Bool(false)
        ));
        assert!(matches!(
            parse_typed_value("BOOL", Some("1")).unwrap(),
            TypedValue::Bool(true)
        ));
        let e = parse_typed_value("BOOLEAN", Some("yes")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn integer_types_parse() {
        assert!(matches!(
            parse_typed_value("TINYINT", Some("127")).unwrap(),
            TypedValue::Int8(127)
        ));
        assert!(matches!(
            parse_typed_value("SMALLINT", Some("-100")).unwrap(),
            TypedValue::Int16(-100)
        ));
        assert!(matches!(
            parse_typed_value("INT", Some("42")).unwrap(),
            TypedValue::Int32(42)
        ));
        assert!(matches!(
            parse_typed_value("BIGINT", Some("9999999999")).unwrap(),
            TypedValue::Int64(9999999999)
        ));
    }

    #[test]
    fn out_of_range_int_errors() {
        let e = parse_typed_value("TINYINT", Some("300")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn floats_parse() {
        assert!(matches!(
            parse_typed_value("FLOAT", Some("3.14")).unwrap(),
            TypedValue::Float32(_)
        ));
        assert!(matches!(
            parse_typed_value("DOUBLE", Some("3.141592653589793")).unwrap(),
            TypedValue::Float64(_)
        ));
    }

    #[test]
    fn string_passes_through() {
        match parse_typed_value("STRING", Some("hello world")).unwrap() {
            TypedValue::String(s) => assert_eq!(s, "hello world"),
            other => panic!("expected String, got {other:?}"),
        }
        // VARCHAR / CHAR aliases.
        assert!(matches!(
            parse_typed_value("VARCHAR", Some("x")).unwrap(),
            TypedValue::String(_)
        ));
    }

    #[test]
    fn date_parses_iso() {
        let v = parse_typed_value("DATE", Some("2026-05-15")).unwrap();
        match v {
            TypedValue::Date(d) => {
                assert_eq!(d, NaiveDate::from_ymd_opt(2026, 5, 15).unwrap())
            }
            other => panic!("expected Date, got {other:?}"),
        }
        let e = parse_typed_value("DATE", Some("not-a-date")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn timestamp_parses_with_and_without_microseconds() {
        let a = parse_typed_value("TIMESTAMP", Some("2026-05-15 12:30:45")).unwrap();
        assert!(matches!(a, TypedValue::Timestamp(_)));
        let b = parse_typed_value("TIMESTAMP", Some("2026-05-15 12:30:45.123456")).unwrap();
        assert!(matches!(b, TypedValue::Timestamp(_)));
        // ISO-8601 T-separator variant (some Python datetime formatters use it).
        let c = parse_typed_value("TIMESTAMP", Some("2026-05-15T12:30:45")).unwrap();
        assert!(matches!(c, TypedValue::Timestamp(_)));
    }

    #[test]
    fn timestamp_ntz_maps_to_distinct_variant() {
        // Same literal as TIMESTAMP, but the type-name distinguishes:
        // TIMESTAMP_NTZ → TypedValue::TimestampNtz, not Timestamp.
        let v = parse_typed_value("TIMESTAMP_NTZ", Some("2026-05-15 12:30:45")).unwrap();
        assert!(matches!(v, TypedValue::TimestampNtz(_)));
        let v = parse_typed_value("TIMESTAMP_NTZ", Some("2026-05-15T12:30:45.123Z")).unwrap();
        assert!(matches!(v, TypedValue::TimestampNtz(_)));
    }

    #[test]
    fn timestamp_ltz_parses_naive_offset_and_z_forms() {
        // Naive form — interpreted as UTC.
        let v = parse_typed_value("TIMESTAMP_LTZ", Some("2026-05-15 12:30:45")).unwrap();
        let TypedValue::TimestampLtz(dt) = v else {
            panic!("expected TimestampLtz");
        };
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "2026-05-15T12:30:45Z"
        );

        // Trailing-Z form — equivalent UTC.
        let v = parse_typed_value("TIMESTAMP_LTZ", Some("2026-05-15T12:30:45Z")).unwrap();
        assert!(matches!(v, TypedValue::TimestampLtz(_)));

        // Explicit positive offset — wall-clock 18:00 in +05:30 →
        // 12:30 UTC. Confirms tz conversion (not just stripping).
        let v = parse_typed_value("TIMESTAMP_LTZ", Some("2026-05-15T18:00:00+05:30")).unwrap();
        let TypedValue::TimestampLtz(dt) = v else {
            panic!("expected TimestampLtz");
        };
        assert_eq!(dt.format("%H:%M").to_string(), "12:30");
    }

    #[test]
    fn timestamp_ltz_rejects_invalid_literal() {
        let e = parse_typed_value("TIMESTAMP_LTZ", Some("not-a-timestamp")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn variant_passes_json_text_through() {
        // Object form — verbatim through to TypedValue::Variant.
        let json = r#"{"k":1,"nested":{"a":[true,null]}}"#;
        let v = parse_typed_value("VARIANT", Some(json)).unwrap();
        match v {
            TypedValue::Variant(s) => assert_eq!(s, json),
            other => panic!("expected Variant, got {other:?}"),
        }
    }

    #[test]
    fn variant_accepts_scalar_json_values() {
        // VARIANT permits any JSON value, not just objects / arrays.
        // Note: `"null"` here is a JSON-null literal inside the VARIANT
        // (i.e. `TypedValue::Variant("null".into())`), NOT SQL NULL.
        // SQL NULL is reached via `value_str=None`, which short-
        // circuits to `TypedValue::Null` before this match arm. See
        // the comment on the `"VARIANT"` arm in `parse_typed_value`.
        for raw in [r#""hello""#, "42", "true", "null", "[1,2,3]"] {
            let v = parse_typed_value("VARIANT", Some(raw)).unwrap();
            assert!(matches!(v, TypedValue::Variant(_)), "case {raw}");
        }
    }

    #[test]
    fn variant_does_not_validate_json() {
        // The kernel doesn't parse VARIANT — malformed JSON is the
        // server's job to reject. Confirms we don't accidentally
        // pre-validate at the kernel boundary.
        let v = parse_typed_value("VARIANT", Some("not even close to json")).unwrap();
        assert!(matches!(v, TypedValue::Variant(_)));
    }

    #[test]
    fn decimal_extracts_precision_and_scale() {
        let v = parse_typed_value("DECIMAL(10,2)", Some("-123.45")).unwrap();
        match v {
            TypedValue::Decimal {
                value,
                precision,
                scale,
            } => {
                assert_eq!(value, "-123.45");
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
        // Spaces between p and s.
        let v = parse_typed_value("DECIMAL(8, 4)", Some("0.0001")).unwrap();
        match v {
            TypedValue::Decimal {
                precision, scale, ..
            } => {
                assert_eq!((precision, scale), (8, 4));
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn decimal_without_precision_errors() {
        let e = parse_typed_value("DECIMAL", Some("1.0")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn binary_is_rejected() {
        // BINARY is documented kernel-side as unsupported on the
        // SEA wire today; the connector backend will need its own
        // unhex(?) workaround if a user passes BinaryParameter.
        let e = parse_typed_value("BINARY", Some("cafe")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
        assert!(e.message.contains("BINARY"));
    }

    #[test]
    fn compound_types_are_rejected() {
        for ty in ["ARRAY", "ARRAY<INT>", "MAP", "STRUCT<a:INT>"] {
            let e = parse_typed_value(ty, Some("x")).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{ty}");
            assert!(e.message.contains("compound"), "{ty}: {}", e.message);
        }
    }

    #[test]
    fn unknown_type_errors() {
        let e = parse_typed_value("MAGIC", Some("x")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn lowercase_sql_type_works() {
        // The connector uppercases via DatabricksSupportedType.name,
        // but accept lowercase too for robustness.
        assert!(matches!(
            parse_typed_value("int", Some("1")).unwrap(),
            TypedValue::Int32(1)
        ));
        assert!(matches!(
            parse_typed_value("decimal(5,2)", Some("1.23")).unwrap(),
            TypedValue::Decimal { .. }
        ));
    }

    #[test]
    fn interval_passes_through() {
        match parse_typed_value("INTERVAL", Some("INTERVAL '1' DAY")).unwrap() {
            TypedValue::Interval(s) => assert_eq!(s, "INTERVAL '1' DAY"),
            other => panic!("expected Interval, got {other:?}"),
        }
    }

    #[test]
    fn float_overflow_and_nan_rejected() {
        for s in ["1e500", "-1e500", "nan", "NaN", "inf", "-Infinity"] {
            let e = parse_typed_value("FLOAT", Some(s)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "FLOAT {s:?}");
            let e = parse_typed_value("DOUBLE", Some(s)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "DOUBLE {s:?}");
        }
    }

    #[test]
    fn decimal_single_arg_defaults_scale_to_zero() {
        match parse_typed_value("DECIMAL(38)", Some("123456789")).unwrap() {
            TypedValue::Decimal {
                precision, scale, ..
            } => assert_eq!((precision, scale), (38, 0)),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn decimal_rejects_bad_value_literal() {
        // The headline contract — `bind_param(1, "abc", "DECIMAL(10,2)")`
        // must fail at bind time, not at execute.
        let e = parse_typed_value("DECIMAL(10,2)", Some("not-a-number")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
        assert!(e.message.contains("DECIMAL"), "{}", e.message);
    }

    #[test]
    fn decimal_accepts_canonical_value_literals() {
        // Only forms with at least one digit on each side of the dot
        // (or no dot at all). Databricks' DECIMAL literal parser
        // rejects "42." and ".5" — so do we, see the dedicated
        // rejection test below.
        for v in ["0", "-0", "-123.45", "0.0001", "+1.5", "123"] {
            let r = parse_typed_value("DECIMAL(10,4)", Some(v));
            assert!(r.is_ok(), "expected {v:?} to parse: {:?}", r.err());
        }
    }

    #[test]
    fn decimal_rejects_partial_dot_forms() {
        // "42." (no fractional digits) and ".5" (no integer digits)
        // round-trip past Rust's `str::parse::<f64>()` but the
        // Databricks server rejects them, so we reject at bind time
        // to keep the headline contract honest. The bare sign "-"
        // is the obvious adjacent case.
        for v in ["42.", ".5", "-.5", "+.", "-", "+", "."] {
            let e = parse_typed_value("DECIMAL(10,4)", Some(v)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{v:?}");
        }
    }

    #[test]
    fn decimal_rejects_out_of_bound_precision_and_scale() {
        for ty in [
            "DECIMAL(0,0)",   // precision below 1
            "DECIMAL(39,0)",  // precision above 38
            "DECIMAL(10,11)", // scale above precision
            "DECIMAL(10,-1)", // scale below 0
        ] {
            let e = parse_typed_value(ty, Some("1.0")).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{ty}");
        }
    }

    #[test]
    fn timestamp_accepts_trailing_z() {
        let v = parse_typed_value("TIMESTAMP", Some("2026-05-15T12:30:45.123Z")).unwrap();
        assert!(matches!(v, TypedValue::Timestamp(_)));
        let v = parse_typed_value("TIMESTAMP", Some("2026-05-15T12:30:45Z")).unwrap();
        assert!(matches!(v, TypedValue::Timestamp(_)));
    }

    #[test]
    fn compound_match_does_not_overreach() {
        // `ARRAYBUFFER` / `MAPLE` / `STRUCTURE` are not real
        // Databricks types, but the compound-rejection branch should
        // not claim them — they should fall through to the generic
        // "unsupported parameter SQL type" message.
        for ty in ["ARRAYBUFFER", "MAPLE", "STRUCTURE"] {
            let e = parse_typed_value(ty, Some("x")).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{ty}");
            assert!(
                e.message.contains("unsupported"),
                "{ty} should fall through, got {}",
                e.message
            );
        }
        // ARRAY/MAP/STRUCT with bracket or paren forms are still
        // recognised as compound types.
        for ty in ["ARRAY", "ARRAY<INT>", "ARRAY(INT)", "MAP<STRING,INT>"] {
            let e = parse_typed_value(ty, Some("x")).unwrap_err();
            assert!(
                e.message.contains("compound"),
                "{ty} should be compound, got {}",
                e.message
            );
        }
    }
}
