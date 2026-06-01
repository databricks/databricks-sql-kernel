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

//! JS↔napi parameter codec — parses a connector-style
//! `{ type, value }` pair into a kernel `TypedValue`.
//!
//! Mirrors `pyo3/src/params.rs`'s contract for the supported SQL types
//! so the two bindings agree on what the kernel sees on the wire. The
//! JS-side `DBSQLParameter` already stringifies values before crossing
//! the FFI (see `lib/DBSQLParameter.ts::toSparkParameter` in the Node
//! driver): the napi binding takes the `(sql_type, value_str)` form
//! directly to avoid a per-call JS-value → Rust-value reflection trip.
//!
//! Known divergence from pyo3: `GEOGRAPHY` / `GEOMETRY` are dispatched
//! to parsers in `pyo3/src/params.rs` (via `is_geo_type_name` →
//! `parse_geography` / `parse_geometry`) but have no arms here yet, so
//! they fall through to the generic `unsupported parameter SQL type`
//! error. Adding the geo arms to napi is tracked as a follow-up; the
//! `geo_falls_through_to_unsupported` test pins the current behaviour so
//! a future GEO-add is a deliberate change.
//!
//! Compound types (`ARRAY` / `MAP` / `STRUCT`) and BINARY are
//! explicitly rejected — the SEA wire does not accept either as a bound
//! parameter today, and we surface that as an `InvalidArgument` error
//! at bind time so the failure mode matches pyo3's `NotSupportedError`
//! shape rather than an opaque server-side execute error.

use chrono::{NaiveDate, NaiveDateTime};
use databricks_sql_kernel::{Error, Result, TypedValue};

/// JS-visible binding for a single positional parameter.
///
/// Shape mirrors the `TSparkParameter` wire object the Thrift backend
/// already emits via `DBSQLParameter.toSparkParameter()` — `type` is the
/// canonical Databricks SQL type name (`"INT"`, `"STRING"`,
/// `"DECIMAL(10,2)"`, ...), `value` is the string-encoded literal or
/// `None` for SQL NULL.
///
/// Why a string for `value` instead of a tagged JS union: round-tripping
/// arbitrary JS values across the FFI requires either (a) a custom
/// napi `FromNapiValue` per arm, or (b) a `serde_json::Value`-style
/// dynamic dispatch on the Rust side. The Node-driver adapter already
/// stringifies before calling the binding (see `DBSQLParameter` and the
/// existing pyo3 wrapper), so the string-in / string-parsed contract
/// adds no JS-side complexity and keeps the kernel-side validation in
/// one place.
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi(object)]
pub struct TypedValueInput {
    /// Canonical Databricks SQL type name. Case-insensitive for the
    /// simple variants; for DECIMAL the parenthesised form
    /// (`"DECIMAL(10,2)"`) is required so the kernel can extract
    /// precision/scale.
    pub sql_type: String,
    /// String-encoded value. `None` always produces `TypedValue::Null`
    /// regardless of `sql_type` — matches the connector's
    /// `VoidParameter` shape and the pyo3 binding's contract.
    pub value: Option<String>,
}

/// Parse a single `TypedValueInput` into a kernel `TypedValue`.
///
/// Public for cross-module use within the crate (the future
/// `Connection::execute_statement` overload will call this for each
/// entry in `ExecuteOptions.positional_params`).
pub(crate) fn parse_typed_value(sql_type: &str, value_str: Option<&str>) -> Result<TypedValue> {
    let s = match value_str {
        None => return Ok(TypedValue::Null),
        Some(s) => s,
    };

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
        // TIMESTAMP / TIMESTAMP_NTZ share `parse_timestamp` for naive
        // parsing; the NTZ arm re-wraps so the SEA wire payload emits
        // the explicit type-name. TIMESTAMP_LTZ has its own parser since
        // it carries a timezone offset.
        "TIMESTAMP" => parse_timestamp(s),
        "TIMESTAMP_NTZ" => parse_timestamp(s).map(|v| match v {
            TypedValue::Timestamp(ts) => TypedValue::TimestampNtz(ts),
            other => other,
        }),
        "TIMESTAMP_LTZ" => parse_timestamp_ltz(s),
        "INTERVAL" => Ok(TypedValue::Interval(s.to_owned())),
        // VARIANT — JSON text passes through verbatim. Same JSON-null
        // vs SQL-NULL distinction as the pyo3 codec: `value=None`
        // short-circuits to TypedValue::Null at the top of this
        // function; `value=Some("null")` lands here as
        // TypedValue::Variant("null") which is a JSON-null VARIANT
        // value, not SQL NULL.
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
/// those followed by `<` (generic) or `(` (function-style). Tighter
/// than a `starts_with("ARRAY")` test, which would also match unrelated
/// names like `ARRAYBUFFER` or `STRUCTURE`.
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
    // JS's `Date.prototype.toISOString()` always emits a trailing `Z`;
    // strip it before parsing since `TypedValue::Timestamp` is a naive
    // `NaiveDateTime` (the SEA wire encodes timestamps in UTC).
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

fn parse_timestamp_ltz(s: &str) -> Result<TypedValue> {
    use chrono::{DateTime, FixedOffset, Utc};

    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(s) {
        return Ok(TypedValue::TimestampLtz(dt.with_timezone(&Utc)));
    }
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
        return Ok(TypedValue::TimestampLtz(dt.with_timezone(&Utc)));
    }
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
    let (precision, scale) = extract_decimal_precision_scale(sql_type)?;
    validate_decimal_literal(value)?;
    Ok(TypedValue::Decimal {
        value: value.to_owned(),
        precision,
        scale,
    })
}

/// Reject DECIMAL value strings that don't represent a finite number.
/// Accepts an optional sign followed by an integer part (digits) and,
/// optionally, a dot and a fractional part — but requires at least one
/// digit on each side of the dot when a dot is present. Forms like
/// `42.`, `.5` and the bare sign `-` are rejected to match Databricks'
/// DECIMAL literal parser. Scientific notation (`1e5`) is disallowed
/// because it changes the implicit precision/scale and the connector
/// never emits it.
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
    // Databricks server spec. Mirrors pyo3 — see `pyo3/src/params.rs`
    // for the rationale.
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

    // ─── Basic-type unit tests ──────────────────────────────────────
    //
    // These mirror the pyo3 codec's tests so a future maintainer who
    // changes one binding's contract sees the other binding's test
    // fail. The 10-type matrix corresponds to the C5 cluster — see
    // `sea-workflow/decisions/2026-05-28-autonomous-parity-decisions.md`
    // row 2.5-2.10 / 3.18-3.22.

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
            parse_typed_value("INTEGER", Some("-42")).unwrap(),
            TypedValue::Int32(-42)
        ));
        assert!(matches!(
            parse_typed_value("BIGINT", Some("9999999999")).unwrap(),
            TypedValue::Int64(9999999999)
        ));
        assert!(matches!(
            parse_typed_value("LONG", Some("9999999999")).unwrap(),
            TypedValue::Int64(9999999999)
        ));
    }

    #[test]
    fn out_of_range_int_errors() {
        let e = parse_typed_value("TINYINT", Some("300")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
        let e = parse_typed_value("INT", Some("9999999999")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn floats_parse() {
        assert!(matches!(
            parse_typed_value("FLOAT", Some("3.14")).unwrap(),
            TypedValue::Float32(_)
        ));
        assert!(matches!(
            parse_typed_value("DOUBLE", Some("2.718281828459045")).unwrap(),
            TypedValue::Float64(_)
        ));
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
    fn string_passes_through() {
        match parse_typed_value("STRING", Some("hello world")).unwrap() {
            TypedValue::String(s) => assert_eq!(s, "hello world"),
            other => panic!("expected String, got {other:?}"),
        }
        assert!(matches!(
            parse_typed_value("VARCHAR", Some("x")).unwrap(),
            TypedValue::String(_)
        ));
        assert!(matches!(
            parse_typed_value("CHAR", Some("y")).unwrap(),
            TypedValue::String(_)
        ));
    }

    #[test]
    fn string_preserves_embedded_quotes() {
        // SEA wire-level escaping is the server's job; the codec must
        // not transform a string with an embedded single quote.
        match parse_typed_value("STRING", Some("test'quote")).unwrap() {
            TypedValue::String(s) => assert_eq!(s, "test'quote"),
            other => panic!("expected String, got {other:?}"),
        }
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
    fn timestamp_parses_naive_and_iso() {
        let a = parse_typed_value("TIMESTAMP", Some("2026-05-15 12:30:45")).unwrap();
        assert!(matches!(a, TypedValue::Timestamp(_)));
        let b = parse_typed_value("TIMESTAMP", Some("2026-05-15 12:30:45.123456")).unwrap();
        assert!(matches!(b, TypedValue::Timestamp(_)));
        let c = parse_typed_value("TIMESTAMP", Some("2026-05-15T12:30:45")).unwrap();
        assert!(matches!(c, TypedValue::Timestamp(_)));
    }

    #[test]
    fn timestamp_accepts_trailing_z() {
        // JS's `Date.prototype.toISOString()` always emits the trailing
        // `Z`. Confirms the strip is in place.
        let v = parse_typed_value("TIMESTAMP", Some("2026-05-15T12:30:45.123Z")).unwrap();
        assert!(matches!(v, TypedValue::Timestamp(_)));
        let v = parse_typed_value("TIMESTAMP", Some("2026-05-15T12:30:45Z")).unwrap();
        assert!(matches!(v, TypedValue::Timestamp(_)));
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
        let e = parse_typed_value("DECIMAL(10,2)", Some("not-a-number")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
        assert!(e.message.contains("DECIMAL"), "{}", e.message);
    }

    #[test]
    fn decimal_rejects_partial_dot_forms() {
        for v in ["42.", ".5", "-.5", "+.", "-", "+", "."] {
            let e = parse_typed_value("DECIMAL(10,4)", Some(v)).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{v:?}");
        }
    }

    #[test]
    fn decimal_rejects_out_of_bound_precision_and_scale() {
        for ty in [
            "DECIMAL(0,0)",
            "DECIMAL(39,0)",
            "DECIMAL(10,11)",
            "DECIMAL(10,-1)",
        ] {
            let e = parse_typed_value(ty, Some("1.0")).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{ty}");
        }
    }

    #[test]
    fn binary_is_rejected() {
        // BINARY is documented kernel-side as unsupported on the SEA
        // wire today; the Node-side adapter must use `unhex(?)` as a
        // workaround. The error message must contain "BINARY" so the
        // adapter's reviver can distinguish this from an arbitrary
        // bind failure.
        let e = parse_typed_value("BINARY", Some("cafe")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
        assert!(e.message.contains("BINARY"), "{}", e.message);
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
    fn compound_match_does_not_overreach() {
        // ARRAYBUFFER / MAPLE / STRUCTURE fall through to the generic
        // "unsupported parameter SQL type" message — they're not
        // compound types and not real Databricks types.
        for ty in ["ARRAYBUFFER", "MAPLE", "STRUCTURE"] {
            let e = parse_typed_value(ty, Some("x")).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{ty}");
            assert!(
                e.message.contains("unsupported"),
                "{ty} should fall through, got {}",
                e.message
            );
        }
    }

    #[test]
    fn geo_falls_through_to_unsupported() {
        // Known divergence from pyo3: GEOGRAPHY / GEOMETRY (bare and
        // SRID-decorated) have no arms in the napi codec, so they fall
        // through to the generic "unsupported parameter SQL type"
        // error. pyo3 dispatches these to parse_geography /
        // parse_geometry. Pinning the fall-through here makes a future
        // GEO-add a deliberate change rather than a silent contract
        // drift between the two bindings.
        for ty in ["GEOGRAPHY", "GEOGRAPHY(4326)", "GEOMETRY", "GEOMETRY(4326)"] {
            let e = parse_typed_value(ty, Some("POINT(0 0)")).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{ty}");
            assert!(
                e.message.contains("unsupported"),
                "{ty} should fall through to unsupported, got {}",
                e.message
            );
        }
    }

    #[test]
    fn lowercase_sql_type_works() {
        // DBSQLParameterType.* enum uppercases, but accept lowercase
        // for robustness against direct napi callers.
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
    fn unknown_type_errors() {
        let e = parse_typed_value("MAGIC", Some("x")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn timestamp_ntz_maps_to_distinct_variant() {
        let v = parse_typed_value("TIMESTAMP_NTZ", Some("2026-05-15 12:30:45")).unwrap();
        assert!(matches!(v, TypedValue::TimestampNtz(_)));
    }

    #[test]
    fn timestamp_ltz_parses_naive_offset_and_z_forms() {
        let v = parse_typed_value("TIMESTAMP_LTZ", Some("2026-05-15 12:30:45")).unwrap();
        let TypedValue::TimestampLtz(dt) = v else {
            panic!("expected TimestampLtz");
        };
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "2026-05-15T12:30:45Z"
        );
        let v = parse_typed_value("TIMESTAMP_LTZ", Some("2026-05-15T18:00:00+05:30")).unwrap();
        let TypedValue::TimestampLtz(dt) = v else {
            panic!("expected TimestampLtz");
        };
        assert_eq!(dt.format("%H:%M").to_string(), "12:30");
    }

    #[test]
    fn variant_passes_json_text_through() {
        let json = r#"{"k":1,"nested":{"a":[true,null]}}"#;
        match parse_typed_value("VARIANT", Some(json)).unwrap() {
            TypedValue::Variant(s) => assert_eq!(s, json),
            other => panic!("expected Variant, got {other:?}"),
        }
    }

    #[test]
    fn interval_passes_through() {
        match parse_typed_value("INTERVAL", Some("INTERVAL '1' DAY")).unwrap() {
            TypedValue::Interval(s) => assert_eq!(s, "INTERVAL '1' DAY"),
            other => panic!("expected Interval, got {other:?}"),
        }
    }

    // ─── 10-type matrix coverage check ──────────────────────────────
    //
    // The C5 cluster's headline contract is that all 10 basic types
    // surface via this codec. This test exercises each one with a
    // representative value that mirrors what the Node adapter will
    // send today.

    #[test]
    fn c5_ten_type_matrix_round_trips() {
        // (sql_type, value_str, predicate-on-the-decoded-TypedValue)
        // BINARY is intentionally NOT in the round-trip list — see
        // binary_is_rejected above for the documented contract.
        type MatrixCase = (&'static str, &'static str, fn(&TypedValue) -> bool);
        let cases: &[MatrixCase] = &[
            ("INT", "42", |v| matches!(v, TypedValue::Int32(42))),
            ("BIGINT", "9999999999", |v| {
                matches!(v, TypedValue::Int64(9999999999))
            }),
            ("FLOAT", "3.14", |v| matches!(v, TypedValue::Float32(_))),
            ("DOUBLE", "2.718281828459045", |v| {
                matches!(v, TypedValue::Float64(_))
            }),
            (
                "STRING",
                "hello world",
                |v| matches!(v, TypedValue::String(s) if s == "hello world"),
            ),
            ("BOOLEAN", "true", |v| matches!(v, TypedValue::Bool(true))),
            ("DATE", "2026-05-15", |v| matches!(v, TypedValue::Date(_))),
            ("TIMESTAMP", "2026-05-15 12:30:45", |v| {
                matches!(v, TypedValue::Timestamp(_))
            }),
            ("DECIMAL(10,2)", "-123.45", |v| {
                matches!(
                    v,
                    TypedValue::Decimal {
                        precision: 10,
                        scale: 2,
                        ..
                    }
                )
            }),
        ];
        for (sql_type, value, pred) in cases {
            let got = parse_typed_value(sql_type, Some(value))
                .unwrap_or_else(|e| panic!("{sql_type} {value:?} should parse: {e:?}"));
            assert!(
                pred(&got),
                "{sql_type} {value:?} decoded to unexpected variant: {got:?}"
            );
        }
        // BINARY — pin the contract: bind-time InvalidArgument.
        let e = parse_typed_value("BINARY", Some("cafe")).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }
}
