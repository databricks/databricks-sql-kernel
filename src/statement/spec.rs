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

//! `StatementSpec` — plain data describing what to execute.
//!
//! Per `docs/spec-v0.md` addendum A1, the kernel splits the statement type
//! into four. `StatementSpec` is the data half: SQL, positional / named
//! parameters, per-statement conf overlay, and result format. It holds no
//! `Session` reference, is constructible in isolation, cheaply cloneable,
//! and is the single source of truth for what `Statement::execute()` /
//! `Statement::submit()` send on the wire.
//!
//! The `Statement<'s>` lifecycle owner exposes a `&mut StatementSpec` via
//! `Statement::spec(&mut self)` so callers mutate the spec in place
//! between executions.

use crate::kernel_error::{Error, ErrorCode, Result};
use std::collections::HashMap;

/// A typed value bound as a SQL parameter.
///
/// `#[non_exhaustive]` so v1+ variants stay non-breaking. The current
/// set mirrors what Databricks SEA accepts. `UInt*` variants are
/// accepted at the public API but promoted to the next wider signed
/// type at the wire boundary, matching `arrow_type_to_databricks`.
///
/// `Decimal::value` is a string in canonical decimal form
/// (e.g. `"-123.456"`) so the variant doesn't pull `rust_decimal`
/// into the public surface and wrappers can pass arbitrarily-precise
/// values without an intermediate type.
///
/// `Interval` is also a string in canonical SQL form
/// (e.g. `"INTERVAL '1' DAY"`) — that's the form SEA expects on the
/// wire.
///
/// **Timestamp variants.** Databricks SQL has three distinct
/// timestamp types and the kernel preserves the distinction:
///
/// - [`TypedValue::Timestamp`] — the historical `TIMESTAMP` type.
///   Wire form is naive (no timezone). Semantics historically vary
///   per session and DBR version; in practice modern sessions treat
///   it as equivalent to `TIMESTAMP_NTZ`. Kept for backwards
///   compatibility with existing wrappers; prefer
///   [`TypedValue::TimestampNtz`] for new code that wants explicit
///   "no-timezone" guarantees.
/// - [`TypedValue::TimestampNtz`] — `TIMESTAMP_NTZ`. Explicitly
///   "no timezone": the server stores the value as-is and does not
///   apply session-timezone interpretation on read.
/// - [`TypedValue::TimestampLtz`] — `TIMESTAMP_LTZ`. "Local time
///   zone": the value is anchored to UTC on the wire; the server
///   may apply session-timezone interpretation in some display
///   contexts. Represented as a tz-aware `chrono::DateTime<Utc>`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum TypedValue {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Decimal {
        value: String,
        precision: u8,
        scale: i8,
    },
    Date(chrono::NaiveDate),
    /// `TIMESTAMP` — see enum-level docs for the relationship to the
    /// NTZ / LTZ variants.
    Timestamp(chrono::NaiveDateTime),
    /// `TIMESTAMP_NTZ` — explicit no-timezone timestamp.
    TimestampNtz(chrono::NaiveDateTime),
    /// `TIMESTAMP_LTZ` — timezone-aware; always UTC on the wire.
    TimestampLtz(chrono::DateTime<chrono::Utc>),
    Interval(String),
}

/// Wire-format requested for the result.
///
/// v0 supports `Arrow` only. The enum is `#[non_exhaustive]` so v1+ can add
/// columnar Thrift, JSON rows, etc. without breaking wrappers. Passing a
/// non-Arrow variant at execute time returns `ErrorCode::InvalidArgument`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ResultFormat {
    #[default]
    Arrow,
}

/// Plain-data description of a statement to execute.
///
/// All fields are public; callers may construct directly or use the fluent
/// `&mut self` setters. Cheap to `Clone` for caller-side caching.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct StatementSpec {
    /// SQL text. `None` until the caller sets it; `execute()` / `submit()`
    /// return `ErrorCode::InvalidArgument` if still `None` at execute time.
    pub sql: Option<String>,

    /// Positional parameters, 1-based on the wire. Index `i` in this Vec
    /// corresponds to ordinal `i + 1`.
    pub positional_params: Vec<TypedValue>,

    /// Named parameters. Mutually compatible with `positional_params`; the
    /// kernel forwards both to the SEA `parameters` field.
    pub named_params: HashMap<String, TypedValue>,

    /// Per-statement conf overlay. Maps to SEA `parameters` /
    /// Thrift `confOverlay`. Merged on top of the session-level
    /// `session_conf` at execute time; this map wins on key collisions.
    pub statement_conf: HashMap<String, String>,

    /// Result wire format.
    pub result_format: ResultFormat,
}

impl StatementSpec {
    /// New empty spec. SQL must be set before execution.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the SQL text. Replaces any previously set SQL.
    pub fn sql(&mut self, sql: impl Into<String>) -> &mut Self {
        self.sql = Some(sql.into());
        self
    }

    /// Bind a positional parameter (1-based ordinal). Returns
    /// `Err(InvalidArgument)` for `ordinal == 0` — the SEA wire format
    /// is 1-based and ordinal 0 is never valid.
    ///
    /// Grows `positional_params` with `TypedValue::Null` placeholders if
    /// `ordinal` skips ahead of the current length.
    pub fn param(&mut self, ordinal: usize, value: TypedValue) -> Result<&mut Self> {
        if ordinal == 0 {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "positional parameter ordinal must be >= 1 (1-based)",
            ));
        }
        let idx = ordinal - 1;
        if idx >= self.positional_params.len() {
            self.positional_params.resize(idx + 1, TypedValue::Null);
        }
        self.positional_params[idx] = value;
        Ok(self)
    }

    /// Bind a named parameter. Returns `Err(InvalidArgument)` if
    /// `name` is empty — the server rejects empty parameter names,
    /// and we'd rather surface that as a typed error than as a
    /// wire-level rejection.
    ///
    /// PR 5 will tighten validation to also reject:
    /// - Embedded NUL bytes (`\0`)
    /// - Names exceeding the SEA wire limit (currently 255 bytes)
    /// - Duplicate names with different casing — SEA is case-
    ///   insensitive on parameter names; passing both `:foo` and
    ///   `:FOO` is silently the same binding and worth surfacing as
    ///   a typed error rather than a last-write-wins surprise.
    pub fn param_named(&mut self, name: impl Into<String>, value: TypedValue) -> Result<&mut Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "named parameter name must not be empty",
            ));
        }
        self.named_params.insert(name, value);
        Ok(self)
    }

    /// Set the per-statement conf overlay. Replaces any previously-set map.
    pub fn statement_conf(&mut self, conf: HashMap<String, String>) -> &mut Self {
        self.statement_conf = conf;
        self
    }

    /// Set the result wire format.
    pub fn result_format(&mut self, fmt: ResultFormat) -> &mut Self {
        self.result_format = fmt;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_spec_is_empty() {
        let s = StatementSpec::new();
        assert!(s.sql.is_none());
        assert!(s.positional_params.is_empty());
        assert!(s.named_params.is_empty());
        assert!(s.statement_conf.is_empty());
        assert_eq!(s.result_format, ResultFormat::Arrow);
    }

    #[test]
    fn fluent_setters_compose() -> Result<()> {
        let mut s = StatementSpec::new();
        s.sql("SELECT ?")
            .param(1, TypedValue::Null)?
            .param_named("k", TypedValue::Null)?
            .result_format(ResultFormat::Arrow);
        assert_eq!(s.sql.as_deref(), Some("SELECT ?"));
        assert_eq!(s.positional_params.len(), 1);
        assert_eq!(s.named_params.len(), 1);
        Ok(())
    }

    #[test]
    fn skipped_positional_ordinals_pad_with_null() -> Result<()> {
        let mut s = StatementSpec::new();
        s.param(3, TypedValue::Null)?;
        assert_eq!(s.positional_params.len(), 3);
        Ok(())
    }

    #[test]
    fn clone_is_independent() {
        let mut a = StatementSpec::new();
        a.sql("SELECT 1");
        let b = a.clone();
        a.sql("SELECT 2");
        assert_eq!(a.sql.as_deref(), Some("SELECT 2"));
        assert_eq!(b.sql.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn param_rejects_zero_ordinal() {
        let mut s = StatementSpec::new();
        let err = s.param(0, TypedValue::Null).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(s.positional_params.is_empty());
    }

    #[test]
    fn param_named_rejects_empty_name() {
        let mut s = StatementSpec::new();
        let err = s.param_named("", TypedValue::Null).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(s.named_params.is_empty());
    }
}
