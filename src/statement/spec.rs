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
///
/// `Variant` carries Databricks `VARIANT` (schema-on-read JSON-like)
/// values as opaque JSON text. The kernel does not parse the JSON —
/// downstream consumers (Python's `json.loads`, Go's
/// `json.Unmarshal`, Node's `JSON.parse`, etc.) decode per their
/// language idioms. On the wire the SEA `param_type` is `"VARIANT"`
/// and the value is the JSON string the caller provided.
///
/// `Geography` and `Geometry` carry Databricks' geospatial types.
/// Two representations are tracked in the same variant for
/// forward-compatibility with SEA's upcoming binary geo path:
///
/// - `wkt`: well-known-text body without an `SRID=N;` prefix
///   (e.g. `"POINT(1 2)"`). Always populated.
/// - `srid`: spatial reference system identifier (e.g. `4326` for
///   WGS 84). `None` when the binding / column has no SRID.
/// - `wkb`: well-known-binary representation. `None` on the EWKT
///   path (today's SEA default for non-JDBC clients). Will be
///   `Some(bytes)` once SEA's per-client-agent binary geo path
///   ships and the kernel opts in — see the SEA Geospatial Arrow
///   design doc for the gating model.
///
/// The kernel does not currently parse WKT into geometric primitives;
/// downstream consumers that need typed primitives (e.g. shapely on
/// Python, orb on Go) decode `wkt` or `wkb` themselves.
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
    /// `VARIANT` — opaque JSON text. See the enum-level docs.
    Variant(String),
    /// `GEOGRAPHY` — see the enum-level docs for the EWKT vs WKB
    /// representation contract.
    Geography {
        wkt: String,
        srid: Option<i32>,
        wkb: Option<Vec<u8>>,
    },
    /// `GEOMETRY` — see the enum-level docs for the EWKT vs WKB
    /// representation contract.
    Geometry {
        wkt: String,
        srid: Option<i32>,
        wkb: Option<Vec<u8>>,
    },
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

/// Wire-format constants for [`TypedValue::Geography`] /
/// [`TypedValue::Geometry`].
///
/// These literals are the contract with the SEA server: the
/// `param_type` family names and the EWKT `SRID=N;` prefix syntax.
/// Centralised here so the bind-side dispatch (pyo3 parser), the
/// bind-side payload mapper (`typed_value_to_sea_payload`), and
/// any future result-side reader all reference one source of truth.
/// Match callers use `.eq_ignore_ascii_case(...)` against the
/// uppercased input; emitting code writes these strings verbatim
/// to the wire.
pub mod geo_wire {
    /// SEA `param_type` family name for [`super::TypedValue::Geography`].
    pub const GEOGRAPHY: &str = "GEOGRAPHY";
    /// SEA `param_type` family name for [`super::TypedValue::Geometry`].
    pub const GEOMETRY: &str = "GEOMETRY";
    /// EWKT prefix marker — appears as `"SRID=N;"` at the head of
    /// the value field when an SRID is bound or emitted.
    pub const EWKT_SRID_PREFIX: &str = "SRID=";
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
    /// corresponds to ordinal `i + 1`. Mutually exclusive with
    /// `named_params` (see below).
    pub positional_params: Vec<TypedValue>,

    /// Named parameters. Mutually exclusive with `positional_params`:
    /// `execute()` / `submit()` return `ErrorCode::InvalidArgument` if
    /// both are non-empty. The SEA server rejects a mixed `parameters`
    /// array with `INVALID_PARAMETER_MARKER_VALUE.MISSING_NAME`, so the
    /// kernel rejects at bind time before the wire.
    pub named_params: HashMap<String, TypedValue>,

    /// Per-statement conf overlay. Maps to SEA `parameters` /
    /// Thrift `confOverlay`. Merged on top of the session-level
    /// `session_conf` at execute time; this map wins on key collisions.
    pub statement_conf: HashMap<String, String>,

    /// Server-side cap on the number of rows returned by this statement,
    /// independent of SQL `LIMIT`. Maps directly to the SEA
    /// `ExecuteStatement.row_limit` wire field (Python `use_sea`'s
    /// `row_limit=` kwarg is the canonical cross-driver surface). `None`
    /// omits the field, so the server applies no driver-imposed cap.
    pub row_limit: Option<i64>,

    /// Server-side statement wait timeout, in whole seconds. Bounds how
    /// long the server waits for the statement to reach a terminal state
    /// before the wait elapses; on elapse the statement is cancelled
    /// (`on_wait_timeout = CANCEL`), surfacing as a timeout to the caller.
    /// This is the per-statement server timeout (JDBC `setQueryTimeout` /
    /// Python `use_sea`'s `wait_timeout=`), distinct from the transport
    /// `overall_timeout`. `None` lets the kernel pick its sync/async
    /// default (see `Statement::execute` / `submit`). The SEA wire caps
    /// the value at 50s; the kernel forwards the requested value verbatim
    /// and lets the server clamp.
    pub query_timeout_secs: Option<u32>,

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

    /// Merge a `query_tags` map into the per-statement conf overlay.
    ///
    /// Serialises `tags` to the comma-separated `key:value` wire shape
    /// the SEA server expects under the `query_tags` conf key (the same
    /// shape NodeJS Thrift's `serializeQueryTags` produces: backslashes
    /// in keys are doubled; backslash / colon / comma in values are
    /// backslash-escaped) and inserts it into `statement_conf` under
    /// `"query_tags"`.
    ///
    /// - An **empty** `tags` map is a no-op (returns `Ok(self)`),
    ///   matching the "empty map means not-supplied" convention.
    /// - If `statement_conf` already carries an explicit `"query_tags"`
    ///   key, returns `Err(InvalidArgument)` — supplying both a
    ///   `queryTags` map and `statementConf["query_tags"]` is an
    ///   ambiguous intent the kernel refuses to silently resolve. Set
    ///   the explicit `statement_conf` map (via [`Self::statement_conf`])
    ///   before calling this so the conflict is detected.
    pub fn query_tags(&mut self, tags: HashMap<String, String>) -> Result<&mut Self> {
        if tags.is_empty() {
            return Ok(self);
        }
        if self.statement_conf.contains_key("query_tags") {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "cannot supply both `queryTags` and `statementConf[\"query_tags\"]` — pick one",
            ));
        }
        self.statement_conf
            .insert("query_tags".to_string(), serialize_query_tags(&tags));
        Ok(self)
    }

    /// Set the server-side row cap (SEA `row_limit`). `None` clears it.
    pub fn row_limit(&mut self, row_limit: Option<i64>) -> &mut Self {
        self.row_limit = row_limit;
        self
    }

    /// Set the per-statement server wait timeout in whole seconds. `None`
    /// clears it (kernel default applies). See the field docs for the
    /// `CANCEL`-on-elapse semantics and the 50s server cap.
    pub fn query_timeout_secs(&mut self, secs: Option<u32>) -> &mut Self {
        self.query_timeout_secs = secs;
        self
    }

    /// Set the result wire format.
    pub fn result_format(&mut self, fmt: ResultFormat) -> &mut Self {
        self.result_format = fmt;
        self
    }
}

/// Serialise a tag map to the comma-separated `key:value` wire shape the
/// SEA server expects under the `query_tags` conf key — identical to the
/// shape NodeJS Thrift's `serializeQueryTags` (`lib/utils/queryTags.ts`)
/// produces. Backslashes in keys are doubled; backslash, colon, and comma
/// in values are each backslash-escaped. The SEA server merges
/// `confOverlay.query_tags` the same way regardless of which backend
/// produced it, so callers porting from Thrift see identical behaviour.
///
/// Tag ordering follows `HashMap` iteration order, which is
/// non-deterministic; the SEA server treats conf values as
/// order-insensitive, so the result is functionally equivalent to the
/// Thrift output regardless of key order.
fn serialize_query_tags(tags: &HashMap<String, String>) -> String {
    tags.iter()
        .map(|(k, v)| {
            let esc_key = k.replace('\\', "\\\\");
            let esc_val = v
                .chars()
                .flat_map(|c| match c {
                    '\\' | ':' | ',' => vec!['\\', c],
                    other => vec![other],
                })
                .collect::<String>();
            format!("{esc_key}:{esc_val}")
        })
        .collect::<Vec<_>>()
        .join(",")
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
        // row_limit / query_timeout default to "omit the field" so the
        // server applies no driver-imposed cap and its own wait default.
        assert!(s.row_limit.is_none());
        assert!(s.query_timeout_secs.is_none());
        assert_eq!(s.result_format, ResultFormat::Arrow);
    }

    #[test]
    fn row_limit_and_query_timeout_setters() {
        let mut s = StatementSpec::new();
        s.row_limit(Some(100)).query_timeout_secs(Some(30));
        assert_eq!(s.row_limit, Some(100));
        assert_eq!(s.query_timeout_secs, Some(30));
        // `None` clears back to the omit-the-field default.
        s.row_limit(None).query_timeout_secs(None);
        assert!(s.row_limit.is_none());
        assert!(s.query_timeout_secs.is_none());
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

    // ─── query_tags merge behaviour ─────────────────────────────────────

    #[test]
    fn query_tags_empty_map_is_noop() {
        let mut s = StatementSpec::new();
        s.query_tags(HashMap::new()).unwrap();
        assert!(s.statement_conf.is_empty());
    }

    #[test]
    fn query_tags_serialised_into_query_tags_key() {
        let mut s = StatementSpec::new();
        let mut tags = HashMap::new();
        tags.insert("team".to_string(), "platform".to_string());
        s.query_tags(tags).unwrap();
        assert_eq!(
            s.statement_conf.get("query_tags").map(String::as_str),
            Some("team:platform"),
        );
    }

    #[test]
    fn query_tags_rejects_dual_definition() {
        let mut s = StatementSpec::new();
        let mut conf = HashMap::new();
        conf.insert("query_tags".to_string(), "explicit:value".to_string());
        s.statement_conf(conf);
        let mut tags = HashMap::new();
        tags.insert("team".to_string(), "platform".to_string());
        let err = s.query_tags(tags).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(
            err.message.contains("cannot supply both"),
            "error must explain the conflict: {}",
            err.message,
        );
        // The explicit key is left untouched.
        assert_eq!(
            s.statement_conf.get("query_tags").map(String::as_str),
            Some("explicit:value"),
        );
    }

    #[test]
    fn query_tags_empty_map_does_not_collide_with_existing_key() {
        // An empty `queryTags: {}` map is ignored, so a caller can pass
        // an explicit `statementConf["query_tags"]` alongside an empty
        // tag map without tripping the ambiguity guard.
        let mut s = StatementSpec::new();
        let mut conf = HashMap::new();
        conf.insert("query_tags".to_string(), "explicit:value".to_string());
        s.statement_conf(conf);
        s.query_tags(HashMap::new()).unwrap();
        assert_eq!(
            s.statement_conf.get("query_tags").map(String::as_str),
            Some("explicit:value"),
        );
    }

    // ─── serialize_query_tags byte-shape parity with Thrift ─────────────

    /// `serializeQueryTags` (NodeJS Thrift, `lib/utils/queryTags.ts`)
    /// is the canonical wire shape for `query_tags`. Pin the per-tag
    /// byte shape produced by the kernel-side serializer against the
    /// algorithm Thrift uses, so a regression on either side fails this
    /// test.
    #[test]
    fn serialize_query_tags_matches_thrift_byte_shape_single_tag() {
        let mut tags = HashMap::new();
        tags.insert("team".to_string(), "platform".to_string());
        assert_eq!(serialize_query_tags(&tags), "team:platform");
    }

    #[test]
    fn serialize_query_tags_matches_thrift_byte_shape_escapes() {
        // Per Thrift's `escapedValue.replace(/[\\:,]/g, c => '\\' + c)`:
        // input  `v:1,v\2`
        // output `v\:1\,v\\2`
        let mut tags = HashMap::new();
        tags.insert("k".to_string(), "v:1,v\\2".to_string());
        assert_eq!(serialize_query_tags(&tags), "k:v\\:1\\,v\\\\2");
    }

    #[test]
    fn serialize_query_tags_matches_thrift_byte_shape_key_backslash_only() {
        // Per Thrift's `escapedKey = key.replace(/\\/g, '\\\\')`:
        // input  `k\1`
        // output `k\\1`
        // Colons/commas in keys are NOT escaped on Thrift's path — only
        // in values. Pins this asymmetry so a future "uniformity"
        // refactor that escapes them in keys too gets caught.
        let mut tags = HashMap::new();
        tags.insert("k\\1".to_string(), "v".to_string());
        assert_eq!(serialize_query_tags(&tags), "k\\\\1:v");
    }

    #[test]
    fn serialize_query_tags_empty_map_yields_empty_string() {
        assert_eq!(serialize_query_tags(&HashMap::new()), "");
    }

    #[test]
    fn serialize_query_tags_multi_tag_contains_each_pair_regardless_of_order() {
        // Multi-tag case: HashMap iteration is non-deterministic, so we
        // can't assert a single byte string. Assert each pair is present
        // and the join uses `,`. SEA treats conf values as
        // order-insensitive, so this is functionally equivalent to the
        // Thrift output regardless of order.
        let mut tags = HashMap::new();
        tags.insert("a".to_string(), "1".to_string());
        tags.insert("b".to_string(), "2".to_string());
        let s = serialize_query_tags(&tags);
        let pairs: std::collections::HashSet<&str> = s.split(',').collect();
        assert!(
            pairs == ["a:1", "b:2"].iter().copied().collect(),
            "got: {s}",
        );
    }
}
