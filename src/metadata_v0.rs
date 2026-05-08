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

//! `Metadata` — flat per-method catalog metadata API.
//!
//! Per `docs/spec-v0.md` "Metadata", the kernel exposes one method per
//! ODBC metadata call. Each returns a `ResultStream` whose schema
//! matches the corresponding ODBC SQLProcedures / SQLTables / SQLColumns
//! / etc. contract. Drivers stop reimplementing the SHOW-CATALOGS /
//! SHOW-TABLES plumbing — the kernel owns it.
//!
//! ## Identifier vs LIKE pattern
//!
//! ODBC metadata calls distinguish *identifiers* (exact catalog / schema
//! / table names) from *LIKE patterns* (with `%` and `_` wildcards).
//! The same `&str` shape conveys neither at the type level, leading to
//! injection bugs and ambiguous escaping. The kernel surfaces the
//! distinction with two newtypes:
//!
//! - [`Identifier`]: an exact name. Backticks-escaped on the wire.
//! - [`LikePattern`]: an ODBC-style pattern. `%` / `_` are preserved as
//!   wildcards; literal `%` / `_` in the source must be backslash-escaped
//!   by the caller per ODBC convention.
//!
//! Each `Metadata` method below is annotated with which parameters take
//! identifiers and which take patterns; the type system enforces it.
//!
//! `Metadata` is `'static` for the same reason as every other v0
//! handle — see `docs/spec-v0.md` addendum A1, "Why no Rust lifetimes
//! on the handles". `Identifier<'a>` and `LikePattern<'a>` *do* carry
//! a lifetime, but only across a single call (the wrapper's host
//! string converted to a Rust `String`, borrowed for the duration of
//! the `await`); the lifetime never leaks into a stored type. See
//! addendum A2 for the rationale.

use crate::kernel_error::{Error, Result};
use crate::metadata::shape;
use crate::metadata::static_data;
use crate::reader::ReshapingReader;
use crate::result_stream::ResultStream;
use crate::session::SessionInner;
use std::sync::Arc;

/// Databricks identifier-length cap. Matches the server-side limit;
/// reject longer strings client-side so we fail fast with a clear
/// error rather than as a cryptic SEA error.
const MAX_IDENTIFIER_BYTES: usize = 255;
const MAX_LIKE_PATTERN_BYTES: usize = 255;

// ─── Identifier vs LikePattern newtypes ─────────────────────────────────────

/// An exact SQL identifier (catalog / schema / table / column /
/// function / procedure name). Backticks-escaped at the SQL boundary.
///
/// `Clone` but not `Copy`: `new()` is fallible (forces wrappers to
/// handle the `Result`), and `Copy` semantics would suggest the type
/// is value-like and trivially aliasable. Explicit `.clone()` keeps
/// the borrow-vs-alias intent visible.
#[derive(Debug, Clone)]
pub struct Identifier<'a>(&'a str);

impl<'a> Identifier<'a> {
    /// Construct from a borrowed string.
    ///
    /// Rejects:
    /// - Empty string — meaningless as an identifier; almost always a
    ///   bug at the wrapper layer.
    /// - Embedded NUL bytes (`\0`) — would terminate the string at the
    ///   first FFI / C ABI boundary and silently truncate the
    ///   identifier in transit.
    /// - Backtick characters (`` ` ``) — the kernel backtick-escapes
    ///   identifiers at the SQL boundary, so a literal backtick would
    ///   break out of the escaping. Databricks itself rejects
    ///   identifiers containing backticks.
    /// - Strings exceeding 255 bytes (Databricks identifier-length
    ///   cap) — fail fast client-side rather than surfacing a
    ///   server-side error.
    pub fn new(s: &'a str) -> Result<Self> {
        if s.is_empty() {
            return Err(Error::invalid_argument().message("identifier must not be empty"));
        }
        if s.len() > MAX_IDENTIFIER_BYTES {
            return Err(Error::invalid_argument().message(format!(
                "identifier exceeds {MAX_IDENTIFIER_BYTES}-byte limit ({} bytes)",
                s.len()
            )));
        }
        if s.contains('\0') {
            return Err(Error::invalid_argument().message("identifier must not contain a NUL byte"));
        }
        if s.contains('`') {
            return Err(Error::invalid_argument()
                .message("identifier must not contain a backtick character"));
        }
        Ok(Self(s))
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

/// A SQL LIKE pattern. `%` matches any sequence; `_` matches one
/// character. Both can be escaped with `\`.
///
/// Distinct from [`Identifier`] at the type level so callers cannot
/// accidentally pass an exact name where a pattern is expected (or
/// vice versa) — eliminates a class of metadata-injection bugs.
///
/// `Clone` but not `Copy` — same rationale as [`Identifier`].
///
/// **No `LikePattern::all()` constructor in v0.** ODBC-style metadata
/// calls treat `None` as "match all"; the kernel's `Metadata` methods
/// take `Option<LikePattern<'_>>` and use `None` for the same intent.
/// Adding a `LikePattern::all()` returning `LikePattern("%")` would
/// give callers two paths for "match all" with subtly different
/// semantics (some ODBC providers treat `%` and the absent parameter
/// differently). Pass `None` instead.
#[derive(Debug, Clone)]
pub struct LikePattern<'a>(&'a str);

impl<'a> LikePattern<'a> {
    /// Construct from a borrowed pattern.
    ///
    /// Rejects:
    /// - Empty string — callers should pass `None` for "match all".
    /// - Embedded NUL bytes — same FFI-truncation hazard as
    ///   [`Identifier::new`].
    /// - Strings exceeding 255 bytes.
    ///
    /// Backslash-escaping of literal `%` / `_` is the **caller's**
    /// responsibility per ODBC convention; the kernel does not
    /// interpret the pattern beyond passing it to the SQL builder.
    pub fn new(s: &'a str) -> Result<Self> {
        if s.is_empty() {
            return Err(Error::invalid_argument()
                .message("LIKE pattern must not be empty (pass None for \"match all\")"));
        }
        if s.len() > MAX_LIKE_PATTERN_BYTES {
            return Err(Error::invalid_argument().message(format!(
                "LIKE pattern exceeds {MAX_LIKE_PATTERN_BYTES}-byte limit ({} bytes)",
                s.len()
            )));
        }
        if s.contains('\0') {
            return Err(
                Error::invalid_argument().message("LIKE pattern must not contain a NUL byte")
            );
        }
        Ok(Self(s))
    }

    /// Borrow the underlying pattern.
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

// ─── Metadata API ───────────────────────────────────────────────────────────

/// Flat catalog-metadata API bound to a session.
///
/// Construct via `Session::metadata()`.
pub struct Metadata {
    session: Arc<SessionInner>,
}

impl Metadata {
    pub(crate) fn new(session: Arc<SessionInner>) -> Self {
        Self { session }
    }

    /// All catalogs visible to the session.
    pub async fn list_catalogs(&self) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let raw = self
            .session
            .transport()
            .list_catalogs(self.session.session_id())
            .await?;
        let reader = ReshapingReader::new(raw.reader, shape::reshape_catalogs)?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// Schemas filtered by catalog (exact) and schema name pattern.
    pub async fn list_schemas(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let catalog_str = catalog.as_ref().map(|i| i.as_str());
        let raw = self
            .session
            .transport()
            .list_schemas(
                self.session.session_id(),
                catalog_str,
                schema_pattern.as_ref().map(|p| p.as_str()),
            )
            .await?;
        // SHOW SCHEMAS IN `catalog` returns rows without the catalog
        // column; supply the queried catalog as the fallback so the
        // JDBC TABLE_CATALOG column is populated correctly.
        let fallback = catalog_str.map(|s| s.to_string());
        let reader = ReshapingReader::new(raw.reader, move |raw| {
            shape::reshape_schemas(raw, fallback.as_deref())
        })?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// Tables filtered by catalog (exact), schema (pattern), table
    /// (pattern), and an optional list of table types (e.g.
    /// `["TABLE", "VIEW"]`).
    pub async fn list_tables(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        table_types: Option<&[&str]>,
    ) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let raw = self
            .session
            .transport()
            .list_tables(
                self.session.session_id(),
                catalog.as_ref().map(|i| i.as_str()),
                schema_pattern.as_ref().map(|p| p.as_str()),
                table_pattern.as_ref().map(|p| p.as_str()),
                table_types,
            )
            .await?;
        // Capture an owned filter for the post-fetch table_types narrowing.
        let filter: Option<Vec<String>> =
            table_types.map(|t| t.iter().map(|s| s.to_string()).collect());
        let reader = ReshapingReader::new(raw.reader, move |raw| {
            shape::reshape_tables(raw, filter.as_deref())
        })?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// Columns of tables matching the filter. Catalog is exact;
    /// schema / table / column are patterns.
    ///
    /// Catalog is **required**: Databricks `SHOW COLUMNS` does not
    /// support an "all catalogs" form. `None` returns
    /// `InvalidArgument`.
    pub async fn list_columns(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        column_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let catalog = catalog.ok_or_else(|| {
            Error::invalid_argument().message(
                "list_columns requires a catalog (SHOW COLUMNS IN ALL CATALOGS is unsupported)",
            )
        })?;
        let raw = self
            .session
            .transport()
            .list_columns(
                self.session.session_id(),
                catalog.as_str(),
                schema_pattern.as_ref().map(|p| p.as_str()),
                table_pattern.as_ref().map(|p| p.as_str()),
                column_pattern.as_ref().map(|p| p.as_str()),
            )
            .await?;
        let reader = ReshapingReader::new(raw.reader, shape::reshape_columns)?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// Primary keys for the given table. All three identifiers are
    /// exact names — ODBC SQLPrimaryKeys does not support patterns.
    ///
    /// Catalog and schema are required at the kernel layer: Databricks
    /// `SHOW KEYS` needs `IN CATALOG / IN SCHEMA / IN TABLE` qualified
    /// fully. Wrappers that want to fall back to a session default
    /// should resolve it before calling.
    pub async fn primary_keys(
        &self,
        catalog: Option<Identifier<'_>>,
        schema: Option<Identifier<'_>>,
        table: Identifier<'_>,
    ) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let catalog = catalog.ok_or_else(|| {
            Error::invalid_argument().message("primary_keys requires a catalog identifier")
        })?;
        let schema = schema.ok_or_else(|| {
            Error::invalid_argument().message("primary_keys requires a schema identifier")
        })?;
        let raw = self
            .session
            .transport()
            .list_primary_keys(
                self.session.session_id(),
                catalog.as_str(),
                schema.as_str(),
                table.as_str(),
            )
            .await?;
        let reader = ReshapingReader::new(raw.reader, shape::reshape_primary_keys)?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// Foreign-key relationships. Both parent and foreign sides are
    /// exact identifiers — ODBC SQLForeignKeys does not support
    /// patterns. The **foreign side** must be fully specified
    /// (catalog + schema + table); the parent side is optional and
    /// matches "any parent" when omitted, matching the underlying
    /// `SHOW FOREIGN KEYS IN TABLE` semantics. Wrappers that want to
    /// query by parent only should resolve to a foreign-side filter
    /// before calling.
    pub async fn foreign_keys(
        &self,
        parent_catalog: Option<Identifier<'_>>,
        parent_schema: Option<Identifier<'_>>,
        parent_table: Option<Identifier<'_>>,
        foreign_catalog: Option<Identifier<'_>>,
        foreign_schema: Option<Identifier<'_>>,
        foreign_table: Option<Identifier<'_>>,
    ) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let foreign_catalog = foreign_catalog.ok_or_else(|| {
            Error::invalid_argument().message("foreign_keys requires foreign_catalog")
        })?;
        let foreign_schema = foreign_schema.ok_or_else(|| {
            Error::invalid_argument().message("foreign_keys requires foreign_schema")
        })?;
        let foreign_table = foreign_table.ok_or_else(|| {
            Error::invalid_argument().message("foreign_keys requires foreign_table")
        })?;
        let raw = self
            .session
            .transport()
            .list_foreign_keys(
                self.session.session_id(),
                parent_catalog.as_ref().map(|i| i.as_str()),
                parent_schema.as_ref().map(|i| i.as_str()),
                parent_table.as_ref().map(|i| i.as_str()),
                foreign_catalog.as_str(),
                foreign_schema.as_str(),
                foreign_table.as_str(),
            )
            .await?;
        let reader = ReshapingReader::new(raw.reader, shape::reshape_foreign_keys)?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// Functions visible to the session. Catalog is exact; schema /
    /// name are patterns.
    pub async fn functions(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        name_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let raw = self
            .session
            .transport()
            .list_functions(
                self.session.session_id(),
                catalog.as_ref().map(|i| i.as_str()),
                schema_pattern.as_ref().map(|p| p.as_str()),
                name_pattern.as_ref().map(|p| p.as_str()),
            )
            .await?;
        let reader = ReshapingReader::new(raw.reader, shape::reshape_functions)?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// Procedures visible to the session. Catalog is exact; schema /
    /// name are patterns.
    pub async fn procedures(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        name_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let raw = self
            .session
            .transport()
            .list_procedures(
                self.session.session_id(),
                catalog.as_ref().map(|i| i.as_str()),
                schema_pattern.as_ref().map(|p| p.as_str()),
                name_pattern.as_ref().map(|p| p.as_str()),
            )
            .await?;
        let reader = ReshapingReader::new(raw.reader, shape::reshape_procedures)?;
        ResultStream::from_reader(Box::new(reader))
    }

    /// SQL data types supported by the workspace. No filters. No wire
    /// call — returns a static in-memory result.
    pub async fn type_info(&self) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let reader = static_data::type_info_reader();
        ResultStream::from_reader(reader)
    }

    /// All table types (`TABLE`, `VIEW`, `SYSTEM TABLE`, …). No
    /// filters. No wire call — returns a static in-memory result
    /// driven by the session's transport list (see
    /// [`Transport::list_table_types`](crate::client::Transport::list_table_types)).
    pub async fn table_types(&self) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        let types = self.session.transport().list_table_types();
        let reader = static_data::table_types_reader(&types);
        ResultStream::from_reader(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel_error::ErrorCode;

    #[test]
    fn identifier_accepts_normal_name() {
        let id = Identifier::new("main").expect("normal name accepted");
        assert_eq!(id.as_str(), "main");
    }

    #[test]
    fn identifier_rejects_empty() {
        let err = Identifier::new("").expect_err("empty identifier rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn identifier_rejects_nul_byte() {
        let err = Identifier::new("foo\0bar").expect_err("NUL byte rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("NUL"));
    }

    #[test]
    fn identifier_rejects_backtick() {
        let err = Identifier::new("foo`bar").expect_err("backtick rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("backtick"));
    }

    #[test]
    fn identifier_rejects_too_long() {
        let s = "a".repeat(MAX_IDENTIFIER_BYTES + 1);
        let err = Identifier::new(&s).expect_err("oversize identifier rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("255"));
    }

    #[test]
    fn identifier_accepts_max_length() {
        let s = "a".repeat(MAX_IDENTIFIER_BYTES);
        Identifier::new(&s).expect("exactly 255 bytes is accepted");
    }

    #[test]
    fn like_pattern_accepts_wildcard_pattern() {
        let pat = LikePattern::new("info%").expect("normal pattern accepted");
        assert_eq!(pat.as_str(), "info%");
    }

    #[test]
    fn like_pattern_rejects_empty() {
        let err = LikePattern::new("").expect_err("empty pattern rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("None"), "{}", err.message);
    }

    #[test]
    fn like_pattern_rejects_nul_byte() {
        let err = LikePattern::new("foo\0bar").expect_err("NUL byte rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn like_pattern_rejects_too_long() {
        let s = "a".repeat(MAX_LIKE_PATTERN_BYTES + 1);
        let err = LikePattern::new(&s).expect_err("oversize pattern rejected");
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }
}
