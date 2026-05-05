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

#![allow(dead_code, unused_variables)]

use crate::kernel_error::Result;
use crate::result_stream::ResultStream;
use crate::session::SessionInner;
use std::sync::Arc;

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
    /// Construct from a borrowed string. Validation lands in PR 6.
    ///
    /// PR 6 will reject:
    /// - Empty string
    /// - Embedded NUL bytes (`\0`) — would terminate the SQL string
    ///   early once cast to a C string in some FFI paths
    /// - Backtick characters (`` ` ``) — would break the kernel's
    ///   backtick-escaping at the SQL boundary
    /// - Strings exceeding 255 bytes (Databricks identifier-length cap)
    pub fn new(s: &'a str) -> Result<Self> {
        // PR 6: implement the validation above.
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
    /// Construct from a borrowed pattern. Validation lands in PR 6.
    ///
    /// PR 6 will reject:
    /// - Empty string
    /// - Embedded NUL bytes
    /// - Strings exceeding 255 bytes
    pub fn new(s: &'a str) -> Result<Self> {
        // PR 6: implement the validation above.
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
        unimplemented!("PR 6: implement list_catalogs()")
    }

    /// Schemas filtered by catalog (exact) and schema name pattern.
    pub async fn list_schemas(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        unimplemented!("PR 6: implement list_schemas()")
    }

    /// Tables filtered by catalog (exact), schema (pattern), table
    /// (pattern), and a comma-of `&str` table types (e.g. `["TABLE",
    /// "VIEW"]`).
    pub async fn list_tables(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        table_types: Option<&[&str]>,
    ) -> Result<ResultStream> {
        unimplemented!("PR 6: implement list_tables()")
    }

    /// Columns of tables matching the filter. Catalog is exact;
    /// schema / table / column are patterns.
    pub async fn list_columns(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        column_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        unimplemented!("PR 6: implement list_columns()")
    }

    /// Primary keys for the given table. All three identifiers are
    /// exact names — ODBC SQLPrimaryKeys does not support patterns.
    pub async fn primary_keys(
        &self,
        catalog: Option<Identifier<'_>>,
        schema: Option<Identifier<'_>>,
        table: Identifier<'_>,
    ) -> Result<ResultStream> {
        unimplemented!("PR 6: implement primary_keys()")
    }

    /// Foreign-key relationships. Both parent and foreign sides are
    /// exact identifiers — ODBC SQLForeignKeys does not support
    /// patterns. Either side may be omitted for "any".
    pub async fn foreign_keys(
        &self,
        parent_catalog: Option<Identifier<'_>>,
        parent_schema: Option<Identifier<'_>>,
        parent_table: Option<Identifier<'_>>,
        foreign_catalog: Option<Identifier<'_>>,
        foreign_schema: Option<Identifier<'_>>,
        foreign_table: Option<Identifier<'_>>,
    ) -> Result<ResultStream> {
        unimplemented!("PR 6: implement foreign_keys()")
    }

    /// Functions visible to the session. Catalog is exact; schema /
    /// name are patterns.
    pub async fn functions(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        name_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        unimplemented!("PR 6: implement functions()")
    }

    /// Procedures visible to the session. Catalog is exact; schema /
    /// name are patterns.
    pub async fn procedures(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        name_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        unimplemented!("PR 6: implement procedures()")
    }

    /// SQL data types supported by the workspace. No filters.
    pub async fn type_info(&self) -> Result<ResultStream> {
        unimplemented!("PR 6: implement type_info()")
    }

    /// All table types (`TABLE`, `VIEW`, `SYSTEM TABLE`, …). No filters.
    pub async fn table_types(&self) -> Result<ResultStream> {
        unimplemented!("PR 6: implement table_types()")
    }
}
