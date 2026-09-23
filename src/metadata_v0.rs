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
//! ODBC metadata call. The kernel returns **JDBC-shape**
//! [`java.sql.DatabaseMetaData`](https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html)
//! result schemas for the four core methods (`list_catalogs`,
//! `list_schemas`, `list_tables`, `list_columns`): the kernel
//! issues the appropriate `SHOW` / `information_schema` query,
//! reshapes the server response (rename columns, synthesize
//! JDBC-only columns as nulls, derive `DATA_TYPE` integer codes),
//! and exposes the JDBC-shape Arrow batch through a `ResultStream`.
//! See [`crate::metadata::thrift_shape`] for the schemas and reshape
//! logic.
//!
//! Rationale: JDBC, ODBC, and the existing PySQL / Node drivers all
//! surface the JDBC `DatabaseMetaData` column-name contract to their
//! users. Pushing the reshape into the kernel keeps every binding
//! free of duplicated rename + synthesis code. ADBC `GetObjects` is
//! the one outlier (deeply nested Arrow struct); the ADBC binding
//! restructures from the JDBC-shape rows.
//!
//! JDBC-shape contracts for the four core methods:
//!
//! - `list_catalogs` → `TABLE_CAT: Utf8`
//! - `list_schemas` → `TABLE_SCHEM, TABLE_CATALOG`
//! - `list_tables` → `TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE,
//!   REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
//!   SELF_REFERENCING_COL_NAME, REF_GENERATION` (last 5 always null
//!   per JDBC spec). `table_types`, when provided, filters rows on
//!   `TABLE_TYPE` kernel-side (Databricks `SHOW TABLES` doesn't
//!   honour the filter server-side).
//! - `list_columns` → 23 columns mirroring JDBC `getColumns`.
//!   `DATA_TYPE` is derived from the server's `columnType` string
//!   via the canonical JDBC type-code table; `TYPE_NAME` preserves
//!   the precise server type so wrappers retain access to it.
//!
//! `primary_keys`, `foreign_keys`, `procedures`, `functions` still
//! pass server shape through unchanged — those reshapes are not yet
//! implemented and remain a wrapper responsibility.
//!
//! `type_info` / `table_types` have no SEA response to reshape. The
//! kernel synthesises `type_info` to match the established Thrift
//! `getTypeInfo` columns and rows, and uses the JDBC `getTableTypes`
//! schema for `table_types` (see [`crate::metadata::schemas`]).
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

use crate::client::transport::ExecuteResult;
use crate::kernel_error::{Error, Result};
use crate::metadata::sql::{LikeMatcher, SqlCommandBuilder};
use crate::metadata::static_data;
use crate::metadata::thrift_shape::{
    self, is_object_not_found, ThriftReshapingReader, GET_CATALOGS_SCHEMA, GET_COLUMNS_SCHEMA,
    GET_CROSS_REFERENCE_SCHEMA, GET_FUNCTIONS_SCHEMA, GET_PRIMARY_KEYS_SCHEMA,
    GET_PROCEDURES_SCHEMA, GET_PROCEDURE_COLUMNS_SCHEMA, GET_SCHEMAS_SCHEMA, GET_TABLES_SCHEMA,
};
use crate::reader::{EmptyReader, InMemoryReader};
use crate::result_stream::{ResultBatch, ResultStream};
use crate::session::SessionInner;
use crate::telemetry::{ExecutionResultFormat, OperationType, StatementTelemetry, StatementType};
use crate::types::sea::StatementState;
use arrow_array::Array;
use arrow_schema::SchemaRef;
use std::sync::Arc;
use tracing::{debug, Instrument};

/// The statement id to hand to
/// [`ResultStream::from_reader_with_close`] as the close target.
///
/// When the `ExecuteStatement` response already reported
/// `status.state == Closed`, the server has already cleaned up the
/// statement server-side, so a follow-up `CloseStatement` DELETE is
/// redundant (the server returns 404 `STATEMENT_NOT_FOUND`). In that
/// case return an empty id so `from_reader_with_close` installs no
/// close guard — mirroring the regular-query path in
/// `statement/mutable.rs`. Otherwise return the real id so the
/// server statement is reaped when the stream is dropped.
fn close_id(raw: &ExecuteResult) -> String {
    if raw.status.state == StatementState::Closed {
        String::new()
    } else {
        raw.statement_id.clone()
    }
}

fn is_native_metadata_result(raw: &ExecuteResult) -> bool {
    raw.manifest
        .as_ref()
        .is_some_and(|manifest| manifest.is_native_metadata_result == Some(true))
}

/// Native metadata already has the canonical Thrift column order and values.
/// Reattach the kernel's public schema so empty and non-empty responses expose
/// the same stable Arrow contract, while deliberately skipping the SQL-result
/// reshape.
fn normalize_native_metadata_batch(
    batch: &arrow_array::RecordBatch,
    schema: SchemaRef,
) -> Result<arrow_array::RecordBatch> {
    arrow_array::RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|e| {
        Error::data_loss().message(format!("invalid Thrift-native metadata result: {e}"))
    })
}

async fn native_or_reshaped_stream(
    raw: ExecuteResult,
    session: &Arc<SessionInner>,
    schema: SchemaRef,
    reshape: Box<thrift_shape::ReshapeFn>,
) -> Result<ResultStream> {
    let stmt_id = close_id(&raw);
    let native = is_native_metadata_result(&raw);
    let native_schema = schema.clone();
    let reader = ThriftReshapingReader::new(
        raw.reader,
        schema,
        Box::new(move |batch| {
            if native {
                normalize_native_metadata_batch(batch, native_schema.clone())
            } else {
                reshape(batch)
            }
        }),
    );
    ResultStream::from_reader_with_close(Box::new(reader), Arc::clone(session), stmt_id).await
}

/// JDBC's `DatabaseMetaData` contract: metadata methods return an
/// empty result set for non-existent catalogs / schemas / tables
/// rather than raising. Translate `is_object_not_found` errors into
/// an empty stream typed to the operation's JDBC schema; pass other
/// errors through. Matches the JDBC driver's
/// `isObjectNotFoundException → empty getXResult` pattern.
async fn not_found_or<F>(
    err: Error,
    schema: SchemaRef,
    telemetry: &StatementTelemetry,
    otherwise: F,
) -> Result<ResultStream>
where
    F: FnOnce(Error) -> Result<ResultStream>,
{
    if is_object_not_found(&err) {
        telemetry.on_result_metadata(ExecutionResultFormat::InlineArrow, false);
        ResultStream::from_reader(Box::new(EmptyReader::new(schema))).await
    } else {
        otherwise(err)
    }
}

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
    /// - Strings consisting only of whitespace — would backtick-escape
    ///   to a blank token and produce a server-side parse error.
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
        if s.trim().is_empty() {
            return Err(Error::invalid_argument().message("identifier must not be all whitespace"));
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

    /// Construct from an optional borrowed string.
    ///
    /// Maps `None` to `Ok(None)` ("not supplied" — e.g. ODBC-style
    /// "match all" for an exact catalog/schema), and `Some(s)` through
    /// [`Identifier::new`] (so whitespace-only / empty / NUL / backtick /
    /// oversize inputs reject as `InvalidArgument`). Single home for the
    /// `Option<String>` → `Option<Identifier>` conversion the wrappers
    /// (napi / pyo3) all need at their metadata call sites.
    pub fn from_opt(s: &'a Option<String>) -> Result<Option<Identifier<'a>>> {
        s.as_deref().map(Identifier::new).transpose()
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
    /// Accepts the **empty string**: it compiles to `LIKE ''`, which matches
    /// only the empty name — i.e. "match nothing", mirroring the Thrift/JDBC
    /// convention where an empty `tableNamePattern` (etc.) returns zero rows.
    /// `None` remains the canonical "match all" intent; empty `""` is the
    /// distinct "match nothing", so both are kept.
    ///
    /// Rejects:
    /// - Non-empty **whitespace-only** strings — almost always a caller
    ///   mistake; pass `None` for "match all".
    /// - Embedded NUL bytes — same FFI-truncation hazard as
    ///   [`Identifier::new`].
    /// - Strings exceeding 255 bytes.
    ///
    /// Backslash-escaping of literal `%` / `_` is the **caller's**
    /// responsibility per ODBC convention; the kernel does not
    /// interpret the pattern beyond passing it to the SQL builder.
    pub fn new(s: &'a str) -> Result<Self> {
        // An empty string is a valid LIKE pattern: it compiles to `LIKE ''`,
        // which matches only the empty name — i.e. "match nothing". This
        // mirrors the Thrift/JDBC convention where an empty tableNamePattern
        // (etc.) returns zero rows. `None` remains the canonical "match all"
        // intent; empty `""` is distinct (match-nothing), so both are kept.
        // Non-empty whitespace-only patterns are still rejected as a likely
        // caller mistake (use `None` for match-all).
        if !s.is_empty() && s.trim().is_empty() {
            return Err(Error::invalid_argument()
                .message("LIKE pattern must not be all whitespace (pass None for \"match all\")"));
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

    /// Construct from an optional borrowed pattern.
    ///
    /// Maps `None` to `Ok(None)` (the canonical "match all" intent), and
    /// `Some(s)` through [`LikePattern::new`] (so whitespace-only / empty
    /// / NUL / oversize inputs reject as `InvalidArgument`). Single home
    /// for the `Option<String>` → `Option<LikePattern>` conversion the
    /// wrappers (napi / pyo3) all need at their metadata call sites.
    pub fn from_opt(s: &'a Option<String>) -> Result<Option<LikePattern<'a>>> {
        s.as_deref().map(LikePattern::new).transpose()
    }

    /// Borrow the underlying pattern.
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

/// Convert binding-layer optional strings for an exact catalog plus a
/// pattern filter.
///
/// Non-empty catalogs remain exact [`Identifier`]s. An empty catalog cannot
/// be represented as an `Identifier`, but JDBC metadata semantics require it
/// to match nothing rather than fail validation. Encode that case as an empty
/// [`LikePattern`] on the accompanying pattern dimension; metadata methods
/// short-circuit empty patterns to a typed empty result before any wire call.
pub fn exact_catalog_and_pattern<'a>(
    catalog: &'a Option<String>,
    pattern: &'a Option<String>,
) -> Result<(Option<Identifier<'a>>, Option<LikePattern<'a>>)> {
    if catalog.as_deref() == Some("") {
        // `None` alone means all catalogs. It is safe here only because the
        // paired empty pattern is checked before list_schemas/list_columns
        // reach the transport; keep that short-circuit ahead of any wire call.
        return Ok((None, Some(LikePattern::new("")?)));
    }
    Ok((
        Identifier::from_opt(catalog)?,
        LikePattern::from_opt(pattern)?,
    ))
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

    fn metadata_telemetry(&self, operation_type: OperationType) -> StatementTelemetry {
        self.session
            .statement_telemetry_observer(StatementType::Metadata, operation_type)
    }

    fn observe_raw_result(telemetry: &StatementTelemetry, raw: &ExecuteResult) {
        telemetry.on_statement_submitted(&raw.statement_id);
        telemetry.on_result_metadata(raw.telemetry_result_format, raw.telemetry_is_compressed);
    }

    fn observe_inline_result(telemetry: &StatementTelemetry) {
        telemetry.on_result_metadata(ExecutionResultFormat::InlineArrow, false);
    }

    fn finish_metadata_call(
        telemetry: StatementTelemetry,
        result: Result<ResultStream>,
    ) -> Result<ResultStream> {
        match result {
            Ok(mut stream) => {
                stream.attach_telemetry(telemetry);
                Ok(stream)
            }
            Err(error) => {
                telemetry.finalize(Some(&error));
                Err(error)
            }
        }
    }

    /// All catalogs visible to the session.
    ///
    /// JDBC `getCatalogs` shape: `TABLE_CAT: Utf8`.
    pub async fn list_catalogs(&self) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListCatalogs);
        let telemetry_for_body = telemetry.clone();
        let result = async { self.list_catalogs_uninstrumented(&telemetry_for_body).await }
            .instrument(span)
            .await;
        Self::finish_metadata_call(telemetry, result)
    }

    async fn list_catalogs_uninstrumented(
        &self,
        telemetry: &StatementTelemetry,
    ) -> Result<ResultStream> {
        let raw = self
            .session
            .transport()
            .list_catalogs(self.session.session_id())
            .await?;
        Self::observe_raw_result(telemetry, &raw);
        native_or_reshaped_stream(
            raw,
            &self.session,
            GET_CATALOGS_SCHEMA.clone(),
            Box::new(thrift_shape::reshape_catalogs),
        )
        .await
    }

    /// Schemas filtered by catalog (exact) and schema name pattern.
    /// An empty schema pattern returns an empty result without a wire call.
    ///
    /// JDBC `getSchemas` shape: `TABLE_SCHEM: Utf8 (not null),
    /// TABLE_CATALOG: Utf8`. When the underlying server response omits
    /// the catalog column (single-catalog `SHOW SCHEMAS IN <catalog>`
    /// form), `TABLE_CATALOG` is filled from the `catalog` argument.
    pub async fn list_schemas(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListSchemas);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            if schema_pattern
                .as_ref()
                .is_some_and(|pattern| pattern.as_str().is_empty())
            {
                Self::observe_inline_result(&telemetry_for_body);
                return ResultStream::from_reader(Box::new(EmptyReader::new(
                    GET_SCHEMAS_SCHEMA.clone(),
                )))
                .await;
            }
            let catalog_str = catalog.as_ref().map(|i| i.as_str().to_owned());
            let raw = match self
                .session
                .transport()
                .list_schemas(
                    self.session.session_id(),
                    catalog.as_ref().map(|i| i.as_str()),
                    schema_pattern.as_ref().map(|p| p.as_str()),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return not_found_or(e, GET_SCHEMAS_SCHEMA.clone(), &telemetry_for_body, Err)
                        .await
                }
            };
            Self::observe_raw_result(&telemetry_for_body, &raw);
            native_or_reshaped_stream(
                raw,
                &self.session,
                GET_SCHEMAS_SCHEMA.clone(),
                Box::new(move |b| thrift_shape::reshape_schemas(b, catalog_str.as_deref())),
            )
            .await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Tables filtered by catalog (**pattern**), schema (pattern),
    /// table (pattern).
    ///
    /// JDBC `getTables` shape: 10 columns — `TABLE_CAT, TABLE_SCHEM,
    /// TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM,
    /// TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION`. The
    /// last five have no server-side source and are emitted as nulls
    /// per JDBC spec.
    ///
    /// ## Catalog is a LIKE pattern
    ///
    /// Unlike every other catalog argument on this surface, `catalog`
    /// here is a [`LikePattern`], mirroring the Thrift protocol where
    /// `TGetTablesReq.catalogName` is a `TPatternOrIdentifier` (ODBC
    /// `SQLTables` treats `CatalogName` as a pattern-value argument
    /// under the default `SQL_ATTR_METADATA_ID = SQL_FALSE`). Because
    /// the Databricks SEA `SHOW` grammar has **no catalog-pattern
    /// position**, the kernel resolves a wildcard catalog by
    /// enumerate-then-fan-out rather than by scanning every catalog:
    ///
    /// - **Wildcard catalog** (`%` / `_` present, unescaped): `SHOW
    ///   CATALOGS`, filter the catalog *names* with [`LikeMatcher`], then
    ///   issue one `SHOW TABLES IN CATALOG <name>` per match (schema /
    ///   table / type pushed down server-side) with bounded parallelism;
    ///   the per-catalog results are concatenated and re-sorted into the
    ///   JDBC row order. This is cheaper than `SHOW TABLES IN ALL
    ///   CATALOGS` for a selective pattern and needs no client-side row
    ///   filtering (each query is already catalog-scoped).
    /// - **Literal catalog** (no active wildcards, e.g. `main`, or an
    ///   escaped `foo\_bar`, or the ODBC `SQL_ATTR_METADATA_ID =
    ///   SQL_TRUE` identifier case): unescape and scope server-side via
    ///   `SHOW TABLES IN CATALOG <name>` — the fast exact path.
    /// - **`None`**: a single `SHOW TABLES IN ALL CATALOGS` — genuinely
    ///   "every catalog", so no enumeration is needed.
    /// - **Empty pattern** (`Some("")`): "match nothing" per
    ///   [`LikePattern`] — returns an empty result with no wire call
    ///   (the schema/table `LIKE ''` convention, applied to the catalog).
    ///
    /// Catalog *names* are matched by [`LikeMatcher`] (a best-effort
    /// reproduction of the server's `SHOW ... LIKE` matching:
    /// case-insensitive, anchored), while schema / table patterns are
    /// matched *server-side* by the per-catalog queries. If Databricks
    /// `SHOW` matching semantics ever drift from `LikeMatcher`, catalog
    /// selection could differ from the schema/table dimensions — keep the
    /// two in sync if server matching changes.
    ///
    /// `list_columns` deliberately keeps an exact catalog identifier —
    /// the Thrift `TGetColumnsReq.catalogName` is a `TIdentifier` that
    /// "must not contain a search pattern", so catalog wildcarding is a
    /// `getTables`-only behaviour.
    ///
    /// `table_types`, when `Some(non-empty)`, filters the result to
    /// rows whose `TABLE_TYPE` matches one of the listed strings —
    /// matches `java.sql.DatabaseMetaData.getTables` behaviour.
    /// Databricks `SHOW TABLES` itself does not accept a type filter,
    /// so the filter is applied kernel-side after reshape.
    pub async fn list_tables(
        &self,
        catalog: Option<LikePattern<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        table_types: Option<&[&str]>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListTables);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            // Route the catalog argument. The SEA `SHOW` grammar has no
            // catalog-pattern position, so a wildcard catalog can't be
            // scoped server-side. Rather than `SHOW TABLES IN ALL
            // CATALOGS` (which scans every table in every catalog and
            // discards non-matches — slow on large workspaces), we
            // enumerate catalogs, filter their names client-side, and fan
            // out one scoped `SHOW TABLES IN CATALOG <name>` per match.
            match catalog.as_ref().map(|p| p.as_str()) {
                // No catalog filter, or a pattern that matches *every*
                // catalog (`%`, `%%`, …): genuinely "every catalog". A
                // single `SHOW TABLES IN ALL CATALOGS` is the right (and
                // cheapest) query — no enumeration or fan-out. Routing `%`
                // here (rather than the fan-out arm below) keeps it
                // semantically identical to `None`: one round trip, and one
                // inaccessible catalog can't fail the whole call.
                None => {
                    self.list_tables_single_catalog(
                        None,
                        schema_pattern,
                        table_pattern,
                        table_types,
                        &telemetry_for_body,
                    )
                    .await
                }
                Some(pat) if SqlCommandBuilder::pattern_matches_all(pat) => {
                    self.list_tables_single_catalog(
                        None,
                        schema_pattern,
                        table_pattern,
                        table_types,
                        &telemetry_for_body,
                    )
                    .await
                }
                // Empty pattern is "match nothing" per `LikePattern`
                // (mirrors the schema/table `LIKE ''` convention). Emit an
                // empty result without a wire call.
                Some("") => {
                    Self::observe_inline_result(&telemetry_for_body);
                    ResultStream::from_reader(Box::new(EmptyReader::new(GET_TABLES_SCHEMA.clone())))
                        .await
                }
                // A selective wildcard pattern, or a literal the `SHOW`
                // builder would misread as "all catalogs" (`*`), is
                // resolved by enumerate-then-fan-out. `LikeMatcher` treats a
                // literal (no `%` / `_`) as an exact anchored name, so this
                // arm covers both "match some catalogs" and the `*`-literal
                // edge with one path. (A match-*all* `%` was already routed
                // to the single-query path above.)
                Some(pat)
                    if SqlCommandBuilder::pattern_has_wildcards(pat)
                        || SqlCommandBuilder::is_wildcard_or_empty_scope(pat) =>
                {
                    self.list_tables_matching_catalogs(
                        pat,
                        schema_pattern,
                        table_pattern,
                        table_types,
                        &telemetry_for_body,
                    )
                    .await
                }
                // Literal catalog: scope server-side (fast path). Unescape
                // `\_` / `\%` first so the identifier is the intended name.
                Some(pat) => {
                    let literal = SqlCommandBuilder::strip_jdbc_escapes(pat);
                    self.list_tables_single_catalog(
                        Some(literal.as_str()),
                        schema_pattern,
                        table_pattern,
                        table_types,
                        &telemetry_for_body,
                    )
                    .await
                }
            }
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Enumerate catalogs, keep those matching `pattern`
    /// (case-insensitive ODBC/JDBC LIKE), and fan out one scoped
    /// `SHOW TABLES IN CATALOG <name>` per match — reproducing Thrift's
    /// catalog wildcarding without the whole-workspace scan of
    /// `SHOW TABLES IN ALL CATALOGS`. The per-catalog queries run with
    /// bounded parallelism; their reshaped rows are concatenated and
    /// re-sorted into the JDBC row order.
    async fn list_tables_matching_catalogs(
        &self,
        pattern: &str,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        table_types: Option<&[&str]>,
        telemetry: &StatementTelemetry,
    ) -> Result<ResultStream> {
        use futures::stream::{FuturesUnordered, StreamExt};

        // 1. Enumerate + filter catalog names client-side.
        let matcher = LikeMatcher::new(pattern);
        let noop = StatementTelemetry::noop();
        let mut catalogs = self.list_catalogs_uninstrumented(&noop).await?;
        let mut names: Vec<String> = Vec::new();
        let mut enumerated = 0usize;
        while let Some(batch) = catalogs.next_batch().await? {
            let ResultBatch::Arrow(rb) = batch;
            // GET_CATALOGS_SCHEMA is a single `TABLE_CAT: Utf8` column.
            let col = rb
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| Error::data_loss().message("TABLE_CAT is not Utf8"))?;
            for i in 0..col.len() {
                enumerated += 1;
                if !col.is_null(i) && matcher.matches(col.value(i)) {
                    names.push(col.value(i).to_owned());
                }
            }
        }
        catalogs.close();
        // One correlatable line tying the fan-out back to its cause: how
        // many catalogs a wildcard `list_tables` matched (and will query),
        // vs how many exist. Lets on-call spot fan-out amplification (a
        // loose pattern hitting hundreds of catalogs) without counting the
        // per-query SQL debug lines from the transport.
        debug!(
            catalog_pattern = pattern,
            catalogs_enumerated = enumerated,
            catalogs_matched = names.len(),
            "list_tables catalog wildcard fan-out"
        );

        // No catalog matched → typed empty result, no further wire calls.
        if names.is_empty() {
            Self::observe_inline_result(telemetry);
            return ResultStream::from_reader(Box::new(EmptyReader::new(
                GET_TABLES_SCHEMA.clone(),
            )))
            .await;
        }

        // 2. Fan out `SHOW TABLES IN CATALOG <name>` per match, bounded.
        // Databricks metadata calls are cheap but the warehouse shouldn't
        // be hit with an unbounded burst when a loose pattern matches many
        // catalogs.
        const MAX_INFLIGHT: usize = 8;
        let schema_str = schema_pattern.as_ref().map(|p| p.as_str().to_owned());
        let table_str = table_pattern.as_ref().map(|p| p.as_str().to_owned());
        let types_owned: Option<Vec<String>> =
            table_types.map(|s| s.iter().map(|t| (*t).to_owned()).collect());

        let mut inflight = FuturesUnordered::new();
        let mut remaining = names.into_iter();
        for name in remaining.by_ref().take(MAX_INFLIGHT) {
            inflight.push(self.fetch_tables_for_catalog(
                name,
                schema_str.clone(),
                table_str.clone(),
                types_owned.clone(),
            ));
        }
        let mut all_batches: Vec<arrow_array::RecordBatch> = Vec::new();
        while let Some(result) = inflight.next().await {
            all_batches.extend(result?);
            if let Some(name) = remaining.next() {
                inflight.push(self.fetch_tables_for_catalog(
                    name,
                    schema_str.clone(),
                    table_str.clone(),
                    types_owned.clone(),
                ));
            }
        }

        // 3. Concatenate + re-sort into the JDBC row order (each
        // per-catalog batch was sorted only within its own catalog).
        let merged = if all_batches.is_empty() {
            None
        } else {
            let concatenated =
                arrow_select::concat::concat_batches(&GET_TABLES_SCHEMA, &all_batches)
                    .map_err(|e| Error::data_loss().message(format!("merge tables: {e}")))?;
            Some(thrift_shape::sort_tables_batch(&concatenated)?)
        };
        // The fan-out performs real per-catalog server statements, but the
        // caller receives one merged in-memory result and there is no single
        // statement id to attribute. Record that aggregate as one synthetic
        // inline LIST_TABLES event; the internal catalog enumeration uses
        // noop telemetry to avoid double-counting a LIST_CATALOGS operation.
        Self::observe_inline_result(telemetry);
        ResultStream::from_reader(Box::new(InMemoryReader::new(
            GET_TABLES_SCHEMA.clone(),
            merged.into_iter().collect(),
        )))
        .await
    }

    /// Fetch + reshape `SHOW TABLES IN CATALOG <catalog>` into JDBC-shape
    /// batches. An object-not-found error (catalog dropped between
    /// enumeration and this call) collapses to zero rows, matching the
    /// JDBC empty-result contract. Used by the fan-out path.
    async fn fetch_tables_for_catalog(
        &self,
        catalog: String,
        schema_pattern: Option<String>,
        table_pattern: Option<String>,
        table_types: Option<Vec<String>>,
    ) -> Result<Vec<arrow_array::RecordBatch>> {
        let types_ref: Option<Vec<&str>> = table_types
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());
        let raw = match self
            .session
            .transport()
            .list_tables(
                self.session.session_id(),
                Some(catalog.as_str()),
                schema_pattern.as_deref(),
                table_pattern.as_deref(),
                types_ref.as_deref(),
            )
            .await
        {
            Ok(r) => r,
            // Catalog vanished mid-enumeration → contribute no rows.
            Err(e) if thrift_shape::is_object_not_found(&e) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        // Read + reshape into `out`. Capture the result rather than `?`-ing
        // out early: the fan-out opens one statement per catalog and
        // `ExecuteResult` has no Drop-based close, so a mid-stream read
        // error must still close this statement or it leaks server-side
        // (unlike the streaming paths, which close via a StatementCloseGuard).
        //
        // `close_id` returns an empty id when the server already reported
        // `status.state == Closed` (statement reaped server-side); skip the
        // redundant CloseStatement DELETE in that case, mirroring the
        // streaming metadata paths and the regular-query path in
        // `statement/mutable.rs`.
        let stmt_id = close_id(&raw);
        let native = is_native_metadata_result(&raw);
        let mut reader = raw.reader;
        let read: Result<Vec<arrow_array::RecordBatch>> = async {
            let mut out = Vec::new();
            while let Some(batch) = reader.next_batch().await? {
                out.push(if native {
                    let native_batch =
                        normalize_native_metadata_batch(&batch, GET_TABLES_SCHEMA.clone())?;
                    thrift_shape::process_native_tables(
                        &native_batch,
                        Some(catalog.as_str()),
                        types_ref.as_deref(),
                    )?
                } else {
                    thrift_shape::reshape_tables(
                        &batch,
                        Some(catalog.as_str()),
                        types_ref.as_deref(),
                    )?
                });
            }
            Ok(out)
        }
        .await;
        // Best-effort close on every exit (success or error), unless the
        // statement was already closed server-side.
        if !stmt_id.is_empty() {
            self.session
                .transport()
                .close_statement(&stmt_id)
                .await
                .ok();
        }
        read
    }

    /// `SHOW TABLES IN CATALOG <catalog>` (or `IN ALL CATALOGS` when
    /// `catalog` is None) as a single lazily-streamed query — the exact
    /// literal-catalog fast path.
    async fn list_tables_single_catalog(
        &self,
        catalog: Option<&str>,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        table_types: Option<&[&str]>,
        telemetry: &StatementTelemetry,
    ) -> Result<ResultStream> {
        let requested_catalog = catalog.map(|c| c.to_owned());
        let types_owned: Option<Vec<String>> =
            table_types.map(|s| s.iter().map(|t| (*t).to_owned()).collect());
        let raw = match self
            .session
            .transport()
            .list_tables(
                self.session.session_id(),
                catalog,
                schema_pattern.as_ref().map(|p| p.as_str()),
                table_pattern.as_ref().map(|p| p.as_str()),
                table_types,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return not_found_or(e, GET_TABLES_SCHEMA.clone(), telemetry, Err).await,
        };
        Self::observe_raw_result(telemetry, &raw);
        let stmt_id = close_id(&raw);
        let native = is_native_metadata_result(&raw);
        let native_schema = GET_TABLES_SCHEMA.clone();
        let reshaped = ThriftReshapingReader::new(
            raw.reader,
            GET_TABLES_SCHEMA.clone(),
            Box::new(move |b| {
                let types_ref: Option<Vec<&str>> = types_owned
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.as_str()).collect());
                if native {
                    let native_batch = normalize_native_metadata_batch(b, native_schema.clone())?;
                    thrift_shape::process_native_tables(
                        &native_batch,
                        requested_catalog.as_deref(),
                        types_ref.as_deref(),
                    )
                } else {
                    thrift_shape::reshape_tables(
                        b,
                        requested_catalog.as_deref(),
                        types_ref.as_deref(),
                    )
                }
            }),
        );
        ResultStream::from_reader_with_close(Box::new(reshaped), Arc::clone(&self.session), stmt_id)
            .await
    }

    /// Columns of tables matching the filter. Catalog is optional and
    /// exact; schema / table / column are patterns.
    /// An empty schema, table, or column pattern returns an empty result
    /// without a wire call.
    ///
    /// `catalog = None` emits `SHOW COLUMNS IN ALL CATALOGS` and
    /// returns rows from every catalog visible to the session. The
    /// server response carries a `catalogName` column so each row's
    /// `TABLE_CAT` is filled correctly without client-side
    /// enumeration. (JDBC's `getColumns(null, …)` historically
    /// enumerates catalogs and unions the results; the kernel does
    /// it in a single round trip.)
    ///
    /// JDBC `getColumns` shape: 23 columns — `TABLE_CAT, TABLE_SCHEM,
    /// TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE,
    /// BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE,
    /// REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
    /// CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,
    /// SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE, SOURCE_DATA_TYPE,
    /// IS_AUTO_INCREMENT`. `DATA_TYPE` is derived per-row from the
    /// server's `columnType` string via the JDBC type-code table;
    /// `TYPE_NAME` preserves the server's precise type string.
    /// Columns without a server-side source (`BUFFER_LENGTH`,
    /// `SQL_DATA_TYPE`, `SQL_DATETIME_SUB`, `CHAR_OCTET_LENGTH`,
    /// `COLUMN_DEF`, `SCOPE_*`, `SOURCE_DATA_TYPE`) are emitted as
    /// null per JDBC spec.
    pub async fn list_columns(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        table_pattern: Option<LikePattern<'_>>,
        column_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListColumns);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            if [&schema_pattern, &table_pattern, &column_pattern]
                .into_iter()
                .any(|pattern| {
                    pattern
                        .as_ref()
                        .is_some_and(|pattern| pattern.as_str().is_empty())
                })
            {
                Self::observe_inline_result(&telemetry_for_body);
                return ResultStream::from_reader(Box::new(EmptyReader::new(
                    GET_COLUMNS_SCHEMA.clone(),
                )))
                .await;
            }
            let catalog_str = catalog.as_ref().map(|i| i.as_str().to_owned());
            let raw = match self
                .session
                .transport()
                .list_columns(
                    self.session.session_id(),
                    catalog.as_ref().map(|i| i.as_str()),
                    schema_pattern.as_ref().map(|p| p.as_str()),
                    table_pattern.as_ref().map(|p| p.as_str()),
                    column_pattern.as_ref().map(|p| p.as_str()),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return not_found_or(e, GET_COLUMNS_SCHEMA.clone(), &telemetry_for_body, Err)
                        .await
                }
            };
            Self::observe_raw_result(&telemetry_for_body, &raw);
            native_or_reshaped_stream(
                raw,
                &self.session,
                GET_COLUMNS_SCHEMA.clone(),
                Box::new(move |b| thrift_shape::reshape_columns(b, catalog_str.as_deref())),
            )
            .await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Primary keys for the given table. All three identifiers are
    /// exact names — ODBC SQLPrimaryKeys does not support patterns.
    ///
    /// Server shape (Databricks `SHOW KEYS`): `catalogName,
    /// namespace, tableName, col_name, keySeq, constraintName,
    /// constraintType, parentCatalogName, parentNamespace,
    /// parentTableName, parentColName, updateRule, deleteRule,
    /// deferrability`. The result is reshaped kernel-side into the
    /// JDBC `getPrimaryKeys` column set (`TABLE_CAT`, `TABLE_SCHEM`,
    /// `TABLE_NAME`, `COLUMN_NAME`, `KEY_SEQ`, `PK_NAME`) by
    /// [`thrift_shape::reshape_primary_keys`] so every binding sees the
    /// canonical names without re-mapping.
    ///
    /// Catalog and schema are required at the kernel layer:
    /// Databricks `SHOW KEYS` needs `IN CATALOG / IN SCHEMA / IN
    /// TABLE` fully qualified. Wrappers that want to fall back to a
    /// session default should resolve it before calling.
    pub async fn primary_keys(
        &self,
        catalog: Option<Identifier<'_>>,
        schema: Option<Identifier<'_>>,
        table: Identifier<'_>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListPrimaryKeys);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            let catalog = catalog.ok_or_else(|| {
                Error::invalid_argument().message("primary_keys requires a catalog identifier")
            })?;
            let schema = schema.ok_or_else(|| {
                Error::invalid_argument().message("primary_keys requires a schema identifier")
            })?;
            let raw = match self
                .session
                .transport()
                .list_primary_keys(
                    self.session.session_id(),
                    catalog.as_str(),
                    schema.as_str(),
                    table.as_str(),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return not_found_or(
                        e,
                        GET_PRIMARY_KEYS_SCHEMA.clone(),
                        &telemetry_for_body,
                        Err,
                    )
                    .await
                }
            };
            Self::observe_raw_result(&telemetry_for_body, &raw);
            native_or_reshaped_stream(
                raw,
                &self.session,
                GET_PRIMARY_KEYS_SCHEMA.clone(),
                Box::new(thrift_shape::reshape_primary_keys),
            )
            .await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Foreign-key relationships. Both parent and foreign sides are
    /// exact identifiers — ODBC SQLForeignKeys does not support
    /// patterns. The parent side is optional and matches "any parent"
    /// when omitted, matching the underlying `SHOW FOREIGN KEYS IN
    /// TABLE` semantics. Wrappers that want to query by parent only
    /// should resolve to a foreign-side filter before calling.
    ///
    /// ## Unspecified foreign table → empty result
    ///
    /// A `None` `foreign_table` means "no foreign table to look up", so
    /// the call returns an **empty result set** (0 rows, correct JDBC
    /// shape) and issues **no** server statement — never an error. This
    /// mirrors the JDBC SEA `listCrossReferences` contract (which keys
    /// solely off the foreign table) and covers the ODBC "primary-keys
    /// only" call form (`SQLForeignKeys` with the FK table left NULL) as
    /// well as the both-tables-NULL case. Only when a `foreign_table`
    /// *is* supplied are `foreign_catalog` / `foreign_schema` required —
    /// the `SHOW FOREIGN KEYS IN CATALOG … IN SCHEMA … IN TABLE …`
    /// grammar needs all three to scope the lookup.
    ///
    /// Server shape: same as [`primary_keys`](Self::primary_keys) —
    /// `SHOW FOREIGN KEYS` shares its result schema with `SHOW KEYS`.
    /// The result is reshaped kernel-side into the JDBC
    /// `getCrossReference` column set (`PKTABLE_*`, `FKTABLE_*`,
    /// `KEY_SEQ`, `UPDATE_RULE`, `DELETE_RULE`, `FK_NAME`, `PK_NAME`,
    /// `DEFERRABILITY`) by [`thrift_shape::reshape_foreign_keys`].
    /// `UPDATE_RULE` / `DELETE_RULE` / `PK_NAME` have no source in the
    /// server response and are emitted as typed nulls (not synthesized).
    pub async fn foreign_keys(
        &self,
        parent_catalog: Option<Identifier<'_>>,
        parent_schema: Option<Identifier<'_>>,
        parent_table: Option<Identifier<'_>>,
        foreign_catalog: Option<Identifier<'_>>,
        foreign_schema: Option<Identifier<'_>>,
        foreign_table: Option<Identifier<'_>>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListCrossReferences);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            // Unspecified foreign table → "nothing to look up": empty
            // result, no wire call (JDBC SEA `listCrossReferences`
            // contract; also the ODBC PK-only `SQLForeignKeys` form and
            // the both-tables-NULL case). Handled before the
            // catalog/schema requirements so a null foreign table never
            // surfaces as an argument error.
            let Some(foreign_table) = foreign_table else {
                Self::observe_inline_result(&telemetry_for_body);
                return ResultStream::from_reader(Box::new(EmptyReader::new(
                    GET_CROSS_REFERENCE_SCHEMA.clone(),
                )))
                .await;
            };
            // A foreign table *is* specified, so its catalog and schema
            // are required to scope the `SHOW FOREIGN KEYS IN CATALOG …
            // IN SCHEMA … IN TABLE …` lookup.
            let foreign_catalog = foreign_catalog.ok_or_else(|| {
                Error::invalid_argument().message("foreign_keys requires foreign_catalog")
            })?;
            let foreign_schema = foreign_schema.ok_or_else(|| {
                Error::invalid_argument().message("foreign_keys requires foreign_schema")
            })?;
            let parent_catalog = parent_catalog.as_ref().map(|i| i.as_str().to_owned());
            let parent_schema = parent_schema.as_ref().map(|i| i.as_str().to_owned());
            let parent_table = parent_table.as_ref().map(|i| i.as_str().to_owned());
            let raw = match self
                .session
                .transport()
                .list_foreign_keys(
                    self.session.session_id(),
                    parent_catalog.as_deref(),
                    parent_schema.as_deref(),
                    parent_table.as_deref(),
                    foreign_catalog.as_str(),
                    foreign_schema.as_str(),
                    foreign_table.as_str(),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return not_found_or(
                        e,
                        GET_CROSS_REFERENCE_SCHEMA.clone(),
                        &telemetry_for_body,
                        Err,
                    )
                    .await
                }
            };
            Self::observe_raw_result(&telemetry_for_body, &raw);
            let stmt_id = close_id(&raw);
            let native = is_native_metadata_result(&raw);
            let native_schema = GET_CROSS_REFERENCE_SCHEMA.clone();
            let reshaped = ThriftReshapingReader::new(
                raw.reader,
                GET_CROSS_REFERENCE_SCHEMA.clone(),
                Box::new(move |batch| {
                    let canonical = if native {
                        normalize_native_metadata_batch(batch, native_schema.clone())?
                    } else {
                        thrift_shape::reshape_foreign_keys(batch)?
                    };
                    thrift_shape::filter_cross_reference_by_parent(
                        &canonical,
                        parent_catalog.as_deref(),
                        parent_schema.as_deref(),
                        parent_table.as_deref(),
                    )
                }),
            );
            ResultStream::from_reader_with_close(
                Box::new(reshaped),
                Arc::clone(&self.session),
                stmt_id,
            )
            .await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Functions visible to the session. Catalog is exact; schema /
    /// name are patterns.
    ///
    /// Server shape (`SELECT ... FROM information_schema.routines`):
    /// `routine_catalog, routine_schema, routine_name, comment,
    /// specific_name`. Reshaped kernel-side into the JDBC `getFunctions`
    /// column set by [`thrift_shape::reshape_functions`].
    pub async fn functions(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        name_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListFunctions);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            let raw = match self
                .session
                .transport()
                .list_functions(
                    self.session.session_id(),
                    catalog.as_ref().map(|i| i.as_str()),
                    schema_pattern.as_ref().map(|p| p.as_str()),
                    name_pattern.as_ref().map(|p| p.as_str()),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return not_found_or(e, GET_FUNCTIONS_SCHEMA.clone(), &telemetry_for_body, Err)
                        .await
                }
            };
            Self::observe_raw_result(&telemetry_for_body, &raw);
            native_or_reshaped_stream(
                raw,
                &self.session,
                GET_FUNCTIONS_SCHEMA.clone(),
                Box::new(thrift_shape::reshape_functions),
            )
            .await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Procedures visible to the session. Catalog is exact; schema /
    /// name are patterns.
    ///
    /// Server shape: same columns as [`functions`](Self::functions).
    /// Reshaped kernel-side into the JDBC `getProcedures` column set by
    /// [`thrift_shape::reshape_procedures`].
    pub async fn procedures(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        name_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        // The backend telemetry enum does not have procedure-specific
        // operation types, so match JDBC and fall back to TYPE_UNSPECIFIED.
        let telemetry = self.metadata_telemetry(OperationType::TypeUnspecified);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            let raw = match self
                .session
                .transport()
                .list_procedures(
                    self.session.session_id(),
                    catalog.as_ref().map(|i| i.as_str()),
                    schema_pattern.as_ref().map(|p| p.as_str()),
                    name_pattern.as_ref().map(|p| p.as_str()),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return not_found_or(e, GET_PROCEDURES_SCHEMA.clone(), &telemetry_for_body, Err)
                        .await
                }
            };
            Self::observe_raw_result(&telemetry_for_body, &raw);
            let stmt_id = close_id(&raw);
            let reshaped = ThriftReshapingReader::new(
                raw.reader,
                GET_PROCEDURES_SCHEMA.clone(),
                Box::new(thrift_shape::reshape_procedures),
            );
            ResultStream::from_reader_with_close(
                Box::new(reshaped),
                Arc::clone(&self.session),
                stmt_id,
            )
            .await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Procedure parameter / return columns. Catalog is exact; schema /
    /// procedure / column are patterns. Mirrors JDBC
    /// `getProcedureColumns` (procedures only — function arguments are
    /// excluded by the `routine_type = 'PROCEDURE'` JOIN in the SQL
    /// builder). Server shape is the raw `information_schema.parameters`
    /// projection; reshaped kernel-side into the JDBC
    /// `getProcedureColumns` column set by
    /// [`thrift_shape::reshape_procedure_columns`].
    pub async fn procedure_columns(
        &self,
        catalog: Option<Identifier<'_>>,
        schema_pattern: Option<LikePattern<'_>>,
        procedure_pattern: Option<LikePattern<'_>>,
        column_pattern: Option<LikePattern<'_>>,
    ) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        // The backend telemetry enum does not have procedure-specific
        // operation types, so match JDBC and fall back to TYPE_UNSPECIFIED.
        let telemetry = self.metadata_telemetry(OperationType::TypeUnspecified);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            let raw = match self
                .session
                .transport()
                .list_procedure_columns(
                    self.session.session_id(),
                    catalog.as_ref().map(|i| i.as_str()),
                    schema_pattern.as_ref().map(|p| p.as_str()),
                    procedure_pattern.as_ref().map(|p| p.as_str()),
                    column_pattern.as_ref().map(|p| p.as_str()),
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return not_found_or(
                        e,
                        GET_PROCEDURE_COLUMNS_SCHEMA.clone(),
                        &telemetry_for_body,
                        Err,
                    )
                    .await
                }
            };
            Self::observe_raw_result(&telemetry_for_body, &raw);
            let stmt_id = close_id(&raw);
            let reshaped = ThriftReshapingReader::new(
                raw.reader,
                GET_PROCEDURE_COLUMNS_SCHEMA.clone(),
                Box::new(thrift_shape::reshape_procedure_columns),
            );
            ResultStream::from_reader_with_close(
                Box::new(reshaped),
                Arc::clone(&self.session),
                stmt_id,
            )
            .await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// Static SQL type catalogue matching Thrift `getTypeInfo`. No filters and
    /// no wire call.
    pub async fn type_info(&self) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListTypeInfo);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            Self::observe_inline_result(&telemetry_for_body);
            let reader = static_data::type_info_reader();
            ResultStream::from_reader(reader).await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
    }

    /// All table types (`TABLE`, `VIEW`, `SYSTEM TABLE`, …). No
    /// filters. No wire call — returns a static in-memory result
    /// driven by the session's transport list (see
    /// [`Transport::list_table_types`](crate::client::Transport::list_table_types)).
    pub async fn table_types(&self) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let telemetry = self.metadata_telemetry(OperationType::ListTableTypes);
        let telemetry_for_body = telemetry.clone();
        let result = async {
            Self::observe_inline_result(&telemetry_for_body);
            let types = self.session.transport().list_table_types();
            let reader = static_data::table_types_reader(&types);
            ResultStream::from_reader(reader).await
        }
        .instrument(span)
        .await;
        Self::finish_metadata_call(telemetry, result)
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
    fn like_pattern_accepts_empty() {
        // Empty pattern is valid → `LIKE ''` (match-nothing), matching the
        // Thrift/JDBC empty-pattern convention. Non-empty whitespace-only is
        // still rejected (see like_pattern_rejects_whitespace_only).
        let p = LikePattern::new("").expect("empty pattern accepted");
        assert_eq!(p.as_str(), "");
    }

    #[test]
    fn exact_catalog_and_pattern_keeps_non_empty_catalog_exact() {
        let catalog = Some("my_catalog".to_owned());
        let pattern = Some("schema%".to_owned());
        let (catalog, pattern) = exact_catalog_and_pattern(&catalog, &pattern).unwrap();

        assert_eq!(catalog.unwrap().as_str(), "my_catalog");
        assert_eq!(pattern.unwrap().as_str(), "schema%");
    }

    #[test]
    fn exact_catalog_and_pattern_routes_empty_catalog_to_empty_pattern() {
        let catalog = Some(String::new());
        let original_pattern = Some("ignored%".to_owned());
        let (catalog, pattern) = exact_catalog_and_pattern(&catalog, &original_pattern).unwrap();

        assert!(catalog.is_none());
        assert_eq!(pattern.unwrap().as_str(), "");
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

    #[test]
    fn identifier_rejects_whitespace_only() {
        for s in &["   ", "\t", "\n", " \t "] {
            let err = Identifier::new(s).expect_err("whitespace-only identifier rejected");
            assert_eq!(err.code, ErrorCode::InvalidArgument);
            assert!(
                err.message.contains("whitespace"),
                "expected 'whitespace' in: {}",
                err.message
            );
        }
    }

    #[test]
    fn like_pattern_rejects_whitespace_only() {
        for s in &["   ", "\t", "\n", " \t "] {
            let err = LikePattern::new(s).expect_err("whitespace-only pattern rejected");
            assert_eq!(err.code, ErrorCode::InvalidArgument);
            assert!(
                err.message.contains("whitespace"),
                "expected 'whitespace' in: {}",
                err.message
            );
        }
    }

    // ─── from_opt — the wrapper-facing Option<String> adapters ──────────

    #[test]
    fn identifier_from_opt_none_is_ok_none() {
        let input: Option<String> = None;
        assert!(Identifier::from_opt(&input).unwrap().is_none());
    }

    #[test]
    fn identifier_from_opt_real_name_is_ok_some() {
        let input = Some("main".to_owned());
        let id = Identifier::from_opt(&input).unwrap().expect("some");
        assert_eq!(id.as_str(), "main");
    }

    #[test]
    fn identifier_from_opt_rejects_whitespace_only() {
        let input = Some("  ".to_owned());
        let err = Identifier::from_opt(&input).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn like_pattern_from_opt_none_is_ok_none() {
        let input: Option<String> = None;
        assert!(LikePattern::from_opt(&input).unwrap().is_none());
    }

    #[test]
    fn like_pattern_from_opt_real_pattern_is_ok_some() {
        let input = Some("info%".to_owned());
        let pat = LikePattern::from_opt(&input).unwrap().expect("some");
        assert_eq!(pat.as_str(), "info%");
    }

    #[test]
    fn like_pattern_from_opt_rejects_whitespace_only() {
        let input = Some("  ".to_owned());
        let err = LikePattern::from_opt(&input).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }
}
