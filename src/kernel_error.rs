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

//! Typed error model for the v0 Kernel API.
//!
//! `Error` carries structured fields (`code`, `message`, `sql_state`,
//! `vendor_code`, `http_status`, `retryable`, `query_id`, `cause`) so
//! wrappers can surface them verbatim without reparsing strings.
//! `ErrorCode` is the discriminator drivers branch on.
//!
//! ## Construction
//!
//! Two equivalent forms:
//!
//! ```ignore
//! // Long form — explicit code + message:
//! Error::new(ErrorCode::InvalidArgument, "bad host URL")
//!
//! // Short form — categorised constructor + chainable message:
//! Error::invalid_argument().message("bad host URL")
//! ```
//!
//! The short form is ergonomically modelled on a categorised-constructor
//! + chainable builder pattern, so error sites stay terse and readable.
//!
//! ## What's `#[non_exhaustive]` and what isn't
//!
//! - `ErrorCode` **is** `#[non_exhaustive]`: v0 ships 13 variants and
//!   the addendum-A8 expansion (AlreadyExists, FailedPrecondition,
//!   Aborted, Unimplemented, …) lands in a follow-on PR. External
//!   `match` arms get a forced `_ =>` fallback — they keep compiling
//!   when the enum grows.
//! - `Error` **is not** `#[non_exhaustive]`: wrappers want to
//!   pattern-match (`let Error { code, message, sql_state, .. }`) for
//!   logging and host-language exception mapping. New fields go
//!   through `Error::new(...).with_*(...)`, so adding a field is
//!   non-breaking without the marker. Keeping the marker would block
//!   useful pattern-matches for no real benefit.

#![allow(dead_code)]

use std::fmt::Write;

/// Discriminator for [`Error`].
///
/// `#[non_exhaustive]`: addendum A8 expands the variant set in a
/// follow-on PR (AlreadyExists, FailedPrecondition, Aborted,
/// Unimplemented, plus an `ErrorCategory` tri-state). Wrappers writing
/// `match` against `ErrorCode` must include a `_ =>` arm so growing
/// the enum stays non-breaking.
///
/// Each variant doc names the meaning, the typical SQLSTATE / HTTP
/// mapping (when applicable), and whether the kernel treats it as
/// retryable by default.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Caller-side input was malformed (missing required field, bad
    /// enum value, ordinal 0 on a 1-based parameter, malformed URL).
    /// Maps to ODBC SQLSTATE `HY009` / `HY090`. Not retryable.
    InvalidArgument,

    /// Authentication failed or no credentials. Server returned HTTP
    /// 401 (or OIDC discovery rejected the token request). Maps to
    /// SQLSTATE `28000`. Not retryable without re-auth.
    Unauthenticated,

    /// Authenticated but not authorized for this resource. Server
    /// returned HTTP 403. Maps to SQLSTATE `42501`. Not retryable.
    PermissionDenied,

    /// Object not found — catalog, schema, table, or `statement_id`.
    /// Server returned HTTP 404 or `STATEMENT_NOT_FOUND`. Not
    /// retryable.
    NotFound,

    /// Quota or rate limit exceeded. Server returned HTTP 429 (or a
    /// 4xx with a Retry-After header). Retryable with backoff.
    ResourceExhausted,

    /// Transient service unavailability. Server returned HTTP 503
    /// (warehouse cold-starting, deployment in progress). Retryable
    /// with backoff.
    Unavailable,

    /// Operation timed out — request body, poll deadline, or
    /// `await_result` ceiling. Retryable depending on idempotency.
    Timeout,

    /// Operation was cancelled — caller-side `cancel()`, dropped
    /// future, or server-side `CancelStatement`. Not retryable
    /// (the cancellation was deliberate).
    Cancelled,

    /// Result data corruption or partial response — chunk download
    /// returned wrong byte count, manifest checksum mismatch, etc.
    /// Maps loosely to ODBC SQLSTATE `01004`. Sometimes retryable.
    DataLoss,

    /// Unexpected internal kernel state — kernel panic, unreachable
    /// branch, dropped invariant. SQLSTATE `HY000`. Not retryable;
    /// indicates a kernel bug.
    Internal,

    /// Statement handle is invalid — usually because the parent
    /// `Statement` was re-executed (the validity flag flipped) or
    /// the handle was already closed. Distinct from `NotFound`
    /// because the handle once existed; the *server* still has its
    /// record. Local short-circuit; no wire call. Not retryable.
    InvalidStatementHandle,

    /// Transport-layer failure — DNS resolution, TCP handshake, TLS
    /// negotiation. Distinct from `Unavailable` (which is server-
    /// reported transient). Retryable with backoff except for certificate
    /// verification failures.
    NetworkError,

    /// Server-reported SQL execution error. `sql_state` and
    /// `vendor_code` carry the structured detail per Databricks
    /// SQLSTATE conventions. Retryability depends on the SQLSTATE
    /// class — `40xxx` (transient) vs `42xxx` (syntactic /
    /// semantic); the addendum-A8 `ErrorCategory` will encode this.
    SqlError,
}

impl ErrorCode {
    /// Stable string slug for cross-binding wire formats. Single source of
    /// truth shared by the pyo3 (`code_str`) and napi (`error_code_str`)
    /// bindings — keeping the mapping here makes drift across bindings
    /// structurally impossible.
    pub fn slug(&self) -> &'static str {
        match self {
            ErrorCode::InvalidArgument => "InvalidArgument",
            ErrorCode::Unauthenticated => "Unauthenticated",
            ErrorCode::PermissionDenied => "PermissionDenied",
            ErrorCode::NotFound => "NotFound",
            ErrorCode::ResourceExhausted => "ResourceExhausted",
            ErrorCode::Unavailable => "Unavailable",
            ErrorCode::Timeout => "Timeout",
            ErrorCode::Cancelled => "Cancelled",
            ErrorCode::DataLoss => "DataLoss",
            ErrorCode::Internal => "Internal",
            ErrorCode::InvalidStatementHandle => "InvalidStatementHandle",
            ErrorCode::NetworkError => "NetworkError",
            ErrorCode::SqlError => "SqlError",
        }
    }
}

/// Typed kernel error.
///
/// `cause` chains preserve the underlying provenance — a `SqlError`
/// may wrap a `NetworkError` if the failure originated below the SEA
/// layer.
///
/// **Construction.** Build via [`Error::new`] (or one of the
/// categorised constructors like [`Error::invalid_argument`]) and
/// chain `with_*` setters. **Pattern-matching is supported** —
/// wrappers writing a comprehensive logger or host-language exception
/// adapter can do `let Error { code, message, sql_state, .. } = err;`.
/// New fields land through additive setters, so the type does not
/// need `#[non_exhaustive]` (and the marker would block the
/// pattern-matching wrappers actually want).
#[derive(Debug, Clone)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,

    /// 5-character SQLSTATE if the server returned one.
    pub sql_state: Option<String>,

    /// Databricks-specific error identifier from the SEA
    /// `ServiceError.error_code` envelope — a stable string slug like
    /// `STATEMENT_NOT_FOUND` or `WAREHOUSE_STOPPED` that wrappers can
    /// branch on to map to host-language exceptions (PySQL's
    /// `OperationalError` taxonomy, JDBC's `SQLException` subclasses).
    /// Distinct from the JDBC-style numeric [`Error::vendor_code`]:
    /// Databricks ships string slugs on the wire today, not integers.
    pub error_code: Option<String>,

    /// Numeric vendor code in the JDBC `SQLException.getErrorCode()` /
    /// ODBC `SQLGetDiagRec` sense. Reserved for transports that ship
    /// integer error codes (currently none — SEA REST always uses the
    /// string [`Error::error_code`]). Wrappers that need to populate
    /// JDBC's numeric `vendor_code` slot today should hash or look up
    /// the string `error_code` themselves.
    pub vendor_code: Option<i32>,

    /// Original local OS error code for language-native filesystem errors.
    /// Uses errno on Unix and the Windows system error code on Windows.
    pub os_error_code: Option<i32>,

    /// HTTP status for transport-layer errors.
    pub http_status: Option<u16>,

    /// Whether the kernel considered this retryable. Wrappers may
    /// surface this in driver-specific retry logic.
    pub retryable: bool,

    /// Server-issued statement / query identifier, when known.
    pub query_id: Option<String>,

    /// Source-code location, attached for `Internal` errors so kernel
    /// bugs surface where they happened. Free-form `file:line` string;
    /// not parsed.
    pub location: Option<String>,

    /// Server-supplied user-facing message captured from the SEA
    /// `StatementStatus.display_message` envelope on terminal-error
    /// statements (`FAILED` / `CANCELED` / `CLOSED-no-data`). Mirrors
    /// the same-named field on the Thrift `TGetOperationStatusResp` so
    /// wrappers that surface a Thrift-shaped operation status to their
    /// host language can populate it on the error envelope too.
    ///
    /// Independent of [`Error::message`] — `message` is the
    /// kernel-formatted summary, `display_message` is the raw
    /// server-supplied string.
    pub display_message: Option<String>,

    /// Server-supplied diagnostic detail captured from the SEA
    /// `StatementStatus.diagnostic_info` envelope on terminal-error
    /// statements. Multi-line text with stack frames or operator
    /// context. Intended for support / log surfaces, not user-facing.
    /// Mirrors the same-named Thrift field.
    pub diagnostic_info: Option<String>,

    /// Server-supplied JSON blob with extended error details, captured
    /// from the SEA `StatementStatus.error_details_json` envelope on
    /// terminal-error statements. Pass-through string — wrappers parse
    /// if they need structured access. Mirrors the same-named Thrift
    /// field.
    ///
    /// **Server-side gating:** populated only when the workspace has
    /// `spark.databricks.sql.errorDetailsJson.enabled = true` on the
    /// SQL cluster (`internal()`, default false in
    /// `DatabricksSQLConf.scala` — not settable via session-conf).
    /// Most workspaces will return `None` here even on terminal-
    /// error statements; admin-enabled workspaces return content
    /// shaped like `{"errorClass": "...", "messageTemplate": "..."}`.
    pub error_details_json: Option<String>,

    /// Underlying cause. Boxed to keep the struct stack-friendly.
    pub cause: Option<Box<Error>>,
}

impl Error {
    // ─── Core constructors ──────────────────────────────────────────

    /// Construct a fresh error with the given code and message. All
    /// other fields default to `None` / `false`.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            sql_state: None,
            error_code: None,
            vendor_code: None,
            os_error_code: None,
            http_status: None,
            retryable: false,
            query_id: None,
            location: None,
            display_message: None,
            diagnostic_info: None,
            error_details_json: None,
            cause: None,
        }
    }

    /// Construct a fresh error with the given code and an empty
    /// message. Used by the categorised constructors below; callers
    /// chain [`Error::message`] to populate.
    fn empty(code: ErrorCode) -> Self {
        Self::new(code, "")
    }

    // ─── Categorised constructors ───────────────────────────────────
    //
    // One constructor per `ErrorCode` variant. Each returns an empty
    // message — call sites chain [`Error::message`] /
    // [`Error::with_source`] / [`Error::sqlstate`] to fill in detail.

    /// `ErrorCode::InvalidArgument`. Caller passed a malformed
    /// argument.
    pub fn invalid_argument() -> Self {
        Self::empty(ErrorCode::InvalidArgument)
    }

    /// `ErrorCode::InvalidArgument`. The kernel's analogue of ADBC's
    /// `InvalidState` — the object isn't in a state where this call
    /// is valid (e.g. statement not prepared). v0 collapses
    /// "invalid args" and "invalid state" into one code; the
    /// distinction adds an A8 variant later if drivers need it.
    pub fn invalid_state() -> Self {
        Self::empty(ErrorCode::InvalidArgument)
    }

    /// `ErrorCode::Unauthenticated`.
    pub fn unauthenticated() -> Self {
        Self::empty(ErrorCode::Unauthenticated)
    }

    /// `ErrorCode::PermissionDenied`.
    pub fn permission_denied() -> Self {
        Self::empty(ErrorCode::PermissionDenied)
    }

    /// `ErrorCode::NotFound`.
    pub fn not_found() -> Self {
        Self::empty(ErrorCode::NotFound)
    }

    /// `ErrorCode::ResourceExhausted`. Default `retryable=true`
    /// per the plan's status-mapping table.
    pub fn resource_exhausted() -> Self {
        Self::empty(ErrorCode::ResourceExhausted).with_retryable(true)
    }

    /// `ErrorCode::Unavailable`. Default `retryable=true`.
    pub fn unavailable() -> Self {
        Self::empty(ErrorCode::Unavailable).with_retryable(true)
    }

    /// `ErrorCode::Timeout`. Default `retryable=true`.
    pub fn timeout() -> Self {
        Self::empty(ErrorCode::Timeout).with_retryable(true)
    }

    /// `ErrorCode::Cancelled`.
    pub fn cancelled() -> Self {
        Self::empty(ErrorCode::Cancelled)
    }

    /// `ErrorCode::DataLoss`.
    pub fn data_loss() -> Self {
        Self::empty(ErrorCode::DataLoss)
    }

    /// `ErrorCode::Internal`. The kernel hit an unexpected state —
    /// kernel bug, unreachable branch, dropped invariant. Caller
    /// should attach a `location()` to surface where.
    pub fn internal() -> Self {
        Self::empty(ErrorCode::Internal)
    }

    /// `ErrorCode::Internal` with a source-code `location` already
    /// attached.
    pub fn internal_at(location: impl AsRef<str>) -> Self {
        Self::internal().location(location)
    }

    /// `ErrorCode::InvalidStatementHandle`.
    pub fn invalid_statement_handle() -> Self {
        Self::empty(ErrorCode::InvalidStatementHandle)
    }

    /// `ErrorCode::NetworkError`. Defaults to retryable; override with
    /// [`Self::with_retryable`] for permanent transport failures.
    pub fn network_error() -> Self {
        Self::empty(ErrorCode::NetworkError).with_retryable(true)
    }

    /// `ErrorCode::SqlError`. Server-reported execution error;
    /// caller fills `sql_state` / `vendor_code` from the response
    /// envelope.
    pub fn sql_error() -> Self {
        Self::empty(ErrorCode::SqlError)
    }

    /// I/O error — maps to `Internal` because Databricks-side I/O
    /// failures are kernel internals (chunk download decoding,
    /// Arrow IPC parse). Network-layer I/O uses
    /// [`Error::network_error`] instead.
    pub fn io() -> Self {
        Self::empty(ErrorCode::Internal)
    }

    /// "Not implemented" — there is no `ErrorCode::NotImplemented`
    /// in the v0 surface (addendum A8 may add one). For now this
    /// maps to `InvalidArgument` with a clear message; callers
    /// chain `.message("foo not yet implemented")`.
    pub fn not_implemented() -> Self {
        Self::empty(ErrorCode::InvalidArgument)
    }

    // ─── Builder API ────────────────────────────────────────────────

    /// Append to the error message; clauses are joined with `; `.
    pub fn message(mut self, msg: impl AsRef<str>) -> Self {
        if !self.message.is_empty() {
            self.message.push_str("; ");
        }
        self.message.push_str(msg.as_ref());
        self
    }

    /// Wrap the error with a context phrase, formatted at call time
    /// as `could not <CONTEXT>: <ERROR>` so the typed `Error` doesn't
    /// need a separate field.
    pub fn context(mut self, ctx: impl AsRef<str>) -> Self {
        let ctx = ctx.as_ref();
        if self.message.is_empty() {
            self.message = format!("could not {ctx}");
        } else {
            self.message = format!("could not {ctx}: {}", self.message);
        }
        self
    }

    /// Append a `format_args!` clause to the message (`; `-joined).
    pub fn format(mut self, args: std::fmt::Arguments) -> Self {
        if !self.message.is_empty() {
            self.message.push_str("; ");
        }
        write!(&mut self.message, "{args}").expect("format into String");
        self
    }

    /// Attach a source-code location. Most callers pass
    /// `concat!(file!(), ":", line!())` or similar; the kernel
    /// doesn't impose a format.
    pub fn location(mut self, loc: impl AsRef<str>) -> Self {
        self.location = Some(loc.as_ref().to_owned());
        self
    }

    pub fn with_sql_state(mut self, sql_state: impl Into<String>) -> Self {
        self.sql_state = Some(sql_state.into());
        self
    }

    /// Attach the Databricks-specific [`Error::error_code`] string slug
    /// (e.g. `STATEMENT_NOT_FOUND`, `WAREHOUSE_STOPPED`).
    pub fn with_error_code(mut self, error_code: impl Into<String>) -> Self {
        self.error_code = Some(error_code.into());
        self
    }

    pub fn with_vendor_code(mut self, vendor_code: i32) -> Self {
        self.vendor_code = Some(vendor_code);
        self
    }

    pub fn with_http_status(mut self, http_status: u16) -> Self {
        self.http_status = Some(http_status);
        self
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.query_id = Some(query_id.into());
        self
    }

    /// Attach the server-supplied `display_message` from a terminal-
    /// error SEA `StatementStatus` envelope. Wrappers surfacing a
    /// Thrift-shaped operation-status object to their host language
    /// should populate this on the error so callers see the same
    /// field they would have read from `TGetOperationStatusResp` on
    /// the Thrift path.
    pub fn with_display_message(mut self, msg: impl Into<String>) -> Self {
        self.display_message = Some(msg.into());
        self
    }

    /// Attach the server-supplied `diagnostic_info` from a terminal-
    /// error SEA `StatementStatus` envelope. Multi-line operator /
    /// stack context; for support surfaces, not user-facing.
    pub fn with_diagnostic_info(mut self, info: impl Into<String>) -> Self {
        self.diagnostic_info = Some(info.into());
        self
    }

    /// Attach the server-supplied `error_details_json` blob from a
    /// terminal-error SEA `StatementStatus` envelope. Unbounded
    /// pass-through string; wrappers parse if they need structured
    /// access.
    pub fn with_error_details_json(mut self, json: impl Into<String>) -> Self {
        self.error_details_json = Some(json.into());
        self
    }

    pub fn with_cause(mut self, cause: Error) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    // ─── Option-helper constructors (ADBC config parsing) ────────────
    //
    // The legacy ADBC config layer in `src/adbc/database.rs` uses
    // `set_unknown_option` / `set_invalid_option` / `option_as_int` /
    // `option_as_string` heavily. The kernel-internal modules (auth,
    // client, etc.) use them when validating their *own* string-keyed
    // option maps before the v0 typed config takes over in PR 2.

    /// Error for when a user requests an unknown option.
    pub fn get_unknown_option<K: std::fmt::Debug>(key: &K) -> Self {
        Self::not_found().format(format_args!("unknown option {key:?}"))
    }

    /// Error for when a user sets an unknown option.
    pub fn set_unknown_option<K: std::fmt::Debug>(key: &K) -> Self {
        Self::invalid_argument().format(format_args!("unknown option {key:?}"))
    }

    // ─── HTTP / wire mapping ────────────────────────────────────────

    /// Map an HTTP status code to the categorised constructor per
    /// the migration plan's mapping table. Returns an error with no
    /// message; caller chains `.message(...)` and `.with_http_status(s)`.
    ///
    /// Status codes outside the table fall back to `Internal` for
    /// 5xx (server-side bug or unexpected envelope) and
    /// `InvalidArgument` for 4xx (client-side bug).
    pub fn from_http_status(status: u16) -> Self {
        let err = match status {
            401 => Self::unauthenticated(),
            403 => Self::permission_denied(),
            404 => Self::not_found(),
            429 => Self::resource_exhausted(),
            503 => Self::unavailable(),
            504 => Self::timeout(),
            400..=499 => Self::invalid_argument(),
            500..=599 => Self::internal(),
            _ => Self::internal(),
        };
        err.with_http_status(status)
    }
}

// ─── std trait impls ────────────────────────────────────────────────

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: ", self.code)?;
        if self.message.is_empty() {
            f.write_str("(no message)")?;
        } else {
            f.write_str(&self.message)?;
        }
        if let Some(s) = &self.sql_state {
            write!(f, " [SQLSTATE {s}]")?;
        }
        if let Some(ec) = &self.error_code {
            write!(f, " [error_code {ec}]")?;
        }
        if let Some(c) = self.vendor_code {
            write!(f, " [vendor {c}]")?;
        }
        if let Some(loc) = &self.location {
            write!(f, " (at {loc})")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(err: arrow_schema::ArrowError) -> Self {
        // Arrow errors are mostly internal (parse / cast / IPC), with
        // a few that map to data-quality (`DataLoss`) or
        // invalid-argument categories.
        match err {
            arrow_schema::ArrowError::NotYetImplemented(msg) => {
                Self::not_implemented().format(format_args!("{msg}"))
            }
            arrow_schema::ArrowError::ExternalError(error) => {
                Self::internal().format(format_args!("{error}"))
            }
            arrow_schema::ArrowError::CastError(msg) => {
                Self::internal().format(format_args!("cast error: {msg}"))
            }
            arrow_schema::ArrowError::MemoryError(msg) => {
                Self::internal().format(format_args!("memory error: {msg}"))
            }
            arrow_schema::ArrowError::ParseError(msg) => {
                Self::internal().format(format_args!("parse error: {msg}"))
            }
            arrow_schema::ArrowError::SchemaError(msg) => {
                Self::internal().format(format_args!("schema error: {msg}"))
            }
            arrow_schema::ArrowError::ComputeError(msg) => {
                Self::internal().format(format_args!("compute error: {msg}"))
            }
            arrow_schema::ArrowError::DivideByZero => {
                Self::data_loss().format(format_args!("divide by zero"))
            }
            arrow_schema::ArrowError::ArithmeticOverflow(msg) => {
                Self::data_loss().format(format_args!("arithmetic overflow: {msg}"))
            }
            arrow_schema::ArrowError::CsvError(msg) => {
                Self::invalid_argument().format(format_args!("CSV error: {msg}"))
            }
            arrow_schema::ArrowError::JsonError(msg) => {
                Self::invalid_argument().format(format_args!("JSON error: {msg}"))
            }
            arrow_schema::ArrowError::IoError(msg, error) => {
                Self::io().format(format_args!("I/O error: {msg}: {error}"))
            }
            arrow_schema::ArrowError::IpcError(msg) => {
                Self::io().format(format_args!("IPC error: {msg}"))
            }
            arrow_schema::ArrowError::InvalidArgumentError(msg) => {
                Self::invalid_argument().message(msg)
            }
            arrow_schema::ArrowError::ParquetError(msg) => {
                Self::io().format(format_args!("Parquet error: {msg}"))
            }
            arrow_schema::ArrowError::CDataInterface(msg) => {
                Self::io().format(format_args!("C Data interface error: {msg}"))
            }
            arrow_schema::ArrowError::DictionaryKeyOverflowError => {
                Self::data_loss().message("dictionary key overflowed")
            }
            arrow_schema::ArrowError::RunEndIndexOverflowError => {
                Self::data_loss().message("run end index overflowed")
            }
            arrow_schema::ArrowError::OffsetOverflowError(size) => {
                Self::data_loss().format(format_args!("offset overflowed at size {size}"))
            }
            other => Self::internal().format(format_args!("{other}")),
        }
    }
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;

// ─── SEA extended-status carry helpers ──────────────────────────────

/// Copy the SEA extended status fields (`display_message`,
/// `diagnostic_info`, `error_details_json`) from a wire status onto a
/// kernel [`Error`].
///
/// Mirrors the Thrift `TGetOperationStatusResp` / `TStatus` contract
/// where the same fields ride on the operation status across
/// terminal-error states. Called by both the sync polling path
/// (`client::sea::SeaRestTransport::terminal_outcome`) and the async
/// poll-driver paths (`statement::executed_async::StatementStatus::
/// from_sea_response`'s `Failed` arm + `await_result`'s `Cancelled`
/// arm) so a Thrift-migrating wrapper sees the same field set on the
/// returned `Error` regardless of which kernel handle the caller is
/// driving.
///
/// `pub(crate)` — internal to the kernel; downstream consumers should
/// read the fields off the returned `Error` directly. The wire-shape
/// `types::sea::StatementStatus` is referenced by full path to avoid
/// pulling the dependency into the `kernel_error` module's public
/// imports.
pub(crate) fn carry_extended_status(
    mut error: Error,
    status: &crate::types::sea::StatementStatus,
) -> Error {
    if let Some(msg) = status.display_message.as_ref() {
        error = error.with_display_message(msg);
    }
    if let Some(info) = status.diagnostic_info.as_ref() {
        error = error.with_diagnostic_info(info);
    }
    if let Some(json) = status.error_details_json.as_ref() {
        error = error.with_error_details_json(json);
    }
    error
}

// ─── SQLSTATE extraction helpers ────────────────────────────────────

/// Extract the SQLSTATE from a SEA statement status.
///
/// Prefer the canonical `status.sql_state`, then parse
/// `status.error.message`. The nested `status.error.sql_state` field
/// belongs to the shared transport-error shape and is intentionally
/// ignored for statement failures.
pub(crate) fn extract_sqlstate_from_status(
    status: &crate::types::sea::StatementStatus,
) -> Option<String> {
    status
        .sql_state
        .as_ref()
        .filter(|s| s.len() == 5)
        .cloned()
        .or_else(|| {
            status
                .error
                .as_ref()
                .and_then(|error| error.message.as_deref())
                .and_then(extract_sqlstate_from_message)
        })
}

/// Extract a SQLSTATE from a free-form error message.
///
/// Many Databricks error envelopes include `SQLSTATE: XXXXX` in the
/// human-readable text in addition to (or instead of) a structured
/// field. This is the fallback when the structured field is absent.
pub fn extract_sqlstate_from_message(message: &str) -> Option<String> {
    let pat = "SQLSTATE:";
    let idx = message.find(pat)?;
    let after = message[idx + pat.len()..].trim_start();
    if after.len() < 5 {
        return None;
    }
    let candidate = &after[..5];
    if candidate.chars().all(|c| c.is_ascii_alphanumeric()) {
        Some(candidate.to_owned())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_carries_code_and_message() {
        let e = Error::new(ErrorCode::NotFound, "missing");
        assert_eq!(e.code, ErrorCode::NotFound);
        assert_eq!(e.message, "missing");
        assert!(e.sql_state.is_none());
        assert!(!e.retryable);
    }

    #[test]
    fn categorised_constructors_pick_correct_codes() {
        assert_eq!(Error::invalid_argument().code, ErrorCode::InvalidArgument);
        assert_eq!(Error::unauthenticated().code, ErrorCode::Unauthenticated);
        assert_eq!(Error::permission_denied().code, ErrorCode::PermissionDenied);
        assert_eq!(Error::not_found().code, ErrorCode::NotFound);
        assert_eq!(
            Error::resource_exhausted().code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(Error::unavailable().code, ErrorCode::Unavailable);
        assert_eq!(Error::timeout().code, ErrorCode::Timeout);
        assert_eq!(Error::cancelled().code, ErrorCode::Cancelled);
        assert_eq!(Error::data_loss().code, ErrorCode::DataLoss);
        assert_eq!(Error::internal().code, ErrorCode::Internal);
        assert_eq!(
            Error::invalid_statement_handle().code,
            ErrorCode::InvalidStatementHandle
        );
        assert_eq!(Error::network_error().code, ErrorCode::NetworkError);
        assert_eq!(Error::sql_error().code, ErrorCode::SqlError);
    }

    #[test]
    fn retryable_defaults_match_plan_table() {
        assert!(Error::resource_exhausted().retryable);
        assert!(Error::unavailable().retryable);
        assert!(Error::timeout().retryable);
        assert!(Error::network_error().retryable);
        assert!(!Error::invalid_argument().retryable);
        assert!(!Error::not_found().retryable);
    }

    #[test]
    fn message_appends_with_separator() {
        let e = Error::invalid_argument()
            .message("bad host")
            .message("must be https://");
        assert_eq!(e.message, "bad host; must be https://");
    }

    #[test]
    fn message_on_empty_does_not_prepend_separator() {
        let e = Error::invalid_argument().message("first");
        assert_eq!(e.message, "first");
    }

    #[test]
    fn context_wraps_existing_message() {
        let e = Error::io()
            .message("connection refused")
            .context("connect to server");
        assert_eq!(e.message, "could not connect to server: connection refused");
    }

    #[test]
    fn context_on_empty_just_says_could_not() {
        let e = Error::io().context("connect");
        assert_eq!(e.message, "could not connect");
    }

    #[test]
    fn format_appends_via_format_args() {
        let e = Error::invalid_argument().format(format_args!("count={}", 42));
        assert_eq!(e.message, "count=42");
    }

    #[test]
    fn from_http_status_picks_correct_codes() {
        assert_eq!(
            Error::from_http_status(401).code,
            ErrorCode::Unauthenticated
        );
        assert_eq!(
            Error::from_http_status(403).code,
            ErrorCode::PermissionDenied
        );
        assert_eq!(Error::from_http_status(404).code, ErrorCode::NotFound);
        assert_eq!(
            Error::from_http_status(429).code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(Error::from_http_status(503).code, ErrorCode::Unavailable);
        assert_eq!(Error::from_http_status(504).code, ErrorCode::Timeout);
        assert_eq!(
            Error::from_http_status(400).code,
            ErrorCode::InvalidArgument
        );
        assert_eq!(Error::from_http_status(500).code, ErrorCode::Internal);
    }

    #[test]
    fn from_http_status_attaches_status_field() {
        let e = Error::from_http_status(429);
        assert_eq!(e.http_status, Some(429));
        assert!(e.retryable);
    }

    #[test]
    fn extract_sqlstate_with_space() {
        let msg = "Error. SQLSTATE: 42601 (line 1)";
        assert_eq!(extract_sqlstate_from_message(msg), Some("42601".to_owned()));
    }

    #[test]
    fn extract_sqlstate_without_space() {
        let msg = "boom. SQLSTATE:42S02 more";
        assert_eq!(extract_sqlstate_from_message(msg), Some("42S02".to_owned()));
    }

    #[test]
    fn extract_sqlstate_absent_returns_none() {
        assert_eq!(extract_sqlstate_from_message("nope"), None);
    }

    #[test]
    fn extract_sqlstate_from_status_prefers_top_level_field() {
        let status: crate::types::sea::StatementStatus = serde_json::from_str(
            r#"{
                "state": "FAILED",
                "sql_state": "57KD0",
                "error": {
                    "message": "legacy SQLSTATE: 42000"
                }
            }"#,
        )
        .unwrap();

        assert_eq!(
            extract_sqlstate_from_status(&status).as_deref(),
            Some("57KD0")
        );
    }

    #[test]
    fn extract_sqlstate_from_status_falls_back_to_error_message() {
        let status: crate::types::sea::StatementStatus = serde_json::from_str(
            r#"{
                "state": "FAILED",
                "error": {
                    "message": "table not found. SQLSTATE: 42P01"
                }
            }"#,
        )
        .unwrap();

        assert_eq!(
            extract_sqlstate_from_status(&status).as_deref(),
            Some("42P01")
        );
    }

    #[test]
    fn extract_sqlstate_from_status_ignores_nested_field() {
        use crate::types::sea::{ServiceError, StatementState, StatementStatus};

        let status = StatementStatus {
            state: StatementState::Failed,
            error: Some(ServiceError {
                error_code: None,
                message: Some("table not found".to_string()),
                sql_state: Some("42P01".to_string()),
            }),
            sql_state: None,
            session_version: None,
            num_modified_rows: None,
            display_message: None,
            diagnostic_info: None,
            error_details_json: None,
        };

        assert_eq!(extract_sqlstate_from_status(&status), None);
    }

    #[test]
    fn display_includes_code_message_and_sqlstate() {
        let e = Error::sql_error()
            .message("syntax error near `FROM`")
            .with_sql_state("42601")
            .with_vendor_code(1234);
        let s = format!("{e}");
        assert!(s.contains("SqlError"));
        assert!(s.contains("syntax error"));
        assert!(s.contains("[SQLSTATE 42601]"));
        assert!(s.contains("[vendor 1234]"));
    }

    #[test]
    fn arrow_error_conversion_categorises() {
        let e: Error = arrow_schema::ArrowError::SchemaError("missing".into()).into();
        assert_eq!(e.code, ErrorCode::Internal);
        assert!(e.message.contains("missing"));

        let e: Error = arrow_schema::ArrowError::InvalidArgumentError("bad".into()).into();
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn with_error_code_populates_string_slug() {
        let e = Error::sql_error()
            .message("statement gone")
            .with_error_code("STATEMENT_NOT_FOUND");
        assert_eq!(e.error_code.as_deref(), Some("STATEMENT_NOT_FOUND"));
        // vendor_code is independent and stays unset
        assert!(e.vendor_code.is_none());
    }

    #[test]
    fn display_surfaces_error_code() {
        let e = Error::sql_error()
            .message("statement gone")
            .with_error_code("STATEMENT_NOT_FOUND")
            .with_sql_state("42P01");
        let s = format!("{e}");
        assert!(s.contains("[error_code STATEMENT_NOT_FOUND]"));
        assert!(s.contains("[SQLSTATE 42P01]"));
    }
}
