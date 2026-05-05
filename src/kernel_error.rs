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
//! ## What's `#[non_exhaustive]` and what isn't
//!
//! - `ErrorCode` **is** `#[non_exhaustive]`: v0 ships 13 variants and
//!   the addendum-A8 expansion (AlreadyExists, FailedPrecondition,
//!   Aborted, Unimplemented, …) lands in a follow-on PR. External
//!   `match` arms get a forced `_ =>` fallback — they keep compiling
//!   when the enum grows.
//! - `Error` **is not** `#[non_exhaustive]`: wrappers want to
//!   pattern-match (`let Error { code, message, sql_state, .. }`) for
//!   logging and host-language exception mapping. New fields go through
//!   `Error::new(...).with_*(...)`, so adding a field is non-breaking
//!   without the marker. Keeping the marker would block useful
//!   pattern-matches for no real benefit.

#![allow(dead_code)]

/// Discriminator for [`Error`].
///
/// `#[non_exhaustive]`: addendum A8 expands the variant set in a
/// follow-on PR (AlreadyExists, FailedPrecondition, Aborted,
/// Unimplemented, plus an `ErrorCategory` tri-state). Wrappers writing
/// `match` against `ErrorCode` must include a `_ =>` arm so growing the
/// enum stays non-breaking.
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
    /// Server returned HTTP 404 or `STATEMENT_NOT_FOUND`. Not retryable.
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
    /// `Statement` was re-executed (the validity flag flipped) or the
    /// handle was already closed. Distinct from `NotFound` because the
    /// handle once existed; the *server* still has its record. Local
    /// short-circuit; no wire call. Not retryable.
    InvalidStatementHandle,

    /// Transport-layer failure — DNS resolution, TCP handshake, TLS
    /// negotiation. Distinct from `Unavailable` (which is server-
    /// reported transient). Retryable with backoff.
    NetworkError,

    /// Server-reported SQL execution error. `sql_state` and
    /// `vendor_code` carry the structured detail per Databricks
    /// SQLSTATE conventions. Retryability depends on the SQLSTATE
    /// class — `40xxx` (transient) vs `42xxx` (syntactic / semantic);
    /// the addendum-A8 `ErrorCategory` will encode this.
    SqlError,
}

/// Typed kernel error.
///
/// `cause` chains preserve the underlying provenance — a `SqlError`
/// may wrap a `NetworkError` if the failure originated below the SEA
/// layer.
///
/// **Construction.** Build via [`Error::new`] and chain `with_*`
/// setters. **Pattern-matching is supported** — wrappers writing a
/// comprehensive logger or host-language exception adapter can do
/// `let Error { code, message, sql_state, .. } = err;`. New fields
/// land through additive setters on `Error::new`, not via struct
/// literals from external crates, so the type does not need
/// `#[non_exhaustive]` (and the marker would block the
/// pattern-matching wrappers actually want).
#[derive(Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,

    /// 5-character SQLSTATE if the server returned one.
    pub sql_state: Option<String>,

    /// Databricks vendor code if the server returned one.
    pub vendor_code: Option<i32>,

    /// HTTP status for transport-layer errors.
    pub http_status: Option<u16>,

    /// Whether the kernel considered this retryable. Wrappers may
    /// surface this in driver-specific retry logic.
    pub retryable: bool,

    /// Server-issued statement / query identifier, when known.
    pub query_id: Option<String>,

    /// Underlying cause. Boxed to keep the struct stack-friendly.
    pub cause: Option<Box<Error>>,
}

impl Error {
    /// Construct a fresh error with the given code and message. All
    /// other fields default to `None` / `false`.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            sql_state: None,
            vendor_code: None,
            http_status: None,
            retryable: false,
            query_id: None,
            cause: None,
        }
    }

    pub fn with_sql_state(mut self, sql_state: impl Into<String>) -> Self {
        self.sql_state = Some(sql_state.into());
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

    pub fn with_cause(mut self, cause: Error) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)?;
        if let Some(s) = &self.sql_state {
            write!(f, " [SQLSTATE {s}]")?;
        }
        if let Some(c) = self.vendor_code {
            write!(f, " [vendor {c}]")?;
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

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;
