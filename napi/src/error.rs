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

//! Kernel-error → `napi::Error` mapping.
//!
//! The kernel returns a richly-typed [`Error`](databricks_sql_kernel::Error)
//! with `code`, `message`, `sql_state`, `error_code`, `vendor_code`,
//! `http_status`, `retryable`, `query_id`, `location`, and `cause`
//! fields. The napi `Error` type only carries `status` + `reason`
//! directly — to attach the extra fields as own-properties on the JS
//! error object we'd need an `Env` reference, which `#[napi] async fn`
//! bodies don't have access to cheaply.
//!
//! Compromise (one helper, DRY): encode a subset of the kernel's
//! structured metadata into the `reason` field as a JSON envelope
//! prefixed with a sentinel `__databricks_error__:` token. The TS
//! adapter detects the sentinel, parses the payload, and reconstructs
//! the typed error class (`DBSQLError`, `AuthError`, …). Plain-string
//! errors from the binding's own code paths fall through the sentinel
//! detection unchanged.
//!
//! ## What's NOT in the envelope today
//!
//! The current v1 envelope schema intentionally omits two kernel
//! `Error` fields:
//!
//! - `location` — `Option<String>` describing the kernel source location
//!   of the error (mostly useful for `ErrorCode::Internal` kernel bugs).
//!   Omitted to keep the wire shape stable for JS consumers; adding it
//!   requires a coordinated TS-side reviver update.
//! - `cause` — recursive `Option<Box<Error>>` carrying the source
//!   error chain. Omitted because recursive serialization needs an
//!   intentional schema decision (max depth? cycle detection?) and the
//!   TS reviver would need matching logic.
//!
//! Both are tracked as follow-up envelope extensions. The kernel's
//! own `message` field already includes the upstream cause's `Display`
//! output via `.context()`, so today's wire shape isn't useless —
//! callers see the message chain, just not the structured cause graph
//! or the kernel-internal `location`.
//!
//! Round 3 may switch to the `Env::create_error` + own-properties
//! pattern once we have a stable point in each entry where `env: Env`
//! is available (likely by wrapping the async glue in a sync entry
//! point that calls `tokio::spawn` after capturing `env`).

use databricks_sql_kernel::{Error as KernelError, ErrorCode};
use napi::{Error as NapiError, Status};
use serde_json::{json, Map, Value};

/// Sentinel that tells the TS adapter the `reason` string is a JSON
/// envelope rather than a plain message. Has to be ASCII-only so it
/// survives any `String` round-trip the napi layer might do.
///
/// **Invariant:** ONLY [`napi_err_from_kernel`] is allowed to write a
/// `NapiError` whose `reason` begins with this string. Any other code
/// path constructing a `napi::Error` (e.g. `util::guarded`'s panic
/// case, or future plain-string failures from the binding's own glue)
/// MUST ensure its message does not start with `ERROR_SENTINEL` —
/// otherwise the TS-side reviver mis-detects a plain message as a
/// structured envelope and either crashes or fabricates fields.
/// [`napi_err_from_kernel`] itself enforces this with a `debug_assert!`
/// to catch accidental kernel `Error::message` payloads that begin
/// with the sentinel (an unlikely but possible source of confusion).
pub(crate) const ERROR_SENTINEL: &str = "__databricks_error__:";

/// Map a kernel [`Error`] into a `napi::Error`. Preserves the kernel
/// `ErrorCode` (mapped to the closest napi `Status`), and stuffs eight
/// of the ten structured kernel-`Error` fields (`code`, `message`,
/// `sql_state`, `error_code`, `vendor_code`, `http_status`, `retryable`,
/// `query_id`) into a JSON envelope on the reason so the TS layer can
/// reconstruct the typed error class. `location` and `cause` are
/// intentionally omitted from the v1 envelope — see the module-level
/// doc for rationale.
pub(crate) fn napi_err_from_kernel(e: KernelError) -> NapiError {
    let status = status_from_kernel_code(e.code);

    // Sentinel-collision defense: a kernel `Error.message` that itself
    // starts with `ERROR_SENTINEL` would, when nested inside the JSON
    // envelope, look indistinguishable from a structured envelope to a
    // naive consumer. Today the only writer of strings starting with
    // `ERROR_SENTINEL` is this function, and kernel messages don't
    // contain the sentinel. The `debug_assert!` traps the case during
    // dev so a future kernel-side change can't silently break the
    // invariant.
    debug_assert!(
        !e.message.starts_with(ERROR_SENTINEL),
        "kernel Error.message must not start with ERROR_SENTINEL (would corrupt the wire envelope)"
    );

    // Build the envelope as a `serde_json::Map` so the TS contract is
    // single-sourced in one place (the key set below) and the value
    // escaping is delegated to serde_json. Optional fields are only
    // inserted when present so the wire shape stays compact and the
    // TS-side `reviver` doesn't have to special-case nulls.
    let mut obj = Map::new();
    obj.insert("code".to_owned(), Value::from(error_code_str(e.code)));
    obj.insert("message".to_owned(), Value::from(e.message));
    if let Some(s) = e.sql_state {
        obj.insert("sqlState".to_owned(), Value::from(s));
    }
    if let Some(ec) = e.error_code {
        obj.insert("errorCode".to_owned(), Value::from(ec));
    }
    if let Some(vc) = e.vendor_code {
        obj.insert("vendorCode".to_owned(), Value::from(vc));
    }
    if let Some(hs) = e.http_status {
        obj.insert("httpStatus".to_owned(), Value::from(hs));
    }
    if e.retryable {
        obj.insert("retryable".to_owned(), Value::from(true));
    }
    if let Some(qid) = e.query_id {
        obj.insert("queryId".to_owned(), Value::from(qid));
    }

    // `to_string` on a JSON object is infallible: every field above is
    // a `String` / `i64` / `u16` / `bool` whose serialization can't fail.
    let payload = json!(obj).to_string();
    let mut envelope = String::with_capacity(ERROR_SENTINEL.len() + payload.len());
    envelope.push_str(ERROR_SENTINEL);
    envelope.push_str(&payload);

    NapiError::new(status, envelope)
}

/// Map kernel `ErrorCode` → napi `Status`. The status is mostly
/// cosmetic on the napi side (the TS layer dispatches on `code` from
/// the envelope); we pick the closest match so unwrapped errors still
/// look reasonable in raw napi consumers.
fn status_from_kernel_code(code: ErrorCode) -> Status {
    match code {
        ErrorCode::InvalidArgument | ErrorCode::InvalidStatementHandle => Status::InvalidArg,
        ErrorCode::Cancelled => Status::Cancelled,
        _ => Status::GenericFailure,
    }
}

/// String tag for each kernel `ErrorCode` — stable across kernel
/// versions because v0's `ErrorCode` is `#[non_exhaustive]` and we
/// pattern-match exhaustively against the known set.
fn error_code_str(code: ErrorCode) -> &'static str {
    match code {
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
        // Forward-compat: ErrorCode is `#[non_exhaustive]`. Any new
        // variant the kernel adds in v0.x lands here until we mirror
        // it in this match. The TS layer treats Unknown as a generic
        // failure.
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_carries_sentinel_and_code() {
        let e = KernelError::invalid_argument().message("hi");
        let napi_err = napi_err_from_kernel(e);
        let reason = napi_err.reason.to_string();
        assert!(reason.starts_with(ERROR_SENTINEL));
        let payload = &reason[ERROR_SENTINEL.len()..];
        let v: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(v["code"], "InvalidArgument");
        assert_eq!(v["message"], "hi");
    }

    #[test]
    fn message_escaping_round_trips() {
        let nasty = "quote\"backslash\\newline\nctrl\u{1}end";
        let e = KernelError::internal().message(nasty);
        let napi_err = napi_err_from_kernel(e);
        let reason = napi_err.reason.to_string();
        let payload = &reason[ERROR_SENTINEL.len()..];
        let v: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(v["message"], nasty);
    }

    /// All known kernel `ErrorCode` variants map to a non-"Unknown"
    /// slug. Regression guard for the `_ => "Unknown"` fallback: if
    /// a future kernel addition adds a variant and we forget to
    /// extend the match here, this test still passes against the old
    /// variant set but the new variant silently slugs as "Unknown".
    /// That degraded behavior is what we want to catch — when this
    /// test is updated for a new variant, the slug change is auditable.
    #[test]
    fn error_code_str_covers_all_known_variants() {
        // Listing all 13 known v0 variants explicitly: this is the
        // single source of truth for the wire-format `code` slug.
        let cases: &[(ErrorCode, &str)] = &[
            (ErrorCode::InvalidArgument, "InvalidArgument"),
            (ErrorCode::Unauthenticated, "Unauthenticated"),
            (ErrorCode::PermissionDenied, "PermissionDenied"),
            (ErrorCode::NotFound, "NotFound"),
            (ErrorCode::ResourceExhausted, "ResourceExhausted"),
            (ErrorCode::Unavailable, "Unavailable"),
            (ErrorCode::Timeout, "Timeout"),
            (ErrorCode::Cancelled, "Cancelled"),
            (ErrorCode::DataLoss, "DataLoss"),
            (ErrorCode::Internal, "Internal"),
            (ErrorCode::InvalidStatementHandle, "InvalidStatementHandle"),
            (ErrorCode::NetworkError, "NetworkError"),
            (ErrorCode::SqlError, "SqlError"),
        ];
        for (code, expected_slug) in cases {
            let slug = error_code_str(*code);
            assert_eq!(
                slug, *expected_slug,
                "ErrorCode::{:?} should slug to {:?}, got {:?}",
                code, expected_slug, slug
            );
            assert_ne!(
                slug, "Unknown",
                "ErrorCode::{:?} fell through to the Unknown fallback",
                code
            );
        }
    }

    #[test]
    fn status_from_kernel_code_maps_special_cases() {
        // Status mapping is mostly cosmetic but the 3 distinguished
        // cases (InvalidArg, Cancelled, GenericFailure) are the contract.
        assert_eq!(
            status_from_kernel_code(ErrorCode::InvalidArgument),
            Status::InvalidArg
        );
        assert_eq!(
            status_from_kernel_code(ErrorCode::InvalidStatementHandle),
            Status::InvalidArg
        );
        assert_eq!(
            status_from_kernel_code(ErrorCode::Cancelled),
            Status::Cancelled
        );
        assert_eq!(
            status_from_kernel_code(ErrorCode::Internal),
            Status::GenericFailure
        );
        assert_eq!(
            status_from_kernel_code(ErrorCode::NetworkError),
            Status::GenericFailure
        );
    }
}
