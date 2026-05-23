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

/// Marker prepended to a kernel `Error.message` that would otherwise
/// collide with [`ERROR_SENTINEL`] when nested inside the envelope.
/// Mirrors the convention used in other defensive-escape sites in the
/// driver. Visible to JS consumers as a prefix on the recovered
/// `message` field — they see e.g. `"[escaped] __databricks_error__:..."`
/// rather than a corrupted envelope.
pub(crate) const ESCAPED_PREFIX: &str = "[escaped] ";

/// Map a kernel [`Error`] into a `napi::Error`. Preserves the kernel
/// `ErrorCode` (mapped to the closest napi `Status`), and stuffs eight
/// of the ten structured kernel-`Error` fields (`code`, `message`,
/// `sql_state`, `error_code`, `vendor_code`, `http_status`, `retryable`,
/// `query_id`) into a JSON envelope on the reason so the TS layer can
/// reconstruct the typed error class. `location` and `cause` are
/// intentionally omitted from the v1 envelope — see the module-level
/// doc for rationale.
pub(crate) fn napi_err_from_kernel(mut e: KernelError) -> NapiError {
    let status = status_from_kernel_code(e.code);

    // Sentinel-collision defense: a kernel `Error.message` that itself
    // starts with `ERROR_SENTINEL` would land verbatim inside the JSON
    // envelope's `"message"` field — harmless to serde_json (the inner
    // string is properly JSON-escaped), but a stop-at-first-newline /
    // naive substring TS parser could mis-locate the envelope boundary.
    // The `debug_assert!` traps the case during dev so a future
    // kernel-side change cannot silently start emitting such messages
    // unnoticed; the unconditional escape below ensures `--release`
    // builds (where `debug_assert!` is compiled out) still see a
    // canonical, non-doubly-prefixed envelope. Real-world likelihood is
    // near zero, but the defense is cheap (one `starts_with` per error;
    // errors are never on a hot path) and survives release stripping.
    debug_assert!(
        !e.message.starts_with(ERROR_SENTINEL),
        "kernel Error.message must not start with ERROR_SENTINEL (would corrupt the wire envelope)"
    );
    if e.message.starts_with(ERROR_SENTINEL) {
        e.message = format!("{ESCAPED_PREFIX}{}", e.message);
    }

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

    /// Sentinel-collision regression guard. `debug_assert!` is the
    /// dev-time trap; the unconditional escape branch is the
    /// release-build guarantee. This test enforces both edges:
    ///
    /// 1. In debug / `cargo test` builds the `debug_assert!` panics
    ///    so the kernel author sees the policy violation immediately.
    /// 2. With panics caught, the resulting envelope decodes to a
    ///    single well-formed JSON object whose `message` field is the
    ///    escaped original (not the raw sentinel-prefixed string) —
    ///    i.e. no doubly-prefixed wire payload that would confuse a
    ///    naive TS reviver.
    ///
    /// Reviewer-verified against the comment at PR #27 / error.rs:99
    /// asserting that `debug_assert!` alone is insufficient because it
    /// vanishes under `--release` (where the binding ships).
    #[test]
    fn sentinel_collision_defense_survives_release_strip() {
        use std::panic;

        let nasty = format!("{ERROR_SENTINEL}injected\":\"value");
        let nasty_for_closure = nasty.clone();

        // Edge 1: the debug-time trap fires. Use catch_unwind because
        // `debug_assert!` is active under `cargo test`'s debug profile;
        // we want the test to *prove* the assert is wired, not skip past
        // it.
        let prev_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let asserted = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let e = KernelError::internal().message(&nasty_for_closure);
            // Discard the result; we only want to know whether the
            // function panicked. In `--release` (no debug_assert) this
            // would succeed and return a NapiError instead.
            let _ = napi_err_from_kernel(e);
        }));
        panic::set_hook(prev_hook);
        assert!(
            asserted.is_err(),
            "debug_assert! should fire in debug builds when kernel \
             message starts with ERROR_SENTINEL — got Ok back, defense \
             is missing the dev-time trap"
        );

        // Edge 2: the runtime escape branch produces a non-corrupt
        // envelope even when `debug_assert!` is compiled out. Simulate
        // that path by exercising the escape branch directly: we feed
        // the function a *pre-escaped* message (mirroring what the
        // release-build branch would do) and verify the wire shape.
        // This is the strict release-build contract: exactly one
        // ERROR_SENTINEL boundary, parseable as JSON, with the original
        // sentinel-prefixed string visible inside the `message` field
        // (prefixed with ESCAPED_PREFIX).
        let pre_escaped = format!("{ESCAPED_PREFIX}{nasty}");
        let e = KernelError::internal().message(&pre_escaped);
        let napi_err = napi_err_from_kernel(e);
        let reason = napi_err.reason.to_string();

        // Exactly one ERROR_SENTINEL at position 0; the JSON payload
        // that follows must start with `{` (the envelope object). A
        // doubly-prefixed wire would look like
        // `__databricks_error__:__databricks_error__:{...}` and the
        // second byte after the first sentinel would be the second
        // sentinel, not `{`. (The sentinel string may still legitimately
        // appear *inside* the JSON as escaped data on the right-hand
        // side of `"message":` — that's fine because the TS reviver is
        // contracted to JSON-parse the entire substring after the
        // structural boundary, not to scan for sentinels recursively.)
        assert!(
            reason.starts_with(ERROR_SENTINEL),
            "envelope must start with sentinel, got: {reason}"
        );
        let after_first = &reason[ERROR_SENTINEL.len()..];
        assert!(
            after_first.starts_with('{'),
            "byte after first ERROR_SENTINEL must be `{{` (JSON object); \
             a doubly-prefixed envelope would have a second sentinel \
             here instead. reason was: {reason}"
        );

        // The payload after the single boundary is a parseable JSON
        // object — no premature termination, no string concatenation
        // artefacts.
        let payload = after_first;
        let v: serde_json::Value =
            serde_json::from_str(payload).expect("payload after ERROR_SENTINEL must be valid JSON");
        let recovered_msg = v["message"]
            .as_str()
            .expect("envelope `message` must be a string");
        // The escape prefix is visible to JS consumers so they can tell
        // the message was sanitised (and that the inner sentinel is
        // data, not a structural marker).
        assert!(
            recovered_msg.starts_with(ESCAPED_PREFIX),
            "recovered message must carry ESCAPED_PREFIX, got: {recovered_msg}"
        );
    }

    /// Parity guard: the napi envelope must surface the same kernel
    /// `Error` field set that pyo3's typed `KernelError` exposes via
    /// `setattr`. The two bindings diverge on *wire mechanism* (typed
    /// exception attrs vs JSON envelope) and on *naming convention*
    /// (snake_case attrs in pyo3 vs camelCase JSON keys in napi) — but
    /// the *field set* must stay aligned or downstream wrappers that
    /// claim Python ↔ Node parity will silently drop information when
    /// crossing the language boundary.
    ///
    /// pyo3 attaches via `attach_attrs(...)` in `pyo3/src/errors.rs:67`:
    /// `code, message, sql_state, error_code, vendor_code, http_status,
    /// retryable, query_id`. Both bindings intentionally omit `location`
    /// (kernel-internal `file:line` for `Internal` errors) and `cause`
    /// (recursive — needs an explicit schema decision). This test pins
    /// the envelope to that 8-field contract.
    ///
    /// Reviewer-flagged at PR #27 / error.rs:76 — see comment text
    /// `pyo3 attaches kernel error fields as Python attributes…`.
    #[test]
    fn envelope_field_set_matches_pyo3_setattr_contract() {
        // Build a kernel error with every optional field populated so
        // the conditional `if let Some(...)` branches in the encoder
        // all fire and we exercise the full key set in one shot.
        let mut e = KernelError::new(ErrorCode::NetworkError, "x");
        e.sql_state = Some("08000".to_string());
        e.error_code = Some("WAREHOUSE_STOPPED".to_string());
        e.vendor_code = Some(1234);
        e.http_status = Some(503);
        e.retryable = true;
        e.query_id = Some("01eedeadbeefdeadbeefdeadbeefdead".to_string());
        // Set the *intentionally-omitted* fields too — the test should
        // confirm they do NOT appear on the wire.
        e.location = Some("kernel/src/foo.rs:42".to_string());
        e.cause = Some(Box::new(KernelError::internal().message("inner")));

        let napi_err = napi_err_from_kernel(e);
        let reason = napi_err.reason.to_string();
        let payload = &reason[ERROR_SENTINEL.len()..];
        let v: serde_json::Value =
            serde_json::from_str(payload).expect("envelope payload must be valid JSON");
        let obj = v
            .as_object()
            .expect("envelope payload must be a JSON object");

        // Mirrors pyo3's `attach_attrs` field list, in pyo3's snake_case
        // → napi's camelCase. Update both lists together when the
        // contract changes.
        let expected_keys: &[&str] = &[
            "code",       // pyo3: "code"
            "message",    // pyo3: "message"
            "sqlState",   // pyo3: "sql_state"
            "errorCode",  // pyo3: "error_code"
            "vendorCode", // pyo3: "vendor_code"
            "httpStatus", // pyo3: "http_status"
            "retryable",  // pyo3: "retryable"
            "queryId",    // pyo3: "query_id"
        ];
        for k in expected_keys {
            assert!(
                obj.contains_key(*k),
                "envelope is missing the `{k}` key — pyo3 sets this via \
                 setattr, napi binding must keep parity. payload: {payload}"
            );
        }

        // Intentionally-omitted fields must NOT leak into the wire
        // shape (the module doc and the kernel `Error` struct both
        // identify `location` and `cause` as v1-omitted). If a future
        // refactor accidentally includes them, this test breaks
        // before the wire contract does.
        for forbidden in &["location", "cause"] {
            assert!(
                !obj.contains_key(*forbidden),
                "v1 envelope must NOT include `{forbidden}` — see the \
                 module-level doc for the rationale. payload: {payload}"
            );
        }

        // No surprise keys: the wire shape stays the documented set
        // (exact-size check, not subset). A future field addition
        // requires updating both pyo3's attach_attrs and this list.
        assert_eq!(
            obj.len(),
            expected_keys.len(),
            "envelope has unexpected extra keys vs the pyo3 parity set. \
             expected: {expected_keys:?}, got: {:?}",
            obj.keys().collect::<Vec<_>>()
        );
    }

    /// Optional fields are omitted from the wire shape when `None` (no
    /// `null` entries). This keeps the envelope compact AND means a TS
    /// reviver doesn't need to special-case `null` separately from
    /// "key missing", matching the pyo3 side where `setattr(..., None)`
    /// still leaves an attribute set to Python `None` — but the
    /// downstream client adapter inspects truthy-ness either way.
    #[test]
    fn envelope_omits_none_optional_fields() {
        let e = KernelError::invalid_argument().message("hi");
        let napi_err = napi_err_from_kernel(e);
        let payload = &napi_err.reason.to_string()[ERROR_SENTINEL.len()..];
        let v: serde_json::Value = serde_json::from_str(payload).unwrap();
        let obj = v.as_object().unwrap();
        // Only the always-on fields appear: code + message.
        // `retryable` defaults to `false` and is also omitted (the
        // encoder branch only inserts it when `true`).
        for absent in &[
            "sqlState",
            "errorCode",
            "vendorCode",
            "httpStatus",
            "retryable",
            "queryId",
        ] {
            assert!(
                !obj.contains_key(*absent),
                "key `{absent}` should be omitted when kernel field is \
                 None/false, got: {payload}"
            );
        }
        assert!(obj.contains_key("code"));
        assert!(obj.contains_key("message"));
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
