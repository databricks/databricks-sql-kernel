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
/// `ErrorCode` (mapped to the closest napi `Status`), and stuffs the
/// structured kernel-`Error` fields into a JSON envelope on the reason
/// so the TS layer can reconstruct the typed error class. `location`
/// and `cause` are intentionally omitted from the v1 envelope — see
/// the module-level doc for rationale.
///
/// Both bindings carry extended status; NAPI encodes camelCase JSON and PyO3
/// attaches snake_case exception attributes.
///
/// The whole JSON-envelope-on-`reason` scheme is interim. The follow-up
/// PR that lands the first `#[napi]` entry points (PR #25) is expected
/// to replace it with typed errors via `Env::create_error` +
/// `set_named_property` (mirroring pyo3's `setattr` approach in
/// `pyo3/src/errors.rs`). The sentinel + envelope vanish once that
/// lands. See `sea-workflow/jira-candidates/2026-05-23-napi-typed-error-refactor.md`
/// for the deferred-spec write-up.
pub(crate) fn napi_err_from_kernel(e: KernelError) -> NapiError {
    let status = status_from_kernel_code(e.code);

    // Sentinel-collision defense (dev-time only): a kernel `Error.message`
    // that itself starts with `ERROR_SENTINEL` would land verbatim inside
    // the JSON envelope's `"message"` field. serde_json escapes it
    // correctly so a JSON-parsing TS reviver still recovers the right
    // payload; the practical hazard is a naive `indexOf(SENTINEL)`-based
    // reviver mis-locating the boundary. Today the kernel never emits
    // such a message; this `debug_assert!` catches any future kernel-side
    // change that would start to. We deliberately do NOT add a
    // release-build runtime escape — the right fix is to delete the
    // envelope scheme entirely (see typed-error refactor deferred-spec)
    // rather than harden the workaround.
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
    if let Some(local) = e.local_error {
        if let Some(errno) = local.raw_os_error {
            obj.insert("localOsError".to_owned(), Value::from(errno));
        }
        obj.insert(
            "localErrorKind".to_owned(),
            Value::from(format!("{:?}", local.kind)),
        );
        obj.insert("localPath".to_owned(), Value::from(local.path));
    }
    // Thrift-parity extended status fields: surfaced on the napi
    // envelope so a JS caller's catch-block sees the same fields the
    // Thrift driver's `TGetOperationStatusResp` exposes. Only
    // populated on terminal-error states (`FAILED` / `CANCELED` /
    // `CLOSED-no-data`) — the kernel's
    // `terminal_outcome::carry_extended_status` is the upstream.
    if let Some(dm) = e.display_message {
        obj.insert("displayMessage".to_owned(), Value::from(dm));
    }
    if let Some(di) = e.diagnostic_info {
        obj.insert("diagnosticInfo".to_owned(), Value::from(di));
    }
    if let Some(ej) = e.error_details_json {
        obj.insert("errorDetailsJson".to_owned(), Value::from(ej));
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
    // Delegate to the kernel's single-source-of-truth slug map so the napi and
    // pyo3 wire slugs can never drift. `ErrorCode` is `#[non_exhaustive]`, but
    // `slug()` is defined in the kernel crate (exhaustive there), so any future
    // variant is covered automatically without a wildcard here.
    code.slug()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_error_metadata_is_optional_and_preserved() {
        let mut error = KernelError::not_found().message("missing local file");
        error.local_error = Some(Box::new(databricks_sql_kernel::LocalFileError {
            raw_os_error: Some(2),
            kind: std::io::ErrorKind::NotFound,
            path: "local-file".into(),
        }));
        let encoded = napi_err_from_kernel(error);
        let value: Value = serde_json::from_str(&encoded.reason[ERROR_SENTINEL.len()..]).unwrap();
        assert_eq!(value["localOsError"], 2);
        assert_eq!(value["localErrorKind"], "NotFound");
        assert_eq!(value["localPath"], "local-file");
        let encoded = napi_err_from_kernel(KernelError::internal());
        let value: Value = serde_json::from_str(&encoded.reason[ERROR_SENTINEL.len()..]).unwrap();
        assert!(value.get("localErrorKind").is_none());
    }

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

    /// Dev-time invariant: a kernel `Error.message` that starts with
    /// `ERROR_SENTINEL` is treated as a policy violation and fires the
    /// `debug_assert!` in `napi_err_from_kernel`. The kernel never emits
    /// such a message today; this test catches any future kernel-side
    /// change that would start to. `cargo test` runs under the debug
    /// profile, so the assertion is active; we use `catch_unwind` to
    /// observe the panic without aborting the test process.
    ///
    /// This is **dev-time only on purpose**. Release builds compile
    /// `debug_assert!` out, so a sentinel-prefixed kernel message would
    /// be embedded inside the JSON envelope's `"message"` field
    /// verbatim — well-formed JSON (serde_json escapes it correctly),
    /// safe for a JSON-parsing TS reviver. The only consumers that
    /// would mis-parse are naive `indexOf(SENTINEL)`-style revivers,
    /// which we explicitly don't support. The deeper fix is to delete
    /// the envelope scheme entirely in favour of typed errors via
    /// `Env::create_error` + `set_named_property`. See
    /// `sea-workflow/jira-candidates/2026-05-23-napi-typed-error-refactor.md`.
    #[test]
    fn sentinel_prefix_triggers_debug_assert() {
        use std::panic;

        let nasty = format!("{ERROR_SENTINEL}injected");
        let prev_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let asserted = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let e = KernelError::internal().message(&nasty);
            let _ = napi_err_from_kernel(e);
        }));
        panic::set_hook(prev_hook);
        assert!(
            asserted.is_err(),
            "debug_assert! should fire in debug builds when kernel \
             message starts with ERROR_SENTINEL — got Ok back, the \
             dev-time trap is missing"
        );
    }

    /// Both bindings expose structured kernel fields; NAPI uses camelCase JSON
    /// and PyO3 uses snake_case exception attributes.
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
        // Thrift-parity extension fields: populated too so the
        // superset relation is exercised in the same fixture.
        e.display_message = Some("Statement failed at operator Scan".to_string());
        e.diagnostic_info = Some("operator=Scan; stage=Compile".to_string());
        e.error_details_json = Some(r#"{"errorClass":"COMPILE_FAILURE"}"#.to_string());
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

        // pyo3's `attach_attrs` field list, in pyo3's snake_case →
        // napi's camelCase. These 8 keys must stay in lockstep across
        // bindings — any divergence on these is a parity regression.
        let pyo3_parity_keys: &[&str] = &[
            "code",       // pyo3: "code"
            "message",    // pyo3: "message"
            "sqlState",   // pyo3: "sql_state"
            "errorCode",  // pyo3: "error_code"
            "vendorCode", // pyo3: "vendor_code"
            "httpStatus", // pyo3: "http_status"
            "retryable",  // pyo3: "retryable"
            "queryId",    // pyo3: "query_id"
        ];
        for k in pyo3_parity_keys {
            assert!(
                obj.contains_key(*k),
                "envelope is missing the `{k}` key — pyo3 sets this via \
                 setattr, napi binding must keep parity. payload: {payload}"
            );
        }

        // Thrift-parity superset: these 3 are napi-only today (pyo3
        // may add them later via attach_attrs). They mirror the
        // same-named optional fields on the NodeJS Thrift
        // `TGetOperationStatusResp`.
        let thrift_parity_keys: &[&str] = &[
            "displayMessage",   // Thrift: displayMessage
            "diagnosticInfo",   // Thrift: diagnosticInfo
            "errorDetailsJson", // Thrift: errorDetailsJson
        ];
        for k in thrift_parity_keys {
            assert!(
                obj.contains_key(*k),
                "envelope is missing the Thrift-parity key `{k}`. payload: {payload}"
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

        // No surprise keys: the wire shape stays the documented set.
        // Exact-size check (not subset) catches accidental additions.
        let expected_total = pyo3_parity_keys.len() + thrift_parity_keys.len();
        assert_eq!(
            obj.len(),
            expected_total,
            "envelope has unexpected extra keys. expected pyo3 parity \
             {pyo3_parity_keys:?} + thrift parity {thrift_parity_keys:?}, \
             got: {:?}",
            obj.keys().collect::<Vec<_>>()
        );
    }

    /// Thrift parity: on a non-terminal-error code (where the kernel
    /// doesn't populate the SEA extended fields) the three Thrift
    /// extension keys are omitted from the envelope. Mirrors the
    /// optional-field-omitted-when-None convention the existing
    /// envelope already enforces for the pyo3-parity fields.
    #[test]
    fn envelope_omits_thrift_extension_fields_when_unpopulated() {
        let e = KernelError::invalid_argument().message("bad arg");
        let napi_err = napi_err_from_kernel(e);
        let payload = &napi_err.reason.to_string()[ERROR_SENTINEL.len()..];
        let v: serde_json::Value = serde_json::from_str(payload).unwrap();
        let obj = v.as_object().unwrap();
        for absent in &["displayMessage", "diagnosticInfo", "errorDetailsJson"] {
            assert!(
                !obj.contains_key(*absent),
                "key `{absent}` should be omitted when kernel field is \
                 None, got: {payload}"
            );
        }
    }

    /// Wire round-trip for the Thrift parity fields: when the kernel
    /// `Error` carries the 3 extended-status strings, the envelope
    /// surfaces them under their camelCase keys with verbatim content.
    /// Pins the exact key names so the TS adapter's typed-error
    /// reviver can rely on them.
    #[test]
    fn envelope_surfaces_thrift_extension_fields_verbatim() {
        let dm = "Statement was canceled by user; runtime 42s";
        let di = "operator=Scan->Filter->Project; stage=Execute; thread=0xfe";
        let ej = r#"{"errorClass":"USER_CANCELLATION","detail":{"reason":"sigint"}}"#;
        let e = KernelError::sql_error()
            .message("canceled")
            .with_display_message(dm)
            .with_diagnostic_info(di)
            .with_error_details_json(ej);
        let napi_err = napi_err_from_kernel(e);
        let payload = &napi_err.reason.to_string()[ERROR_SENTINEL.len()..];
        let v: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(v["displayMessage"], dm);
        assert_eq!(v["diagnosticInfo"], di);
        assert_eq!(v["errorDetailsJson"], ej);
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
            // Thrift-parity extension fields also follow the
            // omit-when-None convention.
            "displayMessage",
            "diagnosticInfo",
            "errorDetailsJson",
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
