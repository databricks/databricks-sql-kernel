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

//! `tracing`→`log` bridge contract for kernel log events.
//!
//! The kernel is built with `tracing`'s `log` feature, so — **when no
//! `tracing` `Subscriber` is installed** — every `tracing::*` event also
//! emits a `log::Record`. The pyo3 binding relies on exactly this: it
//! installs no subscriber and routes those `log` records into Python's
//! `logging` via `pyo3_log`, deriving the logger name from the record
//! `target` (`::` → `.`). These tests pin that the kernel's log target is
//! the contract string `pyo3_log` turns into the `databricks.sql.kernel`
//! Python logger, and that structured fields survive the bridge.
//!
//! ## Why this is a separate integration-test binary
//!
//! The assertions depend on the `log` bridge being live, which requires
//! that **no `tracing` subscriber is installed in this process**. The
//! kernel's own unit-test binary can no longer satisfy that: any
//! `Session::open` (and several unit tests open sessions against a mock)
//! installs a process-global subscriber via `logging::init_logging`
//! (issue #213 wired that in), which suppresses the `log` bridge. A
//! dedicated integration-test binary that opens no session keeps the
//! bridge intact, so these are hosted here rather than in `klog::tests`.
//!
//! The kernel's internal `klog::{debug,warn}` macros are `pub(crate)` and
//! unreachable from an integration test, but they are thin wrappers that
//! emit `tracing::debug!(target: KERNEL_LOG_TARGET, …)`. Emitting on that
//! public target directly is behaviourally identical for the bridge, so
//! these tests exercise the exact same contract.

use std::sync::{Mutex, OnceLock};

use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, HttpConfig, LoggingConfig, Session, KERNEL_LOG_TARGET,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Captured `(target, rendered_message)` pairs. The message is what
/// `tracing`'s `log` feature renders — including any structured fields
/// appended as `key="value"` — so we can assert field propagation, not
/// just the target.
static CAPTURED: OnceLock<Mutex<Vec<(String, String)>>> = OnceLock::new();

struct Capture;
impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        CAPTURED
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push((record.target().to_owned(), record.args().to_string()));
    }
    fn flush(&self) {}
}

/// Install the single process-global `log` logger for this binary, once.
/// `log` allows only one global logger, so both tests share this capture
/// sink (and both read from `CAPTURED`, which accumulates across them).
fn install_capture() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        CAPTURED.get_or_init(|| Mutex::new(Vec::new()));
        // Ignore error: tolerate a logger already installed by a sibling
        // test in this binary — there is exactly one capture sink.
        let _ = log::set_boxed_logger(Box::new(Capture));
        log::set_max_level(log::LevelFilter::Trace);
    });
}

/// The kernel's log events emit a `log::Record` whose target is exactly
/// the contract string — this is the value `pyo3_log` turns into the
/// `databricks.sql.kernel` Python logger. If a future change routes kernel
/// logs through a different target, this fails loudly.
#[test]
fn klog_emits_contract_target() {
    install_capture();

    // Equivalent to the crate-internal `klog::{warn,debug}!` macros, which
    // wrap `tracing::{warn,debug}!(target: KERNEL_LOG_TARGET, …)`.
    tracing::warn!(target: KERNEL_LOG_TARGET, "probe message");
    tracing::debug!(target: KERNEL_LOG_TARGET, field = 1, "probe with field");

    let got = CAPTURED.get().unwrap().lock().unwrap().clone();
    assert!(
        got.iter().any(|(t, _)| t == KERNEL_LOG_TARGET),
        "expected a record with target {KERNEL_LOG_TARGET:?}, got {got:?}"
    );
    assert_eq!(KERNEL_LOG_TARGET, "databricks::sql::kernel");
}

/// A kernel log call with a structured field (e.g. `statement_id = …`)
/// must carry that field through `tracing`'s `log` feature into the
/// `log::Record` — this is what lets a customer correlate interleaved
/// kernel logs to a statement/session in a multi-session deployment.
///
/// The `tracing`→`log` bridge does NOT propagate enclosing *span* fields,
/// only the event's own fields, so reader/factory call sites pass
/// `statement_id` as an explicit event field rather than relying on the
/// session span. This test pins that mechanism: if the bridge behaviour
/// changed (fields stop rendering) or a refactor dropped the field, the
/// rendered message would no longer contain it.
///
/// Shares the single global logger with `klog_emits_contract_target` —
/// both read from `CAPTURED`, which accumulates across tests in this
/// binary — so this asserts on the presence of *its own* distinctive
/// record rather than on counts.
#[test]
fn klog_propagates_correlation_field() {
    install_capture();

    let stmt = "stmt-correlation-probe-7f3a";
    tracing::debug!(target: KERNEL_LOG_TARGET, statement_id = stmt, "creating reader: {} bytes", 42);

    let got = CAPTURED.get().unwrap().lock().unwrap().clone();
    // The field renders into the message as `statement_id="…"`.
    assert!(
        got.iter().any(|(target, msg)| target == KERNEL_LOG_TARGET
            && msg.contains(stmt)
            && msg.contains("statement_id")),
        "expected a kernel-target record whose rendered message carries \
         the `statement_id` field {stmt:?}; got {got:?}"
    );
}

/// GATE REGRESSION (issue #213 follow-up): opening a session with an
/// UNCONFIGURED `LoggingConfig` must NOT install a `tracing` subscriber,
/// so the `tracing`→`log` bridge that `pyo3_log` depends on stays live.
///
/// `Session::open` now consumes `LoggingConfig` (issue #213), but only
/// when the caller explicitly set a `filter` or `file`. The pyo3 binding
/// opens sessions through this same path and never sets logging — if
/// `open()` installed a subscriber for the empty/default config, it would
/// suppress the bridge and silently break all kernel logging into Python.
///
/// This test proves the gate holds: after a default-logging `open()`, a
/// kernel-target event still reaches the `log` capture sink (which only
/// happens while no subscriber is installed). A regression that dropped
/// the `is_some()` gate would install the `RUST_LOG`/warn default here and
/// this probe would stop being captured.
#[tokio::test]
async fn empty_logging_config_open_does_not_install_subscriber() {
    install_capture();

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"session_id":"sess-gate-213","warehouse_id":"test_wh"}"#),
        )
        .mount(&server)
        .await;

    let mut http = HttpConfig::default();
    http.retry_max_retries = 0;

    // No `.logging(...)` — the default (pyo3-equivalent) case.
    let session = Session::builder()
        .connection(
            ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                .expect("valid http_path"),
        )
        .auth(AuthConfig::Pat {
            token: "dapi_test_token".into(),
            identity_federation_client_id: None,
        })
        .http(http)
        .open()
        .await
        .expect("Session::open with default logging must succeed");
    assert_eq!(session.session_id(), "sess-gate-213");

    // Sanity: an explicitly-empty LoggingConfig is the same no-op path.
    let _empty = LoggingConfig::default();

    // The bridge must still be live: a kernel-target event AFTER open()
    // still produces a `log::Record` captured by our sink.
    let probe = "gate-probe-after-open-4d21";
    tracing::warn!(target: KERNEL_LOG_TARGET, probe = probe, "post-open bridge probe");

    let got = CAPTURED.get().unwrap().lock().unwrap().clone();
    assert!(
        got.iter()
            .any(|(target, msg)| target == KERNEL_LOG_TARGET && msg.contains(probe)),
        "issue #213 gate: after a default-logging Session::open, the tracing→log bridge must \
         still be live (no subscriber installed), but the post-open probe {probe:?} was not \
         captured — Session::open installed a subscriber for an unconfigured LoggingConfig, \
         which breaks pyo3_log. got {got:?}"
    );
}
