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

use databricks_sql_kernel::KERNEL_LOG_TARGET;

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
