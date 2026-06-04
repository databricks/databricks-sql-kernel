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

//! Kernel logging shim — fixes every event's `tracing` target to
//! `databricks::sql::kernel`.
//!
//! ## Why this exists
//!
//! The pyo3 binding routes kernel logs into Python's `logging` via
//! `pyo3_log`, which derives the Python logger name from the record
//! `target` by replacing `::` with `.` (and nothing else — there is no
//! rename hook). The default `tracing` target is the module path
//! (`databricks_sql_kernel::session`, …), whose crate-root token has
//! underscores, so it would surface as the Python logger
//! `databricks_sql_kernel.*` — a *sibling* of the connector's
//! `databricks.sql.*` namespace, which `setLevel("databricks.sql")`
//! would not reach.
//!
//! By tagging every event with the explicit target
//! `databricks::sql::kernel`, records map to the Python logger
//! **`databricks.sql.kernel`** — a child of `databricks.sql`, so one
//! `setLevel` on the connector root cascades to the kernel. The logger
//! name is a public, effectively-permanent contract; this shim is what
//! keeps it correct without per-call-site `target:` annotations.
//!
//! ## Usage
//!
//! Replace `use tracing::{debug, warn};` with
//! `use crate::klog::{debug, warn};` in a module. Call sites are
//! unchanged — these macros accept the same forms as `tracing`'s
//! (message, format args, and structured fields). Span macros
//! (`info_span!`, `#[instrument]`) keep coming from `tracing`; their
//! target is set via [`KERNEL_LOG_TARGET`] at the span site (see
//! `session.rs`).
//!
//! A `klog_targets_match` test in this module guards against drift.

/// The single target string every kernel event carries.
///
/// `pub` (not `pub(crate)`) so binding crates can reference the *same*
/// constant instead of re-typing the literal — this is what lets the
/// pyo3 crate filter on exactly the target the kernel emits, with no
/// chance of the two drifting apart. `pyo3_log` maps it to the Python
/// logger `databricks.sql.kernel` via its `::` -> `.` rule. This string
/// is an effectively-permanent public contract (customers configure
/// logging by this name); guarded by drift tests in this module, in
/// `pyo3/src/logging.rs`, and in the connector's `test_logging.py`.
pub const KERNEL_LOG_TARGET: &str = "databricks::sql::kernel";

// The macros are defined with `klog_`-prefixed names and re-exported
// under the bare `debug` / `info` / … names below. The indirection is
// needed because a `macro_rules! debug` (and `warn`) re-exported under
// its own name is `E0659`-ambiguous with the built-in attributes
// `debug` / `warn`; defining under a distinct name and re-exporting
// `as debug` sidesteps the clash while still letting call sites write
// `use crate::klog::{debug, warn};` and call `debug!(…)` unchanged.
macro_rules! klog_debug {
    ($($arg:tt)*) => {
        ::tracing::debug!(target: $crate::klog::KERNEL_LOG_TARGET, $($arg)*)
    };
}
macro_rules! klog_info {
    ($($arg:tt)*) => {
        ::tracing::info!(target: $crate::klog::KERNEL_LOG_TARGET, $($arg)*)
    };
}
macro_rules! klog_warn {
    ($($arg:tt)*) => {
        ::tracing::warn!(target: $crate::klog::KERNEL_LOG_TARGET, $($arg)*)
    };
}
macro_rules! klog_error {
    ($($arg:tt)*) => {
        ::tracing::error!(target: $crate::klog::KERNEL_LOG_TARGET, $($arg)*)
    };
}
// `trace!` is part of the logging surface for completeness; no call
// site uses it today, hence the allows.
#[allow(unused_macros)]
macro_rules! klog_trace {
    ($($arg:tt)*) => {
        ::tracing::trace!(target: $crate::klog::KERNEL_LOG_TARGET, $($arg)*)
    };
}

#[allow(unused_imports)]
pub(crate) use {
    klog_debug as debug, klog_error as error, klog_info as info, klog_trace as trace,
    klog_warn as warn,
};

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    /// Captured `(target, rendered_message)` pairs. The message is what
    /// `tracing`'s `log` feature renders — including any structured
    /// fields appended as `key="value"` — so we can assert field
    /// propagation, not just the target.
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

    /// The kernel is built with `tracing`'s `log` feature and installs no
    /// `Subscriber` in tests, so each `klog::*` event also emits a
    /// `log::Record`. Assert the record target is exactly the contract
    /// string — this is the value `pyo3_log` turns into the
    /// `databricks.sql.kernel` Python logger. If a future change routes
    /// kernel logs through a different target, this fails loudly.
    ///
    /// CONSTRAINT: this test installs a process-global `log` logger via
    /// `set_boxed_logger`. There must be at most ONE such test per test
    /// binary — `log` allows only one global logger, and a second
    /// `set_boxed_logger` from a sibling test would no-op (its `Err` is
    /// ignored), so whichever test runs first wins and the other would
    /// silently capture into the wrong sink. If another global-logger
    /// test is ever added to this crate's unit-test binary, move one of
    /// them to an integration test (separate binary) or gate behind a
    /// shared `Once`. Today this is the only one.
    #[test]
    fn klog_emits_contract_target() {
        CAPTURED.get_or_init(|| Mutex::new(Vec::new()));
        // Ignore error: tolerate a logger already set in this binary
        // (see the CONSTRAINT note above — only one such test exists).
        let _ = log::set_boxed_logger(Box::new(Capture));
        log::set_max_level(log::LevelFilter::Trace);

        crate::klog::warn!("probe message");
        crate::klog::debug!(field = 1, "probe with field");

        let got = CAPTURED.get().unwrap().lock().unwrap().clone();
        assert!(
            got.iter().any(|(t, _)| t == super::KERNEL_LOG_TARGET),
            "expected a record with target {:?}, got {:?}",
            super::KERNEL_LOG_TARGET,
            got
        );
        assert_eq!(super::KERNEL_LOG_TARGET, "databricks::sql::kernel");
    }

    /// Guards correlation-field propagation: a `klog::*` call with a
    /// structured field (e.g. `statement_id = …`) must carry that field
    /// through `tracing`'s `log` feature into the `log::Record` — this is
    /// what lets a customer correlate interleaved kernel logs to a
    /// statement/session in a multi-session deployment.
    ///
    /// The `tracing`→`log` bridge does NOT propagate enclosing *span*
    /// fields, only the event's own fields, so reader/factory call sites
    /// pass `statement_id` as an explicit event field rather than relying
    /// on the session span. This test pins that mechanism: if the bridge
    /// behaviour changed (fields stop rendering) or a refactor dropped
    /// the field, the rendered message would no longer contain it.
    ///
    /// Shares the single global logger with `klog_emits_contract_target`
    /// (see the CONSTRAINT note above) — both read from `CAPTURED`, which
    /// accumulates across tests in this binary, so this asserts on the
    /// presence of *its own* distinctive record rather than on counts.
    #[test]
    fn klog_propagates_correlation_field() {
        CAPTURED.get_or_init(|| Mutex::new(Vec::new()));
        let _ = log::set_boxed_logger(Box::new(Capture));
        log::set_max_level(log::LevelFilter::Trace);

        let stmt = "stmt-correlation-probe-7f3a";
        crate::klog::debug!(statement_id = stmt, "creating reader: {} bytes", 42);

        let got = CAPTURED.get().unwrap().lock().unwrap().clone();
        // The field renders into the message as `statement_id="…"`.
        assert!(
            got.iter()
                .any(|(target, msg)| target == super::KERNEL_LOG_TARGET
                    && msg.contains(stmt)
                    && msg.contains("statement_id")),
            "expected a kernel-target record whose rendered message carries \
             the `statement_id` field {stmt:?}; got {got:?}"
        );
    }
}
