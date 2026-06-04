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

//! Bridge kernel logs into Python's `logging` module.
//!
//! ## How it works
//!
//! The kernel emits its diagnostics via the `tracing` crate, with every
//! event tagged with the target `databricks::sql::kernel` (see the
//! kernel's `klog` shim). The kernel is built with `tracing`'s `log`
//! feature and the Python path installs **no** `tracing` `Subscriber`,
//! so each `tracing` event is also emitted as a `log::Record` carrying
//! that target.
//!
//! [`pyo3_log`] is installed here as the `log` consumer. It maps the
//! record target into a Python logger name by replacing `::` with `.`,
//! so `databricks::sql::kernel` becomes the Python logger
//! **`databricks.sql.kernel`** — a child of the connector's
//! `databricks.sql` namespace. A customer therefore controls kernel
//! verbosity with the ordinary stdlib idiom, and a level set on the
//! connector root cascades to the kernel:
//!
//! ```python
//! import logging
//! logging.getLogger("databricks.sql").setLevel(logging.DEBUG)   # connector + kernel
//! logging.getLogger("databricks.sql.kernel").setLevel(logging.DEBUG)  # kernel only
//! ```
//!
//! ## Filtering happens before the GIL
//!
//! Kernel events fire on tokio worker threads with the GIL released
//! (see `run_kernel!`). `pyo3_log` re-acquires the GIL per surviving
//! record, so we must drop below-threshold events *before* that
//! boundary. Two cheap gates do this: the global `log::max_level`
//! (set by `pyo3_log` from the Python loggers' effective levels) and
//! `pyo3_log`'s per-logger level cache. The customer's single knob
//! (`setLevel`) drives both — there is no separate Rust-side level
//! argument.
//!
//! ## Reconfiguration
//!
//! `pyo3_log` caches logger objects and their levels on first use. If a
//! customer changes levels *after* the first kernel log (e.g. raises a
//! logger to DEBUG mid-session), call [`reset_logging`] to clear the
//! cache so the new level takes effect. The connector calls this after
//! it adjusts logging; most customers configure logging before
//! connecting and never need it.

use pyo3::prelude::*;
use pyo3_log::{Caching, Logger, ResetHandle};
use std::sync::{Mutex, OnceLock};

/// Logger target the kernel tags every event with. Maps to the Python
/// logger `databricks.sql.kernel` via `pyo3_log`'s `::` -> `.` rule.
///
/// Re-exported from the kernel crate (not a local literal) so the target
/// this crate filters on is *guaranteed* identical to the one the kernel
/// emits — the two cannot drift apart.
use databricks_sql_kernel::KERNEL_LOG_TARGET as KERNEL_TARGET;

/// Target for breadcrumbs emitted at the pyo3 boundary itself (GIL
/// detach / call entry), distinct from the kernel core's events. Maps to
/// the Python logger `databricks.sql.kernel.pyo3` — a child of
/// `databricks.sql.kernel`, so the kernel-only scope filter (which keys
/// on the `databricks::sql::kernel` prefix) still forwards it, and a
/// customer can isolate boundary logs from core kernel logs by name.
pub(crate) const PYO3_LOG_TARGET: &str = "databricks::sql::kernel::pyo3";

/// Holds the `ResetHandle` from the one-time install so [`reset_logging`]
/// can clear `pyo3_log`'s caches after a Python-side level change. `None`
/// until [`init_logging`] succeeds.
static RESET_HANDLE: OnceLock<Mutex<Option<ResetHandle>>> = OnceLock::new();

/// Install the `tracing`/`log` -> Python `logging` bridge.
///
/// Idempotent: the first call installs `pyo3_log`; later calls are
/// no-ops (a process may only have one global `log` logger, and a
/// second install would error). Safe to call from every `Session`
/// construction — the connector calls it once at kernel-backend import.
///
/// Forwards records from **all** Rust crates — the kernel's own events
/// (under `databricks.sql.kernel` / `…::pyo3`) *and* the HTTP stack it
/// depends on (`hyper_util.client…`, `reqwest.connect`, `rustls`, …),
/// each landing under its own Python logger name. This is intentional:
/// the verbosity is still gated by each Python logger's level, and the
/// dependency crates are near-silent above DEBUG — so normal operation
/// stays quiet, while a customer who turns on DEBUG to diagnose a
/// connectivity / TLS issue gets the full wire-level picture in one
/// shot, with no flag to flip and no support round-trip to ask for it.
/// The per-record GIL re-acquire cost of forwarding deps only
/// materializes when DEBUG is actually enabled on those loggers; a
/// customer can silence them with the standard
/// `logging.getLogger("hyper_util").setLevel(WARNING)`.
///
/// Returns `true` if this call performed the install, `false` if logging
/// was already initialized.
pub(crate) fn init_logging(py: Python<'_>) -> bool {
    let slot = RESET_HANDLE.get_or_init(|| Mutex::new(None));
    let mut guard = slot.lock().unwrap();
    if guard.is_some() {
        return false;
    }

    // `LoggersAndLevels` caching: resolve each Python logger object and
    // its effective level once, then gate subsequent records cheaply
    // without re-entering Python. The default filter forwards every
    // target at `Trace`; the per-logger Python level does the real
    // gating. The explicit kernel `filter_target` is redundant under a
    // `Trace` default but documents the contract target and guards
    // against a future default change silently dropping kernel records.
    let handle = Logger::new(py, Caching::LoggersAndLevels)
        .expect("failed to construct pyo3_log Logger")
        .filter(log::LevelFilter::Trace)
        .filter_target(KERNEL_TARGET.to_owned(), log::LevelFilter::Trace)
        .install();

    match handle {
        Ok(h) => {
            *guard = Some(h);
            true
        }
        Err(_) => {
            // Another `log` logger is already installed (e.g. the host
            // application set one up). We leave it in place — the host's
            // logger wins and kernel records flow through it. Not an
            // error worth surfacing to Python.
            false
        }
    }
}

/// Clear `pyo3_log`'s logger/level caches so a Python-side logging
/// reconfiguration (e.g. a `setLevel` call after the first kernel log)
/// takes effect. No-op if logging was never initialized.
pub(crate) fn reset_logging() {
    if let Some(slot) = RESET_HANDLE.get() {
        if let Some(handle) = slot.lock().unwrap().as_ref() {
            handle.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KERNEL_TARGET;

    /// Layer 2 of the logger-name drift guard (see also the kernel's
    /// `klog::tests::klog_emits_contract_target` and the Python
    /// `test_logging.py` end-to-end test). The Python logger name
    /// customers configure — `databricks.sql.kernel` — is derived by
    /// `pyo3_log` from this target via `::` -> `.`. The target the kernel
    /// *emits* and the target this crate *filters on* must be the same
    /// string, or kernel records would be filtered out / land on the
    /// wrong logger. This pins both halves of that contract.
    #[test]
    fn kernel_target_matches_contract() {
        // The exact string the kernel's `klog` shim stamps on every
        // event (kept in sync with `databricks_sql_kernel`'s
        // `klog::KERNEL_LOG_TARGET`). pyo3_log maps this to the Python
        // logger `databricks.sql.kernel`.
        assert_eq!(KERNEL_TARGET, "databricks::sql::kernel");
        assert_eq!(
            KERNEL_TARGET.replace("::", "."),
            "databricks.sql.kernel",
            "pyo3_log's ::->. mapping must yield the documented Python logger name"
        );
    }
}
