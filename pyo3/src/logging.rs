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
//! boundary. Two cheap gates do this: the global `log::max_level`, set
//! here to the most verbose level any Python logger accepts (see
//! `PythonLevels`), and `pyo3_log`'s per-logger level cache. The
//! first gate is an atomic compare inside the `log` and `tracing` macros,
//! so a record (or span enter/exit) below every logger's level is dropped
//! before it is built or reaches `pyo3_log`. The customer's single knob
//! (`setLevel`) drives both — there is no separate Rust-side level
//! argument.
//!
//! ## Reconfiguration
//!
//! The max level is recomputed at install, on every `Session`
//! construction ([`refresh_max_level`], which also clears `pyo3_log`'s
//! cache when a level changed) and by [`reset_logging`], so levels set
//! before connecting take effect on their own. `pyo3_log` caches logger
//! objects and their levels on first use. If a customer changes levels on
//! an open session (e.g. raises a logger to DEBUG mid-session), call
//! [`reset_logging`] to recompute the max level and clear the cache so
//! the new level takes effect.
//!
//! ## Interpreter exit
//!
//! Runtime workers can outlive their handle (`KernelRuntime`'s `Drop`
//! doesn't wait for them), and pyo3's finalization check is compiled out
//! under `abi3-py39`, so a worker forwarding a record while the
//! interpreter finalizes can abort the process. An `atexit` hook, which
//! runs before finalization starts, makes the bridge drop records from
//! then on.

use pyo3::prelude::*;
use pyo3_log::{Caching, Logger, ResetHandle};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// One-time install state. Tristate so a *failed* install is sticky: we
/// don't retry `Logger::install()` — which re-walks construction + the
/// global-logger set — on every subsequent `init_logging` call. The
/// installed bridge's reset handle is kept in [`RESET_HANDLE`].
enum LogState {
    /// No install attempted yet.
    NotInstalled,
    /// Installed.
    Installed,
    /// Install was attempted and failed (or `Logger` construction
    /// failed). Sticky — we won't try again this process.
    Failed,
}

/// Holds the install state so repeated `init_logging` calls stay cheap.
/// `Mutex` poisoning is handled (`into_inner`) rather than
/// `unwrap`-panicked — a logging helper must never take down the process.
static LOG_STATE: OnceLock<Mutex<LogState>> = OnceLock::new();

/// Lock the state, recovering the guard if a previous holder panicked
/// while holding it (poisoned). Logging must never panic the caller, so
/// we never `unwrap` the lock.
fn lock_state() -> std::sync::MutexGuard<'static, LogState> {
    LOG_STATE
        .get_or_init(|| Mutex::new(LogState::NotInstalled))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Set by an `atexit` hook; see "Interpreter exit" above.
static INTERPRETER_EXITING: AtomicBool = AtomicBool::new(false);

#[pyfunction]
fn mark_interpreter_exiting() {
    INTERPRETER_EXITING.store(true, Ordering::SeqCst);
}

fn register_exit_hook(py: Python<'_>) -> PyResult<()> {
    let hook = wrap_pyfunction!(mark_interpreter_exiting, py)?;
    py.import("atexit")?.call_method1("register", (hook,))?;
    Ok(())
}

/// Forwards to `pyo3_log` until the interpreter starts exiting.
struct ExitGuard<L>(L);

impl<L: log::Log> log::Log for ExitGuard<L> {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        !INTERPRETER_EXITING.load(Ordering::SeqCst) && self.0.enabled(metadata)
    }

    fn log(&self, record: &log::Record<'_>) {
        if !INTERPRETER_EXITING.load(Ordering::SeqCst) {
            self.0.log(record);
        }
    }

    fn flush(&self) {
        self.0.flush();
    }
}

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
/// Returns `true` if this call performed the install, `false` otherwise
/// (already installed, a prior attempt failed, `Logger` construction
/// failed, or another global `log` logger is already set). Never panics:
/// a logging helper must not be able to tear down the host process.
pub(crate) fn init_logging(py: Python<'_>) -> bool {
    if RESET_HANDLE.get().is_some() {
        return false;
    }
    // Read the levels before taking the install lock: the read runs Python
    // code, during which another thread may take the GIL and then block on
    // that lock.
    let levels = PythonLevels::read(py);
    let mut state = lock_state();
    // Already installed, or a prior attempt failed — don't retry. Retry
    // would re-run `Logger::install()`'s global-logger set, which is not
    // free, on every Session construction.
    if !matches!(*state, LogState::NotInstalled) {
        return false;
    }

    // `LoggersAndLevels` caching: resolve each Python logger object and
    // its effective level once, then gate subsequent records cheaply
    // without re-entering Python. The default filter forwards every
    // target at `Trace`; the per-logger Python level does the real
    // gating. The explicit kernel `filter_target` is redundant under a
    // `Trace` default but documents the contract target and guards
    // against a future default change silently dropping kernel records.
    //
    // `Logger::new` returns a `PyResult` — it can fail if the Python
    // `logging` module can't be imported or the interpreter is mid
    // shutdown. We must NOT `expect()` it: an unwinding panic across the
    // pyo3/FFI boundary is undefined behavior and would tear down the
    // process over a non-essential feature. Treat it as a sticky failure.
    let logger = match Logger::new(py, Caching::LoggersAndLevels) {
        Ok(l) => l,
        Err(_) => {
            *state = LogState::Failed;
            return false;
        }
    };
    let logger = logger
        .filter(log::LevelFilter::Trace)
        .filter_target(KERNEL_TARGET.to_owned(), log::LevelFilter::Trace);

    // What `Logger::install()` does, but behind `ExitGuard`, and with the max
    // level taken from the Python loggers instead of the `Trace` filter above.
    let handle = logger.reset_handle();
    match log::set_boxed_logger(Box::new(ExitGuard(logger))) {
        Ok(()) => {
            let _ = RESET_HANDLE.set(handle);
            apply(levels, false);
            // Best effort: without the hook, records still flow until exit.
            let _ = register_exit_hook(py);
            *state = LogState::Installed;
            true
        }
        Err(_) => {
            // A global `log` logger is already installed (e.g. the host
            // application or another library set one up). We leave it in
            // place; kernel records then flow to *that* logger (whatever
            // sink it points at — e.g. env_logger to stderr), NOT to
            // Python `logging`. Sticky so we don't retry the failing
            // install on every call.
            *state = LogState::Failed;
            false
        }
    }
}

/// Recompute the max level and clear `pyo3_log`'s logger/level caches so a
/// Python-side logging reconfiguration (e.g. a `setLevel` call after the
/// first kernel log) takes effect. No-op if logging was never installed.
/// Never panics.
pub(crate) fn reset_logging(py: Python<'_>) {
    if RESET_HANDLE.get().is_some() {
        apply(PythonLevels::read(py), true);
    }
}

/// Recompute the max level from the Python loggers. Called when a `Session`
/// is created, so levels a customer sets before connecting apply without
/// [`reset_logging`]. If any logger's level changed since the last read,
/// `pyo3_log`'s cache is cleared too: a logger it cached at the old level
/// would otherwise keep dropping newly enabled records. When no level
/// changed, this costs one attribute read. No-op if logging was never
/// installed.
pub(crate) fn refresh_max_level(py: Python<'_>) {
    if RESET_HANDLE.get().is_some() && !levels_unchanged(py) {
        apply(PythonLevels::read(py), false);
    }
}

/// `pyo3_log`'s reset handle, set once the bridge is installed.
static RESET_HANDLE: OnceLock<ResetHandle> = OnceLock::new();

/// Orders level reads: each takes a ticket before it reads.
static NEXT_READ: AtomicU64 = AtomicU64::new(0);

/// The ticket and fingerprint of the last applied read.
static APPLIED: Mutex<(u64, u64)> = Mutex::new((0, 0));

/// Set `log::max_level` from `levels`, and clear `pyo3_log`'s cache if the
/// levels changed or `reset` is set.
///
/// Reads run Python code, so a read can be interrupted by a thread that
/// changes a level and applies its own read. A read that started earlier
/// than the last applied one is therefore not applied: the later read saw
/// at least as recent a configuration. Nothing in here calls Python, so
/// holding `APPLIED` can't deadlock with the GIL.
fn apply(levels: PythonLevels, reset: bool) {
    let Some(handle) = RESET_HANDLE.get() else {
        return;
    };
    let mut applied = APPLIED.lock().unwrap_or_else(|p| p.into_inner());
    let (set_level, clear_cache) = decide(&mut applied, &levels, reset);
    if let Some(level) = set_level {
        log::set_max_level(level);
    }
    if clear_cache {
        handle.reset();
    }
}

/// What [`apply`] does for `levels`, given the `(ticket, fingerprint)` of
/// the last applied read (updated in place): the max level to set, if
/// any, and whether to clear `pyo3_log`'s cache.
fn decide(
    applied: &mut (u64, u64),
    levels: &PythonLevels,
    reset: bool,
) -> (Option<log::LevelFilter>, bool) {
    let (applied_ticket, applied_fingerprint) = *applied;
    if levels.ticket <= applied_ticket {
        return (None, reset);
    }
    *applied = (levels.ticket, levels.fingerprint);
    (
        Some(levels.max_level),
        reset || levels.fingerprint != applied_fingerprint,
    )
}

/// The current `log::max_level`, for tests.
pub(crate) fn max_level_name() -> &'static str {
    match log::max_level() {
        log::LevelFilter::Off => "OFF",
        log::LevelFilter::Error => "ERROR",
        log::LevelFilter::Warn => "WARN",
        log::LevelFilter::Info => "INFO",
        log::LevelFilter::Debug => "DEBUG",
        log::LevelFilter::Trace => "TRACE",
    }
}

/// Name of a logger used only to detect level changes; see
/// [`levels_unchanged`].
const LEVELS_PROBE: &str = "databricks.sql.kernel._levels_probe";

/// Whether no Python logger level changed since the last read.
///
/// `logging` clears every logger's `_cache` on `setLevel()` and
/// `logging.disable()`. [`PythonLevels::read`] fills the probe logger's
/// cache before it reads, so a still-filled cache means nothing changed.
/// Errs towards `false` (do the full read).
fn levels_unchanged(py: Python<'_>) -> bool {
    let check = || -> PyResult<bool> {
        let probe = py
            .import("logging")?
            .call_method1("getLogger", (LEVELS_PROBE,))?;
        Ok(probe.getattr("_cache")?.len()? > 0)
    };
    check().unwrap_or(false)
}

struct PythonLevels {
    ticket: u64,
    /// The most verbose `log` level any Python logger currently accepts.
    max_level: log::LevelFilter,
    /// Changes whenever a logger is added or its effective level changes.
    fingerprint: u64,
}

impl PythonLevels {
    /// Read the levels of the root logger and every existing logger. A
    /// logger nobody has created yet inherits its nearest existing
    /// ancestor's level, so these bound what `pyo3_log` could forward;
    /// Python's `logging.disable()` raises the bound further. If the
    /// loggers can't be read, falls back to `Trace` (forward everything to
    /// `pyo3_log`, the previous behaviour).
    fn read(py: Python<'_>) -> Self {
        let ticket = NEXT_READ.fetch_add(1, Ordering::Relaxed) + 1;
        let read = || -> PyResult<Self> {
            let logging = py.import("logging")?;
            // Fill the probe's cache first, so a level changed during the read
            // empties it again and the next refresh reads anew. A disabled
            // logger never fills its cache, and `dictConfig()` / `fileConfig()`
            // disable existing loggers by default; the probe emits nothing, so
            // re-enable it.
            let probe = logging.call_method1("getLogger", (LEVELS_PROBE,))?;
            probe.setattr("disabled", false)?;
            probe.call_method1("isEnabledFor", (50,))?;
            let root = logging.getattr("root")?;
            let manager = root.getattr("manager")?;
            let logger_type = logging.getattr("Logger")?;
            let mut hasher = DefaultHasher::new();
            let mut lowest: i64 = root.call_method0("getEffectiveLevel")?.extract()?;
            lowest.hash(&mut hasher);
            // Snapshot the values: another thread may create a logger meanwhile.
            let loggers = py
                .import("builtins")?
                .getattr("list")?
                .call1((manager.getattr("loggerDict")?.call_method0("values")?,))?;
            for logger in loggers.try_iter()? {
                let logger = logger?;
                // `loggerDict` also holds `PlaceHolder`s for unnamed parents.
                if logger.is_instance(&logger_type)? {
                    let level: i64 = logger.call_method0("getEffectiveLevel")?.extract()?;
                    (logger.as_ptr() as usize, level).hash(&mut hasher);
                    lowest = lowest.min(level);
                }
            }
            let disabled_up_to: i64 = manager.getattr("disable")?.extract()?;
            disabled_up_to.hash(&mut hasher);
            Ok(Self {
                ticket,
                max_level: level_filter_for_python_threshold(lowest.max(disabled_up_to + 1)),
                fingerprint: hasher.finish(),
            })
        };
        read().unwrap_or(Self {
            ticket,
            max_level: log::LevelFilter::Trace,
            fingerprint: 0,
        })
    }
}

/// The most verbose `log` level whose Python level (as `pyo3_log` maps
/// them: Trace 5, Debug 10, Info 20, Warn 30, Error 40) is at or above a
/// Python logger threshold.
fn level_filter_for_python_threshold(threshold: i64) -> log::LevelFilter {
    match threshold {
        t if t <= 5 => log::LevelFilter::Trace,
        t if t <= 10 => log::LevelFilter::Debug,
        t if t <= 20 => log::LevelFilter::Info,
        t if t <= 30 => log::LevelFilter::Warn,
        t if t <= 40 => log::LevelFilter::Error,
        _ => log::LevelFilter::Off,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decide, level_filter_for_python_threshold, levels_unchanged, mark_interpreter_exiting,
        ExitGuard, PythonLevels, KERNEL_TARGET,
    };
    use log::{LevelFilter, Log};
    use pyo3::prelude::*;
    use pyo3::types::PyDict;
    use std::ffi::CString;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // One test, since Python's logging configuration is process-wide.
    #[test]
    fn reads_python_levels_and_detects_changes() {
        Python::initialize();
        Python::attach(|py| {
            let globals = PyDict::new(py);
            let run = |code: &str| {
                py.run(&CString::new(code).unwrap(), Some(&globals), None)
                    .unwrap()
            };
            run("import logging\nlogging.root.setLevel(logging.WARNING)\nchild = logging.getLogger('kernel_logging_test.child')");

            let levels = PythonLevels::read(py);
            assert_eq!(levels.max_level, LevelFilter::Warn);
            assert!(levels_unchanged(py), "nothing changed since the read");
            let tickets = levels.ticket;

            run("child.setLevel(logging.DEBUG)");
            assert!(!levels_unchanged(py), "setLevel must be detected");
            let levels = PythonLevels::read(py);
            assert_eq!(levels.max_level, LevelFilter::Debug);
            assert!(levels.ticket > tickets, "each read takes a later ticket");
            assert!(levels_unchanged(py));

            run("logging.disable(logging.CRITICAL)");
            assert!(!levels_unchanged(py), "logging.disable must be detected");
            assert_eq!(PythonLevels::read(py).max_level, LevelFilter::Off);

            run("logging.disable(logging.NOTSET)\nchild.setLevel(logging.NOTSET)\nlogging.root.setLevel(logging.NOTSET)");
            assert_eq!(PythonLevels::read(py).max_level, LevelFilter::Trace);

            // A new logger with no level of its own changes nothing.
            run("logging.root.setLevel(logging.WARNING)");
            let before = PythonLevels::read(py).max_level;
            run("logging.getLogger('kernel_logging_test.other')");
            assert!(levels_unchanged(py));
            assert_eq!(PythonLevels::read(py).max_level, before);

            // dictConfig() disables existing loggers, the probe included. A
            // disabled logger never refills its cache, so without re-enabling
            // the probe every later read would look like a change.
            run("import logging.config\nlogging.config.dictConfig({'version': 1})\nchild.setLevel(logging.INFO)");
            assert!(!levels_unchanged(py), "setLevel after dictConfig");
            assert_eq!(PythonLevels::read(py).max_level, LevelFilter::Info);
            assert!(levels_unchanged(py), "the probe works after dictConfig");
            run("child.disabled = False\nchild.setLevel(logging.NOTSET)");
        });
    }

    fn levels(ticket: u64, fingerprint: u64) -> PythonLevels {
        PythonLevels {
            ticket,
            max_level: LevelFilter::Debug,
            fingerprint,
        }
    }

    #[test]
    fn apply_sets_newer_reads_and_resets_on_change() {
        let mut applied = (0, 0);
        // First read: set the level; the fingerprint differs from the
        // initial state, so the cache is cleared too.
        assert_eq!(
            decide(&mut applied, &levels(1, 7), false),
            (Some(LevelFilter::Debug), true)
        );
        assert_eq!(applied, (1, 7));
        // Same levels: set again, keep the cache.
        assert_eq!(
            decide(&mut applied, &levels(2, 7), false),
            (Some(LevelFilter::Debug), false)
        );
        // reset_logging() always clears the cache.
        assert_eq!(
            decide(&mut applied, &levels(3, 7), true),
            (Some(LevelFilter::Debug), true)
        );
        // A read that started before the applied one is dropped...
        decide(&mut applied, &levels(5, 9), false);
        assert_eq!(decide(&mut applied, &levels(4, 8), false), (None, false));
        assert_eq!(applied, (5, 9));
        // ...but an overtaken reset_logging() still clears the cache.
        assert_eq!(decide(&mut applied, &levels(4, 8), true), (None, true));
    }

    #[test]
    fn python_thresholds_map_to_the_levels_pyo3_log_forwards() {
        // pyo3_log maps Trace/Debug/Info/Warn/Error to Python 5/10/20/30/40.
        for (threshold, expected) in [
            (0, LevelFilter::Trace), // NOTSET on the root: everything
            (5, LevelFilter::Trace),
            (6, LevelFilter::Debug),
            (10, LevelFilter::Debug), // DEBUG
            (15, LevelFilter::Info),
            (20, LevelFilter::Info), // INFO
            (30, LevelFilter::Warn), // WARNING, the default
            (40, LevelFilter::Error),
            (41, LevelFilter::Off),
            (51, LevelFilter::Off), // logging.disable(CRITICAL)
        ] {
            assert_eq!(
                level_filter_for_python_threshold(threshold),
                expected,
                "threshold {threshold}"
            );
        }
    }

    #[test]
    fn exit_guard_drops_records_once_interpreter_exits() {
        struct Count(AtomicUsize);
        impl Log for Count {
            fn enabled(&self, _: &log::Metadata<'_>) -> bool {
                true
            }
            fn log(&self, _: &log::Record<'_>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn flush(&self) {}
        }

        let guard = ExitGuard(Count(AtomicUsize::new(0)));
        let record = log::Record::builder().level(log::Level::Error).build();
        guard.log(&record);
        mark_interpreter_exiting();
        guard.log(&record);
        assert!(!guard.enabled(record.metadata()));
        assert_eq!(guard.0 .0.load(Ordering::SeqCst), 1);
    }

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
