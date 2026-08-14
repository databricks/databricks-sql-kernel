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
//! The `tests/klog_bridge.rs` integration test guards against drift in
//! the emitted target (and structured-field propagation).

/// The single target string every kernel event carries.
///
/// `pub` (not `pub(crate)`) so binding crates can reference the *same*
/// constant instead of re-typing the literal — this is what lets the
/// pyo3 crate filter on exactly the target the kernel emits, with no
/// chance of the two drifting apart. `pyo3_log` maps it to the Python
/// logger `databricks.sql.kernel` via its `::` -> `.` rule. This string
/// is an effectively-permanent public contract (customers configure
/// logging by this name); guarded by drift tests in
/// `tests/klog_bridge.rs`, in `pyo3/src/logging.rs`, and in the
/// connector's `test_logging.py`.
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

// The `tracing`→`log` bridge contract for kernel log events (the target
// `pyo3_log` maps to `databricks.sql.kernel`, and structured-field
// propagation) is verified in the `tests/klog_bridge.rs` integration test.
// It cannot live here: those assertions require that NO `tracing`
// subscriber is installed in the process, but this crate's unit-test
// binary now installs one whenever a test opens a session
// (`Session::open` → `logging::init_logging`, issue #213), which suppresses
// the `log` bridge. A dedicated integration-test binary that opens no
// session keeps the bridge intact.
