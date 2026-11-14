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

//! Python bindings for the Databricks SQL Kernel.
//!
//! Phase 1 surface today: `Session`, `Statement`, `ExecutedStatement`,
//! `ExecutedAsyncStatement`, `Metadata`, `ResultStream`. Auth is
//! PAT-only — OAuth and other auth types are reserved in the kernel
//! surface but not yet wired here, pending a decision on whether
//! PySQL owns OAuth or the kernel does (see Phase 2 design notes).
//! Both synchronous `execute()` and async `submit()` paths supported.
//! Built as the `databricks_sql_kernel` extension module via
//! `maturin develop` / `maturin build`.
//!
//! The kernel's API is async — every method call from Python detaches
//! the GIL and `block_on`s on the tokio runtime all sessions share.

// `macros` is `#[macro_use]` so the `run_kernel!` / `alive!` /
// `alive_mut!` macros are visible across the rest of the crate
// without per-file imports.
#[macro_use]
mod macros;

mod auth;
mod errors;
mod executed_async;
mod fetch;
mod gil;
mod logging;
mod metadata;
mod params;
mod result_stream;
mod rows;
mod runtime;
mod session;
mod statement;

use pyo3::prelude::*;

/// Route kernel logs into Python's `logging`. Idempotent — safe to call
/// more than once (later calls are no-ops). Returns `True` if this call
/// performed the install, `False` if logging was already initialized (by
/// an earlier call or because the host process owns the `log` logger).
///
/// The connector calls this automatically when the kernel backend loads,
/// so `use_kernel=True` users need not call it themselves.
///
/// Records are forwarded under per-crate logger names: the kernel's own
/// events under `databricks.sql.kernel` (and the pyo3 boundary under
/// `databricks.sql.kernel.pyo3`), plus the underlying HTTP stack
/// (`hyper_util.client…`, `reqwest.connect`, `rustls`, …). Each is gated
/// by its Python logger's level, so deps stay quiet until a customer
/// raises them — e.g. `logging.getLogger("databricks.sql").setLevel(DEBUG)`
/// for everything, or silence the wire chatter with
/// `logging.getLogger("hyper_util").setLevel(WARNING)`.
#[pyfunction]
fn init_logging(py: Python<'_>) -> bool {
    logging::init_logging(py)
}

/// Re-read the Python logger levels and clear cached ones so a Python-side
/// logging reconfiguration applied *after* the first kernel log (e.g.
/// raising `databricks.sql.kernel` to DEBUG mid-session) takes effect.
/// Levels set before a `Session` is created apply without this call.
#[pyfunction]
fn reset_logging(py: Python<'_>) {
    logging::reset_logging(py);
}

#[pymodule]
fn databricks_sql_kernel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("KernelError", m.py().get_type::<errors::KernelError>())?;
    m.add_class::<session::Session>()?;
    m.add_class::<statement::Statement>()?;
    m.add_class::<statement::ExecutedStatement>()?;
    m.add_class::<statement::StatementCanceller>()?;
    m.add_class::<executed_async::ExecutedAsyncStatement>()?;
    m.add_class::<executed_async::AsyncStatementReader>()?;
    m.add_class::<metadata::Metadata>()?;
    m.add_class::<result_stream::ResultStream>()?;
    m.add_class::<rows::SchemaKey>()?;
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    m.add_function(wrap_pyfunction!(reset_logging, m)?)?;
    m.add_function(wrap_pyfunction!(rows::rows_from_arrow, m)?)?;

    // Install the log bridge at module import so *any* consumer of the
    // bare wheel (the connector, integration tests, custom embedders)
    // gets kernel logs in Python `logging` without an explicit
    // `init_logging()` call. It's idempotent and never panics, so the
    // connector's own call at kernel-backend load is a harmless no-op.
    // Records still only surface once a customer raises the relevant
    // logger's level, so this default-on install is silent until asked.
    logging::init_logging(m.py());
    gil::install_fork_handler();
    Ok(())
}
