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
//! `ExecutedAsyncStatement`, `Metadata`, `ResultStream`. Auth: PAT
//! plus `External` (Python-callable token provider — used by the
//! connector to delegate OAuth back to its own auth provider).
//! Both synchronous `execute()` and async `submit()` paths
//! supported. Built as the `databricks_sql_kernel` extension module
//! via `maturin develop` / `maturin build`.
//!
//! The kernel's API is async — every method call from Python detaches
//! the GIL and `block_on`s on the session-owned tokio runtime.

mod auth;
mod errors;
mod executed_async;
mod fetch;
mod metadata;
mod result_stream;
mod session;
mod statement;

use pyo3::prelude::*;

#[pymodule]
fn databricks_sql_kernel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("KernelError", m.py().get_type::<errors::KernelError>())?;
    m.add_class::<session::Session>()?;
    m.add_class::<statement::Statement>()?;
    m.add_class::<statement::ExecutedStatement>()?;
    m.add_class::<executed_async::ExecutedAsyncStatement>()?;
    m.add_class::<metadata::Metadata>()?;
    m.add_class::<result_stream::ResultStream>()?;
    Ok(())
}
