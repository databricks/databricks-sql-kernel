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

//! `ExecutedAsyncStatement` — Python wrapper for the kernel's
//! async-consumption handle.
//!
//! Returned by `Statement.submit()`. The kernel returned without
//! driving polling; the caller owns the cadence via `status()` /
//! `await_result()` / `fetch_result()`. Mirrors the kernel's
//! `executed_async.rs` one-to-one.

use crate::errors::to_py_err;
use crate::result_stream::ResultStream;
use crate::runtime::KernelRuntime;
use databricks_sql_kernel::{
    ExecutedAsyncStatement as KExecutedAsyncStatement, ExecutedStatementHandle, StatementStatus,
};
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(name = "ExecutedAsyncStatement")]
pub(crate) struct ExecutedAsyncStatement {
    inner: Option<ExecutedAsyncInner>,
    /// Cached statement id so callers can read it after `close()`.
    statement_id: String,
}

struct ExecutedAsyncInner {
    exec: KExecutedAsyncStatement,
    runtime: Arc<KernelRuntime>,
}

impl ExecutedAsyncStatement {
    pub(crate) fn new(exec: KExecutedAsyncStatement, runtime: Arc<KernelRuntime>) -> Self {
        let statement_id = exec.statement_id().to_owned();
        Self {
            statement_id,
            inner: Some(ExecutedAsyncInner { exec, runtime }),
        }
    }
}

#[pymethods]
impl ExecutedAsyncStatement {
    #[getter]
    fn statement_id(&self) -> &str {
        &self.statement_id
    }

    /// Single-shot status check. Returns a tuple
    /// `(state: str, failure: Optional[KernelError])` where:
    ///
    /// - `state` is one of `Pending / Running / Succeeded / Failed /
    ///   Cancelled / Closed`.
    /// - `failure` is `None` unless `state == "Failed"`, in which
    ///   case it carries a `KernelError` instance exposing the
    ///   structured fields (`code`, `message`, `sql_state`,
    ///   `error_code`, `vendor_code`, `http_status`, `retryable`,
    ///   `query_id`) as plain attributes.
    ///
    /// Surfacing failure as a value rather than raising lets polling
    /// callers branch on terminal vs non-terminal states without a
    /// try/except boundary. The connector backend re-raises the
    /// failure as the appropriate PEP 249 exception when it actually
    /// needs to.
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = alive!(self, "ExecutedAsyncStatement");
        let runtime = inner.runtime.clone();
        let status = run_kernel!(py, runtime, inner.exec.status())?;
        // String mapping lives on the kernel `StatementStatus::as_str`
        // so all bindings share one taxonomy.
        let state = status.as_str();
        let failure: Bound<'py, PyAny> = match &status {
            StatementStatus::Failed(err) => {
                // Build the KernelError instance the same way every
                // other failure path does, then hand it to Python as
                // a plain value (not raised). Callers read .code /
                // .sql_state / .query_id directly.
                let py_err = to_py_err(py, err.clone());
                py_err.value(py).clone().into_any()
            }
            _ => py.None().into_bound(py),
        };
        let tuple = pyo3::types::PyTuple::new(py, [state.into_pyobject(py)?.into_any(), failure])?;
        Ok(tuple.into_any())
    }

    /// Block until the statement reaches a terminal state, then
    /// return a `ResultStream` over the results.
    ///
    /// Drives polling on the session runtime; the kernel applies the
    /// session-configured `poll_interval` / `poll_timeout` cadence.
    /// `Failed` / `Cancelled` raise `KernelError`.
    fn await_result(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let inner = alive!(self, "ExecutedAsyncStatement");
        let runtime = inner.runtime.clone();
        let stream = run_kernel!(py, runtime, inner.exec.await_result())?;
        Ok(ResultStream::new(stream, runtime))
    }

    /// Fetch the result without polling. Raises `KernelError(code=
    /// 'InvalidState')` if the statement has not reached a terminal
    /// state — call after `status()` reports `Succeeded`/`Closed`.
    fn fetch_result(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let inner = alive!(self, "ExecutedAsyncStatement");
        let runtime = inner.runtime.clone();
        let stream = run_kernel!(py, runtime, inner.exec.fetch_result())?;
        Ok(ResultStream::new(stream, runtime))
    }

    /// Cancel the in-flight statement on the server.
    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let inner = alive!(self, "ExecutedAsyncStatement");
        let runtime = inner.runtime.clone();
        run_kernel!(py, runtime, inner.exec.cancel())?;
        Ok(())
    }

    /// Close the statement (server-side `CloseStatement`). Idempotent.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        let runtime = inner.runtime.clone();
        let res = run_kernel!(py, runtime, KExecutedAsyncStatement::close(&mut inner.exec));
        self.inner = None;
        res.map(|_| ())
    }

    fn __repr__(&self) -> String {
        let state = if self.inner.is_some() {
            "open"
        } else {
            "closed"
        };
        format!(
            "ExecutedAsyncStatement(statement_id={:?}, state={state})",
            self.statement_id
        )
    }
}
