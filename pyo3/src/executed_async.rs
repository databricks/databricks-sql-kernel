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
    AsyncStatementReader as KAsyncStatementReader,
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

    /// Create an independent read-only handle retaining this statement's session routing.
    /// The owning handle controls cancellation, cleanup and execution telemetry.
    fn reader(&self, py: Python<'_>) -> PyResult<AsyncStatementReader> {
        let inner = alive!(self, "ExecutedAsyncStatement");
        Ok(AsyncStatementReader {
            inner: inner.exec.reader().map_err(|error| to_py_err(py, error))?,
            runtime: Arc::clone(&inner.runtime),
        })
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
        status_to_python(py, status)
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
        KernelRuntime::release(runtime, py);
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

/// Independent status and result access without server lifecycle ownership.
#[pyclass(name = "AsyncStatementReader")]
pub(crate) struct AsyncStatementReader {
    inner: KAsyncStatementReader,
    runtime: Arc<KernelRuntime>,
}

#[pymethods]
impl AsyncStatementReader {
    #[getter]
    fn statement_id(&self) -> &str {
        self.inner.statement_id()
    }

    /// Read server status as `(state, failure)`.
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let status = run_kernel!(py, Arc::clone(&self.runtime), self.inner.status())?;
        status_to_python(py, status)
    }

    /// Poll until terminal and return a fresh result stream. Interrupted reads do not cancel.
    fn await_result(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let runtime = Arc::clone(&self.runtime);
        let stream = run_kernel!(py, runtime, self.inner.await_result())?;
        Ok(ResultStream::new(stream, runtime))
    }

    /// Return a fresh result stream; raises InvalidState if execution is not terminal.
    fn fetch_result(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let runtime = Arc::clone(&self.runtime);
        let stream = run_kernel!(py, runtime, self.inner.fetch_result())?;
        Ok(ResultStream::new(stream, runtime))
    }
}

fn status_to_python<'py>(py: Python<'py>, status: StatementStatus) -> PyResult<Bound<'py, PyAny>> {
    let state = status.as_str();
    let failure = match &status {
        StatementStatus::Failed(err) => to_py_err(py, err.clone()).value(py).clone().into_any(),
        _ => py.None().into_bound(py),
    };
    Ok(pyo3::types::PyTuple::new(py, [state.into_pyobject(py)?.into_any(), failure])?.into_any())
}
