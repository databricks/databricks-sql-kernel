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
use databricks_sql_kernel::{
    ExecutedAsyncStatement as KExecutedAsyncStatement, ExecutedStatementHandle, StatementStatus,
};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Map a kernel `StatementStatus` to a stable string slug. Matches
/// the connector's `CommandState` taxonomy one-for-one so the Python
/// side can branch on the result without holding a reference to a
/// `StatementStatus` enum.
fn status_str(status: &StatementStatus) -> &'static str {
    match status {
        StatementStatus::Pending => "Pending",
        StatementStatus::Running => "Running",
        StatementStatus::Succeeded => "Succeeded",
        StatementStatus::Failed(_) => "Failed",
        StatementStatus::Cancelled => "Cancelled",
        StatementStatus::Closed => "Closed",
        // `StatementStatus` is `#[non_exhaustive]`.
        _ => "Unknown",
    }
}

#[pyclass(name = "ExecutedAsyncStatement", unsendable)]
pub(crate) struct ExecutedAsyncStatement {
    inner: Option<ExecutedAsyncInner>,
    /// Cached statement id so callers can read it after `close()`.
    statement_id: String,
}

struct ExecutedAsyncInner {
    exec: KExecutedAsyncStatement,
    runtime: Arc<Runtime>,
}

impl ExecutedAsyncStatement {
    pub(crate) fn new(exec: KExecutedAsyncStatement, runtime: Arc<Runtime>) -> Self {
        let statement_id = exec.statement_id().to_owned();
        Self {
            statement_id,
            inner: Some(ExecutedAsyncInner { exec, runtime }),
        }
    }

    fn alive(&self) -> PyResult<&ExecutedAsyncInner> {
        self.inner
            .as_ref()
            .ok_or_else(|| crate::errors::KernelError::new_err("ExecutedAsyncStatement is closed"))
    }
}

#[pymethods]
impl ExecutedAsyncStatement {
    #[getter]
    fn statement_id(&self) -> &str {
        &self.statement_id
    }

    /// Single-shot status check. Returns a tuple
    /// `(state: str, failure: Optional[dict])` where:
    ///
    /// - `state` is one of `Pending / Running / Succeeded / Failed /
    ///   Cancelled / Closed`.
    /// - `failure` is `None` unless `state == "Failed"`, in which
    ///   case it carries the same structured-attrs dict that
    ///   `KernelError` exposes (`code`, `message`, `sql_state`,
    ///   `error_code`, `vendor_code`, `http_status`, `retryable`,
    ///   `query_id`).
    ///
    /// Surfacing failure as a dict rather than raising lets polling
    /// callers branch on terminal vs non-terminal states without a
    /// try/except boundary. The connector backend re-raises as
    /// the appropriate PEP 249 exception when it actually needs to.
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.alive()?;
        let status = py
            .detach(|| inner.runtime.block_on(inner.exec.status()))
            .map_err(|e| to_py_err(py, e))?;
        let state = status_str(&status);
        let failure: Option<Bound<'py, PyAny>> = match &status {
            StatementStatus::Failed(err) => {
                // Re-use the same dict-shape `to_py_err` produces so
                // wrappers consume one format end-to-end. The
                // exception carries the dict as its second `arg`.
                let py_err = to_py_err(py, err.clone());
                let args = py_err.value(py).getattr("args")?;
                let tuple: Vec<Bound<'py, PyAny>> = args.extract()?;
                tuple.into_iter().nth(1)
            }
            _ => None,
        };
        let tuple = pyo3::types::PyTuple::new(
            py,
            [
                state.into_pyobject(py)?.into_any(),
                match failure {
                    Some(f) => f,
                    None => py.None().into_bound(py),
                },
            ],
        )?;
        Ok(tuple.into_any())
    }

    /// Block until the statement reaches a terminal state, then
    /// return a `ResultStream` over the results.
    ///
    /// Drives polling on the session runtime; the kernel applies the
    /// session-configured `poll_interval` / `poll_timeout` cadence.
    /// `Failed` / `Cancelled` raise `KernelError`.
    fn await_result(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let inner = self.alive()?;
        let stream = py
            .detach(|| inner.runtime.block_on(inner.exec.await_result()))
            .map_err(|e| to_py_err(py, e))?;
        Ok(ResultStream::new(stream, inner.runtime.clone()))
    }

    /// Fetch the result without polling. Raises `KernelError(code=
    /// 'InvalidState')` if the statement has not reached a terminal
    /// state — call after `status()` reports `Succeeded`/`Closed`.
    fn fetch_result(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let inner = self.alive()?;
        let stream = py
            .detach(|| inner.runtime.block_on(inner.exec.fetch_result()))
            .map_err(|e| to_py_err(py, e))?;
        Ok(ResultStream::new(stream, inner.runtime.clone()))
    }

    /// Cancel the in-flight statement on the server.
    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.alive()?;
        py.detach(|| inner.runtime.block_on(inner.exec.cancel()))
            .map_err(|e| to_py_err(py, e))
    }

    /// Close the statement (server-side `CloseStatement`). Idempotent.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        let runtime = inner.runtime.clone();
        let res = py.detach(|| runtime.block_on(inner.exec.close()));
        self.inner = None;
        res.map_err(|e| to_py_err(py, e))
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
