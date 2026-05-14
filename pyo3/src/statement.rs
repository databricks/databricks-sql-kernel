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

//! `Statement` + `ExecutedStatement` Python classes.
//!
//! Mirrors the kernel's `Statement::execute() -> ExecutedStatement`
//! flow (sync) and `Statement::submit() -> ExecutedAsyncStatement`
//! (async-consumption). The fetch surface on `ExecutedStatement`
//! returns Arrow record batches as pyarrow values.

use crate::executed_async::ExecutedAsyncStatement;
use crate::fetch;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Schema;
use databricks_sql_kernel::{
    ExecutedStatement as KExecutedStatement, ExecutedStatementHandle, Statement as KStatement,
};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Python-facing mutable statement.
///
/// Spec mutation is collapsed into simple `set_*` / `bind_*` methods
/// for now. The full `Statement::spec()` API surface lands in later
/// phases (named params, statement_conf, result_format).
#[pyclass(name = "Statement", unsendable)]
pub(crate) struct Statement {
    inner: Option<StatementInner>,
}

struct StatementInner {
    stmt: KStatement,
    runtime: Arc<Runtime>,
}

impl Statement {
    pub(crate) fn new(stmt: KStatement, runtime: Arc<Runtime>) -> Self {
        Self {
            inner: Some(StatementInner { stmt, runtime }),
        }
    }
}

#[pymethods]
impl Statement {
    /// Set the SQL text to execute. Replaces any previously-set SQL.
    fn set_sql(&mut self, sql: String) -> PyResult<()> {
        let inner = alive_mut!(self, "Statement");
        inner.stmt.spec().sql(sql);
        Ok(())
    }

    /// Wait-for-result execution. Blocks until the kernel finishes
    /// polling and returns an `ExecutedStatement` with the result
    /// stream ready to drain.
    fn execute(&mut self, py: Python<'_>) -> PyResult<ExecutedStatement> {
        let inner = alive_mut!(self, "Statement");
        let runtime = inner.runtime.clone();
        let exec = run_kernel!(py, runtime, inner.stmt.execute())?;
        Ok(ExecutedStatement::new(exec, runtime))
    }

    /// Submit-and-return. Sends `ExecuteStatement` with
    /// `wait_timeout=0s` so the server returns immediately with a
    /// `statement_id`; the kernel does not poll. Returns an
    /// `ExecutedAsyncStatement` the caller drives via `status()` /
    /// `await_result()` / `fetch_result()`.
    ///
    /// Long-running queries that would blow past `execute()`'s sync
    /// polling budget belong on this path.
    fn submit(&mut self, py: Python<'_>) -> PyResult<ExecutedAsyncStatement> {
        let inner = alive_mut!(self, "Statement");
        let runtime = inner.runtime.clone();
        let exec = run_kernel!(py, runtime, inner.stmt.submit())?;
        Ok(ExecutedAsyncStatement::new(exec, runtime))
    }

    /// Drop the statement. Any prior `ExecutedStatement` keeps
    /// working — kernel-side it has its own validity flag and
    /// statement_id and is independent of the `Statement` lifecycle.
    fn close(&mut self) {
        self.inner = None;
    }
}

/// Python-facing executed statement handle. Owns the kernel
/// `ExecutedStatement` and serves the result stream lazily.
#[pyclass(name = "ExecutedStatement", unsendable)]
pub(crate) struct ExecutedStatement {
    inner: Option<ExecutedInner>,
    /// Cached statement id so callers can read it after `close()`
    /// (e.g. for logging the id of a just-closed statement).
    statement_id: String,
    /// Cached schema. Pulled from the kernel once on construction —
    /// available even after `close()` so descriptions stay stable.
    schema: Arc<Schema>,
}

struct ExecutedInner {
    exec: KExecutedStatement,
    runtime: Arc<Runtime>,
    exhausted: bool,
}

impl ExecutedStatement {
    fn new(exec: KExecutedStatement, runtime: Arc<Runtime>) -> Self {
        let statement_id = exec.statement_id().to_owned();
        let schema = exec.schema();
        Self {
            statement_id,
            schema,
            inner: Some(ExecutedInner {
                exec,
                runtime,
                exhausted: false,
            }),
        }
    }
}

#[pymethods]
impl ExecutedStatement {
    #[getter]
    fn statement_id(&self) -> &str {
        &self.statement_id
    }

    /// Result schema as a `pyarrow.Schema`. Available even after
    /// `close()`.
    fn arrow_schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    /// Pull the next batch as a `pyarrow.RecordBatch`, or `None` if
    /// the stream is exhausted.
    fn fetch_next_batch<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let inner = alive_mut!(self, "ExecutedStatement");
        if inner.exhausted {
            return Ok(None);
        }
        let runtime = inner.runtime.clone();
        let batch = run_sync_kernel!(
            py,
            fetch::next_batch(&runtime, inner.exec.result_stream_mut())
        )?;
        match batch {
            Some(b) => Ok(Some(b.to_pyarrow(py)?)),
            None => {
                inner.exhausted = true;
                Ok(None)
            }
        }
    }

    /// Drain the full result and return a single `pyarrow.Table`.
    fn fetch_all_arrow<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = alive_mut!(self, "ExecutedStatement");
        let runtime = inner.runtime.clone();
        let batches = run_sync_kernel!(py, fetch::drain(&runtime, inner.exec.result_stream_mut()))?;
        inner.exhausted = true;
        fetch::batches_to_pyarrow_table(py, &self.schema, batches)
    }

    /// Cancel the in-flight statement on the server. Local validity
    /// flag short-circuits if the parent `Statement` was re-executed
    /// or dropped.
    fn cancel(&mut self, py: Python<'_>) -> PyResult<()> {
        let inner = alive_mut!(self, "ExecutedStatement");
        let runtime = inner.runtime.clone();
        run_kernel!(py, runtime, inner.exec.cancel())?;
        Ok(())
    }

    /// Close the statement (server-side `CloseStatement`). Idempotent.
    /// Subsequent calls on this handle raise `KernelError`.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        let runtime = inner.runtime.clone();
        let res = run_kernel!(py, runtime, inner.exec.close());
        // Drop the kernel handle regardless of close() outcome — the
        // kernel's Drop impl runs fire-and-forget cleanup if close()
        // failed mid-flight.
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
            "ExecutedStatement(statement_id={:?}, state={state})",
            self.statement_id
        )
    }
}
