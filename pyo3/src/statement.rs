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

use crate::errors::to_py_err;
use crate::executed_async::ExecutedAsyncStatement;
use crate::fetch;
use crate::params::parse_typed_value;
use crate::rows::{self, SchemaKey};
use crate::runtime::KernelRuntime;
use arrow_array::RecordBatch;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Schema;
use databricks_sql_kernel::{
    Error as KError, ExecutedStatement as KExecutedStatement, ExecutedStatementHandle, ResultBatch,
    Statement as KStatement, StatementCanceller as KStatementCanceller, TryNextBatch,
};
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Python-facing mutable statement.
///
/// Spec mutation is collapsed into simple `set_*` / `bind_*` methods
/// for now. The full `Statement::spec()` API surface lands in later
/// phases (named params, statement_conf, result_format).
#[pyclass(name = "Statement")]
pub(crate) struct Statement {
    inner: Option<StatementInner>,
}

struct StatementInner {
    stmt: KStatement,
    runtime: Arc<KernelRuntime>,
}

impl Statement {
    pub(crate) fn new(stmt: KStatement, runtime: Arc<KernelRuntime>) -> Self {
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

    /// Set the server-side row cap for this statement. ``None`` clears it.
    fn set_row_limit(&mut self, row_limit: Option<i64>) -> PyResult<()> {
        let inner = alive_mut!(self, "Statement");
        inner.stmt.spec().row_limit(row_limit);
        Ok(())
    }

    /// Set per-statement query tags.
    ///
    /// ``tags`` is a ``dict[str, str | None]`` — a ``None`` value emits
    /// a bare key (no colon) on the wire, matching the connector's
    /// ``Dict[str, Optional[str]]`` contract. The kernel serialises to
    /// the SEA ``query_tags`` conf shape and merges it into the
    /// per-statement conf overlay.
    ///
    /// Raises ``KernelError(code='InvalidArgument')`` if a ``query_tags``
    /// entry was already set explicitly via the statement conf (the
    /// kernel refuses an ambiguous double-definition). An empty ``tags``
    /// dict is a no-op.
    fn set_query_tags(
        &mut self,
        py: Python<'_>,
        tags: HashMap<String, Option<String>>,
    ) -> PyResult<()> {
        let inner = alive_mut!(self, "Statement");
        inner
            .stmt
            .spec()
            .query_tags(tags)
            .map_err(|e| to_py_err(py, e))?;
        Ok(())
    }

    /// Bind a positional parameter.
    ///
    /// ``ordinal`` is 1-based on the SEA wire. ``value_str`` is the
    /// connector's string-serialised form (or ``None`` for SQL
    /// NULL); ``sql_type`` is the Databricks SQL type name.
    ///
    /// Accepted ``sql_type`` values (case-insensitive):
    /// ``"VOID"``, ``"BOOLEAN"`` / ``"BOOL"``, ``"TINYINT"``,
    /// ``"SMALLINT"``, ``"INT"`` / ``"INTEGER"``, ``"BIGINT"`` /
    /// ``"LONG"``, ``"FLOAT"``, ``"DOUBLE"``, ``"STRING"`` /
    /// ``"VARCHAR"`` / ``"CHAR"``, ``"DATE"``, ``"TIMESTAMP"``,
    /// ``"TIMESTAMP_NTZ"``, ``"TIMESTAMP_LTZ"``, ``"INTERVAL"``,
    /// ``"DECIMAL(p,s)"`` and ``"DECIMAL(p)"`` (scale defaults to
    /// 0), ``"GEOGRAPHY"`` / ``"GEOGRAPHY(srid)"``, and
    /// ``"GEOMETRY"`` / ``"GEOMETRY(srid)"``. See
    /// ``pyo3/src/params.rs`` for the authoritative mapping.
    ///
    /// For ``GEOGRAPHY`` / ``GEOMETRY``, the ``value_str`` is either
    /// EWKT (``"SRID=4326;POINT(1 2)"``) or bare WKT
    /// (``"POINT(1 2)"``). If both the type-name and the value carry
    /// an SRID, they must agree — mismatch raises
    /// ``KernelError(code='InvalidArgument', ...)``.
    ///
    /// This shape — string value + type name — exists because the
    /// connector's ``TSparkParameter`` already string-serialises
    /// every value (the Thrift wire format uses
    /// ``stringValue``). Re-parsing into a native Python type just
    /// to re-encode to a kernel ``TypedValue`` would be a lossy
    /// round-trip; the kernel takes the string directly.
    ///
    /// Compound types (ARRAY/MAP/STRUCT) and BINARY are not yet
    /// supported and raise ``KernelError(code='InvalidArgument',
    /// ...)``. Bad value literals (e.g. ``bind_param(1, "abc",
    /// "INT")``) also raise at bind time, before ``execute()``.
    #[pyo3(signature = (ordinal, value_str, sql_type))]
    fn bind_param(
        &mut self,
        py: Python<'_>,
        ordinal: i64,
        value_str: Option<String>,
        sql_type: String,
    ) -> PyResult<()> {
        let inner = alive_mut!(self, "Statement");
        // Reject ordinal < 1 here rather than via PyO3's `usize`
        // extraction. A `usize` parameter would surface a negative
        // ordinal as Python `OverflowError` before the function body
        // ran, breaking the documented `KernelError(InvalidArgument)`
        // contract that wrappers catch on. The kernel itself also
        // rejects 0 (see `StatementSpec::param`), but we surface that
        // here for the same reason — one error code, one type, for
        // every invalid ordinal.
        if ordinal < 1 {
            return Err(to_py_err(
                py,
                KError::invalid_argument()
                    .message("positional parameter ordinal must be >= 1 (1-based)"),
            ));
        }
        let typed =
            parse_typed_value(&sql_type, value_str.as_deref()).map_err(|e| to_py_err(py, e))?;
        inner
            .stmt
            .spec()
            .param(ordinal as usize, typed)
            .map_err(|e| to_py_err(py, e))?;
        Ok(())
    }

    /// Bind a named parameter.
    ///
    /// The SQL must reference the binding as ``:name``. The canonical
    /// SEA proto marks ``StatementParameter.name`` as
    /// ``openapi_required=true``; named binding is the spec-required
    /// public form. ``value_str`` and ``sql_type`` follow the same
    /// conventions as ``bind_param``.
    #[pyo3(signature = (name, value_str, sql_type))]
    fn bind_named_param(
        &mut self,
        py: Python<'_>,
        name: String,
        value_str: Option<String>,
        sql_type: String,
    ) -> PyResult<()> {
        let inner = alive_mut!(self, "Statement");
        let typed =
            parse_typed_value(&sql_type, value_str.as_deref()).map_err(|e| to_py_err(py, e))?;
        inner
            .stmt
            .spec()
            .param_named(name, typed)
            .map_err(|e| to_py_err(py, e))?;
        Ok(())
    }

    /// Produce a detached [`StatementCanceller`] for the statement
    /// about to run (or running) on this `Statement`.
    ///
    /// The sync `execute()` blocks and consumes the statement, so a
    /// second thread can't reach an `ExecutedStatement` to cancel a
    /// *running* query (the default PEP-249 `cursor.cancel()` shape).
    /// Obtain a canceller **before** calling `execute()`; once the
    /// server issues the statement id mid-execute, `canceller.cancel()`
    /// reaches it. Cheap; safe to call from another thread.
    fn canceller(&self) -> PyResult<StatementCanceller> {
        let inner = alive!(self, "Statement");
        Ok(StatementCanceller {
            inner: inner.stmt.canceller(),
            runtime: inner.runtime.clone(),
        })
    }

    /// Wait-for-result execution. Blocks until the kernel finishes
    /// polling and returns an `ExecutedStatement` with the result
    /// stream ready to drain.
    ///
    /// The blocking drive is interruptible: a `KeyboardInterrupt`
    /// (Ctrl-C) raised while this is running aborts the query — the
    /// in-flight future is dropped, firing the kernel's mid-execute
    /// cancel guard so the server-side statement is cancelled too.
    fn execute(&mut self, py: Python<'_>) -> PyResult<ExecutedStatement> {
        let inner = alive_mut!(self, "Statement");
        let runtime = inner.runtime.clone();
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Statement.execute: running statement synchronously (GIL released, interruptible)"
        );
        let exec = run_kernel_interruptible!(py, &runtime, inner.stmt.execute())?;
        Ok(ExecutedStatement::new(exec, runtime))
    }

    /// Submit-and-return. Sends `ExecuteStatement` with
    /// `wait_timeout=0s` so the server returns immediately with a
    /// `statement_id`; the kernel does not poll. Returns an
    /// `ExecutedAsyncStatement` the caller drives via `status()` /
    /// `await_result()` / `fetch_result()`.
    ///
    /// Use this path when the caller needs a poll/cancel handle instead of
    /// waiting synchronously for a terminal state.
    fn submit(&mut self, py: Python<'_>) -> PyResult<ExecutedAsyncStatement> {
        let inner = alive_mut!(self, "Statement");
        let runtime = inner.runtime.clone();
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Statement.submit: submitting statement async (GIL released)"
        );
        let exec = run_kernel!(py, runtime, inner.stmt.submit())?;
        Ok(ExecutedAsyncStatement::new(exec, runtime))
    }

    /// Explicit close — cancels and closes any executed handle this
    /// `Statement` is still tracking, then drops the lifecycle
    /// owner.
    ///
    /// Matches the surface JDBC / ODBC / PySQL users expect:
    /// closing the lifecycle owner releases the associated
    /// server-side statement too. Any `ExecutedStatement` /
    /// `ExecutedAsyncStatement` the caller still holds becomes
    /// invalid; its next call short-circuits locally to
    /// `KernelError(code='InvalidStatementHandle')` without a
    /// network round-trip.
    ///
    /// Idempotent — a second `close()` call is a no-op.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        let StatementInner { stmt, runtime } = inner;
        // Kernel `Statement::close` is async (it awaits
        // CancelStatement + CloseStatement against the tracked
        // executed handle), so we route it through the standard
        // py.detach + block_on path.
        let result = run_kernel!(py, runtime, stmt.close());
        KernelRuntime::release(runtime, py);
        result
    }
}

/// Python-facing executed statement handle. Owns the kernel
/// `ExecutedStatement` and serves the result stream lazily.
#[pyclass(name = "ExecutedStatement")]
pub(crate) struct ExecutedStatement {
    inner: Option<ExecutedInner>,
    /// Cached statement id so callers can read it after `close()`
    /// (e.g. for logging the id of a just-closed statement).
    statement_id: String,
    /// Cached schema. Pulled from the kernel once on construction —
    /// available even after `close()` so descriptions stay stable.
    schema: Arc<Schema>,
    /// Rows modified by the statement (DML: INSERT/UPDATE/DELETE/MERGE),
    /// or `None` for SELECT / warehouses that don't report it. Cached at
    /// construction (it's fixed once the statement completed) so it
    /// survives `close()`, like `statement_id` / `schema`. The connector
    /// surfaces this as `cursor.rowcount`.
    num_modified_rows: Option<i64>,
    /// Whether `fetch_all_rows` converts every column type of `schema`.
    rows_supported: bool,
}

struct ExecutedInner {
    exec: KExecutedStatement,
    runtime: Arc<KernelRuntime>,
    exhausted: bool,
    /// Batches `fetch_all_rows` pulled but didn't convert; served first by
    /// the Arrow fetches.
    pending: VecDeque<RecordBatch>,
}

impl ExecutedStatement {
    fn new(exec: KExecutedStatement, runtime: Arc<KernelRuntime>) -> Self {
        let statement_id = exec.statement_id().to_owned();
        let schema = exec.schema();
        let num_modified_rows = exec.num_modified_rows();
        Self {
            statement_id,
            rows_supported: rows::schema_is_supported(&schema),
            schema,
            num_modified_rows,
            inner: Some(ExecutedInner {
                exec,
                runtime,
                exhausted: false,
                pending: VecDeque::new(),
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

    /// Rows modified by the statement (DML), or `None` for SELECT /
    /// warehouses that don't report it. The connector maps this to
    /// `cursor.rowcount` (falling back to -1 when `None`). Available
    /// after `close()` (cached at construction).
    #[getter]
    fn num_modified_rows(&self) -> Option<i64> {
        self.num_modified_rows
    }

    /// Result schema as a `pyarrow.Schema`. Available even after
    /// `close()`.
    fn arrow_schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    /// A `SchemaKey` equal to another statement's when their result schemas
    /// are equal, for caching what is derived from a schema without
    /// building a `pyarrow.Schema`. Available even after `close()`.
    fn schema_key(&self) -> SchemaKey {
        SchemaKey::new(self.schema.clone())
    }

    /// All remaining rows as a list of tuples of Python values, if there
    /// are at most `max_rows` of them and every column has a type converted
    /// here; otherwise `None`, and the next Arrow fetch returns the batches
    /// this call pulled. Values are what pyarrow's `to_pylist()` returns.
    /// NaN floats return `None` unless `nan_ok`.
    #[pyo3(signature = (max_rows, nan_ok = false))]
    fn fetch_all_rows<'py>(
        &mut self,
        py: Python<'py>,
        max_rows: usize,
        nan_ok: bool,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let inner = alive_mut!(self, "ExecutedStatement");
        if !self.rows_supported {
            return Ok(None);
        }
        if !inner.exhausted {
            let runtime = inner.runtime.clone();
            inner.exhausted = run_sync_kernel!(
                py,
                fetch::fill_exec(&runtime, &mut inner.exec, &mut inner.pending, max_rows)
            )?;
        }
        rows::take_rows(
            py,
            &self.schema,
            inner.exhausted,
            &mut inner.pending,
            max_rows,
            nan_ok,
        )
    }

    /// Pull the next batch as a `pyarrow.RecordBatch`, or `None` if
    /// the stream is exhausted.
    fn fetch_next_batch<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let inner = alive_mut!(self, "ExecutedStatement");
        if let Some(batch) = inner.pending.pop_front() {
            return Ok(Some(batch.to_pyarrow(py)?));
        }
        if inner.exhausted {
            return Ok(None);
        }
        let runtime = inner.runtime.clone();
        // Inline results and already-downloaded CloudFetch chunks are in
        // memory: take them without a GIL hand-off when reading one is only a
        // buffer hand-off. Both paths drive the ExecutedStatement, so
        // auto-close-on-drain still fires CloseStatement once exhausted.
        let ready = if !inner.exec.try_next_batch_is_cheap() {
            None
        } else {
            let _context = runtime.enter();
            match inner.exec.try_next_batch().map_err(|e| to_py_err(py, e))? {
                TryNextBatch::Ready(ResultBatch::Arrow(rb)) => Some(Some(rb.clone())),
                // Same error as the blocking path; falling back would skip it.
                TryNextBatch::Ready(_) => {
                    return Err(to_py_err(
                        py,
                        KError::invalid_argument()
                            .message("kernel returned a non-Arrow ResultBatch variant"),
                    ))
                }
                TryNextBatch::End => Some(None),
                _ => None,
            }
        };
        let batch = match ready {
            Some(batch) => batch,
            None => run_sync_kernel!(py, fetch::next_batch_exec(&runtime, &mut inner.exec))?,
        };
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
        let mut batches: Vec<RecordBatch> = inner.pending.drain(..).collect();
        if !inner.exhausted {
            let runtime = inner.runtime.clone();
            batches.extend(run_sync_kernel!(
                py,
                fetch::drain_exec(&runtime, &mut inner.exec)
            )?);
            inner.exhausted = true;
        }
        fetch::batches_to_pyarrow_table(py, &self.schema, batches)
    }

    /// Cancel the in-flight statement on the server. Local validity
    /// flag short-circuits if the parent `Statement` was re-executed
    /// or dropped.
    fn cancel(&mut self, py: Python<'_>) -> PyResult<()> {
        let inner = alive_mut!(self, "ExecutedStatement");
        // Short-circuit if the stream was fully drained: the kernel's
        // auto-close-on-drain already closed the server statement, so a
        // cancel RPC here would hit an already-terminal/closed statement
        // (a wasted round-trip, benign error at worst). Matches the
        // napi binding's `finished`-flag short-circuit and the JDBC
        // cancel-after-completion no-op contract.
        if inner.exhausted {
            return Ok(());
        }
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
        let res = if inner.exhausted {
            // Drain already fired CloseStatement, so this does no I/O.
            crate::gil::run_ready_first(py, &runtime, KExecutedStatement::close(&mut inner.exec))
                .map_err(|e| to_py_err(py, e))
        } else {
            run_kernel!(py, runtime, KExecutedStatement::close(&mut inner.exec))
        };
        // Drop the kernel handle regardless of close() outcome — the
        // kernel's Drop impl runs fire-and-forget cleanup if close()
        // failed mid-flight.
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
            "ExecutedStatement(statement_id={:?}, state={state})",
            self.statement_id
        )
    }
}

/// Detached canceller for the statement in flight on a [`Statement`].
///
/// Obtained from [`Statement::canceller`] *before* the blocking
/// `execute()` so a second Python thread can cancel a running sync
/// query — the PEP-249 `cursor.cancel()`-from-another-thread shape.
/// Holds no borrow on the `Statement`; safe to call concurrently with
/// the `execute()` it targets.
#[pyclass(name = "StatementCanceller")]
pub(crate) struct StatementCanceller {
    inner: KStatementCanceller,
    runtime: Arc<KernelRuntime>,
}

#[pymethods]
impl StatementCanceller {
    /// Server-side cancel of the in-flight statement.
    ///
    /// No-op if nothing has executed yet, or the query is still in its
    /// initial submit round-trip (no statement id observed). Idempotent
    /// against a server already in a terminal state. Releases the GIL
    /// while the cancel RPC is in flight.
    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = self.runtime.clone();
        run_kernel!(py, runtime, self.inner.cancel())?;
        Ok(())
    }

    /// The server-issued statement id this canceller targets, if one
    /// has been observed yet (`None` before the server issues it).
    fn statement_id(&self) -> Option<String> {
        self.inner.statement_id()
    }
}
