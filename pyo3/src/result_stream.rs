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

//! `ResultStream` — Python class wrapping a kernel `ResultStream`.
//!
//! Returned by `Session.metadata().list_*` calls and (in future)
//! anywhere the kernel hands back a bare `ResultStream` without an
//! enclosing `ExecutedStatement`. Identical fetch surface to
//! `ExecutedStatement` (Arrow batches, drain-to-Table), minus the
//! statement_id / cancel hooks that only make sense for an executed
//! statement.
//!
//! The actual batch-pulling logic lives in `fetch::*` helpers so
//! `ExecutedStatement::fetch_next_batch` / `fetch_all_arrow` can call
//! the same code path through its `result_stream_mut()` borrow.

use crate::fetch;
use crate::rows::{self, SchemaKey};
use crate::runtime::KernelRuntime;
use arrow_array::RecordBatch;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Schema;
use databricks_sql_kernel::ResultStream as KResultStream;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::VecDeque;
use std::sync::Arc;

/// Python-facing borrowed result stream.
#[pyclass(name = "ResultStream")]
pub(crate) struct ResultStream {
    inner: Option<ResultStreamInner>,
    /// Cached schema — survives `close()` so callers can still read
    /// the description after exhausting / closing the stream.
    schema: Arc<Schema>,
    /// Whether `fetch_all_rows` converts every column type of `schema`.
    rows_supported: bool,
}

struct ResultStreamInner {
    stream: KResultStream,
    runtime: Arc<KernelRuntime>,
    exhausted: bool,
    /// Batches `fetch_all_rows` pulled but didn't convert; served first by
    /// the Arrow fetches.
    pending: VecDeque<RecordBatch>,
}

impl ResultStream {
    pub(crate) fn new(stream: KResultStream, runtime: Arc<KernelRuntime>) -> Self {
        let schema = stream.schema();
        Self {
            rows_supported: rows::schema_is_supported(&schema),
            schema,
            inner: Some(ResultStreamInner {
                stream,
                runtime,
                exhausted: false,
                pending: VecDeque::new(),
            }),
        }
    }
}

#[pymethods]
impl ResultStream {
    /// Result schema as a `pyarrow.Schema`. Available even after `close()`.
    fn arrow_schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    /// See `ExecutedStatement.schema_key`.
    fn schema_key(&self) -> SchemaKey {
        SchemaKey::new(self.schema.clone())
    }

    /// See `ExecutedStatement.fetch_all_rows`.
    #[pyo3(signature = (max_rows, nan_ok = false))]
    fn fetch_all_rows<'py>(
        &mut self,
        py: Python<'py>,
        max_rows: usize,
        nan_ok: bool,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let inner = alive_mut!(self, "ResultStream");
        if !self.rows_supported {
            return Ok(None);
        }
        // No GIL hand-off when an earlier call already read more than fits.
        if !inner.exhausted && rows::pending_rows(&inner.pending) <= max_rows {
            let runtime = inner.runtime.clone();
            inner.exhausted = run_sync_kernel!(
                py,
                fetch::fill(&runtime, &mut inner.stream, &mut inner.pending, max_rows)
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

    /// Pull the next batch as a `pyarrow.RecordBatch`, or `None` when
    /// exhausted.
    fn fetch_next_batch<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let inner = alive_mut!(self, "ResultStream");
        if let Some(batch) = inner.pending.pop_front() {
            return Ok(Some(batch.to_pyarrow(py)?));
        }
        if inner.exhausted {
            return Ok(None);
        }
        let runtime = inner.runtime.clone();
        let batch = run_sync_kernel!(py, fetch::next_batch(&runtime, &mut inner.stream))?;
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
        let inner = alive_mut!(self, "ResultStream");
        let mut drained = Vec::new();
        if !inner.exhausted {
            let runtime = inner.runtime.clone();
            drained = run_sync_kernel!(py, fetch::drain(&runtime, &mut inner.stream))?;
            inner.exhausted = true;
        }
        // `pending` stays put if the drain fails, for the next fetch.
        let batches: Vec<RecordBatch> = inner.pending.drain(..).chain(drained).collect();
        fetch::batches_to_pyarrow_table(py, &self.schema, batches)
    }

    /// Release client-side resources. Idempotent. Subsequent calls
    /// on this handle raise `KernelError`.
    fn close(&mut self, py: Python<'_>) {
        if let Some(inner) = self.inner.take() {
            // ResultStream::close() is sync (no server RPC) and
            // consumes self; the kernel's Drop performs the same
            // cleanup, but the explicit call is cheaper to reason
            // about.
            inner.stream.close();
            KernelRuntime::release(inner.runtime, py);
        }
    }

    fn __repr__(&self) -> String {
        let state = if self.inner.is_some() {
            "open"
        } else {
            "closed"
        };
        format!("ResultStream(state={state})")
    }
}
