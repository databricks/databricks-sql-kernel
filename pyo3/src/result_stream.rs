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
//! Returned by metadata calls and asynchronous statement reads. Server cleanup
//! belongs to the metadata close guard or the asynchronous statement owner.

use crate::fetch;
use crate::runtime::KernelRuntime;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Schema;
use databricks_sql_kernel::ResultStream as KResultStream;
use pyo3::prelude::*;
use std::sync::Arc;

/// Python-facing borrowed result stream.
#[pyclass(name = "ResultStream")]
pub(crate) struct ResultStream {
    inner: Option<ResultStreamInner>,
    /// Cached schema — survives `close()` so callers can still read
    /// the description after exhausting / closing the stream.
    schema: Arc<Schema>,
}

struct ResultStreamInner {
    stream: KResultStream,
    runtime: Arc<KernelRuntime>,
    exhausted: bool,
}

impl ResultStream {
    pub(crate) fn new(stream: KResultStream, runtime: Arc<KernelRuntime>) -> Self {
        let schema = stream.schema();
        Self {
            schema,
            inner: Some(ResultStreamInner {
                stream,
                runtime,
                exhausted: false,
            }),
        }
    }
}

#[pyclass(eq, skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeCompletion {
    NotVolume,
    NeedsInputStream,
    Completed,
}

impl From<databricks_sql_kernel::VolumeCompletion> for VolumeCompletion {
    fn from(value: databricks_sql_kernel::VolumeCompletion) -> Self {
        match value {
            databricks_sql_kernel::VolumeCompletion::NotVolume => Self::NotVolume,
            databricks_sql_kernel::VolumeCompletion::NeedsInputStream => Self::NeedsInputStream,
            databricks_sql_kernel::VolumeCompletion::Completed => Self::Completed,
        }
    }
}

#[pymethods]
impl ResultStream {
    #[getter]
    fn is_volume_operation(&self) -> PyResult<bool> {
        let inner = alive!(self, "ResultStream");
        Ok(inner.stream.is_volume_operation())
    }

    #[pyo3(signature = (allowed_paths, remove_not_found_is_success=false, input_bytes=None))]
    fn complete_volume_operation(
        &mut self,
        py: Python<'_>,
        allowed_paths: Vec<String>,
        remove_not_found_is_success: bool,
        input_bytes: Option<Vec<u8>>,
    ) -> PyResult<crate::result_stream::VolumeCompletion> {
        let inner = alive_mut!(self, "ResultStream");
        let runtime = inner.runtime.clone();
        let options = databricks_sql_kernel::VolumeOptions {
            allowed_paths: allowed_paths.into_iter().map(Into::into).collect(),
            remove_not_found_is_success,
        };
        let stream = &mut inner.stream;
        let outcome = run_kernel_interruptible!(
            py,
            &runtime,
            stream.complete_volume_operation(&options, input_bytes)
        )?;
        Ok(outcome.into())
    }

    /// Result schema as a `pyarrow.Schema`. Available even after `close()`.
    fn arrow_schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    /// Pull the next batch as a `pyarrow.RecordBatch`, or `None` when
    /// exhausted.
    fn fetch_next_batch<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let inner = alive_mut!(self, "ResultStream");
        if inner.exhausted {
            return Ok(None);
        }
        let batch = fetch::read_batch(py, fetch::own_batch(inner.stream.try_next_batch()), || {
            fetch::next_batch(&inner.runtime, &mut inner.stream)
        })?;
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
        let runtime = inner.runtime.clone();
        let batches = run_sync_kernel!(py, fetch::drain(&runtime, &mut inner.stream))?;
        inner.exhausted = true;
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
