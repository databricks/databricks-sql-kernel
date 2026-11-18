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

//! Internal helpers for pulling batches out of a `ResultStream`.
//!
//! Shared by `ExecutedStatement` (drives the kernel `ExecutedStatement`'s
//! borrowed result stream) and `ResultStream` (owns its kernel
//! `ResultStream` directly). Both call `next_batch` on the kernel
//! stream and clone the Arrow `RecordBatch` out from under the
//! borrow so the result can cross the GIL boundary as an owned value.
//!
//! A nonblocking probe holds the GIL through kernel post-processing, so large
//! batches requiring column rewrites can delay other Python threads.

use arrow_array::RecordBatch;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Schema;
use databricks_sql_kernel::{
    Error, ExecutedStatement, Result, ResultBatch, ResultStream, TryNextBatch,
};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub(crate) enum BatchRead {
    Ready(RecordBatch),
    Pending,
    End,
}

fn own_batch(read: TryNextBatch<'_>) -> Result<BatchRead> {
    match read {
        TryNextBatch::Ready(ResultBatch::Arrow(batch)) => Ok(BatchRead::Ready(batch.clone())),
        TryNextBatch::Ready(_) => {
            Err(Error::invalid_state().message("kernel returned a non-Arrow ResultBatch variant"))
        }
        TryNextBatch::Pending => Ok(BatchRead::Pending),
        TryNextBatch::End => Ok(BatchRead::End),
        _ => Err(Error::invalid_state().message("kernel returned an unknown batch-read state")),
    }
}

pub(crate) fn try_next_batch(stream: &mut ResultStream) -> Result<BatchRead> {
    own_batch(stream.try_next_batch()?)
}

pub(crate) fn try_next_batch_exec(exec: &mut ExecutedStatement) -> Result<BatchRead> {
    own_batch(exec.try_next_batch()?)
}

pub(crate) fn read_batch(
    py: Python<'_>,
    read: Result<BatchRead>,
    pending: impl FnOnce() -> Result<Option<RecordBatch>> + pyo3::marker::Ungil,
) -> PyResult<Option<RecordBatch>> {
    match read.map_err(|error| crate::errors::to_py_err(py, error))? {
        BatchRead::Ready(batch) => Ok(Some(batch)),
        BatchRead::End => Ok(None),
        BatchRead::Pending => py
            .detach(pending)
            .map_err(|error| crate::errors::to_py_err(py, error)),
    }
}

/// Pull the next batch from the kernel stream and return it as an
/// owned `RecordBatch`, or `None` if the stream is exhausted.
///
/// Caller is responsible for holding `py.detach(...)` open around
/// the call — this function does kernel I/O.
pub(crate) fn next_batch(
    runtime: &Runtime,
    stream: &mut ResultStream,
) -> Result<Option<RecordBatch>> {
    runtime.block_on(async {
        match stream.next_batch().await? {
            Some(ResultBatch::Arrow(rb)) => Ok(Some(rb.clone())),
            Some(_) => Err(Error::invalid_argument()
                .message("kernel returned a non-Arrow ResultBatch variant")),
            None => Ok(None),
        }
    })
}

/// Drain the kernel stream and collect every remaining batch.
pub(crate) fn drain(runtime: &Runtime, stream: &mut ResultStream) -> Result<Vec<RecordBatch>> {
    runtime.block_on(async {
        let mut out = Vec::new();
        loop {
            match stream.next_batch().await? {
                Some(ResultBatch::Arrow(rb)) => out.push(rb.clone()),
                Some(_) => {
                    return Err(Error::invalid_argument()
                        .message("kernel returned a non-Arrow ResultBatch variant"));
                }
                None => break,
            }
        }
        Ok(out)
    })
}

/// Like [`next_batch`], but drives the `ExecutedStatement` (not a bare
/// `ResultStream`) so the kernel's auto-close-on-drain fires when the
/// stream is exhausted. Used by `ExecutedStatement.fetch_next_batch`.
pub(crate) fn next_batch_exec(
    runtime: &Runtime,
    exec: &mut ExecutedStatement,
) -> Result<Option<RecordBatch>> {
    runtime.block_on(async {
        match exec.next_batch().await? {
            Some(ResultBatch::Arrow(rb)) => Ok(Some(rb.clone())),
            Some(_) => Err(Error::invalid_argument()
                .message("kernel returned a non-Arrow ResultBatch variant")),
            None => Ok(None),
        }
    })
}

/// Like [`drain`], but drives the `ExecutedStatement` so auto-close-on-
/// drain fires once the stream is exhausted. Used by
/// `ExecutedStatement.fetch_all_arrow`.
pub(crate) fn drain_exec(
    runtime: &Runtime,
    exec: &mut ExecutedStatement,
) -> Result<Vec<RecordBatch>> {
    runtime.block_on(async {
        let mut out = Vec::new();
        loop {
            match exec.next_batch().await? {
                Some(ResultBatch::Arrow(rb)) => out.push(rb.clone()),
                Some(_) => {
                    return Err(Error::invalid_argument()
                        .message("kernel returned a non-Arrow ResultBatch variant"));
                }
                None => break,
            }
        }
        Ok(out)
    })
}

/// Wrap a Vec of `RecordBatch` into a `pyarrow.Table`. Centralised
/// so both `ExecutedStatement.fetch_all_arrow` and
/// `ResultStream.fetch_all_arrow` produce identical Python objects.
pub(crate) fn batches_to_pyarrow_table<'py>(
    py: Python<'py>,
    schema: &Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> PyResult<Bound<'py, PyAny>> {
    let py_batches: Vec<Bound<'py, PyAny>> = batches
        .into_iter()
        .map(|b| b.to_pyarrow(py))
        .collect::<PyResult<_>>()?;
    let py_schema = schema.as_ref().to_pyarrow(py)?;
    let pa = py.import("pyarrow")?;
    pa.getattr("Table")?
        .call_method1("from_batches", (py_batches, py_schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn only_pending_reads_enter_the_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let entries = AtomicUsize::new(0);
        let pending = || {
            entries.fetch_add(1, Ordering::Relaxed);
            runtime.block_on(async { Ok(None) })
        };
        Python::initialize();
        Python::attach(|py| {
            let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
            let ready = read_batch(py, Ok(BatchRead::Ready(batch.clone())), pending).unwrap();
            assert_eq!(ready, Some(batch));
            assert!(read_batch(py, Ok(BatchRead::End), pending)
                .unwrap()
                .is_none());
            assert_eq!(entries.load(Ordering::Relaxed), 0);

            assert!(read_batch(py, Ok(BatchRead::Pending), pending)
                .unwrap()
                .is_none());
            assert_eq!(entries.load(Ordering::Relaxed), 1);
        });
    }
}
