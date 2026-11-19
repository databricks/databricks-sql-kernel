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

//! Shared batch ownership and GIL handoff for both Python result handles.
//!
//! A nonblocking probe holds the GIL through kernel post-processing, so large
//! batches requiring column rewrites can delay other Python threads.
//!
//! Contended CloudFetch provider-state locks yield `Pending`, allowing the
//! fallback to wait with the GIL released.

use arrow_array::RecordBatch;
use arrow_pyarrow::ToPyArrow;
use arrow_schema::Schema;
use databricks_sql_kernel::reader::TryNextBatch as BatchRead;
use databricks_sql_kernel::{
    Error, ExecutedStatement, Result, ResultBatch, ResultStream, TryNextBatch,
};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn own_arrow_batch(batch: &ResultBatch) -> Result<RecordBatch> {
    match batch {
        ResultBatch::Arrow(batch) => Ok(batch.clone()),
        _ => {
            Err(Error::invalid_argument()
                .message("kernel returned a non-Arrow ResultBatch variant"))
        }
    }
}

pub(crate) fn own_batch(read: Result<TryNextBatch<'_>>) -> Result<BatchRead> {
    match read? {
        TryNextBatch::Ready(batch) => own_arrow_batch(batch).map(BatchRead::Ready),
        TryNextBatch::Pending => Ok(BatchRead::Pending),
        TryNextBatch::End => Ok(BatchRead::End),
        _ => Err(Error::invalid_state().message("kernel returned an unknown batch-read state")),
    }
}

pub(crate) fn read_batch(
    py: Python<'_>,
    read: Result<BatchRead>,
    pending: impl FnOnce() -> Result<Option<RecordBatch>> + pyo3::marker::Ungil,
) -> PyResult<Option<RecordBatch>> {
    let result = match read.map_err(|error| crate::errors::to_py_err(py, error))? {
        BatchRead::Ready(batch) => Ok(Some(batch)),
        BatchRead::End => Ok(None),
        BatchRead::Pending => py.detach(pending),
        _ => Err(Error::invalid_state().message("kernel returned an unknown batch-read state")),
    };
    result.map_err(|error| crate::errors::to_py_err(py, error))
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
    runtime.block_on(async { stream.next_batch().await?.map(own_arrow_batch).transpose() })
}

/// Drain the kernel stream and collect every remaining batch.
pub(crate) fn drain(runtime: &Runtime, stream: &mut ResultStream) -> Result<Vec<RecordBatch>> {
    runtime.block_on(async {
        let mut out = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            out.push(own_arrow_batch(batch)?);
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
    runtime.block_on(async { exec.next_batch().await?.map(own_arrow_batch).transpose() })
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
        while let Some(batch) = exec.next_batch().await? {
            out.push(own_arrow_batch(batch)?);
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
            let (sender, receiver) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                Python::attach(|_| {
                    let _ = sender.send(());
                });
            });
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("pending fallback held the GIL");
            worker.join().unwrap();
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

    #[test]
    fn real_result_handles_return_eof_without_runtime_entry() {
        use pyo3::types::PyDict;

        Python::initialize();
        Python::attach(|py| {
            let globals = PyDict::new(py);
            globals
                .set_item("Session", py.get_type::<crate::session::Session>())
                .unwrap();
            let fixture = std::ffi::CString::new(include_str!("../tests/sql_api.py")).unwrap();
            py.run(&fixture, Some(&globals), None).unwrap();
            py.run(
                c"from contextlib import ExitStack\nresources = ExitStack()",
                Some(&globals),
                None,
            )
            .unwrap();
            let checked =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PyResult<()> {
                    py.run(
                        c"
state = resources.enter_context(sql_api(Session))
results = []
for asynchronous in (False, True):
    statement = state.session.statement()
    resources.callback(statement.close)
    statement.set_sql(\"SELECT 1\")
    owner = statement.submit() if asynchronous else statement.execute()
    resources.callback(owner.close)
    result = owner.await_result() if asynchronous else owner
    resources.callback(result.close)
    results.append(result)
",
                        Some(&globals),
                        None,
                    )?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    // Tokio rejects nested block_on calls through either real handle.
                    runtime.block_on(async {
                        py.run(
                            c"for result in results:\n    assert result.fetch_next_batch() is None",
                            Some(&globals),
                            None,
                        )
                    })
                }));
            let cleanup = py.run(c"resources.close()", Some(&globals), None);
            checked.unwrap().unwrap();
            cleanup.unwrap();
        });
    }
}
