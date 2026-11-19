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
//!
//! Contended CloudFetch provider-state locks yield `Pending`, allowing the
//! fallback to wait with the GIL released.

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

fn own_arrow_batch(batch: &ResultBatch) -> Result<RecordBatch> {
    match batch {
        ResultBatch::Arrow(batch) => Ok(batch.clone()),
        _ => Err(Error::invalid_state().message("kernel returned a non-Arrow ResultBatch variant")),
    }
}

fn own_batch(read: TryNextBatch<'_>) -> Result<BatchRead> {
    match read {
        TryNextBatch::Ready(batch) => own_arrow_batch(batch).map(BatchRead::Ready),
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
        use std::io::{BufRead, BufReader, Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::sync::atomic::AtomicBool;
        use std::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopped);
        let server = std::thread::spawn(move || {
            let body = r#"{"session_id":"00000000-0000-0000-0000-000000000001","statement_id":"00000000-0000-0000-0000-000000000002","status":{"state":"SUCCEEDED"},"manifest":{"format":"ARROW_STREAM","schema":{"column_count":0,"columns":[]},"total_row_count":0,"total_chunk_count":0}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            for stream in listener.incoming() {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let mut stream = stream.unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = BufReader::new(&mut stream);
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    if request.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                    }
                }
                if request.read_exact(&mut vec![0; length]).is_ok() {
                    let _ = stream.write_all(response.as_bytes());
                }
            }
        });
        Python::initialize();
        let checked = Python::attach(|py| -> PyResult<()> {
            let kwargs = PyDict::new(py);
            kwargs.set_item("host", format!("http://{address}"))?;
            kwargs.set_item("http_path", "/sql/1.0/warehouses/0000000000000001")?;
            kwargs.set_item("access_token", "test-token")?;
            kwargs.set_item("telemetry_enabled", false)?;
            kwargs.set_item("request_timeout_secs", 2)?;
            kwargs.set_item("retry_max_attempts", 1)?;
            let session = py
                .get_type::<crate::session::Session>()
                .call((), Some(&kwargs))?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let mut checked = Ok(());
            for asynchronous in [false, true] {
                let statement = session.call_method0("statement")?;
                statement.call_method1("set_sql", ("SELECT 1",))?;
                let owner =
                    statement.call_method0(if asynchronous { "submit" } else { "execute" })?;
                let result = if asynchronous {
                    owner.call_method0("await_result")?
                } else {
                    owner.clone()
                };
                // Tokio rejects nested block_on calls, exposing an unconditional
                // runtime entry through either real Python result handle.
                checked = runtime.block_on(async {
                    assert!(result.call_method0("fetch_next_batch")?.is_none());
                    Ok(())
                });
                result.call_method0("close")?;
                if asynchronous {
                    owner.call_method0("close")?;
                }
                statement.call_method0("close")?;
                if checked.is_err() {
                    break;
                }
            }
            session.call_method0("close")?;
            checked
        });
        stopped.store(true, Ordering::Release);
        let _ = TcpStream::connect(address);
        server.join().unwrap();
        checked.unwrap();
    }
}
