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

//! Result FFI — exports a result stream via the Arrow C Data Interface.
//!
//! `kernel_result_stream_get_schema` / `kernel_result_stream_next_batch`
//! hand back `FFI_ArrowSchema` / `FFI_ArrowArray` (the Arrow C Data
//! Interface) so any C consumer with an Arrow C Data Interface
//! implementation can iterate. The kernel yields **one array per
//! `next_batch` call** (a pull model); consumers that want a
//! self-contained `ArrowArrayStream` wrap this surface on their side.
//!
//! Two stream shapes back the same opaque handle:
//!
//! - **Owned** — a standalone `ResultStream` the handle owns outright.
//!   Produced by the metadata calls (`kernel_metadata_*`), which return a
//!   stream not tied to an executed handle. `next_batch` drives it lazily
//!   on the runtime (via `block_on_ffi`), one batch at a time, so a large
//!   result (e.g. `list_columns` with no catalog filter → SHOW COLUMNS IN
//!   ALL CATALOGS) streams with bounded memory instead of being buffered
//!   up front.
//! - **Executed** — a borrow of the `ExecutedStatement` that produced
//!   it. `next_batch` drives the executed handle's own stream so the
//!   handle's drain-driven auto-close and lazy CloudFetch link fetches
//!   keep working; the caller must keep the executed handle alive until
//!   the stream is closed.

#![allow(non_camel_case_types)]

use super::cancel::{kernel_cancel_token_t, token_ref};
use super::error::{store_and_code, store_invalid_arg};
use super::{catch_ffi, KernelStatusCode};
use crate::kernel_error::{Error, ErrorCode};
use crate::result_stream::{ResultBatch, SchemaRef};
use crate::statement::executed::ExecutedStatement;
use crate::statement::validity::ValidityFlag;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch};
use tokio_util::sync::CancellationToken;

/// Opaque result-stream handle. Boxed [`ResultStreamInner`].
#[repr(C)]
pub struct kernel_result_stream_t {
    _private: [u8; 0],
}

/// Backing state for a result-stream handle.
enum ResultStreamInner {
    /// Self-contained owned stream (metadata path). The handle owns the
    /// `ResultStream` and drives `next_batch` lazily on the runtime (via
    /// `block_on_ffi`) — one batch per C call — so an unbounded metadata
    /// result (e.g. cross-catalog column listing) streams with bounded
    /// memory rather than being drained into a `Vec` at construction.
    Owned {
        schema: SchemaRef,
        stream: crate::result_stream::ResultStream,
    },
    /// Borrow of the executed statement that produced this stream
    /// (sync execute path). The raw pointer is only dereferenced after
    /// `valid.check()` passes; closing the executed handle invalidates
    /// `valid`, so an out-of-order close is a defined
    /// `InvalidStatementHandle` error, not a use-after-free.
    Executed {
        exec: *mut ExecutedStatement,
        valid: ValidityFlag,
    },
}

/// Crate-internal: build an owned result-stream handle that lazily drives
/// the given `ResultStream` (used by the metadata FFI). `schema` is
/// captured up front so `get_schema` needs no runtime; each `next_batch`
/// pulls one batch on the runtime.
pub(crate) fn into_raw_result_stream(
    schema: SchemaRef,
    stream: crate::result_stream::ResultStream,
) -> *mut kernel_result_stream_t {
    Box::into_raw(Box::new(ResultStreamInner::Owned { schema, stream }))
        as *mut kernel_result_stream_t
}

/// Crate-internal: build a result-stream handle that borrows from an
/// executed statement (used by the sync execute path). `valid` is shared
/// with the executed handle and invalidated when it is closed.
///
/// # Safety
/// `exec` must point at the executed handle's `ExecutedStatement` and
/// remain valid as long as `valid.check()` passes.
pub(crate) unsafe fn into_raw_executed_stream(
    exec: *mut ExecutedStatement,
    valid: ValidityFlag,
) -> *mut kernel_result_stream_t {
    Box::into_raw(Box::new(ResultStreamInner::Executed { exec, valid }))
        as *mut kernel_result_stream_t
}

/// Resolve the backing `ExecutedStatement` for the `Executed` shape.
///
/// Two independent stale-handle guards, checked in this order so the
/// pointer is only dereferenced once it is known to be live (either
/// failing yields `InvalidStatementHandle` rather than UB):
///
/// 1. `alive` (shared with the executed handle, invalidated on its
///    `close`) — checked BEFORE the deref, because a closed handle has
///    been freed and the pointer would dangle.
/// 2. `ExecutedStatement::is_valid()` (the flag shared with the parent
///    statement, flipped when that statement is re-executed OR closed) —
///    checked AFTER the deref, since it lives on the (now known-live)
///    object. This is why the parent statement must outlive the stream.
///
/// # Safety
/// Caller must hold `&mut ResultStreamInner`.
unsafe fn executed_ref<'a>(
    exec: *mut ExecutedStatement,
    alive: &ValidityFlag,
) -> Result<&'a mut ExecutedStatement, KernelStatusCode> {
    if alive.check().is_err() {
        return Err(store_and_code(Error::new(
            ErrorCode::InvalidStatementHandle,
            "result stream used after its executed statement was closed",
        )));
    }
    let Some(e) = exec.as_mut() else {
        return Err(store_invalid_arg(
            "result stream: backing executed handle is null",
        ));
    };
    if !e.is_valid() {
        return Err(store_and_code(Error::new(
            ErrorCode::InvalidStatementHandle,
            "result stream's executed statement was invalidated by a re-execute or close on its \
             parent statement; drain/close the stream before closing the statement",
        )));
    }
    Ok(e)
}

/// Export the schema as an `FFI_ArrowSchema`.
///
/// On success, `*out` holds an `FFI_ArrowSchema` the caller releases via
/// the schema's own release callback (per the Arrow C Data Interface
/// contract). Safe to call before or between `next_batch` calls.
///
/// # Safety
/// `stream` must be a live result-stream handle; `out` must be a valid,
/// writable pointer to an `FFI_ArrowSchema`.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_get_schema(
    stream: *mut kernel_result_stream_t,
    out: *mut FFI_ArrowSchema,
) -> KernelStatusCode {
    catch_ffi(|| {
        let Some(inner) = (stream as *mut ResultStreamInner).as_mut() else {
            return store_invalid_arg("kernel_result_stream_get_schema: stream must not be null");
        };
        if out.is_null() {
            return store_invalid_arg("kernel_result_stream_get_schema: out must not be null");
        }
        let schema = match inner {
            ResultStreamInner::Owned { schema, .. } => schema.clone(),
            ResultStreamInner::Executed { exec, valid } => match executed_ref(*exec, valid) {
                Ok(e) => e.schema(),
                Err(code) => return code,
            },
        };
        match FFI_ArrowSchema::try_from(schema.as_ref()) {
            Ok(ffi_schema) => {
                std::ptr::write(out, ffi_schema);
                KernelStatusCode::Success
            }
            Err(e) => store_and_code(Error::from(e)),
        }
    })
}

/// Export the next batch as an `FFI_ArrowArray` (+ its `FFI_ArrowSchema`).
///
/// When the stream is exhausted, returns `KernelStatusCode::Success` and
/// writes a **released-state** (empty) array to `*out_array` — the
/// consumer detects end-of-stream by checking `out_array->release ==
/// NULL` (the Arrow C Data Interface convention for a released array).
///
/// # Safety
/// `stream` must be a live result-stream handle; `out_array` and
/// `out_schema` must be valid, writable pointers.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_next_batch(
    stream: *mut kernel_result_stream_t,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> KernelStatusCode {
    next_batch_inner(stream, out_array, out_schema, None)
}

/// Fetch the next batch, racing the (blocking) fetch against `cancel_token` so
/// a host can interrupt a hung CloudFetch chunk (a wedged S3 / pre-signed-URL
/// GET) instead of blocking on it forever. A **null** `cancel_token` behaves
/// exactly like [`kernel_result_stream_next_batch`].
///
/// If the token fires before the batch is ready, the fetch future is dropped
/// (aborting the in-flight download) and this returns
/// [`KernelStatusCode::Cancelled`]. The stream handle stays valid and owned by
/// the caller — close it with [`kernel_result_stream_close`] as usual; a
/// cancelled fetch does not consume or free it.
///
/// # Safety
/// `stream`, `out_array`, `out_schema` carry the same contract as
/// [`kernel_result_stream_next_batch`]. `cancel_token`, if non-null, must be a
/// live handle from
/// [`kernel_cancel_token_new`](crate::c_abi::cancel::kernel_cancel_token_new)
/// not freed until this call returns; it is safe to fire from another thread
/// while this call is in flight.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_next_batch_cancellable(
    stream: *mut kernel_result_stream_t,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    cancel_token: *const kernel_cancel_token_t,
) -> KernelStatusCode {
    next_batch_inner(stream, out_array, out_schema, token_ref(cancel_token))
}

/// Shared body for the two next-batch entry points. `token` (None for the plain
/// one) is raced against the fetch via `block_on_ffi_cancellable`.
///
/// # Safety
/// See [`kernel_result_stream_next_batch`]. `token` must outlive the call when
/// `Some`.
unsafe fn next_batch_inner(
    stream: *mut kernel_result_stream_t,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    token: Option<&CancellationToken>,
) -> KernelStatusCode {
    catch_ffi(|| {
        let Some(inner) = (stream as *mut ResultStreamInner).as_mut() else {
            return store_invalid_arg("kernel_result_stream_next_batch: stream must not be null");
        };
        if out_array.is_null() || out_schema.is_null() {
            return store_invalid_arg(
                "kernel_result_stream_next_batch: out pointers must not be null",
            );
        }

        // Pull the next owned batch from whichever backing shape this is.
        let next: Option<Result<RecordBatch, Error>> = match inner {
            // Owned (metadata): drive the owned ResultStream lazily on the
            // runtime, one batch at a time, so an unbounded result streams
            // with bounded memory. Running through block_on_ffi_cancellable
            // keeps the (possibly CloudFetch-backed) reader on the tokio
            // reactor rather than off a plain C thread, and lets a fired token
            // drop the fetch future.
            ResultStreamInner::Owned { stream, .. } => {
                match super::block_on_ffi_cancellable(token, move || {
                    let stream: &mut crate::result_stream::ResultStream = stream;
                    Box::pin(async move {
                        #[allow(
                            unreachable_patterns,
                            reason = "forward-compat fallback for future #[non_exhaustive] ResultBatch variants"
                        )]
                        match stream.next_batch().await? {
                            Some(ResultBatch::Arrow(rb)) => Ok(Some(rb.clone())),
                            Some(_) => Err(Error::not_implemented().message(
                                "metadata stream yielded a non-Arrow batch; the C ABI exports \
                                 Arrow only",
                            )),
                            None => Ok(None),
                        }
                    })
                }) {
                    Ok(Some(rb)) => Some(Ok(rb)),
                    Ok(None) => None,
                    Err(code) => return code, // error already stored
                }
            }
            ResultStreamInner::Executed { exec, valid } => {
                let exec = match executed_ref(*exec, valid) {
                    Ok(e) => e,
                    Err(code) => return code,
                };
                // Drive the executed handle's own next_batch so the
                // drain-driven auto-close and lazy CloudFetch link fetches
                // keep working. Convert the borrowed batch into an owned
                // RecordBatch for FFI export. Explicit match on the
                // #[non_exhaustive] ResultBatch so a future non-Arrow
                // variant surfaces as a defined error, not a compile break
                // with no fallback. Cancellable so a hung CloudFetch chunk on
                // the query path can be interrupted.
                match super::block_on_ffi_cancellable(token, move || {
                    let exec: &mut ExecutedStatement = exec;
                    Box::pin(async move {
                        #[allow(
                            unreachable_patterns,
                            reason = "forward-compat fallback for future #[non_exhaustive] ResultBatch variants"
                        )]
                        match exec.next_batch().await? {
                            Some(ResultBatch::Arrow(rb)) => Ok(Some(rb.clone())),
                            Some(_) => Err(Error::not_implemented().message(
                                "result stream yielded a non-Arrow batch; the C ABI exports \
                                 Arrow only",
                            )),
                            None => Ok(None),
                        }
                    })
                }) {
                    Ok(Some(rb)) => Some(Ok(rb)),
                    Ok(None) => None,
                    Err(code) => return code, // error already stored
                }
            }
        };

        match next {
            None => {
                // End-of-stream: hand back a released (empty) array. A
                // default `FFI_ArrowArray` has a null release callback,
                // which the Arrow C Data Interface treats as "released".
                std::ptr::write(out_array, FFI_ArrowArray::empty());
                std::ptr::write(out_schema, FFI_ArrowSchema::empty());
                KernelStatusCode::Success
            }
            Some(Ok(batch)) => {
                // A RecordBatch maps to a StructArray for the C Data
                // Interface export (the Arrow-canonical batch-as-array form).
                let struct_array: arrow_array::StructArray = batch.into();
                match arrow_array::ffi::to_ffi(&struct_array.into_data()) {
                    Ok((ffi_array, ffi_schema)) => {
                        std::ptr::write(out_array, ffi_array);
                        std::ptr::write(out_schema, ffi_schema);
                        KernelStatusCode::Success
                    }
                    Err(e) => store_and_code(Error::from(e)),
                }
            }
            Some(Err(e)) => store_and_code(e),
        }
    })
}

/// Free client-side resources held by the stream. Local-only; issues no
/// server RPC. For the **executed** shape this frees only the borrow
/// wrapper, not the executed handle itself (close that separately via
/// `kernel_executed_statement_close`).
///
/// # Safety
/// `stream` must have been returned by a `kernel_*` call that produces a
/// result stream and not previously closed.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_close(
    stream: *mut kernel_result_stream_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    if stream.is_null() {
        return store_invalid_arg("kernel_result_stream_close: stream must not be null");
    }
    drop(Box::from_raw(stream as *mut ResultStreamInner));
    KernelStatusCode::Success
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn one_row_batch() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![7]))])
            .unwrap();
        (schema, batch)
    }

    /// Build an Owned result-stream handle over the given in-memory
    /// batches (mirrors the metadata FFI's lazy `ResultStream`-backed
    /// handle) using a throwaway runtime for the async `from_reader`.
    fn owned_stream_over(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> *mut kernel_result_stream_t {
        use crate::reader::test_utils::MockReader;
        use crate::result_stream::ResultStream;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stream = rt.block_on(async {
            ResultStream::from_reader(Box::new(MockReader::new(batches)))
                .await
                .unwrap()
        });
        into_raw_result_stream(schema, stream)
    }

    /// End-of-stream on an Owned stream returns `Success` with a
    /// **released** array (`release == null`) — the sentinel ODBC / Go
    /// consumers loop on. A regression that returned a non-released empty
    /// array would loop them forever, so pin it.
    #[test]
    fn owned_stream_exhaustion_yields_released_array() {
        let (schema, batch) = one_row_batch();
        let stream = owned_stream_over(schema, vec![batch]);

        // First call: a real batch (release non-null).
        let mut arr = FFI_ArrowArray::empty();
        let mut sch = FFI_ArrowSchema::empty();
        let rc = unsafe { kernel_result_stream_next_batch(stream, &mut arr, &mut sch) };
        assert_eq!(rc, KernelStatusCode::Success);
        assert!(!arr.is_released(), "first batch should be a live array");

        // Second call: exhausted → Success + released array.
        let mut arr2 = FFI_ArrowArray::empty();
        let mut sch2 = FFI_ArrowSchema::empty();
        let rc2 = unsafe { kernel_result_stream_next_batch(stream, &mut arr2, &mut sch2) };
        assert_eq!(rc2, KernelStatusCode::Success);
        assert!(
            arr2.is_released(),
            "exhausted stream must yield a released array"
        );

        unsafe { kernel_result_stream_close(stream) };
    }

    /// Null-pointer guards return `InvalidArgument`, never crash.
    #[test]
    fn null_guards_return_invalid_argument() {
        let mut arr = FFI_ArrowArray::empty();
        let mut sch = FFI_ArrowSchema::empty();
        let rc =
            unsafe { kernel_result_stream_next_batch(std::ptr::null_mut(), &mut arr, &mut sch) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);

        let rc = unsafe { kernel_result_stream_get_schema(std::ptr::null_mut(), &mut sch) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);

        let rc = unsafe { kernel_result_stream_close(std::ptr::null_mut()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
    }
}
