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
//! - **Owned** — a `ResultStream` consumed into an `OwnedBatchIter` plus
//!   its schema. Produced by the metadata calls (`kernel_metadata_*`),
//!   which return a standalone stream not tied to an executed handle.
//! - **Executed** — a borrow of the `ExecutedStatement` that produced
//!   it. `next_batch` drives the executed handle's own stream so the
//!   handle's drain-driven auto-close and lazy CloudFetch link fetches
//!   keep working; the caller must keep the executed handle alive until
//!   the stream is closed.

#![allow(non_camel_case_types)]

use super::error::{store_and_code, store_invalid_arg};
use super::{catch_ffi, KernelStatusCode};
use crate::kernel_error::{Error, ErrorCode};
use crate::result_stream::{ResultBatch, SchemaRef};
use crate::statement::executed::ExecutedStatement;
use crate::statement::validity::ValidityFlag;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch};

/// Opaque result-stream handle. Boxed [`ResultStreamInner`].
#[repr(C)]
pub struct kernel_result_stream_t {
    _private: [u8; 0],
}

/// Backing state for a result-stream handle.
enum ResultStreamInner {
    /// Self-contained owned stream (metadata path). The batches are
    /// **fully drained on the runtime at construction** (metadata results
    /// are bounded), so iterating here is pure in-memory — no tokio
    /// reactor needed, sidestepping the off-runtime CloudFetch hang.
    Owned {
        schema: SchemaRef,
        batches: std::vec::IntoIter<RecordBatch>,
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

/// Crate-internal: build an owned result-stream handle from a schema and
/// a fully-materialised batch list (used by the metadata FFI, which
/// drains its bounded result on the runtime before calling this).
pub(crate) fn into_raw_result_stream(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> *mut kernel_result_stream_t {
    Box::into_raw(Box::new(ResultStreamInner::Owned {
        schema,
        batches: batches.into_iter(),
    })) as *mut kernel_result_stream_t
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

/// Resolve the backing `ExecutedStatement` for the `Executed` shape,
/// checking the shared validity flag first so a closed executed handle
/// yields `InvalidStatementHandle` rather than a dangling deref.
///
/// # Safety
/// Caller must hold `&mut ResultStreamInner`.
unsafe fn executed_ref<'a>(
    exec: *mut ExecutedStatement,
    valid: &ValidityFlag,
) -> Result<&'a mut ExecutedStatement, KernelStatusCode> {
    if valid.check().is_err() {
        return Err(store_and_code(Error::new(
            ErrorCode::InvalidStatementHandle,
            "result stream used after its executed statement was closed",
        )));
    }
    match exec.as_mut() {
        Some(e) => Ok(e),
        None => Err(store_invalid_arg(
            "result stream: backing executed handle is null",
        )),
    }
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
            // Owned (metadata): batches were fully drained on the runtime
            // at construction, so this is a pure in-memory pop — no tokio
            // reactor needed (avoids the off-runtime CloudFetch hang).
            ResultStreamInner::Owned { batches, .. } => batches.next().map(Ok),
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
                // with no fallback.
                match super::block_on_ffi(move || {
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

    /// End-of-stream on an Owned stream returns `Success` with a
    /// **released** array (`release == null`) — the sentinel ODBC / Go
    /// consumers loop on. A regression that returned a non-released empty
    /// array would loop them forever, so pin it.
    #[test]
    fn owned_stream_exhaustion_yields_released_array() {
        let (schema, batch) = one_row_batch();
        let stream = into_raw_result_stream(schema, vec![batch]);

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
