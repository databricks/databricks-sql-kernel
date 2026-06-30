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

use super::KernelStatusCode;
use crate::kernel_error::Error;
use crate::result_stream::{OwnedBatchIter, ResultBatch, SchemaRef};
use crate::statement::executed::ExecutedStatement;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::Array;

/// Opaque result-stream handle. Boxed [`ResultStreamInner`].
#[repr(C)]
pub struct kernel_result_stream_t {
    _private: [u8; 0],
}

/// Backing state for a result-stream handle.
enum ResultStreamInner {
    /// Self-contained owned stream (metadata path).
    Owned {
        schema: SchemaRef,
        iter: OwnedBatchIter,
    },
    /// Borrow of the executed statement that produced this stream
    /// (sync execute path). The pointer is valid as long as the
    /// executed handle is — the C consumer must not close the executed
    /// handle before closing this stream.
    Executed { exec: *mut ExecutedStatement },
}

/// Crate-internal: build an owned result-stream handle from a schema and
/// an owned batch iterator (used by the metadata FFI).
pub(crate) fn into_raw_result_stream(
    schema: SchemaRef,
    iter: OwnedBatchIter,
) -> *mut kernel_result_stream_t {
    Box::into_raw(Box::new(ResultStreamInner::Owned { schema, iter }))
        as *mut kernel_result_stream_t
}

/// Crate-internal: build a result-stream handle that borrows from an
/// executed statement (used by the sync execute path).
///
/// # Safety
/// `exec` must remain valid for the lifetime of the returned stream
/// handle.
pub(crate) unsafe fn into_raw_executed_stream(
    exec: *mut ExecutedStatement,
) -> *mut kernel_result_stream_t {
    Box::into_raw(Box::new(ResultStreamInner::Executed { exec })) as *mut kernel_result_stream_t
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
    crate::c_abi::error::clear_error();
    let Some(inner) = (stream as *mut ResultStreamInner).as_mut() else {
        return store_null("kernel_result_stream_get_schema: stream must not be null");
    };
    if out.is_null() {
        return store_null("kernel_result_stream_get_schema: out must not be null");
    }
    let schema = match inner {
        ResultStreamInner::Owned { schema, .. } => schema.clone(),
        ResultStreamInner::Executed { exec } => {
            let Some(exec) = exec.as_ref() else {
                return store_null(
                    "kernel_result_stream_get_schema: backing executed handle is null",
                );
            };
            exec.schema()
        }
    };
    match FFI_ArrowSchema::try_from(schema.as_ref()) {
        Ok(ffi_schema) => {
            std::ptr::write(out, ffi_schema);
            KernelStatusCode::Success
        }
        Err(e) => store_error(Error::from(e)),
    }
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
    crate::c_abi::error::clear_error();
    let Some(inner) = (stream as *mut ResultStreamInner).as_mut() else {
        return store_null("kernel_result_stream_next_batch: stream must not be null");
    };
    if out_array.is_null() || out_schema.is_null() {
        return store_null("kernel_result_stream_next_batch: out pointers must not be null");
    }

    // Pull the next owned batch from whichever backing shape this is.
    let next = match inner {
        ResultStreamInner::Owned { iter, .. } => iter.next(),
        ResultStreamInner::Executed { exec } => {
            let Some(exec) = exec.as_mut() else {
                return store_null(
                    "kernel_result_stream_next_batch: backing executed handle is null",
                );
            };
            // Drive the executed handle's own next_batch so the
            // drain-driven auto-close and lazy CloudFetch link fetches
            // keep working. Convert the borrowed batch into an owned
            // RecordBatch for FFI export.
            match super::block_on_ffi(move || {
                let exec: &mut ExecutedStatement = exec;
                Box::pin(async move {
                    Ok(exec
                        .next_batch()
                        .await?
                        .map(|ResultBatch::Arrow(rb)| rb.clone()))
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
                Err(e) => store_error(Error::from(e)),
            }
        }
        Some(Err(e)) => store_error(e),
    }
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
        return store_null("kernel_result_stream_close: stream must not be null");
    }
    drop(Box::from_raw(stream as *mut ResultStreamInner));
    KernelStatusCode::Success
}

/// Store an `InvalidArgument` for a null pointer and return its code.
fn store_null(msg: &str) -> KernelStatusCode {
    crate::c_abi::error::store_error(Error::invalid_argument().message(msg));
    KernelStatusCode::InvalidArgument
}

/// Store a typed error and return its mapped status code.
fn store_error(e: Error) -> KernelStatusCode {
    let code = KernelStatusCode::from(e.code);
    crate::c_abi::error::store_error(e);
    code
}
