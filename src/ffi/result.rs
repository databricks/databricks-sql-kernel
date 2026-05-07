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

//! Result FFI — exports `ResultStream` via the Arrow C Data Interface.
//!
//! `kernel_result_stream_get_schema` / `kernel_result_stream_next_batch`
//! return `FFI_ArrowSchema` / `FFI_ArrowArray` so any C consumer with an
//! Arrow C Data Interface implementation can iterate. PR 7c implements
//! the actual export plumbing using the `arrow` crate's `ffi` feature.

#![allow(dead_code, unused_variables, non_camel_case_types)]

use super::KernelStatusCode;

#[repr(C)]
pub struct kernel_result_stream_t {
    _private: [u8; 0],
}

/// Export the schema as an `FFI_ArrowSchema`. The caller releases via
/// the schema's release callback.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_get_schema(
    stream: *mut kernel_result_stream_t,
    // out: *mut FFI_ArrowSchema,
) -> KernelStatusCode {
    unimplemented!("PR 7c: arrow::ffi schema export")
}

/// Export the next batch as an `FFI_ArrowArray`. Returns
/// `KernelStatusCode::Success` with a released-state array when the stream
/// is exhausted.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_next_batch(
    stream: *mut kernel_result_stream_t,
    // out: *mut FFI_ArrowArray,
) -> KernelStatusCode {
    unimplemented!("PR 7c: arrow::ffi array export, drives next_batch()")
}

/// Free client-side resources held by the stream — buffered chunks,
/// the link prefetcher, in-flight downloaders. Local-only; does not
/// issue any server-side RPC. To cancel the underlying statement,
/// call `kernel_executed_statement_cancel` /
/// `kernel_executed_async_statement_cancel` on the executed handle
/// that produced the stream.
///
/// # Safety
///
/// `stream` must have been returned by
/// `kernel_executed_async_statement_await_result` /
/// `kernel_executed_async_statement_fetch_result` and not previously
/// closed (or borrowed via `kernel_executed_statement_get_result_stream`,
/// in which case the executed handle owns the stream and frees it on
/// its own `_close`). After this call, `stream` is invalid; passing
/// it to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_close(
    stream: *mut kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7c")
}
