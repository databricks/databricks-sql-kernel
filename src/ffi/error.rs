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

//! Thread-local last-error storage and the `kernel_get_last_error` C
//! entry point.
//!
//! Pattern borrowed from the legacy `src/adbc/ffi/error.rs`: every C
//! function clears the buffer at entry and writes to it on error;
//! callers fetch detail with `kernel_get_last_error`.

#![allow(dead_code, unused_variables)]

use crate::kernel_error::Error;
use std::ffi::c_char;

/// Mirror of `crate::kernel_error::Error` in `repr(C)` form.
///
/// String fields are owned by the kernel's thread-local buffer; the
/// caller copies them out before the next FFI call on this thread
/// clears the buffer. Pointers are nullable for fields the underlying
/// `Error` left as `None`.
#[repr(C)]
pub struct KernelError {
    /// Discriminant matches `KernelStatusCode` (cast-equal). Use
    /// `KernelStatusCode::from(error.code)` on the Rust side to
    /// convert; the i32 here is the discriminant value of that enum.
    pub code: i32,
    pub message: *const c_char,
    /// 5 ASCII chars + NUL, or null if absent.
    pub sql_state: *const c_char,
    /// `0` if absent (the underlying `Error::vendor_code` was `None`).
    pub vendor_code: i32,
    /// `0` if absent.
    pub http_status: u16,
    pub retryable: bool,
    pub query_id: *const c_char,
}

/// Fetch the last error set on this thread.
///
/// Returns `false` if no error has been recorded since the last
/// `clear_last_error()` (or thread-start).
///
/// # Safety
/// `out` must be a valid, writable pointer to a `KernelError`. The
/// pointer fields it returns are valid until the next FFI call on
/// this thread.
#[no_mangle]
pub unsafe extern "C" fn kernel_get_last_error(out: *mut KernelError) -> bool {
    unimplemented!("PR 7a: thread-local last-error pattern")
}

/// Crate-internal: write a typed kernel `Error` into the thread-local
/// last-error buffer. Every FFI function that returns a non-`Success`
/// status code calls this with the underlying `Error` first; the
/// consumer then retrieves it via `kernel_get_last_error`.
///
/// PR 7a fills the body. The signature is captured here so PR 7a's
/// implementation is a body-only change with no public-API surface.
pub(crate) fn store_error(err: Error) {
    unimplemented!("PR 7a: serialize Error into the thread-local KernelError buffer")
}

/// Crate-internal: clear the thread-local last-error buffer. Called
/// at the entry of every FFI function so a previous thread's stale
/// error doesn't leak into a subsequent call.
pub(crate) fn clear_error() {
    unimplemented!("PR 7a: zero out the thread-local")
}
