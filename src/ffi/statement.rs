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

//! Statement FFI — four opaque handle types per `docs/spec-v0.md`
//! addendum A1.
//!
//! ## Error reporting convention
//!
//! Every function returns [`KernelStatusCode`]. On any non-`Success`
//! return, the consumer calls [`crate::ffi::error::kernel_get_last_error`]
//! to populate a [`crate::ffi::error::KernelError`] with the typed
//! detail (sql_state, vendor_code, retryable, query_id, …). The
//! thread-local error buffer is reset at the start of every FFI
//! function.

#![allow(dead_code, unused_variables, non_camel_case_types)]

use super::KernelStatusCode;
use std::ffi::c_char;

// ─── Opaque handles ──────────────────────────────────────────────────────────

/// Plain-data spec (sql, params, conf, result format). Constructible
/// without a session; useful for caller-side spec building.
#[repr(C)]
pub struct kernel_statement_spec_t {
    _private: [u8; 0],
}

/// Lifecycle owner. Returned by `kernel_session_new_statement`. Owns
/// one `kernel_statement_spec_t` and at most one live executed handle.
#[repr(C)]
pub struct kernel_statement_t {
    _private: [u8; 0],
}

/// Sync-consumption executed handle. Returned by
/// `kernel_statement_execute`.
#[repr(C)]
pub struct kernel_executed_statement_t {
    _private: [u8; 0],
}

/// Async-consumption executed handle. Returned by
/// `kernel_statement_submit`.
#[repr(C)]
pub struct kernel_executed_async_statement_t {
    _private: [u8; 0],
}

/// Server-side execution status — C-side mirror of
/// [`crate::statement::executed_async::StatementStatus`].
///
/// The Rust-side `StatementStatus::Failed(Error)` carries an `Error`
/// payload that cannot be encoded as a scalar enum. The C ABI
/// flattens to the kind tag here; on `Failed`, the consumer calls
/// [`crate::ffi::error::kernel_get_last_error`] to retrieve the
/// detail.
///
/// `#[repr(C)]` with explicit discriminants so wrappers can declare
/// the enum verbatim in their generated bindings.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelStatementStatusKind {
    Pending = 0,
    Running = 1,
    Succeeded = 2,
    /// Server-side execution failed. Call `kernel_get_last_error` for
    /// the underlying `KernelError` (sql_state, vendor_code, …).
    Failed = 3,
    Cancelled = 4,
    Closed = 5,
}

// ─── Spec setters ────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn kernel_statement_spec_new(
    out: *mut *mut kernel_statement_spec_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b: alloc StatementSpec, return owned ptr")
}

/// Free a `kernel_statement_spec_t`.
///
/// # Safety
///
/// `spec` must have been returned by `kernel_statement_spec_new` and
/// not previously freed. After this call, `spec` is invalid; passing
/// it to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_spec_free(
    spec: *mut kernel_statement_spec_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

#[no_mangle]
pub unsafe extern "C" fn kernel_statement_spec_set_sql(
    spec: *mut kernel_statement_spec_t,
    sql: *const c_char,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

// PR 7b: bind_param, bind_param_named, set_statement_conf, set_result_format

// ─── Statement (lifecycle owner) ─────────────────────────────────────────────

/// Borrow the statement's spec. Mutating the returned spec mutates
/// the statement's spec in place.
///
/// **Lifetime:** the returned pointer is valid as long as `stmt` is.
/// Do not pass it to `kernel_statement_spec_free` — it is borrowed,
/// not owned.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_get_spec(
    stmt: *mut kernel_statement_t,
    out: *mut *mut kernel_statement_spec_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b: return &mut self.spec via raw ptr")
}

/// Wait-for-result execution. Auto-cancels + closes any prior
/// executed handle from this statement. On success, `*out` holds an
/// owned `kernel_executed_statement_t` the caller releases with
/// `kernel_executed_statement_close`.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_execute(
    stmt: *mut kernel_statement_t,
    out: *mut *mut kernel_executed_statement_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b: block_on Statement::execute")
}

/// Submit-and-return. Same auto-cancel-and-close semantics. On
/// success, `*out` holds an owned
/// `kernel_executed_async_statement_t` the caller releases with
/// `kernel_executed_async_statement_close`.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_submit(
    stmt: *mut kernel_statement_t,
    out: *mut *mut kernel_executed_async_statement_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b: block_on Statement::submit")
}

/// Free the lifecycle owner.
///
/// **Non-destructive for executed-handle pointers.** If a previously
/// returned `kernel_executed_statement_t` or
/// `kernel_executed_async_statement_t` from this statement is still
/// held by the caller, that pointer **remains safe to call** after
/// this function returns: subsequent calls dispatch through the
/// shared validity flag and return `KernelStatusCode::InvalidStatementHandle`
/// (no network round-trip). The caller is still responsible for
/// freeing each executed-handle pointer via the corresponding
/// `kernel_executed_*_close` function.
///
/// Net effect: this function frees only the statement-lifecycle box
/// and flips the validity flag. It does *not* free executed-handle
/// boxes — those live independently with their own refcounts.
///
/// # Safety
///
/// `stmt` must have been returned by `kernel_session_new_statement`
/// and not previously closed. After this call, `stmt` is invalid;
/// passing it to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_close(stmt: *mut kernel_statement_t) -> KernelStatusCode {
    unimplemented!("PR 7b: flip validity flag, drop the lifecycle owner box")
}

// ─── ExecutedStatement (sync) ────────────────────────────────────────────────

/// Server-issued statement id, NUL-terminated.
///
/// **Lifetime:** the returned pointer is valid as long as `exec` is.
/// Do not free it; do not retain past
/// `kernel_executed_statement_close`.
///
/// **On stale handle:** returns `null` if `exec` has been invalidated
/// by a re-execute on the parent statement. Callers should null-check
/// before dereferencing.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_id(
    exec: *const kernel_executed_statement_t,
) -> *const c_char {
    unimplemented!("PR 7b")
}

/// Borrow the result stream. Pointer is valid as long as `exec` is.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_get_result_stream(
    exec: *mut kernel_executed_statement_t,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_cancel(
    exec: *mut kernel_executed_statement_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

/// Free a sync executed handle.
///
/// # Safety
///
/// `exec` must have been returned by `kernel_statement_execute` and
/// not previously closed. After this call, `exec` is invalid;
/// passing it to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_close(
    exec: *mut kernel_executed_statement_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

// ─── ExecutedAsyncStatement (async) ──────────────────────────────────────────

/// Server-issued statement id, NUL-terminated.
///
/// **Lifetime:** the returned pointer is valid as long as `exec` is.
/// Do not free it; do not retain past
/// `kernel_executed_async_statement_close`.
///
/// **On stale handle:** returns `null` if `exec` has been invalidated
/// by a re-execute on the parent statement.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_async_statement_id(
    exec: *const kernel_executed_async_statement_t,
) -> *const c_char {
    unimplemented!("PR 7b")
}

/// One-shot status check. Writes a [`KernelStatementStatusKind`] kind
/// tag to `out_status`. On `Failed`, the caller invokes
/// `kernel_get_last_error` to retrieve the underlying error detail.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_async_statement_status(
    exec: *mut kernel_executed_async_statement_t,
    out_status: *mut KernelStatementStatusKind,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

/// Block until the server reaches a terminal state, then return the
/// result stream. On success, `*out` holds an owned
/// `kernel_result_stream_t` the caller releases with
/// `kernel_result_stream_close`.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_async_statement_await_result(
    exec: *mut kernel_executed_async_statement_t,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

/// Fetch the result without polling. Same ownership rules as
/// `kernel_executed_async_statement_await_result`.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_async_statement_fetch_result(
    exec: *mut kernel_executed_async_statement_t,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

#[no_mangle]
pub unsafe extern "C" fn kernel_executed_async_statement_cancel(
    exec: *mut kernel_executed_async_statement_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}

/// Free an async executed handle.
///
/// # Safety
///
/// `exec` must have been returned by `kernel_statement_submit` and
/// not previously closed. After this call, `exec` is invalid;
/// passing it to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_async_statement_close(
    exec: *mut kernel_executed_async_statement_t,
) -> KernelStatusCode {
    unimplemented!("PR 7b")
}
