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

//! Statement FFI — set sql, bind params, execute, and fetch results.
//!
//! ## Scope in v0
//!
//! The C ABI wires the **synchronous** execute path
//! (`kernel_statement_execute` → `kernel_executed_statement_t` →
//! `kernel_result_stream_t`), which is what the ODBC consumer needs. The
//! **async** submit path (`kernel_statement_submit` +
//! `kernel_executed_async_statement_*`) keeps its skeleton declarations
//! and `unimplemented!()` bodies until a consumer needs caller-driven
//! polling over the C ABI; Rust-crate consumers use `Statement::submit`
//! directly.
//!
//! ## Error reporting convention
//!
//! Every function returns [`KernelStatusCode`]. On any non-`Success`
//! return, the consumer calls [`crate::c_abi::error::kernel_get_last_error`]
//! to populate a [`crate::c_abi::error::KernelError`] with the typed
//! detail. The thread-local error buffer is reset at the start of every
//! FFI function.

#![allow(non_camel_case_types)]

use super::error::{store_and_code, store_invalid_arg};
use super::result::{into_raw_executed_stream, kernel_result_stream_t};
use super::{block_on_ffi, catch_ffi, KernelStatusCode};
use crate::kernel_error::Error;
use crate::statement::executed::ExecutedStatement;
use crate::statement::mutable::Statement;
use crate::statement::spec::TypedValue;
use crate::statement::validity::ValidityFlag;
use std::ffi::{c_char, CStr};

/// Upper bound on a positional parameter ordinal accepted over the C ABI.
///
/// `StatementSpec::param` grows `positional_params` with
/// `Vec::resize(ordinal, Null)`, so an unbounded caller-supplied ordinal
/// (e.g. `usize::MAX`) would trigger a multi-GB allocation / capacity
/// overflow before execution. SEA itself caps parameter markers far
/// below this; 64Ki is comfortably above any real statement and cheap to
/// reject past.
const MAX_PARAM_ORDINAL: usize = 65_536;

/// Owned box behind a `kernel_executed_statement_t`.
///
/// Wraps the [`ExecutedStatement`] together with a [`ValidityFlag`]
/// shared with any result stream borrowed from it
/// (`kernel_executed_statement_get_result_stream`). Closing the executed
/// handle invalidates the flag *before* dropping the box, so a stream
/// used after its executed handle is closed short-circuits to
/// `InvalidStatementHandle` instead of dereferencing freed memory.
pub(crate) struct ExecutedHandle {
    pub(crate) exec: ExecutedStatement,
    pub(crate) stream_valid: ValidityFlag,
}

// ─── Opaque handles ──────────────────────────────────────────────────────────

/// Lifecycle owner. Returned by `kernel_session_new_statement`. Boxed
/// [`Statement`] on the Rust side.
#[repr(C)]
pub struct kernel_statement_t {
    _private: [u8; 0],
}

/// Sync-consumption executed handle. Returned by
/// [`kernel_statement_execute`]. Boxed [`ExecutedStatement`].
#[repr(C)]
pub struct kernel_executed_statement_t {
    _private: [u8; 0],
}

/// Async-consumption executed handle. Returned by
/// `kernel_statement_submit` (deferred in v0 — see module docs).
#[repr(C)]
pub struct kernel_executed_async_statement_t {
    _private: [u8; 0],
}

/// Server-side execution status — C-side mirror of
/// [`crate::statement::executed_async::StatementStatus`].
///
/// `#[repr(C)]` with explicit discriminants so wrappers can declare the
/// enum verbatim in their generated bindings.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelStatementStatusKind {
    Pending = 0,
    Running = 1,
    Succeeded = 2,
    /// Server-side execution failed. Call `kernel_get_last_error` for
    /// the underlying `KernelError`.
    Failed = 3,
    Cancelled = 4,
    Closed = 5,
}

// ─── Crate-internal raw-handle helpers ───────────────────────────────────────

/// Box a [`Statement`] and erase it to the opaque C handle type.
pub(crate) fn into_raw_statement(stmt: Statement) -> *mut kernel_statement_t {
    Box::into_raw(Box::new(stmt)) as *mut kernel_statement_t
}

/// Borrow the boxed [`Statement`] behind a handle, mutably.
///
/// # Safety
/// `stmt` must be a live handle from `kernel_session_new_statement`.
unsafe fn statement_mut<'a>(stmt: *mut kernel_statement_t) -> Option<&'a mut Statement> {
    (stmt as *mut Statement).as_mut()
}

// ─── SQL + parameter binding ─────────────────────────────────────────────────

/// Set the statement's SQL text, replacing any previously set SQL.
///
/// # Safety
/// `stmt` must be a live statement handle; `sql` must be a
/// NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_set_sql(
    stmt: *mut kernel_statement_t,
    sql: *const c_char,
) -> KernelStatusCode {
    with_statement(stmt, |stmt| {
        if sql.is_null() {
            return Err(Error::invalid_argument().message("sql must not be null"));
        }
        let sql = CStr::from_ptr(sql)
            .to_str()
            .map_err(|_| Error::invalid_argument().message("sql is not valid UTF-8"))?;
        stmt.spec().sql(sql);
        Ok(())
    })
}

/// Bind a 1-based positional parameter to a NULL value.
///
/// # Safety
/// `stmt` must be a live statement handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_bind_null(
    stmt: *mut kernel_statement_t,
    ordinal: usize,
) -> KernelStatusCode {
    bind_positional(stmt, ordinal, TypedValue::Null)
}

/// Bind a 1-based positional `BIGINT` parameter.
///
/// # Safety
/// `stmt` must be a live statement handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_bind_int64(
    stmt: *mut kernel_statement_t,
    ordinal: usize,
    value: i64,
) -> KernelStatusCode {
    bind_positional(stmt, ordinal, TypedValue::Int64(value))
}

/// Bind a 1-based positional `DOUBLE` parameter.
///
/// # Safety
/// `stmt` must be a live statement handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_bind_double(
    stmt: *mut kernel_statement_t,
    ordinal: usize,
    value: f64,
) -> KernelStatusCode {
    bind_positional(stmt, ordinal, TypedValue::Float64(value))
}

/// Bind a 1-based positional `BOOLEAN` parameter.
///
/// # Safety
/// `stmt` must be a live statement handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_bind_bool(
    stmt: *mut kernel_statement_t,
    ordinal: usize,
    value: bool,
) -> KernelStatusCode {
    bind_positional(stmt, ordinal, TypedValue::Bool(value))
}

/// Bind a 1-based positional `STRING` parameter.
///
/// # Safety
/// `stmt` must be a live statement handle; `value` must be a
/// NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_bind_string(
    stmt: *mut kernel_statement_t,
    ordinal: usize,
    value: *const c_char,
) -> KernelStatusCode {
    with_statement(stmt, |stmt| {
        if value.is_null() {
            return Err(
                Error::invalid_argument().message("string parameter value must not be null")
            );
        }
        check_ordinal(ordinal)?;
        let value = CStr::from_ptr(value).to_str().map_err(|_| {
            Error::invalid_argument().message("string parameter is not valid UTF-8")
        })?;
        stmt.spec()
            .param(ordinal, TypedValue::String(value.to_owned()))?;
        Ok(())
    })
}

/// Shared body for the scalar `bind_*` setters.
///
/// # Safety
/// `stmt` must be a live statement handle.
unsafe fn bind_positional(
    stmt: *mut kernel_statement_t,
    ordinal: usize,
    value: TypedValue,
) -> KernelStatusCode {
    with_statement(stmt, |stmt| {
        check_ordinal(ordinal)?;
        stmt.spec().param(ordinal, value)?;
        Ok(())
    })
}

/// Reject an out-of-range positional ordinal before it reaches
/// `StatementSpec::param` (whose `Vec::resize(ordinal, …)` would
/// otherwise allocate unboundedly / overflow).
fn check_ordinal(ordinal: usize) -> Result<(), Error> {
    if ordinal > MAX_PARAM_ORDINAL {
        return Err(Error::invalid_argument().message(format!(
            "positional parameter ordinal {ordinal} exceeds the maximum of {MAX_PARAM_ORDINAL}"
        )));
    }
    Ok(())
}

// ─── Execute (sync) ──────────────────────────────────────────────────────────

/// Wait-for-result execution. Auto-cancels + closes any prior executed
/// handle from this statement. On success, `*out` holds an owned
/// `kernel_executed_statement_t` the caller releases with
/// [`kernel_executed_statement_close`].
///
/// **Re-execution invalidates prior handles.** Calling this again on the
/// same statement auto-cancels and closes any `kernel_executed_statement_t`
/// previously returned by it; the stale handle's calls then return
/// `KernelStatusCode::InvalidStatementHandle`. Don't retain two live
/// executed handles from one statement.
///
/// **Panic contract.** On a `KernelStatusCode::Internal` return caused by
/// a caught panic mid-`execute`, the statement handle may be
/// half-mutated; the caller must treat it as invalid and only
/// `kernel_statement_close` it (do not re-`execute`).
///
/// # Safety
/// `stmt` must be a live statement handle; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_execute(
    stmt: *mut kernel_statement_t,
    out: *mut *mut kernel_executed_statement_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(stmt) = statement_mut(stmt) else {
        return store_invalid_arg("kernel_statement_execute: stmt must not be null");
    };
    if out.is_null() {
        return store_invalid_arg("kernel_statement_execute: out must not be null");
    }

    // `Statement::execute` borrows `&mut self`; we need that borrow to
    // live across the `block_on`. Raw-pointer the statement so the
    // closure can re-borrow it inside the runtime without entangling
    // lifetimes with `block_on_ffi`'s future bound. `block_on_ffi`
    // catches any panic (see the panic contract above).
    let stmt_ptr: *mut Statement = stmt;
    let executed = block_on_ffi(move || {
        let stmt: &mut Statement = unsafe { &mut *stmt_ptr };
        Box::pin(async move { stmt.execute().await })
    });
    match executed {
        Ok(exec) => {
            let handle = ExecutedHandle {
                exec,
                stream_valid: ValidityFlag::new(),
            };
            std::ptr::write(
                out,
                Box::into_raw(Box::new(handle)) as *mut kernel_executed_statement_t,
            );
            KernelStatusCode::Success
        }
        Err(code) => code,
    }
}

/// Free the statement-lifecycle owner. Does not free executed-handle
/// boxes (those are owned independently); see the spec contract.
///
/// # Safety
/// `stmt` must have been returned by `kernel_session_new_statement` and
/// not previously closed.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_close(stmt: *mut kernel_statement_t) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    if stmt.is_null() {
        return store_invalid_arg("kernel_statement_close: stmt must not be null");
    }
    drop(Box::from_raw(stmt as *mut Statement));
    KernelStatusCode::Success
}

// ─── ExecutedStatement (sync) ────────────────────────────────────────────────

/// Obtain a result-stream handle that **borrows** the sync executed
/// handle. The returned `kernel_result_stream_t` drives the executed
/// handle's own stream, so the executed handle's drain-driven
/// auto-close and lazy CloudFetch link fetches keep working.
///
/// **Lifetime:** the returned stream borrows the executed handle. Closing
/// the executed handle (`kernel_executed_statement_close`) invalidates the
/// stream: subsequent stream calls return
/// `KernelStatusCode::InvalidStatementHandle` (a defined error, not
/// undefined behaviour). Close the stream first for orderly teardown.
///
/// On success `*out` holds the stream handle.
///
/// # Safety
/// `exec` must be a live executed handle; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_get_result_stream(
    exec: *mut kernel_executed_statement_t,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(handle) = (exec as *mut ExecutedHandle).as_mut() else {
        return store_invalid_arg("get_result_stream: exec must not be null");
    };
    if out.is_null() {
        return store_invalid_arg("get_result_stream: out must not be null");
    }
    // Share the validity flag with the stream so an out-of-order close of
    // the executed handle turns stream use into a defined error rather
    // than a use-after-free.
    let exec_ptr: *mut ExecutedStatement = &mut handle.exec;
    std::ptr::write(
        out,
        into_raw_executed_stream(exec_ptr, handle.stream_valid.clone()),
    );
    KernelStatusCode::Success
}

/// Number of rows modified by a DML statement, or `-1` if not applicable
/// / unknown. `-1` with **no** stored last error means "not applicable";
/// `-1` after a stored `InvalidArgument` means the handle was null.
///
/// # Safety
/// `exec` must be a live executed handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_num_modified_rows(
    exec: *const kernel_executed_statement_t,
) -> i64 {
    crate::c_abi::error::clear_error();
    let Some(handle) = (exec as *const ExecutedHandle).as_ref() else {
        crate::c_abi::error::store_invalid_arg(
            "kernel_executed_statement_num_modified_rows: exec must not be null",
        );
        return -1;
    };
    handle.exec.num_modified_rows().unwrap_or(-1)
}

/// Free a sync executed handle. Invalidates any result stream borrowed
/// from it before dropping (so a stream outliving its handle fails
/// cleanly rather than dereferencing freed memory).
///
/// # Safety
/// `exec` must have been returned by [`kernel_statement_execute`] and
/// not previously closed.
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_close(
    exec: *mut kernel_executed_statement_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    if exec.is_null() {
        return store_invalid_arg("kernel_executed_statement_close: exec must not be null");
    }
    let handle = Box::from_raw(exec as *mut ExecutedHandle);
    // Invalidate before drop so a still-open borrowed stream sees the
    // handle as gone and returns InvalidStatementHandle.
    handle.stream_valid.invalidate();
    drop(handle);
    KernelStatusCode::Success
}

// ─── ExecutedAsyncStatement (async) — deferred in v0 ─────────────────────────
//
// The async submit path keeps its skeleton until a consumer needs
// caller-driven polling over the C ABI. Rust-crate consumers use
// `Statement::submit` + `ExecutedAsyncStatement` directly.

/// Submit-and-return. **Deferred in v0** — see module docs.
///
/// # Safety
/// `stmt` / `out` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_submit(
    stmt: *mut kernel_statement_t,
    out: *mut *mut kernel_executed_async_statement_t,
) -> KernelStatusCode {
    let _ = (stmt, out);
    crate::c_abi::error::clear_error();
    store_invalid_arg(
        "async submit is not exposed over the v0 C ABI; use the synchronous execute path",
    )
}

// ─── Shared helpers ──────────────────────────────────────────────────────────

/// Null-check the statement handle and run the fallible closure `f`
/// against the borrowed statement, catching panics at the FFI boundary
/// (via [`catch_ffi`]) and storing/translating any typed error.
///
/// # Safety
/// `stmt` must be a live `*mut kernel_statement_t`.
unsafe fn with_statement(
    stmt: *mut kernel_statement_t,
    f: impl FnOnce(&mut Statement) -> Result<(), Error>,
) -> KernelStatusCode {
    // `catch_ffi` clears the thread-local error and guards against a
    // panic unwinding across `extern "C"` (e.g. from spec mutation).
    catch_ffi(|| {
        let Some(stmt) = statement_mut(stmt) else {
            return store_invalid_arg("statement handle must not be null");
        };
        match f(stmt) {
            Ok(()) => KernelStatusCode::Success,
            Err(e) => store_and_code(e),
        }
    })
}
