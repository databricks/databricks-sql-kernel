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
use std::ffi::{c_char, CStr, CString};

pub use crate::statement::timeout::MAX_CLIENT_QUERY_TIMEOUT_MS;

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
/// Wraps the [`ExecutedStatement`] with the state the C ABI needs to keep
/// stale-handle access defined rather than UB:
///
/// - `alive`: a [`ValidityFlag`] shared with any borrowed result stream.
///   `kernel_executed_statement_close` invalidates it *before* dropping
///   the box, so a stream used after its executed handle is closed
///   short-circuits to `InvalidStatementHandle` instead of dereferencing
///   freed memory (out-of-order **close** guard).
/// - `stream_taken`: enforces **at most one** live result stream per
///   executed handle. Two streams would each reborrow `&mut exec` into a
///   raw pointer and alias `&mut ExecutedStatement` — UB. A second
///   `get_result_stream` returns `InvalidArgument`.
///
/// Note the re-execute guard is separate: a result stream also checks
/// `exec.is_valid()` (the `ExecutedStatement`'s own flag, flipped when the
/// parent `Statement` is re-executed) — see `result.rs`.
pub(crate) struct ExecutedHandle {
    pub(crate) exec: ExecutedStatement,
    pub(crate) alive: ValidityFlag,
    pub(crate) stream_taken: std::sync::atomic::AtomicBool,
    /// The server statement id as a NUL-terminated C string, built once at
    /// construction so `kernel_executed_statement_query_id` can hand back a
    /// borrowed `*const c_char` valid for the handle's lifetime (no per-call
    /// allocation, nothing for the caller to free). Any interior NUL is
    /// stripped (replaced with `?`) via `to_cstring_lossy` — the same semantics
    /// as the error-path `KernelError.query_id` — so the two query-id paths
    /// agree. A statement id is a server-generated UUID, so this never occurs in
    /// practice.
    pub(crate) statement_id_c: CString,
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

/// Detached canceller for a sync-executing statement. Returned by
/// `kernel_statement_canceller_new`. Holds only cloned Arcs, so it is safe to
/// use concurrently with the blocking `kernel_statement_execute` on the
/// originating statement.
///
/// Statement-scoped, not execution-scoped: it tracks whichever query is
/// currently in flight on its originating statement. Re-executing the same
/// statement retargets the canceller to the new query, so a canceller retained
/// across a re-execute cancels the *later* query. Use one canceller per execute:
/// obtain it before `kernel_statement_execute`, `kernel_statement_canceller_free`
/// it after that execute returns, before re-executing.
#[repr(C)]
pub struct kernel_statement_canceller_t {
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

/// Set the statement's SQL text, replacing any previously set SQL. Also clears
/// every previously-bound parameter (raw and typed), so a reused statement
/// handle begins each query with a fresh parameter set — see
/// [`crate::statement::spec::StatementSpec::sql`].
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

/// Bind a pre-marshalled parameter: host-supplied Databricks type name +
/// stringified value, positional or named. This is the binding path for a host
/// driver that already stringifies values and knows the SQL type (e.g. the Go
/// driver's `backend.Param{Name, Type, Value}`) — it avoids round-tripping
/// through the kernel's `TypedValue` inference.
///
/// - `name`: NULL/empty → positional (ordinal assigned at execute in push
///   order); non-empty → named (`:name` marker).
/// - `sql_type`: Databricks type name (e.g. "BIGINT", "STRING", "DECIMAL(5,2)").
///   For a NULL value the host passes "VOID" (matches the driver convention).
/// - `value`: NULL → SQL NULL; otherwise the stringified value sent verbatim.
///
/// Positional and named raw params are mutually exclusive within one statement
/// (enforced at execute), as are raw vs TypedValue params.
///
/// # Safety
/// `stmt` must be a live statement handle; `sql_type` must be a NUL-terminated C
/// string; `name`/`value` are NUL-terminated C strings or NULL.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_bind_parameter(
    stmt: *mut kernel_statement_t,
    name: *const c_char,
    sql_type: *const c_char,
    value: *const c_char,
) -> KernelStatusCode {
    with_statement(stmt, |stmt| {
        if sql_type.is_null() {
            return Err(Error::invalid_argument().message("parameter sql_type must not be null"));
        }
        let sql_type = CStr::from_ptr(sql_type)
            .to_str()
            .map_err(|_| {
                Error::invalid_argument().message("parameter sql_type is not valid UTF-8")
            })?
            .to_owned();
        // Reject an empty type here rather than letting it reach the wire as
        // `"type":""` and surface as an opaque server BAD_REQUEST at execute —
        // symmetric with the NULL guard above and with param_named rejecting an
        // empty name (a bind-time typed error beats a wire-level rejection).
        if sql_type.is_empty() {
            return Err(Error::invalid_argument().message("parameter sql_type must not be empty"));
        }
        let name_opt = if name.is_null() {
            None
        } else {
            let s = CStr::from_ptr(name).to_str().map_err(|_| {
                Error::invalid_argument().message("parameter name is not valid UTF-8")
            })?;
            if s.is_empty() {
                None
            } else {
                Some(s.to_owned())
            }
        };
        let value_opt = if value.is_null() {
            None
        } else {
            Some(
                CStr::from_ptr(value)
                    .to_str()
                    .map_err(|_| {
                        Error::invalid_argument().message("parameter value is not valid UTF-8")
                    })?
                    .to_owned(),
            )
        };
        stmt.spec().param_raw(name_opt, sql_type, value_opt);
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

/// Set per-statement query tags — key/value annotations sent with this
/// statement's execution for cost attribution and tracking. `query_tags` is the
/// serialized wire form: comma-separated `key:value` pairs, with a bare `key`
/// (no colon) for a valueless tag, e.g. `team:eng,job:etl,production`; backslash
/// escapes the separators in a value (`\:`, `\,`, `\\`). This is the same string
/// [`crate::statement::spec::serialize_query_tags`] produces, so a host that
/// already serializes tags for the Thrift `confOverlay` path (e.g. the Go/Node
/// connectors) passes it through unchanged. The kernel re-parses it into the
/// native SEA `query_tags` array at execute time (see
/// [`crate::statement::mutable`]).
///
/// A non-empty value **replaces** the statement's query tags; an **empty string
/// clears** any previously-set tags. Clearing matters because query tags persist
/// across [`kernel_statement_set_sql`] (which resets bound parameters but not the
/// statement conf), so a reused statement handle would otherwise carry an earlier
/// query's tags into later executions — passing `""` before re-executing resets
/// them. (This is a deliberate divergence from the typed
/// [`crate::statement::spec::StatementSpec::query_tags`], whose empty **map** is a
/// no-op: this single C entry point makes empty = clear so the C surface can both
/// set and reset without a second function.)
///
/// # Safety
/// `stmt` must be a live statement handle; `query_tags` must be a NUL-terminated
/// UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_set_query_tags(
    stmt: *mut kernel_statement_t,
    query_tags: *const c_char,
) -> KernelStatusCode {
    with_statement(stmt, |stmt| {
        if query_tags.is_null() {
            return Err(Error::invalid_argument().message("query_tags must not be null"));
        }
        let tags = CStr::from_ptr(query_tags)
            .to_str()
            .map_err(|_| Error::invalid_argument().message("query_tags is not valid UTF-8"))?;
        // Empty → clear. statement_conf survives set_sql, so a reused handle would
        // otherwise leak a prior query's tags; an explicit empty string is the C
        // ABI's way to reset them before re-executing.
        if tags.is_empty() {
            stmt.spec().statement_conf.remove("query_tags");
            return Ok(());
        }
        // The value is already in the wire form the SEA execute path reads back
        // from `statement_conf["query_tags"]`, so store it verbatim rather than
        // round-tripping through the typed `query_tags(map)` builder.
        stmt.spec()
            .statement_conf
            .insert("query_tags".to_string(), tags.to_string());
        Ok(())
    })
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

    execute_with_deadline(stmt, None, out)
}

/// Wait-for-result execution with an execution-scoped client deadline.
///
/// `timeout_ms == 0` disables the client deadline. A positive timeout covers
/// C-ABI runtime admission, the initial ExecuteStatement request, retries and
/// backoff, poll sleeps, status requests, and terminal-state mapping. Reader /
/// schema materialisation and result fetching are outside it. The absolute
/// deadline is captured on this native entry thread before work is queued onto
/// the kernel runtime.
///
/// On expiry this returns [`KernelStatusCode::Timeout`], stores a non-retryable
/// `HYT00` error (including the query id when already known), leaves `*out`
/// null, and admits best-effort cancel/close work without waiting for it.
///
/// # Safety
/// `stmt` must be a live statement handle; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_execute_with_timeout_ms(
    stmt: *mut kernel_statement_t,
    timeout_ms: u64,
    out: *mut *mut kernel_executed_statement_t,
) -> KernelStatusCode {
    // Capture the monotonic origin at native entry, before validation, runtime
    // initialisation, or queueing. Validation remains synchronous and RPC-free,
    // but any time it consumes is still part of a positive caller budget.
    let entered_at = std::time::Instant::now();
    crate::c_abi::error::clear_error();
    if out.is_null() {
        return store_invalid_arg("kernel_statement_execute_with_timeout_ms: out must not be null");
    }
    std::ptr::write(out, std::ptr::null_mut());
    let Some(stmt) = statement_mut(stmt) else {
        return store_invalid_arg(
            "kernel_statement_execute_with_timeout_ms: stmt must not be null",
        );
    };
    // Validate against the same finite range as the Rust surface, then derive
    // from the instant captured above. No runtime work or RPC precedes this.
    let deadline = match crate::statement::timeout::deadline_from_timeout_at(
        std::time::Duration::from_millis(timeout_ms),
        entered_at,
    ) {
        Ok(deadline) => deadline,
        Err(error) => return store_and_code(error),
    };

    execute_with_deadline(stmt, deadline, out)
}

/// Shared sync-execute bridge after pointer/range validation and optional
/// deadline capture. The timeout entry point initializes `out` to null before
/// calling this helper; the legacy entry point deliberately preserves its
/// existing failure-path out-parameter behavior.
unsafe fn execute_with_deadline(
    stmt: &mut Statement,
    deadline: Option<std::time::Instant>,
    out: *mut *mut kernel_executed_statement_t,
) -> KernelStatusCode {
    // `Statement::execute` borrows `&mut self`; we need that borrow to
    // live across the `block_on`. Raw-pointer the statement so the
    // closure can re-borrow it inside the runtime without entangling
    // lifetimes with `block_on_ffi`'s future bound. `block_on_ffi`
    // catches any panic (see the panic contract above).
    let stmt_ptr: *mut Statement = stmt;
    let executed = block_on_ffi(move || {
        let stmt: &mut Statement = unsafe { &mut *stmt_ptr };
        Box::pin(async move { stmt.execute_with_deadline(deadline).await })
    });
    match executed {
        Ok(exec) => {
            // Snapshot the server statement id as a C string now, while the exec
            // handle is fresh, so the query-id accessor is a borrow with no
            // allocation. `to_cstring_lossy` strips any interior NUL (→ `?`)
            // rather than discarding the whole id — never happens for a
            // server-generated UUID, but keeps this success-path id and the
            // error-path `KernelError.query_id` on identical NUL semantics, and
            // guarantees the accessor stays non-NULL for any live handle per its
            // header contract.
            let statement_id_c = crate::c_abi::error::to_cstring_lossy(exec.statement_id());
            let handle = ExecutedHandle {
                exec,
                alive: ValidityFlag::new(),
                stream_taken: std::sync::atomic::AtomicBool::new(false),
                statement_id_c,
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
/// boxes (those are separate allocations, closed via
/// `kernel_executed_statement_close`).
///
/// Close the statement LAST: it shares a validity flag with the executed
/// handles it produced, so closing it invalidates any still-live executed
/// handle (and its borrowed result stream), which then return
/// `InvalidStatementHandle` on further use. Drain/close streams and
/// executed handles first. See the `databricks_kernel.h` contract.
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

// ─── Sync-execute cancellation (detached canceller) ──────────────────────────

/// Owned box behind a `kernel_statement_canceller_t`; holds a detached
/// [`StatementCanceller`].
struct CancellerHandle {
    canceller: crate::statement::mutable::StatementCanceller,
}

/// Create a detached canceller for `stmt`, obtained BEFORE
/// `kernel_statement_execute` (which blocks the calling thread, so the
/// statement cannot be cancelled through its own handle). From another thread,
/// `kernel_statement_canceller_cancel` then cancels the in-flight execute. Free
/// with `kernel_statement_canceller_free`.
///
/// The canceller holds its own reference to the session, so it keeps that
/// session's client-side state alive until freed and must be freed regardless of
/// whether the statement or session is closed. Its free is order-independent:
/// `kernel_statement_canceller_free` drives no RPC (it only drops the box), so it
/// may run before or after the statement/session close — the cloned session `Arc`
/// keeps the client side alive for a cancel even after `kernel_session_close`
/// (exercised by `canceller_outlives_statement_and_session_close`).
///
/// # Safety
/// `stmt` must be a live statement handle; `out` must be writable. `stmt` must
/// not have an `kernel_statement_execute` in flight on another thread: this
/// borrows the statement mutably, and execute holds its own mutable borrow for
/// the duration of the query, so obtain the canceller before dispatching.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_canceller_new(
    stmt: *mut kernel_statement_t,
    out: *mut *mut kernel_statement_canceller_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(stmt) = statement_mut(stmt) else {
        return store_invalid_arg("kernel_statement_canceller_new: stmt must not be null");
    };
    if out.is_null() {
        return store_invalid_arg("kernel_statement_canceller_new: out must not be null");
    }
    let handle = CancellerHandle {
        canceller: stmt.canceller(),
    };
    std::ptr::write(
        out,
        Box::into_raw(Box::new(handle)) as *mut kernel_statement_canceller_t,
    );
    KernelStatusCode::Success
}

/// Fire a server-side cancel of the statement this canceller was created for.
/// Idempotent and safe to call concurrently with `kernel_statement_execute` on
/// the originating statement.
///
/// On `Success`, `*dispatched` reports whether a cancel RPC was POSTed:
/// `true` if a server statement id had been observed and the RPC was sent for
/// it, `false` if no id has been observed yet — nothing has executed, or the
/// query is still in its initial round-trip before the server issued an id. Both
/// cases return `Success`; the flag is how a host distinguishes a dispatched
/// cancel from a no-op. `*dispatched` is set to `false` before the RPC and to
/// the outcome on `Success`, so it is always defined once this returns; a null
/// `dispatched` is ignored.
///
/// `dispatched == true` means only that a cancel was POSTed for the currently
/// observed statement id — **not** that a running query was interrupted. The id
/// slot retains the last observed id after a query finishes, so a cancel fired
/// post-terminal (or, in the narrow re-execute handoff, against a superseded id)
/// still POSTs and still reports `true`; the server treats a cancel of an
/// already-finished statement as its own no-op.
///
/// # Safety
/// `canceller` must be a live handle from `kernel_statement_canceller_new`.
/// `dispatched`, if non-null, must be writable. The handle must not be freed by
/// another thread while this call is in flight: it blocks on the cancel RPC,
/// which on a retryable server error retries with backoff and can therefore run
/// far longer than one round-trip — bounded by the SEA retry budget (the
/// `overall_timeout` / `max_retries` defaults in `crate::client::retry`), which
/// can reach minutes during a partial server outage. A host wiring this into a
/// Ctrl-C / query-timeout handler should drive it from an abandonable/joinable
/// thread rather than the app's main thread.
/// Order any `kernel_statement_canceller_free` after this returns.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_canceller_cancel(
    canceller: *mut kernel_statement_canceller_t,
    dispatched: *mut bool,
) -> KernelStatusCode {
    catch_ffi(|| {
        // Define the out-param before anything else, so a caller that reads it
        // sees `false` (no cancel dispatched) on every path, including the
        // null-handle and error returns below.
        if !dispatched.is_null() {
            std::ptr::write(dispatched, false);
        }
        let Some(handle) = (canceller as *mut CancellerHandle).as_ref() else {
            return store_invalid_arg("kernel_statement_canceller_cancel: handle must not be null");
        };
        // Clone the canceller (two Arc bumps) so the blocking cancel RPC owns its
        // session and id-slot handles rather than borrowing through this box.
        let canceller = handle.canceller.clone();
        match block_on_ffi(move || Box::pin(async move { canceller.cancel().await })) {
            Ok(fired) => {
                if !dispatched.is_null() {
                    std::ptr::write(dispatched, fired);
                }
                KernelStatusCode::Success
            }
            Err(code) => code,
        }
    })
}

/// Free a canceller handle. Does not touch the originating statement.
///
/// # Safety
/// `canceller` must have been returned by `kernel_statement_canceller_new` and
/// not previously freed. The canceller is single-owner: freeing it while another
/// thread is inside `kernel_statement_canceller_cancel` on this handle is
/// undefined behavior (use-after-free), not merely wasteful. That call blocks on
/// the cancel RPC, which on a retryable server error retries with backoff and can
/// stay in flight far longer than one round-trip, so join or otherwise quiesce
/// any cancelling thread before freeing.
#[no_mangle]
pub unsafe extern "C" fn kernel_statement_canceller_free(
    canceller: *mut kernel_statement_canceller_t,
) -> KernelStatusCode {
    catch_ffi(|| {
        if canceller.is_null() {
            return store_invalid_arg("kernel_statement_canceller_free: handle must not be null");
        }
        drop(Box::from_raw(canceller as *mut CancellerHandle));
        KernelStatusCode::Success
    })
}

// ─── ExecutedStatement (sync) ────────────────────────────────────────────────

/// Obtain a result-stream handle that **borrows** the sync executed
/// handle. The returned `kernel_result_stream_t` drives the executed
/// handle's own stream, so the executed handle's drain-driven
/// auto-close and lazy CloudFetch link fetches keep working.
///
/// **At most one stream per handle.** A second call returns
/// `InvalidArgument` — handing out two streams would alias `&mut
/// ExecutedStatement` (UB) and corrupt the shared drain state.
///
/// **Lifetime / validity:** the returned stream borrows the executed
/// handle. It short-circuits to `InvalidStatementHandle` (a defined
/// error, never UB) if either (a) the executed handle is closed first
/// (`kernel_executed_statement_close`), or (b) the parent statement is
/// re-executed (which invalidates this handle). Close the stream first
/// for orderly teardown.
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
    // Reject if the parent statement was re-executed or closed (this
    // handle shares the statement's validity flag; both flip it).
    if !handle.exec.is_valid() {
        return store_and_code(Error::new(
            crate::kernel_error::ErrorCode::InvalidStatementHandle,
            "executed statement was invalidated by a re-execute or close on its parent statement; \
             drain/close result streams and executed handles before closing the statement",
        ));
    }
    // At most one live stream per handle: a second reborrow of `&mut exec`
    // into a raw pointer would alias `&mut ExecutedStatement` (UB).
    if handle
        .stream_taken
        .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        return store_invalid_arg(
            "a result stream has already been taken from this executed statement; \
             only one is allowed",
        );
    }
    // The stream shares `alive` (invalidated on close) for the
    // out-of-order-close UAF guard; it separately calls `exec.is_valid()`
    // through the pointer on each use to honour re-execute invalidation.
    let exec_ptr: *mut ExecutedStatement = &mut handle.exec;
    std::ptr::write(
        out,
        into_raw_executed_stream(exec_ptr, handle.alive.clone()),
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
    // Stale after a re-execute or close on the parent statement → -1 with
    // a stored InvalidStatementHandle (distinct from the not-applicable
    // -1, which stores no error).
    if !handle.exec.is_valid() {
        crate::c_abi::error::store_and_code(Error::new(
            crate::kernel_error::ErrorCode::InvalidStatementHandle,
            "executed statement was invalidated by a re-execute or close on its parent statement; \
             query num_modified_rows before closing the statement",
        ));
        return -1;
    }
    handle.exec.num_modified_rows().unwrap_or(-1)
}

/// The server statement (query) id of a successfully executed statement, as a
/// NUL-terminated C string. This is the success-path counterpart to the query id
/// on `KernelError` (which crosses only on failure): a host driver uses it for
/// EXECUTE_STATEMENT telemetry and query-history correlation.
///
/// The returned pointer is **borrowed** from the executed handle and valid until
/// the handle is closed (`kernel_executed_statement_close`); the caller must NOT
/// free it and should copy it out if it needs to outlive the handle. Returns
/// NULL on a null or invalidated handle (with a stored last error). A successful
/// execute always has an id, so a live handle never returns NULL.
///
/// # Safety
/// `exec` must be a live executed handle from [`kernel_statement_execute`].
#[no_mangle]
pub unsafe extern "C" fn kernel_executed_statement_query_id(
    exec: *const kernel_executed_statement_t,
) -> *const c_char {
    crate::c_abi::error::clear_error();
    let Some(handle) = (exec as *const ExecutedHandle).as_ref() else {
        crate::c_abi::error::store_invalid_arg(
            "kernel_executed_statement_query_id: exec must not be null",
        );
        return std::ptr::null();
    };
    // Stale after a re-execute or close on the parent statement → NULL with a
    // stored InvalidStatementHandle, matching num_modified_rows' staleness rule.
    if !handle.exec.is_valid() {
        crate::c_abi::error::store_and_code(Error::new(
            crate::kernel_error::ErrorCode::InvalidStatementHandle,
            "executed statement was invalidated by a re-execute or close on its parent statement; \
             read query_id before closing the statement",
        ));
        return std::ptr::null();
    }
    handle.statement_id_c.as_ptr()
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
    handle.alive.invalidate();
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

#[cfg(test)]
mod tests {
    use super::*;

    // Null-pointer guards only: a bad handle yields InvalidArgument, never a
    // crash. The success + concurrent-cancel-during-execute paths are covered by
    // tests/v0_c_abi_canceller_wiremock.rs, which drives these entry points
    // against a mock SEA server.
    #[test]
    fn set_query_tags_null_handle_returns_invalid_argument() {
        // A null statement handle is rejected before the tags are read, so this
        // never crashes. (The happy path — a live handle — is covered where the
        // serialized string reaches the wire array: see the
        // `build_execute_params_reads_raw_query_tags_conf` test in
        // `statement::mutable`.)
        let rc =
            unsafe { kernel_statement_set_query_tags(std::ptr::null_mut(), c"team:eng".as_ptr()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
    }

    #[test]
    fn canceller_null_guards_return_invalid_argument() {
        let mut out: *mut kernel_statement_canceller_t = std::ptr::null_mut();
        let rc = unsafe { kernel_statement_canceller_new(std::ptr::null_mut(), &mut out) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);

        // A null dispatched out-param is tolerated; the null handle is what makes
        // this InvalidArgument.
        let mut dispatched = false;
        let rc =
            unsafe { kernel_statement_canceller_cancel(std::ptr::null_mut(), &mut dispatched) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
        let rc = unsafe {
            kernel_statement_canceller_cancel(std::ptr::null_mut(), std::ptr::null_mut())
        };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);

        let rc = unsafe { kernel_statement_canceller_free(std::ptr::null_mut()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
    }
}
