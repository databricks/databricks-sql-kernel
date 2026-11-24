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

use super::error::{store_and_code, store_invalid_arg};
use super::{block_on_ffi, catch_ffi, KernelStatusCode};
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

/// Opaque result-stream canceller handle. Boxed [`ResultStreamCanceller`].
#[repr(C)]
pub struct kernel_result_stream_canceller_t {
    _private: [u8; 0],
}

/// Backing state for a result-stream handle: a shared cancellation token plus
/// the stream's backing shape.
///
/// The `cancel` token is the interrupt hook (odbc#355-B). `next_batch` drives
/// the batch fetch inside a `tokio::select!` against `cancel.cancelled()`, so a
/// consumer that fired the token from another thread — via a
/// [`kernel_result_stream_canceller_t`] cloned from it — makes an in-flight
/// `next_batch` return promptly with [`KernelStatusCode::Cancelled`] instead of
/// blocking until the (possibly slow / CloudFetch-link) fetch finishes on its
/// own. The token is a purely client-side signal (no server RPC), is `Sync`,
/// and touches none of the stream's `&mut` state, so a cancel racing a
/// `next_batch`/`close` on the owning thread is well-defined: it only flips an
/// atomic the fetch's `select!` observes.
///
/// SCOPE (odbc#355 review): firing this token bounds the CALLER'S wait — the
/// `next_batch` future is dropped at its await point and the C call returns
/// promptly — but it does NOT abort the underlying request. For a CloudFetch
/// download the provider runs the fetch on a spawned task and awaits its
/// `JoinHandle`; dropping the `next_batch` future drops that await, not the
/// detached download, which keeps a runtime worker + connection busy until it
/// finishes or hits its own timeout. This token is intentionally not wired into
/// the provider's own `cancel_token`. A host using this for bounded TEARDOWN
/// gets a bounded caller-return, not immediate reclamation of the in-flight
/// download — close the stream (and, for a sync-executed statement, the executed
/// handle) to release those on their own completion.
struct ResultStreamInner {
    volume: Option<std::sync::Arc<crate::volume::VolumeState>>,
    /// Fired by a cloned [`ResultStreamCanceller`] to interrupt an in-flight
    /// `next_batch`. Once fired the stream is permanently aborted: `next_batch`
    /// returns `Cancelled` on every subsequent call rather than re-driving a
    /// partially-consumed reader (the fetch future was dropped mid-await, so
    /// the reader's position is indeterminate).
    cancel: CancellationToken,
    shape: ResultStreamShape,
}

/// The backing shape of a result stream (independent of cancellation).
enum ResultStreamShape {
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

/// Detached canceller for a result stream. Holds a clone of the stream's
/// [`CancellationToken`], so firing it interrupts an in-flight `next_batch`.
/// It also stops a running Volume file transfer.
///
/// A `CancellationToken` clone shares the same underlying atomic state, so the
/// canceller keeps working after the stream handle is closed — closing the
/// stream drops the stream's own token clone but not this one — and firing it
/// then is a harmless no-op (no `next_batch` is in flight). It holds no
/// reference to the stream's `&mut` state, so it is safe to `cancel()` from any
/// thread concurrently with `next_batch`/`close` on the owning thread.
struct ResultStreamCanceller {
    volume: Option<std::sync::Arc<crate::volume::VolumeState>>,
    cancel: CancellationToken,
}

/// Crate-internal: build an owned result-stream handle that lazily drives
/// the given `ResultStream` (used by the metadata FFI). `schema` is
/// captured up front so `get_schema` needs no runtime; each `next_batch`
/// pulls one batch on the runtime.
pub(crate) fn into_raw_result_stream(
    schema: SchemaRef,
    stream: crate::result_stream::ResultStream,
) -> *mut kernel_result_stream_t {
    Box::into_raw(Box::new(ResultStreamInner {
        volume: stream.volume_state(),
        cancel: CancellationToken::new(),
        shape: ResultStreamShape::Owned { schema, stream },
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
    Box::into_raw(Box::new(ResultStreamInner {
        volume: (*exec).result_stream().volume_state(),
        cancel: CancellationToken::new(),
        shape: ResultStreamShape::Executed { exec, valid },
    })) as *mut kernel_result_stream_t
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

/// Shared cancellation wrapper for a single batch fetch (odbc#355-B, review F1).
///
/// Runs `fetch` (the backing shape's `next_batch` future — the owned
/// `ResultStream`'s or the executed handle's) inside a `tokio::select!` against
/// `cancel.cancelled()`, and maps the fetched batch to an owned `RecordBatch`
/// for FFI export. BOTH C ABI stream shapes route their fetch through this one
/// function, so the cancellation/select logic is a single code path (rather than
/// duplicated per arm): the Owned unit test that fires a cancel mid-fetch
/// therefore exercises the exact `select!` the Executed arm uses too.
///
/// `fetch` yields `Result<Option<B>>` where `B` borrows a `ResultBatch` — the
/// owned arm's `next_batch` yields `Option<ResultBatch>` (B = ResultBatch), the
/// executed arm's yields `Option<&ResultBatch>` (B = &ResultBatch); both are
/// `.borrow()`ed and cloned here. `kind` names the shape for the non-Arrow error.
///
/// `biased` prefers the fetch: if a batch and a cancel are both ready, the
/// completed batch is returned rather than discarded by a same-instant cancel.
async fn select_next_batch<B, Fut>(
    fetch: Fut,
    cancel: CancellationToken,
    kind: &str,
) -> crate::kernel_error::Result<Option<RecordBatch>>
where
    B: std::borrow::Borrow<ResultBatch>,
    Fut: std::future::Future<Output = crate::kernel_error::Result<Option<B>>>,
{
    tokio::select! {
        biased;
        fetched = fetch => {
            #[allow(
                unreachable_patterns,
                reason = "forward-compat fallback for future #[non_exhaustive] ResultBatch variants"
            )]
            match fetched? {
                Some(b) => match b.borrow() {
                    ResultBatch::Arrow(rb) => Ok(Some(rb.clone())),
                    _ => Err(Error::not_implemented().message(
                        format!("{kind} yielded a non-Arrow batch; the C ABI exports Arrow only"),
                    )),
                },
                None => Ok(None),
            }
        }
        _ = cancel.cancelled() => Err(Error::cancelled()
            .message("result stream cancelled during next_batch")
            .with_retryable(false)),
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
        if stream.is_null() {
            return store_invalid_arg("kernel_result_stream_get_schema: stream must not be null");
        }
        if out.is_null() {
            return store_invalid_arg("kernel_result_stream_get_schema: out must not be null");
        }
        // Project to `&mut (*p).shape` only, never a whole-struct `&mut` — so a
        // concurrent `kernel_result_stream_canceller_new` (which reads only the
        // disjoint `cancel` field) does not alias it (odbc#355 review F1).
        // SAFETY: live box (non-null checked); `shape` has a single accessor per
        // the no-concurrent-next_batch/get_schema contract.
        let p = stream as *mut ResultStreamInner;
        let shape: &mut ResultStreamShape = &mut *std::ptr::addr_of_mut!((*p).shape);
        let schema = match shape {
            ResultStreamShape::Owned { schema, .. } => schema.clone(),
            ResultStreamShape::Executed { exec, valid } => match executed_ref(*exec, valid) {
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
        if stream.is_null() {
            return store_invalid_arg("kernel_result_stream_next_batch: stream must not be null");
        }
        if out_array.is_null() || out_schema.is_null() {
            return store_invalid_arg(
                "kernel_result_stream_next_batch: out pointers must not be null",
            );
        }

        // Borrow the two fields SEPARATELY through raw-pointer projection rather
        // than forming a single `&mut ResultStreamInner` over the whole struct
        // (odbc#355 review F1). A whole-struct `&mut` carries a noalias guarantee
        // over the ENTIRE allocation, so holding it across the fetch below would
        // be UB the instant another thread calls `kernel_result_stream_canceller
        // _new` — which reads `cancel` and which this API's docs explicitly bless
        // calling while a `next_batch` is in flight. Projecting to `&mut (*p).shape`
        // and `&(*p).cancel` yields references over DISJOINT fields: the fetch's
        // live borrow covers only `shape`, and a concurrent `canceller_new`'s read
        // covers only `cancel` (a `Sync` atomic), so they never alias.
        let p = stream as *mut ResultStreamInner;
        // SAFETY: `p` is a live result-stream box (non-null checked above); the
        // caller's contract forbids a *second* next_batch/close on the same handle
        // concurrently, so `shape` has a single mutable accessor. `cancel` is only
        // read here (clone) and by a concurrent canceller — a disjoint, Sync field.
        let cancel: CancellationToken = (*std::ptr::addr_of!((*p).cancel)).clone();
        let shape: &mut ResultStreamShape = &mut *std::ptr::addr_of_mut!((*p).shape);

        // Abort hook (odbc#355-B). If the stream's cancellation token was fired
        // — either before this call or, below, DURING the fetch — the batch
        // fetch is abandoned and this returns `Cancelled` promptly rather than
        // blocking on a slow (CloudFetch-link) request. Once fired the token
        // stays fired, so every subsequent `next_batch` also returns here: the
        // fetch future was dropped mid-await, leaving the reader's position
        // indeterminate, so re-driving it is not sound. A pre-check short-circuits
        // the already-cancelled case without touching the runtime; the in-flight
        // case is handled by the `select!` inside each fetch future below.
        if cancel.is_cancelled() {
            return store_and_code(
                Error::cancelled()
                    .message("result stream was cancelled")
                    .with_retryable(false),
            );
        }

        // Pull the next owned batch from whichever backing shape this is. Both
        // shapes run their batch fetch through the SAME cancellation wrapper
        // (`select_next_batch`): a `tokio::select!` against a clone of the
        // stream's cancellation token, so a cancel fired from another thread
        // while the fetch is in flight wins the race and unwinds the fetch
        // (dropping its future at the await point) instead of waiting for it to
        // complete. Sharing one wrapper is deliberate (odbc#355 review F1): the
        // cancellation/`select!` logic — the part under test — is then a single
        // code path exercised by the Owned unit test, not duplicated per arm; the
        // arms differ only in HOW they produce the batch future (the owned
        // ResultStream vs the executed handle's drain-driven / lazy-CloudFetch
        // next_batch) and the Executed arm's extra `executed_ref` stale-handle
        // guard.
        let next: Option<Result<RecordBatch, Error>> = match shape {
            // Owned (metadata): drive the owned ResultStream lazily on the
            // runtime, one batch at a time, so an unbounded result streams
            // with bounded memory. Running through block_on_ffi keeps the
            // (possibly CloudFetch-backed) reader on the tokio reactor
            // rather than off a plain C thread.
            ResultStreamShape::Owned { stream, .. } => {
                match block_on_ffi(move || {
                    let stream: &mut crate::result_stream::ResultStream = stream;
                    Box::pin(select_next_batch(
                        stream.next_batch(),
                        cancel,
                        "metadata stream",
                    ))
                }) {
                    Ok(Some(rb)) => Some(Ok(rb)),
                    Ok(None) => None,
                    Err(code) => return code, // error already stored
                }
            }
            ResultStreamShape::Executed { exec, valid } => {
                let exec = match executed_ref(*exec, valid) {
                    Ok(e) => e,
                    Err(code) => return code,
                };
                // Drive the executed handle's own next_batch so the drain-driven
                // auto-close and lazy CloudFetch link fetches keep working. Same
                // cancellation wrapper as the Owned arm above.
                match block_on_ffi(move || {
                    let exec: &mut ExecutedStatement = exec;
                    Box::pin(select_next_batch(
                        exec.next_batch(),
                        cancel,
                        "result stream",
                    ))
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

/// Attempt to fetch one batch without waiting for provider progress.
///
/// # Safety
/// `stream` must be a live result-stream handle from a `kernel_*` call that
/// produces one, and not previously closed; `ready`, `out_array`, and
/// `out_schema` must be valid, writable pointers. On `Success` with
/// `*ready == true` the Arrow outputs are transferred to the caller (an
/// end-of-stream yields a released array); with `*ready == false` they are left
/// untouched.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_try_next_batch(
    stream: *mut kernel_result_stream_t,
    ready: *mut bool,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> KernelStatusCode {
    catch_ffi(|| {
        if stream.is_null() || ready.is_null() || out_array.is_null() || out_schema.is_null() {
            return store_invalid_arg("try_next_batch: pointers must not be null");
        }
        std::ptr::write(ready, false);
        let inner = stream as *mut ResultStreamInner;
        if (*std::ptr::addr_of!((*inner).cancel)).is_cancelled() {
            return store_and_code(
                Error::cancelled()
                    .message("result stream was cancelled")
                    .with_retryable(false),
            );
        }
        let shape = &mut *std::ptr::addr_of_mut!((*inner).shape);
        let outcome = match shape {
            ResultStreamShape::Owned { stream, .. } => stream.try_next_batch(),
            ResultStreamShape::Executed { exec, valid } => {
                let exec = match executed_ref(*exec, valid) {
                    Ok(exec) => exec,
                    Err(code) => return code,
                };
                exec.try_next_batch()
            }
        };
        match outcome {
            Ok(crate::result_stream::TryNextBatch::Pending) => KernelStatusCode::Success,
            Ok(crate::result_stream::TryNextBatch::End) => {
                std::ptr::write(ready, true);
                std::ptr::write(out_array, FFI_ArrowArray::empty());
                std::ptr::write(out_schema, FFI_ArrowSchema::empty());
                KernelStatusCode::Success
            }
            Ok(crate::result_stream::TryNextBatch::Ready(batch)) => {
                #[allow(
                    unreachable_patterns,
                    reason = "forward-compat fallback for future #[non_exhaustive] ResultBatch variants"
                )]
                let arrow = match batch {
                    ResultBatch::Arrow(batch) => batch,
                    _ => return store_and_code(Error::not_implemented().message(
                        "try_next_batch yielded a non-Arrow batch; the C ABI exports Arrow only",
                    )),
                };
                let struct_array: arrow_array::StructArray = arrow.clone().into();
                match arrow_array::ffi::to_ffi(&struct_array.into_data()) {
                    Ok((array, schema)) => {
                        std::ptr::write(ready, true);
                        std::ptr::write(out_array, array);
                        std::ptr::write(out_schema, schema);
                        KernelStatusCode::Success
                    }
                    Err(error) => store_and_code(Error::from(error)),
                }
            }
            Err(error) => store_and_code(error),
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

// ─── Result-stream cancellation (detached canceller) ─────────────────────────

/// Create a detached canceller for `stream` (odbc#355-B). Obtain it while the
/// stream is live; from another thread, `kernel_result_stream_canceller_cancel`
/// then interrupts an in-flight `kernel_result_stream_next_batch`, which returns
/// `KernelStatusCode::Cancelled` promptly instead of blocking until the fetch
/// completes on its own.
///
/// The canceller holds only a clone of the stream's cancellation token (a shared
/// atomic flag), so — unlike the statement canceller — it borrows none of the
/// stream's `&mut` state and issues no server RPC. It is therefore safe to obtain
/// while a `next_batch` is already in flight, and safe to `cancel()`/`free()` in
/// any order relative to `kernel_result_stream_close`: firing it after the stream
/// is closed is a harmless no-op (the shared token outlives the stream's own
/// clone). Free it with `kernel_result_stream_canceller_free`; the free drives no
/// RPC and only drops the box.
///
/// # Safety
/// `stream` must be a live result-stream handle from a `kernel_*` call that
/// produces one; `out` must be a valid, writable pointer. Unlike the statement
/// canceller, a `next_batch` MAY be in flight on another thread when this is
/// called — it reads ONLY the shared `cancel` token field, via raw-pointer
/// projection, and never forms a reference over the whole `ResultStreamInner`.
/// That is what keeps it sound against a concurrent `next_batch`, which borrows
/// only the disjoint `shape` field (a whole-struct `&`/`&mut` would alias even
/// though the fields touched differ — odbc#355 review F1).
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_canceller_new(
    stream: *mut kernel_result_stream_t,
    out: *mut *mut kernel_result_stream_canceller_t,
) -> KernelStatusCode {
    // Wrap the body in catch_ffi (like _cancel / _free) so a panic — e.g. from the
    // Box allocation — cannot unwind across the extern "C" boundary (UB); catch_ffi
    // also clears the thread-local error up front (odbc#355 review).
    catch_ffi(|| {
        if stream.is_null() {
            return store_invalid_arg(
                "kernel_result_stream_canceller_new: stream must not be null",
            );
        }
        if out.is_null() {
            return store_invalid_arg("kernel_result_stream_canceller_new: out must not be null");
        }
        // Read ONLY the `cancel` field through a raw-pointer projection — never a
        // reference over the whole struct — so this is sound even while another
        // thread holds `&mut (*p).shape` inside next_batch (disjoint fields; `cancel`
        // is a `Sync` atomic). SAFETY: `stream` is a live box (non-null checked).
        let p = stream as *mut ResultStreamInner;
        let handle = ResultStreamCanceller {
            volume: (*std::ptr::addr_of!((*p).volume)).clone(),
            cancel: (*std::ptr::addr_of!((*p).cancel)).clone(),
        };
        std::ptr::write(
            out,
            Box::into_raw(Box::new(handle)) as *mut kernel_result_stream_canceller_t,
        );
        KernelStatusCode::Success
    })
}

/// Fire the cancellation of the result stream this canceller was created for
/// (odbc#355-B). Purely client-side (no server RPC): it flips the shared token,
/// which an in-flight `kernel_result_stream_next_batch` observes via its
/// `tokio::select!` and unwinds. It also stops a running Volume file transfer.
/// Idempotent, non-blocking, and safe to call concurrently with
/// `next_batch`/`close` on the stream's owning thread.
///
/// Always returns `Success` for a live handle. After this returns, the stream is
/// permanently aborted: every subsequent `next_batch` returns `Cancelled`.
///
/// # Safety
/// `canceller` must be a live handle from `kernel_result_stream_canceller_new`.
/// Unlike the statement canceller, this does NOT block on an RPC, so it need not
/// be driven from an abandonable thread — but it must still not be freed by
/// another thread concurrently with this call.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_canceller_cancel(
    canceller: *mut kernel_result_stream_canceller_t,
) -> KernelStatusCode {
    catch_ffi(|| {
        let Some(handle) = (canceller as *mut ResultStreamCanceller).as_ref() else {
            return store_invalid_arg(
                "kernel_result_stream_canceller_cancel: handle must not be null",
            );
        };
        if let Some(volume) = &handle.volume {
            volume.cancel();
        }
        handle.cancel.cancel();
        KernelStatusCode::Success
    })
}

/// Free a result-stream canceller handle. Does not touch the originating stream.
/// Drives no RPC (only drops the box), so its order relative to
/// `kernel_result_stream_close` is free.
///
/// # Safety
/// `canceller` must have been returned by `kernel_result_stream_canceller_new`
/// and not previously freed. Must not be freed while another thread is inside
/// `kernel_result_stream_canceller_cancel` on this handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_canceller_free(
    canceller: *mut kernel_result_stream_canceller_t,
) -> KernelStatusCode {
    catch_ffi(|| {
        if canceller.is_null() {
            return store_invalid_arg(
                "kernel_result_stream_canceller_free: handle must not be null",
            );
        }
        drop(Box::from_raw(canceller as *mut ResultStreamCanceller));
        KernelStatusCode::Success
    })
}

/// Same flag, read from the stream. Still succeeds after the executed statement
/// is closed.
/// # Safety
/// `stream` must be live and `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_is_volume_operation(
    stream: *const kernel_result_stream_t,
    out: *mut bool,
) -> KernelStatusCode {
    catch_ffi(|| {
        if stream.is_null() || out.is_null() {
            return store_invalid_arg("Volume marker: null pointer");
        }
        let p = stream as *const ResultStreamInner;
        std::ptr::write(out, (*std::ptr::addr_of!((*p).volume)).is_some());
        KernelStatusCode::Success
    })
}

/// Runs the file transfer for a Volume PUT, GET or REMOVE result. Call before
/// fetching; the rows stay readable afterwards.
/// `allowed_paths_json`: JSON array of local directories the transfer may use.
/// `remove_not_found_is_success`: report success when the remote file is missing.
/// Returns NeedsInputStream for a stream upload, which this API cannot perform.
/// `kernel_result_stream_canceller_cancel` interrupts it from another thread.
/// # Safety
/// `stream` must be live with exclusive fetch access, `allowed_paths_json` must
/// be NUL-terminated UTF-8, and `out_status` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_result_stream_complete_volume_operation(
    stream: *mut kernel_result_stream_t,
    allowed_paths_json: *const std::ffi::c_char,
    remove_not_found_is_success: bool,
    out_status: *mut crate::VolumeCompletion,
) -> KernelStatusCode {
    catch_ffi(|| {
        if stream.is_null() || allowed_paths_json.is_null() || out_status.is_null() {
            return store_invalid_arg("Volume completion: null pointer");
        }
        let roots = match std::ffi::CStr::from_ptr(allowed_paths_json)
            .to_str()
            .ok()
            .and_then(|text| serde_json::from_str::<Vec<String>>(text).ok())
        {
            Some(roots) => roots,
            None => {
                return store_invalid_arg("Volume allowed paths must be a JSON array of strings")
            }
        };
        let options = crate::VolumeOptions::new(
            roots.into_iter().map(Into::into).collect(),
            remove_not_found_is_success,
        );
        let p = stream as *mut ResultStreamInner;
        let shape = &mut *std::ptr::addr_of_mut!((*p).shape);
        let result = match shape {
            ResultStreamShape::Owned { stream, .. } => {
                block_on_ffi(|| stream.complete_volume_operation(&options, None))
            }
            ResultStreamShape::Executed { exec, valid } => {
                let exec = match executed_ref(*exec, valid) {
                    Ok(exec) => exec,
                    Err(code) => return code,
                };
                block_on_ffi(|| {
                    exec.result_stream_mut()
                        .complete_volume_operation(&options, None)
                })
            }
        };
        match result {
            Ok(status) => {
                std::ptr::write(out_status, status);
                KernelStatusCode::Success
            }
            Err(code) => code,
        }
    })
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

    #[test]
    fn volume_completion_observes_detached_cancel() {
        use crate::reader::test_utils::MockReader;
        use crate::result_stream::ResultStream;
        use arrow_array::StringArray;
        use std::ffi::CString;

        let schema = Arc::new(Schema::new(
            ["operation", "presignedUrl", "headers", "localFile"]
                .map(|name| Field::new(name, DataType::Utf8, false))
                .to_vec(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            ["REMOVE", "http://unused.invalid/object", "{}", ""]
                .into_iter()
                .map(|value| Arc::new(StringArray::from(vec![value])) as _)
                .collect(),
        )
        .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let volume = crate::volume::VolumeState::new(
            Default::default(),
            Arc::new(
                crate::client::DatabricksHttpClient::with_default_retry(Default::default())
                    .unwrap()
                    .storage_client(None),
            ),
            Default::default(),
        );
        let stream = rt.block_on(async {
            let mut stream =
                ResultStream::from_reader(Box::new(MockReader::new(vec![batch.clone()])))
                    .await
                    .unwrap();
            stream.attach_volume(Arc::clone(&volume));
            into_raw_result_stream(schema.clone(), stream)
        });
        let mut is_volume = false;
        assert_eq!(
            unsafe { kernel_result_stream_is_volume_operation(stream, &mut is_volume) },
            KernelStatusCode::Success
        );
        assert!(is_volume);
        let roots = CString::new("[]").unwrap();
        let mut status = crate::VolumeCompletion::NotVolume;
        let mut canceller = std::ptr::null_mut();
        assert_eq!(
            unsafe { kernel_result_stream_canceller_new(stream, &mut canceller) },
            KernelStatusCode::Success
        );
        assert_eq!(
            unsafe { kernel_result_stream_canceller_cancel(canceller) },
            KernelStatusCode::Success
        );
        assert_eq!(
            unsafe {
                kernel_result_stream_complete_volume_operation(
                    stream,
                    roots.as_ptr(),
                    false,
                    &mut status,
                )
            },
            KernelStatusCode::Cancelled
        );
        unsafe { kernel_result_stream_canceller_free(canceller) };
        unsafe { kernel_result_stream_close(stream) };
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

        let mut ready = false;
        let rc = unsafe {
            kernel_result_stream_try_next_batch(
                std::ptr::null_mut(),
                &mut ready,
                &mut arr,
                &mut sch,
            )
        };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
    }

    #[test]
    fn try_next_batch_returns_ready_batch_then_end() {
        let (schema, batch) = one_row_batch();
        let stream = owned_stream_over(schema, vec![batch]);
        let mut ready = false;
        let mut arr = FFI_ArrowArray::empty();
        let mut sch = FFI_ArrowSchema::empty();
        assert_eq!(
            unsafe { kernel_result_stream_try_next_batch(stream, &mut ready, &mut arr, &mut sch) },
            KernelStatusCode::Success
        );
        assert!(ready);
        assert!(!arr.is_released());

        ready = false;
        assert_eq!(
            unsafe { kernel_result_stream_try_next_batch(stream, &mut ready, &mut arr, &mut sch) },
            KernelStatusCode::Success
        );
        assert!(ready);
        assert!(arr.is_released());
        unsafe { kernel_result_stream_close(stream) };
    }

    #[test]
    fn try_next_batch_reports_pending_without_entering_async_reader() {
        use crate::reader::test_utils::BlockingReader;
        use crate::result_stream::ResultStream;

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let entered = Arc::new(tokio::sync::Notify::new());
        let reader = BlockingReader {
            schema: schema.clone(),
            entered: entered.clone(),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result_stream =
            rt.block_on(async { ResultStream::from_reader(Box::new(reader)).await.unwrap() });
        let stream = into_raw_result_stream(schema, result_stream);

        let mut ready = true;
        let mut arr = FFI_ArrowArray::empty();
        let mut sch = FFI_ArrowSchema::empty();
        assert_eq!(
            unsafe { kernel_result_stream_try_next_batch(stream, &mut ready, &mut arr, &mut sch) },
            KernelStatusCode::Success
        );
        assert!(!ready);
        unsafe { kernel_result_stream_close(stream) };
    }

    /// A reader that errors on fetch must surface the TYPED error on the
    /// caller thread: `try_next_batch` returns the mapped status code AND
    /// `kernel_get_last_error` reports the structured fields (code / message /
    /// retryable). This pins the `Err(error) => store_and_code(error)` branch —
    /// a regression that dropped the error (falling through with no
    /// `kernel_get_last_error` detail) or lost the typed fields would pass CI
    /// otherwise.
    #[test]
    fn try_next_batch_surfaces_typed_error_on_caller_thread() {
        use crate::reader::test_utils::ErrorReader;
        use crate::result_stream::ResultStream;

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result_stream = rt.block_on(async {
            ResultStream::from_reader(Box::new(ErrorReader {
                schema: schema.clone(),
            }))
            .await
            .unwrap()
        });
        let stream = into_raw_result_stream(schema, result_stream);

        let mut ready = true;
        let mut arr = FFI_ArrowArray::empty();
        let mut sch = FFI_ArrowSchema::empty();
        // `Error::io()` maps to `ErrorCode::Internal` -> `KernelStatusCode::Internal`.
        let rc =
            unsafe { kernel_result_stream_try_next_batch(stream, &mut ready, &mut arr, &mut sch) };
        assert_eq!(rc, KernelStatusCode::Internal);
        assert!(!ready, "a failing fetch must not report a ready batch");
        assert!(arr.is_released(), "no array should be handed out on error");

        // The typed error must be retrievable on THIS (caller) thread.
        let mut err = std::mem::MaybeUninit::<crate::c_abi::error::KernelError>::uninit();
        let present = unsafe { crate::c_abi::error::kernel_get_last_error(err.as_mut_ptr()) };
        assert!(
            present,
            "a typed error must be recorded after a failed fetch"
        );
        let err = unsafe { err.assume_init() };
        assert_eq!(err.code, KernelStatusCode::Internal as i32);
        let msg = unsafe { std::ffi::CStr::from_ptr(err.message) }
            .to_str()
            .unwrap();
        assert!(
            msg.contains("simulated read error"),
            "message should round-trip the reader's error, got: {msg}"
        );

        unsafe { kernel_result_stream_close(stream) };
    }

    /// Canceller null guards (odbc#355-B).
    #[test]
    fn canceller_null_guards_return_invalid_argument() {
        let mut out: *mut kernel_result_stream_canceller_t = std::ptr::null_mut();
        let rc = unsafe { kernel_result_stream_canceller_new(std::ptr::null_mut(), &mut out) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);

        let rc = unsafe { kernel_result_stream_canceller_cancel(std::ptr::null_mut()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);

        let rc = unsafe { kernel_result_stream_canceller_free(std::ptr::null_mut()) };
        assert_eq!(rc, KernelStatusCode::InvalidArgument);
    }

    /// Firing the canceller BEFORE `next_batch` makes it return `Cancelled`
    /// without touching the reader — the pre-check path (odbc#355-B).
    #[test]
    fn cancel_before_next_batch_returns_cancelled() {
        let (schema, batch) = one_row_batch();
        let stream = owned_stream_over(schema, vec![batch]);

        let mut canceller: *mut kernel_result_stream_canceller_t = std::ptr::null_mut();
        assert_eq!(
            unsafe { kernel_result_stream_canceller_new(stream, &mut canceller) },
            KernelStatusCode::Success
        );
        // canceller_cancel itself returns Success (the SIGNAL was delivered);
        // it is the next_batch that then reports Cancelled.
        assert_eq!(
            unsafe { kernel_result_stream_canceller_cancel(canceller) },
            KernelStatusCode::Success
        );

        // The very first next_batch after a cancel returns Cancelled, NOT the
        // buffered batch — the stream is aborted.
        let mut arr = FFI_ArrowArray::empty();
        let mut sch = FFI_ArrowSchema::empty();
        let rc = unsafe { kernel_result_stream_next_batch(stream, &mut arr, &mut sch) };
        assert_eq!(rc, KernelStatusCode::Cancelled);
        assert!(
            arr.is_released(),
            "no batch should be handed out after cancel"
        );

        // A second call still reports Cancelled (permanent abort), never hangs
        // or re-drives the reader.
        let rc2 = unsafe { kernel_result_stream_next_batch(stream, &mut arr, &mut sch) };
        assert_eq!(rc2, KernelStatusCode::Cancelled);

        unsafe { kernel_result_stream_canceller_free(canceller) };
        unsafe { kernel_result_stream_close(stream) };
    }

    /// The headline in-flight test (odbc#355-B): a `next_batch` blocked inside
    /// the reader's fetch is interrupted by a cancel fired from ANOTHER thread,
    /// and returns `Cancelled` promptly rather than waiting for the fetch (which
    /// this test never releases). Without the `select!` against the token, the
    /// consumer thread would block forever on the reader.
    ///
    /// This also exercises the blessed CONCURRENT path (odbc#355 review F1): the
    /// canceller is obtained via `kernel_result_stream_canceller_new` AFTER the
    /// fetch thread has parked inside `next_batch`, i.e. while a `next_batch` is
    /// in flight — the exact race the field-projection soundness fix protects.
    /// Run under `cargo +nightly miri test` / ThreadSanitizer to check the
    /// aliasing at the reference level, not just the observable outcome.
    #[test]
    fn cancel_interrupts_an_in_flight_next_batch() {
        use crate::reader::test_utils::BlockingReader;
        use crate::result_stream::ResultStream;
        use std::sync::atomic::{AtomicI32, Ordering};

        // Raw pointers are not Send; wrap for the deliberate cross-thread move
        // (the C ABI is explicitly callable from any non-runtime thread).
        struct SendPtr<T>(*mut T);
        unsafe impl<T> Send for SendPtr<T> {}
        impl<T> SendPtr<T> {
            fn get(self) -> *mut T {
                self.0
            }
        }

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let reader = BlockingReader {
            schema: schema.clone(),
            entered: entered.clone(),
            release: release.clone(),
        };

        // Build the owned stream over the blocking reader on a throwaway runtime
        // (from_reader is async).
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stream_ptr = {
            let stream =
                rt.block_on(async { ResultStream::from_reader(Box::new(reader)).await.unwrap() });
            into_raw_result_stream(schema, stream)
        };

        // Drive next_batch on a worker thread; it parks inside the reader.
        let stream_send = SendPtr(stream_ptr);
        let rc_slot = Arc::new(AtomicI32::new(-1));
        let rc_slot_thread = rc_slot.clone();
        let fetch_thread = std::thread::spawn(move || {
            let stream = stream_send.get();
            let mut arr = FFI_ArrowArray::empty();
            let mut sch = FFI_ArrowSchema::empty();
            let rc = unsafe { kernel_result_stream_next_batch(stream, &mut arr, &mut sch) };
            rc_slot_thread.store(rc as i32, Ordering::SeqCst);
        });

        // Wait until the reader is definitely parked inside the fetch await, so
        // the cancel lands on a genuinely in-flight next_batch (not before it).
        rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
                .await
                .expect("reader did not enter next_batch within 5s");
        });

        // Obtain the canceller NOW — while next_batch is in flight on the other
        // thread. This is the concurrent-`canceller_new`-during-`next_batch` path
        // the docs bless; it must be sound (disjoint field projection) and must
        // succeed here.
        let mut canceller: *mut kernel_result_stream_canceller_t = std::ptr::null_mut();
        assert_eq!(
            unsafe { kernel_result_stream_canceller_new(stream_ptr, &mut canceller) },
            KernelStatusCode::Success
        );

        // Fire the cancel from this (different) thread; the parked next_batch
        // must unwind. Bound the join so a regression (no interrupt) fails the
        // test instead of hanging — the reader is never released.
        assert_eq!(
            unsafe { kernel_result_stream_canceller_cancel(canceller) },
            KernelStatusCode::Success
        );
        fetch_thread
            .join()
            .expect("next_batch thread panicked or hung");
        assert_eq!(
            rc_slot.load(Ordering::SeqCst),
            KernelStatusCode::Cancelled as i32,
            "an in-flight next_batch interrupted by cancel must return Cancelled"
        );

        unsafe { kernel_result_stream_canceller_free(canceller) };
        unsafe { kernel_result_stream_close(stream_ptr) };
    }

    /// The canceller outlives the stream: closing the stream first, then firing
    /// and freeing the canceller, is a harmless no-op — the shared token clone
    /// keeps working after the stream's own clone is dropped (odbc#355-B).
    #[test]
    fn canceller_outlives_stream_close() {
        let (schema, batch) = one_row_batch();
        let stream = owned_stream_over(schema, vec![batch]);

        let mut canceller: *mut kernel_result_stream_canceller_t = std::ptr::null_mut();
        assert_eq!(
            unsafe { kernel_result_stream_canceller_new(stream, &mut canceller) },
            KernelStatusCode::Success
        );

        // Close the stream BEFORE cancelling/freeing the canceller.
        unsafe { kernel_result_stream_close(stream) };

        // Firing the (now orphaned) canceller is a safe no-op, and freeing it
        // does not touch the freed stream.
        assert_eq!(
            unsafe { kernel_result_stream_canceller_cancel(canceller) },
            KernelStatusCode::Success
        );
        assert_eq!(
            unsafe { kernel_result_stream_canceller_free(canceller) },
            KernelStatusCode::Success
        );
    }
}
