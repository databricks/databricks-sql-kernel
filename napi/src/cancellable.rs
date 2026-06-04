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

//! Cancellable sync-execute handle.
//!
//! The blocking `Connection.executeStatement` builds a kernel
//! `Statement`, awaits `execute()` (which polls to a terminal state),
//! and only then hands JS a `Statement` wrapper. A query that runs for
//! several seconds server-side is therefore *uncancellable* from JS on
//! that path — there is no handle to call `cancel()` on until the
//! blocking call has already resolved.
//!
//! `Connection.executeStatementCancellable` closes that gap. It builds
//! the kernel `Statement`, captures a detached
//! [`StatementCanceller`](databricks_sql_kernel::StatementCanceller)
//! **before** dispatching `execute()`, and returns a
//! `CancellableExecution` handle that exposes:
//!
//! - `cancel()` — fires the detached canceller (session + the shared
//!   in-flight-id slot). Lock-free: it does not contend with the
//!   in-flight `result()`. Once the transport publishes the server
//!   statement id mid-execute, the cancel reaches a still-running query;
//!   if fired before the id is observed it is a no-op (the kernel's
//!   per-execute drop guard still covers the future-dropped case).
//! - `result()` — drives the blocking `execute()` and resolves to a
//!   `Statement` (the same wrapper `executeStatement` returns), ready to
//!   stream batches.
//!
//! This mirrors `AsyncStatement` on the submit path (which captures an
//! `AsyncStatementCanceller` at construction so `cancel()` interrupts an
//! in-flight `awaitResult()`), and the pyo3 `Statement.canceller()` /
//! `Statement.execute()` split from PR #121 — the canceller is obtained
//! before the blocking drive so a concurrent task can cancel a running
//! query.
//!
//! **API-shape note (vs pyo3):** the two bindings expose the same capability
//! with different shapes. pyo3 splits it across the statement builder —
//! `stmt.canceller()` returns a detached canceller and `stmt.execute()` drives
//! the blocking call separately. napi instead returns a single combined
//! `CancellableExecution` from `Connection.executeStatementCancellable(...)`
//! that owns both halves (`cancel()` + `result()`), because the napi surface
//! has no exposed builder-shape `Statement` to hang a `canceller()` off. Same
//! semantics, fewer moving parts for the JS caller.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_sql_kernel::{
    Error as KernelError, ErrorCode, Statement as KernelStatement, StatementCanceller,
};

use crate::error::napi_err_from_kernel;
use crate::statement::{statement_closed_err, Statement};
use crate::util::guarded;

/// Handle returned by `Connection.executeStatementCancellable`. Owns the
/// built-but-not-yet-executed kernel `Statement` plus a detached
/// [`StatementCanceller`] captured before dispatch, so JS can fire a
/// server-side cancel while the blocking `result()` is in flight.
///
/// `pending` is `Arc<Mutex<Option<KernelStatement>>>` so `result()` can
/// `.take()` the statement (the kernel `execute()` borrows it `&mut`,
/// then it moves into the produced `Statement` wrapper to keep its
/// `ValidityFlag` set — see `statement.rs`). A second `result()` call
/// after the first resolved surfaces `InvalidStatementHandle`.
#[napi]
pub struct CancellableExecution {
    /// The built kernel `Statement`, awaiting its blocking `execute()`.
    /// Taken by the first `result()` call.
    pending: Arc<Mutex<Option<KernelStatement>>>,
    /// Detached canceller (session + shared in-flight-id slot), captured
    /// before `execute()` dispatches. Held **outside** `pending` so
    /// `cancel()` never contends for the mutex `result()` holds across
    /// the blocking drive. Cheap to clone (two `Arc` bumps).
    canceller: StatementCanceller,
    /// Lock-free guard against a `result()` after the statement was
    /// taken. Set by `result()` before it takes `pending`; read by a
    /// second `result()` so the double-drive surfaces
    /// `InvalidStatementHandle` without taking the mutex.
    consumed: Arc<AtomicBool>,
    /// Set by `cancel()`. Read in `result()`'s error arm to normalise the
    /// kernel's sync-cancel quirk (`execute()` surfaces a server-side cancel
    /// as `InvalidArgument`, not `Cancelled`) into `Cancelled`, so every napi
    /// consumer sees a consistent cancelled status — matching the async path.
    cancel_fired: Arc<AtomicBool>,
    /// Set by `result()` once it resolves successfully. Read by `cancel()` to
    /// skip a redundant server-side cancel RPC on an already-finished
    /// statement (parity with `AsyncStatement`/`Statement` cancel gating).
    finished: Arc<AtomicBool>,
}

impl CancellableExecution {
    /// Crate-internal constructor — called from
    /// `Connection::execute_statement_cancellable` once the kernel
    /// `Statement` is built (SQL + params + conf set) but **before**
    /// `execute()` is dispatched. Captures the canceller off the
    /// statement here so it targets the execution `result()` will drive.
    pub(crate) fn new(stmt: KernelStatement) -> Self {
        let canceller = stmt.canceller();
        Self {
            pending: Arc::new(Mutex::new(Some(stmt))),
            canceller,
            consumed: Arc::new(AtomicBool::new(false)),
            cancel_fired: Arc::new(AtomicBool::new(false)),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[napi]
impl CancellableExecution {
    /// The server-issued statement id this execution targets, if the
    /// server has issued one yet (`null` before the initial submit
    /// round-trip publishes it mid-`result()`). Useful for log
    /// correlation while the blocking drive is in flight.
    #[napi(getter)]
    pub fn statement_id(&self) -> Option<String> {
        self.canceller.statement_id()
    }

    /// Drive the blocking `execute()` and resolve to a `Statement`
    /// (identical to what `executeStatement` returns) once the kernel
    /// reaches a terminal state and the result stream is ready.
    ///
    /// Consumes the pending statement: a second `result()` call returns
    /// `KernelError(InvalidStatementHandle)`. The future is
    /// drop-cancel-safe — the kernel's per-execute `MidExecuteCancelState`
    /// guard fires a fire-and-forget `cancel_statement` if this future is
    /// dropped mid-flight (`Promise.race` / timeout loser), independently
    /// of an explicit `cancel()`.
    ///
    /// On a server-side cancel the kernel's blocking `execute()` currently
    /// surfaces `InvalidArgument` (a known kernel quirk — the async path
    /// returns `Cancelled`). When this handle's `cancel()` fired, we normalise
    /// that into `Cancelled` here so JS callers can rely on a single
    /// cancelled-status code regardless of execution path.
    #[napi]
    pub async fn result(&self) -> napi::Result<Statement> {
        // Mark consumed before taking, so a racing second `result()`
        // short-circuits to the closed error rather than awaiting a
        // taken-out statement.
        if self.consumed.swap(true, Ordering::AcqRel) {
            return Err(statement_closed_err());
        }
        let pending = Arc::clone(&self.pending);
        let cancel_fired = Arc::clone(&self.cancel_fired);
        let finished = Arc::clone(&self.finished);
        guarded(async move {
            let mut stmt = {
                let mut guard = pending.lock().await;
                guard.take().ok_or_else(statement_closed_err)?
            };
            match stmt.execute().await {
                Ok(executed) => {
                    // Mark finished so a later `cancel()` skips a redundant RPC.
                    finished.store(true, Ordering::Release);
                    // Pass the parent kernel `Statement` into the JS wrapper so
                    // its `Drop` doesn't invalidate the executed handle via the
                    // shared `ValidityFlag` — same rationale as
                    // `Connection::execute_statement`.
                    Ok(Statement::from_executed(stmt, executed))
                }
                Err(kernel_err) => {
                    // Normalise the sync-cancel `InvalidArgument` quirk into
                    // `Cancelled` when the caller fired `cancel()`. Any other
                    // failure (or a non-cancel `InvalidArgument`) passes through
                    // verbatim.
                    if cancel_fired.load(Ordering::Acquire)
                        && kernel_err.code == ErrorCode::InvalidArgument
                    {
                        Err(napi_err_from_kernel(
                            KernelError::cancelled().message("statement cancelled by client"),
                        ))
                    } else {
                        Err(napi_err_from_kernel(kernel_err))
                    }
                }
            }
        })
        .await
    }

    /// Server-side cancel of the in-flight statement.
    ///
    /// Lock-free: fires the detached `StatementCanceller` captured at
    /// construction rather than taking the mutex `result()` holds, so it
    /// interrupts a still-running blocking `result()` instead of queueing
    /// behind it. No-op (returns `Ok`) if `result()` already finished
    /// successfully, or if no statement id has been observed yet (query still
    /// in its initial submit round-trip), and idempotent against a server
    /// already in a terminal state.
    #[napi]
    pub async fn cancel(&self) -> napi::Result<()> {
        // Skip the server-side cancel RPC if the statement already finished —
        // it would be a redundant no-op-at-server call and a confusing log line.
        if self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        // Record intent so `result()` can normalise the kernel's sync-cancel
        // `InvalidArgument` into `Cancelled`.
        self.cancel_fired.store(true, Ordering::Release);
        let canceller = self.canceller.clone();
        guarded(async move { canceller.cancel().await.map_err(napi_err_from_kernel) }).await
    }
}
