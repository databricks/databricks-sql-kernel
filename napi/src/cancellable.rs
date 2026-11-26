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
    /// Set by `cancel()` **only when it actually dispatched a server-side
    /// cancel** (a statement id was already observed). Read in `result()`'s
    /// error arm to normalise the kernel's sync-cancel quirk (`execute()`
    /// surfaces a server-side cancel as `InvalidArgument`, not `Cancelled`)
    /// into `Cancelled`. Gating on real dispatch — rather than merely "JS
    /// called `cancel()`" — is what keeps a first-window no-op `cancel()` from
    /// mis-attributing an unrelated `InvalidArgument` (a genuine SQL/parse
    /// error) as cancelled and discarding its diagnostic.
    cancel_fired: Arc<AtomicBool>,
    /// Set by `result()` once it reaches a terminal state (success **or**
    /// error). Read by `cancel()` to skip a redundant server-side cancel RPC on
    /// an already-finished statement (parity with `AsyncStatement`/`Statement`
    /// cancel gating).
    terminal: Arc<AtomicBool>,
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
            terminal: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Normalise a kernel `execute()` error for the napi sync path. When this
/// handle actually dispatched a server-side cancel (`cancel_fired`), the
/// kernel's sync-cancel quirk — a cancel surfaced as `InvalidArgument` rather
/// than `Cancelled` (the async path returns `Cancelled`) — is translated to
/// `Cancelled` so JS callers get one cancelled-status code. Every other error,
/// and an `InvalidArgument` when no cancel was dispatched (a genuine SQL/parse
/// error), passes through verbatim with its original diagnostic intact.
///
/// Pure + side-effect-free so the predicate can be unit-tested without a
/// session (see the `tests` module).
fn normalize_execute_err(err: KernelError, cancel_fired: bool) -> KernelError {
    if cancel_fired && err.code == ErrorCode::InvalidArgument {
        KernelError::cancelled().message("statement cancelled by client")
    } else {
        err
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
    /// returns `Cancelled`). When this handle's `cancel()` actually dispatched a
    /// server-side cancel, we normalise that into `Cancelled` here so JS callers
    /// can rely on a single cancelled-status code regardless of execution path.
    ///
    /// Three outcomes can race the blocking drive: (1) a natural terminal state
    /// → `Ok` or the genuine error; (2) an explicit `cancel()` that dispatched a
    /// server cancel → this `result()` rejects with a `Cancelled`-coded error
    /// (the normalisation above); (3) the future being **dropped** mid-flight
    /// (`Promise.race`/timeout loser) → the kernel's `MidExecuteCancelState`
    /// drop-guard fires a fire-and-forget `cancel_statement`, but there is no
    /// `result()` left to observe a code. Only (2) yields a `Cancelled` error.
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
        let terminal = Arc::clone(&self.terminal);
        guarded(async move {
            let mut stmt = {
                let mut guard = pending.lock().await;
                guard.take().ok_or_else(statement_closed_err)?
            };
            let outcome = stmt.execute().await;
            // The statement is terminal either way now — let a racing/later
            // `cancel()` short-circuit rather than fire a redundant RPC.
            terminal.store(true, Ordering::Release);
            match outcome {
                // Pass the parent kernel `Statement` into the JS wrapper so its
                // `Drop` doesn't invalidate the executed handle via the shared
                // `ValidityFlag` — same rationale as `Connection::execute_statement`.
                Ok(executed) => Ok(Statement::from_executed(stmt, executed)),
                Err(kernel_err) => Err(napi_err_from_kernel(normalize_execute_err(
                    kernel_err,
                    cancel_fired.load(Ordering::Acquire),
                ))),
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
    /// already in a terminal state. It still stops a Volume transfer in progress
    /// after `result()` has finished.
    #[napi]
    pub async fn cancel(&self) -> napi::Result<()> {
        self.canceller.cancel_volume_operation();
        // Skip the server-side cancel RPC once the statement is terminal (success
        // OR error) — it would be a redundant no-op-at-server call + confusing log.
        if self.terminal.load(Ordering::Acquire) {
            return Ok(());
        }
        // Only flag `cancel_fired` when a server-side cancel is actually
        // dispatched. `canceller.cancel()` reports whether it dispatched (an id
        // had been observed) from the SAME id read that makes the decision, so
        // there is no time-of-check/time-of-use gap. A first-window `cancel()`
        // (before the id is published) returns `false` and must NOT arm the
        // `InvalidArgument`→`Cancelled` normalisation — otherwise a later genuine
        // `InvalidArgument` (a real SQL/parse error) would be mislabelled
        // "cancelled" with its diagnostic discarded. A pre-check on
        // `statement_id()` here would be racy (the id can publish between the
        // check and the canceller's own read); using the return value closes it.
        let canceller = self.canceller.clone();
        let cancel_fired = Arc::clone(&self.cancel_fired);
        guarded(async move {
            let dispatched = canceller.cancel().await.map_err(napi_err_from_kernel)?;
            if dispatched {
                cancel_fired.store(true, Ordering::Release);
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_execute_err;
    use databricks_sql_kernel::{Error as KernelError, ErrorCode};

    // The load-bearing wire-contract logic: which `execute()` errors become
    // `Cancelled`. Driven synthetically — no live session required.

    #[test]
    fn normalizes_invalid_argument_to_cancelled_when_cancel_dispatched() {
        let out = normalize_execute_err(KernelError::invalid_argument(), true);
        assert_eq!(out.code, ErrorCode::Cancelled);
    }

    #[test]
    fn preserves_invalid_argument_when_no_cancel_dispatched() {
        // A genuine SQL/parse error (InvalidArgument) with no dispatched cancel
        // must keep its code + diagnostic — never mislabelled "cancelled".
        let out = normalize_execute_err(KernelError::invalid_argument(), false);
        assert_eq!(out.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn preserves_non_invalid_argument_errors_even_after_cancel() {
        // Only the sync-cancel `InvalidArgument` quirk is normalised; any other
        // error passes through verbatim regardless of the cancel flag.
        let out = normalize_execute_err(KernelError::network_error(), true);
        assert_eq!(out.code, ErrorCode::NetworkError);
    }
}
