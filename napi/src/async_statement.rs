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

//! Opaque `AsyncStatement` + `AsyncResultHandle` wrappers around the
//! kernel's submit/poll/await_result path.
//!
//! - `Connection.submitStatement(sql, options?)` builds a kernel
//!   `Statement`, calls `submit()` (which sends `wait_timeout=0s` so
//!   the server returns Pending/Running immediately with a
//!   statement_id), and wraps the returned `ExecutedAsyncStatement`
//!   in this napi class. JS callers drive polling via `status()` or
//!   `awaitResult()`.
//!
//! - `AsyncStatement.status()` issues a single GetStatementStatus
//!   RPC, mapped to a JS-side string enum
//!   (`'Pending'|'Running'|'Succeeded'|'Failed'|'Cancelled'|'Closed'`).
//!   The Failed variant carries the same error envelope the sync
//!   path produces (sql_state / error_code / query_id when present).
//!
//! - `AsyncStatement.awaitResult()` blocks until the server reaches
//!   a terminal state, then returns an `AsyncResultHandle` over the
//!   materialised `ResultStream`. It holds `inner.lock()` across the
//!   whole poll. Two independent cancellation paths cover it:
//!   (1) the kernel's own `AwaitResultCancelGuard` handles drop-cancel
//!   — if the JS Promise's underlying tokio future is dropped mid-poll
//!   (a timeout wrapper, a `tokio::select!` loser, a `Promise.race`
//!   loser), the kernel fires a fire-and-forget `cancel_statement` so
//!   the server stops charging DBUs on the orphaned query; and
//!   (2) an explicit `AsyncStatement.cancel()` (see below) fires a
//!   server-side cancel *without* taking the lock, flipping the
//!   statement terminal so the parked `awaitResult()` poll observes
//!   `Cancelled` and returns. The `util::guarded` `catch_unwind`
//!   wrapper applied to every entry here covers the
//!   V8-panic-across-boundary case on top of both.
//!
//! `close()` routes through the `ExecutedStatementHandle` trait the
//! kernel implements for both `ExecutedStatement` and
//! `ExecutedAsyncStatement`, holding `inner.lock()` like the sync path.
//! `cancel()` is the one method that does *not* take the lock — it
//! fires through a detached `AsyncStatementCanceller` (session +
//! statement_id) so it can interrupt an in-flight `awaitResult()`
//! rather than queue behind it. The `Arc<Mutex<Option<…>>>` shape is
//! otherwise the same as `Connection`/`Statement` so Drop's
//! fire-and-forget cleanup behaves consistently.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use arrow_schema::SchemaRef;
use databricks_sql_kernel::{
    AsyncStatementCanceller, Error as KernelError, ExecutedAsyncStatement, ExecutedStatementHandle,
    ResultBatch, ResultStream, Statement as KernelStatement, StatementStatus,
};

use crate::error::napi_err_from_kernel;
use crate::result::{encode_ipc_stream, ArrowBatch, ArrowSchema};
use crate::runtime;
use crate::util::guarded;

// ───────────────────────── shared helpers ─────────────────────────

/// Closed-state error builder for both `AsyncStatement` and
/// `AsyncResultHandle`. Mirrors the sync `Statement` path's
/// `statement_closed_err` so the TS reviver sees the same
/// `KernelError(code='InvalidStatementHandle')` shape regardless of
/// which napi class produced the error.
fn closed_err(what: &str) -> napi::Error {
    napi_err_from_kernel(
        KernelError::invalid_statement_handle().message(format!("{what} already closed")),
    )
}

// Arrow IPC encoding is shared with the sync `Statement` path via
// `crate::result::encode_ipc_stream` (its single home — see the
// encoder-lift refactor). Both paths emit byte-identical envelopes and
// route encode failures through the same kernel error envelope.

// ───────────────────────── AsyncStatement ─────────────────────────

/// Inner state for the JS-visible `AsyncStatement` wrapper. Same
/// shape rationale as `StatementInner` in `statement.rs`: we hold
/// the parent kernel `Statement` alive so its `Drop` (which
/// invalidates the executed handle via the shared `ValidityFlag`)
/// doesn't fire while JS still holds this wrapper.
struct AsyncStatementInner {
    /// The kernel's `Statement` (mutable). Kept alive solely to
    /// prevent its `Drop` from invalidating `executed_async`.
    _parent: KernelStatement,
    /// The kernel async-consumption handle — drives `status()` /
    /// `awaitResult()` / `cancel()` / `close()`.
    executed_async: ExecutedAsyncStatement,
}

/// Opaque async-statement handle.
///
/// Returned by `Connection.submitStatement(...)` after the kernel
/// `Statement::submit()` returns (server sent `wait_timeout=0s`, so
/// the response carries a `statement_id` but the statement is still
/// `Pending`/`Running`). JS drives polling via `status()` /
/// `awaitResult()`.
///
/// Concurrency shape: `status()`, `awaitResult()`, and `close()` take
/// `inner.lock()` and hold the guard across the kernel `.await` (tokio
/// `Mutex` is FIFO), so `status()` / `close()` queue behind any
/// in-flight `awaitResult()` until it returns naturally. `cancel()` is
/// the deliberate exception: it does **not** touch `inner` — it fires
/// through the detached `AsyncStatementCanceller` (session +
/// statement_id, captured at construction), so an explicit
/// `stmt.cancel()` interrupts an in-flight `awaitResult()` instead of
/// queueing behind it. The server-side cancel flips the statement
/// terminal, which the parked `awaitResult()` poll loop observes
/// (`Cancelled`) and returns on. The kernel's `AwaitResultCancelGuard`
/// still covers the drop-cancel case (Promise.race / timeout)
/// independently — see module docs.
#[napi]
pub struct AsyncStatement {
    inner: Arc<Mutex<Option<AsyncStatementInner>>>,
    /// Detached canceller (session handle + statement_id), captured at
    /// construction and held **outside** `inner` so `cancel()` can fire
    /// a server-side cancel without contending for the mutex an
    /// in-flight `awaitResult()` holds. See the struct-level
    /// concurrency note.
    canceller: AsyncStatementCanceller,
    /// Lock-free mirror of "has `close()` taken `inner`". Set by
    /// `close()`; read by the lock-free `cancel()` so a cancel after an
    /// explicit close still surfaces `InvalidStatementHandle` without
    /// taking the mutex.
    closed: Arc<AtomicBool>,
    /// Server-issued statement id. Cached at construction; readable
    /// even after `close()` for log correlation against kernel /
    /// server logs which key on the same id.
    statement_id: String,
}

impl AsyncStatement {
    /// Crate-internal constructor — called from
    /// `Connection::submit_statement` once the kernel hands back
    /// the `ExecutedAsyncStatement`. The parent kernel `Statement`
    /// is passed in alongside so its `Drop` doesn't fire while JS
    /// still holds the executed-async handle (see
    /// `StatementInner` docs in `statement.rs` for the
    /// validity-flag rationale).
    pub(crate) fn from_executed_async(
        parent: KernelStatement,
        executed_async: ExecutedAsyncStatement,
    ) -> Self {
        let statement_id = executed_async.statement_id().to_owned();
        // Capture the detached canceller before `executed_async` moves
        // into the mutex, so `cancel()` never needs `inner.lock()`.
        let canceller = executed_async.canceller();
        Self {
            inner: Arc::new(Mutex::new(Some(AsyncStatementInner {
                _parent: parent,
                executed_async,
            }))),
            canceller,
            closed: Arc::new(AtomicBool::new(false)),
            statement_id,
        }
    }
}

#[napi]
impl AsyncStatement {
    /// Server-issued statement id. Cached at construction; readable
    /// even after `close()` so JS-side log lines can correlate
    /// against kernel / server logs which key on the same id.
    #[napi(getter)]
    pub fn statement_id(&self) -> &str {
        &self.statement_id
    }

    /// One-shot status check. Returns a string enum matching the
    /// kernel `StatementStatus` shape:
    /// `'Pending' | 'Running' | 'Succeeded' | 'Failed' |
    /// 'Cancelled' | 'Closed'`. Returns
    /// `KernelError(InvalidStatementHandle)` if the statement has
    /// been explicitly `close()`d.
    ///
    /// The `Failed` variant collapses to the string `'Failed'` on
    /// the JS side; the underlying error envelope (sql_state /
    /// error_code / query_id) is surfaced by `awaitResult()`'s
    /// rejection, which is where callers actually need the typed
    /// error. `status()` is intended for polling progress UIs
    /// that only need the state name.
    #[napi]
    pub async fn status(&self) -> napi::Result<String> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(|| closed_err("AsyncStatement"))?;
            let status = inner_state
                .executed_async
                .status()
                .await
                .map_err(napi_err_from_kernel)?;
            Ok(status_to_string(&status))
        })
        .await
    }

    /// Block until the server reaches a terminal state, then return
    /// an `AsyncResultHandle` that wraps the materialised result
    /// stream. The handle exposes `fetchNextBatch()` / `schema()`
    /// for consuming the result, plus `statementId` for log
    /// correlation.
    ///
    /// Drop-cancel safety: kernel `await_result` installs
    /// `AwaitResultCancelGuard` which fires a fire-and-forget
    /// `cancel_statement` if the future is dropped mid-poll
    /// (timeout, tokio::select! loser, JS-side `Promise.race`
    /// loser). The `util::guarded` `catch_unwind` here covers the
    /// V8-panic-across-boundary case on top. Returns
    /// `KernelError(InvalidStatementHandle)` if the statement has
    /// been explicitly `close()`d.
    #[napi]
    pub async fn await_result(&self) -> napi::Result<AsyncResultHandle> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            // Acquire and HOLD the lock across the kernel await:
            // dropping the guard while the long-running
            // `await_result()` is in-flight would let `close()` /
            // `status()` race for the inner state. `cancel()` is
            // intentionally lock-free (it fires the detached
            // `AsyncStatementCanceller`, not the mutex-guarded handle),
            // so it can still interrupt this await; the kernel-side
            // `AwaitResultCancelGuard` covers the drop-the-future case.
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(|| closed_err("AsyncStatement"))?;
            let stream = inner_state
                .executed_async
                .await_result()
                .await
                .map_err(napi_err_from_kernel)?;
            let statement_id = inner_state.executed_async.statement_id().to_owned();
            let schema = stream.schema();
            Ok(AsyncResultHandle {
                inner: Arc::new(Mutex::new(Some(stream))),
                schema,
                statement_id,
            })
        })
        .await
    }

    /// Server-side cancel. Returns
    /// `KernelError(InvalidStatementHandle)` if the statement has
    /// been explicitly `close()`d. Idempotent against a server
    /// that already reached a terminal state — the kernel's
    /// `cancel_statement` is a no-op there.
    ///
    /// **Lock-free by design.** Unlike `status()` / `awaitResult()` /
    /// `close()`, this does not take `inner.lock()` — it fires through
    /// the detached `AsyncStatementCanceller` captured at construction.
    /// That lets `stmt.cancel()` interrupt an in-flight `awaitResult()`
    /// (which holds the mutex for the whole poll) instead of queueing
    /// behind it: the server-side cancel flips the statement terminal,
    /// the parked `awaitResult()` poll loop observes `Cancelled` and
    /// returns. The closed-state check reads a lock-free flag so a
    /// cancel after an explicit `close()` still surfaces
    /// `InvalidStatementHandle`.
    #[napi]
    pub async fn cancel(&self) -> napi::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(closed_err("AsyncStatement"));
        }
        let canceller = self.canceller.clone();
        guarded(async move { canceller.cancel().await.map_err(napi_err_from_kernel) }).await
    }

    /// Explicit close. Idempotent — a second call on an
    /// already-closed handle returns `Ok(())`. On `Err`, the napi
    /// inner is already `None`, so a JS-side retry sees the
    /// closed-handle short-circuit and returns `Ok(())` without
    /// re-attempting the wire call. The kernel's own `Drop`
    /// fire-and-forget retry runs once in the background.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let inner = Arc::clone(&self.inner);
        let closed = Arc::clone(&self.closed);
        guarded(async move {
            let taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            let Some(inner_state) = taken else {
                return Ok(());
            };
            // Flip the lock-free flag once we own the close so a racing
            // lock-free `cancel()` short-circuits to InvalidStatementHandle
            // rather than firing a wire cancel on a closing statement.
            closed.store(true, Ordering::Release);
            inner_state
                .executed_async
                .close()
                .await
                .map_err(napi_err_from_kernel)
        })
        .await
    }
}

impl Drop for AsyncStatement {
    fn drop(&mut self) {
        // Best-effort cleanup — same shape as `Connection::Drop`.
        // JS callers needing a guaranteed close-with-error-surface
        // call `close()` explicitly.
        let Some(handle) = runtime::try_get_handle() else {
            return;
        };
        let inner = Arc::clone(&self.inner);
        handle.spawn(async move {
            let _taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
        });
    }
}

// ─────────────────────── AsyncResultHandle ────────────────────────

/// Opaque result-fetch handle returned by
/// `AsyncStatement.awaitResult()`. Wraps a kernel `ResultStream`
/// directly; structurally analogous to the sync `Statement`'s
/// fetch-side surface (`fetchNextBatch` / `schema` /
/// `statementId`).
///
/// `cancel()` / `close()` are not exposed: the parent
/// `AsyncStatement` owns server-side lifecycle. A `close()` here
/// would create dual-ownership of the same statement_id with
/// inconsistent close semantics. Callers `close()` the parent
/// `AsyncStatement` after they're done fetching.
///
/// Schema is cached at construction so it survives the underlying
/// stream being drained; mirrors the sync `Statement.schema()`
/// post-close contract.
#[napi]
pub struct AsyncResultHandle {
    /// `Option<ResultStream>` only so a future explicit-close path
    /// can `.take()` consistently. Today the slot is filled at
    /// construction and never emptied until Drop.
    inner: Arc<Mutex<Option<ResultStream>>>,
    schema: SchemaRef,
    statement_id: String,
}

#[napi]
impl AsyncResultHandle {
    /// Server-issued statement id. Cached at construction; readable
    /// for log correlation. Matches the parent `AsyncStatement`'s
    /// `statementId`.
    #[napi(getter)]
    pub fn statement_id(&self) -> &str {
        &self.statement_id
    }

    /// Pull the next batch of results. Returns `null` when the
    /// stream is exhausted. The returned `ArrowBatch.ipcBytes` is a
    /// complete Arrow IPC stream (schema header + 1 record-batch
    /// message), suitable for handing to `apache-arrow`'s
    /// `RecordBatchReader`. Byte-identical to the sync
    /// `Statement.fetchNextBatch()` payload for the same query.
    #[napi]
    pub async fn fetch_next_batch(&self) -> napi::Result<Option<ArrowBatch>> {
        let inner = Arc::clone(&self.inner);
        let schema = self.schema.clone();
        guarded(async move {
            let mut guard = inner.lock().await;
            let stream = guard
                .as_mut()
                .ok_or_else(|| closed_err("AsyncResultHandle"))?;
            let maybe_batch = stream.next_batch().await.map_err(napi_err_from_kernel)?;
            let Some(batch) = maybe_batch else {
                return Ok(None);
            };
            let record_batch = match batch {
                ResultBatch::Arrow(rb) => rb,
                _ => {
                    return Err(napi_err_from_kernel(
                        KernelError::invalid_argument().message(format!(
                            "non-Arrow ResultBatch variant from kernel — \
                             databricks-sea-native v{} cannot decode this batch \
                             shape, upgrade the binding",
                            env!("CARGO_PKG_VERSION"),
                        )),
                    ));
                }
            };
            let bytes = encode_ipc_stream(&schema, Some(record_batch))?;
            Ok(Some(ArrowBatch {
                ipc_bytes: bytes.into(),
            }))
        })
        .await
    }

    /// Result schema as an Arrow IPC payload (schema header only,
    /// no record-batch message). Available before any batches have
    /// been fetched. Sync because the body has no `.await` —
    /// `encode_ipc_stream` is pure CPU work over the cached
    /// `Arc<Schema>`.
    #[napi]
    pub fn schema(&self) -> napi::Result<ArrowSchema> {
        let bytes = encode_ipc_stream(&self.schema, None)?;
        Ok(ArrowSchema {
            ipc_bytes: bytes.into(),
        })
    }
}

// ───────────────────────── status mapping ─────────────────────────

/// Map `StatementStatus` to the JS-visible string. The `Failed`
/// variant collapses to `"Failed"`; the underlying error envelope
/// is surfaced only via `awaitResult()`'s rejection, which is where
/// callers actually need the typed error. `status()` is for
/// progress UIs.
fn status_to_string(s: &StatementStatus) -> String {
    match s {
        StatementStatus::Pending => "Pending",
        StatementStatus::Running => "Running",
        StatementStatus::Succeeded => "Succeeded",
        StatementStatus::Failed(_) => "Failed",
        StatementStatus::Cancelled => "Cancelled",
        StatementStatus::Closed => "Closed",
        // `StatementStatus` is `#[non_exhaustive]`. A future kernel
        // variant the binding doesn't recognise surfaces as
        // `"Unknown"` so JS callers see something deterministic
        // instead of a panic; upgrading the binding maps the variant
        // to its real string. If you see this in production logs,
        // the binding is older than the kernel it's linked against.
        _ => "Unknown",
    }
    .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_to_string_covers_all_variants() {
        assert_eq!(status_to_string(&StatementStatus::Pending), "Pending");
        assert_eq!(status_to_string(&StatementStatus::Running), "Running");
        assert_eq!(status_to_string(&StatementStatus::Succeeded), "Succeeded");
        assert_eq!(status_to_string(&StatementStatus::Cancelled), "Cancelled");
        assert_eq!(status_to_string(&StatementStatus::Closed), "Closed");
        let failed = StatementStatus::Failed(KernelError::sql_error().message("boom"));
        assert_eq!(status_to_string(&failed), "Failed");
    }

    #[test]
    fn closed_err_carries_invalid_statement_handle_envelope() {
        let err = closed_err("AsyncStatement");
        assert!(
            err.reason.starts_with(crate::error::ERROR_SENTINEL),
            "expected envelope-sentinel-prefixed error, got: {}",
            err.reason,
        );
        assert!(
            err.reason.contains(r#""code":"InvalidStatementHandle""#),
            "envelope payload missing InvalidStatementHandle: {}",
            err.reason,
        );
        assert!(
            err.reason.contains("AsyncStatement already closed"),
            "closed-state error must carry the human message: {}",
            err.reason,
        );
    }

    #[test]
    fn closed_err_status_collapses_to_invalid_arg() {
        let err = closed_err("AsyncResultHandle");
        assert_eq!(err.status, napi::Status::InvalidArg);
    }
}
