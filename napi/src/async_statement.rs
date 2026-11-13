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
//! `close()` routes through the kernel's concrete async handle close,
//! holding `inner.lock()` like the sync path.
//! `cancel()` is the one method that does *not* take the lock — it
//! fires through a detached `AsyncStatementCanceller` (session +
//! statement_id) so it can interrupt an in-flight `awaitResult()`
//! rather than queue behind it. The `Arc<Mutex<Option<…>>>` shape is
//! otherwise the same as `Connection`/`Statement` so Drop's
//! fire-and-forget cleanup behaves consistently.
//!
//! `close()` also releases the canceller and the result streams of the
//! `AsyncResultHandle`s it produced. Both hold the kernel session, and with
//! it the host's shared telemetry client, so leaving them to JS GC kept that
//! client (and its configuration) alive after every session had closed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, Weak};
use tokio::sync::Mutex;

use arrow_schema::SchemaRef;
use databricks_sql_kernel::{
    AsyncStatementCanceller, Error as KernelError, ExecutedAsyncStatement, ExecutedStatementHandle,
    ResultBatch, ResultStream, Statement as KernelStatement,
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

/// An `AsyncResultHandle`'s stream, shared with its parent `AsyncStatement`
/// so the parent's `close()` can release it.
type ResultSlot = Mutex<Option<ResultStream>>;

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
    /// concurrency note. A std mutex (never held across an `.await`) so
    /// `close()` can drop it; `None` once closed.
    canceller: Arc<std::sync::Mutex<Option<AsyncStatementCanceller>>>,
    /// Result slots of the `AsyncResultHandle`s returned by
    /// `awaitResult()`, emptied by `close()`. Weak so a handle JS has
    /// already dropped is not kept alive here.
    results: Arc<std::sync::Mutex<Vec<Weak<ResultSlot>>>>,
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
            canceller: Arc::new(std::sync::Mutex::new(Some(canceller))),
            results: Arc::new(std::sync::Mutex::new(Vec::new())),
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
    /// 'Cancelled' | 'Closed' | 'Unknown'`. (`'Unknown'` is the
    /// `#[non_exhaustive]` forward-compat catch-all that
    /// `StatementStatus::as_str` can return — consumers switching on
    /// the state must handle it.) Returns
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
            // String mapping lives on the kernel `StatementStatus::as_str`
            // so all bindings share one taxonomy (incl. the
            // `#[non_exhaustive]` `"Unknown"` catch-all).
            Ok(status.as_str().to_owned())
        })
        .await
    }

    // ─── SEA extended status accessors (Thrift parity, async path) ─────
    //
    // Mirror the same-named accessors on the sync `Statement`. The
    // underlying values come off the kernel `ExecutedAsyncStatement`'s
    // latest observed `StatementStatus`; for a DML the kernel derives
    // `num_modified_rows` from the result that rides on the terminal
    // GetStatement poll (see `SeaRestTransport::enrich_num_modified_rows`),
    // so a caller gets the count after the statement has reached a terminal
    // state via `status()` / `awaitResult()` — no extra fetch. `null` until
    // then, and on SELECT / warehouses that don't surface the counter.
    // Throws `InvalidStatementHandle` after `close()`, matching `status()`.

    /// Rows modified by the statement (UPDATE / INSERT / DELETE / MERGE).
    #[napi]
    pub async fn num_modified_rows(&self) -> napi::Result<Option<i64>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(|| closed_err("AsyncStatement"))?;
            Ok(inner_state.executed_async.num_modified_rows())
        })
        .await
    }

    /// Server-supplied user-facing message (may contain SQL fragments —
    /// redact before centralised logging).
    #[napi]
    pub async fn display_message(&self) -> napi::Result<Option<String>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(|| closed_err("AsyncStatement"))?;
            Ok(inner_state.executed_async.display_message())
        })
        .await
    }

    /// Server-supplied diagnostic detail.
    #[napi]
    pub async fn diagnostic_info(&self) -> napi::Result<Option<String>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(|| closed_err("AsyncStatement"))?;
            Ok(inner_state.executed_async.diagnostic_info())
        })
        .await
    }

    /// Server-supplied structured error detail (JSON), when enabled.
    #[napi]
    pub async fn error_details_json(&self) -> napi::Result<Option<String>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(|| closed_err("AsyncStatement"))?;
            Ok(inner_state.executed_async.error_details_json())
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
        let results = Arc::clone(&self.results);
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
            let slot = Arc::new(Mutex::new(Some(stream)));
            // Registered while `inner` is still held, so a `close()` queued
            // behind this call always sees the slot.
            results
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(Arc::downgrade(&slot));
            Ok(AsyncResultHandle {
                inner: slot,
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
        let canceller = self
            .canceller
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(canceller) = canceller else {
            return Err(closed_err("AsyncStatement"));
        };
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
        let canceller = Arc::clone(&self.canceller);
        let results = Arc::clone(&self.results);
        guarded(async move {
            let taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            let Some(mut inner_state) = taken else {
                return Ok(());
            };
            // Flip the lock-free flag once we own the close so a racing
            // lock-free `cancel()` short-circuits to InvalidStatementHandle
            // rather than firing a wire cancel on a closing statement.
            closed.store(true, Ordering::Release);
            let outcome = ExecutedAsyncStatement::close(&mut inner_state.executed_async)
                .await
                .map_err(napi_err_from_kernel);
            // Release everything that holds the session, even if the
            // close RPC failed: JS may keep this object (and its result
            // handles) reachable long after close.
            drop(inner_state);
            canceller
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            let slots =
                std::mem::take(&mut *results.lock().unwrap_or_else(PoisonError::into_inner));
            for slot in slots.iter().filter_map(Weak::upgrade) {
                slot.lock().await.take();
            }
            outcome
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
    /// Emptied when the parent `AsyncStatement` closes; fetching after
    /// that returns `InvalidStatementHandle`.
    inner: Arc<ResultSlot>,
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

// The `StatementStatus` → JS-string mapping now lives on the kernel as
// `StatementStatus::as_str` (one taxonomy for all bindings, including
// the `#[non_exhaustive]` `"Unknown"` catch-all). `status()` calls it at
// the call site above.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use databricks_sql_kernel::{AuthConfig, ConnectionConfig, Session, TelemetryConfig};
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    const FIRST_SESSION: &str = "sess-napi-lease-first";
    const SECOND_SESSION: &str = "sess-napi-lease-second";
    const STATEMENT: &str = "stmt-napi-lease";

    async fn lease_server() -> MockServer {
        let server = MockServer::start().await;
        for session_id in [FIRST_SESSION, SECOND_SESSION] {
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/warehouses/test_wh/sessions"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({ "session_id": session_id })),
                )
                .up_to_n_times(1)
                .mount(&server)
                .await;
        }
        Mock::given(method("DELETE"))
            .and(path_regex(r"^/api/2\.0/sql/sessions/.+"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": STATEMENT,
                "status": { "state": "PENDING" },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/2.0/sql/statements/{STATEMENT}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": STATEMENT,
                "status": { "state": "SUCCEEDED" },
                "manifest": {
                    "format": "ARROW_STREAM",
                    "schema": {
                        "column_count": 1,
                        "columns": [{
                            "name": "n", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0,
                        }],
                    },
                    "total_chunk_count": 0,
                    "total_row_count": 0,
                    "truncated": false,
                },
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path_regex(r"^/api/2\.0/sql/statements/.+"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/telemetry-ext"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        server
    }

    async fn telemetry_connection(
        server: &MockServer,
        batch_size: usize,
        flush_interval: Duration,
    ) -> Connection {
        let mut telemetry = TelemetryConfig::default();
        telemetry.set_enabled(true);
        telemetry.batch_size = batch_size;
        telemetry.flush_interval = flush_interval;
        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid HTTP path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .telemetry(telemetry)
            .open()
            .await
            .expect("session opens against mock server");
        let session_id = session.session_id().to_owned();
        Connection {
            inner: Arc::new(Mutex::new(Some(session))),
            session_id,
        }
    }

    async fn exports_for(server: &MockServer, session_id: &str) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|request| {
                request.url.path() == "/telemetry-ext"
                    && String::from_utf8_lossy(&request.body).contains(session_id)
            })
            .count()
    }

    /// The host's telemetry client must be released once its sessions and
    /// their statements are closed, even while JS still references the closed
    /// handles. Otherwise the next session on the host reuses it and ignores
    /// its own telemetry settings.
    async fn next_session_gets_its_own_telemetry_client(fetch_result: bool) {
        let server = lease_server().await;
        // Batch size 1: this client exports every event at once.
        let first = telemetry_connection(&server, 1, Duration::from_millis(100)).await;
        let statement = first
            .submit_statement("SELECT 1 AS n".into(), None)
            .await
            .expect("submit");
        let result = if fetch_result {
            Some(statement.await_result().await.expect("await result"))
        } else {
            None
        };
        statement.close().await.expect("close statement");
        first.close().await.expect("close first session");

        let second = telemetry_connection(&server, 1_000, Duration::from_secs(300)).await;
        // Inheriting the first client would export CREATE_SESSION right away.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            exports_for(&server, SECOND_SESSION).await,
            0,
            "second session reused the first session's telemetry client"
        );
        second.close().await.expect("close second session");
        assert!(
            exports_for(&server, SECOND_SESSION).await > 0,
            "closing the last session must flush its events"
        );
        if let Some(result) = &result {
            let err = result
                .fetch_next_batch()
                .await
                .err()
                .expect("fetching after the parent closed must fail");
            assert!(err.reason.contains("AsyncResultHandle already closed"));
        }
        // Dropped only now, as JS GC would.
        drop((statement, result));
    }

    #[tokio::test]
    async fn close_releases_session_held_by_canceller() {
        next_session_gets_its_own_telemetry_client(false).await;
    }

    #[tokio::test]
    async fn close_releases_session_held_by_result_handles() {
        next_session_gets_its_own_telemetry_client(true).await;
    }
}
