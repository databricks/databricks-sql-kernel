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

//! Opaque `Statement` wrapper around the kernel's `ExecutedStatement`.
//!
//! - `Statement.fetchNextBatch() -> Option<ArrowBatch>` — drives
//!   `ResultStream::next_batch().await`, serialises the borrowed
//!   `RecordBatch` to Arrow IPC bytes, returns them to JS.
//! - `Statement.schema() -> ArrowSchema` — returns the schema cached
//!   at construction time, serialised as a schema-only IPC payload.
//!   Available even after `close()` so JS callers can build error
//!   reports against a disposed statement.
//! - `Statement.cancel()` / `Statement.close()` — forwards to
//!   `ExecutedStatement::cancel/close` via the
//!   `ExecutedStatementHandle` trait. Drop fires-and-forgets close
//!   if not already explicitly closed.
//! - `Statement.statementId` — server-issued statement id, cached at
//!   construction and exposed for log correlation against kernel /
//!   server logs which key on the same id.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use arrow_schema::SchemaRef;
use databricks_sql_kernel::{
    Error as KernelError, ExecutedStatement, ExecutedStatementHandle, ResultBatch, ResultStream,
    Statement as KernelStatement,
};

use crate::error::napi_err_from_kernel;
use crate::result::{ArrowBatch, ArrowSchema};
use crate::runtime;
use crate::util::guarded;

/// Inner state for the JS-visible `Statement` wrapper.
///
/// **Why we also hold the parent kernel `Statement`:** the kernel's
/// `Statement::Drop` (`src/statement/mutable.rs:341-361`) invalidates
/// its produced `ExecutedStatement` via the shared `ValidityFlag`. If
/// the binding wrapper held only the `ExecutedStatement`, the parent
/// kernel `Statement` constructed in `Connection::execute_statement`
/// would drop at the end of that function, flipping the validity flag
/// and rendering every subsequent `cancel()` / `close()` call against
/// the executed handle a no-op that throws
/// `InvalidStatementHandle("executed-statement handle has been
/// invalidated by a re-execute on its parent Statement")`.
///
/// Keeping the kernel `Statement` alive in the same `Arc<Mutex<…>>`
/// as the `ExecutedStatement` ties their lifetimes together — the
/// validity flag stays set for as long as JS holds the wrapper.
struct StatementInner {
    /// The kernel's `Statement` (mutable). Kept alive solely to
    /// prevent its `Drop` from invalidating `executed`.
    _parent: KernelStatement,
    /// The executed handle — drives `fetchNextBatch` / `cancel` /
    /// `close`.
    executed: ExecutedStatement,
}

/// Discriminated inner state for `Statement`.
enum AnyInner {
    /// Produced by `Connection::execute_statement`. Boxed to keep the
    /// enum size close to the `Metadata` variant (StatementInner is
    /// large; ResultStream is much smaller).
    Executed(Box<StatementInner>),
    /// Produced by metadata methods — wraps a bare `ResultStream`.
    Metadata(ResultStream),
}

impl AnyInner {
    /// Pull the next batch, variant-aware so the executed path gets the
    /// kernel's **auto-close-on-drain** (fire-and-forget CloseStatement
    /// once the stream is exhausted, after all lazy CloudFetch link
    /// fetches). The metadata variant uses the bare stream's
    /// `next_batch` — it carries its own close-on-drop guard
    /// (`ResultStream::from_reader_with_close`), so it must NOT route
    /// through the executed auto-close.
    async fn next_batch(&mut self) -> databricks_sql_kernel::Result<Option<&ResultBatch>> {
        match self {
            AnyInner::Executed(s) => s.executed.next_batch().await,
            AnyInner::Metadata(rs) => rs.next_batch().await,
        }
    }
}

/// Opaque executed-statement handle.
///
/// **Current concurrency shape** — every method takes `inner.lock()`
/// and holds the guard across the kernel `.await`. tokio `Mutex` is
/// FIFO, so cancel/close queue behind any in-flight `fetchNextBatch`
/// until it returns naturally. This is a known limitation that exists
/// because the napi shape has not yet been split into an
/// `Arc<dyn ExecutedStatementHandle>` (for cancel/close, which the
/// kernel exposes as `&self`-callable) plus a `Mutex<Option<…>>` only
/// for the borrowed-mut fetch path. The lock-shape refactor needs a
/// small kernel-side accessor and lands in a follow-up PR — see
/// `sea-workflow/jira-candidates/2026-05-24-napi-cancel-during-fetch.md`.
///
/// `schema` and `statement_id` are cached at construction so they
/// survive `close()` — JS callers building error reports against a
/// disposed statement can still read them.
#[napi]
pub struct Statement {
    /// Discriminated inner state: the `Executed` variant wraps the
    /// kernel `Statement` + `ExecutedStatement` pair (the parent kept
    /// alive to prevent its `Drop` from invalidating the executed
    /// handle via the shared `ValidityFlag`); the `Metadata` variant
    /// wraps a bare `ResultStream` produced by `Metadata::list_*` /
    /// `primary_keys` / `foreign_keys` / `type_info` / `table_types`.
    inner: Arc<Mutex<Option<AnyInner>>>,
    /// Schema captured at construction. Available even after `close()`.
    /// For the `Executed` variant this is the executed handle's
    /// schema; for the `Metadata` variant it's the result-stream's
    /// schema. Either way, cached so JS callers can build error
    /// reports against a disposed statement.
    schema: SchemaRef,
    /// Server-issued statement id. Populated for the `Executed`
    /// variant; empty string for the `Metadata` variant (the kernel
    /// does not emit a statement id for SHOW/information_schema
    /// queries driven through `Metadata`).
    statement_id: String,
    /// Set to `true` when `fetchNextBatch` observes the natural end of
    /// the stream (`None`). `cancel()` short-circuits to `Ok(())` when
    /// this is set, matching the JDBC `Statement.cancel()` no-op-after-
    /// completion contract — without it, a defensive cancel after the
    /// stream is drained surfaces an opaque server-side error.
    /// (Irrelevant for the `Metadata` variant where cancel is already
    /// a no-op kernel-side; harmless to carry the field regardless.)
    finished: Arc<AtomicBool>,
}

impl Statement {
    /// Crate-internal constructor — called from
    /// `Connection::execute_statement` once the kernel hands back the
    /// `ExecutedStatement`. The parent kernel `Statement` is passed
    /// in alongside so its `Drop` doesn't fire while the JS side
    /// still holds the executed handle (see `StatementInner` docs
    /// above). `schema` and `statement_id` are read off the executed
    /// handle here and cached on the wrapper so they remain readable
    /// after `close()` for log correlation / error reporting.
    pub(crate) fn from_executed(parent: KernelStatement, executed: ExecutedStatement) -> Self {
        let schema = executed.schema();
        let statement_id = executed.statement_id().to_owned();
        Self {
            inner: Arc::new(Mutex::new(Some(AnyInner::Executed(Box::new(
                StatementInner {
                    _parent: parent,
                    executed,
                },
            ))))),
            schema,
            statement_id,
            finished: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Crate-internal constructor for metadata calls. Wraps a bare
    /// `ResultStream` returned by `Metadata::list_*` / `primary_keys` /
    /// `foreign_keys` / `type_info` / `table_types`. Cancel is a no-op
    /// for metadata streams (the kernel has no in-flight cancellation
    /// surface for them); close just drops the stream.
    ///
    /// `schema` is captured from the stream so the `schema()` getter
    /// works after close. `statement_id` is empty (kernel does not
    /// emit one for metadata queries). `finished` defaults false and
    /// is unread on this path (cancel is already a no-op).
    pub(crate) fn from_result_stream(stream: ResultStream) -> Self {
        let schema = stream.schema();
        Self {
            inner: Arc::new(Mutex::new(Some(AnyInner::Metadata(stream)))),
            schema,
            statement_id: String::new(),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[napi]
impl Statement {
    /// Server-issued statement id. Cached at construction; readable
    /// even after `close()` so JS-side log lines can correlate against
    /// kernel / server logs which key on the same id.
    #[napi(getter)]
    pub fn statement_id(&self) -> &str {
        &self.statement_id
    }

    // ─── SEA extended status accessors (Thrift parity) ─────────────────
    //
    // Mirror the same-named optional fields on the NodeJS Thrift
    // driver's `TGetOperationStatusResp`. Populated whenever the SEA
    // server sets them on the terminal `StatementStatus` envelope; the
    // kernel stores the status struct once at construction so the
    // accessors are read-only and lock-free at the kernel layer.
    //
    // These methods are `async` because the napi binding's inner
    // state sits inside `Arc<Mutex<Option<StatementInner>>>` and the
    // lock shape mirrors `fetchNextBatch` / `cancel` — a brief tokio
    // mutex held across the kernel accessor call. After `close()` the
    // inner state has been taken; the accessors throw an
    // `InvalidStatementHandle` error in that case, matching the
    // existing `cancel()` semantics.

    /// Number of rows modified by the statement (UPDATE / INSERT /
    /// DELETE / MERGE). `null` for SELECT and on warehouses that don't
    /// surface the counter. Mirrors Thrift's
    /// `TGetOperationStatusResp.numModifiedRows`.
    #[napi]
    pub async fn num_modified_rows(&self) -> napi::Result<Option<i64>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(statement_closed_err)?;
            match inner_state {
                AnyInner::Executed(s) => Ok(s.executed.num_modified_rows()),
                AnyInner::Metadata(_) => Ok(None),
            }
        })
        .await
    }

    // TODO(pr23-followup): mirror a `progress()` accessor here once
    // the kernel exposes one. Empirically, the SEA server does not
    // populate progress today (verified 2026-05-27 against pecotesting:
    // a 37s heavy TPC-H query polled 7 times with
    // `getProgressUpdate=true` on the Thrift NodeJS path returned
    // `progressUpdateResponse = null` on every poll). When the server
    // grows progress support, expose it here under the same name JS
    // callers expect from Thrift's `TGetOperationStatusResp.progressUpdateResponse`
    // — likely as a struct mirroring `TProgressUpdateResp`, not a bare
    // float.

    /// Server-supplied user-facing message. Mirrors Thrift's
    /// `TGetOperationStatusResp.displayMessage`. **PII / sensitive-
    /// data note:** may contain SQL fragments or parameter values —
    /// redact before centralised logging.
    ///
    /// Populated on `Succeeded` / `Closed-with-inline-data` paths.
    /// On terminal-error states (`Failed` / `Cancelled` /
    /// `Closed-no-data`) the kernel returns an Error instead of a
    /// `Statement`, and the same field rides on the JS Error envelope
    /// under the same `displayMessage` key.
    #[napi]
    pub async fn display_message(&self) -> napi::Result<Option<String>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(statement_closed_err)?;
            match inner_state {
                AnyInner::Executed(s) => Ok(s.executed.display_message().map(str::to_owned)),
                AnyInner::Metadata(_) => Ok(None),
            }
        })
        .await
    }

    /// Server-supplied diagnostic detail — multi-line operator /
    /// stack context. Mirrors Thrift's
    /// `TGetOperationStatusResp.diagnosticInfo`. For support surfaces,
    /// not user-facing. Same reachability + PII caveats as
    /// `displayMessage`.
    #[napi]
    pub async fn diagnostic_info(&self) -> napi::Result<Option<String>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(statement_closed_err)?;
            match inner_state {
                AnyInner::Executed(s) => Ok(s.executed.diagnostic_info().map(str::to_owned)),
                AnyInner::Metadata(_) => Ok(None),
            }
        })
        .await
    }

    /// Server-supplied JSON blob with extended error details. Mirrors
    /// Thrift's `TGetOperationStatusResp.errorDetailsJson`.
    /// Pass-through string — JS callers parse with `JSON.parse` if
    /// they need structured access.
    ///
    /// **Server-side gating:** populated only when the workspace has
    /// `spark.databricks.sql.errorDetailsJson.enabled = true` on the
    /// underlying SQL cluster. The flag is internal-only / default-
    /// false in the Databricks runtime, so for most JS callers this
    /// will return `null`. Admin-enabled workspaces return content
    /// shaped like `{"errorClass": "...", "messageTemplate": "..."}`.
    ///
    /// **Unbounded:** when populated, server can return a multi-MB
    /// blob; size before logging.
    #[napi]
    pub async fn error_details_json(&self) -> napi::Result<Option<String>> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let inner_state = guard.as_ref().ok_or_else(statement_closed_err)?;
            match inner_state {
                AnyInner::Executed(s) => Ok(s.executed.error_details_json().map(str::to_owned)),
                AnyInner::Metadata(_) => Ok(None),
            }
        })
        .await
    }

    /// Pull the next batch of results. Returns `null` when the stream
    /// is exhausted. The returned `ArrowBatch.ipcBytes` is a complete
    /// Arrow IPC stream (schema header + 1 record-batch message)
    /// suitable for handing to `apache-arrow`'s `RecordBatchReader`.
    ///
    /// On `Err`, the stream is in an unspecified state — call
    /// `close()` and discard the `Statement`. Subsequent
    /// `fetchNextBatch()` calls after an error are not guaranteed to
    /// succeed or fail consistently.
    #[napi]
    pub async fn fetch_next_batch(&self) -> napi::Result<Option<ArrowBatch>> {
        let inner = Arc::clone(&self.inner);
        let schema = self.schema.clone();
        let finished = Arc::clone(&self.finished);
        guarded(async move {
            let mut guard = inner.lock().await;
            let inner_state = guard.as_mut().ok_or_else(statement_closed_err)?;
            // The IPC schema header serialised into every batch payload
            // comes from the cached `self.schema` captured outside the
            // lock (see method start), so no need to re-fetch from the
            // stream.
            // Variant-aware: the executed path auto-closes the server
            // statement on drain; the metadata path uses its own
            // close-on-drop guard. (See `AnyInner::next_batch`.)
            let maybe_batch = inner_state
                .next_batch()
                .await
                .map_err(napi_err_from_kernel)?;
            let Some(batch) = maybe_batch else {
                // End of stream — record so a defensive cancel after
                // natural completion can short-circuit to Ok(()).
                finished.store(true, Ordering::Release);
                return Ok(None);
            };
            // `ResultBatch` is `#[non_exhaustive]`; v0 only ever
            // yields `Arrow`. The error arm exists for forward
            // compat — v1+ may add ColumnarThrift / JsonRows / etc.,
            // and we want the binding to surface that as a typed
            // error rather than silently misbehaving.
            let record_batch = match batch {
                ResultBatch::Arrow(rb) => rb,
                _ => {
                    // `InvalidArgument`, not `Internal`: this isn't a
                    // kernel bug (which would page oncall via the
                    // Internal-category dashboards) — it's "binding
                    // too old for the kernel it's linked against."
                    // The kernel has rolled a new ResultBatch variant
                    // that this binding's decode path doesn't know
                    // how to handle, and the JS-side adapter should
                    // surface this as a version-skew error to the
                    // user. Matches the sibling-binding convention.
                    return Err(napi_err_from_kernel(
                        KernelError::invalid_argument().message(format!(
                            "non-Arrow ResultBatch variant from kernel — \
                             databricks-sql-kernel-napi v{} cannot decode this batch \
                             shape, upgrade the binding",
                            env!("CARGO_PKG_VERSION"),
                        )),
                    ));
                }
            };
            let bytes = crate::result::encode_ipc_stream(&schema, Some(record_batch))?;
            Ok(Some(ArrowBatch {
                ipc_bytes: bytes.into(),
            }))
        })
        .await
    }

    /// Result schema as an Arrow IPC payload (schema header only, no
    /// record-batch message). Available before any batches have been
    /// fetched, and remains available after `close()` — the kernel
    /// materialises the schema eagerly so JS callers can build error
    /// reports against a disposed statement.
    ///
    /// Sync because the body has no `.await` — `encode_ipc_stream` is
    /// pure CPU work over an `Arc<Schema>` already cached on the
    /// wrapper. Mirrors `pyo3/src/statement.rs::arrow_schema` (sync).
    /// napi-rs converts a panic in a sync `#[napi]` entry point into a
    /// thrown JS error via its own macro-expanded boundary, so the
    /// `util::guarded` `catch_unwind` wrapper that the `async fn`
    /// entry points use is not required for this method.
    #[napi]
    pub fn schema(&self) -> napi::Result<ArrowSchema> {
        let bytes = crate::result::encode_ipc_stream(&self.schema, None)?;
        Ok(ArrowSchema {
            ipc_bytes: bytes.into(),
        })
    }

    /// Server-side cancel.
    ///
    /// For executed statements: short-circuits to `Ok(())` if
    /// `fetchNextBatch` has already returned `null` (stream
    /// naturally exhausted) — matches the JDBC `Statement.cancel()`
    /// no-op-after-completion contract, so JS callers can fire cancel
    /// defensively without distinguishing "real cancel" from "raced
    /// with natural completion."
    ///
    /// For metadata streams: no-op (the kernel has no in-flight
    /// cancellation surface for metadata calls today).
    ///
    /// Returns `KernelError(InvalidStatementHandle)` if the statement
    /// has been explicitly `close()`d.
    #[napi]
    pub async fn cancel(&self) -> napi::Result<()> {
        // Idempotent against natural completion.
        if self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        let inner = Arc::clone(&self.inner);
        let finished = Arc::clone(&self.finished);
        guarded(async move {
            let guard = inner.lock().await;
            // Re-check `finished` after acquiring the lock to close the
            // fetch-vs-cancel TOCTOU: a concurrent `fetch_next_batch` may have
            // drained to completion and set `finished` while this cancel was
            // queued on the mutex. Without the re-check we'd dispatch a
            // `CancelStatement` against an already-terminal statement (a
            // redundant, wire-benign-but-wasteful RPC).
            if finished.load(Ordering::Acquire) {
                return Ok(());
            }
            let inner_state = guard.as_ref().ok_or_else(statement_closed_err)?;
            match inner_state {
                AnyInner::Executed(s) => s.executed.cancel().await.map_err(napi_err_from_kernel),
                // Metadata streams have no server-side cancel handle, so the
                // query keeps running server-side. Emit a breadcrumb so a
                // "cancel() didn't cancel" report is diagnosable from logs.
                AnyInner::Metadata(_) => {
                    tracing::debug!(
                        "cancel() on a metadata statement is a no-op (no server-side cancel handle); the query continues server-side"
                    );
                    Ok(())
                }
            }
        })
        .await
    }

    /// Explicit close.
    ///
    /// For executed statements: awaits the server-side `CloseStatement`
    /// so the JS caller can observe failures (auth revoked mid-session,
    /// network error, server-side error). Idempotent — a second call
    /// on an already-closed statement returns `Ok`.
    ///
    /// **Errors are terminal from the JS side.** The kernel executed
    /// handle is taken out of `inner` BEFORE the wire `CloseStatement`
    /// runs (so `Drop` knows there's nothing left to clean up). On
    /// `Err`, the napi `inner` is already `None`, so a JS-side retry
    /// sees a closed statement and returns `Ok(())` without re-
    /// attempting the wire call. The kernel-level `ExecutedStatement`
    /// has been consumed at that point and the value is dropped on
    /// the way out of the closure — the kernel's `ExecutedStatement::
    /// Drop` then fires-and-forgets a single retry on the captured
    /// runtime. The JS caller can log the error but cannot drive a
    /// further retry. If you need retry-on-failure semantics for
    /// `CloseStatement`, layer them above this method.
    ///
    /// For metadata streams: drops the stream (no server round-trip
    /// needed — metadata results have no in-flight server-side
    /// resource to release).
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            // Take the inner state out so `Drop` knows there's nothing
            // left to clean up.
            let inner_state = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            match inner_state {
                Some(AnyInner::Executed(s)) => {
                    s.executed.close().await.map_err(napi_err_from_kernel)?;
                    // _parent drops here, after close has resolved.
                }
                // Metadata stream: just drop it.
                Some(AnyInner::Metadata(_)) | None => {}
            }
            Ok(())
        })
        .await
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        // Best-effort cleanup: schedule a drop of the kernel handle on
        // the captured tokio runtime. JS callers who need a
        // guaranteed `close_statement` round-trip must call `close()`
        // explicitly — `Drop` cannot `.await` and cannot surface
        // failures, and the spawned task is subject to the runtime's
        // lifetime. If process exit tears down the runtime before the
        // task runs, the handle is dropped without a wire close; the
        // server reclaims the statement when the session expires.
        let Some(handle) = runtime::try_get_handle() else {
            return;
        };
        let inner = Arc::clone(&self.inner);
        handle.spawn(async move {
            // Drop the inner state on the runtime. The kernel's
            // `ExecutedStatement::Drop` already spawns a fire-and-forget
            // `close_statement` against its own captured handle, so we
            // just need to ensure the value is dropped inside a tokio
            // context (the kernel's Drop reads `runtime_handle.clone()`
            // and spawns; that handle is the same one we captured
            // here). The kernel `Statement` we kept alive in
            // `_parent` drops at the same time, but it only owns
            // local state (its Drop is non-async — see
            // `src/statement/mutable.rs:341-361`).
            let _taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
        });
    }
}

/// Closed-state error builder. Routes through the kernel error
/// envelope so the TS-side reviver mints a typed
/// `KernelError(code='InvalidStatementHandle')` rather than a raw
/// `Status::InvalidArg` string the adapter can only match by
/// substring. Single source of truth for all four call sites
/// (fetch_next_batch / cancel and their Connection-side cousin) —
/// keeps the wire-message shape stable for the TS reviver's contract
/// test.
pub(crate) fn statement_closed_err() -> napi::Error {
    napi_err_from_kernel(
        KernelError::invalid_statement_handle().message("statement already closed"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    // Arrow IPC encoding is exercised by `encode_ipc_stream`'s own
    // round-trip tests in `result.rs` (its new home — see the
    // encoder-lift refactor). `schema()` / `fetchNextBatch()` just
    // delegate to it, so the load-bearing assertions live there.

    // ─── Closed-state behaviour ─────────────────────────────────────
    //
    // These tests pin the contract that the closed-state error
    // shape is the kernel envelope (sentinel + `code` field), not a
    // raw `Status::InvalidArg` string the TS reviver can only match
    // by substring. We call the `statement_closed_err` helper
    // directly rather than awaiting `#[napi]` methods: the napi-rs
    // attribute macro injects code paths that link against the
    // native napi runtime (`napi_get_reference_value` etc.) which
    // aren't available in `cargo test`. Testing the helper is the
    // load-bearing assertion — the methods all funnel through it.

    fn assert_invalid_statement_handle_envelope(err: &napi::Error) {
        let msg = err.reason.as_str();
        assert!(
            msg.starts_with(crate::error::ERROR_SENTINEL),
            "expected envelope-sentinel-prefixed error, got: {msg}",
        );
        assert!(
            msg.contains(r#""code":"InvalidStatementHandle""#),
            "envelope payload missing InvalidStatementHandle code: {msg}",
        );
    }

    #[test]
    fn statement_closed_err_carries_invalid_statement_handle_envelope() {
        // Single source of truth for all four call sites that surface
        // closed-state errors. Pinning the wire shape here means a
        // future maintainer who changes the error builder will break
        // this test instead of silently breaking the TS adapter's
        // reviver.
        let err = statement_closed_err();
        assert_invalid_statement_handle_envelope(&err);
        // The human-readable message survives — TS-side error
        // .stack frames stay informative.
        assert!(
            err.reason.contains("statement already closed"),
            "closed-state error must carry the human message: {}",
            err.reason,
        );
    }

    #[test]
    fn statement_closed_err_status_collapses_to_invalid_arg() {
        // The napi `Status` field collapses to `InvalidArg` because
        // it's an enum with no `InvalidStatementHandle` variant. The
        // TS adapter discriminates on the envelope `code` field, not
        // on this status. Pin it so a future kernel-side ErrorCode
        // -> Status mapping change is loud.
        let err = statement_closed_err();
        assert_eq!(err.status, napi::Status::InvalidArg);
    }

    #[test]
    fn statement_id_is_cached_and_survives_close() {
        // `statement_id` is stored on the wrapper, not delegated to
        // the kernel handle. Once cached, `close()` (which sets
        // `inner = None`) cannot null it out.
        let stmt = Statement {
            inner: Arc::new(Mutex::new(None)), // "already closed"
            schema: make_schema(),
            statement_id: "stmt-survives-close".to_owned(),
            finished: Arc::new(AtomicBool::new(false)),
        };
        assert_eq!(stmt.statement_id(), "stmt-survives-close");
    }

    #[test]
    fn schema_field_is_cached_arc() {
        // Schema is held as a cheap Arc, not a fresh clone — so the
        // getter is O(1) even on a closed statement. Verify by
        // confirming Arc::strong_count rises when we clone.
        let schema = make_schema();
        let stmt = Statement {
            inner: Arc::new(Mutex::new(None)),
            schema: schema.clone(),
            statement_id: "x".to_owned(),
            finished: Arc::new(AtomicBool::new(false)),
        };
        // Two refs alive: the local + the stmt field.
        assert!(Arc::strong_count(&stmt.schema) >= 2);
        drop(stmt);
        // After dropping the stmt only the local remains.
        assert_eq!(Arc::strong_count(&schema), 1);
    }

    #[test]
    fn finished_flag_starts_false_and_can_be_toggled() {
        // Pin the cancel-after-completion contract at field level:
        // `cancel()` reads `finished` and short-circuits to Ok(())
        // when set, matching the JDBC no-op-after-completion
        // semantics. fetch_next_batch sets it when it returns None.
        let stmt = Statement {
            inner: Arc::new(Mutex::new(None)),
            schema: make_schema(),
            statement_id: "x".to_owned(),
            finished: Arc::new(AtomicBool::new(false)),
        };
        assert!(!stmt.finished.load(Ordering::Acquire));
        stmt.finished.store(true, Ordering::Release);
        assert!(stmt.finished.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn cancel_short_circuits_when_finished_even_on_closed_inner() {
        // `finished = true` must short-circuit cancel to Ok(()) BEFORE the
        // closed-state check — so a cancel racing a just-completed fetch is a
        // no-op, not a CancelStatement RPC (and not a closed-handle error).
        let stmt = Statement {
            inner: Arc::new(Mutex::new(None)), // closed
            schema: make_schema(),
            statement_id: "x".to_owned(),
            finished: Arc::new(AtomicBool::new(true)),
        };
        assert!(stmt.cancel().await.is_ok());
    }
}
