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

//! `ExecutedStatement` — sync-consumption executed handle.
//!
//! Returned by `Statement::execute()`. Per `docs/spec-v0.md` addendum
//! A1, the kernel has already polled the server to completion when
//! this handle is constructed; results are ready to consume via
//! [`ExecutedStatement::result_stream_mut`].
//!
//! `'static` for the same reason as every other v0 handle — see
//! `docs/spec-v0.md` addendum A1, "Why no Rust lifetimes on the
//! handles". Validity-flag plumbing is documented in A1, "Validity
//! flag for zombie handles", and lives in `crate::statement::validity`.
//!
//! Result-stream access is non-consuming for JDBC/ODBC parity: the
//! caller finishes iterating rows and then closes the statement
//! separately.

use crate::kernel_error::Result;
use crate::klog::warn;
use crate::result_stream::{ResultBatch, ResultStream, SchemaRef};
use crate::session::SessionInner;
use crate::statement::handle::ExecutedStatementHandle;
use crate::statement::validity::ValidityFlag;
use crate::types::sea::StatementStatus as SeaStatementStatus;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::Instrument;

/// Sync-consumption executed handle.
pub struct ExecutedStatement {
    session: Arc<SessionInner>,
    statement_id: String,
    /// Shared with the parent `Statement`; flipped on re-execute or
    /// when the parent `Statement` is dropped. Reads short-circuit
    /// stale calls locally with no network round-trip.
    valid: ValidityFlag,
    result_stream: ResultStream,
    /// Terminal SEA `StatementStatus` captured at construction.
    /// Kept as the wire-shape struct so the accessor methods can
    /// hand back individual fields without re-querying the server.
    /// Mutated only at construction; no interior mutability.
    status: SeaStatementStatus,
    /// Flips to `true` after the first successful (or the first
    /// duplicate-call short-circuited) `close()`. Tells `Drop` to
    /// skip its fire-and-forget cleanup so an explicit close
    /// doesn't trigger a redundant background spawn.
    closed: Arc<AtomicBool>,
}

impl ExecutedStatement {
    /// Crate-internal constructor. Built by [`Statement::execute`].
    ///
    /// `closed` is a shared flag — the parent [`Statement`] owns the
    /// same `Arc` so that flipping `closed` here also short-circuits
    /// the lifecycle-owner's `invalidate_prior` cleanup path. The
    /// parent pre-sets the flag to `true` when the server returned
    /// `state=CLOSED` in its execute response, so neither side fires
    /// the redundant `CloseStatement` round-trip.
    pub(crate) fn new(
        session: Arc<SessionInner>,
        statement_id: String,
        valid: ValidityFlag,
        result_stream: ResultStream,
        closed: Arc<AtomicBool>,
        status: SeaStatementStatus,
    ) -> Self {
        Self {
            session,
            statement_id,
            valid,
            result_stream,
            closed,
            status,
        }
    }

    /// Result schema. Available without iterating any batches; mirrors
    /// the underlying `ResultStream::schema()`.
    pub fn schema(&self) -> SchemaRef {
        self.result_stream.schema()
    }

    /// Borrow the result stream immutably. JDBC/ODBC parity — the
    /// caller separates "done iterating" from "done with statement".
    pub fn result_stream(&self) -> &ResultStream {
        &self.result_stream
    }

    /// Mutable borrow of the result stream for batch / row consumption.
    ///
    /// **Borrow note:** the returned reference borrows `&mut self`. Drop
    /// it (close the scope, or rebind to a shorter-lived let) before
    /// calling [`ExecutedStatementHandle::cancel`] or
    /// [`ExecutedStatementHandle::close`] on this `ExecutedStatement`,
    /// otherwise the borrow checker rejects the call.
    pub fn result_stream_mut(&mut self) -> &mut ResultStream {
        &mut self.result_stream
    }

    /// Pull the next result batch, **auto-closing the server statement
    /// when the stream is exhausted**.
    ///
    /// Identical to `result_stream_mut().next_batch()` except that the
    /// first time it observes end-of-stream (`Ok(None)`), it spawns a
    /// fire-and-forget `CloseStatement` (guarded by the shared `closed`
    /// flag, so `Drop` / explicit `close()` never double-fire).
    ///
    /// Why this matters: for a large CloudFetch result whose chunk
    /// *links* are paginated (`all_fetched=false`), the reader fetches
    /// later links lazily via `get_result_chunks` against the live
    /// server statement *during consumption*. Closing the statement
    /// before the stream is drained (as a premature connector
    /// `Statement::close()` did) breaks those lazy fetches. Closing
    /// *here*, only once `next_batch` has returned `None`, guarantees
    /// every lazy link fetch has already completed — so the close is
    /// always safe, and the connector no longer needs to close the
    /// statement on the sync path at all.
    ///
    /// Bindings should call this instead of
    /// `result_stream_mut().next_batch()` so the auto-close fires.
    pub async fn next_batch(&mut self) -> Result<Option<&ResultBatch>> {
        // NLL: when `next_batch` yields `Some`, the returned reference
        // keeps `self.result_stream` borrowed, so we return it directly.
        // When it yields `None`, that borrow has ended — so we can read
        // the other `self` fields to fire the auto-close. We branch on a
        // cheap `is_some()` check (not the reference itself) to let the
        // borrow end before the `None` arm touches `self`.
        if self.result_stream.next_batch().await?.is_some() {
            // Re-borrow and return the batch the stream just buffered.
            // `current()` returns the same already-fetched batch without
            // advancing the stream.
            return Ok(self.result_stream.current());
        }
        // End-of-stream: every lazy link fetch has completed, so the
        // server statement is safe to close.
        self.spawn_close_if_open();
        Ok(None)
    }

    /// Spawn a fire-and-forget `CloseStatement` unless the statement is
    /// already closed (`closed` flag). Shared by [`Self::next_batch`]'s
    /// auto-close-on-drain and `Drop`. The `swap` makes whichever fires
    /// first win; the other observes `closed=true` and skips.
    fn spawn_close_if_open(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let transport = Arc::clone(self.session.transport());
        let runtime = self.session.runtime_handle().clone();
        let stmt_id = self.statement_id.clone();
        runtime.spawn(async move {
            if let Err(e) = transport.close_statement(&stmt_id).await {
                warn!(
                    statement_id = %stmt_id,
                    error = %e,
                    "fire-and-forget close_statement failed; \
                     server-side resource reclaimed when the session closes"
                );
            }
        });
    }

    /// Rows modified by the statement (UPDATE / INSERT / DELETE /
    /// MERGE). Absent for SELECT and on warehouses that don't surface
    /// the counter.
    ///
    /// Reachable on the `Succeeded` / `Closed` paths — the kernel returns
    /// `Err` (no `ExecutedStatement`) on `Failed` / `Cancelled`. (A `Closed`
    /// response with no result data is a valid empty result, not an error.)
    pub fn num_modified_rows(&self) -> Option<i64> {
        self.status.num_modified_rows
    }

    /// Whether this executed handle is still valid, i.e. the parent
    /// `Statement` has not been re-executed (which flips the shared
    /// `ValidityFlag` and closes this statement server-side). The C ABI
    /// checks this so a stale handle — or a result stream borrowed from
    /// it — returns `InvalidStatementHandle` rather than driving a
    /// superseded server statement. Crate-internal; Rust-crate consumers
    /// hold the handle by value and can't observe a re-execute.
    pub(crate) fn is_valid(&self) -> bool {
        self.valid.check().is_ok()
    }

    // TODO(pr23-followup): add `progress()` once the SEA server emits
    // a progress field. Verified empirically on 2026-05-27 that a 37s
    // TPC-H query against a SQL warehouse returns no
    // `progressUpdateResponse` even on the Thrift path that explicitly
    // requests it (`getProgressUpdate=true`). The SDK Java
    // `StatementStatus` model also has no progress field. Adding an
    // accessor that always returns None would mislead callers; revisit
    // when the SEA server grows progress support, ideally surfacing
    // the same struct shape Thrift uses
    // (`TProgressUpdateResp { headerNames, rows, footerSummary,
    // progressedPercentage, status, startTime }`) rather than a bare
    // `Option<f64>`.

    /// Server-supplied user-facing message. Distinct from the
    /// `error.message` that the kernel folds into `Error::sql_error()`
    /// on the `Failed` path.
    ///
    /// **Reachability:** populated on `Succeeded` / `Closed` (incl. an empty
    /// `Closed`). Failed / Cancelled return `Err` before the `ExecutedStatement`
    /// is constructed, so cancellation-reason messages from those paths are NOT
    /// reachable here — they flow through the `Error` instead.
    ///
    /// **PII / sensitive-data note:** server-supplied free-form text
    /// may include SQL fragments or parameter values. Redact before
    /// centralised logging.
    pub fn display_message(&self) -> Option<&str> {
        self.status.display_message.as_deref()
    }

    /// Server-supplied diagnostic detail — multi-line text with stack
    /// frames or operator context. Intended for log / support
    /// surfaces; not user-facing.
    ///
    /// **Reachability:** populated on `Succeeded` / `Closed`
    /// only — same caveat as [`Self::display_message`].
    ///
    /// **PII / sensitive-data note:** like `display_message`, this is
    /// server-supplied free-form text and may include SQL fragments
    /// or values. Redact before centralised logging.
    pub fn diagnostic_info(&self) -> Option<&str> {
        self.status.diagnostic_info.as_deref()
    }

    /// Server-supplied JSON blob with extended error details.
    /// Pass-through string — wrappers parse if they need structured
    /// access.
    ///
    /// **Reachability:** populated on `Succeeded` / `Closed`
    /// only. The corresponding details for Failed statements flow
    /// through the returned `Error`, not this accessor.
    ///
    /// **Server-side gating:** populated only when the workspace has
    /// `spark.databricks.sql.errorDetailsJson.enabled = true` set on
    /// the underlying SQL cluster. The flag is `internal()` and
    /// false-by-default in `DatabricksSQLConf.scala`, so it is NOT
    /// settable from a client session-configuration request. Most
    /// workspaces will return `None` here; admin-enabled workspaces
    /// return content shaped like
    /// `{"errorClass": "...", "messageTemplate": "..."}`.
    ///
    /// **Unbounded:** when populated, the server can return a
    /// multi-MB blob. Wrappers surfacing this should bound size
    /// before logging.
    pub fn error_details_json(&self) -> Option<&str> {
        self.status.error_details_json.as_deref()
    }
}

#[async_trait]
impl ExecutedStatementHandle for ExecutedStatement {
    fn statement_id(&self) -> &str {
        &self.statement_id
    }

    async fn cancel(&self) -> Result<()> {
        // Local validity short-circuits a wire call against an
        // already-closed/superseded handle. `instrument` (rather
        // than `.entered()`) so the future stays `Send` per the
        // `ExecutedStatementHandle` trait bound.
        async {
            self.valid.check()?;
            self.session
                .transport()
                .cancel_statement(&self.statement_id)
                .await
        }
        .instrument(self.session.log_span().clone())
        .await
    }

    async fn close(&self) -> Result<()> {
        // Idempotent on success: a prior successful close sets
        // `closed=true` and subsequent calls short-circuit.
        //
        // On wire failure we deliberately keep `closed=false` so that
        // `Drop`'s fire-and-forget retry path runs as a fallback —
        // otherwise a transient close failure would silently leak the
        // server-side statement until session expiry. Mirrors
        // `Session::close`.
        //
        // The `&self` signature allows two callers to race and both
        // hit the wire. The server-side close is idempotent (it
        // returns the same Ok for duplicate close_statement calls),
        // so the only cost of the race is one redundant RPC — far
        // better than dropping the retry contract.
        async {
            if self.closed.load(Ordering::Acquire) {
                return Ok(());
            }
            self.session
                .transport()
                .close_statement(&self.statement_id)
                .await?;
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
        .instrument(self.session.log_span().clone())
        .await
    }
}

impl Drop for ExecutedStatement {
    fn drop(&mut self) {
        // Backstop for the not-fully-drained case (partial fetch /
        // abandoned result / early error): if the stream was drained,
        // `next_batch`'s auto-close already fired and flipped `closed`,
        // so this is a no-op (the `swap` inside sees `closed=true`).
        // Drop must not panic and can't surface failure — fire-and-
        // forget; the runtime keeps the task alive until completion.
        self.spawn_close_if_open();
    }
}
