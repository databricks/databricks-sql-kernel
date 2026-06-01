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

//! `ExecutedAsyncStatement` — async-consumption executed handle.
//!
//! Returned by `Statement::submit()`. Per `docs/spec-v0.md` addendum
//! A1, the kernel returned without driving polling; the caller owns
//! the polling cadence via `status()` / `await_result()`. The server
//! may have already completed the statement by the time this handle
//! is constructed — the name "async" refers to the *consumption
//! mode*, not a state claim.
//!
//! Same `'static` + [`ValidityFlag`] story as `ExecutedStatement`:
//! the handle short-circuits to `InvalidStatementHandle` locally when
//! the parent `Statement` is re-executed, avoiding wasted network
//! round-trips on zombie handles.
//!
//! ## Extended SEA status fields (`num_modified_rows`,
//! `display_message`, `diagnostic_info`, `error_details_json`)
//!
//! Mirrored from the sync [`crate::ExecutedStatement`] handle:
//! [`ExecutedAsyncStatement::num_modified_rows`] /
//! [`ExecutedAsyncStatement::display_message`] /
//! [`ExecutedAsyncStatement::diagnostic_info`] /
//! [`ExecutedAsyncStatement::error_details_json`] all return the value
//! from the most recent SEA `StatementStatus` this handle has
//! observed (initial `submit_statement` response or any subsequent
//! `status()` / `await_result` poll). The Thrift `IOperation.status()`
//! contract — same field set available on the same handle regardless
//! of how the caller drove polling — is preserved on both kernel
//! consumption modes. Wrappers presenting a Thrift-shaped operation
//! API to their host language (e.g. the napi binding's `Statement`
//! class) can route through whichever handle their language idiom
//! prefers; field availability is identical.

use crate::client::ExecuteResponse;
use crate::kernel_error::Error;
use crate::kernel_error::Result;
use crate::result_stream::ResultStream;
use crate::session::SessionInner;
use crate::statement::handle::ExecutedStatementHandle;
use crate::statement::validity::ValidityFlag;
use crate::types::sea::StatementState;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{warn, Instrument};

/// Server-side execution status.
///
/// Mirrors the SEA `Status` enum. `Failed` carries the underlying
/// `Error` so callers see `vendor_code` / `sql_state` / etc.
//
// `large_enum_variant`: `Failed(Error)` is intentionally inline rather
// than boxed. `Error` has grown to ~216 bytes after picking up the
// Thrift-parity `display_message` / `diagnostic_info` /
// `error_details_json` fields, but this enum is constructed at most
// once per `status()` poll and immediately matched on by the caller —
// the per-call size is irrelevant in practice, and boxing here would
// force every pyo3 / direct-Rust caller pattern-matching on
// `Failed(err)` to update.
#[allow(clippy::large_enum_variant)]
#[non_exhaustive]
#[derive(Debug)]
pub enum StatementStatus {
    /// Server has not yet started the statement.
    Pending,
    /// Server is actively executing.
    Running,
    /// Terminal — results are ready.
    Succeeded,
    /// Terminal — server reported a failure. Carries the structured
    /// `Error` with `sql_state` / `error_code` / `query_id`
    /// populated when the wire envelope provided them.
    Failed(Error),
    /// Terminal — statement was cancelled.
    Cancelled,
    /// Terminal — statement was closed. The server emits this when
    /// it has delivered inline results and immediately released
    /// resources (no further fetching is needed). The kernel
    /// treats `Closed` as a terminal "result-ready" state.
    Closed,
}

impl StatementStatus {
    /// Stable string slug for this status, matching the connector's
    /// `CommandState` taxonomy one-for-one:
    /// `"Pending" | "Running" | "Succeeded" | "Failed" | "Cancelled" |
    /// "Closed"`. The `Failed` variant collapses to `"Failed"` (the
    /// carried `Error` is surfaced separately by callers that need the
    /// structured failure). `StatementStatus` is `#[non_exhaustive]`, so
    /// a future variant a stale caller doesn't recognise maps to
    /// `"Unknown"` — bindings get something deterministic instead of a
    /// panic.
    pub fn as_str(&self) -> &'static str {
        // The `_` arm is unreachable *within* this crate (the enum is
        // fully covered here), but `StatementStatus` is
        // `#[non_exhaustive]`, so a future variant must map to something
        // deterministic for downstream callers rather than panic.
        #[allow(unreachable_patterns)]
        match self {
            StatementStatus::Pending => "Pending",
            StatementStatus::Running => "Running",
            StatementStatus::Succeeded => "Succeeded",
            StatementStatus::Failed(_) => "Failed",
            StatementStatus::Cancelled => "Cancelled",
            StatementStatus::Closed => "Closed",
            _ => "Unknown",
        }
    }

    /// Map a SEA `ExecuteResponse` to its public kernel status. The
    /// `Failed` arm preserves the same structured error
    /// `Transport::execute_statement`'s sync polling produces, so
    /// callers get identical `sql_state` / `error_code` /
    /// `query_id` regardless of which path surfaced the failure.
    pub(crate) fn from_sea_response(response: &ExecuteResponse) -> Self {
        match response.status.state {
            StatementState::Pending => StatementStatus::Pending,
            StatementState::Running => StatementStatus::Running,
            StatementState::Succeeded => StatementStatus::Succeeded,
            StatementState::Canceled => StatementStatus::Cancelled,
            StatementState::Closed => StatementStatus::Closed,
            StatementState::Failed => {
                let service_error = response.status.error.as_ref();
                let error_msg = service_error
                    .and_then(|e| e.message.clone())
                    .unwrap_or_else(|| "Unknown error".to_string());
                let mut error = Error::sql_error()
                    .message(&error_msg)
                    .with_query_id(&response.statement_id);
                if let Some(err) = service_error {
                    let sqlstate = err
                        .sql_state
                        .as_ref()
                        .filter(|s| s.len() == 5)
                        .cloned()
                        .or_else(|| crate::kernel_error::extract_sqlstate_from_message(&error_msg));
                    if let Some(s) = sqlstate {
                        error = error.with_sql_state(s);
                    }
                    if let Some(ec) = err.error_code.as_deref().filter(|s| !s.is_empty()) {
                        error = error.with_error_code(ec);
                    }
                }
                // Mirror the sync path: carry the SEA extended status
                // fields (display_message / diagnostic_info /
                // error_details_json) onto the returned Error so the
                // napi binding's JS Error envelope sees the same field
                // set Thrift's `TGetOperationStatusResp` exposed on
                // failed operations, regardless of which kernel handle
                // surfaced the failure.
                StatementStatus::Failed(crate::kernel_error::carry_extended_status(
                    error,
                    &response.status,
                ))
            }
        }
    }
}

/// Async-consumption executed handle.
pub struct ExecutedAsyncStatement {
    session: Arc<SessionInner>,
    statement_id: String,
    /// Shared with the parent `Statement`; flipped on re-execute or
    /// when the parent `Statement` is dropped. Reads short-circuit
    /// stale calls locally with no network round-trip.
    valid: ValidityFlag,
    /// Flips to `true` after the first successful (or
    /// short-circuited) `close()`. Tells `Drop` to skip its
    /// fire-and-forget cleanup.
    closed: Arc<AtomicBool>,
    /// Last SEA `StatementStatus` observed by this handle (initial
    /// `submit_statement` response or any subsequent `status()` /
    /// `await_result` poll). Read by the `num_modified_rows()` /
    /// `display_message()` / `diagnostic_info()` / `error_details_json()`
    /// accessors — matching the same field set the sync
    /// [`crate::ExecutedStatement`] exposes so the napi binding's
    /// `Statement` JS class (which mirrors the Thrift
    /// `TGetOperationStatusResp` field set) sees a uniform contract
    /// whether the kernel handle was produced via `execute()` or
    /// `submit()`.
    ///
    /// `std::sync::Mutex` (not `tokio::sync::Mutex`): the guard is
    /// only held across the cheap `Option::clone()` /
    /// `Option::replace` paths inside the accessors and the poll
    /// loop's post-await write. Holding it across `.await` would
    /// pessimise scheduling without buying anything — the accessors
    /// don't `.await` and the poll loop releases the lock before
    /// every network round-trip.
    latest_status: Arc<std::sync::Mutex<crate::types::sea::StatementStatus>>,
}

impl ExecutedAsyncStatement {
    /// Crate-internal constructor. Built by [`Statement::submit`].
    ///
    /// `closed` is shared with the parent [`Statement`] so flipping it
    /// here also short-circuits the lifecycle-owner's
    /// `invalidate_prior` cleanup path. The submit path always sends
    /// `wait_timeout=0s`, so the server returns `Pending`/`Running` and
    /// the flag starts `false`; the share-and-flip pattern still
    /// applies once the caller (or `Drop`) closes either handle.
    ///
    /// `initial_status` is the SEA `StatementStatus` from the
    /// `submit_statement` response — the very first observation the
    /// kernel has of this server-side statement. Stored so accessors
    /// return useful values even before the caller drives a single
    /// `status()` / `await_result()` poll (e.g. when the server
    /// already finished by the time `submit` returned).
    pub(crate) fn new(
        session: Arc<SessionInner>,
        statement_id: String,
        valid: ValidityFlag,
        closed: Arc<AtomicBool>,
        initial_status: crate::types::sea::StatementStatus,
    ) -> Self {
        Self {
            session,
            statement_id,
            valid,
            closed,
            latest_status: Arc::new(std::sync::Mutex::new(initial_status)),
        }
    }

    /// Number of rows modified by the statement (UPDATE / INSERT /
    /// DELETE / MERGE). Reflects the most recent SEA
    /// `StatementStatus` this handle has seen (initial submit
    /// response, or any subsequent `status()` / `await_result()`
    /// poll). Absent on SELECT and on warehouses that don't surface
    /// the counter.
    ///
    /// Mirrors the same-named accessor on the sync
    /// [`crate::ExecutedStatement`] and the same-named optional field
    /// on the Thrift `TGetOperationStatusResp`.
    pub fn num_modified_rows(&self) -> Option<i64> {
        self.latest_status
            .lock()
            .ok()
            .and_then(|s| s.num_modified_rows)
    }

    /// Server-supplied user-facing message. Reflects the most recent
    /// SEA `StatementStatus` this handle has seen.
    ///
    /// **PII / sensitive-data note:** server-supplied free-form text
    /// may include SQL fragments or parameter values. Redact before
    /// centralised logging.
    pub fn display_message(&self) -> Option<String> {
        self.latest_status
            .lock()
            .ok()
            .and_then(|s| s.display_message.clone())
    }

    /// Server-supplied diagnostic detail — multi-line operator /
    /// stack context. Reflects the most recent SEA `StatementStatus`
    /// this handle has seen.
    ///
    /// **PII / sensitive-data note:** like `display_message`, this is
    /// server-supplied free-form text and may include SQL fragments
    /// or values. Redact before centralised logging.
    pub fn diagnostic_info(&self) -> Option<String> {
        self.latest_status
            .lock()
            .ok()
            .and_then(|s| s.diagnostic_info.clone())
    }

    /// Server-supplied JSON blob with extended error details.
    /// Reflects the most recent SEA `StatementStatus` this handle has
    /// seen.
    ///
    /// **Server-side gating:** populated only when the workspace has
    /// `spark.databricks.sql.errorDetailsJson.enabled = true` set on
    /// the cluster (per `DatabricksSQLConf` — internal flag, default
    /// false, not settable via session-conf). Outside of admin-
    /// enabled workspaces the field is always None even on Failed
    /// statements that otherwise populate `display_message` /
    /// `diagnostic_info`.
    ///
    /// **Unbounded:** when populated, content is `{"errorClass": "...",
    /// "messageTemplate": "..."}` server-side, but the kernel passes
    /// the string through verbatim — wrappers parse if they need
    /// structured access and bound size before logging.
    pub fn error_details_json(&self) -> Option<String> {
        self.latest_status
            .lock()
            .ok()
            .and_then(|s| s.error_details_json.clone())
    }

    /// Internal helper — overwrite the cached `StatementStatus` with
    /// a fresh poll's response. Called by `status()` /
    /// `await_result()` after every successful
    /// `statement_status` RPC. Lock poisoning is intentionally
    /// swallowed: a poisoned lock here would only happen if a panic
    /// landed mid-write, and the recovery path is to keep the prior
    /// status visible (worse: a fresh poll's status lost; better:
    /// the handle's accessor surface keeps working).
    fn record_latest_status(&self, fresh: crate::types::sea::StatementStatus) {
        if let Ok(mut guard) = self.latest_status.lock() {
            *guard = fresh;
        }
    }

    /// One-shot status check. Non-blocking. Issues a single
    /// GetStatementStatus RPC. Returns `Err(InvalidStatementHandle)`
    /// locally — without hitting the wire — if the parent `Statement`
    /// has already invalidated this handle.
    pub async fn status(&self) -> Result<StatementStatus> {
        let span = self.session.log_span().clone();
        async {
            self.valid.check()?;
            let response = self
                .session
                .transport()
                .statement_status(&self.statement_id)
                .await?;
            // Cache the wire status so the extended-field accessors
            // reflect what THIS handle has observed (instead of the
            // stale initial-submit snapshot).
            self.record_latest_status(response.status.clone());
            Ok(StatementStatus::from_sea_response(&response))
        }
        .instrument(span)
        .await
    }

    /// Block until the server reaches a terminal state, then return
    /// the result stream. Internally polls with backoff using the
    /// session's [`crate::client::TransportConfig`] cadence (the
    /// same `poll_interval` / `poll_timeout` the sync `execute()`
    /// path uses).
    ///
    /// **Drop-cancellation.** Dropping the returned future before
    /// it resolves to a terminal state (e.g. a winning branch in
    /// `tokio::select!`, a timeout wrapper firing) spawns a
    /// fire-and-forget `cancel_statement` against the server so a
    /// long-running query doesn't keep charging DBUs after the
    /// caller has given up. The cancel is best-effort; success is
    /// not awaited.
    ///
    /// **Polling timeout.** When the transport's
    /// `TransportConfig.poll_timeout` elapses without reaching a
    /// terminal state, returns `Error::timeout()` (same shape the
    /// sync polling path produces). The caller may issue a fresh
    /// `status()` or `await_result()` to resume.
    pub async fn await_result(&self) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        async {
            self.valid.check()?;

            // Poll cadence + ceiling come from the transport's config —
            // same source the sync `wait_for_completion` path uses, so
            // a caller's `SessionBuilder` tuning applies to both modes.
            let cfg = self.session.transport().poll_config();
            let start = std::time::Instant::now();

            // RAII guard: if the future is dropped mid-poll (timeout,
            // tokio::select! loser, task cancellation), spawn a
            // best-effort `cancel_statement` so the server stops
            // charging DBUs on a now-orphaned query. Defused once we
            // reach a terminal state on the success or error path.
            let cancel_guard =
                AwaitResultCancelGuard::new(Arc::clone(&self.session), self.statement_id.clone());

            loop {
                self.valid.check()?;
                let response = self
                    .session
                    .transport()
                    .statement_status(&self.statement_id)
                    .await?;
                // Cache the wire status so the extended-field
                // accessors reflect THIS poll's observation — not the
                // initial-submit snapshot that's now stale by minutes.
                self.record_latest_status(response.status.clone());
                match StatementStatus::from_sea_response(&response) {
                    StatementStatus::Succeeded | StatementStatus::Closed => {
                        cancel_guard.defuse();
                        return self.materialise_result().await;
                    }
                    StatementStatus::Failed(e) => {
                        // Server already decided this is terminal — no
                        // need to fire a redundant cancel.
                        cancel_guard.defuse();
                        return Err(e);
                    }
                    StatementStatus::Cancelled => {
                        cancel_guard.defuse();
                        // Carry the SEA extended status fields onto
                        // the returned Error — mirrors the sync
                        // `terminal_outcome::Canceled` arm so the
                        // Cancelled-state envelope contract is
                        // identical across kernel handles.
                        let error = Error::invalid_state()
                            .message(format!("Statement {} was cancelled", self.statement_id))
                            .with_query_id(&self.statement_id);
                        return Err(crate::kernel_error::carry_extended_status(
                            error,
                            &response.status,
                        ));
                    }
                    StatementStatus::Pending | StatementStatus::Running => {
                        if start.elapsed() > cfg.poll_timeout {
                            // Internal poll-timeout: we asked to wait,
                            // we gave up. Leave the cancel guard armed
                            // so the server stops charging DBUs on the
                            // now-orphaned query when we return — same
                            // policy as a caller-initiated future drop.
                            return Err(Error::timeout().message(format!(
                                "Statement {} did not reach a terminal state within {:?}",
                                self.statement_id, cfg.poll_timeout
                            )));
                        }
                        tokio::time::sleep(cfg.poll_interval).await;
                    }
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Fetch the result without polling. Errors if the statement has
    /// not finished — the precise error code is tracked under the
    /// pending `ErrorCode` taxonomy expansion (see spec addendum A8).
    /// Until then, returns `ErrorCode::InvalidState` with a clear
    /// message pointing callers at [`status`](Self::status). Useful
    /// when the caller has already confirmed completion themselves.
    pub async fn fetch_result(&self) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        async {
            self.valid.check()?;
            self.materialise_result().await
        }
        .instrument(span)
        .await
    }

    /// Internal: resolve the reader for an (expected to be) terminal
    /// statement. Returns `InvalidState` if the server reports the
    /// statement is still running. The error includes the observed
    /// status so callers don't have to re-query, and points at the
    /// actionable follow-up.
    async fn materialise_result(&self) -> Result<ResultStream> {
        match self
            .session
            .transport()
            .fetch_completed_result(&self.statement_id)
            .await?
        {
            Some(result) => ResultStream::from_reader(result.reader).await,
            None => {
                // Re-read the status so the error message names the
                // observed state (Pending vs Running). Best-effort —
                // if the status call itself fails, fall back to the
                // generic non-terminal message.
                let observed = match self
                    .session
                    .transport()
                    .statement_status(&self.statement_id)
                    .await
                {
                    Ok(r) => match r.status.state {
                        crate::types::sea::StatementState::Pending => "Pending",
                        crate::types::sea::StatementState::Running => "Running",
                        _ => "non-terminal",
                    },
                    Err(_) => "non-terminal",
                };
                Err(Error::invalid_state().message(format!(
                    "Statement {} is not terminal yet (status: {}); call await_result() to \
                     block until done, or poll status() and re-call fetch_result() once it \
                     reports Succeeded / Closed",
                    self.statement_id, observed
                )))
            }
        }
    }
}

/// RAII guard that fires a fire-and-forget `cancel_statement` on
/// drop unless [`defuse`](AwaitResultCancelGuard::defuse) was called.
///
/// Installed at the top of `await_result` so that a future dropped
/// mid-poll (the canonical `tokio::select!` / `tokio::time::timeout`
/// pattern) doesn't leave a long-running query charging DBUs.
/// Defuses on every terminal status branch in the poll loop —
/// success, failure, server-reported cancel — so the cancel only
/// fires on caller-initiated drops.
struct AwaitResultCancelGuard {
    session: Arc<SessionInner>,
    statement_id: String,
    defused: bool,
}

impl AwaitResultCancelGuard {
    fn new(session: Arc<SessionInner>, statement_id: String) -> Self {
        Self {
            session,
            statement_id,
            defused: false,
        }
    }

    fn defuse(mut self) {
        self.defused = true;
        // `Drop` will see `defused = true` and skip.
    }
}

impl Drop for AwaitResultCancelGuard {
    fn drop(&mut self) {
        if self.defused {
            return;
        }
        // Spawn fire-and-forget. Drop must not panic and we can't
        // surface a failure here. The runtime keeps the background
        // task alive until the cancel completes.
        let transport = Arc::clone(self.session.transport());
        let runtime = self.session.runtime_handle().clone();
        let stmt_id = self.statement_id.clone();
        runtime.spawn(async move {
            if let Err(e) = transport.cancel_statement(&stmt_id).await {
                warn!(
                    statement_id = %stmt_id,
                    error = %e,
                    "fire-and-forget cancel_statement on await_result drop failed",
                );
            }
        });
    }
}

#[async_trait]
impl ExecutedStatementHandle for ExecutedAsyncStatement {
    fn statement_id(&self) -> &str {
        &self.statement_id
    }

    async fn cancel(&self) -> Result<()> {
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
        // `Session::close` and the sibling `ExecutedStatement::close`.
        //
        // The `&self` signature allows two callers to race and both
        // hit the wire. The server-side close is idempotent (it
        // returns the same Ok for duplicate close_statement calls),
        // so the only cost of the race is one redundant RPC — far
        // better than dropping the retry contract.
        //
        // Note: today's SEA transport at `src/client/sea.rs::
        // close_statement` swallows wire errors and always returns
        // `Ok(())`, so the retry-on-failure branch is reachable only
        // once the transport is changed to surface real errors. The
        // load-then-store-on-success shape is the forward-prep for
        // that change — see the JIRA candidate at
        // `sea-workflow/jira-candidates/2026-05-25-kernel-sea-close-statement-surface-errors.md`.
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

impl ExecutedAsyncStatement {
    /// Produce a detached [`AsyncStatementCanceller`] that can fire a
    /// server-side cancel for this statement *without borrowing the
    /// handle*.
    ///
    /// Use when the handle is parked inside a long-running
    /// [`await_result`](Self::await_result) — behind a binding-side
    /// lock, say — and an out-of-band `cancel()` must still reach the
    /// server. The canceller flips the statement to a terminal state,
    /// which the parked `await_result` poll loop observes (`Cancelled`)
    /// and returns on, so an explicit cancel interrupts the await
    /// instead of queueing behind it.
    pub fn canceller(&self) -> AsyncStatementCanceller {
        AsyncStatementCanceller {
            session: Arc::clone(&self.session),
            statement_id: self.statement_id.clone(),
            valid: self.valid.clone(),
        }
    }
}

/// Detached canceller for an in-flight async statement.
///
/// Holds only the shared session handle, the statement id, and the
/// shared [`ValidityFlag`] — **not** the [`ExecutedAsyncStatement`]
/// itself — so a binding can fire a server-side cancel while the handle
/// is borrowed elsewhere (e.g. parked inside a long-running
/// [`ExecutedAsyncStatement::await_result`] that a binding-side lock is
/// held across). Produced by [`ExecutedAsyncStatement::canceller`].
///
/// Cheap to [`Clone`] (one `Arc` bump plus a `String` / flag clone).
#[derive(Clone)]
pub struct AsyncStatementCanceller {
    session: Arc<SessionInner>,
    statement_id: String,
    valid: ValidityFlag,
}

impl AsyncStatementCanceller {
    /// Server-side cancel via `cancel_statement(statement_id)`.
    ///
    /// Short-circuits to `InvalidStatementHandle` (no wire round-trip)
    /// if the parent handle was invalidated (re-execute / parent
    /// `Statement` drop). Idempotent against a server already in a
    /// terminal state — the kernel's `cancel_statement` is a no-op
    /// there. Byte-for-byte the same operation as
    /// [`ExecutedAsyncStatement::cancel`]; the two share no mutable
    /// state, so this can run concurrently with an in-flight
    /// `await_result` on the same statement.
    pub async fn cancel(&self) -> Result<()> {
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

    /// The server-issued statement id this canceller targets.
    pub fn statement_id(&self) -> &str {
        &self.statement_id
    }
}

impl Drop for ExecutedAsyncStatement {
    fn drop(&mut self) {
        // `swap` (not `load`) so the shared `closed` flag — held by the
        // parent `Statement::last_closed` — reflects that Drop is now
        // handling cleanup, preventing a duplicate `CloseStatement`
        // from `invalidate_prior` on the next `execute()` / `submit()`.
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        // Fire-and-forget close on the session's runtime — mirrors
        // ExecutedStatement::Drop. Errors are swallowed by design;
        // Drop must not panic and the server reclaims the statement
        // when the session closes anyway.
        let transport = Arc::clone(self.session.transport());
        let runtime = self.session.runtime_handle().clone();
        let stmt_id = self.statement_id.clone();
        runtime.spawn(async move {
            if let Err(e) = transport.close_statement(&stmt_id).await {
                warn!(
                    statement_id = %stmt_id,
                    error = %e,
                    "fire-and-forget close_statement on ExecutedAsyncStatement::Drop failed; \
                     server-side resource reclaimed when the session closes"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_as_str_covers_all_variants() {
        assert_eq!(StatementStatus::Pending.as_str(), "Pending");
        assert_eq!(StatementStatus::Running.as_str(), "Running");
        assert_eq!(StatementStatus::Succeeded.as_str(), "Succeeded");
        assert_eq!(StatementStatus::Cancelled.as_str(), "Cancelled");
        assert_eq!(StatementStatus::Closed.as_str(), "Closed");
        let failed = StatementStatus::Failed(Error::sql_error().message("boom"));
        assert_eq!(failed.as_str(), "Failed");
    }
}
