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
//! These accessors live on the **sync** [`crate::ExecutedStatement`]
//! handle only. Async-consumption callers who need them should use
//! [`crate::statement::mutable::Statement::execute`] (which drives the
//! polling loop internally and returns a sync handle) rather than
//! `submit()`. The async surface is shaped for the
//! poll-loop-without-metadata use case; mirroring the accessors here
//! requires storing a status snapshot per-poll, which is tracked as a
//! follow-up. Wrappers presenting a Thrift-shaped operation API to
//! their host language (e.g. the napi binding's `Statement` class
//! mirroring `IOperation`) route through the sync handle so the
//! extended fields are available alongside fetch / cancel / close.

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
                StatementStatus::Failed(error)
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
    pub(crate) fn new(
        session: Arc<SessionInner>,
        statement_id: String,
        valid: ValidityFlag,
        closed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            session,
            statement_id,
            valid,
            closed,
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
                        return Err(Error::invalid_state()
                            .message(format!("Statement {} was cancelled", self.statement_id)));
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
