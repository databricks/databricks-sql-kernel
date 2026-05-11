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
    pub(crate) fn new(
        session: Arc<SessionInner>,
        statement_id: String,
        valid: ValidityFlag,
    ) -> Self {
        Self {
            session,
            statement_id,
            valid,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// One-shot status check. Non-blocking. Issues a single
    /// GetStatementStatus RPC. Returns `Err(InvalidStatementHandle)`
    /// locally — without hitting the wire — if the parent `Statement`
    /// has already invalidated this handle.
    pub async fn status(&self) -> Result<StatementStatus> {
        let _g = self.session.log_span().clone().entered();
        self.valid.check()?;
        let response = self
            .session
            .transport()
            .statement_status(&self.statement_id)
            .await?;
        Ok(StatementStatus::from_sea_response(&response))
    }

    /// Block until the server reaches a terminal state, then return
    /// the result stream. Internally polls with backoff using the
    /// session's [`crate::client::TransportConfig`] cadence (the
    /// same `poll_interval` the sync `execute()` path uses).
    pub async fn await_result(&self) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        self.valid.check()?;

        // Poll cadence comes from the session's defaults; the kernel
        // doesn't expose it as a per-handle knob today (callers who
        // need tighter control roll their own status() loop).
        let poll_interval = std::time::Duration::from_millis(500);

        loop {
            self.valid.check()?;
            let response = self
                .session
                .transport()
                .statement_status(&self.statement_id)
                .await?;
            match StatementStatus::from_sea_response(&response) {
                StatementStatus::Succeeded | StatementStatus::Closed => {
                    return self.materialise_result().await;
                }
                StatementStatus::Failed(e) => return Err(e),
                StatementStatus::Cancelled => {
                    return Err(Error::invalid_state()
                        .message(format!("Statement {} was cancelled", self.statement_id)));
                }
                StatementStatus::Pending | StatementStatus::Running => {
                    tokio::time::sleep(poll_interval).await;
                }
            }
        }
    }

    /// Fetch the result without polling. Errors if the statement has
    /// not finished — the precise error code is tracked under the
    /// pending `ErrorCode` taxonomy expansion (see spec addendum A8).
    /// Until then, returns `ErrorCode::InvalidState` with a clear
    /// message pointing callers at [`status`](Self::status). Useful
    /// when the caller has already confirmed completion themselves.
    pub async fn fetch_result(&self) -> Result<ResultStream> {
        let _g = self.session.log_span().clone().entered();
        self.valid.check()?;
        self.materialise_result().await
    }

    /// Internal: resolve the reader for an (expected to be) terminal
    /// statement. Returns `InvalidState` if the server reports the
    /// statement is still running.
    async fn materialise_result(&self) -> Result<ResultStream> {
        match self
            .session
            .transport()
            .fetch_completed_result(&self.statement_id)
            .await?
        {
            Some(result) => ResultStream::from_reader(result.reader),
            None => Err(Error::invalid_state().message(format!(
                "Statement {} is not terminal yet; call status() and try again",
                self.statement_id
            ))),
        }
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
        // Idempotent — the first call flips the flag; subsequent
        // calls return Ok(()) without hitting the wire. Matches
        // ExecutedStatement::close's contract.
        async {
            if self.closed.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            self.session
                .transport()
                .close_statement(&self.statement_id)
                .await
        }
        .instrument(self.session.log_span().clone())
        .await
    }
}

impl Drop for ExecutedAsyncStatement {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
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
