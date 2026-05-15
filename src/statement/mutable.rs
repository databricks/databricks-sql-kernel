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

//! `Statement` — the lifecycle owner.
//!
//! Per `docs/spec-v0.md` addendum A1, `Statement` owns one
//! `StatementSpec` and at most one live executed handle (sync or async).
//! It is the mutable, reusable surface returned by `Session::statement()`.
//!
//! `'static` for the same reason as every other v0 handle — see
//! `docs/spec-v0.md` addendum A1, "Why no Rust lifetimes on the
//! handles". The borrow against the parent session is expressed
//! through `Arc<SessionInner>`.
//!
//! ## Cancellation
//!
//! Cancellation lives on the executed handle, not on `Statement`. Two
//! cases cover the lifetime of a query:
//!
//! - **Before `execute()` / `submit()` returns**: the caller has the
//!   future. They cancel by dropping it (`tokio::select!`, timeout
//!   wrapper, etc.). The future installs a `Drop` guard that
//!   fires-and-forgets `CancelStatement` against the in-flight
//!   `statement_id`, so the server stops charging DBUs even though no
//!   handle was ever returned.
//! - **After `execute()` / `submit()` returns**: the caller has an
//!   `ExecutedStatement` / `ExecutedAsyncStatement` and calls
//!   `cancel()` on the handle (via `ExecutedStatementHandle`).
//!
//! There is intentionally no `Statement::cancel` — every cancellable
//! state has a more specific receiver, and adding a method here would
//! only introduce ambiguity about which `cancel()` to call.
//!
//! ## Re-execution
//!
//! Calling `execute()` or `submit()` again on the same `Statement`
//! auto-cancels + closes any prior executed handle. The spec persists
//! across re-executions; only the executed handle is replaced. The
//! prior handle becomes invalid; subsequent calls on it return
//! `ErrorCode::InvalidStatementHandle`. The invalidation propagates
//! via the [`ValidityFlag`] shared with each executed handle, so the
//! stale handle short-circuits locally — no network round-trip. See
//! `crate::statement::validity` for the flag implementation.

use crate::kernel_error::{Error, Result};
use crate::result_stream::ResultStream;
use crate::session::SessionInner;
use crate::statement::executed::ExecutedStatement;
use crate::statement::executed_async::ExecutedAsyncStatement;
use crate::statement::params::typed_value_to_sea_parameter;
use crate::statement::spec::{ResultFormat, StatementSpec};
use crate::statement::validity::ValidityFlag;
use crate::types::sea::{ExecuteParams, StatementState};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{warn, Instrument};

/// Lifecycle owner for a single statement spec.
///
/// Mutable, reusable. Mutate the spec in place via [`Statement::spec`],
/// then call [`Statement::execute`] or [`Statement::submit`] to produce
/// an immutable executed handle.
pub struct Statement {
    session: Arc<SessionInner>,
    spec: StatementSpec,
    /// Server-issued id of the most recent executed handle from this
    /// `Statement`. `None` until the first `execute()` / `submit()`.
    /// On re-execute, the kernel reads this slot, fires CancelStatement
    /// + CloseStatement against it, then overwrites with the new id.
    last_statement_id: Option<String>,
    /// Validity flag shared with the most recent executed handle.
    /// `Some` while a handle is live; `None` between constructions and
    /// after `Drop`. See `validity::ValidityFlag` for the
    /// Acquire/Release contract.
    last_valid: Option<ValidityFlag>,
    /// `closed` flag shared with the most recent `ExecutedStatement`.
    /// Pre-set to `true` when the server returned `state=CLOSED` in its
    /// execute response (the inline-result fast path), and flipped to
    /// `true` by `ExecutedStatement::close`. Lets `invalidate_prior`
    /// skip the CancelStatement + CloseStatement round-trips when the
    /// server side is already gone.
    last_closed: Option<Arc<AtomicBool>>,
}

impl Statement {
    /// Construct a new statement bound to `session`. Crate-private —
    /// wrappers obtain a `Statement` via `Session::statement()`.
    pub(crate) fn new(session: Arc<SessionInner>) -> Self {
        Self {
            session,
            spec: StatementSpec::new(),
            last_statement_id: None,
            last_valid: None,
            last_closed: None,
        }
    }

    /// Mutable access to the spec. Mutate (sql, params, conf, result
    /// format) before calling `execute()` / `submit()`.
    pub fn spec(&mut self) -> &mut StatementSpec {
        &mut self.spec
    }

    /// Read-only access to the spec, for logging / assertions.
    pub fn spec_ref(&self) -> &StatementSpec {
        &self.spec
    }

    /// Wait-for-result execution. The kernel sends ExecuteStatement
    /// **without** a `wait_timeout` field — the SEA server's
    /// default behaviour is to hold the connection for up to 10s,
    /// same wait as `wait_timeout=10s`, but it auto-closes the
    /// statement on success and returns `state=CLOSED`. Setting
    /// `wait_timeout=10s` explicitly returns `state=SUCCEEDED`
    /// (statement still alive server-side, requires a separate
    /// `CloseStatement`) and adds ~150–260ms to the response —
    /// observed against Dogfood; see PR description for the bench
    /// numbers. The kernel polls internally if the query needs
    /// longer than 10s.
    ///
    /// If a prior executed handle is still tracked by this `Statement`,
    /// it is auto-cancelled and closed before issuing the new
    /// ExecuteStatement, and the prior handle's
    /// [`ValidityFlag`] is flipped so subsequent calls on it
    /// short-circuit to `InvalidStatementHandle` without hitting the
    /// wire.
    pub async fn execute(&mut self) -> Result<ExecutedStatement> {
        // `instrument` (rather than `.entered()`) so the resulting
        // future stays `Send` — napi-rs's `execute_tokio_future` and
        // `tokio::spawn` both require `Send`, and `EnteredSpan` is
        // `!Send`. Clone the span up-front to avoid double-borrowing
        // `self.session` (once in the async body, once on the
        // `.instrument()` argument).
        let span = self.session.log_span().clone();
        async {
            self.invalidate_prior().await;
            let (sql, exec_params) = self.prepare_for_dispatch(None)?;

            let cancel_state = MidExecuteCancelState::new(Arc::clone(&self.session));
            let guard = cancel_state.guard();

            // SeaRestTransport::execute_statement polls to completion
            // (10s server-side wait_timeout, then GetStatementStatus
            // loop until terminal).
            let result = self
                .session
                .transport()
                .execute_statement(self.session.session_id(), &sql, &exec_params)
                .await?;

            cancel_state.observe(&result.statement_id);
            guard.defuse();

            let valid = ValidityFlag::new();
            self.last_statement_id = Some(result.statement_id.clone());
            self.last_valid = Some(valid.clone());

            let valid = ValidityFlag::new();
            // Only `Closed` triggers the short-circuit. `Succeeded` means
            // the server still owns the statement and we must issue
            // CloseStatement. `Failed` / `Canceled` are terminal but still
            // server-side-tracked — also fall through to the standard close
            // path. See `prepare_for_dispatch` for why omitting
            // `wait_timeout` typically yields `Closed` on the inline-result
            // fast path.
            let already_closed = result.server_state == StatementState::Closed;
            let closed = Arc::new(AtomicBool::new(already_closed));
            self.last_statement_id = Some(result.statement_id.clone());
            self.last_valid = Some(valid.clone());
            self.last_closed = Some(Arc::clone(&closed));

            let result_stream = ResultStream::from_reader(result.reader).await?;
            Ok(ExecutedStatement::new(
                Arc::clone(&self.session),
                result.statement_id,
                valid,
                result_stream,
                closed,
            ))
        }
        .instrument(span)
        .await
    }

    /// Submit-and-return. `wait_timeout=0s`; no direct results. Same
    /// auto-cancel-and-close semantics as `execute()`.
    ///
    /// Returns immediately once the server hands back a
    /// `statement_id` — the caller drives polling via
    /// [`ExecutedAsyncStatement::status`] / `await_result` /
    /// `fetch_result`. Long-running queries that would blow past
    /// `execute()`'s sync polling budget belong on this path.
    pub async fn submit(&mut self) -> Result<ExecutedAsyncStatement> {
        let span = self.session.log_span().clone();
        async {
            self.invalidate_prior().await;
            let (sql, exec_params) = self.prepare_for_dispatch(Some("0s"))?;

            let cancel_state = MidExecuteCancelState::new(Arc::clone(&self.session));
            let guard = cancel_state.guard();

            let response = self
                .session
                .transport()
                .submit_statement(self.session.session_id(), &sql, &exec_params)
                .await?;

            cancel_state.observe(&response.statement_id);
            guard.defuse();

            let valid = ValidityFlag::new();
            self.last_statement_id = Some(response.statement_id.clone());
            self.last_valid = Some(valid.clone());

            // submit path always uses wait_timeout=0s, so the server can't
            // return CLOSED yet; start the flag false and let later close()
            // calls flip it.
            let closed = Arc::new(AtomicBool::new(false));
            self.last_statement_id = Some(response.statement_id.clone());
            self.last_valid = Some(valid.clone());
            self.last_closed = Some(Arc::clone(&closed));

            Ok(ExecutedAsyncStatement::new(
                Arc::clone(&self.session),
                response.statement_id,
                valid,
                closed,
            ))
        }
        .instrument(span)
        .await
    }

    /// Explicit close. Cancels and closes any executed handle this
    /// `Statement` is still tracking, then drops the lifecycle
    /// owner. Mirrors what users of JDBC `Statement.close()` /
    /// ODBC `SQLFreeStmt(SQL_CLOSE)` / PySQL `cursor.close()`
    /// already expect: closing the lifecycle owner releases the
    /// associated server-side resources too.
    ///
    /// Implementation: just re-uses [`invalidate_prior`] which is
    /// already what `execute()` / `submit()` call before re-use,
    /// so the wire shape (CancelStatement + CloseStatement against
    /// the tracked `last_statement_id`, validity flag flipped) is
    /// identical to the auto-cancel-on-reuse path. Errors are
    /// swallowed as `warn!` for the same reason as that path —
    /// transient failures on a now-orphaned statement should not
    /// surface as user-visible errors; the server reclaims on
    /// session close anyway.
    ///
    /// After `close()`, any `ExecutedStatement` /
    /// `ExecutedAsyncStatement` the caller still holds
    /// short-circuits to `InvalidStatementHandle` locally on its
    /// next call — no wire round-trip.
    ///
    /// Symmetric with [`Session::close`]: consumes `self` so a
    /// double-close is a compile error. `Drop` runs as a fallback
    /// for callers that forget to call `close()` explicitly, but
    /// the explicit path is the supported one.
    pub async fn close(mut self) -> Result<()> {
        let _g = self.session.log_span().clone().entered();
        self.invalidate_prior().await;
        Ok(())
    }

    /// Shared spec-validation + `ExecuteParams` builder used by both
    /// [`execute`](Self::execute) and [`submit`](Self::submit).
    ///
    /// `wait_timeout_secs`:
    /// - `Some("0s")` — async submit; server returns immediately.
    /// - `None` — sync execute; omit the field so the server uses
    ///   its default wait (up to ~10s, auto-closes on success and
    ///   returns `state=CLOSED`). Setting `Some("10s")` explicitly
    ///   produces equivalent wait behaviour but the server then
    ///   returns `state=SUCCEEDED` (statement still alive) and is
    ///   ~150–260ms slower per request on Dogfood — verified by
    ///   probe.
    /// - Other concrete values (e.g. `Some("5s")`) are valid for
    ///   callers that want explicit control.
    fn prepare_for_dispatch(
        &self,
        wait_timeout_secs: Option<&str>,
    ) -> Result<(String, ExecuteParams)> {
        let sql = self
            .spec
            .sql
            .as_deref()
            .ok_or_else(|| {
                Error::invalid_argument().message("StatementSpec.sql must be set before execute()")
            })?
            .to_owned();
        if self.spec.result_format != ResultFormat::Arrow {
            return Err(Error::invalid_argument().message("v0 supports ResultFormat::Arrow only"));
        }
        if !self.spec.named_params.is_empty() {
            return Err(Error::invalid_argument().message(
                "named parameter bindings are not supported in v0 — SEA wire format \
                 carries positional bindings only",
            ));
        }

        // `wait_timeout = None` means "omit the field"; the server
        // applies its default (~10s wait + auto-close on success).
        // `on_wait_timeout` only matters when wait_timeout is set,
        // so mirror the presence.
        let mut exec_params = ExecuteParams {
            wait_timeout: wait_timeout_secs.map(|s| s.to_string()),
            on_wait_timeout: wait_timeout_secs.map(|_| "CONTINUE".to_string()),
            ..ExecuteParams::default()
        };
        for (i, value) in self.spec.positional_params.iter().enumerate() {
            exec_params
                .parameters
                .push(typed_value_to_sea_parameter((i + 1) as i32, value)?);
        }
        Ok((sql, exec_params))
    }

    /// Auto-cancel + close any prior executed handle this `Statement`
    /// produced. Errors are swallowed because re-execute should never
    /// fail because the *prior* server-side resource refused to clean
    /// up.
    async fn invalidate_prior(&mut self) {
        if let Some(valid) = self.last_valid.take() {
            valid.invalidate();
        }
        // If the shared `closed` flag is already set, the server side
        // is gone (either the server auto-closed on the inline-result
        // fast path, or the caller already closed the executed
        // handle). Flip it to claim the cleanup duty in case it
        // wasn't set yet, and skip the round-trips.
        let already_closed = self
            .last_closed
            .take()
            .map(|f| f.swap(true, Ordering::AcqRel))
            .unwrap_or(false);
        if let Some(prior_id) = self.last_statement_id.take() {
            if already_closed {
                return;
            }
            // Best-effort cancel + close. We don't await the cancel
            // separately — close is idempotent and the server
            // releases resources on close regardless of cancel
            // status.
            if let Err(e) = self.session.transport().cancel_statement(&prior_id).await {
                warn!(
                    statement_id = %prior_id,
                    error = %e,
                    "auto-cancel of prior statement failed; closing anyway"
                );
            }
            if let Err(e) = self.session.transport().close_statement(&prior_id).await {
                warn!(
                    statement_id = %prior_id,
                    error = %e,
                    "auto-close of prior statement failed; will be reclaimed by session close"
                );
            }
        }
    }
}

/// Shared state between `Statement::execute` and its `Drop` guard.
///
/// The guard reads `observed_id` on drop. If `defuse` was called
/// before the drop, the guard is a no-op; otherwise the guard
/// spawns a fire-and-forget `cancel_statement` against the observed
/// statement_id (if any).
struct MidExecuteCancelState {
    session: Arc<SessionInner>,
    observed_id: std::sync::Mutex<Option<String>>,
    defused: std::sync::atomic::AtomicBool,
}

impl MidExecuteCancelState {
    fn new(session: Arc<SessionInner>) -> Arc<Self> {
        Arc::new(Self {
            session,
            observed_id: std::sync::Mutex::new(None),
            defused: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn observe(&self, id: &str) {
        if let Ok(mut slot) = self.observed_id.lock() {
            *slot = Some(id.to_owned());
        }
    }

    fn guard(self: &Arc<Self>) -> MidExecuteCancelGuard {
        MidExecuteCancelGuard {
            state: Arc::clone(self),
        }
    }
}

/// RAII guard that fires a fire-and-forget `cancel_statement` on
/// drop unless [`defuse`](MidExecuteCancelGuard::defuse) was called.
struct MidExecuteCancelGuard {
    state: Arc<MidExecuteCancelState>,
}

impl MidExecuteCancelGuard {
    fn defuse(self) {
        self.state
            .defused
            .store(true, std::sync::atomic::Ordering::Release);
        // Drop runs, sees defused=true, and skips.
    }
}

impl Drop for MidExecuteCancelGuard {
    fn drop(&mut self) {
        if self
            .state
            .defused
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        let id = self.state.observed_id.lock().ok().and_then(|g| g.clone());
        let Some(id) = id else {
            return;
        };
        let transport = Arc::clone(self.state.session.transport());
        self.state.session.runtime_handle().spawn(async move {
            let _ = transport.cancel_statement(&id).await;
        });
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        // Flip the validity flag so any executed handle the caller
        // still holds short-circuits to InvalidStatementHandle on its
        // next call without a network round-trip. Server-side cleanup
        // for handles that *are* still live is the executed handle's
        // own Drop responsibility (it owns the Arc<SessionInner> too,
        // so its Drop runs after this when the handle outlives the
        // Statement).
        if let Some(valid) = self.last_valid.take() {
            valid.invalidate();
        }
        // `last_statement_id` is left as-is. By construction it is
        // only set immediately before `ExecutedStatement::new`
        // wraps it; if the caller still holds an `ExecutedStatement`,
        // that handle owns the close-on-drop responsibility. If the
        // caller dropped the handle before this `Statement`, the
        // handle's own `Drop` already spawned the close. There is no
        // remaining cleanup obligation here.
    }
}
