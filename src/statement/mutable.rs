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
//! auto-closes any prior executed handle (fire-and-forget — see
//! [`Statement::invalidate_prior`]). The spec persists across
//! re-executions; only the executed handle is replaced. The prior
//! handle becomes invalid; subsequent calls on it return
//! `ErrorCode::InvalidStatementHandle`. The invalidation propagates
//! via the [`ValidityFlag`] shared with each executed handle, so the
//! stale handle short-circuits locally — no network round-trip. See
//! `crate::statement::validity` for the flag implementation.

use crate::kernel_error::{Error, Result};
use crate::klog::warn;
use crate::result_stream::ResultStream;
use crate::session::SessionInner;
use crate::statement::executed::ExecutedStatement;
use crate::statement::executed_async::ExecutedAsyncStatement;
use crate::statement::params::{named_typed_value_to_sea_parameter, typed_value_to_sea_parameter};
use crate::statement::spec::{ResultFormat, StatementSpec};
use crate::statement::validity::ValidityFlag;
use crate::types::sea::{ExecuteParams, StatementState};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tracing::Instrument;

/// Shared slot holding the server statement id of the statement
/// currently in flight on a [`Statement`] (or its most recent
/// execution). Written by `execute()` / `submit()` as soon as the
/// transport surfaces the id; read by [`StatementCanceller`] so an
/// out-of-band `cancel()` from another thread can reach a *running*
/// sync query — the case the per-call drop guard can't cover because
/// the binding's `block_on` runs the execute future to completion.
type InflightId = Arc<Mutex<Option<String>>>;

/// Resolve the SEA `(wait_timeout, on_wait_timeout)` pair for an execute /
/// submit dispatch. Pure so the precedence rules are unit-testable without
/// a live `Session`.
///
/// - `caller_wait_secs = Some(s)` — the caller pinned the wait shape
///   (e.g. `submit` passes `"0s"` for async). Respected verbatim with
///   `CONTINUE`; overriding it would make async submit block.
/// - `caller_wait_secs = None` — the sync `execute` path. A per-statement
///   `spec_query_timeout_secs` then bounds the server wait and `CANCEL`s
///   the statement on elapse (JDBC `setQueryTimeout` / Python `use_sea`'s
///   `wait_timeout` semantics). With no spec timeout, both are omitted so
///   the server applies its default (~10s wait + auto-close on success).
fn resolve_wait_timeout(
    caller_wait_secs: Option<&str>,
    spec_query_timeout_secs: Option<u32>,
) -> (Option<String>, Option<String>) {
    match caller_wait_secs {
        Some(s) => (Some(s.to_string()), Some("CONTINUE".to_string())),
        None => match spec_query_timeout_secs {
            Some(secs) => (Some(format!("{secs}s")), Some("CANCEL".to_string())),
            None => (None, None),
        },
    }
}

/// Build the `(sql, ExecuteParams)` dispatch pair from a statement spec.
///
/// Pure — borrows `spec`, touches no `Session` — so the spec → wire-params
/// mapping (row_limit, the resolved wait/on-wait pair, and **1-based**
/// positional + named parameter binding) is unit-testable without a live
/// session. Shared by both the sync and async dispatch paths via
/// [`Statement::prepare_for_dispatch`].
fn build_execute_params(
    spec: &StatementSpec,
    wait_timeout_secs: Option<&str>,
) -> Result<(String, ExecuteParams)> {
    let sql = spec
        .sql
        .as_deref()
        .ok_or_else(|| {
            Error::invalid_argument().message("StatementSpec.sql must be set before execute()")
        })?
        .to_owned();
    if spec.result_format != ResultFormat::Arrow {
        return Err(Error::invalid_argument().message("v0 supports ResultFormat::Arrow only"));
    }
    // Positional and named parameters are mutually exclusive: the SEA
    // server rejects a mixed `parameters` array with
    // `INVALID_PARAMETER_MARKER_VALUE.MISSING_NAME` (a `BAD_REQUEST`).
    // Reject at bind time in the kernel so every binding (napi, pyo3,
    // direct Rust) gets the same InvalidArgument before the wire,
    // rather than an opaque server round-trip failure.
    if !spec.positional_params.is_empty() && !spec.named_params.is_empty() {
        return Err(Error::invalid_argument()
            .message("positional and named parameters are mutually exclusive; pass only one"));
    }
    let (wait_timeout, on_wait_timeout) =
        resolve_wait_timeout(wait_timeout_secs, spec.query_timeout_secs);
    let mut exec_params = ExecuteParams {
        wait_timeout,
        on_wait_timeout,
        row_limit: spec.row_limit,
        ..ExecuteParams::default()
    };
    for (i, value) in spec.positional_params.iter().enumerate() {
        exec_params
            .parameters
            .push(typed_value_to_sea_parameter((i + 1) as i32, value)?);
    }
    for (name, value) in spec.named_params.iter() {
        exec_params
            .parameters
            .push(named_typed_value_to_sea_parameter(name.clone(), value)?);
    }
    Ok((sql, exec_params))
}

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
    /// On re-execute, the kernel reads this slot and spawns a fire-and-
    /// forget `CloseStatement` against it (skipped if the server already
    /// auto-closed via the inline-result fast path), then overwrites
    /// with the new id.
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
    /// skip the `CloseStatement` round-trip entirely when the server
    /// side is already gone.
    last_closed: Option<Arc<AtomicBool>>,
    /// Server statement id of the in-flight (or most recent) execution,
    /// shared with any [`StatementCanceller`] handed out via
    /// [`Statement::canceller`]. Populated mid-execute as soon as the
    /// transport returns the id; reset to `None` at the start of each
    /// new `execute()` / `submit()` so a canceller never targets a
    /// stale prior statement.
    inflight_id: InflightId,
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
            inflight_id: Arc::new(Mutex::new(None)),
        }
    }

    /// Produce a detached [`StatementCanceller`] that can cancel the
    /// statement currently in flight on this `Statement` *without
    /// borrowing it*.
    ///
    /// This is the sync-execute counterpart to
    /// [`ExecutedAsyncStatement::canceller`](crate::statement::executed_async::ExecutedAsyncStatement::canceller).
    /// The blocking `execute()` consumes `&mut self` and runs to
    /// completion inside the binding's `block_on`, so a second thread
    /// can't reach an `ExecutedStatement` to cancel a *running* query.
    /// Obtain a `StatementCanceller` **before** calling `execute()`;
    /// once the server issues the statement id mid-execute, the
    /// canceller's [`cancel`](StatementCanceller::cancel) reaches it.
    ///
    /// Cheap to clone (two `Arc` bumps). If `cancel()` fires before the
    /// id is observed (query still in the initial submit round-trip) it
    /// is a no-op; the per-call drop guard still covers the
    /// future-dropped case.
    pub fn canceller(&self) -> StatementCanceller {
        StatementCanceller {
            session: Arc::clone(&self.session),
            inflight_id: Arc::clone(&self.inflight_id),
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
    /// the kernel spawns a fire-and-forget `CloseStatement` against it
    /// before issuing the new ExecuteStatement (skipped when the
    /// inline-result fast path already returned `state=CLOSED`), and
    /// the prior handle's [`ValidityFlag`] is flipped so subsequent
    /// calls on it short-circuit to `InvalidStatementHandle` without
    /// hitting the wire.
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
            let (sql, mut exec_params) = self.prepare_for_dispatch(None)?;

            let cancel_state = MidExecuteCancelState::new(
                Arc::clone(&self.session),
                Arc::clone(&self.inflight_id),
            );
            let guard = cancel_state.guard();

            // Publish the server statement id to the shared in-flight
            // slot as soon as the initial POST returns (the transport
            // fills this before its poll loop) so a concurrent
            // `StatementCanceller` can cancel a still-running query.
            // `MidExecuteCancelState::new` already cleared the slot, so
            // set the observer after it.
            exec_params.inflight_id_observer = Some(Arc::clone(&self.inflight_id));

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
            // Only `Closed` triggers the short-circuit. `Succeeded` means
            // the server still owns the statement and we must issue
            // CloseStatement. `Failed` / `Canceled` are terminal but still
            // server-side-tracked — also fall through to the standard close
            // path. See `prepare_for_dispatch` for why omitting
            // `wait_timeout` typically yields `Closed` on the inline-result
            // fast path.
            let already_closed = result.status.state == StatementState::Closed;
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
                result.status,
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

            let cancel_state = MidExecuteCancelState::new(
                Arc::clone(&self.session),
                Arc::clone(&self.inflight_id),
            );
            let guard = cancel_state.guard();

            let response = self
                .session
                .transport()
                .submit_statement(self.session.session_id(), &sql, &exec_params)
                .await?;

            cancel_state.observe(&response.statement_id);
            guard.defuse();

            let valid = ValidityFlag::new();
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
                // Seed the executed handle with the SEA status the
                // server returned on the initial `submit_statement`
                // response. Subsequent `status()` / `await_result()`
                // polls overwrite it, but accessors return useful
                // values immediately even before the caller drives
                // the first poll.
                response.status,
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
        let span = self.session.log_span().clone();
        async {
            self.invalidate_prior().await;
            Ok(())
        }
        .instrument(span)
        .await
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
        build_execute_params(&self.spec, wait_timeout_secs)
    }

    /// Auto-close any prior executed handle this `Statement` produced.
    ///
    /// Happy path: by the time the caller is starting a *new*
    /// `execute()`, the prior statement has already completed
    /// server-side (either auto-closed via the inline-result fast
    /// path's `state=CLOSED`, or sitting in `SUCCEEDED` after the
    /// consumer drained the result stream). There is nothing in
    /// flight to cancel — `cancel_statement` would be a wasted
    /// round-trip asking the server to interrupt a query that's
    /// already done. Skip it and only fire `close_statement` to
    /// release the server-side handle.
    ///
    /// `close_statement` is spawned fire-and-forget so the next
    /// `execute()` can fire without waiting on the cleanup RPC.
    /// On cluster networks each saved RTT is ~5-20ms; on tight
    /// re-execute loops over cloud-fetched results that's a
    /// measurable per-iter win. Correctness is preserved because
    /// the server's close_statement is idempotent and a transient
    /// failure is harmless — the server reclaims the handle at
    /// session expiry. Mirrors `ExecutedStatement::drop`.
    ///
    /// In-flight cancel (interrupting a still-running query) is
    /// handled separately by [`MidExecuteCancelGuard`], which fires
    /// only when `execute()` itself is dropped mid-await.
    async fn invalidate_prior(&mut self) {
        if let Some(valid) = self.last_valid.take() {
            valid.invalidate();
        }
        // If the shared `closed` flag is already set, the server side
        // is gone (server auto-closed on the inline-result fast path,
        // or the caller already closed the executed handle). Flip it
        // to claim the cleanup duty in case it wasn't set yet, and
        // skip the round-trip.
        let already_closed = self
            .last_closed
            .take()
            .map(|f| f.swap(true, Ordering::AcqRel))
            .unwrap_or(false);
        if let Some(prior_id) = self.last_statement_id.take() {
            if already_closed {
                return;
            }
            let transport = Arc::clone(self.session.transport());
            self.session.runtime_handle().spawn(async move {
                if let Err(e) = transport.close_statement(&prior_id).await {
                    warn!(
                        statement_id = %prior_id,
                        error = %e,
                        "auto-close of prior statement failed; will be reclaimed by session close"
                    );
                }
            });
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
    /// Shared with the owning [`Statement::inflight_id`] so an
    /// out-of-band [`StatementCanceller`] reads the same id this guard
    /// observes. Reset to `None` on construction (new execution).
    observed_id: InflightId,
    defused: std::sync::atomic::AtomicBool,
}

impl MidExecuteCancelState {
    /// `inflight_id` is the owning `Statement`'s shared slot. We clear
    /// it here so a canceller can't fire against a prior statement
    /// while this execution is still establishing its own id.
    fn new(session: Arc<SessionInner>, inflight_id: InflightId) -> Arc<Self> {
        if let Ok(mut slot) = inflight_id.lock() {
            *slot = None;
        }
        Arc::new(Self {
            session,
            observed_id: inflight_id,
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

/// Detached canceller for the statement currently in flight on a
/// [`Statement`]. Produced by [`Statement::canceller`].
///
/// Holds only the shared session handle and the `Statement`'s shared
/// in-flight-id slot — **not** the `Statement` itself — so a binding
/// can fire a server-side cancel from another thread while `execute()`
/// is blocked. The sync-execute analogue of
/// [`AsyncStatementCanceller`](crate::statement::executed_async::AsyncStatementCanceller).
///
/// Cheap to [`Clone`] (two `Arc` bumps).
#[derive(Clone)]
pub struct StatementCanceller {
    session: Arc<SessionInner>,
    inflight_id: InflightId,
}

impl StatementCanceller {
    /// Server-side cancel of the in-flight statement via
    /// `cancel_statement(statement_id)`.
    ///
    /// Returns `Ok(true)` if a cancel RPC was actually dispatched (a
    /// statement id had been observed) and `Ok(false)` if it was a no-op
    /// because no id has been observed yet — either nothing has executed,
    /// or the query is still in its initial submit round-trip and the
    /// server hasn't issued an id. The dispatched/no-op decision and the
    /// returned flag come from the *same* id read, so a caller can attribute
    /// a subsequent execute error to this cancel without a separate (racy)
    /// `statement_id()` re-check.
    ///
    /// Idempotent against a server already in a terminal state (the
    /// kernel's `cancel_statement` is a no-op there). Safe to call
    /// concurrently with the `execute()` it targets — the two share
    /// only the id slot (behind a `Mutex`), no mutable statement state.
    pub async fn cancel(&self) -> Result<bool> {
        let id = self.inflight_id.lock().ok().and_then(|g| g.clone());
        let Some(id) = id else {
            return Ok(false);
        };
        async {
            self.session.transport().cancel_statement(&id).await?;
            Ok(true)
        }
        .instrument(self.session.log_span().clone())
        .await
    }

    /// The server-issued statement id this canceller currently targets,
    /// if one has been observed yet.
    pub fn statement_id(&self) -> Option<String> {
        self.inflight_id.lock().ok().and_then(|g| g.clone())
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

#[cfg(test)]
mod tests {
    use super::{build_execute_params, resolve_wait_timeout};
    use crate::statement::spec::{StatementSpec, TypedValue};

    #[test]
    fn async_submit_caller_value_is_respected_verbatim() {
        // submit() pins "0s" for async; never overridden, even if a spec
        // query_timeout is set (overriding would make submit block).
        let (wait, on_wait) = resolve_wait_timeout(Some("0s"), Some(30));
        assert_eq!(wait.as_deref(), Some("0s"));
        assert_eq!(on_wait.as_deref(), Some("CONTINUE"));
    }

    #[test]
    fn sync_with_query_timeout_cancels_on_elapse() {
        // execute() passes None; a spec query_timeout bounds the wait and
        // CANCELs the statement on elapse (server statement-timeout).
        let (wait, on_wait) = resolve_wait_timeout(None, Some(30));
        assert_eq!(wait.as_deref(), Some("30s"));
        assert_eq!(on_wait.as_deref(), Some("CANCEL"));
    }

    #[test]
    fn sync_without_query_timeout_omits_field() {
        // No caller pin and no spec timeout: omit both so the server uses
        // its default (~10s wait + auto-close on success).
        let (wait, on_wait) = resolve_wait_timeout(None, None);
        assert_eq!(wait, None);
        assert_eq!(on_wait, None);
    }

    #[test]
    fn build_execute_params_threads_row_limit_to_wire() {
        // Headline parity feature: spec.row_limit must reach
        // ExecuteParams.row_limit. A regression dropping it would otherwise
        // pass every other test (the seam this pins).
        let mut spec = StatementSpec::new();
        spec.sql("SELECT 1").row_limit(Some(100));
        let (sql, params) = build_execute_params(&spec, None).unwrap();
        assert_eq!(sql, "SELECT 1");
        assert_eq!(params.row_limit, Some(100));
    }

    #[test]
    fn build_execute_params_binds_positional_params_one_based() {
        let mut spec = StatementSpec::new();
        spec.sql("SELECT ?, ?");
        spec.param(1, TypedValue::Bool(true)).unwrap();
        spec.param(2, TypedValue::Bool(false)).unwrap();
        let (_, params) = build_execute_params(&spec, None).unwrap();
        assert_eq!(params.parameters.len(), 2);
        // Positional binding is 1-based on the wire and carries no name.
        assert_eq!(params.parameters[0].ordinal, Some(1));
        assert_eq!(params.parameters[1].ordinal, Some(2));
        assert!(params.parameters[0].name.is_none());
    }

    #[test]
    fn build_execute_params_binds_named_params() {
        let mut spec = StatementSpec::new();
        spec.sql("SELECT :flag");
        spec.param_named("flag", TypedValue::Bool(true)).unwrap();
        let (_, params) = build_execute_params(&spec, None).unwrap();
        assert_eq!(params.parameters.len(), 1);
        assert_eq!(params.parameters[0].name.as_deref(), Some("flag"));
        assert!(params.parameters[0].ordinal.is_none());
    }

    #[test]
    fn build_execute_params_requires_sql() {
        // Malformed/incomplete spec fails before any wire dispatch.
        let spec = StatementSpec::new();
        assert!(build_execute_params(&spec, None).is_err());
    }

    #[test]
    fn build_execute_params_rejects_mixed_positional_and_named() {
        // Positional + named bindings are mutually exclusive — the
        // kernel rejects at bind time so every binding (napi, pyo3,
        // direct Rust) gets InvalidArgument before the wire instead of
        // an opaque server `BAD_REQUEST` / `MISSING_NAME`.
        let mut spec = StatementSpec::new();
        spec.sql("SELECT ? AS pos, :n AS named");
        spec.param(1, TypedValue::Int64(7)).unwrap();
        spec.param_named("n", TypedValue::Int64(99)).unwrap();
        let err = build_execute_params(&spec, None)
            .expect_err("mixed positional + named must be rejected");
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(
            err.message.contains("mutually exclusive"),
            "expected 'mutually exclusive' in error, got: {}",
            err.message
        );
    }
}
