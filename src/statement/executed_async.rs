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

use crate::client::{ExecuteResponse, ExecutionTimeout, SessionAttribution};
use crate::kernel_error::{Error, Result};
use crate::klog::warn;
use crate::result_stream::ResultStream;
use crate::session::SessionInner;
use crate::statement::handle::ExecutedStatementHandle;
use crate::statement::validity::ValidityFlag;
use crate::telemetry::StatementTelemetry;
use crate::types::sea::StatementState;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::Instrument;

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
                // Server-reported failure → SqlError. See the sync path
                // (`SeaClient::terminal_outcome`) for why the `error_code`
                // slug is carried but not used to pick the bucket: SEA's
                // `BAD_REQUEST` is overloaded across genuine SQL errors.
                let mut error = Error::sql_error()
                    .message(&error_msg)
                    .with_query_id(&response.statement_id);
                if let Some(s) = crate::kernel_error::extract_sqlstate_from_status(&response.status)
                {
                    error = error.with_sql_state(s);
                }
                if let Some(err) = service_error {
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
    volume: Option<Arc<crate::volume::VolumeState>>,
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
    /// Server auto-close can retain results, so owner release has a separate flag.
    owner_released: Arc<AtomicBool>,
    /// Whether this handle *owns* the server statement's lifecycle.
    ///
    /// `true` for handles produced by [`Statement::submit`] (the
    /// submitting handle): they may close the statement on `Drop` /
    /// `close()`, cancel it on a dropped `await_result()` poll (the
    /// drop-cancel DBU-safety guard), and `cancel()` actively
    /// terminates it.
    ///
    /// `false` for handles produced by
    /// [`Session::attach_async_statement`](crate::Session::attach_async_statement)
    /// — a *read-only adopt* of a statement someone else owns. A
    /// non-owning handle must never tear the statement down out from
    /// under its owner: it does not arm the `await_result` drop-cancel
    /// guard, and `cancel()` returns an error instead of firing a
    /// server-side cancel. (`Drop` / `close()` are already inert on
    /// attached handles via `closed=true`.)
    owning: bool,
    session_attribution: SessionAttribution,
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
    /// Take-once ownership avoids retaining raw payloads after reader construction.
    pending_result: std::sync::Mutex<Option<ExecuteResponse>>,
    statement_telemetry: StatementTelemetry,
    /// Set once result materialisation hands success finalization to
    /// the returned `ResultStream`.
    result_stream_owns_success_telemetry: AtomicBool,
    /// Absolute execution deadline retained across caller-driven polls.
    client_deadline: Option<std::time::Instant>,
    /// When `true`, the caller explicitly disabled the client timeout
    /// (`ExecutionTimeout::Unlimited`), so the poll loop must NOT fall
    /// back to the transport's default `poll_timeout` ceiling. Mirrors
    /// the sync `wait_for_terminal` gating, which only applies that
    /// ceiling under `ExecutionTimeout::Default`. Without this, an
    /// "unlimited" submit still dies at the 600s default poll_timeout.
    poll_timeout_disabled: bool,
    client_timeout_expired: AtomicBool,
    /// Latched once this handle observes a terminal server state
    /// (`Succeeded` / `Failed` / `Cancelled` / `Closed`), including
    /// the initial-submit snapshot. The client deadline governs only
    /// the pre-terminal window ("initial submit through observed
    /// terminal status"): once a terminal state has been seen, a
    /// subsequent `status()` must return that state regardless of the
    /// deadline, instead of masking it with a `Timeout`/`HYT00` and
    /// firing a wasted cancel+close against an already-finished
    /// statement.
    terminal_observed: AtomicBool,
}

/// Map an [`ExecutionTimeout`] onto the async handle's two deadline
/// knobs: `(client_deadline, poll_timeout_disabled)`.
///
/// Mirrors the sync `wait_for_terminal` gating so the two consumption
/// modes agree on what "unlimited" means:
/// - `Deadline` → retain an absolute client deadline AND bypass the
///   `poll_timeout` ceiling: the client deadline *is* the ceiling. The
///   sync path only applies `poll_timeout` under `Default`, so a
///   `Deadline` runs to the full client deadline even when it exceeds
///   the 600s default `poll_timeout` (`MAX_CLIENT_QUERY_TIMEOUT_MS`
///   permits deadlines far beyond 600s). Leaving the ceiling armed here
///   would abort such a query at 600s with the generic internal
///   poll-timeout error instead of the caller's deadline firing with
///   the `Timeout`/`HYT00` client-query-timeout error.
/// - `Unlimited` → no client deadline AND bypass the `poll_timeout`
///   ceiling, so an explicitly-unlimited submit does not silently die
///   at the 600s default.
/// - `Default` → no client deadline, keep the `poll_timeout` ceiling.
fn deadline_policy(timeout: ExecutionTimeout) -> (Option<std::time::Instant>, bool) {
    match timeout {
        ExecutionTimeout::Deadline(deadline) => (Some(deadline), true),
        ExecutionTimeout::Unlimited => (None, true),
        ExecutionTimeout::Default => (None, false),
    }
}

impl ExecutedAsyncStatement {
    fn cancel_volume(&self) {
        if let Some(volume) = &self.volume {
            volume.cancel();
        }
    }

    /// Produce the stable client query-timeout error and schedule bounded,
    /// best-effort server cleanup when a deadline retained across
    /// caller-driven status polls expires.
    pub(crate) fn expire_client_query_timeout(&self) -> Error {
        self.cancel_volume();
        self.owner_released.store(true, Ordering::Release);
        self.clear_pending_result();
        if !self.client_timeout_expired.swap(true, Ordering::AcqRel) {
            self.session
                .dispatch_timeout_cleanup(Some(self.statement_id.clone()));
        }
        let error = crate::statement::timeout::client_query_timeout_error(Some(&self.statement_id));
        self.statement_telemetry.finalize(Some(&error));
        error
    }

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
    ///
    /// `timeout` selects the handle's deadline policy, mirroring the
    /// sync `wait_for_terminal` gating: `Deadline` retains an absolute
    /// client deadline across polls; `Unlimited` disables BOTH that
    /// deadline and the transport's default `poll_timeout` ceiling;
    /// `Default` keeps the `poll_timeout` ceiling with no client
    /// deadline.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session: Arc<SessionInner>,
        statement_id: String,
        valid: ValidityFlag,
        closed: Arc<AtomicBool>,
        initial_status: crate::types::sea::StatementStatus,
        statement_telemetry: StatementTelemetry,
        timeout: ExecutionTimeout,
        volume: Arc<crate::volume::VolumeState>,
    ) -> Self {
        let (client_deadline, poll_timeout_disabled) = deadline_policy(timeout);
        let initial_terminal = initial_status.state.is_terminal();
        Self {
            session,
            statement_id,
            valid,
            closed,
            owner_released: Arc::new(AtomicBool::new(false)),
            volume: Some(volume),
            // The submitting handle owns the statement lifecycle.
            owning: true,
            session_attribution: SessionAttribution::Current,
            latest_status: Arc::new(std::sync::Mutex::new(initial_status)),
            pending_result: std::sync::Mutex::new(None),
            statement_telemetry,
            result_stream_owns_success_telemetry: AtomicBool::new(false),
            client_deadline,
            poll_timeout_disabled,
            client_timeout_expired: AtomicBool::new(false),
            terminal_observed: AtomicBool::new(initial_terminal),
        }
    }

    /// Crate-internal constructor for a **non-owning** (read-only
    /// adopt) handle, built by
    /// [`Session::attach_async_statement`](crate::Session::attach_async_statement).
    ///
    /// The handle observes a statement someone else owns, so it must
    /// never tear it down:
    /// - `valid` is a fresh standalone [`ValidityFlag`] — no parent
    ///   `Statement` to share with, so a re-execute elsewhere can't
    ///   invalidate it.
    /// - `closed` starts `true` so `Drop` and `close()` are inert (they
    ///   never fire `close_statement`).
    /// - `owning` is `false` so `await_result()` does not arm the
    ///   drop-cancel guard and `cancel()` errors instead of firing a
    ///   server-side cancel.
    ///
    /// `initial_status` is seeded from the `GetStatementStatus` the
    /// attach path already issued.
    pub(crate) fn new_attached(
        session: Arc<SessionInner>,
        statement_id: String,
        initial_status: crate::types::sea::StatementStatus,
    ) -> Self {
        Self::new_read_only(
            session,
            statement_id,
            ValidityFlag::new(),
            Arc::new(AtomicBool::new(false)),
            SessionAttribution::Detached,
            initial_status,
        )
    }

    fn new_read_only(
        session: Arc<SessionInner>,
        statement_id: String,
        valid: ValidityFlag,
        owner_released: Arc<AtomicBool>,
        session_attribution: SessionAttribution,
        initial_status: crate::types::sea::StatementStatus,
    ) -> Self {
        let initial_terminal = initial_status.state.is_terminal();
        Self {
            session,
            statement_id,
            valid,
            closed: Arc::new(AtomicBool::new(true)),
            owner_released,
            volume: None,
            owning: false,
            session_attribution,
            latest_status: Arc::new(std::sync::Mutex::new(initial_status)),
            pending_result: std::sync::Mutex::new(None),
            statement_telemetry: StatementTelemetry::noop(),
            result_stream_owns_success_telemetry: AtomicBool::new(false),
            client_deadline: None,
            poll_timeout_disabled: false,
            client_timeout_expired: AtomicBool::new(false),
            terminal_observed: AtomicBool::new(initial_terminal),
        }
    }

    /// Create an independent [`AsyncStatementReader`] without a server request.
    pub fn reader(&self) -> Result<AsyncStatementReader> {
        self.valid.check()?;
        self.check_owner_available()?;
        let status = self
            .latest_status
            .lock()
            .map_err(|error| Error::internal().message(format!("Statement status lock: {error}")))?
            .clone();
        let mut inner = Self::new_read_only(
            Arc::clone(&self.session),
            self.statement_id.clone(),
            self.valid.clone(),
            Arc::clone(&self.owner_released),
            self.session_attribution,
            status,
        );
        inner.volume = self.volume.clone();
        Ok(AsyncStatementReader { inner })
    }

    fn check_owner_available(&self) -> Result<()> {
        if self.owner_released.load(Ordering::Acquire) {
            return Err(Error::invalid_statement_handle()
                .message("the owning asynchronous statement has been closed or dropped")
                .with_query_id(&self.statement_id));
        }
        Ok(())
    }

    fn check_read_validity(&self) -> Result<()> {
        self.valid.check()?;
        if !self.owning {
            self.check_owner_available()?;
        }
        Ok(())
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

    /// Returns [`ErrorCode::InvalidStatementHandle`](crate::ErrorCode::InvalidStatementHandle)
    /// after parent-statement invalidation. Non-owning handles also fail after
    /// their owner is released.
    pub fn try_num_modified_rows(&self) -> Result<Option<i64>> {
        self.check_read_validity()?;
        let count = self.num_modified_rows();
        self.check_read_validity()?;
        Ok(count)
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

    /// Publish observed metadata together with an optional response for reuse.
    /// Lock poisoning is intentionally swallowed: a poisoned lock here
    /// would only happen if a panic
    /// landed mid-write, and the recovery path is to keep the prior
    /// status visible (worse: a fresh poll's status lost; better:
    /// the handle's accessor surface keeps working).
    fn record_latest_status(
        &self,
        fresh: crate::types::sea::StatementStatus,
        response: Option<ExecuteResponse>,
    ) {
        // Payload before metadata keeps concurrent publications from mixing observations.
        let pending = self.pending_result.lock().ok();
        if let Ok(mut guard) = self.latest_status.lock() {
            *guard = fresh;
        }
        if let Some(mut pending) = pending {
            *pending = response.filter(|response| {
                matches!(
                    response.status.state,
                    StatementState::Succeeded | StatementState::Closed
                ) && !self.owner_released.load(Ordering::Acquire)
            });
        }
    }

    fn clear_pending_result(&self) {
        if let Ok(mut pending) = self.pending_result.lock() {
            *pending = None;
        }
    }

    fn take_pending_result(&self) -> Option<ExecuteResponse> {
        let mut pending = self.pending_result.lock().ok()?;
        if self.owner_released.load(Ordering::Acquire) {
            *pending = None;
            return None;
        }
        pending.take()
    }

    fn cancelled_error(&self, status: &crate::types::sea::StatementStatus) -> Error {
        let error = Error::cancelled()
            .message(format!("Statement {} was cancelled", self.statement_id))
            .with_query_id(&self.statement_id);
        crate::kernel_error::carry_extended_status(error, status)
    }

    /// Synthesize the terminal cancel `Error` from this handle's most
    /// recently observed [`crate::types::sea::StatementStatus`] (the
    /// cache that `status()` / `await_result()` refresh after every
    /// poll). This is the same structured error the internal
    /// `cancelled_error` / `await_result` telemetry path builds, so
    /// out-of-crate drivers (the C ABI) that only see the unit
    /// `StatementStatus::Cancelled` variant can recover it without
    /// re-implementing the extended-status carry-over inline.
    ///
    /// Callers should invoke this immediately after a `status()` /
    /// `await_result()` poll returned `Cancelled`, so the cached
    /// status reflects that poll. If the cache lock is poisoned the
    /// extended fields are simply omitted (best-effort), matching the
    /// swallow-on-poison policy of the accessors.
    pub(crate) fn cancelled_error_from_cache(&self) -> Error {
        let error = Error::cancelled()
            .message(format!("Statement {} was cancelled", self.statement_id))
            .with_query_id(&self.statement_id);
        match self.latest_status.lock() {
            Ok(guard) => crate::kernel_error::carry_extended_status(error, &guard),
            Err(_) => error,
        }
    }

    /// One-shot status check. Non-blocking. Issues a single
    /// GetStatementStatus RPC. Returns `Err(InvalidStatementHandle)`
    /// locally — without hitting the wire — if the parent `Statement`
    /// has already invalidated this handle.
    ///
    /// A successful terminal check keeps one complete response, including any
    /// inline rows, on this handle for its next result read. Status-only callers
    /// should close or drop owning handles when finished, and drop independent
    /// readers or attached handles to release their saved payload.
    pub async fn status(&self) -> Result<StatementStatus> {
        let span = self.session.log_span().clone();
        let result = async {
            self.check_read_validity()?;
            self.clear_pending_result();
            // The client deadline governs only the pre-terminal window
            // ("initial submit through observed terminal status"). Once
            // this handle has observed a terminal state, `status()`
            // re-polls and returns it regardless of the deadline, rather
            // than masking the terminal outcome with a `Timeout`/`HYT00`
            // and firing a wasted cancel+close against an already-
            // finished statement.
            let enforce_deadline = !self.terminal_observed.load(Ordering::Acquire);
            if enforce_deadline
                && (self.client_timeout_expired.load(Ordering::Acquire)
                    || self
                        .client_deadline
                        .is_some_and(|deadline| std::time::Instant::now() >= deadline))
            {
                return Err(self.expire_client_query_timeout());
            }
            let status_started = std::time::Instant::now();
            let poll = self
                .session
                .transport()
                .statement_status(&self.statement_id, self.session_attribution);
            let response = match self.client_deadline {
                Some(deadline) if enforce_deadline => {
                    let poll_deadline = deadline
                        .checked_add(crate::statement::timeout::CLIENT_QUERY_TIMEOUT_STATUS_GRACE)
                        .unwrap_or(deadline);
                    match tokio::time::timeout_at(
                        tokio::time::Instant::from_std(poll_deadline),
                        poll,
                    )
                    .await
                    {
                        Ok(result) => result?,
                        Err(_) => return Err(self.expire_client_query_timeout()),
                    }
                }
                _ => poll.await?,
            };
            self.check_read_validity()?;
            self.statement_telemetry
                .record_poll(status_started.elapsed());
            // Latch the terminal observation so subsequent `status()`
            // calls stop enforcing the client deadline against this
            // already-finished statement.
            if response.status.state.is_terminal() {
                self.terminal_observed.store(true, Ordering::Release);
            }
            let status = StatementStatus::from_sea_response(&response);
            if enforce_deadline
                && matches!(status, StatementStatus::Pending | StatementStatus::Running)
                && self
                    .client_deadline
                    .is_some_and(|deadline| std::time::Instant::now() >= deadline)
            {
                self.record_latest_status(response.status.clone(), None);
                return Err(self.expire_client_query_timeout());
            }
            match &status {
                StatementStatus::Failed(error) => {
                    self.statement_telemetry.finalize(Some(error));
                }
                StatementStatus::Cancelled => {
                    let error = self.cancelled_error(&response.status);
                    self.statement_telemetry.finalize(Some(&error));
                }
                StatementStatus::Pending
                | StatementStatus::Running
                | StatementStatus::Succeeded
                | StatementStatus::Closed => {}
            }
            self.record_latest_status(response.status.clone(), Some(response));
            Ok(status)
        }
        .instrument(span)
        .await;
        if let Err(error) = &result {
            self.clear_pending_result();
            self.statement_telemetry.finalize(Some(error));
        }
        result
    }

    /// Block until the server reaches a terminal state, then return
    /// the result stream. Internally polls using the session's
    /// [`crate::client::TransportConfig`] cadence and ceiling.
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
    /// terminal state, returns `Error::timeout()`. For owning handles this also
    /// drops the armed cancel guard, which sends a best-effort
    /// server-side cancel; attached read-only handles do not cancel.
    pub async fn await_result(&self) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let result = async {
            self.check_read_validity()?;
            if let Some(response) = self.take_pending_result() {
                return self.materialise_result(response).await;
            }

            let cfg = self.session.transport().poll_config();
            let start = std::time::Instant::now();

            // RAII guard: if the future is dropped mid-poll (timeout,
            // tokio::select! loser, task cancellation), spawn a
            // best-effort `cancel_statement` so the server stops
            // charging DBUs on a now-orphaned query. Defused once we
            // reach a terminal state on the success or error path.
            //
            // ONLY for owning handles. A non-owning (attached) handle is
            // a read-only adopt of a statement someone else owns —
            // dropping its `await_result()` mid-poll (e.g. a passive
            // observer wrapping it in a `tokio::time::timeout`) must NOT
            // cancel the owner's statement. Leaving the guard `None`
            // makes the drop a true no-op.
            let mut cancel_guard = if self.owning {
                Some(AwaitResultCancelGuard::new(
                    Arc::clone(&self.session),
                    self.statement_id.clone(),
                ))
            } else {
                None
            };

            loop {
                self.check_read_validity()?;
                // The client deadline governs only the pre-terminal
                // window. Once this handle has observed a terminal state
                // (here or via a prior `status()` / initial-submit
                // snapshot), stop enforcing the deadline so we proceed to
                // materialise the finished result instead of masking it
                // with a `Timeout`/`HYT00` and firing a wasted
                // cancel+close against an already-finished statement —
                // mirrors the `enforce_deadline` gating in `status()`.
                let enforce_deadline = !self.terminal_observed.load(Ordering::Acquire);
                if enforce_deadline
                    && (self.client_timeout_expired.load(Ordering::Acquire)
                        || self
                            .client_deadline
                            .is_some_and(|deadline| std::time::Instant::now() >= deadline))
                {
                    if let Some(g) = cancel_guard.take() {
                        g.defuse();
                    }
                    return Err(self.expire_client_query_timeout());
                }
                let status_started = std::time::Instant::now();
                let poll = self
                    .session
                    .transport()
                    .statement_status(&self.statement_id, self.session_attribution);
                let response = match self.client_deadline {
                    Some(deadline) if enforce_deadline => {
                        let poll_deadline = deadline
                            .checked_add(
                                crate::statement::timeout::CLIENT_QUERY_TIMEOUT_STATUS_GRACE,
                            )
                            .unwrap_or(deadline);
                        match tokio::time::timeout_at(
                            tokio::time::Instant::from_std(poll_deadline),
                            poll,
                        )
                        .await
                        {
                            Ok(result) => result?,
                            Err(_) => {
                                if let Some(g) = cancel_guard.take() {
                                    g.defuse();
                                }
                                return Err(self.expire_client_query_timeout());
                            }
                        }
                    }
                    _ => poll.await?,
                };
                self.check_read_validity()?;
                self.statement_telemetry
                    .record_poll(status_started.elapsed());
                // Cache the wire status so the extended-field
                // accessors reflect THIS poll's observation — not the
                // initial-submit snapshot that's now stale by minutes.
                self.record_latest_status(response.status.clone(), None);
                // Latch the terminal observation so a later `status()`
                // call stops enforcing the client deadline against this
                // already-finished statement.
                if response.status.state.is_terminal() {
                    self.terminal_observed.store(true, Ordering::Release);
                }
                match StatementStatus::from_sea_response(&response) {
                    StatementStatus::Succeeded | StatementStatus::Closed => {
                        if let Some(g) = cancel_guard.take() {
                            g.defuse();
                        }
                        return self.materialise_result(response).await;
                    }
                    StatementStatus::Failed(e) => {
                        // Server already decided this is terminal — no
                        // need to fire a redundant cancel.
                        if let Some(g) = cancel_guard.take() {
                            g.defuse();
                        }
                        return Err(e);
                    }
                    StatementStatus::Cancelled => {
                        if let Some(g) = cancel_guard.take() {
                            g.defuse();
                        }
                        // Carry the SEA extended status fields onto
                        // the returned Error — mirrors the sync
                        // `terminal_outcome::Canceled` arm so the
                        // Cancelled-state envelope contract is
                        // identical across kernel handles. `cancelled()`
                        // (not invalid_state) so the connector maps it to
                        // OperationalError, not ProgrammingError — see the
                        // sync arm in client/sea.rs for the rationale.
                        let error = self.cancelled_error(&response.status);
                        return Err(error);
                    }
                    StatementStatus::Pending | StatementStatus::Running => {
                        if enforce_deadline
                            && self
                                .client_deadline
                                .is_some_and(|deadline| std::time::Instant::now() >= deadline)
                        {
                            if let Some(g) = cancel_guard.take() {
                                g.defuse();
                            }
                            return Err(self.expire_client_query_timeout());
                        }
                        if !self.poll_timeout_disabled && start.elapsed() > cfg.poll_timeout {
                            // Internal poll-timeout: we asked to wait,
                            // we gave up. Leave the cancel guard armed
                            // so the server stops charging DBUs on the
                            // now-orphaned query when we return — same
                            // policy as a caller-initiated future drop.
                            return Err(Error::timeout()
                                .message(format!(
                                    "Statement {} did not reach a terminal state within {:?}",
                                    self.statement_id, cfg.poll_timeout
                                ))
                                .with_query_id(&self.statement_id));
                        }
                        tokio::time::sleep(cfg.poll_interval).await;
                    }
                }
            }
        }
        .instrument(span)
        .await;
        if let Err(error) = &result {
            self.clear_pending_result();
            self.statement_telemetry.finalize(Some(error));
        }
        result
    }

    /// Fetch the result without polling. Errors if the statement has
    /// not finished — the precise error code is tracked under the
    /// pending `ErrorCode` taxonomy expansion (see spec addendum A8).
    /// Until then, returns `ErrorCode::InvalidState` with a clear
    /// message pointing callers at [`status`](Self::status). Useful
    /// when the caller has already confirmed completion themselves.
    pub async fn fetch_result(&self) -> Result<ResultStream> {
        let span = self.session.log_span().clone();
        let result = async {
            self.check_read_validity()?;
            let response = match self.take_pending_result() {
                Some(response) => response,
                None => {
                    self.clear_pending_result();
                    let response = self
                        .session
                        .transport()
                        .statement_status(&self.statement_id, self.session_attribution)
                        .await?;
                    self.check_read_validity()?;
                    self.clear_pending_result();
                    response
                }
            };
            self.materialise_result(response).await
        }
        .instrument(span)
        .await;
        if let Err(error) = &result {
            self.clear_pending_result();
            self.statement_telemetry.finalize(Some(error));
        }
        result
    }

    /// Internal: resolve the reader for an (expected to be) terminal
    /// statement. Returns `InvalidState` if the server reports the
    /// statement is still running. The error includes the observed
    /// status so callers don't have to re-query, and points at the
    /// actionable follow-up.
    async fn materialise_result(&self, response: ExecuteResponse) -> Result<ResultStream> {
        self.check_read_validity()?;
        let observed = match response.status.state {
            StatementState::Pending => "Pending",
            StatementState::Running => "Running",
            _ => "non-terminal",
        };
        let result = self
            .session
            .transport()
            .completed_result(response, self.session_attribution)
            .await?;
        self.check_read_validity()?;
        match result {
            Some(result) => {
                self.statement_telemetry.on_result_metadata(
                    result.telemetry_result_format,
                    result.telemetry_is_compressed,
                );
                // If the server already auto-closed the statement
                // (`state == Closed`), pre-set the shared `closed` flag so
                // this handle's `close()` / `Drop` — and `invalidate_prior`
                // on the parent `Statement`'s next execute — skip the
                // redundant `CloseStatement` DELETE (the server returns 404
                // STATEMENT_NOT_FOUND on it). Mirrors the sync `execute()`
                // path's `already_closed` short-circuit (issue #168). This
                // covers both `await_result()` and `fetch_result()`, which
                // funnel through here. `Succeeded` leaves the statement
                // server-side, so the flag stays as-is and the close fires.
                if result.status.state == crate::types::sea::StatementState::Closed {
                    self.closed.store(true, Ordering::Release);
                }
                let marked = result
                    .manifest
                    .as_ref()
                    .is_some_and(|m| m.is_volume_operation);
                let mut stream = ResultStream::from_reader(result.reader).await?;
                if let Some(volume) = &self.volume {
                    stream.attach_volume(marked, Arc::clone(volume));
                }
                stream.attach_telemetry(self.statement_telemetry.clone());
                // SEA carries no row count on the async status either; for a
                // DML the count rides in the result set. Derive it once
                // (non-consuming) and record it onto the latest status so
                // `num_modified_rows()` matches the sync path / Thrift.
                if self.num_modified_rows().is_none() {
                    if let Some(n) =
                        crate::statement::dml_count::read_dml_affected_rows(&mut stream).await?
                    {
                        if let Ok(mut guard) = self.latest_status.lock() {
                            guard.num_modified_rows = Some(n);
                        }
                    }
                }
                self.check_read_validity()?;
                self.result_stream_owns_success_telemetry
                    .store(true, Ordering::Release);
                Ok(stream)
            }
            None => Err(Error::invalid_state().message(format!(
                "Statement {} is not terminal yet (status: {}); call await_result() to \
                     block until done, or poll status() and re-call fetch_result() once it \
                     reports Succeeded / Closed",
                self.statement_id, observed
            ))),
        }
    }
}

/// Independent read access to an asynchronous statement, created by
/// [`ExecutedAsyncStatement::reader`].
///
/// Session attribution is inherited from the source handle, including unknown
/// attribution for ID-only attachments. The reader shares parent-statement
/// invalidation but has independent status and result state. It does not inherit
/// the execution deadline or emit execution telemetry. Dropping a reader never
/// cancels or closes the statement; the owner controls its server lifetime.
/// Derived readers become invalid when the owner closes, drops, or starts timeout cleanup.
pub struct AsyncStatementReader {
    inner: ExecutedAsyncStatement,
}

impl AsyncStatementReader {
    pub fn statement_id(&self) -> &str {
        &self.inner.statement_id
    }

    /// Returns [`ErrorCode::InvalidStatementHandle`](crate::ErrorCode::InvalidStatementHandle)
    /// after parent-statement invalidation or owner release.
    pub fn num_modified_rows(&self) -> Result<Option<i64>> {
        self.inner.try_num_modified_rows()
    }

    /// Read the current server status. See [`ExecutedAsyncStatement::status`].
    pub async fn status(&self) -> Result<StatementStatus> {
        self.inner.status().await
    }

    /// Poll until terminal and return a fresh result stream.
    ///
    /// The session's polling timeout applies to this read. Timing out or
    /// dropping the future leaves the server statement running.
    pub async fn await_result(&self) -> Result<ResultStream> {
        self.inner.await_result().await
    }

    /// Return a fresh result stream, or `InvalidState` if execution is not terminal.
    pub async fn fetch_result(&self) -> Result<ResultStream> {
        self.inner.fetch_result().await
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
            // A non-owning (attached) handle must not cancel a statement
            // it merely adopted to observe — that would terminate the
            // owner's query. Refuse rather than silently firing the
            // server-side cancel.
            if !self.owning {
                return Err(Error::invalid_state().message(
                    "cancel() is not permitted on an attached (read-only) \
                     ExecutedAsyncStatement; the statement is owned by whoever \
                     submitted it",
                ));
            }
            self.cancel_volume();
            self.session
                .transport()
                .cancel_statement(&self.statement_id)
                .await
        }
        .instrument(self.session.log_span().clone())
        .await
    }

    async fn close(&self) -> Result<()> {
        self.close_server_statement().await
    }
}

impl ExecutedAsyncStatement {
    /// Explicit close for concrete async statements.
    ///
    /// Once [`await_result`](Self::await_result) / [`fetch_result`](Self::fetch_result)
    /// returns a [`ResultStream`], that stream owns success finalization so full
    /// result consumption can include result metrics. Explicitly closing the owning
    /// async statement is the binding-visible lifecycle boundary, though, so finalize
    /// here as a deterministic backstop for partial-fetch callers that keep the stream
    /// alive past session close.
    pub async fn close(&mut self) -> Result<()> {
        self.statement_telemetry.finalize(None);
        self.close_server_statement().await
    }

    async fn close_server_statement(&self) -> Result<()> {
        if self.owning {
            self.cancel_volume();
            self.owner_released.store(true, Ordering::Release);
            self.clear_pending_result();
        }
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
        // that change.
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
            // Inherit ownership: a canceller derived from an attached
            // (read-only) handle must not cancel the owner's statement
            // either — otherwise the footgun just moves to the detached
            // canceller.
            owning: self.owning,
            volume: self.volume.clone(),
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
    volume: Option<Arc<crate::volume::VolumeState>>,
    session: Arc<SessionInner>,
    statement_id: String,
    valid: ValidityFlag,
    /// Inherited from the producing handle — `false` for a canceller
    /// derived from an attached (read-only) handle, which must not
    /// cancel a statement it doesn't own. See
    /// [`ExecutedAsyncStatement::owning`].
    owning: bool,
}

impl AsyncStatementCanceller {
    /// Cancel local Volume completion without waiting for a server request.
    pub fn cancel_volume_operation(&self) {
        if self.owning {
            if let Some(volume) = &self.volume {
                volume.cancel();
            }
        }
    }

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
            if !self.owning {
                return Err(Error::invalid_state().message(
                    "cancel() is not permitted on a canceller derived from an \
                     attached (read-only) ExecutedAsyncStatement; the statement \
                     is owned by whoever submitted it",
                ));
            }
            self.cancel_volume_operation();
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
        if self.owning {
            self.cancel_volume();
            self.owner_released.store(true, Ordering::Release);
            self.clear_pending_result();
        }
        // Ensure submit-and-drop / status-only async statements still
        // emit one terminal telemetry event. Once a result stream has
        // been returned, that stream owns success finalization so it can
        // include result/chunk metrics even if this handle is dropped
        // before the stream is consumed.
        if !self
            .result_stream_owns_success_telemetry
            .load(Ordering::Acquire)
        {
            self.statement_telemetry.finalize(None);
        }

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

    #[test]
    fn deadline_policy_mirrors_sync_timeout_gating() {
        // Default: no client deadline, poll_timeout ceiling stays on.
        assert_eq!(
            deadline_policy(ExecutionTimeout::Default),
            (None, false),
            "Default must keep the transport poll_timeout ceiling"
        );
        // Unlimited: no client deadline AND the poll_timeout ceiling is
        // bypassed — regression guard for an explicitly-unlimited submit
        // silently dying at the 600s default poll_timeout.
        assert_eq!(
            deadline_policy(ExecutionTimeout::Unlimited),
            (None, true),
            "Unlimited must disable the poll_timeout ceiling"
        );
        // Deadline: retains the absolute client deadline AND bypasses the
        // poll_timeout ceiling — the client deadline *is* the ceiling.
        // Regression guard: MAX_CLIENT_QUERY_TIMEOUT_MS permits deadlines
        // far beyond the 600s default poll_timeout, so leaving the ceiling
        // armed would abort such a query at 600s with the generic internal
        // poll-timeout error instead of the caller's deadline firing. The
        // sync wait_for_terminal only applies poll_timeout under Default.
        let deadline = std::time::Instant::now();
        assert_eq!(
            deadline_policy(ExecutionTimeout::Deadline(deadline)),
            (Some(deadline), true),
            "Deadline must retain the client deadline AND disable the poll_timeout ceiling"
        );
    }
}
