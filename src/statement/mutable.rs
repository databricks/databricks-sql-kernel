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

#![allow(dead_code, unused_variables)]

use crate::kernel_error::Result;
use crate::session::SessionInner;
use crate::statement::executed::ExecutedStatement;
use crate::statement::executed_async::ExecutedAsyncStatement;
use crate::statement::spec::StatementSpec;
use crate::statement::validity::ValidityFlag;
use std::sync::Arc;

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

    /// Wait-for-result execution. Sends ExecuteStatement with
    /// `wait_timeout=10s`; the kernel polls internally if the server
    /// does not finish in 10s. Returns when results are ready.
    ///
    /// If a prior executed handle is still tracked by this `Statement`,
    /// it is auto-cancelled and closed before issuing the new
    /// ExecuteStatement.
    pub async fn execute(&mut self) -> Result<ExecutedStatement> {
        unimplemented!("PR 5: implement execute()")
    }

    /// Submit-and-return. `wait_timeout=0s`; no direct results. Same
    /// auto-cancel-and-close semantics as `execute()`.
    pub async fn submit(&mut self) -> Result<ExecutedAsyncStatement> {
        unimplemented!("PR 5: implement submit()")
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        // (1) Flip the validity flag so any executed handle the caller
        // still holds short-circuits to InvalidStatementHandle on its
        // next call without a network round-trip. Server-side cleanup
        // for handles that *are* still live is the executed handle's
        // own Drop responsibility (it owns the Arc<SessionInner> too,
        // so its Drop runs after this when the handle outlives the
        // Statement).
        if let Some(valid) = self.last_valid.take() {
            valid.invalidate();
        }
        // (2) Orphan-id safety net: if a `last_statement_id` was
        // recorded but no executed handle is tracking it (the
        // in-flight `execute()` future was dropped after the server
        // assigned an id but before the handle was constructed and
        // returned), fire-and-forget Cancel+Close so the server
        // doesn't keep charging DBUs. Idempotent CloseStatement makes
        // double-close from another path safe.
        //
        // PR 5 implementation note: distinguishing "handle returned
        // and still live" from "handle never returned" needs a flag
        // beyond what's in this struct today (the in-flight `execute`
        // body sets `last_statement_id` on the manifest before
        // wrapping into the handle). Track a `handle_returned: bool`
        // on `Statement`, set it when execute/submit returns the
        // handle, and check `last_statement_id.is_some() &&
        // !handle_returned` here. PR 5 plumbs the transport through
        // and adds the field; the orphan-cleanup body lives here.
        let _orphan_id = self.last_statement_id.take();
    }
}
