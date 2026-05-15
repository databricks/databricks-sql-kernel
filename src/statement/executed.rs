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
use crate::result_stream::{ResultStream, SchemaRef};
use crate::session::SessionInner;
use crate::statement::handle::ExecutedStatementHandle;
use crate::statement::validity::ValidityFlag;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{warn, Instrument};

/// Sync-consumption executed handle.
pub struct ExecutedStatement {
    session: Arc<SessionInner>,
    statement_id: String,
    /// Shared with the parent `Statement`; flipped on re-execute or
    /// when the parent `Statement` is dropped. Reads short-circuit
    /// stale calls locally with no network round-trip.
    valid: ValidityFlag,
    result_stream: ResultStream,
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
    ) -> Self {
        Self {
            session,
            statement_id,
            valid,
            result_stream,
            closed,
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
        // Idempotent: the first call flips the flag; subsequent
        // calls return Ok(()) without hitting the wire. The
        // server-side close is idempotent too, but skipping the
        // round-trip is cheaper and avoids ambiguous "double-close"
        // log noise.
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

impl Drop for ExecutedStatement {
    fn drop(&mut self) {
        // If close() already ran, nothing to do.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        // Fire-and-forget CloseStatement. Drop must not panic and
        // we can't surface a failure here; the runtime keeps the
        // background task alive until completion. Swallows errors
        // by design.
        let transport = Arc::clone(self.session.transport());
        let runtime = self.session.runtime_handle().clone();
        let stmt_id = self.statement_id.clone();
        runtime.spawn(async move {
            if let Err(e) = transport.close_statement(&stmt_id).await {
                warn!(
                    statement_id = %stmt_id,
                    error = %e,
                    "fire-and-forget close_statement on Drop failed; \
                     server-side resource reclaimed when the session closes"
                );
            }
        });
    }
}
