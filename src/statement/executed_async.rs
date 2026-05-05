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

#![allow(dead_code, unused_variables)]

use crate::kernel_error::Error;
use crate::kernel_error::Result;
use crate::result_stream::ResultStream;
use crate::session::SessionInner;
use crate::statement::handle::ExecutedStatementHandle;
use crate::statement::validity::ValidityFlag;
use async_trait::async_trait;
use std::sync::Arc;

/// Server-side execution status.
///
/// Mirrors the SEA `Status` enum. `Failed` carries the underlying
/// `Error` so callers see `vendor_code` / `sql_state` / etc.
#[non_exhaustive]
#[derive(Debug)]
pub enum StatementStatus {
    Pending,
    Running,
    Succeeded,
    Failed(Error),
    Cancelled,
    Closed,
}

/// Async-consumption executed handle.
pub struct ExecutedAsyncStatement {
    session: Arc<SessionInner>,
    statement_id: String,
    /// Shared with the parent `Statement`; flipped on re-execute or
    /// when the parent `Statement` is dropped. Reads short-circuit
    /// stale calls locally with no network round-trip.
    valid: ValidityFlag,
}

impl ExecutedAsyncStatement {
    /// One-shot status check. Non-blocking. Issues a single
    /// GetStatementStatus RPC. Returns `Err(InvalidStatementHandle)`
    /// locally — without hitting the wire — if the parent `Statement`
    /// has already invalidated this handle.
    pub async fn status(&self) -> Result<StatementStatus> {
        unimplemented!("PR 5: validity check, then Transport::get_statement_status")
    }

    /// Block until the server reaches a terminal state, then return
    /// the result stream. Internally polls with backoff.
    pub async fn await_result(&self) -> Result<ResultStream> {
        unimplemented!("PR 5: implement await_result()")
    }

    /// Fetch the result without polling. Errors if the statement has
    /// not finished — the precise error code is tracked under the
    /// pending `ErrorCode` taxonomy expansion (see addendum A8). Until
    /// then PR 5 returns `ErrorCode::InvalidArgument` with a clear
    /// message; v0.1 will tighten this once `FailedPrecondition` /
    /// `InvalidState` lands. Useful when the caller has already
    /// confirmed completion via `status()`.
    pub async fn fetch_result(&self) -> Result<ResultStream> {
        unimplemented!("PR 5: implement fetch_result()")
    }
}

#[async_trait]
impl ExecutedStatementHandle for ExecutedAsyncStatement {
    fn statement_id(&self) -> &str {
        &self.statement_id
    }

    async fn cancel(&self) -> Result<()> {
        unimplemented!("PR 5: validity check, then Transport::cancel_statement")
    }

    async fn close(&self) -> Result<()> {
        unimplemented!("PR 5: implement close() — idempotent")
    }
}

impl Drop for ExecutedAsyncStatement {
    fn drop(&mut self) {
        // PR 5: fire-and-forget CloseStatement if close() wasn't called.
    }
}
