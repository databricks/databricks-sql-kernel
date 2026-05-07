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

#![allow(dead_code, unused_variables)]

use crate::kernel_error::Result;
use crate::result_stream::{ResultStream, SchemaRef};
use crate::session::SessionInner;
use crate::statement::handle::ExecutedStatementHandle;
use crate::statement::validity::ValidityFlag;
use async_trait::async_trait;
use std::sync::Arc;

/// Sync-consumption executed handle.
pub struct ExecutedStatement {
    session: Arc<SessionInner>,
    statement_id: String,
    /// Shared with the parent `Statement`; flipped on re-execute or
    /// when the parent `Statement` is dropped. Reads short-circuit
    /// stale calls locally with no network round-trip.
    valid: ValidityFlag,
    result_stream: ResultStream,
}

impl ExecutedStatement {
    /// Result schema. Available without iterating any batches; mirrors
    /// the underlying `ResultStream::schema()`.
    pub fn schema(&self) -> SchemaRef {
        self.result_stream.schema()
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
        unimplemented!("PR 5: validity check, then Transport::cancel_statement")
    }

    async fn close(&self) -> Result<()> {
        unimplemented!("PR 5: implement close() — idempotent")
    }
}

impl Drop for ExecutedStatement {
    fn drop(&mut self) {
        // PR 5: fire-and-forget CloseStatement if close() wasn't called.
    }
}
