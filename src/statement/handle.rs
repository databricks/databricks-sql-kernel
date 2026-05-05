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

//! `ExecutedStatementHandle` — shared trait for both executed handles.
//!
//! Per `docs/spec-v0.md` addendum A1, both `ExecutedStatement` and
//! `ExecutedAsyncStatement` carry a `statement_id` and can be cancelled
//! and closed. Wrappers (especially the C ABI) often want to handle
//! "any executed statement" generically — this trait gives them a
//! unified shape without parallel function families.

#![allow(dead_code)]

use crate::kernel_error::Result;
use async_trait::async_trait;

/// Shared surface across [`ExecutedStatement`](super::executed::ExecutedStatement)
/// and [`ExecutedAsyncStatement`](super::executed_async::ExecutedAsyncStatement).
///
/// `close` takes `&self` (not `self`) so the trait stays object-safe
/// (`Box<dyn ExecutedStatementHandle>` is the C ABI's expected pattern).
/// Implementations make `close` idempotent — a second call is a no-op
/// success.
#[async_trait]
pub trait ExecutedStatementHandle: Send + Sync {
    /// Server-issued statement identifier.
    fn statement_id(&self) -> &str;

    /// Cancel a still-running server-side execution. No-op if the server
    /// already finished. Returns `Err(InvalidStatementHandle)` locally
    /// — without hitting the wire — if the handle has been invalidated
    /// by a re-execute on the parent `Statement`.
    async fn cancel(&self) -> Result<()>;

    /// Close the handle. Idempotent: a second call returns `Ok(())`
    /// without contacting the server. `Drop` also fires-and-forgets
    /// cleanup if `close` was never called explicitly.
    async fn close(&self) -> Result<()>;
}
