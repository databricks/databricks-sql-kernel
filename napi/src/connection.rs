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

//! Opaque `Connection` wrapper around the kernel's `Session`.
//!
//! The kernel collapses ADBC's `Database` + `Connection` into a single
//! `Session`. We keep the wrapper name `Connection` on the JS side because
//! that matches the existing Node driver's mental model.
//!
//! M0 surface (Round 2):
//! - `Connection.executeStatement(sql)` — builds a kernel `Statement`,
//!   sets the SQL, awaits `execute()`, wraps the result in a
//!   JS-visible `Statement` opaque handle. Catalog / schema /
//!   sessionConf are session-level (`openSession` options), matching
//!   pyo3 — the SEA wire protocol carries them on `CreateSession`,
//!   not per-statement. Positional / named parameters land in M1.
//! - `Connection.close()` — explicit async close. Drop schedules a
//!   fire-and-forget close on the captured runtime handle if explicit
//!   close was never called.

use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_sql_kernel::Session;

use crate::error::napi_err_from_kernel;
use crate::runtime;
use crate::statement::Statement;
use crate::util::guarded;

/// Opaque connection handle wrapping a kernel `Session`.
///
/// `inner` is `Arc<Mutex<Option<Session>>>` so:
/// - the Drop impl can clone the `Arc` and `.take()` the session on a
///   background tokio task without holding `&mut self` (which Drop is
///   forbidden from doing across an `await`),
/// - `executeStatement` can share immutable access to the session via
///   the `Arc<SessionInner>` clones the kernel makes internally
///   (`Session::statement()` only needs `&self`).
#[napi]
pub struct Connection {
    pub(crate) inner: Arc<Mutex<Option<Session>>>,
}

#[napi]
impl Connection {
    /// Execute a SQL statement and return a Statement handle that
    /// streams batches via `fetchNextBatch()`.
    ///
    /// No per-statement options: catalog / schema / sessionConf are
    /// session-level (`openSession`). Positional / named parameters
    /// land in M1 via `Statement::spec().param(…)` on the kernel.
    #[napi]
    pub async fn execute_statement(&self, sql: String) -> napi::Result<Statement> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let session = guard.as_ref().ok_or_else(|| {
                napi::Error::new(napi::Status::InvalidArg, "connection already closed")
            })?;

            let mut stmt = session.statement();
            stmt.spec().sql(sql);

            let executed = stmt.execute().await.map_err(napi_err_from_kernel)?;
            Ok(Statement::from_executed(executed))
        })
        .await
    }

    /// Explicit close. Awaits the server-side `DeleteSession` so the
    /// JS caller can observe failures (auth revoked mid-session,
    /// warehouse stopped, network error). Idempotent — a second call
    /// on an already-closed connection returns `Ok`.
    ///
    /// On `Err`, the kernel leaves its internal `closed` flag false
    /// so `Drop`'s fire-and-forget cleanup retries; the caller still
    /// sees the error and can surface it to logs / telemetry.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let session = {
                let mut guard = inner.lock().await;
                guard.take()
            };
            let Some(session) = session else {
                return Ok(());
            };
            session.close().await.map_err(napi_err_from_kernel)
        })
        .await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Fire-and-forget close on the captured runtime. If `close()`
        // was already called explicitly, `inner` holds `None` and the
        // spawned task is a trivial no-op. `Drop` itself cannot
        // `.await`, so callers who care about observing
        // `DeleteSession` failures must call `close()` explicitly.
        let Some(handle) = runtime::try_get_handle() else {
            // No async entry point ever ran — there's nothing to close.
            return;
        };
        let inner = Arc::clone(&self.inner);
        handle.spawn(async move {
            // Drop the session value on the runtime. The kernel's
            // `SessionInner::Drop` spawns a fire-and-forget
            // `delete_session` against its own captured handle, so
            // dropping inside a tokio context is sufficient cleanup.
            let _taken = {
                let mut guard = inner.lock().await;
                guard.take()
            };
        });
    }
}
