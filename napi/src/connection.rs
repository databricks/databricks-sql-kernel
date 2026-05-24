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
//! `Session`. We keep the wrapper name `Connection` on the JS side
//! because that matches the existing Node driver's mental model.
//!
//! - `Connection.executeStatement(sql)` — builds a kernel `Statement`,
//!   sets the SQL, awaits `execute()`, and wraps the result in a
//!   JS-visible `Statement` opaque handle. Catalog / schema /
//!   sessionConf are session-level (`openSession` options) — the SEA
//!   wire protocol carries them on `CreateSession`, not per-statement.
//! - `Connection.close()` — explicit async close. Drop schedules a
//!   fire-and-forget close on the captured runtime handle if explicit
//!   close was never called.

use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_sql_kernel::{Error as KernelError, Session};

use crate::error::napi_err_from_kernel;
use crate::runtime;
use crate::statement::Statement;
use crate::util::guarded;

/// Closed-state error builder. Mirrors `statement::statement_closed_err`
/// — route through the kernel envelope so the TS reviver mints a typed
/// `KernelError(code='InvalidStatementHandle')`. Single source of truth
/// for `executeStatement` on a closed Connection.
fn connection_closed_err() -> napi::Error {
    napi_err_from_kernel(
        KernelError::invalid_statement_handle().message("connection already closed"),
    )
}

/// Opaque connection handle wrapping a kernel `Session`.
///
/// `inner` is `Arc<Mutex<Option<Session>>>` so:
/// - the Drop impl can clone the `Arc` and `.take()` the session on a
///   background tokio task without holding `&mut self` (which Drop is
///   forbidden from doing across an `await`),
/// - `close()` can `.take()` the session to consume it for the kernel's
///   move-by-value `Session::close(self)` signature.
///
/// **Current concurrency shape** — `executeStatement` holds
/// `inner.lock()` across `stmt.execute().await`, so two concurrent
/// `Promise.all([executeStatement(q1), executeStatement(q2)])` calls
/// on the same Connection serialise even though the kernel transport
/// supports concurrent statements per session, and `close()` blocks
/// behind any in-flight execute. The kernel's `Session::statement()`
/// is `&self`-callable, so the right shape is `Arc<Session>` with
/// concurrent execute paths; that lands in the follow-up lock-shape
/// refactor — see
/// `sea-workflow/jira-candidates/2026-05-24-napi-cancel-during-fetch.md`.
#[napi]
pub struct Connection {
    pub(crate) inner: Arc<Mutex<Option<Session>>>,
    /// Server-issued session id. Cached at construction; readable
    /// even after `close()` so JS-side log lines can correlate
    /// against kernel / server logs which key on the same id.
    pub(crate) session_id: String,
}

#[napi]
impl Connection {
    /// Server-issued session id. Cached at construction; readable
    /// even after `close()` so JS-side log lines can correlate
    /// against kernel / server logs which key on the same id.
    #[napi(getter)]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Execute a SQL statement and return a Statement handle that
    /// streams batches via `fetchNextBatch()`.
    ///
    /// No per-statement options: catalog / schema / sessionConf are
    /// session-level (`openSession`).
    #[napi]
    pub async fn execute_statement(&self, sql: String) -> napi::Result<Statement> {
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let session = guard.as_ref().ok_or_else(connection_closed_err)?;

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
        // Best-effort cleanup: schedule a drop of the kernel handle on
        // the captured tokio runtime. JS callers who need a
        // guaranteed `DeleteSession` round-trip must call `close()`
        // explicitly — `Drop` cannot `.await` and cannot surface
        // failures, and the spawned task is subject to the runtime's
        // lifetime. If process exit tears down the runtime before the
        // task runs, the handle is dropped without a wire close; the
        // server reclaims the session on its idle-timeout schedule.
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

#[cfg(test)]
mod tests {
    use super::*;

    // The closed-state error shape pins the TS adapter's reviver
    // contract — a future maintainer who switches back to raw
    // `napi::Error::new(Status::InvalidArg, "...")` will break this
    // test instead of silently breaking the adapter.

    fn assert_invalid_statement_handle_envelope(err: &napi::Error) {
        let msg = err.reason.as_str();
        assert!(
            msg.starts_with(crate::error::ERROR_SENTINEL),
            "expected envelope-sentinel-prefixed error, got: {msg}",
        );
        assert!(
            msg.contains(r#""code":"InvalidStatementHandle""#),
            "envelope payload missing InvalidStatementHandle code: {msg}",
        );
    }

    #[test]
    fn connection_closed_err_carries_invalid_statement_handle_envelope() {
        let err = connection_closed_err();
        assert_invalid_statement_handle_envelope(&err);
        assert!(
            err.reason.contains("connection already closed"),
            "closed-state error must carry the human message: {}",
            err.reason,
        );
    }

    #[test]
    fn connection_closed_err_status_collapses_to_invalid_arg() {
        let err = connection_closed_err();
        assert_eq!(err.status, napi::Status::InvalidArg);
    }

    #[test]
    fn session_id_is_cached_and_survives_close() {
        // The cached id is stored on the wrapper, not on the kernel
        // Session — so even after close() takes the session out of
        // `inner`, the getter still returns the original id.
        let conn = Connection {
            inner: Arc::new(Mutex::new(None)), // "already closed"
            session_id: "session-survives-close".to_owned(),
        };
        assert_eq!(conn.session_id(), "session-survives-close");
    }
}
