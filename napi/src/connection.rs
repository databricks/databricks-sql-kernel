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

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_sql_kernel::{Error as KernelError, Session, TypedValue};

use crate::async_statement::AsyncStatement;
use crate::error::napi_err_from_kernel;
use crate::params::{parse_typed_value, TypedValueInput};
use crate::runtime;
use crate::statement::Statement;
use crate::util::guarded;

/// Per-statement options for `Connection.executeStatement`.
///
/// Mirrors the kernel `StatementSpec` knobs that are safe to thread
/// through napi without a kernel-side change. Today this covers:
/// - `statementConf` — per-statement Spark conf overlay
///   (`StatementSpec.statement_conf` → SEA `parameters` /
///   Thrift `confOverlay`)
/// - `queryTags` — convenience wrapper over `statementConf` with
///   key `query_tags`; serialised to the same comma-separated
///   `key:value` wire shape NodeJS Thrift's `serializeQueryTags`
///   produces (`lib/utils/queryTags.ts`). Backslashes in keys are
///   doubled; backslash/colon/comma in values are backslash-escaped.
///
/// `rowLimit` (SEA `row_limit`) is exposed here and threaded onto the kernel
/// `StatementSpec`. `positionalParams` (`?`) and `namedParams` (`:name`)
/// carry bound query parameters, decoded via `params::parse_typed_value`.
/// (There is no `queryTimeoutSecs`: it abused the SEA `wait_timeout` inline-hold
/// window and was removed — a real per-statement timeout is `STATEMENT_TIMEOUT`.)
///
/// **Tag-order caveat (M4 parity note).** The napi `queryTags` field
/// is a Rust `HashMap<String, String>` whose iteration order is
/// non-deterministic, so the serialised `query_tags` value may have
/// a different key order than Thrift's `serializeQueryTags` (which
/// iterates `Object.keys(...)` in insertion order) for the same
/// input. The SEA server is order-insensitive on conf values, so
/// the two are functionally equivalent. If a caller needs
/// byte-identical Thrift parity, the JS adapter pre-serialises via
/// `serializeQueryTags` and writes the result into
/// `statementConf["query_tags"]` directly — see
/// `SeaSessionBackend.executeStatement` in the NodeJS driver. This
/// path is the one the production code uses.
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi(object)]
pub struct ExecuteOptions {
    /// Per-statement Spark conf overlay. Merged on top of the
    /// session-level `sessionConf` at execute time; this map wins
    /// on key collisions. Unknown keys are rejected by the server.
    pub statement_conf: Option<HashMap<String, String>>,
    /// Query tags as key→value pairs. Serialised to a comma-
    /// separated `key:value` string (backslash-escaping `\`, `:`,
    /// `,`) and placed into `statementConf["query_tags"]`, matching
    /// NodeJS Thrift's `serializeQueryTags` wire shape. Passing
    /// both `queryTags` AND a `query_tags` key in `statementConf`
    /// raises `InvalidArgument` — the caller's intent is ambiguous
    /// so we refuse to silently pick one over the other.
    ///
    /// A **`null`** value emits a **bare key** (no colon) — e.g.
    /// `{ production: null }` → `"production"` — matching the
    /// connectors' `key`-only tag form.
    ///
    /// See the struct-level "Tag-order caveat" for the
    /// HashMap-iteration-order vs `Object.keys`-iteration-order
    /// divergence and the byte-identical-Thrift-parity workaround.
    pub query_tags: Option<HashMap<String, Option<String>>>,
    /// Server-side cap on the number of rows this statement returns
    /// (SEA `row_limit`), independent of any SQL `LIMIT`. Maps to
    /// `StatementSpec.row_limit`. Omitted ⇒ no driver-imposed cap.
    pub row_limit: Option<i64>,
    /// Positional parameters, in 1-based wire order. Index `i` in this
    /// Vec corresponds to the `i+1`-th `?` placeholder in the SQL.
    /// Each entry is a `{ sqlType, value }` pair — `value` is the
    /// string-encoded literal or `null` for SQL NULL. Mirrors
    /// `StatementSpec::positional_params`; decoded via [`parse_typed_value`].
    pub positional_params: Option<Vec<TypedValueInput>>,
    /// Named parameters (`:name` placeholders). Each carries its `name`
    /// alongside the `{ sqlType, value? }` pair. Mapped to a kernel
    /// `TypedValue` via the same [`parse_typed_value`] codec and bound with
    /// `StatementSpec::param_named`. Named is the SEA-spec-required public
    /// param form (`StatementParameter.name` is `openapi_required`);
    /// positional is the documented-undocumented variant. The two are
    /// mutually exclusive at the SQL level (`?` vs `:name`).
    pub named_params: Option<Vec<NamedTypedValueInput>>,
}

/// A named bound parameter — a [`TypedValueInput`] plus its `:name`. Kept a
/// distinct napi object (rather than an optional `name` on `TypedValueInput`)
/// so the positional surface stays a clean ordered list with no name field.
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi(object)]
pub struct NamedTypedValueInput {
    pub name: String,
    pub sql_type: String,
    pub value: Option<String>,
}

/// Translate `ExecuteOptions.positional_params` into the kernel's
/// `Vec<TypedValue>`, parsing each entry through [`parse_typed_value`]
/// (same validation surface as the pyo3 binding). Returns `Err` on the
/// first malformed entry so the kernel never receives a partially-bound
/// spec.
fn decode_positional_params(inputs: Vec<TypedValueInput>) -> Result<Vec<TypedValue>, napi::Error> {
    inputs
        .into_iter()
        .map(|tv| {
            parse_typed_value(&tv.sql_type, tv.value.as_deref()).map_err(napi_err_from_kernel)
        })
        .collect()
}

/// Translate `ExecuteOptions.named_params` into `(name, TypedValue)` pairs,
/// parsing each value through the same [`parse_typed_value`] codec as the
/// positional path. Returns `Err` on the first malformed entry.
fn decode_named_params(
    inputs: Vec<NamedTypedValueInput>,
) -> Result<Vec<(String, TypedValue)>, napi::Error> {
    inputs
        .into_iter()
        .map(|nv| {
            parse_typed_value(&nv.sql_type, nv.value.as_deref())
                .map(|tv| (nv.name, tv))
                .map_err(napi_err_from_kernel)
        })
        .collect()
}

/// Closed-state error builder. Mirrors `statement::statement_closed_err`
/// — route through the kernel envelope so the TS reviver mints a typed
/// `KernelError(code='InvalidStatementHandle')`. Single source of truth
/// for `executeStatement` on a closed Connection. Also reused by
/// `metadata::take_metadata` so closed-state errors share one envelope.
pub(crate) fn connection_closed_err() -> napi::Error {
    napi_err_from_kernel(
        KernelError::invalid_statement_handle().message("connection already closed"),
    )
}

/// Build a kernel `Statement` from a decoded option set, under the caller's
/// session lock. Shared verbatim by `execute_statement` and `submit_statement`
/// so the option → spec mapping lives in exactly one place (any new option is
/// edited once). `Session::statement()` clones the session's internal `Arc`,
/// so the returned `Statement` is independent of the borrow and the caller can
/// release the lock before `execute()` / `submit()`.
#[allow(clippy::too_many_arguments)]
fn build_statement(
    session: &Session,
    sql: String,
    statement_conf: Option<HashMap<String, String>>,
    query_tags: Option<HashMap<String, Option<String>>>,
    row_limit: Option<i64>,
    positional_params: Vec<TypedValue>,
    named_params: Vec<(String, TypedValue)>,
) -> napi::Result<databricks_sql_kernel::Statement> {
    // Positional/named mutual-exclusivity is enforced in the kernel's
    // `build_execute_params` (StatementSpec dispatch), so every binding
    // — napi, pyo3, direct Rust — gets the same bind-time InvalidArgument.
    // No napi-layer guard needed here.
    let mut stmt = session.statement();
    stmt.spec().sql(sql);
    // Set the explicit per-statement conf overlay first (only when non-empty)
    // so the kernel's `query_tags` merge can detect a dual definition against it.
    if let Some(conf) = statement_conf {
        if !conf.is_empty() {
            stmt.spec().statement_conf(conf);
        }
    }
    // Fold the `queryTags` convenience map into the conf overlay. Empty map is
    // a no-op; a dual definition with `statementConf["query_tags"]` rejects.
    // A `null` JS value maps to `None` → a bare key on the wire.
    if let Some(tags) = query_tags {
        stmt.spec().query_tags(tags).map_err(napi_err_from_kernel)?;
    }
    // `None` leaves the kernel's omit-the-field default in place.
    stmt.spec().row_limit(row_limit);
    // Bind positional params 1-based on the wire.
    for (i, value) in positional_params.into_iter().enumerate() {
        stmt.spec()
            .param(i + 1, value)
            .map_err(napi_err_from_kernel)?;
    }
    // Bind named params (`:name`).
    for (name, value) in named_params {
        stmt.spec()
            .param_named(name, value)
            .map_err(napi_err_from_kernel)?;
    }
    Ok(stmt)
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
/// **Concurrency shape** — both `executeStatement` and
/// `submitStatement` build the kernel `Statement` under `inner.lock()`
/// and then RELEASE the guard before the wire call
/// (`stmt.execute().await` / `stmt.submit().await`). `Session::statement()`
/// is `&self`-callable and only clones the session's internal `Arc`, so
/// the built statement is independent of the guard. Concurrent
/// `Promise.all([executeStatement(q1), submitStatement(q2)])` therefore
/// serialise only for the microsecond statement-build, not the network
/// round-trip, and `close()` never blocks behind an in-flight execute or
/// submit. See
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
    /// Catalog / schema / sessionConf are session-level
    /// (`openSession`). Per-statement options on `ExecuteOptions`:
    /// - `statementConf` — per-statement Spark conf overlay
    /// - `queryTags` — serialised to a comma-separated `key:value`
    ///   string and placed in `statement_conf["query_tags"]`,
    ///   matching NodeJS Thrift's `serializeQueryTags` wire shape
    ///
    /// `options` is omitted/`None` for the no-options path; passing
    /// `{ statementConf: {} }` (an empty map) is treated the same as
    /// omission to keep the wire shape stable for the common case.
    #[napi]
    pub async fn execute_statement(
        &self,
        sql: String,
        mut options: Option<ExecuteOptions>,
    ) -> napi::Result<Statement> {
        let inner = Arc::clone(&self.inner);
        // Pull the scalar knobs out (both `i64`/`u32` are `Copy`) before
        // the `.take()`s below consume the map fields out of `options`.
        let row_limit = options.as_ref().and_then(|o| o.row_limit);        // Decode positional params before `options` is consumed below; a
        // malformed entry surfaces as InvalidArgument here, before any wire
        // round-trip.
        let positional_params = options
            .as_mut()
            .and_then(|o| o.positional_params.take())
            .map(decode_positional_params)
            .transpose()?
            .unwrap_or_default();
        let named_params = options
            .as_mut()
            .and_then(|o| o.named_params.take())
            .map(decode_named_params)
            .transpose()?
            .unwrap_or_default();
        // Pull the explicit `statement_conf` map and the `query_tags`
        // convenience map out of `options`. The kernel `StatementSpec`
        // owns the merge + serialisation: set the explicit conf first,
        // then fold `query_tags` in via `spec.query_tags(...)`, which
        // serialises to the Thrift wire shape and rejects the dual
        // `queryTags` + `statementConf["query_tags"]` definition.
        let statement_conf = options.as_mut().and_then(|o| o.statement_conf.take());
        let query_tags = options.as_mut().and_then(|o| o.query_tags.take());
        guarded(async move {
            // Build the kernel `Statement` while holding the session lock,
            // then RELEASE the lock before `execute().await`. `Session::statement()`
            // returns an owned `Statement` (it clones the session's internal
            // `Arc`), so the built statement is independent of the guard — we
            // do not need to hold the mutex across the network round-trip.
            // This unblocks concurrent `Promise.all([executeStatement(...)])`
            // (they serialise only for the microsecond statement-build, not the
            // wire call) and stops `close()` from blocking behind an in-flight
            // execute. See jira-candidate 2026-05-24-napi-cancel-during-fetch
            // (connection-side half).
            let mut stmt = {
                let guard = inner.lock().await;
                let session = guard.as_ref().ok_or_else(connection_closed_err)?;
                build_statement(
                    session,
                    sql,
                    statement_conf,
                    query_tags,
                    row_limit,
                    positional_params,
                    named_params,
                )?
                // session lock released here
            };

            let executed = stmt.execute().await.map_err(napi_err_from_kernel)?;
            // Pass the parent kernel `Statement` into the JS wrapper.
            // Dropping it here would invalidate the executed handle
            // via the shared ValidityFlag — see `StatementInner` docs
            // in `statement.rs` for the rationale.
            Ok(Statement::from_executed(stmt, executed))
        })
        .await
    }

    /// directResults execute — the Thrift/JDBC model. Sends ExecuteStatement
    /// with no `wait_timeout` field (server applies its ~10s default inline wait
    /// and auto-closes on success) and returns WITHOUT polling past it:
    ///
    /// - a **`Statement`** (left arm) when the query finished within the inline
    ///   wait — terminal, result ready inline, `close()` is a clean release;
    /// - an **`AsyncStatement`** (right arm) when it did not — a poll/cancel
    ///   handle the caller drives (`status()` / `awaitResult()` / `cancel()`).
    ///
    /// JS distinguishes the arms by feature-detecting `awaitResult` (present
    /// only on `AsyncStatement`). This is the path that gives mid-run cancel for
    /// long queries WITHOUT the eager-handle / close-drives workaround: the
    /// returned handle always corresponds to a server-owned statement.
    ///
    /// **Load-bearing contract:** the kernel's `DirectStatement::{Completed,
    /// Running}` discriminant cannot ride on these opaque `#[napi]` classes, so
    /// consumers MUST feature-detect via `awaitResult` (the only member unique to
    /// `AsyncStatement`). `Statement` (the Completed arm) MUST NOT gain an
    /// `awaitResult` member, or every consumer silently misroutes. The pyo3
    /// binding makes the same `await_result`-probe assumption.
    #[napi]
    pub async fn execute_statement_direct(
        &self,
        sql: String,
        mut options: Option<ExecuteOptions>,
    ) -> napi::Result<napi::Either<Statement, AsyncStatement>> {
        let inner = Arc::clone(&self.inner);
        let row_limit = options.as_ref().and_then(|o| o.row_limit);        let positional_params = options
            .as_mut()
            .and_then(|o| o.positional_params.take())
            .map(decode_positional_params)
            .transpose()?
            .unwrap_or_default();
        let named_params = options
            .as_mut()
            .and_then(|o| o.named_params.take())
            .map(decode_named_params)
            .transpose()?
            .unwrap_or_default();
        let statement_conf = options.as_mut().and_then(|o| o.statement_conf.take());
        let query_tags = options.as_mut().and_then(|o| o.query_tags.take());
        guarded(async move {
            let mut stmt = {
                let guard = inner.lock().await;
                let session = guard.as_ref().ok_or_else(connection_closed_err)?;
                build_statement(
                    session,
                    sql,
                    statement_conf,
                    query_tags,
                    row_limit,
                    positional_params,
                    named_params,
                )?
            };

            match stmt.execute_direct().await.map_err(napi_err_from_kernel)? {
                databricks_sql_kernel::DirectStatement::Completed(executed) => {
                    Ok(napi::Either::A(Statement::from_executed(stmt, executed)))
                }
                databricks_sql_kernel::DirectStatement::Running(executed_async) => Ok(
                    napi::Either::B(AsyncStatement::from_executed_async(stmt, executed_async)),
                ),
            }
        })
        .await
    }

    /// Execute a SQL statement on the blocking (sync) path, but return a
    /// `CancellableExecution` handle so a concurrent JS task can cancel
    /// the query *while it is still running server-side*.
    ///
    /// `executeStatement` builds the kernel `Statement`, awaits the
    /// blocking `execute()`, and only then hands JS a `Statement` — so a
    /// query that runs for several seconds is uncancellable from JS on
    /// that path (there is no handle until the blocking call resolves).
    /// This method instead builds the statement, captures a detached
    /// `StatementCanceller` **before** dispatching `execute()`, and hands
    /// JS a `CancellableExecution` immediately. The caller drives the
    /// blocking execution via `result()` (resolves to the same
    /// `Statement` `executeStatement` returns) and can fire `cancel()`
    /// concurrently to interrupt a still-running query mid-COMPUTE.
    ///
    /// Option semantics are identical to `executeStatement`.
    /// Mirrors the pyo3 `Statement.canceller()` / `Statement.execute()`
    /// split (PR #121): obtain the canceller before the blocking drive.
    #[napi]
    pub async fn execute_statement_cancellable(
        &self,
        sql: String,
        mut options: Option<ExecuteOptions>,
    ) -> napi::Result<crate::cancellable::CancellableExecution> {
        let inner = Arc::clone(&self.inner);
        // Same option decoding as `execute_statement`.
        let row_limit = options.as_ref().and_then(|o| o.row_limit);        let positional_params = options
            .as_mut()
            .and_then(|o| o.positional_params.take())
            .map(decode_positional_params)
            .transpose()?
            .unwrap_or_default();
        let named_params = options
            .as_mut()
            .and_then(|o| o.named_params.take())
            .map(decode_named_params)
            .transpose()?
            .unwrap_or_default();
        let statement_conf = options.as_mut().and_then(|o| o.statement_conf.take());
        let query_tags = options.as_mut().and_then(|o| o.query_tags.take());
        guarded(async move {
            // Build the kernel `Statement` under the session lock, release
            // the lock, then hand the built statement to the
            // `CancellableExecution` WITHOUT dispatching `execute()` here —
            // the blocking drive runs in `CancellableExecution::result()`,
            // after JS has the cancellable handle in hand. The canceller is
            // captured inside `CancellableExecution::new` off the built
            // statement before any execute dispatch.
            let stmt = {
                let guard = inner.lock().await;
                let session = guard.as_ref().ok_or_else(connection_closed_err)?;
                build_statement(
                    session,
                    sql,
                    statement_conf,
                    query_tags,
                    row_limit,
                    positional_params,
                    named_params,
                )?
                // session lock released here
            };
            Ok(crate::cancellable::CancellableExecution::new(stmt))
        })
        .await
    }

    /// Submit a SQL statement and return immediately with an
    /// `AsyncStatement` handle, without blocking until the query
    /// finishes. The kernel's `Statement::submit()` sends
    /// `wait_timeout=0s`, so the server responds as soon as it has a
    /// `statement_id` (state `Pending`/`Running`); JS drives polling
    /// via `AsyncStatement.status()` and materialises results with
    /// `AsyncStatement.awaitResult()`.
    ///
    /// This is the async-execution path the Thrift backend always
    /// uses (`runAsync: true`): the SEA backend submits, returns a
    /// pending operation handle, and polls to terminal during
    /// fetch. Option semantics (statementConf / queryTags /
    /// rowLimit / positional + named params) match `executeStatement`.
    /// Submit always sends `wait_timeout=0s` so the call returns
    /// immediately; the caller drives completion via `status()` /
    /// `awaitResult()`. Only the blocking-vs-pending return contract
    /// differs from `executeStatement`.
    #[napi]
    pub async fn submit_statement(
        &self,
        sql: String,
        mut options: Option<ExecuteOptions>,
    ) -> napi::Result<AsyncStatement> {
        let inner = Arc::clone(&self.inner);
        // Same option decoding as `execute_statement` — malformed
        // params surface as InvalidArgument before any wire round-trip.
        let row_limit = options.as_ref().and_then(|o| o.row_limit);
        let positional_params = options
            .as_mut()
            .and_then(|o| o.positional_params.take())
            .map(decode_positional_params)
            .transpose()?
            .unwrap_or_default();
        let named_params = options
            .as_mut()
            .and_then(|o| o.named_params.take())
            .map(decode_named_params)
            .transpose()?
            .unwrap_or_default();
        // Same conf + query_tags plumbing as `execute_statement`: the
        // kernel `StatementSpec` owns the merge + serialisation.
        let statement_conf = options.as_mut().and_then(|o| o.statement_conf.take());
        let query_tags = options.as_mut().and_then(|o| o.query_tags.take());
        guarded(async move {
            // Build the kernel `Statement` while holding the session lock,
            // then RELEASE the lock before `submit().await` — same shape as
            // `execute_statement`. `Session::statement()` clones the session's
            // internal `Arc`, so the built statement is independent of the
            // guard and we never need the mutex across the wire round-trip.
            // This keeps concurrent `submitStatement` calls from serialising
            // on the session mutex and stops `close()` blocking behind an
            // in-flight submit (matters on transport retries — submit itself
            // sends `wait_timeout=0s`). See jira-candidate
            // 2026-05-24-napi-cancel-during-fetch (connection-side half).
            let mut stmt = {
                let guard = inner.lock().await;
                let session = guard.as_ref().ok_or_else(connection_closed_err)?;
                build_statement(
                    session,
                    sql,
                    statement_conf,
                    query_tags,
                    row_limit,
                    positional_params,
                    named_params,
                )?
                // session lock released here
            };

            let executed_async = stmt.submit().await.map_err(napi_err_from_kernel)?;
            // Pass the parent kernel `Statement` into the JS wrapper so
            // its `Drop` doesn't invalidate the executed-async handle
            // via the shared ValidityFlag while JS still holds it.
            Ok(AsyncStatement::from_executed_async(stmt, executed_async))
        })
        .await
    }

    /// Explicit close. Awaits the server-side `DeleteSession` so the
    /// JS caller can observe failures (auth revoked mid-session,
    /// warehouse stopped, network error). Idempotent — a second call
    /// on an already-closed connection returns `Ok`.
    ///
    /// **Errors are terminal from the JS side.** The kernel session
    /// handle is consumed (`take()`) BEFORE the wire `DeleteSession`
    /// runs, because `Session::close` takes `self` by value. On `Err`,
    /// the napi `inner` is already `None`, so a JS-side retry sees a
    /// closed connection and returns `Ok(())` without re-attempting
    /// the wire call. The kernel's own `Drop` fire-and-forget retry
    /// runs once in the background — the JS caller can log the error
    /// but cannot drive a retry. If you need retry-on-failure
    /// semantics for `DeleteSession`, layer them above this method.
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
