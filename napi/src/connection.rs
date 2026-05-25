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

use databricks_sql_kernel::{Error as KernelError, Session};

use crate::error::napi_err_from_kernel;
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
///   key `query_tags`; serialised to JSON before being placed in the
///   conf overlay. Mirrors NodeJS Thrift's `serializeQueryTags`
///   convention (`lib/DBSQLSession.ts::serializeQueryTags`).
///
/// Positional parameters, named parameters, row limit, and wait
/// timeout are deferred: they require either a non-trivial JS↔napi
/// `TypedValue` mapping (params) or a kernel-side `StatementSpec`
/// extension (`row_limit`/`wait_timeout`). Both land in follow-on PRs.
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
    /// Query tags as key→value pairs. JSON-encoded into the
    /// `query_tags` conf overlay key, matching NodeJS Thrift's
    /// `serializeQueryTags` shape. Passing both `queryTags` and a
    /// `query_tags` key in `statementConf` raises `InvalidArgument`
    /// — the explicit field wins to avoid silent overwrite.
    pub query_tags: Option<HashMap<String, String>>,
}

/// Closed-state error builder. Mirrors `statement::statement_closed_err`
/// — route through the kernel envelope so the TS reviver mints a typed
/// `KernelError(code='InvalidStatementHandle')`. Single source of truth
/// for `executeStatement` on a closed Connection.
fn connection_closed_err() -> napi::Error {
    napi_err_from_kernel(
        KernelError::invalid_statement_handle().message("connection already closed"),
    )
}

/// Merge `statement_conf` and `query_tags` from JS-supplied
/// `ExecuteOptions` into the single `HashMap<String, String>` the
/// kernel's `StatementSpec::statement_conf` accepts.
///
/// Returns `Ok(None)` when no per-statement conf is set (caller skips
/// the `.statement_conf(...)` builder call so the kernel sees the
/// session-level conf untouched). Returns `Ok(Some(conf))` when at
/// least one knob was set. Returns `Err(InvalidArgument)` when both
/// `queryTags` and `statementConf["query_tags"]` are present — the
/// caller's intent is ambiguous.
fn build_merged_statement_conf(
    options: Option<ExecuteOptions>,
) -> napi::Result<Option<HashMap<String, String>>> {
    let Some(options) = options else {
        return Ok(None);
    };

    let mut conf = options.statement_conf.unwrap_or_default();
    if let Some(tags) = options.query_tags {
        if !tags.is_empty() && conf.contains_key("query_tags") {
            return Err(napi_err_from_kernel(
                KernelError::invalid_argument().message(
                    "ExecuteOptions: cannot supply both `queryTags` and \
                     `statementConf[\"query_tags\"]` — pick one",
                ),
            ));
        }
        if !tags.is_empty() {
            conf.insert("query_tags".to_string(), serialize_query_tags(&tags));
        }
    }

    if conf.is_empty() {
        Ok(None)
    } else {
        Ok(Some(conf))
    }
}

/// Serialise a tag map to the same wire shape NodeJS Thrift's
/// `serializeQueryTags` produces: comma-separated `key:value` pairs.
/// Backslashes in keys are doubled; backslash, colon, and comma in
/// values are backslash-escaped. Null/empty values would produce a
/// bare key with no colon — but the napi `HashMap<String, String>`
/// shape can't carry nulls today, so every tag here is `key:value`.
///
/// Match the Thrift wire shape exactly: the SEA server merges
/// `confOverlay.query_tags` the same way regardless of which backend
/// produced it, and JS callers porting from Thrift expect identical
/// behaviour.
fn serialize_query_tags(tags: &HashMap<String, String>) -> String {
    tags.iter()
        .map(|(k, v)| {
            let esc_key = k.replace('\\', "\\\\");
            let esc_val = v
                .chars()
                .flat_map(|c| match c {
                    '\\' | ':' | ',' => vec!['\\', c],
                    other => vec![other],
                })
                .collect::<String>();
            format!("{esc_key}:{esc_val}")
        })
        .collect::<Vec<_>>()
        .join(",")
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
    /// Catalog / schema / sessionConf are session-level
    /// (`openSession`). Per-statement options on `ExecuteOptions`:
    /// - `statementConf` — per-statement Spark conf overlay
    /// - `queryTags` — JSON-encoded into `statement_conf["query_tags"]`
    ///   matching NodeJS Thrift's `serializeQueryTags` shape
    ///
    /// `options` is omitted/`None` for the no-options path; passing
    /// `{ statementConf: {} }` (an empty map) is treated the same as
    /// omission to keep the wire shape stable for the common case.
    #[napi]
    pub async fn execute_statement(
        &self,
        sql: String,
        options: Option<ExecuteOptions>,
    ) -> napi::Result<Statement> {
        let inner = Arc::clone(&self.inner);
        // Merge `query_tags` into `statement_conf` at the napi boundary
        // so the kernel sees a single conf map. JSON-encode the tag
        // map to match the wire shape NodeJS Thrift produces in
        // `serializeQueryTags` (`{"key":"value", ...}`).
        let merged_conf = build_merged_statement_conf(options)?;
        guarded(async move {
            let guard = inner.lock().await;
            let session = guard.as_ref().ok_or_else(connection_closed_err)?;

            let mut stmt = session.statement();
            stmt.spec().sql(sql);
            if let Some(conf) = merged_conf {
                stmt.spec().statement_conf(conf);
            }

            let executed = stmt.execute().await.map_err(napi_err_from_kernel)?;
            // Pass the parent kernel `Statement` into the JS wrapper.
            // Dropping it here would invalidate the executed handle
            // via the shared ValidityFlag — see `StatementInner` docs
            // in `statement.rs` for the rationale.
            Ok(Statement::from_executed(stmt, executed))
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

    // ─── ExecuteOptions / build_merged_statement_conf ───────────────────

    #[test]
    fn merged_conf_none_when_options_is_none() {
        let result = build_merged_statement_conf(None).unwrap();
        assert!(result.is_none(), "no options → no statement_conf");
    }

    #[test]
    fn merged_conf_none_when_both_maps_empty() {
        let result = build_merged_statement_conf(Some(ExecuteOptions {
            statement_conf: Some(HashMap::new()),
            query_tags: Some(HashMap::new()),
        }))
        .unwrap();
        assert!(result.is_none(), "empty maps → no statement_conf");
    }

    #[test]
    fn merged_conf_passes_through_statement_conf() {
        let mut conf = HashMap::new();
        conf.insert("ANSI_MODE".to_string(), "true".to_string());
        let result = build_merged_statement_conf(Some(ExecuteOptions {
            statement_conf: Some(conf),
            query_tags: None,
        }))
        .unwrap()
        .unwrap();
        assert_eq!(result.get("ANSI_MODE").map(String::as_str), Some("true"));
    }

    #[test]
    fn merged_conf_query_tags_serialised_into_query_tags_key() {
        let mut tags = HashMap::new();
        tags.insert("team".to_string(), "platform".to_string());
        let result = build_merged_statement_conf(Some(ExecuteOptions {
            statement_conf: None,
            query_tags: Some(tags),
        }))
        .unwrap()
        .unwrap();
        // Single-tag case is deterministic regardless of HashMap order.
        assert_eq!(
            result.get("query_tags").map(String::as_str),
            Some("team:platform"),
        );
    }

    #[test]
    fn merged_conf_query_tags_escape_special_chars_in_values() {
        let mut tags = HashMap::new();
        tags.insert("k".to_string(), "v:1,v\\2".to_string());
        let result = build_merged_statement_conf(Some(ExecuteOptions {
            statement_conf: None,
            query_tags: Some(tags),
        }))
        .unwrap()
        .unwrap();
        let encoded = result.get("query_tags").unwrap();
        // `:` → `\:`, `,` → `\,`, `\` → `\\`. Order in the input is
        // `v:1,v\2` so the encoded form is `v\:1\,v\\2`.
        assert_eq!(encoded, "k:v\\:1\\,v\\\\2");
    }

    #[test]
    fn merged_conf_query_tags_escape_backslash_in_keys() {
        let mut tags = HashMap::new();
        tags.insert("k\\1".to_string(), "v".to_string());
        let result = build_merged_statement_conf(Some(ExecuteOptions {
            statement_conf: None,
            query_tags: Some(tags),
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            result.get("query_tags").map(String::as_str),
            Some("k\\\\1:v")
        );
    }

    #[test]
    fn merged_conf_rejects_dual_query_tags_definition() {
        let mut conf = HashMap::new();
        conf.insert("query_tags".to_string(), "explicit:value".to_string());
        let mut tags = HashMap::new();
        tags.insert("team".to_string(), "platform".to_string());
        let err = build_merged_statement_conf(Some(ExecuteOptions {
            statement_conf: Some(conf),
            query_tags: Some(tags),
        }))
        .expect_err("must reject when both are set");
        assert!(
            err.reason.contains("cannot supply both"),
            "error must explain the conflict: {}",
            err.reason,
        );
    }

    #[test]
    fn merged_conf_empty_query_tags_does_not_collide_with_existing_key() {
        // An empty `queryTags: {}` map should be ignored, so the
        // caller can still pass an explicit
        // `statementConf["query_tags"]` alongside `queryTags: {}`
        // without tripping the ambiguity guard. Convention: empty
        // input maps mean "not supplied".
        let mut conf = HashMap::new();
        conf.insert("query_tags".to_string(), "explicit:value".to_string());
        let result = build_merged_statement_conf(Some(ExecuteOptions {
            statement_conf: Some(conf),
            query_tags: Some(HashMap::new()),
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            result.get("query_tags").map(String::as_str),
            Some("explicit:value"),
        );
    }
}
