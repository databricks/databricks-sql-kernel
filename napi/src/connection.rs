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

use databricks_sql_kernel::{Error as KernelError, Session, TypedValue};

use crate::error::napi_err_from_kernel;
use crate::params::{parse_typed_value, TypedValueInput};
use crate::runtime;
use crate::statement::Statement;
use crate::util::guarded;

/// JS-visible options bag for `Connection.executeStatement`.
///
/// All fields are optional. The default-everything shape (no options
/// argument on the JS side, or `{}`) is equivalent to the parameterless
/// `executeStatement(sql)` form preserved from the napi-skeleton PR.
///
/// Carries `positional_params` (`?`) and `named_params` (`:name`) — both
/// decoded through the same `parse_typed_value` codec. Keeping the surface
/// as a struct lets future statement-level knobs (conf overlay, result
/// format) land non-breakingly. Mirrors `StatementSpec`'s plain-data shape
/// kernel-side.
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi(object)]
pub struct ExecuteOptions {
    /// Positional parameters, in 1-based wire order. Index `i` in this
    /// Vec corresponds to the `i+1`-th `?` placeholder in the SQL.
    /// Each entry is a `{ sqlType, value }` pair — `value` is the
    /// string-encoded literal or `null` for SQL NULL.
    ///
    /// Mirrors `StatementSpec::positional_params` (kernel-side) and
    /// `pyo3::Statement::bind_param` (pyo3-side). The JS adapter is
    /// expected to stringify values before crossing the FFI — see
    /// `DBSQLParameter.toSparkParameter()` on the Node-driver side for
    /// the canonical stringification rules.
    pub positional_params: Option<Vec<TypedValueInput>>,
    /// Named parameters (`:name` placeholders). Each carries its `name`
    /// alongside the `{ sqlType, value? }` pair, decoded via the same
    /// [`parse_typed_value`] codec and bound with `StatementSpec::param_named`.
    /// Named is the SEA-spec-required public param form
    /// (`StatementParameter.name` is `openapi_required`); positional is the
    /// documented-undocumented variant. Mutually exclusive with
    /// `positional_params` at the SQL level (`?` vs `:name`).
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

/// Translate the JS-side `ExecuteOptions.positional_params` into the
/// kernel's `Vec<TypedValue>`. Each entry is parsed through
/// [`parse_typed_value`] so the validation surface (bad literal, bad
/// DECIMAL precision, unsupported type) is the same as the pyo3
/// binding's.
///
/// Returns `Err` on the FIRST malformed entry — the JS caller sees the
/// first failure's structured envelope, and the kernel never receives
/// a partially-bound spec. Matches how pyo3 surfaces bind failures
/// (eager, per-call).
fn decode_positional_params(inputs: Vec<TypedValueInput>) -> Result<Vec<TypedValue>, napi::Error> {
    inputs
        .into_iter()
        .map(|tv| {
            parse_typed_value(&tv.sql_type, tv.value.as_deref()).map_err(napi_err_from_kernel)
        })
        .collect()
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
    /// `options` is optional. The `positional_params` field, when
    /// present, is the typed-value codec input the kernel binds against
    /// the `?` placeholders in `sql`. Catalog / schema / sessionConf
    /// remain session-level (`openSession`) — the SEA wire protocol
    /// carries them on `CreateSession`, not per-statement.
    ///
    /// Bind-time errors (bad DECIMAL precision, unsupported SQL type,
    /// non-finite FLOAT/DOUBLE) surface as `KernelError(code=
    /// 'InvalidArgument')` BEFORE the wire `ExecuteStatement` call —
    /// matches the pyo3 binding's eager-bind contract and the JDBC /
    /// ODBC drivers' "fail at setParameter" expectation.
    #[napi]
    pub async fn execute_statement(
        &self,
        sql: String,
        options: Option<ExecuteOptions>,
    ) -> napi::Result<Statement> {
        // Decode params BEFORE acquiring the session lock — bind-time
        // validation failures should not block other concurrent
        // executors waiting on the mutex. The kernel `StatementSpec`
        // is plain data and accepts a pre-built `Vec<TypedValue>` via
        // the `positional_params` field, so we can do this work on the
        // caller's tokio task.
        let (positional, named) = match options {
            Some(o) => {
                let positional = match o.positional_params {
                    Some(inputs) => decode_positional_params(inputs)?,
                    None => Vec::new(),
                };
                let named = match o.named_params {
                    Some(inputs) => decode_named_params(inputs)?,
                    None => Vec::new(),
                };
                (positional, named)
            }
            None => (Vec::new(), Vec::new()),
        };
        let inner = Arc::clone(&self.inner);
        guarded(async move {
            let guard = inner.lock().await;
            let session = guard.as_ref().ok_or_else(connection_closed_err)?;

            let mut stmt = session.statement();
            stmt.spec().sql(sql);
            if !positional.is_empty() {
                // Set the whole vec at once — `StatementSpec::positional_params`
                // is a public field, so direct assignment is the
                // intended kernel surface. Avoids the per-call `param(i,
                // v)` resize loop that's there for sparse-bind cases.
                stmt.spec().positional_params = positional;
            }
            // Bind named params (`:name`) via the spec builder.
            for (name, value) in named {
                stmt.spec()
                    .param_named(name, value)
                    .map_err(napi_err_from_kernel)?;
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

    // ─── decode_positional_params ──────────────────────────────────
    //
    // The decode happens BEFORE the session lock — these tests pin
    // that contract at the helper level. End-to-end behavior is
    // covered by the params.rs codec tests; here we focus on the
    // happy-path Vec composition and the error-envelope shape.

    #[test]
    fn decode_positional_params_empty_vec_round_trips() {
        let got = decode_positional_params(Vec::new()).expect("empty decode succeeds");
        assert!(got.is_empty());
    }

    #[test]
    fn decode_positional_params_10_type_matrix() {
        // The C5 cluster's headline 10-type matrix, end-to-end through
        // the decode helper rather than through `parse_typed_value`
        // directly. Confirms the `TypedValueInput` → `TypedValue`
        // mapping survives the Vec round trip.
        let inputs = vec![
            TypedValueInput {
                sql_type: "INT".into(),
                value: Some("42".into()),
            },
            TypedValueInput {
                sql_type: "BIGINT".into(),
                value: Some("9999999999".into()),
            },
            TypedValueInput {
                sql_type: "FLOAT".into(),
                value: Some("3.14".into()),
            },
            TypedValueInput {
                sql_type: "DOUBLE".into(),
                value: Some("2.718281828459045".into()),
            },
            TypedValueInput {
                sql_type: "STRING".into(),
                value: Some("hello world".into()),
            },
            TypedValueInput {
                sql_type: "BOOLEAN".into(),
                value: Some("true".into()),
            },
            TypedValueInput {
                sql_type: "DATE".into(),
                value: Some("2026-05-15".into()),
            },
            TypedValueInput {
                sql_type: "TIMESTAMP".into(),
                value: Some("2026-05-15 12:30:45".into()),
            },
            TypedValueInput {
                sql_type: "DECIMAL(10,2)".into(),
                value: Some("-123.45".into()),
            },
        ];
        let decoded = decode_positional_params(inputs).expect("10-type decode succeeds");
        assert_eq!(decoded.len(), 9);
        // BINARY — pin the bind-time-rejection contract through the
        // helper. The 10th type in the matrix produces an
        // InvalidArgument envelope rather than aborting the whole vec
        // silently.
        let err = decode_positional_params(vec![TypedValueInput {
            sql_type: "BINARY".into(),
            value: Some("cafe".into()),
        }])
        .unwrap_err();
        let reason = err.reason.as_str();
        assert!(
            reason.starts_with(crate::error::ERROR_SENTINEL),
            "BINARY rejection must be enveloped, got: {reason}"
        );
        assert!(
            reason.contains(r#""code":"InvalidArgument""#),
            "BINARY rejection must carry InvalidArgument, got: {reason}"
        );
    }

    #[test]
    fn decode_positional_params_short_circuits_on_first_bad_entry() {
        // First bad entry stops decoding — the JS caller sees one
        // structured envelope rather than a multi-error report. The
        // good entry that follows must not contribute to the result.
        let inputs = vec![
            TypedValueInput {
                sql_type: "INT".into(),
                value: Some("42".into()),
            },
            TypedValueInput {
                sql_type: "DECIMAL(10,2)".into(),
                value: Some("not-a-number".into()),
            },
            TypedValueInput {
                sql_type: "STRING".into(),
                value: Some("should-not-decode".into()),
            },
        ];
        let err = decode_positional_params(inputs).unwrap_err();
        let reason = err.reason.as_str();
        assert!(reason.starts_with(crate::error::ERROR_SENTINEL));
        assert!(
            reason.contains("DECIMAL"),
            "first failure should be the DECIMAL one, got: {reason}"
        );
    }

    #[test]
    fn decode_positional_params_null_value_becomes_kernel_null() {
        // `value: None` ⇒ `TypedValue::Null` regardless of `sql_type` —
        // matches the connector's VoidParameter shape and pyo3.
        let inputs = vec![TypedValueInput {
            sql_type: "INT".into(),
            value: None,
        }];
        let decoded = decode_positional_params(inputs).expect("null decode succeeds");
        assert_eq!(decoded.len(), 1);
        assert!(matches!(decoded[0], TypedValue::Null));
    }
}
