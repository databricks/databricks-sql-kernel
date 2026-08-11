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

//! `Session` — Python class wrapping the kernel's `Session`.
//!
//! Owns a multi-thread tokio runtime per session (one tokio worker
//! pool drives all I/O for this session). The runtime handle is
//! cloned into child `Statement` / `ExecutedStatement` objects so
//! they `block_on` the same pool without keeping the `Session`
//! pyclass alive longer than the user keeps a reference.

use crate::auth::{build_auth_config, OAuthParams};
use crate::executed_async::ExecutedAsyncStatement;
use crate::metadata::Metadata;
use crate::statement::Statement;
use databricks_sql_kernel::Session as KSession;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

/// Python-facing session handle.
///
/// Both fields of `SessionInner` are `Send + Sync` (the kernel
/// `Session` asserts it in `lib.rs`; `tokio::runtime::Runtime` is
/// documented `Send + Sync`), so the pyclass is **not** `unsendable`.
/// This lets Python use the same `Session` from multiple threads —
/// the legitimate pattern for `databricks-sql-python` connections
/// shared across a worker pool. The `Send + Sync` invariant is
/// regression-asserted in the test module at the bottom of this file.
#[pyclass(name = "Session")]
pub(crate) struct Session {
    /// `None` after `close()`. Methods return a clean Python error
    /// rather than panicking on a closed session.
    inner: Option<SessionInner>,
}

pub(crate) struct SessionInner {
    pub(crate) session: KSession,
    pub(crate) runtime: Arc<Runtime>,
}

#[pymethods]
impl Session {
    /// Open a new session against the given workspace.
    ///
    /// `host` may be bare (`my-workspace.databricks.com`) or
    /// fully-qualified (`https://...`). `http_path` is the SQL
    /// endpoint path.
    ///
    /// Auth: pass `auth_type` plus the matching credentials.
    ///
    /// - `"pat"` (or omit `auth_type` and pass `access_token`):
    ///   personal access token.
    /// - `"oauth-m2m"`: service-principal client credentials —
    ///   `client_id` + `client_secret` (optional `oauth_scopes`,
    ///   `token_url`).
    /// - `"oauth-u2m"`: browser authorization-code + PKCE flow
    ///   (optional `client_id`, `redirect_port`, `oauth_scopes`,
    ///   `token_url`, `oauth_callback_timeout_secs`).
    ///
    /// OAuth is kernel-owned: the kernel acquires and transparently
    /// refreshes tokens itself.
    ///
    /// `identity_federation_client_id` is optional and enables SP-wide
    /// Workload Identity Federation for mandatory OIDC token exchange:
    /// when the authenticated token is issued by a party other than the
    /// target workspace, the kernel exchanges it for an in-house
    /// Databricks token, sending this client id. Omit it for BYOT /
    /// account-wide WIF. It is not a feature toggle — the exchange is
    /// always attempted, gated only by the token's issuer.
    ///
    /// TLS: all knobs are optional relaxations / additions on top of
    /// the secure default (validate system trust store, verify chain
    /// + hostname). `tls_ca_cert` adds a PEM CA bundle (bytes);
    /// `tls_skip_verify` disables all chain validation;
    /// `tls_skip_hostname_verify` disables the hostname check;
    /// `tls_client_cert` + `tls_client_key` (both PEM bytes) configure
    /// mutual TLS. The two mTLS fields must be set together.
    ///
    /// Retry/backoff: all optional, default to the kernel's built-in
    /// policy (1s/60s backoff, 6 total attempts, 900s budget) when
    /// unset. `retry_min_wait_secs` / `retry_max_wait_secs` bound the
    /// exponential backoff (and clamp a server `Retry-After`);
    /// `retry_max_attempts` is the **total** number of attempts
    /// (converted to the kernel's retries-after-first internally, so
    /// `0`/`1` both mean a single attempt); `retry_overall_timeout_secs`
    /// caps cumulative time across retries. These apply to SQL +
    /// CloudFetch requests; token-endpoint (auth) calls keep a fixed
    /// fast-fail policy.
    ///
    /// `http_headers` is a list of `(name, value)` pairs sent on every
    /// request — used to forward the connector's caller headers (which
    /// also carry the composed `User-Agent`). The kernel applies them
    /// before its own `Authorization` / `x-databricks-org-id` headers,
    /// which always take precedence; a caller header for either of those
    /// names is ignored.
    ///
    /// `session_conf` is the server-bound session configuration map
    /// — passed through to SEA `session_confs` verbatim. Use for
    /// `ANSI_MODE`, `TIMEZONE`, `STATEMENT_TIMEOUT`, query-tag
    /// presets, etc. The kernel does not validate keys; the server
    /// rejects unknown ones with a `KernelError(code='SqlError')`
    /// when it picks them up.
    #[new]
    #[pyo3(signature = (
        host,
        http_path,
        *,
        auth_type=None,
        access_token=None,
        client_id=None,
        client_secret=None,
        oauth_scopes=None,
        token_url=None,
        redirect_port=None,
        oauth_callback_timeout_secs=None,
        identity_federation_client_id=None,
        tls_ca_cert=None,
        tls_skip_verify=false,
        tls_skip_hostname_verify=false,
        tls_client_cert=None,
        tls_client_key=None,
        retry_min_wait_secs=None,
        retry_max_wait_secs=None,
        retry_max_attempts=None,
        retry_overall_timeout_secs=None,
        http_headers=None,
        catalog=None,
        schema=None,
        session_conf=None,
        complex_types_as_json=false,
        intervals_as_string=false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        host: String,
        http_path: String,
        auth_type: Option<String>,
        access_token: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        oauth_scopes: Option<Vec<String>>,
        token_url: Option<String>,
        redirect_port: Option<u16>,
        oauth_callback_timeout_secs: Option<u64>,
        identity_federation_client_id: Option<String>,
        tls_ca_cert: Option<Vec<u8>>,
        tls_skip_verify: bool,
        tls_skip_hostname_verify: bool,
        tls_client_cert: Option<Vec<u8>>,
        tls_client_key: Option<Vec<u8>>,
        retry_min_wait_secs: Option<u64>,
        retry_max_wait_secs: Option<u64>,
        retry_max_attempts: Option<u32>,
        retry_overall_timeout_secs: Option<u64>,
        http_headers: Option<Vec<(String, String)>>,
        catalog: Option<String>,
        schema: Option<String>,
        session_conf: Option<HashMap<String, String>>,
        complex_types_as_json: bool,
        intervals_as_string: bool,
    ) -> PyResult<Self> {
        // Resolve auth_type. Explicit `auth_type` always wins. When
        // omitted, default to "pat" if `access_token` was passed
        // (back-compat for existing PAT callers). A bare
        // `Session(host, http_path)` with no auth is still rejected.
        let resolved_auth_type = match auth_type.as_deref() {
            Some(t) => t.to_owned(),
            None if access_token.is_some() => "pat".to_owned(),
            None => {
                // Boundary-only breadcrumb: this rejection happens in
                // pyo3 *before* any kernel code runs, so the kernel emits
                // nothing. Without this, a customer who turned on DEBUG to
                // chase a connection failure would see an empty log.
                tracing::debug!(
                    target: crate::logging::PYO3_LOG_TARGET,
                    "Session.__new__ rejected: no auth credentials provided"
                );
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Session requires auth: pass access_token (PAT) or set \
                     auth_type to 'oauth-m2m' / 'oauth-u2m' with the matching \
                     credentials",
                ));
            }
        };
        let oauth = OAuthParams {
            client_id,
            client_secret,
            scopes: oauth_scopes,
            token_url,
            redirect_port,
            callback_timeout_secs: oauth_callback_timeout_secs,
        };
        let auth_config = build_auth_config(&resolved_auth_type, access_token, oauth)?;

        // Build a TLS config only when the caller relaxed or extended
        // the secure default — otherwise leave the builder on its
        // default (full validation) so we don't allocate a config that
        // changes nothing.
        let tls_config = if tls_ca_cert.is_some()
            || tls_skip_verify
            || tls_skip_hostname_verify
            || tls_client_cert.is_some()
            || tls_client_key.is_some()
        {
            let mut tls = databricks_sql_kernel::TlsConfig::default();
            tls.custom_ca_cert = tls_ca_cert;
            tls.accept_self_signed = tls_skip_verify;
            tls.skip_hostname_verification = tls_skip_hostname_verify;
            tls.client_cert_pem = tls_client_cert;
            tls.client_key_pem = tls_client_key;
            Some(tls)
        } else {
            None
        };

        // Build an HttpConfig only when the caller tuned retry/backoff
        // or supplied custom headers; otherwise leave the builder on its
        // defaults (1s/60s/5-retries, no extra headers).
        //
        // `retry_max_attempts` is the **total** number of attempts
        // (matching the connector's `_retry_stop_after_attempts_count`
        // and JDBC's count semantics). The kernel's `retry_max_retries`
        // counts retries *after* the first attempt, so we convert with
        // `max(0, attempts - 1)` here — centralising the off-by-one so
        // wrappers can pass an intuitive total. `retry_max_attempts = 0`
        // or `1` both mean "one attempt, no retries".
        //
        // `http_headers` are extra headers sent on every request (the
        // connector forwards its `http_headers`, which also carries the
        // composed `User-Agent`). The kernel applies them before its own
        // `Authorization` / `x-databricks-org-id`, which always win.
        let has_http_headers = http_headers.as_ref().is_some_and(|h| !h.is_empty());
        let http_config = if retry_min_wait_secs.is_some()
            || retry_max_wait_secs.is_some()
            || retry_max_attempts.is_some()
            || retry_overall_timeout_secs.is_some()
            || has_http_headers
        {
            let mut http = databricks_sql_kernel::HttpConfig::default();
            if let Some(s) = retry_min_wait_secs {
                http.retry_min_wait = std::time::Duration::from_secs(s);
            }
            if let Some(s) = retry_max_wait_secs {
                http.retry_max_wait = std::time::Duration::from_secs(s);
            }
            if let Some(attempts) = retry_max_attempts {
                http.retry_max_retries = attempts.saturating_sub(1);
            }
            if let Some(s) = retry_overall_timeout_secs {
                http.overall_timeout = std::time::Duration::from_secs(s);
            }
            if let Some(headers) = http_headers {
                http.custom_headers = headers;
            }
            Some(http)
        } else {
            None
        };

        // One tokio runtime per session. Multi-thread so CloudFetch
        // chunk downloads can overlap with the main poll loop. Worker
        // count left to tokio's default (logical CPU count).
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .enable_all()
                .thread_name("dbx-sql-kernel")
                .build()
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to build tokio runtime: {e}"
                    ))
                })?,
        );

        // `Session::open` is async and grabs the current tokio handle.
        // Drive it on the runtime; the macro detaches the GIL for the
        // I/O and maps kernel errors into `KernelError`.
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Session.__new__: opening kernel session (GIL released)"
        );
        let rt = runtime.clone();
        let session = run_kernel!(py, rt, async {
            let mut builder = KSession::builder()
                .http_path(host, http_path)
                .auth(auth_config);
            if let Some(client_id) = identity_federation_client_id {
                builder = builder.identity_federation_client_id(client_id);
            }
            if let Some(tls) = tls_config {
                builder = builder.tls(tls);
            }
            if let Some(http) = http_config {
                builder = builder.http(http);
            }
            if catalog.is_some() || schema.is_some() {
                let mut defaults = databricks_sql_kernel::DefaultOpts::default();
                defaults.catalog = catalog;
                defaults.schema = schema;
                builder = builder.defaults(defaults);
            }
            if let Some(conf) = session_conf {
                builder = builder.session_conf(conf);
            }
            // Merge result-config knobs into one `ResultConfig` so two
            // toggles don't clobber each other when both are set.
            if complex_types_as_json || intervals_as_string {
                let mut result = databricks_sql_kernel::ResultConfig::default();
                result.complex_types_as_json = complex_types_as_json;
                result.intervals_as_string = intervals_as_string;
                builder = builder.result(result);
            }
            builder.open().await
        })?;

        Ok(Self {
            inner: Some(SessionInner { session, runtime }),
        })
    }

    /// Server-issued session id.
    #[getter]
    fn session_id(&self) -> PyResult<String> {
        let inner = alive!(self, "Session");
        Ok(inner.session.session_id().to_owned())
    }

    /// Build a new `Statement` bound to this session.
    fn statement(&self) -> PyResult<Statement> {
        let inner = alive!(self, "Session");
        let kstmt = inner.session.statement();
        Ok(Statement::new(kstmt, inner.runtime.clone()))
    }

    /// Metadata operations bound to this session (flat per-method,
    /// ODBC-shaped). Returns a fresh `Metadata` handle on every call;
    /// the handle is cheap to construct and throw away.
    fn metadata(&self) -> PyResult<Metadata> {
        let inner = alive!(self, "Session");
        let kmeta = inner.session.metadata();
        Ok(Metadata::new(kmeta, inner.runtime.clone()))
    }

    /// Re-attach to an already-submitted server statement by its id,
    /// returning an `ExecutedAsyncStatement` the caller can poll
    /// (`status()`), await (`await_result()`), or cancel/close — without
    /// having been the handle that submitted it.
    ///
    /// This is the resume path the connector uses for
    /// `get_query_state` / `get_async_execution_result`: server
    /// statement state is keyed solely by the id, so a caller holding
    /// only the id can recover a working handle bound to this session.
    ///
    /// Issues one `GetStatementStatus` to seed the handle. Raises
    /// `KernelError` with code `NotFound` if the server doesn't
    /// recognise the id (never submitted, or its result TTL elapsed). A
    /// *closed* statement is not an error — the server keeps reporting
    /// `CLOSED` (and terminal `SUCCEEDED`/`FAILED`) until GC, so the
    /// returned handle's `status()` surfaces the real server state.
    fn attach_async_statement(
        &self,
        py: Python<'_>,
        statement_id: String,
    ) -> PyResult<ExecutedAsyncStatement> {
        let inner = alive!(self, "Session");
        let runtime = inner.runtime.clone();
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Session.attach_async_statement: re-attaching by id (GIL released)"
        );
        let exec = run_kernel!(py, runtime, inner.session.attach_async_statement(statement_id))?;
        Ok(ExecutedAsyncStatement::new(exec, runtime))
    }

    /// Explicitly close the session. Idempotent. Subsequent calls on
    /// this `Session` raise `KernelError`.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        let SessionInner { session, runtime } = inner;
        let rt = runtime;
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Session.close: closing kernel session (GIL released)"
        );
        run_kernel!(py, rt, session.close())?;
        Ok(())
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            Some(i) => format!(
                "Session(session_id={:?}, closed=False)",
                i.session.session_id()
            ),
            None => "Session(closed=True)".to_owned(),
        }
    }
}

#[cfg(test)]
mod send_sync_tests {
    use super::*;
    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn runtime_arc_is_send_sync() {
        assert_send_sync::<Arc<Runtime>>();
        assert_send_sync::<SessionInner>();
    }
}
