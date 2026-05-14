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

use crate::auth::build_auth_config;
use crate::metadata::Metadata;
use crate::statement::Statement;
use databricks_sql_kernel::Session as KSession;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

/// Python-facing session handle.
///
/// `unsendable` — the kernel `Session` is `Send + Sync` (asserted in
/// the kernel's `lib.rs`), but the tokio `Runtime` is `!Send` once it
/// has been driven. Marking the pyclass unsendable keeps Python from
/// trying to migrate it across threads.
#[pyclass(name = "Session", unsendable)]
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
    /// Auth selection:
    ///
    /// - `auth_type="pat"` (default when `access_token` is provided):
    ///   `access_token` carries the bearer token. Kernel uses it
    ///   verbatim for every request.
    /// - `auth_type="external"`: `token_callback` is a no-arg
    ///   Python callable that returns a fresh bearer token. The
    ///   kernel invokes it **once per HTTP request that needs
    ///   authentication**, so the callable MUST be cheap (in-process
    ///   cache lookup is the right pattern; a network round-trip
    ///   inside the callback will multiply request latency
    ///   noticeably under load). The callable owns its own caching
    ///   and refresh — the kernel does not cache the returned
    ///   token across calls. Use this to delegate OAuth / MSAL /
    ///   federation to the connector's existing auth provider.
    ///
    /// Omitting `auth_type` is equivalent to `"pat"` for backward
    /// compatibility with Phase-1 callers that only had PAT.
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
        token_callback=None,
        catalog=None,
        schema=None,
        session_conf=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        host: String,
        http_path: String,
        auth_type: Option<String>,
        access_token: Option<String>,
        token_callback: Option<Py<PyAny>>,
        catalog: Option<String>,
        schema: Option<String>,
        session_conf: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        // Resolve auth_type: default to "pat" only when the caller
        // also passed access_token. This keeps existing PAT callers
        // working without changes while still rejecting ambiguous
        // calls like `Session(host, http_path)` with no auth.
        let resolved_auth_type = match auth_type.as_deref() {
            Some(t) => t.to_owned(),
            None if access_token.is_some() => "pat".to_owned(),
            None => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Session requires auth: pass auth_type='pat' with access_token, \
                     or auth_type='external' with token_callback",
                ));
            }
        };
        let auth_config = build_auth_config(&resolved_auth_type, access_token, token_callback)?;

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
        let rt = runtime.clone();
        let session = run_kernel!(py, rt, async {
            let mut builder = KSession::builder()
                .http_path(host, http_path)
                .auth(auth_config);
            if catalog.is_some() || schema.is_some() {
                let mut defaults = databricks_sql_kernel::DefaultOpts::default();
                defaults.catalog = catalog;
                defaults.schema = schema;
                builder = builder.defaults(defaults);
            }
            if let Some(conf) = session_conf {
                builder = builder.session_conf(conf);
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

    /// Explicitly close the session. Idempotent. Subsequent calls on
    /// this `Session` raise `KernelError`.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        let SessionInner { session, runtime } = inner;
        let rt = runtime;
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
