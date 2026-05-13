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

use crate::errors::to_py_err;
use crate::metadata::Metadata;
use crate::statement::Statement;
use databricks_sql_kernel::{AuthConfig, Session as KSession};
use pyo3::prelude::*;
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
    /// PAT auth only in this phase. `host` may be bare
    /// (`my-workspace.databricks.com`) or fully-qualified
    /// (`https://...`). `http_path` is the SQL endpoint path.
    #[new]
    #[pyo3(signature = (host, http_path, *, access_token, catalog=None, schema=None))]
    fn new(
        py: Python<'_>,
        host: String,
        http_path: String,
        access_token: String,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> PyResult<Self> {
        if access_token.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "access_token must not be empty",
            ));
        }

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
        // Drive it on our runtime; detach the GIL while we wait so
        // other Python threads can run.
        let rt = runtime.clone();
        let open_result = py.detach(|| {
            rt.block_on(async {
                let mut builder =
                    KSession::builder()
                        .http_path(host, http_path)
                        .auth(AuthConfig::Pat {
                            token: access_token,
                        });
                if catalog.is_some() || schema.is_some() {
                    let mut defaults = databricks_sql_kernel::DefaultOpts::default();
                    defaults.catalog = catalog;
                    defaults.schema = schema;
                    builder = builder.defaults(defaults);
                }
                builder.open().await
            })
        });

        let session = open_result.map_err(|e| to_py_err(py, e))?;
        Ok(Self {
            inner: Some(SessionInner { session, runtime }),
        })
    }

    /// Server-issued session id.
    #[getter]
    fn session_id(&self) -> PyResult<String> {
        let inner = self.alive()?;
        Ok(inner.session.session_id().to_owned())
    }

    /// Build a new `Statement` bound to this session.
    fn statement(&self) -> PyResult<Statement> {
        let inner = self.alive()?;
        let kstmt = inner.session.statement();
        Ok(Statement::new(kstmt, inner.runtime.clone()))
    }

    /// Metadata operations bound to this session (flat per-method,
    /// ODBC-shaped). Returns a fresh `Metadata` handle on every call;
    /// the handle is cheap to construct and throw away.
    fn metadata(&self) -> PyResult<Metadata> {
        let inner = self.alive()?;
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
        let close_result = py.detach(|| runtime.block_on(session.close()));
        close_result.map_err(|e| to_py_err(py, e))
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

impl Session {
    fn alive(&self) -> PyResult<&SessionInner> {
        self.inner
            .as_ref()
            .ok_or_else(|| crate::errors::KernelError::new_err("Session is closed"))
    }
}
