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

//! Metadata methods on `Connection` — napi-rs layer over the kernel's
//! flat `Metadata` API.
//!
//! Each method mirrors a `databricks_sql_kernel::Metadata` method
//! one-to-one and returns the same `Statement` type that
//! `executeStatement` returns, so the JS side can wrap any result in
//! `KernelOperationBackend` uniformly. The `Statement` is constructed via
//! `Statement::from_result_stream` — no parent kernel `Statement` is
//! involved (metadata calls don't use the mutable `Statement` path).
//!
//! Parameter mapping from JS strings to kernel newtypes follows the
//! pyo3 binding pattern. Empty catalog filters for schemas and columns
//! become empty patterns (match nothing); functions pass an empty catalog
//! through to native `GetFunctions`. Other validation remains kernel-owned
//! and surfaces as `InvalidArgument` through the standard error envelope.
//!
//! ## `!Send` future workaround
//!
//! The kernel's `Metadata` methods hold a `tracing::EnteredSpan`
//! (`!Send`) across their internal `.await` points, making each
//! method's future `!Send`. napi-rs requires the top-level async fn
//! future to be `Send`. The fix: acquire the lock, clone a `Metadata`
//! handle (`Send + Sync` — wraps `Arc<SessionInner>`), release the
//! lock, and then run the `!Send` kernel future inside
//! `tokio::task::spawn_blocking` via `Handle::block_on`. A blocking
//! thread is outside the tokio executor's scheduler, so `block_on` is
//! safe there and the `!Send` constraint does not propagate to the
//! enclosing `Send` async fn.
//!
//! ## Cancel on metadata `Statement`s
//!
//! Cancel is a no-op for metadata streams: the kernel has no
//! in-flight cancellation surface for metadata calls today. This is
//! documented on `Statement::cancel` and tracked as a kernel follow-up
//! for v1.

use std::sync::Arc;

use databricks_sql_kernel::metadata_v0::exact_catalog_and_pattern;
use databricks_sql_kernel::{
    Error as KernelError, Identifier, LikePattern, Metadata, Result as KResult, ResultStream,
};

use crate::connection::{connection_closed_err, Connection};
use crate::error::napi_err_from_kernel;
use crate::statement::Statement;
use crate::util::guarded;

// ─── Helpers ─────────────────────────────────────────────────────────────────
//
// The `Option<String>` → `Option<Identifier>` / `Option<LikePattern>`
// conversion lives on the kernel newtypes as `Identifier::from_opt` /
// `LikePattern::from_opt` (one home shared by all bindings); call sites
// below use those directly and keep the napi error mapping
// (`napi_err_from_kernel`) on the run-on-blocking boundary.

/// Run a `!Send` async kernel metadata operation on a blocking thread.
///
/// `Metadata` itself is `Send + 'static` (wraps `Arc<SessionInner>`).
/// The *futures* its methods return are `!Send` because they hold a
/// `tracing::EnteredSpan` across internal await points. Running them
/// inside `spawn_blocking` moves execution to a thread-pool thread
/// where `Handle::block_on` is safe and the `!Send` constraint doesn't
/// infect the enclosing napi async fn.
async fn run_on_blocking<F, Fut>(f: F) -> napi::Result<ResultStream>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = KResult<ResultStream>>,
{
    // Capture the handle before entering spawn_blocking. Production uses the
    // process-global napi runtime captured by the first async entry point.
    // Hostless Rust tests use their current test runtime so they do not mutate
    // that global and race the cold-cache runtime test.
    #[cfg(not(test))]
    let handle = crate::runtime::get_handle().clone();
    #[cfg(test)]
    let handle = tokio::runtime::Handle::current();
    // Each metadata RPC parks one blocking-pool thread for the whole wire
    // round-trip (the kernel futures are `!Send`; see the doc above). Bracket
    // it with debug breadcrumbs so a "metadata hangs under load" incident — a
    // fan-out exhausting the blocking pool — is localizable from logs without
    // a profiler. (Long-term fix tracked separately: make the metadata futures
    // `Send` via `.instrument(span)` so they can await on a worker thread.)
    tracing::debug!("metadata RPC: entering blocking pool");
    let stream = tokio::task::spawn_blocking(move || handle.block_on(f()))
        .await
        .map_err(|e| {
            napi_err_from_kernel(
                KernelError::internal().message(format!("metadata task join failed: {e}")),
            )
        })?
        .map_err(napi_err_from_kernel)?;
    tracing::debug!("metadata RPC: blocking-pool thread released");
    Ok(stream)
}

/// Acquire the lock, return a `Metadata` handle, and release the lock.
async fn take_metadata(
    inner: &Arc<tokio::sync::Mutex<Option<databricks_sql_kernel::Session>>>,
) -> napi::Result<Metadata> {
    let guard = inner.lock().await;
    let session = guard.as_ref().ok_or_else(connection_closed_err)?;
    Ok(session.metadata())
}

// ─── Metadata impls on Connection ───────────────────────────────────────────

#[napi]
impl Connection {
    /// All catalogs visible to the session.
    ///
    /// JDBC `getCatalogs` shape: `TABLE_CAT: Utf8`.
    #[napi]
    pub async fn list_catalogs(&self) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move { meta.list_catalogs().await }).await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Schemas filtered by catalog (exact) and schema name pattern. An empty
    /// catalog matches nothing; non-empty catalogs remain exact identifiers.
    ///
    /// JDBC `getSchemas` shape: `TABLE_SCHEM, TABLE_CATALOG`.
    #[napi]
    pub async fn list_schemas(
        &self,
        catalog: Option<String>,
        schema_pattern: Option<String>,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let (cat, pat) = exact_catalog_and_pattern(&catalog, &schema_pattern)?;
                meta.list_schemas(cat, pat).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Tables filtered by catalog (**pattern**), schema (pattern), table
    /// (pattern).
    ///
    /// The catalog is an ODBC/JDBC LIKE pattern (`%` / `_`), matching
    /// Thrift `getTables`: a wildcard catalog matches multiple catalogs;
    /// a literal name (or an escaped `\_` / `\%`) takes the fast exact
    /// path. `undefined`/omitted catalog means "all catalogs", while an
    /// **empty string** means "match nothing" (zero rows) — pass
    /// `undefined`, not `""`, for all catalogs. The catalog pattern is
    /// validated (whitespace-only / NUL / >255 bytes are rejected with an
    /// error).
    ///
    /// JDBC `getTables` shape: 10 columns. `tableTypes`, when provided,
    /// filters rows by `TABLE_TYPE` kernel-side.
    ///
    /// `tableTypes` is an advisory filter. Databricks `SHOW TABLES` does
    /// NOT honour the table-type filter server-side; the kernel applies
    /// it client-side after the result returns. Callers expecting
    /// server-side rejection of off-type tables should not rely on this.
    #[napi]
    pub async fn list_tables(
        &self,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        table_pattern: Option<String>,
        table_types: Option<Vec<String>>,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let cat = LikePattern::from_opt(&catalog)?;
                let sch = LikePattern::from_opt(&schema_pattern)?;
                let tbl = LikePattern::from_opt(&table_pattern)?;
                let types_owned: Option<Vec<&str>> = table_types
                    .as_ref()
                    .map(|v| v.iter().map(String::as_str).collect());
                let types_slice: Option<&[&str]> = types_owned.as_deref();
                meta.list_tables(cat, sch, tbl, types_slice).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Columns of tables matching the filter. An empty catalog matches
    /// nothing; non-empty catalogs remain exact identifiers.
    ///
    /// JDBC `getColumns` shape: 23 columns.
    #[napi]
    pub async fn list_columns(
        &self,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        table_pattern: Option<String>,
        column_pattern: Option<String>,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let (cat, sch) = exact_catalog_and_pattern(&catalog, &schema_pattern)?;
                let tbl = LikePattern::from_opt(&table_pattern)?;
                let col = LikePattern::from_opt(&column_pattern)?;
                meta.list_columns(cat, sch, tbl, col).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Functions visible to the session. An empty `catalog` is forwarded to
    /// SEA for native `GetFunctions`; `schemaPattern` and `functionPattern` are SQL LIKE.
    #[napi]
    pub async fn list_functions(
        &self,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        function_pattern: Option<String>,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let cat = catalog
                    .as_deref()
                    .map(Identifier::new_allow_empty)
                    .transpose()?;
                let sch = LikePattern::from_opt(&schema_pattern)?;
                let nam = LikePattern::from_opt(&function_pattern)?;
                meta.functions(cat, sch, nam).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Procedures visible to the session. `catalog` is exact;
    /// `schemaPattern` and `procedurePattern` are SQL LIKE.
    #[napi]
    pub async fn list_procedures(
        &self,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        procedure_pattern: Option<String>,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let cat = Identifier::from_opt(&catalog)?;
                let sch = LikePattern::from_opt(&schema_pattern)?;
                let nam = LikePattern::from_opt(&procedure_pattern)?;
                meta.procedures(cat, sch, nam).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// All table types (`TABLE`, `VIEW`, `SYSTEM TABLE`, …).
    /// No wire call — static in-memory result.
    #[napi]
    pub async fn list_table_types(&self) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move { meta.table_types().await }).await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// SQL data types supported by the workspace.
    /// No wire call — static in-memory result.
    #[napi]
    pub async fn list_type_info(&self) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move { meta.type_info().await }).await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Primary keys for the given table. All three identifiers are
    /// exact — ODBC `SQLPrimaryKeys` does not support patterns.
    #[napi]
    pub async fn get_primary_keys(
        &self,
        catalog: String,
        schema: String,
        table: String,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let cat = Some(Identifier::new(&catalog)?);
                let sch = Some(Identifier::new(&schema)?);
                let tbl = Identifier::new(&table)?;
                meta.primary_keys(cat, sch, tbl).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Foreign-key relationships. The parent side is optional. When the
    /// foreign table is omitted, returns an empty result without issuing a
    /// server statement. When it is provided, its catalog and schema are
    /// required. All identifiers are exact — no LIKE patterns.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn get_cross_reference(
        &self,
        parent_catalog: Option<String>,
        parent_schema: Option<String>,
        parent_table: Option<String>,
        foreign_catalog: Option<String>,
        foreign_schema: Option<String>,
        foreign_table: Option<String>,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let pc = Identifier::from_opt(&parent_catalog)?;
                let ps = Identifier::from_opt(&parent_schema)?;
                let pt = Identifier::from_opt(&parent_table)?;
                let fc = Identifier::from_opt(&foreign_catalog)?;
                let fs = Identifier::from_opt(&foreign_schema)?;
                let ft = Identifier::from_opt(&foreign_table)?;
                meta.foreign_keys(pc, ps, pt, fc, fs, ft).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use databricks_sql_kernel::{AuthConfig, ConnectionConfig, ErrorCode, Session};
    use serde_json::json;
    use tokio::sync::Mutex;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // The `Option<String>` → kernel-newtype conversion now lives on the
    // kernel as `Identifier::from_opt` / `LikePattern::from_opt` (tested
    // exhaustively there). These thin smoke tests pin that the binding
    // calls through to the same contract — whitespace-only rejects as
    // `InvalidArgument`, `None` passes through as `Ok(None)`.

    #[test]
    fn identifier_from_opt_rejects_whitespace_only() {
        let input = Some("  ".to_owned());
        let err = Identifier::from_opt(&input).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn identifier_from_opt_none_is_ok_none() {
        let input: Option<String> = None;
        assert!(Identifier::from_opt(&input).unwrap().is_none());
    }

    #[test]
    fn like_pattern_from_opt_rejects_whitespace_only() {
        let input = Some("  ".to_owned());
        let err = LikePattern::from_opt(&input).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn like_pattern_from_opt_none_is_ok_none() {
        let input: Option<String> = None;
        assert!(LikePattern::from_opt(&input).unwrap().is_none());
    }

    #[tokio::test]
    async fn metadata_short_circuits_return_empty_without_statement_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "session_id": "sess-napi-metadata" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-napi-metadata"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid HTTP path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .open()
            .await
            .expect("session opens against mock server");
        let connection = Connection {
            inner: Arc::new(Mutex::new(Some(session))),
            session_id: "sess-napi-metadata".to_owned(),
        };

        let statements = [
            connection
                .list_schemas(Some(String::new()), None)
                .await
                .expect("empty catalog returns empty schemas"),
            connection
                .list_columns(Some(String::new()), None, None, None)
                .await
                .expect("empty catalog returns empty columns"),
            connection
                .get_cross_reference(
                    Some("main".to_owned()),
                    Some("default".to_owned()),
                    Some("parent".to_owned()),
                    None,
                    None,
                    None,
                )
                .await
                .expect("missing foreign table returns empty cross references"),
        ];
        for statement in statements {
            assert!(statement
                .fetch_next_batch()
                .await
                .expect("empty metadata stream fetch succeeds")
                .is_none());
            statement.close().await.expect("statement closes");
        }

        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            requests
                .iter()
                .filter(|request| {
                    request.method.as_str() == "POST"
                        && request.url.path() == "/api/2.0/sql/statements"
                })
                .count(),
            0,
            "metadata short-circuits must not execute a statement"
        );

        connection.close().await.expect("connection closes");
    }
}
