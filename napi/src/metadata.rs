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
//! `SeaOperationBackend` uniformly. The `Statement` is constructed via
//! `Statement::from_result_stream` — no parent kernel `Statement` is
//! involved (metadata calls don't use the mutable `Statement` path).
//!
//! Parameter mapping from JS strings to kernel newtypes follows the
//! pyo3 binding pattern: `Option<String>` → `Option<Identifier<'_>>`
//! or `Option<LikePattern<'_>>`, with runtime validation left to the
//! kernel. Rejected inputs surface as `InvalidArgument` via the
//! standard kernel-error envelope.
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

use databricks_sql_kernel::{
    Error as KernelError, Identifier, LikePattern, Metadata, Result as KResult, ResultStream,
};

use crate::connection::{connection_closed_err, Connection};
use crate::error::napi_err_from_kernel;
use crate::runtime::get_handle;
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
    // Capture the handle before entering spawn_blocking; get_handle()
    // must be called from inside an async context (it snapshots
    // Handle::current() on first call).
    let handle = get_handle().clone();
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

    /// Schemas filtered by catalog (exact) and schema name pattern.
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
                let cat = Identifier::from_opt(&catalog)?;
                let pat = LikePattern::from_opt(&schema_pattern)?;
                meta.list_schemas(cat, pat).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Tables filtered by catalog (exact), schema (pattern), table (pattern).
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
                let cat = Identifier::from_opt(&catalog)?;
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

    /// Columns of tables matching the filter.
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
                let cat = Identifier::from_opt(&catalog)?;
                let sch = LikePattern::from_opt(&schema_pattern)?;
                let tbl = LikePattern::from_opt(&table_pattern)?;
                let col = LikePattern::from_opt(&column_pattern)?;
                meta.list_columns(cat, sch, tbl, col).await
            })
            .await?;
            Ok(Statement::from_result_stream(stream))
        })
        .await
    }

    /// Functions visible to the session. `catalog` is exact;
    /// `schemaPattern` and `functionPattern` are SQL LIKE.
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
                let cat = Identifier::from_opt(&catalog)?;
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

    /// Foreign-key relationships. The foreign side must be fully
    /// specified (catalog + schema + table); the parent side is
    /// optional. All identifiers are exact — no LIKE patterns.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn get_cross_reference(
        &self,
        parent_catalog: Option<String>,
        parent_schema: Option<String>,
        parent_table: Option<String>,
        foreign_catalog: String,
        foreign_schema: String,
        foreign_table: String,
    ) -> napi::Result<Statement> {
        let meta = take_metadata(&self.inner).await?;
        guarded(async move {
            let stream = run_on_blocking(move || async move {
                let pc = Identifier::from_opt(&parent_catalog)?;
                let ps = Identifier::from_opt(&parent_schema)?;
                let pt = Identifier::from_opt(&parent_table)?;
                let fc = Some(Identifier::new(&foreign_catalog)?);
                let fs = Some(Identifier::new(&foreign_schema)?);
                let ft = Some(Identifier::new(&foreign_table)?);
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
    use databricks_sql_kernel::ErrorCode;

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
}
