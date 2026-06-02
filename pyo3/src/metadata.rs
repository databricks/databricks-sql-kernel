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

//! `Metadata` — Python wrapper for the kernel's flat metadata API.
//!
//! Mirrors `kernel::Metadata` one-to-one. Each method blocks on the
//! session's tokio runtime, returns a `ResultStream` that the caller
//! drains the same way it drains `ExecutedStatement` results.
//!
//! The kernel splits exact identifiers (`Identifier`) from LIKE
//! patterns (`LikePattern`) at the type level. The Python surface
//! collapses both back to `Option[str]` because Python lacks a
//! convenient way to express two distinct string-flavoured types at
//! a call site. The kernel's runtime checks (empty, NUL, backticks
//! for identifiers; length caps for both) still apply; bad input
//! surfaces as `KernelError(code='InvalidArgument', ...)`.
//!
//! ## Why the methods aren't fully macro-ised
//!
//! The 10 metadata methods share an outer shape (release GIL, run
//! future on session runtime, wrap result in `ResultStream`), which
//! `run_kernel_block!` collapses. They differ in:
//!
//! - which positional args are required vs optional,
//! - which args are exact identifiers vs LIKE patterns,
//! - the `list_tables` extra `table_types: Option<Vec<String>>`.
//!
//! A declarative macro that covered all the variations would need
//! its own DSL and would be harder to read than the explicit forms.
//! The outer-scaffolding macro is where the win lives; the inner
//! conversions stay explicit and grep-able.

use crate::result_stream::ResultStream;
use databricks_sql_kernel::{Identifier, LikePattern, Metadata as KMetadata};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Python-facing metadata handle.
///
/// Constructed via `Session.metadata()`. Cheap to construct and
/// throw away — the underlying kernel `Metadata` is a thin wrapper
/// around the shared session state.
#[pyclass(name = "Metadata")]
pub(crate) struct Metadata {
    inner: KMetadata,
    runtime: Arc<Runtime>,
}

impl Metadata {
    pub(crate) fn new(inner: KMetadata, runtime: Arc<Runtime>) -> Self {
        Self { inner, runtime }
    }
}

// The `Option<String>` → `Option<Identifier>` / `Option<LikePattern>`
// conversion lives on the kernel newtypes as `Identifier::from_opt` /
// `LikePattern::from_opt` (one home shared by all bindings, with empty /
// NUL / backtick / whitespace rejection kernel-side surfacing as
// `InvalidArgument`). Call sites below use those directly.

#[pymethods]
impl Metadata {
    /// All catalogs visible to the session.
    fn list_catalogs(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, { self.inner.list_catalogs().await })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// Schemas filtered by catalog (exact) and schema name pattern.
    #[pyo3(signature = (*, catalog=None, schema_pattern=None))]
    fn list_schemas(
        &self,
        py: Python<'_>,
        catalog: Option<String>,
        schema_pattern: Option<String>,
    ) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, {
            let cat = Identifier::from_opt(&catalog)?;
            let pat = LikePattern::from_opt(&schema_pattern)?;
            self.inner.list_schemas(cat, pat).await
        })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// Tables filtered by catalog (exact), schema (pattern), table
    /// (pattern). `table_types` is advisory — Databricks does not
    /// honour it server-side; wrappers filter the returned
    /// `tableType` column themselves.
    #[pyo3(signature = (*, catalog=None, schema_pattern=None, table_pattern=None, table_types=None))]
    fn list_tables(
        &self,
        py: Python<'_>,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        table_pattern: Option<String>,
        table_types: Option<Vec<String>>,
    ) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, {
            let cat = Identifier::from_opt(&catalog)?;
            let sch = LikePattern::from_opt(&schema_pattern)?;
            let tbl = LikePattern::from_opt(&table_pattern)?;
            let types_owned: Option<Vec<&str>> = table_types
                .as_ref()
                .map(|v| v.iter().map(String::as_str).collect());
            let types_slice: Option<&[&str]> = types_owned.as_deref();
            self.inner.list_tables(cat, sch, tbl, types_slice).await
        })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// Columns of tables matching the filter. `catalog = None` emits
    /// `SHOW COLUMNS IN ALL CATALOGS`; otherwise scoped to the named
    /// catalog. Schema / table / column patterns are SQL LIKE.
    #[pyo3(signature = (*, catalog=None, schema_pattern=None, table_pattern=None, column_pattern=None))]
    fn list_columns(
        &self,
        py: Python<'_>,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        table_pattern: Option<String>,
        column_pattern: Option<String>,
    ) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, {
            let cat = match catalog.as_deref() {
                Some(s) => Some(Identifier::new(s)?),
                None => None,
            };
            let sch = LikePattern::from_opt(&schema_pattern)?;
            let tbl = LikePattern::from_opt(&table_pattern)?;
            let col = LikePattern::from_opt(&column_pattern)?;
            self.inner.list_columns(cat, sch, tbl, col).await
        })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// Primary keys for the given table. All identifiers are exact.
    #[pyo3(signature = (*, catalog, schema, table))]
    fn primary_keys(
        &self,
        py: Python<'_>,
        catalog: String,
        schema: String,
        table: String,
    ) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, {
            let cat = Some(Identifier::new(&catalog)?);
            let sch = Some(Identifier::new(&schema)?);
            let tbl = Identifier::new(&table)?;
            self.inner.primary_keys(cat, sch, tbl).await
        })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// Foreign-key relationships. The foreign side must be fully
    /// specified (catalog + schema + table); the parent side is
    /// optional.
    #[pyo3(signature = (*,
        parent_catalog=None, parent_schema=None, parent_table=None,
        foreign_catalog, foreign_schema, foreign_table))]
    #[allow(clippy::too_many_arguments)]
    fn foreign_keys(
        &self,
        py: Python<'_>,
        parent_catalog: Option<String>,
        parent_schema: Option<String>,
        parent_table: Option<String>,
        foreign_catalog: String,
        foreign_schema: String,
        foreign_table: String,
    ) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, {
            let pc = Identifier::from_opt(&parent_catalog)?;
            let ps = Identifier::from_opt(&parent_schema)?;
            let pt = Identifier::from_opt(&parent_table)?;
            let fc = Some(Identifier::new(&foreign_catalog)?);
            let fs = Some(Identifier::new(&foreign_schema)?);
            let ft = Some(Identifier::new(&foreign_table)?);
            self.inner.foreign_keys(pc, ps, pt, fc, fs, ft).await
        })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// Functions visible to the session.
    #[pyo3(signature = (*, catalog=None, schema_pattern=None, name_pattern=None))]
    fn functions(
        &self,
        py: Python<'_>,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        name_pattern: Option<String>,
    ) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, {
            let cat = Identifier::from_opt(&catalog)?;
            let sch = LikePattern::from_opt(&schema_pattern)?;
            let nam = LikePattern::from_opt(&name_pattern)?;
            self.inner.functions(cat, sch, nam).await
        })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// Procedures visible to the session.
    #[pyo3(signature = (*, catalog=None, schema_pattern=None, name_pattern=None))]
    fn procedures(
        &self,
        py: Python<'_>,
        catalog: Option<String>,
        schema_pattern: Option<String>,
        name_pattern: Option<String>,
    ) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, {
            let cat = Identifier::from_opt(&catalog)?;
            let sch = LikePattern::from_opt(&schema_pattern)?;
            let nam = LikePattern::from_opt(&name_pattern)?;
            self.inner.procedures(cat, sch, nam).await
        })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// SQL data types supported by the workspace. No filters; no
    /// wire call (static in-memory result).
    fn type_info(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, { self.inner.type_info().await })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }

    /// All table types. No wire call.
    fn table_types(&self, py: Python<'_>) -> PyResult<ResultStream> {
        let stream = run_kernel_block!(py, self.runtime, { self.inner.table_types().await })?;
        Ok(ResultStream::new(stream, self.runtime.clone()))
    }
}
