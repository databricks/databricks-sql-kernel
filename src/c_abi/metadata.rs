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

//! Metadata FFI — one C entry point per `Metadata` method. Each returns
//! a `kernel_result_stream_t` the caller drives via
//! `kernel_result_stream_*`.
//!
//! Each call captures the result schema and consumes the stream into an
//! owned batch iterator (metadata results are bounded — a handful of
//! batches at most), so the returned handle is self-contained and does
//! not borrow the session. The session must still outlive the call
//! itself.
//!
//! Results carry the **server-shaped** (JDBC-canonical) columns the
//! kernel's `Metadata` produces; ODBC / JDBC drivers keep their own
//! rename / reshape layer on top.

use super::result::{into_raw_result_stream, kernel_result_stream_t};
use super::session::kernel_session_t;
use super::{block_on_ffi, KernelStatusCode};
use crate::kernel_error::{Error, Result};
use crate::metadata_v0::{Identifier, LikePattern};
use crate::result_stream::{OwnedBatchIter, ResultStream, SchemaRef};
use crate::session::Session;
use std::ffi::{c_char, CStr};

/// Read an optional C string: null → `None`, else borrowed `&str`
/// (errors on non-UTF-8).
///
/// # Safety
/// `ptr`, if non-null, must point to a NUL-terminated C string.
unsafe fn opt_str<'a>(ptr: *const c_char, what: &str) -> Result<Option<&'a str>> {
    if ptr.is_null() {
        return Ok(None);
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map(Some)
        .map_err(|_| Error::invalid_argument().message(format!("{what} is not valid UTF-8")))
}

/// Read a required C string as a borrowed `&str`.
///
/// # Safety
/// As [`opt_str`].
unsafe fn req_str<'a>(ptr: *const c_char, what: &str) -> Result<&'a str> {
    opt_str(ptr, what)?
        .ok_or_else(|| Error::invalid_argument().message(format!("{what} must not be null")))
}

/// Optional [`Identifier`] from a C string (null → `None`).
unsafe fn opt_ident<'a>(ptr: *const c_char, what: &str) -> Result<Option<Identifier<'a>>> {
    match opt_str(ptr, what)? {
        Some(s) => Ok(Some(Identifier::new(s)?)),
        None => Ok(None),
    }
}

/// Optional [`LikePattern`] from a C string (null → `None`).
unsafe fn opt_pattern<'a>(ptr: *const c_char, what: &str) -> Result<Option<LikePattern<'a>>> {
    match opt_str(ptr, what)? {
        Some(s) => Ok(Some(LikePattern::new(s)?)),
        None => Ok(None),
    }
}

/// Capture a `ResultStream`'s schema and consume it into an owned batch
/// iterator, ready to wrap in a `kernel_result_stream_t`.
fn into_parts(stream: ResultStream) -> (SchemaRef, OwnedBatchIter) {
    let schema = stream.schema();
    (schema, stream.into_owned_batches())
}

/// Shared body for the metadata entry points: clear the error, null-check
/// `session` / `out`, run the async builder on the runtime, and write the
/// resulting owned stream handle.
///
/// # Safety
/// `session` must be a live session handle; `out` must be writable.
unsafe fn run_metadata<F>(
    session: *mut kernel_session_t,
    out: *mut *mut kernel_result_stream_t,
    build: F,
) -> KernelStatusCode
where
    F: FnOnce(
        &Session,
    )
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ResultStream>> + '_>>,
{
    crate::c_abi::error::clear_error();
    let Some(session) = (session as *const Session).as_ref() else {
        return store_null("metadata: session must not be null");
    };
    if out.is_null() {
        return store_null("metadata: out must not be null");
    }
    let result = block_on_ffi(move || async move {
        let stream = build(session).await?;
        Ok(into_parts(stream))
    });
    match result {
        Ok((schema, iter)) => {
            std::ptr::write(out, into_raw_result_stream(schema, iter));
            KernelStatusCode::Success
        }
        Err(code) => code,
    }
}

/// List the catalogs visible to the session.
///
/// # Safety
/// `session` must be a live handle from `kernel_session_open`; `out`
/// must be a valid, writable pointer.
#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_catalogs(
    session: *mut kernel_session_t,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    run_metadata(session, out, |session| {
        Box::pin(async move { session.metadata().list_catalogs().await })
    })
}

/// List schemas filtered by catalog (exact) and schema-name pattern.
///
/// # Safety
/// `session` must be a live session handle; the string args, if
/// non-null, must be NUL-terminated C strings; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_schemas(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    run_metadata(session, out, |session| {
        Box::pin(async move {
            let catalog = opt_ident(catalog, "catalog")?;
            let schema_pattern = opt_pattern(schema_pattern, "schema_pattern")?;
            session
                .metadata()
                .list_schemas(catalog, schema_pattern)
                .await
        })
    })
}

/// List tables filtered by catalog (exact), schema / table patterns,
/// and an optional comma-separated `table_types` filter.
///
/// # Safety
/// `session` must be a live session handle; the string args, if
/// non-null, must be NUL-terminated C strings; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_tables(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    table_pattern: *const c_char,
    table_types_csv: *const c_char,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    run_metadata(session, out, |session| {
        Box::pin(async move {
            let catalog = opt_ident(catalog, "catalog")?;
            let schema_pattern = opt_pattern(schema_pattern, "schema_pattern")?;
            let table_pattern = opt_pattern(table_pattern, "table_pattern")?;
            // Split the comma-separated table types into a borrowed slice.
            let types_csv = opt_str(table_types_csv, "table_types")?;
            let types_owned: Option<Vec<&str>> = types_csv.map(|csv| {
                csv.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect()
            });
            session
                .metadata()
                .list_tables(
                    catalog,
                    schema_pattern,
                    table_pattern,
                    types_owned.as_deref(),
                )
                .await
        })
    })
}

/// List columns filtered by catalog (exact) and schema / table / column
/// patterns.
///
/// # Safety
/// `session` must be a live session handle; the string args, if
/// non-null, must be NUL-terminated C strings; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_columns(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    table_pattern: *const c_char,
    column_pattern: *const c_char,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    run_metadata(session, out, |session| {
        Box::pin(async move {
            let catalog = opt_ident(catalog, "catalog")?;
            let schema_pattern = opt_pattern(schema_pattern, "schema_pattern")?;
            let table_pattern = opt_pattern(table_pattern, "table_pattern")?;
            let column_pattern = opt_pattern(column_pattern, "column_pattern")?;
            session
                .metadata()
                .list_columns(catalog, schema_pattern, table_pattern, column_pattern)
                .await
        })
    })
}

/// Primary keys for an exactly-named table.
///
/// # Safety
/// `session` must be a live session handle; `table` must be a
/// NUL-terminated C string; `catalog` / `schema`, if non-null, must be
/// NUL-terminated C strings; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_primary_keys(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    run_metadata(session, out, |session| {
        Box::pin(async move {
            let catalog = opt_ident(catalog, "catalog")?;
            let schema = opt_ident(schema, "schema")?;
            let table = Identifier::new(req_str(table, "table")?)?;
            session
                .metadata()
                .primary_keys(catalog, schema, table)
                .await
        })
    })
}

/// Foreign keys. Mirrors ODBC `SQLForeignKeys`: the `parent_*` triple is
/// the referenced (primary-key) table, the `foreign_*` triple the
/// referencing table. The kernel requires the `foreign_*` triple (the
/// referencing table) to be fully specified; the `parent_*` triple is
/// optional and filters the referenced side.
///
/// # Safety
/// `session` must be a live session handle; the identifier args, if
/// non-null, must be NUL-terminated C strings; `out` must be writable.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn kernel_metadata_foreign_keys(
    session: *mut kernel_session_t,
    parent_catalog: *const c_char,
    parent_schema: *const c_char,
    parent_table: *const c_char,
    foreign_catalog: *const c_char,
    foreign_schema: *const c_char,
    foreign_table: *const c_char,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    run_metadata(session, out, |session| {
        Box::pin(async move {
            let parent_catalog = opt_ident(parent_catalog, "parent_catalog")?;
            let parent_schema = opt_ident(parent_schema, "parent_schema")?;
            let parent_table = opt_ident(parent_table, "parent_table")?;
            let foreign_catalog = opt_ident(foreign_catalog, "foreign_catalog")?;
            let foreign_schema = opt_ident(foreign_schema, "foreign_schema")?;
            let foreign_table = opt_ident(foreign_table, "foreign_table")?;
            session
                .metadata()
                .foreign_keys(
                    parent_catalog,
                    parent_schema,
                    parent_table,
                    foreign_catalog,
                    foreign_schema,
                    foreign_table,
                )
                .await
        })
    })
}

/// Store an `InvalidArgument` for a null pointer and return its code.
fn store_null(msg: &str) -> KernelStatusCode {
    crate::c_abi::error::store_error(Error::invalid_argument().message(msg));
    KernelStatusCode::InvalidArgument
}

// PR 7d follow-ups: functions, procedures, type_info, table_types — the
// kernel `Metadata` exposes these (some synthesised in-memory). Wire them
// over the C ABI when an ODBC / Go consumer needs SQLGetTypeInfo /
// SQLProcedures.
