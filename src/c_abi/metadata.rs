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
//! Each call captures the result schema up front and hands the owned
//! `ResultStream` to a self-contained handle that drives it lazily (one
//! batch per `kernel_result_stream_next_batch`), so the handle does not
//! borrow the session and a large result (e.g. a cross-catalog column
//! listing) streams with bounded memory. The session must still outlive
//! the call itself.
//!
//! Results carry the **server-shaped** (JDBC-canonical) columns the
//! kernel's `Metadata` produces; ODBC / JDBC drivers keep their own
//! rename / reshape layer on top.

use super::error::store_invalid_arg;
use super::result::{into_raw_result_stream, kernel_result_stream_t};
use super::session::kernel_session_t;
use super::{block_on_ffi, KernelStatusCode};
use crate::kernel_error::{Error, Result};
use crate::metadata_v0::{Identifier, LikePattern};
use crate::result_stream::ResultStream;
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

/// Read an optional C string, treating both a null pointer AND an empty /
/// whitespace-only string as "unfiltered" (`None`).
///
/// ODBC/JDBC drivers routinely pass an empty string rather than NULL to
/// mean "match all catalogs/schemas/tables"; the header only documents
/// NULL, but rejecting `""` with `InvalidArgument` (as `Identifier::new`
/// would) breaks those callers. Normalising empty → `None` here makes the
/// two spellings equivalent for every filter arg.
///
/// # Safety
/// As [`opt_str`].
unsafe fn opt_filter_str<'a>(ptr: *const c_char, what: &str) -> Result<Option<&'a str>> {
    Ok(opt_str(ptr, what)?.filter(|s| !s.trim().is_empty()))
}

/// Optional [`Identifier`] from a C string (null or empty → `None`).
unsafe fn opt_ident<'a>(ptr: *const c_char, what: &str) -> Result<Option<Identifier<'a>>> {
    match opt_filter_str(ptr, what)? {
        Some(s) => Ok(Some(Identifier::new(s)?)),
        None => Ok(None),
    }
}

/// Optional [`LikePattern`] from a C string (null or empty → `None`).
unsafe fn opt_pattern<'a>(ptr: *const c_char, what: &str) -> Result<Option<LikePattern<'a>>> {
    match opt_filter_str(ptr, what)? {
        Some(s) => Ok(Some(LikePattern::new(s)?)),
        None => Ok(None),
    }
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
        return store_invalid_arg("metadata: session must not be null");
    };
    if out.is_null() {
        return store_invalid_arg("metadata: out must not be null");
    }
    // Build the stream on the runtime and capture its schema; the stream
    // itself is handed to the owned handle and driven lazily (one batch
    // per `kernel_result_stream_next_batch`), so a large metadata result
    // (e.g. cross-catalog column listing) is not buffered up front.
    let result = block_on_ffi(move || async move {
        let stream = build(session).await?;
        let schema = stream.schema();
        Ok((schema, stream))
    });
    match result {
        Ok((schema, stream)) => {
            std::ptr::write(out, into_raw_result_stream(schema, stream));
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

/// Split a comma-separated `table_types` CSV into a borrowed slice of
/// non-empty, trimmed type names.
///
/// Empty entries are dropped, so ODBC's "" / whitespace / comma-only
/// junk (e.g. `""`, `" "`, `",,"`) normalizes to an empty `Vec`. An
/// empty `Vec` means "all types" (like a NULL arg): the kernel treats
/// `Some(&[])` the same as `None` (match-all) since #187/#201, so an
/// empty result needs no collapse to `None` to avoid a match-nothing
/// bug — the ODBC-specific empty→"all types" convention is preserved
/// either way.
fn split_table_types_csv(csv: &str) -> Vec<&str> {
    csv.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect()
}

/// List tables filtered by catalog (**pattern**), schema / table
/// patterns, and an optional comma-separated `table_types` filter.
///
/// The catalog is an ODBC/JDBC LIKE pattern here (`%` / `_`),
/// matching Thrift's `TGetTablesReq.catalogName` and ODBC `SQLTables`
/// under the default `SQL_ATTR_METADATA_ID = SQL_FALSE`. A literal
/// catalog (no wildcards, or `SQL_ATTR_METADATA_ID = SQL_TRUE`
/// identifier semantics) still works — it takes the fast exact
/// single-catalog path. See [`Metadata::list_tables`](crate::metadata_v0::Metadata::list_tables).
///
/// **Empty-string catalog:** at this C ABI boundary a `NULL` **or an
/// empty / whitespace-only** `catalog` is normalized to "all catalogs"
/// (the ODBC convention where drivers pass `""` for "no catalog
/// filter"). This differs from the Rust / pyo3 / napi surface, where an
/// empty-string catalog is a valid `LikePattern` meaning "match
/// nothing" (zero rows). C / ODBC callers who want "match nothing"
/// should not rely on `""`; the pattern-value distinction only exists
/// on the typed surfaces.
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
            let catalog = opt_pattern(catalog, "catalog")?;
            let schema_pattern = opt_pattern(schema_pattern, "schema_pattern")?;
            let table_pattern = opt_pattern(table_pattern, "table_pattern")?;
            let types_csv = opt_str(table_types_csv, "table_types")?;
            let types_owned = types_csv.map(split_table_types_csv);
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

/// List the table types the workspace supports (`TABLE`, `VIEW`,
/// `SYSTEM TABLE`, …). No filters and no wire call — the set is derived
/// from the session's transport (see `Metadata::table_types`). Backs the
/// ODBC `SQLTables(table_type = SQL_ALL_TABLE_TYPES)` enumeration mode.
/// The stream carries a single `TABLE_TYPE` column (JDBC `getTableTypes`
/// shape).
///
/// # Safety
/// `session` must be a live handle from `kernel_session_open`; `out`
/// must be a valid, writable pointer.
#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_table_types(
    session: *mut kernel_session_t,
    out: *mut *mut kernel_result_stream_t,
) -> KernelStatusCode {
    run_metadata(session, out, |session| {
        Box::pin(async move { session.metadata().table_types().await })
    })
}

// PR 7d follow-ups: functions, procedures, type_info — the kernel
// `Metadata` exposes these (some synthesised in-memory). Wire them over
// the C ABI when an ODBC / Go consumer needs SQLGetTypeInfo /
// SQLProcedures / SQLPrimaryKeys-adjacent surfaces.

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    /// A null pointer, an empty string, and a whitespace-only string all
    /// mean "unfiltered" (`None`); a real value is borrowed through. This
    /// lets ODBC/JDBC callers pass "" (their common "match all" spelling)
    /// instead of NULL without hitting `Identifier`'s empty-string reject.
    #[test]
    fn opt_filter_str_treats_null_and_blank_as_unfiltered() {
        unsafe {
            assert_eq!(opt_filter_str(std::ptr::null(), "catalog").unwrap(), None);

            let empty = CString::new("").unwrap();
            assert_eq!(opt_filter_str(empty.as_ptr(), "catalog").unwrap(), None);

            let blank = CString::new("   ").unwrap();
            assert_eq!(opt_filter_str(blank.as_ptr(), "catalog").unwrap(), None);

            let real = CString::new("main").unwrap();
            assert_eq!(
                opt_filter_str(real.as_ptr(), "catalog").unwrap(),
                Some("main")
            );
        }
    }

    /// `opt_ident` inherits the same normalisation: null / empty / blank →
    /// `None` (no `InvalidArgument`), a real value → `Some(ident)`.
    #[test]
    fn opt_ident_maps_empty_to_none_not_error() {
        unsafe {
            assert!(opt_ident(std::ptr::null(), "catalog").unwrap().is_none());

            let empty = CString::new("").unwrap();
            assert!(
                opt_ident(empty.as_ptr(), "catalog").unwrap().is_none(),
                "empty identifier must be treated as unfiltered, not rejected"
            );

            let real = CString::new("main").unwrap();
            assert!(opt_ident(real.as_ptr(), "catalog").unwrap().is_some());
        }
    }

    /// ODBC's "" / whitespace / comma-only `table_types` spellings all
    /// normalize to an empty type list, which `list_tables` treats as
    /// "all types" (match-all, like a NULL arg) since #187/#201. A
    /// non-empty CSV parses into its trimmed, non-empty entries.
    #[test]
    fn split_table_types_csv_normalizes_empty_and_parses_values() {
        // Empty / whitespace / comma-only junk → empty list (all types).
        assert!(split_table_types_csv("").is_empty());
        assert!(split_table_types_csv("   ").is_empty());
        assert!(split_table_types_csv(",").is_empty());
        assert!(split_table_types_csv(",,").is_empty());
        assert!(split_table_types_csv(" , , ").is_empty());

        // Real values are trimmed; interspersed empties are dropped.
        assert_eq!(split_table_types_csv("TABLE"), vec!["TABLE"]);
        assert_eq!(split_table_types_csv("TABLE, VIEW"), vec!["TABLE", "VIEW"]);
        assert_eq!(
            split_table_types_csv(" TABLE ,, VIEW ,"),
            vec!["TABLE", "VIEW"]
        );
    }
}
