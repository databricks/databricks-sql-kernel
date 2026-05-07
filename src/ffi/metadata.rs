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

//! Metadata FFI — one C entry point per `Metadata` method. Each
//! returns a `kernel_result_stream_t` the caller drives via
//! `kernel_result_stream_*`. (`Metadata` is `'static`; addendum A1.)

#![allow(dead_code, unused_variables)]

use super::session::kernel_session_t;
use super::KernelStatusCode;
use std::ffi::c_char;

#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_catalogs(
    session: *mut kernel_session_t,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7d")
}

#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_schemas(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema_pattern: *const c_char,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7d")
}

#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_tables(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema: *const c_char,
    table_pattern: *const c_char,
    table_types_csv: *const c_char,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7d")
}

#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_list_columns(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    column_pattern: *const c_char,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7d")
}

#[no_mangle]
pub unsafe extern "C" fn kernel_metadata_primary_keys(
    session: *mut kernel_session_t,
    catalog: *const c_char,
    schema: *const c_char,
    table: *const c_char,
    out: *mut *mut super::result::kernel_result_stream_t,
) -> KernelStatusCode {
    unimplemented!("PR 7d")
}

// PR 7d: foreign_keys, functions, procedures, type_info, table_types
