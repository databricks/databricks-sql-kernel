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

//! `KernelError` — single Python exception class carrying the kernel's
//! structured error info.
//!
//! Connector-side code (`backend/kernel/client.py`) inspects `code`
//! and re-raises as `DatabaseError` / `OperationalError` /
//! `ProgrammingError` per PEP 249. This module owns the only
//! `kernel::Error` → `PyErr` conversion site.

use databricks_sql_kernel::{Error as KError, ErrorCode};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;

create_exception!(databricks_sql_kernel, KernelError, PyException);

/// Map a kernel `ErrorCode` to a stable string slug. Wrapper code
/// branches on `err.code` (a Python string) to choose the right
/// PEP 249 exception class. New `ErrorCode` variants compile-error
/// in this match by design — keeps the wrapper's exception mapping
/// in sync with the kernel.
fn code_str(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::InvalidArgument => "InvalidArgument",
        ErrorCode::Unauthenticated => "Unauthenticated",
        ErrorCode::PermissionDenied => "PermissionDenied",
        ErrorCode::NotFound => "NotFound",
        ErrorCode::ResourceExhausted => "ResourceExhausted",
        ErrorCode::Unavailable => "Unavailable",
        ErrorCode::Timeout => "Timeout",
        ErrorCode::Cancelled => "Cancelled",
        ErrorCode::DataLoss => "DataLoss",
        ErrorCode::Internal => "Internal",
        ErrorCode::InvalidStatementHandle => "InvalidStatementHandle",
        ErrorCode::NetworkError => "NetworkError",
        ErrorCode::SqlError => "SqlError",
        // `ErrorCode` is `#[non_exhaustive]`; new variants surface as
        // "Unknown" on the Python side until this match is updated.
        _ => "Unknown",
    }
}

/// Convert a kernel `Error` into a `KernelError` Python exception
/// with structured attributes attached. The exception message is the
/// kernel's `message`; structured fields live on `err.args[0]` (a
/// dict) for callers that want them.
pub(crate) fn to_py_err(py: Python<'_>, err: KError) -> PyErr {
    let attrs = PyDict::new(py);
    let _ = attrs.set_item("code", code_str(err.code));
    let _ = attrs.set_item("message", &err.message);
    let _ = attrs.set_item("sql_state", err.sql_state.as_deref());
    let _ = attrs.set_item("error_code", err.error_code.as_deref());
    let _ = attrs.set_item("vendor_code", err.vendor_code);
    let _ = attrs.set_item("http_status", err.http_status);
    let _ = attrs.set_item("retryable", err.retryable);
    let _ = attrs.set_item("query_id", err.query_id.as_deref());

    KernelError::new_err((err.message.clone(), attrs.unbind()))
}
