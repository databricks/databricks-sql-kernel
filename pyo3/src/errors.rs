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

//! `KernelError` — Python exception carrying the kernel's structured
//! error info.
//!
//! Subclassing native exception types is not allowed in `abi3` mode
//! (PyO3 emits a compile-time error: "with the abi3 feature enabled,
//! PyO3 does not support subclassing native types"). To keep one
//! wheel per platform across Python 3.9+, `KernelError` is built
//! via `create_exception!` (subclasses `Exception` at the Python C
//! level, not at the Rust pyclass level) and the structured fields
//! are attached as Python attributes via `setattr` so callers still
//! read `err.code` / `err.sql_state` / etc. directly — no
//! `err.args[1]['code']` tuple-unpacking.
//!
//! Connector-side code (`backend/kernel/client.py`) inspects
//! `err.code` and re-raises as `DatabaseError` / `OperationalError`
//! / `ProgrammingError` per PEP 249. This module owns the only
//! `kernel::Error -> PyErr` conversion site (`to_py_err`).

use databricks_sql_kernel::{Error as KError, ErrorCode};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(databricks_sql_kernel, KernelError, PyException);

/// Map a kernel `ErrorCode` to a stable string slug.
///
/// `ErrorCode` is `#[non_exhaustive]`; new kernel variants surface
/// as `"Unknown"` on the Python side until this match is updated.
fn code_str(code: ErrorCode) -> &'static str {
    // Delegate to the kernel's single-source-of-truth slug map (shared with
    // the napi binding) so the wire slugs can never drift across bindings.
    code.slug()
}

/// Common attribute-set used by both `to_py_err` (kernel error
/// conversion) and `closed_err` (use-after-close guard). Pulls one
/// pyo3 attribute-setting site so the field list stays in lockstep.
#[allow(clippy::too_many_arguments)]
fn attach_attrs(
    py: Python<'_>,
    bound: &Bound<'_, PyAny>,
    code: &str,
    message: &str,
    sql_state: Option<&str>,
    error_code: Option<&str>,
    vendor_code: Option<i32>,
    http_status: Option<u16>,
    retryable: bool,
    query_id: Option<&str>,
) -> PyResult<()> {
    bound.setattr("code", code)?;
    bound.setattr("message", message)?;
    bound.setattr("sql_state", sql_state)?;
    bound.setattr("error_code", error_code)?;
    bound.setattr("vendor_code", vendor_code)?;
    bound.setattr("http_status", http_status)?;
    bound.setattr("retryable", retryable)?;
    bound.setattr("query_id", query_id)?;
    let _ = py; // silence unused-var warning if pyo3 ever drops the Python<'_> arg
    Ok(())
}

/// Convert a kernel `Error` into a `KernelError` Python exception
/// with the structured fields attached as attributes. Python sees
/// `err.code`, `err.message`, `err.sql_state`, etc.; `str(err)` is
/// the kernel's message text.
pub(crate) fn to_py_err(py: Python<'_>, err: KError) -> PyErr {
    // Boundary-only breadcrumb: the kernel logged *what* failed; this
    // records the kernel `Error` -> Python `KernelError` translation
    // (and which code slug drove it), which is knowledge unique to the
    // pyo3 layer. Helps explain "why did I get this exception type".
    tracing::debug!(
        target: crate::logging::PYO3_LOG_TARGET,
        code = code_str(err.code),
        retryable = err.retryable,
        "mapping kernel error to KernelError: {}",
        err.message
    );
    let py_err = KernelError::new_err(err.message.clone());
    // Materialise the exception value so we can attach attributes.
    let bound = py_err.value(py);
    if let Err(setattr_err) = attach_attrs(
        py,
        bound.as_any(),
        code_str(err.code),
        &err.message,
        err.sql_state.as_deref(),
        err.error_code.as_deref(),
        err.vendor_code,
        err.http_status,
        err.retryable,
        err.query_id.as_deref(),
    ) {
        // Attribute attachment is best-effort. If something goes
        // wrong (extremely unlikely for plain string/int setattrs),
        // surface that error so the caller sees it rather than a
        // partially-populated exception. Fold the original kernel
        // message into the new error so debugging context isn't
        // lost.
        return setattr_err;
    }
    py_err
}

/// Build a `KernelError` carrying an `InvalidStatementHandle` code
/// for the "this handle is closed" guard. Used by every
/// `alive()` / `alive_mut()` helper across the wrapper.
///
/// Acquires the GIL internally so callers don't have to thread a
/// `Python<'_>` through their guard helpers.
pub(crate) fn closed_err(handle_name: &str) -> PyErr {
    Python::attach(|py| {
        let message = format!("{handle_name} is closed");
        let py_err = KernelError::new_err(message.clone());
        let bound = py_err.value(py);
        if let Err(setattr_err) = attach_attrs(
            py,
            bound.as_any(),
            "InvalidStatementHandle",
            &message,
            None,
            None,
            None,
            None,
            false,
            None,
        ) {
            return setattr_err;
        }
        py_err
    })
}
