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

//! Thread-local last-error storage and the `kernel_get_last_error` C
//! entry point.
//!
//! Pattern borrowed from the legacy `src/adbc/ffi/error.rs`: every C
//! function clears the buffer at entry and writes to it on error;
//! callers fetch detail with `kernel_get_last_error`.

use super::KernelStatusCode;
use crate::kernel_error::Error;
use std::cell::RefCell;
use std::ffi::{c_char, CString};

/// Mirror of `crate::kernel_error::Error` in `repr(C)` form.
///
/// String fields are owned by the kernel's thread-local buffer; the
/// caller copies them out before the next FFI call on this thread
/// clears the buffer. Pointers are nullable for fields the underlying
/// `Error` left as `None`.
#[repr(C)]
pub struct KernelError {
    /// Discriminant matches `KernelStatusCode` (cast-equal). Use
    /// `KernelStatusCode::from(error.code)` on the Rust side to
    /// convert; the i32 here is the discriminant value of that enum.
    pub code: i32,
    pub message: *const c_char,
    /// 5 ASCII chars + NUL, or null if absent.
    pub sql_state: *const c_char,
    /// `0` if absent (the underlying `Error::vendor_code` was `None`).
    pub vendor_code: i32,
    /// `0` if absent.
    pub http_status: u16,
    pub retryable: bool,
    pub query_id: *const c_char,
}

/// Owned backing store for the thread-local last error.
///
/// Holds the `CString`s alive so the `*const c_char` pointers handed
/// back through [`KernelError`] stay valid until the next FFI call on
/// this thread clears or overwrites the buffer. `code == None` means
/// "no error recorded since the last clear".
#[derive(Default)]
struct LastError {
    local_os_error: i32,
    local_path: Option<CString>,
    code: Option<crate::kernel_error::ErrorCode>,
    message: Option<CString>,
    sql_state: Option<CString>,
    vendor_code: i32,
    http_status: u16,
    retryable: bool,
    query_id: Option<CString>,
    error_code: Option<CString>,
}

thread_local! {
    /// Per-thread last-error slot. The C ABI is call-response on a
    /// single thread: a function clears this on entry, populates it on
    /// failure, and the caller reads it via `kernel_get_last_error`
    /// before making the next FFI call on the same thread.
    static LAST_ERROR: RefCell<LastError> = RefCell::new(LastError::default());
}

/// Turn a Rust `String` into a `CString`, replacing any interior NUL
/// bytes with `?` so a message that happens to contain `\0` can't make
/// `CString::new` fail (which would lose the error detail entirely).
///
/// Shared by every Rust-to-C string path that accepts arbitrary Rust text,
/// including errors, statement IDs, and callback-delivered log records, so
/// they have identical interior-NUL semantics (`\0` → `?`).
pub(crate) fn to_cstring_lossy(s: &str) -> CString {
    // Fast path: no interior NUL.
    match CString::new(s) {
        Ok(c) => c,
        Err(_) => {
            let sanitised: String = s.chars().map(|c| if c == '\0' { '?' } else { c }).collect();
            CString::new(sanitised).unwrap_or_default()
        }
    }
}

/// Fetch the last error set on this thread.
///
/// Returns `false` if no error has been recorded since the last
/// [`clear_error`] (or thread start), leaving `*out` untouched. On
/// `true`, `*out` is fully populated; its `*const c_char` fields point
/// into this thread's last-error buffer and are valid until the next
/// `kernel_*` call on this thread.
///
/// # Safety
/// `out` must be a valid, writable pointer to a `KernelError`. The
/// pointer fields it returns are valid until the next FFI call on
/// this thread.
#[no_mangle]
pub unsafe extern "C" fn kernel_get_last_error(out: *mut KernelError) -> bool {
    if out.is_null() {
        return false;
    }
    LAST_ERROR.with(|cell| {
        let last = cell.borrow();
        let Some(code) = last.code else {
            return false;
        };
        let as_ptr = |c: &Option<CString>| c.as_ref().map_or(std::ptr::null(), |s| s.as_ptr());
        // SAFETY: `out` is non-null and writable per the contract.
        std::ptr::write(
            out,
            KernelError {
                code: KernelStatusCode::from(code) as i32,
                message: as_ptr(&last.message),
                sql_state: as_ptr(&last.sql_state),
                vendor_code: last.vendor_code,
                http_status: last.http_status,
                retryable: last.retryable,
                query_id: as_ptr(&last.query_id),
            },
        );
        true
    })
}

/// Original local OS error number, or zero when unavailable. Does not clear the error.
#[no_mangle]
pub extern "C" fn kernel_get_last_error_local_os_error() -> i32 {
    LAST_ERROR.with(|last| last.borrow().local_os_error)
}

/// The error's string code, such as VOLUME_PATH_NOT_ALLOWED, or null.
/// Valid until the next call that clears the error.
#[no_mangle]
pub extern "C" fn kernel_get_last_error_code() -> *const c_char {
    LAST_ERROR.with(|last| {
        last.borrow()
            .error_code
            .as_ref()
            .map_or(std::ptr::null(), |s| s.as_ptr())
    })
}

/// Borrowed requested local path, or null. Valid until the next error-clearing call.
#[no_mangle]
pub extern "C" fn kernel_get_last_error_local_path() -> *const c_char {
    LAST_ERROR.with(|last| {
        last.borrow()
            .local_path
            .as_ref()
            .map_or(std::ptr::null(), |s| s.as_ptr())
    })
}

/// Crate-internal: write a typed kernel [`Error`] into the thread-local
/// last-error buffer. Every FFI function that returns a non-`Success`
/// status code calls this (via [`crate::c_abi::block_on_ffi`] or directly)
/// so the consumer can retrieve the structured detail with
/// [`kernel_get_last_error`].
pub(crate) fn store_error(err: Error) {
    LAST_ERROR.with(|cell| {
        let mut last = cell.borrow_mut();
        // Prefer the structured `vendor_code`; the SEA wire ships a
        // string `error_code` slug, which we surface in the message
        // rather than the numeric slot (the C struct has no string
        // error-code field in v0).
        let mut message = err.message.clone();
        if let Some(ec) = err.error_code.as_deref() {
            if !message.is_empty() {
                message.push_str("; ");
            }
            message.push_str("error_code=");
            message.push_str(ec);
        }
        *last = LastError {
            local_os_error: err
                .local_error
                .as_ref()
                .and_then(|e| e.raw_os_error)
                .unwrap_or(0),
            local_path: err.local_error.as_ref().map(|e| to_cstring_lossy(&e.path)),
            code: Some(err.code),
            message: Some(to_cstring_lossy(&message)),
            sql_state: err.sql_state.as_deref().map(to_cstring_lossy),
            vendor_code: err.vendor_code.unwrap_or(0),
            http_status: err.http_status.unwrap_or(0),
            retryable: err.retryable,
            query_id: err.query_id.as_deref().map(to_cstring_lossy),
            error_code: err.error_code.as_deref().map(to_cstring_lossy),
        };
    });
}

/// Crate-internal: clear the thread-local last-error buffer. Called at
/// the entry of every FFI function (via [`crate::c_abi::block_on_ffi`] or
/// directly) so a previous call's stale error can't leak into a
/// subsequent one that succeeds.
pub(crate) fn clear_error() {
    LAST_ERROR.with(|cell| *cell.borrow_mut() = LastError::default());
}

/// Store a typed [`Error`] and return its mapped [`KernelStatusCode`].
///
/// The single "map code + store + return code" helper shared by every
/// C entry point that surfaces a typed error (previously reimplemented
/// per module).
pub(crate) fn store_and_code(err: Error) -> KernelStatusCode {
    let code = KernelStatusCode::from(err.code);
    store_error(err);
    code
}

/// Store an `InvalidArgument` with `msg` and return
/// [`KernelStatusCode::InvalidArgument`]. Used for the null-pointer /
/// bad-argument guards at the top of every entry point.
pub(crate) fn store_invalid_arg(msg: &str) -> KernelStatusCode {
    store_and_code(Error::invalid_argument().message(msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: read the thread-local error into a `KernelError` and run
    /// `f` with the borrowed C strings still alive.
    fn with_last_error<R>(f: impl FnOnce(Option<KernelError>) -> R) -> R {
        let mut out = std::mem::MaybeUninit::<KernelError>::uninit();
        // SAFETY: out is a valid writable pointer.
        let present = unsafe { kernel_get_last_error(out.as_mut_ptr()) };
        if present {
            // SAFETY: kernel_get_last_error returned true so out is init.
            f(Some(unsafe { out.assume_init() }))
        } else {
            f(None)
        }
    }

    #[test]
    fn no_error_after_clear() {
        clear_error();
        with_last_error(|e| assert!(e.is_none()));
    }

    #[test]
    fn store_then_fetch_roundtrips_core_fields() {
        store_error(
            Error::sql_error()
                .message("boom")
                .with_sql_state("42601")
                .with_http_status(400)
                .with_query_id("q-123"),
        );
        with_last_error(|e| {
            let e = e.expect("error present");
            assert_eq!(e.code, KernelStatusCode::SqlError as i32);
            assert_eq!(e.http_status, 400);
            // SAFETY: message is non-null after a store.
            let msg = unsafe { std::ffi::CStr::from_ptr(e.message) }
                .to_str()
                .unwrap();
            assert!(msg.contains("boom"));
            let ss = unsafe { std::ffi::CStr::from_ptr(e.sql_state) }
                .to_str()
                .unwrap();
            assert_eq!(ss, "42601");
            let qid = unsafe { std::ffi::CStr::from_ptr(e.query_id) }
                .to_str()
                .unwrap();
            assert_eq!(qid, "q-123");
        });
        clear_error();
    }

    #[test]
    fn store_error_folds_error_code_slug_into_message() {
        let mut error = Error::sql_error()
            .message("statement gone")
            .with_error_code("STATEMENT_NOT_FOUND");
        error.local_error = Some(Box::new(crate::kernel_error::LocalFileError::new(
            Some(2),
            std::io::ErrorKind::NotFound,
            "local-file",
        )));
        store_error(error);
        with_last_error(|e| {
            let e = e.expect("error present");
            let msg = unsafe { std::ffi::CStr::from_ptr(e.message) }
                .to_str()
                .unwrap();
            assert!(msg.contains("statement gone"));
            assert!(msg.contains("STATEMENT_NOT_FOUND"));
            assert_eq!(
                unsafe { std::ffi::CStr::from_ptr(kernel_get_last_error_code()) }
                    .to_str()
                    .unwrap(),
                "STATEMENT_NOT_FOUND"
            );
        });
        assert_eq!(kernel_get_last_error_local_os_error(), 2);
        assert_eq!(
            unsafe { std::ffi::CStr::from_ptr(kernel_get_last_error_local_path()) }
                .to_str()
                .unwrap(),
            "local-file"
        );
        with_last_error(|error| assert!(error.is_some()));
        clear_error();
        assert!(kernel_get_last_error_code().is_null());
        assert_eq!(kernel_get_last_error_local_os_error(), 0);
        assert!(kernel_get_last_error_local_path().is_null());
    }

    #[test]
    fn retryable_flag_carries_through() {
        store_error(Error::unavailable().message("cold start"));
        with_last_error(|e| {
            let e = e.expect("error present");
            assert_eq!(e.code, KernelStatusCode::Unavailable as i32);
            assert!(e.retryable);
        });
        clear_error();
    }

    #[test]
    fn message_with_interior_nul_is_sanitised() {
        store_error(Error::internal().message("a\0b"));
        with_last_error(|e| {
            let e = e.expect("error present");
            let msg = unsafe { std::ffi::CStr::from_ptr(e.message) }
                .to_str()
                .unwrap();
            assert_eq!(msg, "a?b");
        });
        clear_error();
    }
}
