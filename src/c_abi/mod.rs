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

//! Custom C ABI for the v0 Kernel — for ODBC and Go consumers that
//! cannot link the Rust crate directly.
//!
//! Per `docs/spec-v0.md` "Architecture", this layer is a thin shim over
//! the async kernel core: it owns a process-wide tokio runtime, marshals
//! C strings / structs into `SessionConfig`, and exposes opaque handles
//! for `Session`, `Statement`, `ExecutedStatement`,
//! `ExecutedAsyncStatement`, and `ResultStream`.
//!
//! ## Why this lives in the core crate (and the bindings don't)
//!
//! The PyO3 (`pyo3/`) and napi (`napi/`) bindings are *separate Cargo
//! workspaces* that depend on the kernel via `path = ".."`. They are
//! split out for one reason: they build the kernel with `tls-rustls`,
//! while the kernel core builds with the default `tls-native`. Living in
//! one workspace would unify those features (`tls-native ∪ tls-rustls`),
//! trip the mutual-exclusion `compile_error!` in `lib.rs`, and otherwise
//! link two TLS stacks into one artifact.
//!
//! The C ABI has no such conflict — it uses the same `tls-native` backend
//! as the core. Keeping it here means it compiles straight into
//! `libdatabricks_sql_kernel.{a,so}`, which is exactly what a C / C++
//! consumer wants to link: one static library, no satellite crate. The
//! module is named `c_abi` (not `ffi`) to distinguish it from the
//! PyO3 / napi FFI bindings, which are also "FFI" in the broad sense.
//!
//! A `cbindgen`-generated header is a v0.1 follow-up; today the header at
//! `include/databricks_kernel.h` is hand-maintained alongside this module.

// Each `extern "C"` function carries its own `# Safety` section; the
// module-wide allow that masked them in the skeleton phase is gone now
// that bodies have landed.

pub mod error;
pub mod metadata;
pub mod result;
pub mod session;
pub mod statement;

use std::sync::OnceLock;

/// Process-wide tokio runtime owned by the C ABI.
///
/// Rust-crate consumers (PyO3 / napi-rs) own their own runtime and call
/// the async surface directly; the C ABI has no caller-owned runtime to
/// borrow, so it owns one here and `block_on`s every entry point through
/// it. Initialised lazily on the first `kernel_session_open` and shared
/// by every session for the life of the process (matches the
/// migration-plan §5.2 contract).
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Fetch (initialising on first use) the process-wide C-ABI runtime.
///
/// A multi-thread runtime so concurrent FFI calls from different host
/// threads (e.g. an ODBC app issuing queries on several connections)
/// don't serialise behind a single worker. Returns an [`Error`] tagged
/// `Internal` if the runtime fails to build — the only failure mode is
/// the OS refusing to spawn worker threads, which is fatal and not
/// retryable.
///
/// [`Error`]: crate::kernel_error::Error
pub(crate) fn runtime() -> crate::kernel_error::Result<&'static tokio::runtime::Runtime> {
    // `OnceLock::get_or_init` can't carry a fallible initialiser, so do
    // the build-or-fetch dance by hand: try to fetch, and on a miss
    // build once and install. A lost race just drops our spare runtime.
    if let Some(rt) = RUNTIME.get() {
        return Ok(rt);
    }
    let built = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("kernel-c-abi")
        .build()
        .map_err(|e| {
            crate::kernel_error::Error::internal()
                .message(format!("failed to build the C-ABI tokio runtime: {e}"))
                .location(concat!(file!(), ":", line!()))
        })?;
    // If another thread installed one between our `get` and here, keep
    // theirs and drop ours.
    let _ = RUNTIME.set(built);
    Ok(RUNTIME.get().expect("runtime just set"))
}

/// Run a fallible async block to completion on the process-wide runtime,
/// translating the outcome into a [`KernelStatusCode`] and storing the
/// typed [`Error`] in the thread-local last-error buffer on failure.
///
/// Every `extern "C"` entry point that drives async work funnels through
/// here so the runtime-fetch, `block_on`, error-store, and status-map
/// steps stay in exactly one place. A panic inside the future is caught
/// at the `block_on` edge and surfaced as `Internal` rather than
/// unwinding across the FFI boundary (which is UB).
///
/// [`KernelStatusCode`]: crate::c_abi::KernelStatusCode
/// [`Error`]: crate::kernel_error::Error
pub(crate) fn block_on_ffi<F, Fut, T>(fut_builder: F) -> Result<T, KernelStatusCode>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::kernel_error::Result<T>>,
{
    error::clear_error();
    let rt = match runtime() {
        Ok(rt) => rt,
        Err(e) => {
            let code = KernelStatusCode::from(e.code);
            error::store_error(e);
            return Err(code);
        }
    };
    // Catch panics so a kernel bug can't unwind across `extern "C"`.
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rt.block_on(fut_builder())));
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => {
            let code = KernelStatusCode::from(e.code);
            error::store_error(e);
            Err(code)
        }
        Err(panic) => {
            let detail = panic_message(&panic);
            let e = crate::kernel_error::Error::internal()
                .message(format!("panic in kernel C ABI: {detail}"))
                .location(concat!(file!(), ":", line!()));
            error::store_error(e);
            Err(KernelStatusCode::Internal)
        }
    }
}

/// Best-effort extraction of a human-readable message from a caught
/// panic payload.
fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Status enum returned by every C entry point.
///
/// Variants parallel [`crate::kernel_error::ErrorCode`] one-to-one. The
/// [`From<ErrorCode>`] conversion below uses an exhaustive match (no
/// `_ =>` arm) so adding a variant to `ErrorCode` is a compile error
/// here until `KernelStatusCode` grows the matching variant. That
/// catches FFI/Rust drift mechanically.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelStatusCode {
    Success = 0,
    InvalidArgument = 1,
    Unauthenticated = 2,
    PermissionDenied = 3,
    NotFound = 4,
    ResourceExhausted = 5,
    Unavailable = 6,
    Timeout = 7,
    Cancelled = 8,
    DataLoss = 9,
    Internal = 10,
    InvalidStatementHandle = 11,
    NetworkError = 12,
    SqlError = 13,
}

impl From<crate::kernel_error::ErrorCode> for KernelStatusCode {
    fn from(code: crate::kernel_error::ErrorCode) -> Self {
        use crate::kernel_error::ErrorCode;
        // Exhaustive match — no `_ =>` arm. Adding a variant to
        // `ErrorCode` is a compile error here, forcing the FFI to
        // declare the corresponding `KernelStatusCode` variant.
        match code {
            ErrorCode::InvalidArgument => Self::InvalidArgument,
            ErrorCode::Unauthenticated => Self::Unauthenticated,
            ErrorCode::PermissionDenied => Self::PermissionDenied,
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::ResourceExhausted => Self::ResourceExhausted,
            ErrorCode::Unavailable => Self::Unavailable,
            ErrorCode::Timeout => Self::Timeout,
            ErrorCode::Cancelled => Self::Cancelled,
            ErrorCode::DataLoss => Self::DataLoss,
            ErrorCode::Internal => Self::Internal,
            ErrorCode::InvalidStatementHandle => Self::InvalidStatementHandle,
            ErrorCode::NetworkError => Self::NetworkError,
            ErrorCode::SqlError => Self::SqlError,
        }
    }
}
