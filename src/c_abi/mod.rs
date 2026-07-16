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

pub mod cancel;
pub mod error;
pub mod logging;
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
/// **Threading contract:** the C ABI must be driven from threads that are
/// NOT already running inside a Tokio runtime. `Runtime::block_on` panics
/// ("Cannot start a runtime from within a runtime") if called from a
/// worker of any runtime — including this one — so a reentrant C-ABI call
/// (e.g. a consumer that invokes a `kernel_*` entry point from a callback
/// executing on a runtime worker) is rejected up front with a clear
/// `Internal` error instead of the opaque caught-panic path. Native
/// (non-runtime) caller threads — the intended ODBC / Go usage — are
/// unaffected.
///
/// [`KernelStatusCode`]: crate::c_abi::KernelStatusCode
/// [`Error`]: crate::kernel_error::Error
pub(crate) fn block_on_ffi<F, Fut, T>(fut_builder: F) -> Result<T, KernelStatusCode>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::kernel_error::Result<T>>,
{
    error::clear_error();
    // Reject reentrancy explicitly: calling `block_on` from within any
    // Tokio runtime would panic. Surface a typed, actionable error rather
    // than a caught panic mapped to a generic `Internal`.
    if tokio::runtime::Handle::try_current().is_ok() {
        let e = crate::kernel_error::Error::new(
            crate::kernel_error::ErrorCode::Internal,
            "kernel C ABI called from within a Tokio runtime thread; entry points must be \
             invoked from a native (non-async-runtime) thread",
        );
        let code = KernelStatusCode::from(e.code);
        error::store_error(e);
        return Err(code);
    }
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
        Err(panic) => Err(handle_panic(panic)),
    }
}

/// Like [`block_on_ffi`], but races the future against a cooperative
/// [`CancellationToken`] so a host watcher thread can interrupt an otherwise
/// uninterruptible blocking C-ABI call (connect, or a hung result-stream
/// fetch).
///
/// When `token` fires before the future completes, this **drops the future**
/// and returns [`KernelStatusCode::Cancelled`] with a typed `Cancelled` error
/// in the thread-local buffer. Dropping the future is what makes the cancel
/// *real* rather than a bare stop-waiting: the kernel's HTTP client is
/// `reqwest`, whose in-flight request futures abort when dropped (the crate's
/// documented cancellation model — see the CloudFetch streaming provider's own
/// token use), so the underlying socket work is torn down, not leaked.
///
/// A **null** token means "no cancellation" and this behaves exactly like
/// [`block_on_ffi`] — so a single implementation body can back both the plain
/// and the `*_cancellable` entry points. An **already-fired** token short-
/// circuits to `Cancelled` without starting the future (matching a caller whose
/// deadline had already elapsed at the call site).
///
/// The same threading contract as [`block_on_ffi`] applies (must be driven
/// from a non-runtime thread); reentrancy is rejected identically.
///
/// [`CancellationToken`]: tokio_util::sync::CancellationToken
pub(crate) fn block_on_ffi_cancellable<F, Fut, T>(
    token: Option<&tokio_util::sync::CancellationToken>,
    fut_builder: F,
) -> Result<T, KernelStatusCode>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::kernel_error::Result<T>>,
{
    // No token → identical to the plain path (one body, two entry shapes).
    let Some(token) = token else {
        return block_on_ffi(fut_builder);
    };

    error::clear_error();
    // Reject reentrancy explicitly (same reasoning as block_on_ffi): block_on
    // from within any Tokio runtime would panic.
    if tokio::runtime::Handle::try_current().is_ok() {
        let e = crate::kernel_error::Error::new(
            crate::kernel_error::ErrorCode::Internal,
            "kernel C ABI called from within a Tokio runtime thread; entry points must be \
             invoked from a native (non-async-runtime) thread",
        );
        let code = KernelStatusCode::from(e.code);
        error::store_error(e);
        return Err(code);
    }
    let rt = match runtime() {
        Ok(rt) => rt,
        Err(e) => {
            let code = KernelStatusCode::from(e.code);
            error::store_error(e);
            return Err(code);
        }
    };

    // Race the work against the token. `select!` drops the losing branch's
    // future, so a cancel here drops the in-flight request future. Wrapped in
    // catch_unwind on the same AssertUnwindSafe basis as block_on_ffi so a
    // panic inside the future can't unwind across `extern "C"`.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async {
            tokio::select! {
                // Bias toward the cancel branch so an already-fired token is
                // honoured deterministically before the work is polled — the
                // "ctx already done at call time" case must not run the future.
                biased;
                _ = token.cancelled() => None,
                out = fut_builder() => Some(out),
            }
        })
    }));
    match result {
        Ok(Some(Ok(value))) => Ok(value),
        Ok(Some(Err(e))) => {
            let code = KernelStatusCode::from(e.code);
            error::store_error(e);
            Err(code)
        }
        Ok(None) => {
            // Cancelled: the future was dropped mid-flight. Surface a typed
            // Cancelled error so the host sees the same shape as a server-side
            // Cancelled, and can prefer its own deadline/context error.
            let e = crate::kernel_error::Error::cancelled()
                .message("kernel C-ABI call cancelled by the caller's cancel token");
            let code = KernelStatusCode::from(e.code);
            error::store_error(e);
            Err(code)
        }
        Err(panic) => Err(handle_panic(panic)),
    }
}

/// Run a **synchronous** fallible closure at an `extern "C"` boundary,
/// catching any panic so it can't unwind across the FFI frame (UB) and
/// mapping it to [`KernelStatusCode::Internal`].
///
/// Sibling of [`block_on_ffi`] for entry points that do no async work
/// (parameter binding, config setters, the Arrow C Data Interface
/// export, handle teardown). The closure returns a `KernelStatusCode`
/// directly — it is expected to have already stored any typed error via
/// [`error::store_error`] on its own failure paths; this wrapper only
/// adds the panic guard (and clears the thread-local error on entry, so
/// a caller reading `kernel_get_last_error` after a `Success` never sees
/// a stale error).
///
/// # Safety note
/// The closure runs under `AssertUnwindSafe`; callers that hold a `&mut`
/// to a handle across this boundary must treat the handle as potentially
/// half-mutated on an `Internal` return (see `kernel_statement_execute`).
pub(crate) fn catch_ffi<F>(f: F) -> KernelStatusCode
where
    F: FnOnce() -> KernelStatusCode,
{
    error::clear_error();
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(code) => code,
        Err(panic) => handle_panic(panic),
    }
}

/// Shared panic handler for both FFI wrappers: log it (so a recurring
/// panic bug is visible to on-call rather than silently folded into
/// `Internal`), store a typed `Internal` error in the thread-local
/// buffer, and return the `Internal` status code.
fn handle_panic(panic: Box<dyn std::any::Any + Send>) -> KernelStatusCode {
    let detail = panic_message(&panic);
    // Log at ERROR so a recurring panic (bad CloudFetch batch, reader
    // unwrap) is observable in aggregate — a caught panic across the C
    // ABI is always a kernel bug, never a routine query failure.
    tracing::error!(
        target: crate::klog::KERNEL_LOG_TARGET,
        detail = %detail,
        "panic caught at the kernel C ABI boundary"
    );
    let e = crate::kernel_error::Error::internal()
        .message(format!("panic in kernel C ABI: {detail}"))
        .location(concat!(file!(), ":", line!()));
    error::store_error(e);
    KernelStatusCode::Internal
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

/// Current C-ABI version. Bump this on any change that alters a struct
/// layout ([`KernelStatusCode`], `KernelError`) or the meaning of an
/// existing symbol, so a consumer compiled against one header can detect
/// a library built against a different one. Kept in lockstep with the
/// `DATABRICKS_KERNEL_ABI_VERSION` macro in `include/databricks_kernel.h`
/// (the C-smoke test asserts the two agree). Pure v0.x aid — there is no
/// ABI stability promise until v1.0.
pub const KERNEL_ABI_VERSION: u32 = 1;

/// Report the C-ABI version compiled into this library. A consumer that
/// linked a header declaring `DATABRICKS_KERNEL_ABI_VERSION` should refuse
/// to run against a library whose `kernel_abi_version()` differs, since a
/// mismatch means the `KernelStatusCode` enum / `KernelError` layout it
/// reads may not match what this library writes. Infallible and
/// side-effect-free, so it takes no config and returns the value directly
/// (no `KernelStatusCode` / last-error dance).
#[no_mangle]
pub extern "C" fn kernel_abi_version() -> u32 {
    KERNEL_ABI_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exported `kernel_abi_version()` returns the crate-level
    /// `KERNEL_ABI_VERSION` constant, so the two can't drift. (The C side's
    /// `DATABRICKS_KERNEL_ABI_VERSION` macro is checked against this in the
    /// C-smoke test, closing the Rust↔header loop.)
    #[test]
    fn abi_version_is_current() {
        assert_eq!(kernel_abi_version(), KERNEL_ABI_VERSION);
    }

    /// `block_on_ffi` must reject a reentrant call (one made from a thread
    /// already inside a Tokio runtime) with a typed `Internal` error and a
    /// populated last-error, rather than letting `Runtime::block_on` panic
    /// into the caught-panic path (which also maps to `Internal` but loses
    /// the actionable message). Guards the threading contract documented on
    /// `block_on_ffi`.
    #[test]
    fn block_on_ffi_rejects_reentrancy_from_runtime_thread() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            let result: Result<(), KernelStatusCode> = block_on_ffi(|| async { Ok(()) });
            assert_eq!(result, Err(KernelStatusCode::Internal));

            let mut out = std::mem::MaybeUninit::<crate::c_abi::error::KernelError>::uninit();
            // SAFETY: out is a valid writable pointer.
            let present = unsafe { crate::c_abi::error::kernel_get_last_error(out.as_mut_ptr()) };
            assert!(present, "reentrancy must populate the last-error buffer");
            // SAFETY: present is true, so out is initialised.
            let err = unsafe { out.assume_init() };
            // SAFETY: message is non-null after a store.
            let msg = unsafe { std::ffi::CStr::from_ptr(err.message) }
                .to_str()
                .unwrap();
            assert!(
                msg.contains("within a Tokio runtime"),
                "reentrancy error should explain the threading contract, got: {msg}",
            );
        });
    }
}
