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
/// borrow, so it owns one here and drives every entry point through it
/// (see [`block_on_ffi`], which spawns onto this runtime rather than
/// blocking the caller thread on it). Initialised lazily on the first
/// `kernel_session_open` and shared by every session for the life of the
/// process (matches the migration-plan §5.2 contract).
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
    // On Windows, pin the module that contains the kernel into the process so
    // it can never be unloaded while these worker threads are alive (#276).
    // Must happen once the runtime — and therefore its detached `kernel-c-abi`
    // worker threads — exists.
    #[cfg(windows)]
    pin_containing_module();
    // If another thread installed one between our `get` and here, keep
    // theirs and drop ours.
    let _ = RUNTIME.set(built);
    Ok(RUNTIME.get().expect("runtime just set"))
}

/// Pin the module containing the kernel so the ODBC / Go host cannot unload it
/// while the C-ABI runtime's worker threads are still running.
///
/// The process-wide runtime (see [`runtime`]) is multi-thread and is never shut
/// down: it spawns detached `kernel-c-abi` OS worker threads that outlive any
/// single ODBC connection. The ODBC DriverManager, however, loads **and
/// unloads** the driver DLL (which statically links this kernel) across
/// connections. If `FreeLibrary` unmaps the DLL while those worker threads are
/// still alive, they end up executing freed code — an Access Violation — and
/// tokio's per-thread runtime-context guard is dropped after its thread-local
/// `CONTEXT` has already been torn down, tripping the "EnterGuard values
/// dropped out of order" panic (databricks/databricks-sql-kernel#276). The
/// in-flight [`block_on_ffi`] task then never completes and the caller
/// deadlocks on its result channel.
///
/// `GetModuleHandleExW` with `FROM_ADDRESS` resolves the module that contains a
/// given code address, and `PIN` raises that module's reference count so it is
/// never unloaded for the life of the process. We pass the address of this
/// function itself (which lives in the kernel's statically-linked code), so the
/// pinned module is whatever the kernel is linked into — the ODBC driver DLL
/// for C-ABI consumers. Pinning is idempotent, so a lost runtime-build race
/// calling it twice is harmless. This is the Windows-only complement to the
/// spawn-not-`block_on` change in [`block_on_ffi`]: that keeps *foreign caller*
/// threads out of the runtime; this keeps the runtime's *own* threads from
/// being unmapped underneath themselves.
#[cfg(windows)]
fn pin_containing_module() {
    const GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS: u32 = 0x0000_0004;
    const GET_MODULE_HANDLE_EX_FLAG_PIN: u32 = 0x0000_0001;
    // Declared inline rather than via a `windows-sys` dependency: `kernel32` is
    // always linked on the MSVC target, and this avoids a new crate + its
    // cargo-deny license review for a single call.
    extern "system" {
        fn GetModuleHandleExW(
            dw_flags: u32,
            lp_module_name: *const u16,
            ph_module: *mut *mut core::ffi::c_void,
        ) -> i32;
    }
    let mut module: *mut core::ffi::c_void = core::ptr::null_mut();
    // SAFETY: with `FROM_ADDRESS`, `lp_module_name` is interpreted as an
    // address, not a wide string; we pass a valid code address (this function)
    // and a writable out-pointer. The returned handle is intentionally not
    // released — pinning is meant to be permanent.
    unsafe {
        GetModuleHandleExW(
            GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_PIN,
            pin_containing_module as usize as *const u16,
            &mut module,
        );
    }
}

/// Run a fallible async block to completion on the process-wide runtime,
/// translating the outcome into a [`KernelStatusCode`] and storing the
/// typed [`Error`] in the thread-local last-error buffer on failure.
///
/// Every `extern "C"` entry point that drives async work funnels through
/// here so the runtime-fetch, spawn, error-store, and status-map steps
/// stay in exactly one place. A panic inside the future is caught and
/// surfaced as `Internal` rather than unwinding across the FFI boundary
/// (which is UB).
///
/// **How the work is driven (and why it isn't `block_on`):** the future is
/// `spawn`ed onto the runtime's own worker threads and this thread blocks on
/// a plain `std` channel for the result. It deliberately does NOT call
/// `Runtime::block_on` on the caller thread. `block_on` *enters* the runtime
/// on the calling thread, registering tokio's runtime-context thread-local
/// there; if that thread is a native OS thread the kernel does not own (the
/// intended ODBC / Go usage — e.g. the ODBC driver's result-prefetch thread)
/// and is later torn down on Windows/MSVC, that context guard plus the
/// `LAST_ERROR` `CString` thread-local are dropped during TLS teardown in an
/// order that trips tokio's "EnterGuard dropped out of order" panic or the
/// "System allocator may not use TLS with destructors" abort
/// (databricks/databricks-sql-kernel#276). Spawning-and-blocking keeps the
/// caller thread entirely outside the runtime, so no runtime-context TLS is
/// registered on it and there is nothing to tear down.
///
/// Because the future is moved onto worker threads it must be `Send` (and its
/// output `Send`); every kernel entry point's future already is.
///
/// **Threading contract:** the C ABI must be driven from threads that are
/// NOT already running inside a Tokio runtime. A reentrant C-ABI call (a
/// consumer that invokes a `kernel_*` entry point from a callback executing
/// on a runtime worker) would block that worker on the result channel and can
/// deadlock a single-threaded runtime, so it is rejected up front with a
/// clear `Internal` error. Native (non-runtime) caller threads — the intended
/// ODBC / Go usage — are unaffected.
///
/// [`KernelStatusCode`]: crate::c_abi::KernelStatusCode
/// [`Error`]: crate::kernel_error::Error
pub(crate) fn block_on_ffi<F, Fut, T>(fut_builder: F) -> Result<T, KernelStatusCode>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::kernel_error::Result<T>> + Send,
    T: Send,
{
    use futures::future::FutureExt as _;

    error::clear_error();
    // Reject reentrancy explicitly: a consumer that invokes a `kernel_*`
    // entry point from a thread already running inside a Tokio runtime would
    // otherwise have this thread block (on the channel below) waiting for a
    // task that shares its runtime — a deadlock risk on a single-threaded
    // runtime. Surface a typed, actionable error instead. `Handle::try_current`
    // only reads (initialising it empty) the caller thread's Tokio context; it
    // never *enters* the runtime, so it leaves no runtime-context state that
    // could outlive into the thread's TLS teardown (the #276 hazard).
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

    // Drive the future on the runtime's OWN worker threads and block this
    // (foreign) caller thread on a plain `std` channel — instead of calling
    // `rt.block_on` here. This is the fix for #276: `Runtime::block_on`
    // *enters* the runtime on the calling thread, registering tokio's
    // runtime-context thread-local on it. When that thread is a native OS
    // thread the kernel does not own (e.g. the ODBC driver's prefetch
    // thread) and is later torn down on Windows/MSVC, tokio's context guard
    // and the `LAST_ERROR` `CString` thread-local are dropped during TLS
    // teardown in an order that trips tokio's "EnterGuard dropped out of
    // order" panic or the "System allocator may not use TLS with destructors"
    // abort. Spawning the work onto the runtime and blocking on a channel
    // means the caller thread NEVER enters the runtime, so no runtime-context
    // TLS is ever registered on it.
    //
    // A panic inside the future is caught here (rather than at a `block_on`
    // edge) so it can't unwind across the `extern "C"` boundary (UB) and is
    // surfaced as `Internal`.
    type PanicPayload = Box<dyn std::any::Any + Send>;
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<crate::kernel_error::Result<T>, PanicPayload>>(1);
    let future = fut_builder();
    let task = async move {
        let outcome = std::panic::AssertUnwindSafe(future).catch_unwind().await;
        // `future` (and any borrows it held) is fully dropped by the time
        // `.catch_unwind().await` resolves, i.e. before we signal completion.
        let _ = tx.send(outcome);
    };
    // SAFETY: `future` may borrow data owned by the caller (e.g. a
    // `&mut Statement` reached through a raw pointer at the entry points).
    // We extend the task's lifetime to `'static` only so it can be spawned,
    // and then BLOCK on `rx.recv()` below until the task has produced its
    // outcome — at which point the future, and every borrow it held, has
    // already been dropped. The borrows therefore never outlive their owner,
    // and the extended lifetime is not observable past this call. (Even if
    // the runtime were torn down and dropped the task without running it, the
    // drop releases the borrows before `rx.recv()` observes the closed
    // channel.) The process-wide runtime is a `'static` that never shuts down.
    let task: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>> = unsafe {
        std::mem::transmute::<
            std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>,
            std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>,
        >(Box::pin(task))
    };
    rt.spawn(task);

    match rx.recv() {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(e))) => {
            let code = KernelStatusCode::from(e.code);
            error::store_error(e);
            Err(code)
        }
        Ok(Err(panic)) => Err(handle_panic(panic)),
        Err(_) => {
            // The channel closed without a value — only reachable if the
            // runtime dropped the task before it ran (never happens for the
            // process-wide runtime). The future is dropped by then, so this is
            // sound; report it as `Internal`.
            let e = crate::kernel_error::Error::internal()
                .message("kernel C ABI runtime task was dropped before completion")
                .location(concat!(file!(), ":", line!()));
            let code = KernelStatusCode::from(e.code);
            error::store_error(e);
            Err(code)
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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

    /// `block_on_ffi` must drive the future on a runtime **worker** thread, not
    /// on the calling thread — the core of the #276 fix. `Runtime::block_on`
    /// (the pre-fix behaviour) drives the future on the *caller* thread, which
    /// registers tokio's runtime-context thread-local there; when the caller is
    /// a native OS thread the kernel does not own (the ODBC driver's prefetch
    /// thread) and is later torn down on Windows/MSVC, that TLS teardown
    /// corrupts the tokio context ("EnterGuard dropped out of order" panic /
    /// "System allocator may not use TLS with destructors" abort). Asserting
    /// the future ran on a *different* thread than the caller guards that the
    /// work is spawned onto the runtime, not `block_on`'d on the caller — so no
    /// runtime-context TLS is ever registered on a foreign caller thread.
    ///
    /// This is the portable regression guard for the fix; the end-to-end crash
    /// only manifests through the ODBC driver's driver-manager e2e suite
    /// (databricks-odbc#283), which skips the affected tests until the kernel
    /// pin carrying this fix is bumped.
    #[test]
    fn block_on_ffi_runs_future_off_the_caller_thread() {
        let caller = std::thread::current().id();
        let ran_on: std::thread::ThreadId = block_on_ffi(|| async move {
            Ok::<_, crate::kernel_error::Error>(std::thread::current().id())
        })
        .expect("trivial future completes successfully");
        assert_ne!(
            ran_on, caller,
            "block_on_ffi must spawn the future onto a runtime worker (never `block_on` it \
             on the caller thread), so no runtime-context TLS is registered on a foreign \
             caller thread — see #276",
        );
    }
}
