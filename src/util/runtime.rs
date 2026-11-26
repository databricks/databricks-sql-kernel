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

//! Tokio runtime bridging utilities for sync ↔ async boundaries.

/// Bridge a sync call site to an async kernel operation, safely
/// whether or not the caller is already inside a tokio runtime.
///
/// Naively calling `runtime_handle.block_on(fut)` works when invoked
/// from outside any tokio runtime, but panics ("Cannot start a
/// runtime from within a runtime") when the kernel is being driven
/// by a wrapper that already owns a `block_on` higher up the stack —
/// exactly what the PyO3 binding does for every kernel call (see
/// issue #19).
///
/// `tokio::task::block_in_place` tells the runtime "I'm about to do
/// something blocking; move other tasks to a different worker
/// thread." After that, `runtime_handle.block_on(fut)` is safe
/// because the runtime considers this thread off-budget. We pass the
/// captured handle (not `Handle::current()`) so the function honors
/// the documented invariant that callers thread the kernel's session
/// handle through to here.
///
/// **Requires a multi-threaded runtime when called from inside a
/// runtime context.** `block_in_place` panics on current-thread
/// runtimes. The kernel's session runtime is multi-thread (see
/// `Builder::new_multi_thread` in the PyO3 wrapper and the
/// `flavor = "multi_thread"` annotation on the regression test for
/// issue #19), and any caller driving the kernel from a runtime
/// context should also be on multi-thread for the same concurrent-IO
/// reasons.
pub(crate) fn block_on_in_runtime<F>(runtime_handle: &tokio::runtime::Handle, fut: F) -> F::Output
where
    F: std::future::Future,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        // Inside a runtime: escape via block_in_place + the
        // captured handle.
        tokio::task::block_in_place(|| runtime_handle.block_on(fut))
    } else {
        // Outside any runtime (direct Rust-crate use, e.g. the
        // ODBC C ABI shim's sync entry points): block_on the
        // captured handle directly. `block_in_place` would panic
        // on a current-thread runtime, so we keep the old path for
        // callers that aren't already in a runtime.
        runtime_handle.block_on(fut)
    }
}

/// Bridge from a sync iterator to an async kernel future when no
/// runtime handle has been threaded through.
///
/// Three cases:
///
/// - **No tokio runtime on this thread** (plain Rust use, the C ABI
///   shim's sync entry points): falls back to
///   `futures::executor::block_on`. Sufficient for readers whose
///   futures are immediately ready (in-memory data); the only safe
///   option when nobody set up a reactor.
///
/// - **Multi-thread tokio runtime**: escapes via `block_in_place` +
///   the discovered handle (same shape as
///   [`block_on_in_runtime`]). Tells the runtime "I'm about to do
///   something blocking" so other tasks can migrate to a different
///   worker, then drives the future to completion. This is the
///   primary CloudFetch path under napi-rs.
///
/// - **Current-thread tokio runtime**: `block_in_place` panics here
///   ("can call blocking only when running on the multi-threaded
///   runtime"). For in-memory readers (immediately-ready futures)
///   `futures::executor::block_on` still works because the future
///   never yields, so we fall back to it. For futures that genuinely
///   await tokio primitives (CloudFetch / timer / Notify), this path
///   cannot bridge safely — sync iteration over CloudFetch from a
///   current-thread runtime is a logically impossible request. The
///   fallback at least doesn't crash unit tests that iterate
///   in-memory mock readers from inside `#[tokio::test]`.
///
/// Used by [`crate::result_stream::OwnedBatchIter`] and
/// [`crate::reader::ResultReaderAdapter`] — sync iterator adapters
/// over `Box<dyn ResultReader>` where the caller couldn't thread a
/// handle through at construction time (e.g. arrow's
/// `RecordBatchReader` impl). Async-aware kernel callers should
/// drive the reader via `next_batch().await` instead.
pub(crate) fn block_on_current_runtime<F>(fut: F) -> F::Output
where
    F: std::future::Future,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(fut))
            }
            // CurrentThread (or any future single-worker flavor):
            // block_in_place panics. Fall back to executor::block_on
            // — works for immediately-ready futures (in-memory
            // readers), and there's no safe option for futures that
            // need this runtime's reactor anyway.
            _ => futures::executor::block_on(fut),
        },
        Err(_) => futures::executor::block_on(fut),
    }
}

/// Tokio normally gives runtime workers a 2 MiB stack, but without an explicit
/// override its thread builder honours the embedding process's `RUST_MIN_STACK`.
/// Native consumers may set that variable for their own thread pools; the
/// kernel-owned runtimes (the C-ABI runtime in [`crate::c_abi`] and the
/// telemetry runtime in [`crate::telemetry`]) must not be shrunk below Tokio's
/// supported default, or a real HTTP export / query can overflow the worker
/// stack and abort the process.
///
/// This is a single source of truth deliberately shared by both runtimes: two
/// independent copies of a safety-critical stack floor can silently drift (see
/// databricks/databricks-sql-kernel#328, the floor-not-cap bug).
pub(crate) const RUNTIME_THREAD_STACK_SIZE: usize = 2 * 1024 * 1024;

/// Resolve the worker-thread stack size for a kernel-owned tokio runtime from
/// the ambient `RUST_MIN_STACK` value (parsed to `Some(bytes)`, or `None` when
/// unset or unparseable).
///
/// [`RUNTIME_THREAD_STACK_SIZE`] (2 MiB) is a **floor, not a cap**: a too-small
/// ambient `RUST_MIN_STACK` is clamped up to the floor (preserving
/// databricks/databricks-sql-kernel#326), while a host that deliberately asked
/// for *more* than 2 MiB keeps its larger stack rather than being silently
/// shrunk (databricks/databricks-sql-kernel#328).
pub(crate) fn runtime_thread_stack_size(ambient_rust_min_stack: Option<usize>) -> usize {
    ambient_rust_min_stack
        .unwrap_or(0)
        .max(RUNTIME_THREAD_STACK_SIZE)
}

/// Read and parse the ambient `RUST_MIN_STACK` env var into `Some(bytes)`, or
/// `None` when it is unset or not a valid `usize`.
pub(crate) fn ambient_rust_min_stack() -> Option<usize> {
    std::env::var("RUST_MIN_STACK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
}

/// `on_thread_start` hook for kernel-owned runtimes. std's `write_vectored` is a plain
/// `writev`, so a write after a peer reset raises SIGPIPE on this thread, and non-Rust hosts
/// that don't ignore it (Go) die. Only kernel threads change; the write still fails with EPIPE.
#[cfg(unix)]
pub(crate) fn block_sigpipe_on_current_thread() {
    // SAFETY: initialises a stack-local `sigset_t` and changes only this thread's mask.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGPIPE);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

/// Windows has no SIGPIPE.
#[cfg(not(unix))]
pub(crate) fn block_sigpipe_on_current_thread() {}

/// Whether SIGPIPE is in the calling thread's `SigBlk` mask.
#[cfg(all(test, target_os = "linux"))]
fn sigpipe_blocked() -> bool {
    let path = "/proc/thread-self/status";
    let status = std::fs::read_to_string(path).unwrap();
    let mask = status
        .lines()
        .find_map(|l| l.strip_prefix("SigBlk:"))
        .unwrap();
    let mask = u64::from_str_radix(mask.trim(), 16).unwrap();
    (mask & (1u64 << (libc::SIGPIPE - 1))) != 0
}

/// Asserts that a worker and a blocking-pool thread of `handle` block SIGPIPE.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn assert_runtime_threads_block_sigpipe(handle: &tokio::runtime::Handle) {
    assert!(!sigpipe_blocked(), "caller already blocks SIGPIPE");
    let worker = handle.spawn(async { sigpipe_blocked() });
    let blocking = handle.spawn_blocking(sigpipe_blocked);
    let on_worker = futures::executor::block_on(worker).unwrap();
    let on_blocking = futures::executor::block_on(blocking).unwrap();
    assert!(on_worker, "runtime worker must block SIGPIPE");
    assert!(on_blocking, "blocking-pool thread must block SIGPIPE");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Floor preserved (databricks/databricks-sql-kernel#326): a too-small or
    /// unset ambient `RUST_MIN_STACK` is clamped UP to the 2 MiB floor so a
    /// real HTTP export / query doesn't overflow the worker stack.
    #[test]
    fn runtime_stack_size_clamps_small_rust_min_stack_up_to_floor() {
        assert_eq!(
            runtime_thread_stack_size(Some(16_384)),
            RUNTIME_THREAD_STACK_SIZE,
            "a RUST_MIN_STACK below the floor must clamp up to 2 MiB"
        );
        assert_eq!(
            runtime_thread_stack_size(None),
            RUNTIME_THREAD_STACK_SIZE,
            "an unset RUST_MIN_STACK must default to the 2 MiB floor"
        );
    }

    /// Floor, not cap (databricks/databricks-sql-kernel#328): a host that
    /// deliberately asked for more than 2 MiB keeps its larger stack rather
    /// than being silently shrunk. The databricks-odbc partner runs with
    /// `RUST_MIN_STACK=3194880`.
    #[test]
    fn runtime_stack_size_preserves_larger_rust_min_stack() {
        let larger = RUNTIME_THREAD_STACK_SIZE + 1_024 * 1_024;
        assert_eq!(
            runtime_thread_stack_size(Some(larger)),
            larger,
            "a RUST_MIN_STACK larger than the 2 MiB floor must be preserved, not shrunk"
        );
    }
}
