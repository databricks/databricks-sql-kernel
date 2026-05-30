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
///
/// **`#[cfg(feature = "oauth")]`** — the only callsites are the
/// OAuth providers' sync `AuthProvider::get_auth_header` impls
/// (`auth/oauth/m2m.rs` + `auth/oauth/u2m.rs`). PAT auth is fully
/// synchronous and doesn't need the runtime bridge.
#[cfg(feature = "oauth")]
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
