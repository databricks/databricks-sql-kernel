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
