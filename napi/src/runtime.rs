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

//! Captured tokio `Handle` for napi-rs's process-global runtime.
//!
//! Per the napi-rs patterns doc (pattern #2): the first time any
//! `#[napi] async fn` runs, we are guaranteed to be on napi-rs's tokio
//! runtime. We snapshot the current `Handle` then and stash a clone in
//! a process-static `OnceCell`. Every subsequent kernel construction
//! reads the captured handle and hands a clone to the kernel, so
//! Drop-time cleanup (which runs on the V8 GC thread, *outside* any
//! tokio context) can still `spawn` cleanup tasks onto the same
//! runtime napi-rs is driving.
//!
//! `Handle::current()` MUST NOT be called from a synchronous JS-thread
//! entry point or from module init — both run before napi-rs has
//! constructed its runtime and would panic. `get()` returns `None` in
//! that case so callers can surface a useful error rather than abort.

use once_cell::sync::OnceCell;
use tokio::runtime::Handle;

static RUNTIME_HANDLE: OnceCell<Handle> = OnceCell::new();

/// Capture the current tokio runtime handle on first call, return a
/// reference to the captured clone on subsequent calls.
///
/// MUST be called from inside a `#[napi] async fn` body (or any other
/// tokio runtime context); otherwise `Handle::current()` panics on the
/// very first call. Subsequent calls are infallible and lock-free.
///
/// Round 1b has no async entry points that exercise this yet; Round 2
/// will call it from `Database::open()` and other `#[napi] async fn`s.
/// `try_get_handle` has a test in this crate so it's reachable from
/// the test build; `get_handle()` itself cannot be tested without a
/// real tokio runtime context and is intentionally dead until Round 2,
/// so it keeps its own `allow(dead_code)`.
///
/// # PERF NOTE — pool sizing & sharing
///
/// The handle returned here is napi-rs's *process-global*
/// `tokio::runtime::Runtime` (created via
/// `tokio::runtime::Runtime::new()` in
/// `napi-2.16.17/src/tokio_runtime.rs:11`). Default worker-thread count
/// is `num_cpus`. **The host's `num_cpus` is the raw CPU count, not the
/// cgroup CPU quota** — containers / serverless functions with a CPU
/// limit (k8s `resources.limits.cpu`, Lambda 0.5-vCPU plans) still get
/// a pool sized for the underlying host, which over-provisions threads
/// and inflates context-switch overhead.
///
/// Override knobs (must be in effect *before* napi-rs initializes its
/// runtime, i.e. before the first `#[napi] async fn` runs):
/// - `TOKIO_WORKER_THREADS` env var, read by `tokio::runtime::Runtime::new()`.
///   Set this in the JS-side launcher / Docker `ENV` / k8s pod spec.
/// - `napi::create_custom_tokio_runtime(rt)` from a
///   `#[napi::module_init]` block — fully programmatic, but only the
///   binding crate can call it.
///
/// Note: this is NOT `NAPI_RS_WORKER_THREADS`; that name does not exist
/// in napi-rs 2.x. Caveat for anyone copying patterns from older
/// napi-rs docs.
///
/// # Pool-sharing divergence from pyo3
///
/// pyo3 builds a tokio runtime per `Session`. This binding shares
/// **one** global pool across every `Connection` / `Statement` /
/// `ResultStream`. CloudFetch (`src/reader/cloudfetch/`) spawns N+5
/// concurrent tasks per active statement via the captured handle — at
/// scale (many parallel statements) the shared pool contention is the
/// dominant runtime overhead, whereas pyo3's per-session isolation
/// avoids cross-statement interference. PR #25's sizing benchmarks
/// must account for this when comparing napi vs pyo3 absolute numbers.
#[allow(dead_code)]
pub(crate) fn get_handle() -> &'static Handle {
    RUNTIME_HANDLE.get_or_init(Handle::current)
}

/// Non-panicking accessor — returns `None` if `get_handle()` has not
/// been called yet. Drop impls and other GC-thread call sites use this
/// to short-circuit cleanup when no async entry point has ever run
/// (i.e. there is no kernel state that needs closing either).
pub(crate) fn try_get_handle() -> Option<&'static Handle> {
    RUNTIME_HANDLE.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: we cannot exercise `get_handle()` from the unit-test binary
    // because that would require running inside a tokio runtime context
    // AND would race with `try_get_handle_returns_none_before_init`
    // (the OnceCell is process-static). Round 2's `Database::open()`
    // integration tests will exercise the populated path end-to-end.
    //
    // What we CAN test in isolation: the cold-cache accessor returns
    // `None` when nothing has populated the OnceCell yet. This is the
    // case Drop impls rely on (GC-thread cleanup with no live async
    // entry points) and is the regression-risk path.
    #[test]
    fn try_get_handle_returns_none_before_init() {
        // `cargo test --lib` runs each test in its own process by
        // default, so we can assume the OnceCell is empty here.
        assert!(try_get_handle().is_none());
    }
}
