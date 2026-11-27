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

//! `KernelRuntime` — the session-owned tokio runtime.

use pyo3::prelude::*;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Upper bound on how long an explicit `close()` waits for the runtime to
/// stop. Shutdown also waits on blocking-pool work (e.g. the OAuth refresh
/// in `src/auth/oauth/token_store.rs`), so this caps `close()` latency.
const CLOSE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Tokio runtime that never blocks on its workers while the GIL may be held.
///
/// A plain `Runtime` drop blocks until every worker exits, but a worker
/// forwarding a log record through `pyo3_log` waits for the GIL, so
/// dropping it with the GIL held deadlocks. Explicit `close()` calls go
/// through [`release`](Self::release), which waits with the GIL released.
/// `Drop` — reached when Python frees a handle — uses `shutdown_background`,
/// so workers may still run briefly after it returns.
///
/// Handle structs must declare their kernel object before this field:
/// fields drop in order, and kernel `Drop` impls queue cleanup on it.
pub(crate) struct KernelRuntime(Option<Runtime>);

impl KernelRuntime {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self(Some(runtime))
    }

    /// Builds a session's runtime. Multi-thread so CloudFetch chunk downloads
    /// can overlap with the main poll loop; worker count is tokio's default.
    pub(crate) fn build() -> PyResult<Self> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("dbx-sql-kernel")
            .on_thread_start(databricks_sql_kernel::block_sigpipe_on_current_thread)
            .build()
            .map(Self::new)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to build tokio runtime: {e}"
                ))
            })
    }

    /// Release a handle at the end of an explicit `close()`. If it was the
    /// last one, shut down and wait for the workers with the GIL released,
    /// so none is still logging into Python after `close()` returns.
    pub(crate) fn release(runtime: Arc<Self>, py: Python<'_>) {
        if let Ok(mut runtime) = Arc::try_unwrap(runtime) {
            if let Some(runtime) = runtime.0.take() {
                py.detach(|| runtime.shutdown_timeout(CLOSE_SHUTDOWN_TIMEOUT));
            }
        }
    }
}

impl Deref for KernelRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Runtime {
        self.0.as_ref().expect("runtime is only taken on shutdown")
    }
}

impl Drop for KernelRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KernelRuntime;
    use pyo3::prelude::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    /// One-worker runtime whose worker is mid-task and needs the GIL, which
    /// the calling (attached) thread holds. Stands in for a worker
    /// forwarding a record via pyo3_log. The flag is set once it got the GIL.
    fn runtime_with_gil_blocked_worker() -> (KernelRuntime, Arc<AtomicBool>) {
        let runtime = KernelRuntime::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .build()
                .expect("build runtime"),
        );
        let finished = Arc::new(AtomicBool::new(false));
        let flag = finished.clone();
        let (started_tx, started_rx) = mpsc::channel();
        runtime.spawn(async move {
            started_tx.send(()).expect("signal started");
            Python::attach(|_py| {});
            flag.store(true, Ordering::SeqCst);
        });
        started_rx.recv().expect("worker started");
        (runtime, finished)
    }

    /// Runs `f` on its own attached thread, so a regression fails the test
    /// instead of hanging it.
    fn run_attached<T: Send + 'static>(f: fn(Python<'_>) -> T, what: &str) -> T {
        Python::initialize();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let out = Python::attach(f);
            done_tx.send(out).expect("signal done");
        });
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| panic!("{what} deadlocked on a GIL-blocked worker"))
    }

    /// Whether SIGPIPE (13 on Linux) is in the calling thread's `SigBlk` mask.
    #[cfg(target_os = "linux")]
    fn sigpipe_blocked() -> bool {
        let path = "/proc/thread-self/status";
        let status = std::fs::read_to_string(path).unwrap();
        let mask = status
            .lines()
            .find_map(|line| line.strip_prefix("SigBlk:"))
            .unwrap();
        let mask = u64::from_str_radix(mask.trim(), 16).unwrap();
        (mask & (1 << (13 - 1))) != 0
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn session_runtime_threads_block_sigpipe() {
        assert!(!sigpipe_blocked(), "caller already blocks SIGPIPE");
        let runtime = KernelRuntime::build().expect("build runtime");
        let worker = runtime.spawn(async { sigpipe_blocked() });
        let blocking = runtime.spawn_blocking(sigpipe_blocked);
        let on_worker = runtime.block_on(worker).unwrap();
        let on_blocking = runtime.block_on(blocking).unwrap();
        assert!(on_worker, "runtime worker must block SIGPIPE");
        assert!(on_blocking, "blocking-pool thread must block SIGPIPE");
    }

    #[test]
    fn drop_with_gil_held_does_not_wait_for_gil_blocked_worker() {
        run_attached(
            |_py| drop(runtime_with_gil_blocked_worker().0),
            "dropping the runtime with the GIL held",
        );
    }

    #[test]
    fn release_waits_for_gil_blocked_worker_to_finish() {
        let finished_at_return = run_attached(
            |py| {
                let (runtime, finished) = runtime_with_gil_blocked_worker();
                KernelRuntime::release(Arc::new(runtime), py);
                // Still attached: a worker that hasn't run yet can't now.
                finished.load(Ordering::SeqCst)
            },
            "release()",
        );
        assert!(
            finished_at_return,
            "release() returned before the worker finished"
        );
    }
}
