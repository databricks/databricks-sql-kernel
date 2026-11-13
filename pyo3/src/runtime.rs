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

//! `KernelRuntime` — the tokio runtime that drives every session.

use pyo3::prelude::*;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

type Slot = Option<(u32, Arc<KernelRuntime>)>;

/// The process-wide runtime, tagged with the pid that built it.
static SHARED: Mutex<Slot> = Mutex::new(None);

fn lock_slot() -> MutexGuard<'static, Slot> {
    SHARED.lock().unwrap_or_else(|e| e.into_inner())
}

fn installed(slot: &Slot, pid: u32) -> Option<Arc<KernelRuntime>> {
    slot.as_ref()
        .filter(|(owner, _)| *owner == pid)
        .map(|(_, runtime)| runtime.clone())
}

/// Runtime shared by every `Session` in the process.
///
/// One worker pool per process rather than per session: a per-session
/// multi-thread runtime starts a worker per CPU for each connection, so
/// 200 sessions on 16 cores held 3,200 kernel threads. The static keeps a
/// reference, so `release` never shuts this runtime down.
///
/// Worker threads don't survive `fork()`, so a child process builds its
/// own runtime. The parent's is leaked rather than dropped, because
/// dropping it would wait on threads that don't exist in the child.
///
/// The lock is never held while building: a `fork()` taken while it is
/// held would leave it locked forever in the child.
pub(crate) fn shared() -> std::io::Result<Arc<KernelRuntime>> {
    let pid = std::process::id();
    if let Some(runtime) = installed(&lock_slot(), pid) {
        return Ok(runtime);
    }
    let built = Arc::new(KernelRuntime::new(
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("dbx-sql-kernel")
            .build()?,
    ));
    let mut slot = lock_slot();
    // Another thread may have installed one meanwhile; ours, still idle,
    // is dropped after the lock is released.
    if let Some(runtime) = installed(&slot, pid) {
        return Ok(runtime);
    }
    if let Some((_, inherited)) = slot.replace((pid, built.clone())) {
        std::mem::forget(inherited);
    }
    Ok(built)
}

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

    /// Release a handle at the end of an explicit `close()`. If it was the
    /// last one, shut down and wait for the workers with the GIL released,
    /// so none is still logging into Python after `close()` returns. The
    /// [`shared`] runtime is never the last one.
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

    #[test]
    fn shared_runtime_is_reused_and_survives_release() {
        let still_running = run_attached(
            |py| {
                let first = super::shared().expect("shared runtime");
                let second = super::shared().expect("shared runtime");
                assert!(Arc::ptr_eq(&first, &second));
                KernelRuntime::release(first, py);
                py.detach(|| second.block_on(async { 7 })) == 7
            },
            "release() of the shared runtime",
        );
        assert!(still_running, "shared runtime stopped after release()");
    }
}
