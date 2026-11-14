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

//! Helpers that avoid needless GIL hand-offs.
//!
//! Each `py.detach` releases the GIL and re-acquires it on return. With
//! hundreds of Python threads sharing one connection pool, every
//! re-acquire waits behind the other threads, so a call that needs no
//! I/O is cheaper done with the GIL held.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyModule;
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use tokio::runtime::Runtime;

static THREADING: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

/// Incremented in the child after each `fork()`, where the forking thread
/// becomes the main thread, so answers cached before it are recomputed.
static FORK_GENERATION: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// This thread's `on_main_thread` answer and the fork generation it was
    /// computed in.
    static MAIN_THREAD: Cell<Option<(u64, bool)>> = const { Cell::new(None) };
}

/// Whether `on_main_thread` may cache: only once forks are tracked.
static CACHE_ENABLED: AtomicBool = AtomicBool::new(false);

/// Recompute cached `on_main_thread` answers in forked children. Called
/// once, at module import. Without it (registration failed), every call
/// asks `threading`, as before caching.
pub(crate) fn install_fork_handler() {
    #[cfg(unix)]
    {
        extern "C" fn after_fork_in_child() {
            FORK_GENERATION.fetch_add(1, Ordering::Relaxed);
        }
        static INSTALL: std::sync::Once = std::sync::Once::new();
        // SAFETY: registers a handler that only bumps an atomic, which is
        // async-signal-safe as `fork()` requires of child handlers.
        INSTALL.call_once(|| {
            if unsafe { libc::pthread_atfork(None, None, Some(after_fork_in_child)) } == 0 {
                CACHE_ENABLED.store(true, Ordering::Relaxed);
            }
        });
    }
    #[cfg(not(unix))]
    CACHE_ENABLED.store(true, Ordering::Relaxed);
}

/// Whether the calling thread is Python's main thread, the only one that
/// runs signal handlers. Errs towards `true` (a wrong `true` only keeps
/// signal checks on).
///
/// Either id may match: `threading` refreshes the main thread's `ident`
/// after `fork()` but not its `native_id`, while greenlet libraries
/// (gevent, eventlet) patch `get_ident` but not the OS-level `native_id`.
/// The answer only changes for a thread at `fork()`, so it is cached per
/// thread until the next one; asking `threading` costs a few Python calls
/// under the GIL on every `execute()`.
pub(crate) fn on_main_thread(py: Python<'_>) -> bool {
    let cache = CACHE_ENABLED.load(Ordering::Relaxed);
    let generation = FORK_GENERATION.load(Ordering::Relaxed);
    if let (true, Some((cached_generation, main))) = (cache, MAIN_THREAD.get()) {
        if cached_generation == generation {
            return main;
        }
    }
    let check = || -> PyResult<bool> {
        let threading = THREADING
            .get_or_try_init(py, || py.import("threading").map(Bound::unbind))?
            .bind(py);
        let main = threading.call_method0("main_thread")?;
        if threading
            .call_method0("get_ident")?
            .eq(main.getattr("ident")?)?
        {
            return Ok(true);
        }
        threading
            .call_method0("get_native_id")?
            .eq(main.getattr("native_id")?)
    };
    match check() {
        Ok(main) => {
            MAIN_THREAD.set(Some((generation, main)));
            main
        }
        Err(_) => true,
    }
}

/// Drive `fut` to completion, polling it once with the GIL held first and
/// releasing the GIL only if it isn't ready.
///
/// Only for futures whose first poll never blocks the thread: no network
/// I/O and no token refresh before the first `.await` that can pend.
pub(crate) fn run_ready_first<F>(py: Python<'_>, runtime: &Runtime, fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    let mut fut = std::pin::pin!(fut);
    let first = {
        let _context = runtime.enter();
        fut.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    };
    match first {
        Poll::Ready(output) => output,
        Poll::Pending => py.detach(|| runtime.block_on(fut)),
    }
}

#[cfg(test)]
mod tests {
    use super::{on_main_thread, run_ready_first, FORK_GENERATION, MAIN_THREAD};
    use pyo3::prelude::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[test]
    fn main_thread_answer_is_cached_until_a_fork() {
        super::install_fork_handler();
        assert!(super::CACHE_ENABLED.load(Ordering::Relaxed));
        Python::initialize();
        // `threading` takes the thread that imports it first as the main
        // thread; the thread spawned below is never that one.
        Python::attach(|py| py.import("threading").map(drop)).unwrap();
        std::thread::spawn(|| {
            Python::attach(|py| {
                assert!(!on_main_thread(py));
                let (generation, main) = MAIN_THREAD.get().expect("cached");
                assert!(!main);
                // A stale answer from before a fork is recomputed.
                MAIN_THREAD.set(Some((generation, true)));
                assert!(on_main_thread(py));
                FORK_GENERATION.fetch_add(1, Ordering::Relaxed);
                assert!(!on_main_thread(py));
            })
        })
        .join()
        .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn forked_children_see_a_new_generation() {
        super::install_fork_handler();
        let before = FORK_GENERATION.load(Ordering::Relaxed);
        // SAFETY: the child only reads an atomic and exits.
        match unsafe { libc::fork() } {
            0 => unsafe {
                // `>`: another test may bump the counter in parallel.
                let bumped = FORK_GENERATION.load(Ordering::Relaxed) > before;
                libc::_exit(if bumped { 0 } else { 1 })
            },
            pid => {
                let mut status = 0;
                assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
                assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
            }
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime")
    }

    #[test]
    fn ready_future_completes_on_first_poll() {
        Python::initialize();
        let runtime = runtime();
        let out = Python::attach(|py| run_ready_first(py, &runtime, async { 7 }));
        assert_eq!(out, 7);
    }

    #[test]
    fn pending_future_finishes_on_the_runtime() {
        Python::initialize();
        let runtime = runtime();
        let (tx, rx) = tokio::sync::oneshot::channel();
        runtime.spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tx.send(7);
        });
        // The first poll registers a no-op waker; block_on must re-poll.
        let out = Python::attach(|py| run_ready_first(py, &runtime, async { rx.await.ok() }));
        assert_eq!(out, Some(7));
    }
}
