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
use std::future::Future;
use std::task::{Context, Poll, Waker};
use tokio::runtime::Runtime;

static THREADING: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

/// Whether the calling thread is Python's main thread, the only one that
/// runs signal handlers. Errs towards `true` (a wrong `true` only keeps
/// signal checks on).
///
/// Either id may match: `threading` refreshes the main thread's `ident`
/// after `fork()` but not its `native_id`, while greenlet libraries
/// (gevent, eventlet) patch `get_ident` but not the OS-level `native_id`.
pub(crate) fn on_main_thread(py: Python<'_>) -> bool {
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
    check().unwrap_or(true)
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
    use super::run_ready_first;
    use pyo3::prelude::*;
    use std::time::Duration;

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
