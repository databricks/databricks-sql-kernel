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

use std::ops::Deref;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Tokio runtime that never blocks on its workers while the GIL may be held.
///
/// A plain `Runtime` drop blocks until every worker exits, but a worker
/// forwarding a log record through `pyo3_log` waits for the GIL, so
/// dropping it with the GIL held deadlocks. `Session.close()` instead calls
/// [`shutdown_timeout`](Self::shutdown_timeout) with the GIL released.
/// `Drop` — reached when Python frees a handle — uses `shutdown_background`,
/// so workers may still run briefly after it returns.
pub(crate) struct KernelRuntime(Option<Runtime>);

impl KernelRuntime {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self(Some(runtime))
    }

    /// Shut down, waiting up to `timeout` for the workers to exit. Call
    /// with the GIL released: workers may need it to finish.
    pub(crate) fn shutdown_timeout(mut self, timeout: Duration) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_timeout(timeout);
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
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn drop_with_gil_held_does_not_wait_for_gil_blocked_worker() {
        Python::initialize();
        let (done_tx, done_rx) = mpsc::channel();
        // Own thread so a regression fails the test instead of hanging it.
        std::thread::spawn(move || {
            Python::attach(|_py| {
                let runtime = KernelRuntime::new(
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(1)
                        .build()
                        .expect("build runtime"),
                );
                let (started_tx, started_rx) = mpsc::channel();
                // Stands in for a worker forwarding a record via pyo3_log.
                runtime.spawn(async move {
                    started_tx.send(()).expect("signal started");
                    Python::attach(|_py| {});
                });
                started_rx.recv().expect("worker started");
                drop(runtime);
            });
            done_tx.send(()).expect("signal done");
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(10)).is_ok(),
            "dropping the runtime with the GIL held deadlocked on its worker"
        );
    }
}
