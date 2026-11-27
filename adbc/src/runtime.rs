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

use std::future::Future;
use std::ops::Deref;
use std::panic::resume_unwind;
use std::sync::Arc;

use tokio::runtime::{Handle, Runtime, RuntimeFlavor};

pub(crate) const DEFAULT_WORKER_THREADS: usize = 2;

/// A Tokio runtime used by the synchronous ADBC surface.
///
/// Driver-owned runtimes are shut down in the background so the last ADBC
/// handle can safely be dropped from within an asynchronous context. Injected
/// runtimes remain owned and managed by their caller.
#[derive(Debug)]
pub(crate) enum RuntimeHandle {
    Owned(Option<Runtime>),
    Injected(Arc<Runtime>),
}

impl RuntimeHandle {
    pub(crate) fn create() -> std::io::Result<Arc<Self>> {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(DEFAULT_WORKER_THREADS)
            .enable_all()
            .on_thread_start(databricks_sql_kernel::block_sigpipe_on_current_thread)
            .build()
            .map(|runtime| Arc::new(Self::Owned(Some(runtime))))
    }

    pub(crate) fn injected(runtime: Arc<Runtime>) -> Arc<Self> {
        Arc::new(Self::Injected(runtime))
    }
}

impl Deref for RuntimeHandle {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(Some(runtime)) => runtime,
            Self::Owned(None) => unreachable!("owned runtime is only taken during drop"),
            Self::Injected(runtime) => runtime,
        }
    }
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        if let Self::Owned(runtime) = self {
            if let Some(runtime) = runtime.take() {
                runtime.shutdown_background();
            }
        }
    }
}

pub(crate) trait RuntimeExt {
    fn block_on_safe<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send;
}

impl RuntimeExt for Runtime {
    fn block_on_safe<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        match Handle::try_current().map(|handle| handle.runtime_flavor()) {
            Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(|| self.block_on(future)),
            Ok(RuntimeFlavor::CurrentThread) => {
                std::thread::scope(|scope| match scope.spawn(|| self.block_on(future)).join() {
                    Ok(output) => output,
                    Err(cause) => resume_unwind(cause),
                })
            }
            _ => self.block_on(future),
        }
    }
}

impl RuntimeExt for RuntimeHandle {
    fn block_on_safe<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        self.deref().block_on_safe(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private_runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn blocks_outside_tokio() {
        assert_eq!(private_runtime().block_on_safe(async { 42 }), 42);
    }

    #[test]
    fn blocks_inside_multithread_tokio() {
        let private = private_runtime();
        let caller = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(
            caller.block_on(async { private.block_on_safe(async { 42 }) }),
            42
        );
    }

    #[test]
    fn blocks_inside_current_thread_tokio() {
        let private = private_runtime();
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(
            caller.block_on(async { private.block_on_safe(async { 42 }) }),
            42
        );
    }

    #[test]
    fn owned_runtime_drops_inside_multithread_tokio() {
        let caller = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        caller.block_on(async { drop(RuntimeHandle::create().unwrap()) });
    }

    #[test]
    fn owned_runtime_drops_inside_current_thread_tokio() {
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        caller.block_on(async { drop(RuntimeHandle::create().unwrap()) });
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
    fn owned_runtime_threads_block_sigpipe() {
        assert!(!sigpipe_blocked(), "caller already blocks SIGPIPE");
        let runtime = RuntimeHandle::create().unwrap();
        let worker = runtime.spawn(async { sigpipe_blocked() });
        let blocking = runtime.spawn_blocking(sigpipe_blocked);
        let on_worker = runtime.block_on(worker).unwrap();
        let on_blocking = runtime.block_on(blocking).unwrap();
        assert!(on_worker, "runtime worker must block SIGPIPE");
        assert!(on_blocking, "blocking-pool thread must block SIGPIPE");
    }

    #[test]
    fn injected_runtime_is_not_shut_down_by_handle() {
        let runtime = Arc::new(private_runtime());
        drop(RuntimeHandle::injected(Arc::clone(&runtime)));
        assert_eq!(runtime.block_on(async { 42 }), 42);
    }

    #[test]
    fn injected_runtime_can_be_used_from_within_itself() {
        let runtime = Arc::new(private_runtime());
        let handle = RuntimeHandle::injected(Arc::clone(&runtime));
        assert_eq!(
            runtime.block_on(async { handle.block_on_safe(async { 42 }) }),
            42
        );
    }
}
