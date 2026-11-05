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
use std::panic::resume_unwind;

use tokio::runtime::{Handle, Runtime, RuntimeFlavor};

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
}
