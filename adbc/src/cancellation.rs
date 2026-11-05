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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use adbc_core::error::{Result, Status};

use crate::error::adbc_error;

type Cancel = Arc<dyn Fn() -> Result<()> + Send + Sync>;
type IsActive = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone)]
struct Entry {
    cancel: Cancel,
    is_active: IsActive,
    cancelled: Arc<AtomicBool>,
}

#[derive(Default)]
pub(crate) struct CancellationRegistry {
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, Entry>>,
}

impl CancellationRegistry {
    pub(crate) fn register(self: &Arc<Self>, cancel: Cancel) -> Result<OperationGuard> {
        self.register_while(cancel, Arc::new(|| true))
    }

    pub(crate) fn register_while(
        self: &Arc<Self>,
        cancel: Cancel,
        is_active: IsActive,
    ) -> Result<OperationGuard> {
        self.register_while_with_signal(cancel, is_active, Arc::new(AtomicBool::new(false)))
    }

    pub(crate) fn register_while_with_signal(
        self: &Arc<Self>,
        cancel: Cancel,
        is_active: IsActive,
        cancelled: Arc<AtomicBool>,
    ) -> Result<OperationGuard> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.active
            .lock()
            .map_err(|_| adbc_error("connection cancellation lock is poisoned", Status::Internal))?
            .insert(
                id,
                Entry {
                    cancel,
                    is_active,
                    cancelled: Arc::clone(&cancelled),
                },
            );
        Ok(OperationGuard {
            registry: Arc::clone(self),
            id,
            cancelled,
        })
    }

    pub(crate) fn cancel(&self) -> Result<()> {
        let active = self
            .active
            .lock()
            .map_err(|_| adbc_error("connection cancellation lock is poisoned", Status::Internal))?
            .values()
            .filter(|entry| (entry.is_active)())
            .cloned()
            .collect::<Vec<_>>();
        if active.is_empty() {
            return Err(adbc_error(
                "connection has no active operation to cancel",
                Status::InvalidState,
            ));
        }
        let mut first_error = None;
        for entry in active {
            if !(entry.is_active)() {
                continue;
            }
            match (entry.cancel)() {
                Ok(()) => entry.cancelled.store(true, Ordering::Release),
                Err(error) if first_error.is_none() => {
                    first_error = Some(adbc_error(
                        format!("connection could not cancel an active operation: {error}"),
                        Status::Unknown,
                    ));
                }
                Err(_) => {}
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn remove(&self, id: u64) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&id);
        }
    }
}

pub(crate) struct OperationGuard {
    registry: Arc<CancellationRegistry>,
    id: u64,
    cancelled: Arc<AtomicBool>,
}

impl OperationGuard {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.registry.remove(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[test]
    fn idle_connection_returns_invalid_state() {
        let registry = CancellationRegistry::default();
        assert_eq!(registry.cancel().unwrap_err().status, Status::InvalidState);
    }

    #[test]
    fn cancel_dispatches_every_active_operation_without_holding_registry_lock() {
        let registry = Arc::new(CancellationRegistry::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let guards = (0..2)
            .map(|_| {
                let calls = Arc::clone(&calls);
                let registry = Arc::clone(&registry);
                Arc::clone(&registry)
                    .register(Arc::new(move || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        let reentrant = registry.register(Arc::new(|| Ok(())))?;
                        drop(reentrant);
                        Ok(())
                    }))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        registry.cancel().unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        drop(guards);
        assert_eq!(registry.cancel().unwrap_err().status, Status::InvalidState);
    }

    #[test]
    fn one_failure_does_not_prevent_other_operations_from_being_cancelled() {
        let registry = Arc::new(CancellationRegistry::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let _failing = registry
            .register(Arc::new(|| Err(adbc_error("cancel failed", Status::IO))))
            .unwrap();
        let callback_calls = Arc::clone(&calls);
        let _successful = registry
            .register(Arc::new(move || {
                callback_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }))
            .unwrap();

        assert_eq!(registry.cancel().unwrap_err().status, Status::Unknown);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn completed_operation_is_no_longer_cancelled() {
        let registry = Arc::new(CancellationRegistry::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let guard = registry
            .register(Arc::new(move || {
                callback_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }))
            .unwrap();
        drop(guard);

        assert_eq!(registry.cancel().unwrap_err().status, Status::InvalidState);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn successful_dispatch_marks_operation_cancelled() {
        let registry = Arc::new(CancellationRegistry::default());
        let guard = registry.register(Arc::new(|| Ok(()))).unwrap();

        assert!(!guard.is_cancelled());
        registry.cancel().unwrap();
        assert!(guard.is_cancelled());
    }

    #[test]
    fn failed_dispatch_does_not_mark_operation_cancelled() {
        let registry = Arc::new(CancellationRegistry::default());
        let guard = registry
            .register(Arc::new(|| Err(adbc_error("cancel failed", Status::IO))))
            .unwrap();

        assert!(registry.cancel().is_err());
        assert!(!guard.is_cancelled());
    }

    #[test]
    fn inactive_registration_is_equivalent_to_idle() {
        let registry = Arc::new(CancellationRegistry::default());
        let _guard = registry
            .register_while(Arc::new(|| Ok(())), Arc::new(|| false))
            .unwrap();

        assert_eq!(registry.cancel().unwrap_err().status, Status::InvalidState);
    }
}
