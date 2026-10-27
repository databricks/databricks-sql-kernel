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

use super::client::TelemetryClient;
use super::exporter::TelemetryExporter;
use crate::config::TelemetryConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Default)]
pub(super) struct TelemetryClientManager {
    clients: Mutex<HashMap<String, TelemetryClientHolder>>,
}

impl TelemetryClientManager {
    pub(super) fn global() -> &'static Self {
        static INSTANCE: OnceLock<TelemetryClientManager> = OnceLock::new();
        INSTANCE.get_or_init(Self::default)
    }

    pub(super) fn get_or_create(
        &'static self,
        host: &str,
        config: &TelemetryConfig,
        exporter: Arc<dyn TelemetryExporter>,
        bypass_cache: bool,
    ) -> Option<TelemetryClientLease> {
        if bypass_cache {
            // Used only for injected test exporters. A fresh unmanaged client
            // prevents tests that share a mock host from reusing a global host
            // client that points at another test's capturing exporter.
            return Some(TelemetryClientLease::unmanaged(TelemetryClient::new(
                config, exporter,
            )?));
        }

        let mut clients = self.clients.lock().expect("poisoned telemetry clients");
        if let Some(holder) = clients.get_mut(host) {
            holder.session_count += 1;
            holder.lease_count += 1;
            return Some(TelemetryClientLease::managed(
                host,
                Arc::clone(&holder.client),
                self,
            ));
        }

        let client = TelemetryClient::new(config, exporter)?;
        clients.insert(
            host.to_string(),
            TelemetryClientHolder {
                client: Arc::clone(&client),
                session_count: 1,
                lease_count: 1,
            },
        );
        Some(TelemetryClientLease::managed(host, client, self))
    }

    #[cfg(test)]
    fn get_or_create_test(
        &'static self,
        host: &str,
        config: &TelemetryConfig,
        exporter: Arc<dyn TelemetryExporter>,
        bypass_cache: bool,
    ) -> TelemetryClientLease {
        self.get_or_create(host, config, exporter, bypass_cache)
            .expect("telemetry runtime should start")
    }

    fn release_session(&self, host: &str) -> Option<Arc<TelemetryClient>> {
        let mut clients = self.clients.lock().expect("poisoned telemetry clients");
        let holder = clients.get_mut(host)?;
        debug_assert!(holder.session_count > 0);
        holder.session_count -= 1;
        if holder.session_count == 0 {
            Some(Arc::clone(&holder.client))
        } else {
            None
        }
    }

    fn release_lease(&self, host: &str, session_released: bool) -> LeaseDropAction {
        let mut clients = self.clients.lock().expect("poisoned telemetry clients");
        let Some(holder) = clients.get_mut(host) else {
            return LeaseDropAction::None;
        };
        if !session_released {
            debug_assert!(holder.session_count > 0);
            holder.session_count -= 1;
        }
        debug_assert!(holder.lease_count > 0);
        holder.lease_count -= 1;
        if holder.lease_count == 0 {
            return clients
                .remove(host)
                .map(|holder| LeaseDropAction::Close(holder.client))
                .unwrap_or(LeaseDropAction::None);
        }
        if !session_released && holder.session_count == 0 {
            LeaseDropAction::Flush(Arc::clone(&holder.client))
        } else {
            LeaseDropAction::None
        }
    }

    #[cfg(test)]
    fn tracked_client_count(&self) -> usize {
        self.clients
            .lock()
            .expect("poisoned telemetry clients")
            .len()
    }

    #[cfg(test)]
    fn session_count(&self, host: &str) -> Option<usize> {
        self.clients
            .lock()
            .expect("poisoned telemetry clients")
            .get(host)
            .map(|holder| holder.session_count)
    }
}

struct TelemetryClientHolder {
    client: Arc<TelemetryClient>,
    // Session.close has not finished for these leases. Reaching zero triggers
    // the bounded close-time flush.
    session_count: usize,
    // RealConnectionTelemetry may outlive Session.close through child handles.
    // Keep the client open until none can enqueue a late event.
    lease_count: usize,
}

enum LeaseDropAction {
    None,
    Flush(Arc<TelemetryClient>),
    Close(Arc<TelemetryClient>),
}

pub(super) struct TelemetryClientLease {
    host: Option<String>,
    client: Arc<TelemetryClient>,
    manager: Option<&'static TelemetryClientManager>,
    session_released: AtomicBool,
}

impl TelemetryClientLease {
    fn managed(
        host: &str,
        client: Arc<TelemetryClient>,
        manager: &'static TelemetryClientManager,
    ) -> Self {
        Self {
            host: Some(host.to_string()),
            client,
            manager: Some(manager),
            session_released: AtomicBool::new(false),
        }
    }

    fn unmanaged(client: Arc<TelemetryClient>) -> Self {
        Self {
            host: None,
            client,
            manager: None,
            session_released: AtomicBool::new(false),
        }
    }

    pub(super) fn client(&self) -> &Arc<TelemetryClient> {
        &self.client
    }

    pub(super) async fn release(&self) {
        if self.session_released.swap(true, Ordering::AcqRel) {
            return;
        }

        let client_to_flush = match (self.manager, self.host.as_deref()) {
            (Some(manager), Some(host)) => manager.release_session(host),
            _ => Some(Arc::clone(&self.client)),
        };
        if let Some(client) = client_to_flush {
            // The task continues if the caller cancels Session::close mid-flush.
            let _ = client.schedule_flush_bounded().await;
        }
    }
}

impl Drop for TelemetryClientLease {
    fn drop(&mut self) {
        let session_released = self.session_released.swap(true, Ordering::AcqRel);
        let action = match (self.manager, self.host.as_deref()) {
            (Some(manager), Some(host)) => manager.release_lease(host, session_released),
            _ => LeaseDropAction::Close(Arc::clone(&self.client)),
        };
        match action {
            LeaseDropAction::None => {}
            LeaseDropAction::Flush(client) => {
                drop(client.schedule_flush_bounded());
            }
            LeaseDropAction::Close(client) => {
                drop(client.schedule_close());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::models::TelemetryFrontendLog;
    use crate::telemetry::test_support::CapturingTelemetryExporter;
    use std::time::Duration;

    #[tokio::test]
    async fn manager_shares_clients_per_host() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter: Arc<dyn TelemetryExporter> = Arc::new(CapturingTelemetryExporter::default());
        let config = TelemetryConfig {
            enabled: Some(true),
            ..TelemetryConfig::default()
        };

        let first = manager.get_or_create_test("example.com", &config, exporter.clone(), false);
        let second = manager.get_or_create_test("example.com", &config, exporter, false);

        assert!(Arc::ptr_eq(first.client(), second.client()));
        assert_eq!(manager.tracked_client_count(), 1);
        first.release().await;
        second.release().await;
    }

    #[tokio::test]
    async fn last_lease_drop_removes_host_client() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter: Arc<dyn TelemetryExporter> = Arc::new(CapturingTelemetryExporter::default());
        let config = TelemetryConfig {
            enabled: Some(true),
            ..TelemetryConfig::default()
        };

        let first = manager.get_or_create_test("example.com", &config, exporter, false);
        let second = manager.get_or_create_test(
            "example.com",
            &config,
            Arc::new(CapturingTelemetryExporter::default()) as Arc<dyn TelemetryExporter>,
            false,
        );

        first.release().await;
        assert_eq!(manager.tracked_client_count(), 1);
        second.release().await;
        assert_eq!(manager.tracked_client_count(), 1);
        drop(first);
        drop(second);
        assert_eq!(manager.tracked_client_count(), 0);
    }

    #[tokio::test]
    async fn drop_removes_last_host_client() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter: Arc<dyn TelemetryExporter> = Arc::new(CapturingTelemetryExporter::default());
        let config = TelemetryConfig {
            enabled: Some(true),
            ..TelemetryConfig::default()
        };

        let lease = manager.get_or_create_test("example.com", &config, exporter, false);
        assert_eq!(manager.tracked_client_count(), 1);

        drop(lease);
        wait_for_tracked_client_count(manager, 0).await;
    }

    #[tokio::test]
    async fn drop_after_release_does_not_double_decrement() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter: Arc<dyn TelemetryExporter> = Arc::new(CapturingTelemetryExporter::default());
        let config = TelemetryConfig {
            enabled: Some(true),
            ..TelemetryConfig::default()
        };

        let first = manager.get_or_create_test("example.com", &config, exporter.clone(), false);
        let second = manager.get_or_create_test("example.com", &config, exporter, false);

        first.release().await;
        assert_eq!(manager.tracked_client_count(), 1);
        drop(first);
        assert_eq!(manager.tracked_client_count(), 1);

        second.release().await;
        assert_eq!(manager.tracked_client_count(), 1);
        drop(second);
        assert_eq!(manager.tracked_client_count(), 0);
    }

    #[tokio::test]
    async fn dropping_one_shared_lease_keeps_client_until_last_lease_drops() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter: Arc<dyn TelemetryExporter> = Arc::new(CapturingTelemetryExporter::default());
        let config = TelemetryConfig {
            enabled: Some(true),
            ..TelemetryConfig::default()
        };

        let first = manager.get_or_create_test("example.com", &config, exporter.clone(), false);
        let second = manager.get_or_create_test("example.com", &config, exporter, false);

        drop(first);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(manager.tracked_client_count(), 1);

        drop(second);
        wait_for_tracked_client_count(manager, 0).await;
    }

    #[tokio::test]
    async fn shared_client_flushes_only_after_last_explicit_release() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 100,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let first =
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false);
        let second =
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false);
        first.client().enqueue(TelemetryFrontendLog::default());

        first.release().await;
        assert_eq!(manager.session_count("example.com"), Some(1));
        assert!(exporter.requests().is_empty());

        second.release().await;
        assert_eq!(manager.session_count("example.com"), Some(0));
        assert_eq!(exporter.requests().len(), 1);
        drop(first);
        drop(second);
        assert_eq!(manager.session_count("example.com"), None);
    }

    #[tokio::test]
    async fn shared_client_flushes_only_after_last_drop() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 100,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let first =
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false);
        let second =
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false);
        first.client().enqueue(TelemetryFrontendLog::default());

        drop(first);
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.session_count("example.com") != Some(1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first lease should be released");
        assert!(exporter.requests().is_empty());

        drop(second);
        wait_for_tracked_client_count(manager, 0).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while exporter.requests().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("last lease should flush");
        assert_eq!(exporter.requests().len(), 1);
    }

    #[test]
    fn timer_survives_creator_session_runtime_shutdown() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 100,
            flush_interval: Duration::from_millis(20),
            ..TelemetryConfig::default()
        };
        let creator_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("creator runtime");
        let first = creator_runtime.block_on(async {
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false)
        });
        let second =
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false);
        drop(creator_runtime);
        drop(first);
        second.client().enqueue(TelemetryFrontendLog::default());

        wait_for_export_sync(&exporter);
        assert_eq!(exporter.requests().len(), 1);
        drop(second);
    }

    #[test]
    fn last_drop_outside_runtime_flushes_pending_events() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 100,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("session runtime");
        let lease = runtime.block_on(async {
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false)
        });
        lease.client().enqueue(TelemetryFrontendLog::default());
        drop(runtime);
        drop(lease);

        wait_for_export_sync(&exporter);
        assert_eq!(exporter.requests().len(), 1);
    }

    #[tokio::test]
    async fn final_session_release_keeps_client_open_for_late_observer_event() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 100,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let lease =
            manager.get_or_create_test("example.com", &config, Arc::new(exporter.clone()), false);
        lease.release().await;
        lease.client().enqueue(TelemetryFrontendLog::default());
        drop(lease);

        tokio::time::timeout(Duration::from_secs(1), async {
            while exporter.requests().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late event should flush when the observer lease drops");
        assert_eq!(exporter.requests().len(), 1);
    }

    fn wait_for_export_sync(exporter: &CapturingTelemetryExporter) {
        let started = std::time::Instant::now();
        while exporter.requests().is_empty() && started.elapsed() < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !exporter.requests().is_empty(),
            "pending event should flush"
        );
    }

    async fn wait_for_tracked_client_count(manager: &TelemetryClientManager, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if manager.tracked_client_count() == expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tracked client count should converge");
    }
}
