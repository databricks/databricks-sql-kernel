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
    ) -> TelemetryClientLease {
        if bypass_cache {
            // Used only for injected test exporters. A fresh unmanaged client
            // prevents tests that share a mock host from reusing a global host
            // client that points at another test's capturing exporter.
            return TelemetryClientLease::unmanaged(TelemetryClient::new(config, exporter));
        }

        let mut clients = self.clients.lock().expect("poisoned telemetry clients");
        if let Some(holder) = clients.get_mut(host) {
            holder.ref_count += 1;
            return TelemetryClientLease::managed(host, Arc::clone(&holder.client), self);
        }

        let client = TelemetryClient::new(config, exporter);
        clients.insert(
            host.to_string(),
            TelemetryClientHolder {
                client: Arc::clone(&client),
                ref_count: 1,
            },
        );
        TelemetryClientLease::managed(host, client, self)
    }

    async fn release(&self, host: &str) {
        let client_to_close = self.release_ref(host);
        if let Some(client) = client_to_close {
            client.close().await;
        }
    }

    fn release_ref(&self, host: &str) -> Option<Arc<TelemetryClient>> {
        let mut clients = self.clients.lock().expect("poisoned telemetry clients");
        let holder = clients.get_mut(host)?;
        holder.ref_count = holder.ref_count.saturating_sub(1);
        if holder.ref_count == 0 {
            clients.remove(host).map(|holder| holder.client)
        } else {
            None
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
    fn ref_count(&self, host: &str) -> Option<usize> {
        self.clients
            .lock()
            .expect("poisoned telemetry clients")
            .get(host)
            .map(|holder| holder.ref_count)
    }
}

struct TelemetryClientHolder {
    client: Arc<TelemetryClient>,
    ref_count: usize,
}

pub(super) struct TelemetryClientLease {
    host: Option<String>,
    client: Arc<TelemetryClient>,
    manager: Option<&'static TelemetryClientManager>,
    released: AtomicBool,
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
            released: AtomicBool::new(false),
        }
    }

    fn unmanaged(client: Arc<TelemetryClient>) -> Self {
        Self {
            host: None,
            client,
            manager: None,
            released: AtomicBool::new(false),
        }
    }

    pub(super) fn client(&self) -> &Arc<TelemetryClient> {
        &self.client
    }

    pub(super) async fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }

        match (self.manager, self.host.as_deref()) {
            (Some(manager), Some(host)) => manager.release(host).await,
            _ => self.client.close().await,
        }
    }
}

impl Drop for TelemetryClientLease {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }

        let client = Arc::clone(&self.client);
        match (self.manager, self.host.clone()) {
            (Some(manager), Some(host)) => {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        if let Some(client_to_close) = manager.release_ref(&host) {
                            client_to_close.close().await;
                        }
                    });
                } else {
                    let _ = manager.release_ref(&host);
                }
            }
            _ => {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        client.close().await;
                    });
                }
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

        let first = manager.get_or_create("example.com", &config, exporter.clone(), false);
        let second = manager.get_or_create("example.com", &config, exporter, false);

        assert!(Arc::ptr_eq(first.client(), second.client()));
        assert_eq!(manager.tracked_client_count(), 1);
        first.release().await;
        second.release().await;
    }

    #[tokio::test]
    async fn release_removes_last_host_client() {
        let manager = Box::leak(Box::new(TelemetryClientManager::default()));
        let exporter: Arc<dyn TelemetryExporter> = Arc::new(CapturingTelemetryExporter::default());
        let config = TelemetryConfig {
            enabled: Some(true),
            ..TelemetryConfig::default()
        };

        let first = manager.get_or_create("example.com", &config, exporter, false);
        let second = manager.get_or_create(
            "example.com",
            &config,
            Arc::new(CapturingTelemetryExporter::default()) as Arc<dyn TelemetryExporter>,
            false,
        );

        first.release().await;
        assert_eq!(manager.tracked_client_count(), 1);
        second.release().await;
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

        let lease = manager.get_or_create("example.com", &config, exporter, false);
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

        let first = manager.get_or_create("example.com", &config, exporter.clone(), false);
        let second = manager.get_or_create("example.com", &config, exporter, false);

        first.release().await;
        assert_eq!(manager.tracked_client_count(), 1);
        drop(first);
        assert_eq!(manager.tracked_client_count(), 1);

        second.release().await;
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

        let first = manager.get_or_create("example.com", &config, exporter.clone(), false);
        let second = manager.get_or_create("example.com", &config, exporter, false);

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
            manager.get_or_create("example.com", &config, Arc::new(exporter.clone()), false);
        let second =
            manager.get_or_create("example.com", &config, Arc::new(exporter.clone()), false);
        first.client().enqueue(TelemetryFrontendLog::default());

        first.release().await;
        assert_eq!(manager.ref_count("example.com"), Some(1));
        assert!(exporter.requests().is_empty());

        second.release().await;
        assert_eq!(manager.ref_count("example.com"), None);
        assert_eq!(exporter.requests().len(), 1);
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
            manager.get_or_create("example.com", &config, Arc::new(exporter.clone()), false);
        let second =
            manager.get_or_create("example.com", &config, Arc::new(exporter.clone()), false);
        first.client().enqueue(TelemetryFrontendLog::default());

        drop(first);
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.ref_count("example.com") != Some(1) {
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
