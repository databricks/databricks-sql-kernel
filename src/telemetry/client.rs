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

use super::exporter::{export_batch, TelemetryExporter};
use super::models::TelemetryFrontendLog;
use crate::config::TelemetryConfig;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const STATE_ACTIVE: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;

pub(super) struct TelemetryClient {
    exporter: Arc<dyn TelemetryExporter>,
    batch_size: usize,
    flush_interval: std::time::Duration,
    close_flush_timeout: std::time::Duration,
    pending: Mutex<Vec<TelemetryFrontendLog>>,
    queue_count: AtomicUsize,
    flush_lock: tokio::sync::Mutex<()>,
    state: AtomicU8,
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl TelemetryClient {
    pub(super) fn new(config: &TelemetryConfig, exporter: Arc<dyn TelemetryExporter>) -> Arc<Self> {
        assert!(
            !config.flush_interval.is_zero(),
            "TelemetryClient requires a non-zero flush interval; TelemetryRuntime should filter invalid configs"
        );
        let client = Arc::new(Self {
            exporter,
            batch_size: config.batch_size,
            flush_interval: config.flush_interval,
            close_flush_timeout: config.close_flush_timeout,
            pending: Mutex::new(Vec::new()),
            queue_count: AtomicUsize::new(0),
            flush_lock: tokio::sync::Mutex::new(()),
            state: AtomicU8::new(STATE_ACTIVE),
            timer: Mutex::new(None),
        });
        client.start_timer();
        client
    }

    pub(super) fn enqueue(self: &Arc<Self>, log: TelemetryFrontendLog) {
        if self.state.load(Ordering::Acquire) != STATE_ACTIVE {
            return;
        }

        let count = {
            let mut pending = self.pending.lock().expect("poisoned telemetry queue");
            pending.push(log);
            self.queue_count.fetch_add(1, Ordering::AcqRel) + 1
        };

        if count >= self.batch_size {
            let client = Arc::clone(self);
            tokio::spawn(async move {
                client.flush_once_if_idle().await;
            });
        }
    }

    pub(super) async fn flush_bounded(self: &Arc<Self>) {
        if self.state.load(Ordering::Acquire) == STATE_CLOSED {
            return;
        }

        let timeout = self.close_flush_timeout;
        let _ = tokio::time::timeout(timeout, self.flush_once_wait()).await;
    }

    pub(super) async fn close(self: &Arc<Self>) {
        if self
            .state
            .compare_exchange(
                STATE_ACTIVE,
                STATE_CLOSING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }

        if let Some(timer) = self.timer.lock().expect("poisoned telemetry timer").take() {
            timer.abort();
        }
        self.flush_bounded().await;
        self.state.store(STATE_CLOSED, Ordering::Release);
    }

    async fn flush_once_if_idle(self: Arc<Self>) {
        let Ok(_guard) = self.flush_lock.try_lock() else {
            return;
        };
        self.export_pending().await;
    }

    async fn flush_once_wait(self: &Arc<Self>) {
        let _guard = self.flush_lock.lock().await;
        self.export_pending().await;
    }

    async fn export_pending(&self) {
        let batch = self.drain_pending();
        if batch.is_empty() {
            return;
        }

        export_batch(Arc::clone(&self.exporter), batch).await;
    }

    fn drain_pending(&self) -> Vec<TelemetryFrontendLog> {
        let drained = self
            .pending
            .lock()
            .expect("poisoned telemetry queue")
            .drain(..)
            .collect::<Vec<_>>();
        self.queue_count.fetch_sub(drained.len(), Ordering::AcqRel);
        drained
    }

    fn start_timer(self: &Arc<Self>) {
        let client = Arc::downgrade(self);
        let interval = self.flush_interval;
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(client) = client.upgrade() else {
                    break;
                };
                if client.state.load(Ordering::Acquire) != STATE_ACTIVE {
                    break;
                }
                client.flush_once_if_idle().await;
            }
        });
        *self.timer.lock().expect("poisoned telemetry timer") = Some(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::test_support::CapturingTelemetryExporter;
    use crate::telemetry::TelemetryRequest;
    use std::time::Duration;

    #[tokio::test]
    async fn batch_size_triggers_background_flush() {
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: true,
            batch_size: 2,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()));

        client.enqueue(sample_log("one"));
        assert!(exporter.requests().is_empty());
        client.enqueue(sample_log("two"));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !exporter.requests().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("batch flush should complete");

        assert_eq!(exporter.requests()[0].proto_logs.len(), 2);
        client.close().await;
    }

    #[tokio::test]
    async fn interval_triggers_flush_before_batch_size() {
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: true,
            batch_size: 100,
            flush_interval: Duration::from_millis(20),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()));

        client.enqueue(sample_log("one"));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !exporter.requests().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("interval flush should complete");

        assert_eq!(exporter.requests()[0].proto_logs.len(), 1);
        client.close().await;
    }

    #[tokio::test]
    async fn close_flushes_pending_events_and_drops_late_enqueue() {
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: true,
            batch_size: 100,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()));

        client.enqueue(sample_log("one"));
        client.close().await;
        client.enqueue(sample_log("late"));

        let requests = exporter.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].proto_logs.len(), 1);
    }

    fn sample_log(id: &str) -> TelemetryFrontendLog {
        TelemetryFrontendLog {
            frontend_log_event_id: id.to_string(),
            ..TelemetryFrontendLog::default()
        }
    }

    #[allow(dead_code)]
    fn assert_request_send_sync()
    where
        TelemetryRequest: Send + Sync,
    {
    }
}
