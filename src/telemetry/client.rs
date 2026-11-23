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
use tokio::runtime::{Handle, Runtime};

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
    flush_lock: Arc<tokio::sync::Mutex<()>>,
    state: AtomicU8,
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
    runtime_handle: Handle,
}

impl TelemetryClient {
    pub(super) fn new(
        config: &TelemetryConfig,
        exporter: Arc<dyn TelemetryExporter>,
    ) -> Option<Arc<Self>> {
        assert!(
            !config.flush_interval.is_zero(),
            "TelemetryClient requires a non-zero flush interval; TelemetryRuntime should filter invalid configs"
        );
        let runtime_handle = match telemetry_runtime_handle() {
            Ok(handle) => handle,
            Err(error) => {
                tracing::debug!(%error, "failed to create telemetry runtime; disabling telemetry");
                return None;
            }
        };
        let client = Arc::new(Self {
            exporter,
            batch_size: config.batch_size,
            flush_interval: config.flush_interval,
            close_flush_timeout: config.close_flush_timeout,
            pending: Mutex::new(Vec::new()),
            queue_count: AtomicUsize::new(0),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
            state: AtomicU8::new(STATE_ACTIVE),
            timer: Mutex::new(None),
            runtime_handle,
        });
        client.start_timer();
        Some(client)
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
            let Ok(guard) = Arc::clone(&self.flush_lock).try_lock_owned() else {
                return;
            };
            let client = Arc::clone(self);
            self.runtime_handle.spawn(async move {
                let _guard = guard;
                client.export_pending().await;
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

    pub(super) fn schedule_flush_bounded(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let client = Arc::clone(self);
        self.runtime_handle.spawn(async move {
            client.flush_bounded().await;
        })
    }

    pub(super) fn schedule_close(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let client = Arc::clone(self);
        self.runtime_handle.spawn(async move {
            client.close().await;
        })
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
        let handle = self.runtime_handle.spawn(async move {
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

// A real HTTP telemetry export needs more than a tiny stack: a host running
// with e.g. RUST_MIN_STACK=16384 would otherwise overflow the worker stack and
// abort the process. The worker stack floor is shared with the C-ABI runtime
// via `crate::util::runtime` (single source of truth); see that module for the
// floor-not-cap semantics and the #326 / #328 history.
use crate::util::runtime::{ambient_rust_min_stack, runtime_thread_stack_size};

fn telemetry_runtime_handle() -> std::io::Result<Handle> {
    // A host client can outlive the session that created it. Keep its timer,
    // batch tasks, and Drop cleanup independent of any session-owned runtime.
    //
    // Only a successfully-built runtime is cached. A build failure is NOT
    // memoized, so a transient failure (e.g. the OS momentarily can't spawn
    // the worker thread under resource pressure) is retried on the next
    // session rather than poisoning telemetry for the rest of the process.
    static RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);
    let mut runtime = RUNTIME.lock().expect("poisoned telemetry runtime");
    if let Some(runtime) = runtime.as_ref() {
        return Ok(runtime.handle().clone());
    }
    let built = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("dbx-sql-telemetry")
        .thread_stack_size(runtime_thread_stack_size(ambient_rust_min_stack()))
        .enable_all()
        .build()?;
    let handle = built.handle().clone();
    *runtime = Some(built);
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::test_support::CapturingTelemetryExporter;
    use crate::telemetry::TelemetryRequest;
    use std::time::Duration;

    // The pure floor-not-cap resolution (#326 / #328) is exercised once over
    // the shared helper in `crate::util::runtime`.

    #[tokio::test]
    async fn batch_size_triggers_background_flush() {
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 2,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");

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
    async fn enqueue_from_non_runtime_thread_uses_client_runtime() {
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 1,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");

        let worker_client = Arc::clone(&client);
        std::thread::spawn(move || {
            worker_client.enqueue(sample_log("one"));
        })
        .join()
        .expect("enqueue from plain thread should not panic");

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

        assert_eq!(exporter.requests()[0].proto_logs.len(), 1);
        client.close().await;
    }

    #[tokio::test]
    async fn interval_triggers_flush_before_batch_size() {
        let exporter = CapturingTelemetryExporter::default();
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 100,
            flush_interval: Duration::from_millis(20),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");

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
            enabled: Some(true),
            batch_size: 100,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");

        client.enqueue(sample_log("one"));
        client.close().await;
        client.enqueue(sample_log("late"));

        let requests = exporter.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].proto_logs.len(), 1);
    }

    #[test]
    fn burst_reserves_one_flush_before_the_runtime_runs() {
        let exporter = CapturingTelemetryExporter::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = Arc::new(TelemetryClient {
            exporter: Arc::new(exporter.clone()),
            batch_size: 100,
            flush_interval: Duration::from_secs(60),
            close_flush_timeout: Duration::from_secs(5),
            pending: Mutex::new(Vec::new()),
            queue_count: AtomicUsize::new(0),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
            state: AtomicU8::new(STATE_ACTIVE),
            timer: Mutex::new(None),
            runtime_handle: runtime.handle().clone(),
        });

        for index in 0..2_000 {
            client.enqueue(sample_log(&index.to_string()));
        }
        assert_eq!(runtime.metrics().num_alive_tasks(), 1);

        runtime.block_on(client.close());
        let mut ids: Vec<usize> = exporter
            .requests()
            .iter()
            .flat_map(|request| &request.proto_logs)
            .map(|log| event_id(log).parse().unwrap())
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..2_000).collect::<Vec<_>>());
    }

    #[derive(Default)]
    struct PausedExporter {
        captured: CapturingTelemetryExporter,
        started: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }

    impl TelemetryExporter for PausedExporter {
        fn export<'a>(
            &'a self,
            request: TelemetryRequest,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = super::super::TelemetryExportResult> + Send + 'a>,
        > {
            Box::pin(async move {
                if request
                    .proto_logs
                    .iter()
                    .any(|log| event_id(log) == "first")
                {
                    self.started.notify_one();
                    self.resume.notified().await;
                }
                self.captured.export(request).await
            })
        }
    }

    #[tokio::test]
    async fn timer_and_close_flush_events_enqueued_during_export() {
        for close in [false, true] {
            let exporter = Arc::new(PausedExporter::default());
            let config = TelemetryConfig {
                enabled: Some(true),
                batch_size: 1,
                flush_interval: if close {
                    Duration::from_secs(60)
                } else {
                    Duration::from_millis(20)
                },
                close_flush_timeout: Duration::from_secs(5),
                ..TelemetryConfig::default()
            };
            let client = TelemetryClient::new(&config, exporter.clone()).unwrap();
            client.enqueue(sample_log("first"));
            tokio::time::timeout(Duration::from_secs(5), exporter.started.notified())
                .await
                .expect("first export should start");
            client.enqueue(sample_log("second"));

            let closing = if close {
                let client = Arc::clone(&client);
                Some(tokio::spawn(async move { client.close().await }))
            } else {
                None
            };
            if close {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while client.state.load(Ordering::Acquire) == STATE_ACTIVE {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("close should wait for the running export");
            }
            exporter.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(5), async {
                while exporter.captured.requests().len() < 2 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("pending event should be flushed");
            if let Some(closing) = closing {
                closing.await.unwrap();
            } else {
                client.close().await;
            }

            let ids: Vec<String> = exporter
                .captured
                .requests()
                .into_iter()
                .flat_map(|request| request.proto_logs)
                .map(|log| event_id(&log))
                .collect();
            assert_eq!(ids, ["first", "second"]);
        }
    }

    fn event_id(log: &str) -> String {
        serde_json::from_str::<TelemetryFrontendLog>(log)
            .unwrap()
            .frontend_log_event_id
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
