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

use super::exporter::{export_batch, TelemetryExportError, TelemetryExporter};
use super::models::TelemetryFrontendLog;
use crate::config::TelemetryConfig;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};

const STATE_ACTIVE: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;

// After a throttled background export, background flushes pause for this
// long, doubling per consecutive throttled export up to the cap. The endpoint
// sends `Retry-After: 1` when it throttles.
#[cfg(not(test))]
const INITIAL_THROTTLE_BACKOFF: Duration = Duration::from_secs(1);
#[cfg(test)]
const INITIAL_THROTTLE_BACKOFF: Duration = Duration::from_millis(200);
const MAX_THROTTLE_BACKOFF: Duration = Duration::from_secs(30);
/// Events kept while throttled (at least one batch); past this the oldest are
/// dropped. Sized for several seconds of backoff at ~1k events/s (~14 MB).
#[cfg(not(test))]
const MAX_PENDING_EVENTS: usize = 10_000;
#[cfg(test)]
const MAX_PENDING_EVENTS: usize = 20;
/// Most events per POST (at least one batch), so a backlog left by throttling
/// goes out in bounded requests (~1.4 MB) rather than one large one.
#[cfg(not(test))]
const MAX_EVENTS_PER_EXPORT: usize = 1_000;
#[cfg(test)]
const MAX_EVENTS_PER_EXPORT: usize = 50;

pub(super) struct TelemetryClient {
    exporter: Arc<dyn TelemetryExporter>,
    batch_size: usize,
    flush_interval: Duration,
    close_flush_timeout: Duration,
    pending: Mutex<Vec<TelemetryFrontendLog>>,
    queue_count: AtomicUsize,
    flush_lock: tokio::sync::Mutex<()>,
    throttle: Mutex<Throttle>,
    state: AtomicU8,
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
    runtime_handle: Handle,
}

#[derive(Default)]
struct Throttle {
    until: Option<Instant>,
    backoff: Duration,
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
            flush_lock: tokio::sync::Mutex::new(()),
            throttle: Mutex::new(Throttle::default()),
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

        if count >= self.batch_size && !self.throttled() {
            let client = Arc::clone(self);
            self.runtime_handle.spawn(async move {
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

    /// Background (batch-size or timer) flush. Skipped while throttled; a
    /// throttled batch goes back to the queue for a later flush.
    async fn flush_once_if_idle(self: Arc<Self>) {
        if self.throttled() {
            return;
        }
        let Ok(_guard) = self.flush_lock.try_lock() else {
            return;
        };
        // Keep going while a backlog left by throttling still fills a batch.
        loop {
            let delivered = self.export_once(true).await;
            if !delivered || self.queue_count.load(Ordering::Acquire) < self.batch_size {
                break;
            }
        }
    }

    /// Session-release and close flush. Sends everything queued without
    /// waiting out a backoff. While the client stays in use a throttled batch
    /// is kept; once it is closing, it is dropped as before.
    async fn flush_once_wait(self: &Arc<Self>) {
        let _guard = self.flush_lock.lock().await;
        let requeue_if_throttled = self.state.load(Ordering::Acquire) == STATE_ACTIVE;
        while self.export_once(requeue_if_throttled).await {}
    }

    /// Export the oldest queued events in one request. Returns whether a
    /// non-empty batch was delivered.
    async fn export_once(&self, requeue_if_throttled: bool) -> bool {
        let batch = self.drain_pending(MAX_EVENTS_PER_EXPORT.max(self.batch_size));
        if batch.is_empty() {
            return false;
        }

        match export_batch(Arc::clone(&self.exporter), &batch).await {
            Ok(()) => {
                *self.throttle.lock().expect("poisoned telemetry throttle") = Throttle::default();
                true
            }
            Err(TelemetryExportError::Throttled(_)) if requeue_if_throttled => {
                self.back_off();
                self.requeue(batch);
                false
            }
            Err(_) => false,
        }
    }

    fn throttled(&self) -> bool {
        let throttle = self.throttle.lock().expect("poisoned telemetry throttle");
        throttle.until.is_some_and(|until| Instant::now() < until)
    }

    fn back_off(&self) {
        let mut throttle = self.throttle.lock().expect("poisoned telemetry throttle");
        throttle.backoff = if throttle.backoff.is_zero() {
            INITIAL_THROTTLE_BACKOFF
        } else {
            (throttle.backoff * 2).min(MAX_THROTTLE_BACKOFF)
        };
        throttle.until = Some(Instant::now() + throttle.backoff);
    }

    /// Put a throttled batch back ahead of events queued since it was drained.
    fn requeue(&self, mut batch: Vec<TelemetryFrontendLog>) {
        let mut pending = self.pending.lock().expect("poisoned telemetry queue");
        batch.append(&mut pending);
        let cap = MAX_PENDING_EVENTS.max(self.batch_size);
        let excess = batch.len().saturating_sub(cap);
        if excess > 0 {
            batch.drain(..excess);
            tracing::debug!(
                dropped = excess,
                "telemetry backlog full while throttled; dropping oldest events"
            );
        }
        self.queue_count.store(batch.len(), Ordering::Release);
        *pending = batch;
    }

    fn drain_pending(&self, limit: usize) -> Vec<TelemetryFrontendLog> {
        let drained = {
            let mut pending = self.pending.lock().expect("poisoned telemetry queue");
            let n = pending.len().min(limit);
            pending.drain(..n).collect::<Vec<_>>()
        };
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

    #[tokio::test]
    async fn throttled_background_export_is_retried_after_backoff() {
        let exporter = ThrottlingExporter::new(1);
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 2,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");

        client.enqueue(sample_log("one"));
        client.enqueue(sample_log("two"));
        wait_until(|| client.throttled()).await;

        // A full batch inside the backoff does not trigger another export.
        client.enqueue(sample_log("three"));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(exporter.ids().len(), 1);

        tokio::time::sleep(INITIAL_THROTTLE_BACKOFF).await;
        client.enqueue(sample_log("four"));
        wait_until(|| exporter.ids().len() == 2).await;

        assert_eq!(exporter.ids()[1], ["one", "two", "three", "four"]);
        assert!(!client.throttled());
        client.close().await;
    }

    #[tokio::test]
    async fn close_flush_ignores_backoff_and_does_not_requeue() {
        let exporter = ThrottlingExporter::new(usize::MAX);
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 1,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");

        client.enqueue(sample_log("one"));
        wait_until(|| client.throttled()).await;
        client.enqueue(sample_log("two"));
        assert!(client.throttled());

        client.close().await;

        assert_eq!(exporter.ids(), [vec!["one"], vec!["one", "two"]]);
        assert!(client.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn session_release_flush_keeps_throttled_batch() {
        let exporter = ThrottlingExporter::new(usize::MAX);
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 1,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");

        client.enqueue(sample_log("one"));
        wait_until(|| client.throttled()).await;

        // The flush run when a host's last session closes ignores the backoff,
        // but the client stays in use, so a throttled batch is kept.
        client.flush_bounded().await;

        assert_eq!(exporter.ids(), [vec!["one"], vec!["one"]]);
        assert_eq!(client.pending.lock().unwrap().len(), 1);
        assert_eq!(
            client.throttle.lock().unwrap().backoff,
            INITIAL_THROTTLE_BACKOFF * 2
        );
        client.close().await;
    }

    #[tokio::test]
    async fn flush_sends_backlog_in_bounded_posts() {
        let exporter = ThrottlingExporter::new(0);
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 2,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");
        let backlog = (0..120)
            .map(|i| sample_log(&format!("e{i}")))
            .collect::<Vec<_>>();
        client.queue_count.store(backlog.len(), Ordering::Release);
        *client.pending.lock().unwrap() = backlog;

        client.flush_bounded().await;

        let sizes = exporter.ids().iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes, [MAX_EVENTS_PER_EXPORT, MAX_EVENTS_PER_EXPORT, 20]);
        assert_eq!(exporter.ids()[0][0], "e0");
        client.close().await;
    }

    #[tokio::test]
    async fn background_flush_drains_backlog_in_bounded_posts() {
        let exporter = ThrottlingExporter::new(0);
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 2,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(exporter.clone()))
            .expect("telemetry runtime should start");
        let backlog = (0..119)
            .map(|i| sample_log(&format!("e{i}")))
            .collect::<Vec<_>>();
        client.queue_count.store(backlog.len(), Ordering::Release);
        *client.pending.lock().unwrap() = backlog;

        client.enqueue(sample_log("e119"));
        wait_until(|| exporter.ids().len() == 3).await;

        let sizes = exporter.ids().iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes, [MAX_EVENTS_PER_EXPORT, MAX_EVENTS_PER_EXPORT, 20]);
        assert!(client.pending.lock().unwrap().is_empty());
        client.close().await;
    }

    #[tokio::test]
    async fn requeue_keeps_oldest_first_and_caps_backlog() {
        let config = TelemetryConfig {
            enabled: Some(true),
            batch_size: 2,
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(ThrottlingExporter::new(0)))
            .expect("telemetry runtime should start");
        *client.pending.lock().unwrap() = (0..5).map(|i| sample_log(&format!("new{i}"))).collect();

        client.requeue((0..18).map(|i| sample_log(&format!("old{i}"))).collect());

        let pending = client.pending.lock().unwrap();
        assert_eq!(pending.len(), MAX_PENDING_EVENTS);
        assert_eq!(pending[0].frontend_log_event_id, "old3");
        assert_eq!(pending[19].frontend_log_event_id, "new4");
        assert_eq!(client.queue_count.load(Ordering::Acquire), pending.len());
    }

    #[tokio::test]
    async fn backoff_doubles_up_to_cap() {
        let config = TelemetryConfig {
            enabled: Some(true),
            flush_interval: Duration::from_secs(60),
            ..TelemetryConfig::default()
        };
        let client = TelemetryClient::new(&config, Arc::new(ThrottlingExporter::new(0)))
            .expect("telemetry runtime should start");

        let mut seen = Vec::new();
        for _ in 0..10 {
            client.back_off();
            seen.push(client.throttle.lock().unwrap().backoff);
        }

        assert_eq!(seen[0], INITIAL_THROTTLE_BACKOFF);
        assert_eq!(seen[1], INITIAL_THROTTLE_BACKOFF * 2);
        assert_eq!(*seen.last().unwrap(), MAX_THROTTLE_BACKOFF);
    }

    /// Throttles the first `throttled` exports, then accepts; records the
    /// event ids of every export attempt.
    #[derive(Clone)]
    struct ThrottlingExporter {
        throttled: usize,
        attempts: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl ThrottlingExporter {
        fn new(throttled: usize) -> Self {
            Self {
                throttled,
                attempts: Arc::default(),
            }
        }

        fn ids(&self) -> Vec<Vec<String>> {
            self.attempts.lock().unwrap().clone()
        }
    }

    impl TelemetryExporter for ThrottlingExporter {
        fn export<'a>(
            &'a self,
            request: TelemetryRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = super::super::exporter::TelemetryExportResult>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let ids = request
                    .proto_logs
                    .iter()
                    .map(|log| {
                        serde_json::from_str::<serde_json::Value>(log).unwrap()
                            ["frontend_log_event_id"]
                            .as_str()
                            .unwrap()
                            .to_string()
                    })
                    .collect();
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push(ids);
                if attempts.len() <= self.throttled {
                    Err(TelemetryExportError::Throttled("HTTP 429".to_string()))
                } else {
                    Ok(())
                }
            })
        }
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition should become true");
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
