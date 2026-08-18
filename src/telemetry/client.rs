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
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(super) struct TelemetryClient {
    exporter: Arc<dyn TelemetryExporter>,
    batch_size: usize,
    close_flush_timeout: Duration,
    pending: Mutex<Vec<TelemetryFrontendLog>>,
    in_flight: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl TelemetryClient {
    pub(super) fn new(
        exporter: Arc<dyn TelemetryExporter>,
        batch_size: usize,
        close_flush_timeout: Duration,
    ) -> Self {
        Self {
            exporter,
            batch_size,
            close_flush_timeout,
            pending: Mutex::new(Vec::new()),
            in_flight: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn enqueue(&self, log: TelemetryFrontendLog) {
        let batch = {
            let mut pending = self.pending.lock().expect("poisoned telemetry queue");
            pending.push(log);
            if pending.len() >= self.batch_size {
                Some(pending.drain(..).collect::<Vec<_>>())
            } else {
                None
            }
        };

        if let Some(batch) = batch {
            let exporter = Arc::clone(&self.exporter);
            let handle = tokio::spawn(async move {
                export_batch(exporter, batch).await;
            });
            let mut in_flight = self
                .in_flight
                .lock()
                .expect("poisoned telemetry in-flight queue");
            // Prune completed handles so a long-lived session with a small
            // `batch_size` doesn't accumulate finished handles until close.
            in_flight.retain(|handle| !handle.is_finished());
            in_flight.push(handle);
        }
    }

    pub(super) async fn flush_bounded(&self) {
        let batch = self.drain_pending();
        let in_flight = self.drain_in_flight();
        if batch.is_empty() && in_flight.is_empty() {
            return;
        }

        let exporter = Arc::clone(&self.exporter);
        let timeout = self.close_flush_timeout;
        let _ = tokio::time::timeout(timeout, async move {
            for handle in in_flight {
                let _ = handle.await;
            }
            if !batch.is_empty() {
                export_batch(exporter, batch).await;
            }
        })
        .await;
    }

    fn drain_pending(&self) -> Vec<TelemetryFrontendLog> {
        self.pending
            .lock()
            .expect("poisoned telemetry queue")
            .drain(..)
            .collect()
    }

    fn drain_in_flight(&self) -> Vec<tokio::task::JoinHandle<()>> {
        self.in_flight
            .lock()
            .expect("poisoned telemetry in-flight queue")
            .drain(..)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::test_support::CapturingTelemetryExporter;

    fn client(exporter: Arc<CapturingTelemetryExporter>, batch_size: usize) -> TelemetryClient {
        TelemetryClient::new(
            exporter as Arc<dyn TelemetryExporter>,
            batch_size,
            Duration::from_secs(5),
        )
    }

    #[tokio::test]
    async fn enqueue_spawns_export_when_batch_size_reached() {
        let exporter = Arc::new(CapturingTelemetryExporter::default());
        let client = client(Arc::clone(&exporter), 1);

        // With batch_size == 1, the single enqueue reaches the threshold, drains
        // `pending` into a batch, and spawns `export_batch` on a stored in-flight
        // handle — all mid-session, before any `flush`/close.
        client.enqueue(TelemetryFrontendLog::default());

        assert!(
            client
                .pending
                .lock()
                .expect("poisoned telemetry queue")
                .is_empty(),
            "pending should be drained once the batch is spawned"
        );

        let handles = client.drain_in_flight();
        assert_eq!(
            handles.len(),
            1,
            "enqueue should spawn exactly one export task"
        );
        for handle in handles {
            handle.await.expect("spawned export task panicked");
        }

        let requests = exporter.requests();
        assert_eq!(
            requests.len(),
            1,
            "exporter should receive the batch mid-session (before close)"
        );
        assert_eq!(requests[0].proto_logs.len(), 1);
    }

    #[tokio::test]
    async fn flush_bounded_joins_in_flight_export() {
        let exporter = Arc::new(CapturingTelemetryExporter::default());
        let client = client(Arc::clone(&exporter), 1);

        // Spawns a batch export via the in-flight path...
        client.enqueue(TelemetryFrontendLog::default());
        // ...which flush_bounded must await through its stored handle.
        client.flush_bounded().await;

        let requests = exporter.requests();
        assert_eq!(
            requests.len(),
            1,
            "flush_bounded should join the in-flight export so its batch is exported"
        );
        assert_eq!(requests[0].proto_logs.len(), 1);
    }

    #[tokio::test]
    async fn flush_bounded_exports_remaining_pending_below_batch_size() {
        let exporter = Arc::new(CapturingTelemetryExporter::default());
        let client = client(Arc::clone(&exporter), 200);

        // Below the batch threshold: nothing is spawned, the event sits in
        // `pending` until flush drains and exports it directly.
        client.enqueue(TelemetryFrontendLog::default());
        assert!(client.drain_in_flight().is_empty());

        client.flush_bounded().await;

        let requests = exporter.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].proto_logs.len(), 1);
    }
}
