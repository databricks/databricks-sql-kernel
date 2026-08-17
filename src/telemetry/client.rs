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
            self.in_flight
                .lock()
                .expect("poisoned telemetry in-flight queue")
                .push(handle);
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
