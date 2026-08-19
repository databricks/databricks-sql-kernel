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

use super::models::{ChunkDetails, ExecutionResultFormat, OperationType, StatementType};
use super::runtime::ConnectionTelemetry;
use crate::kernel_error::Error as KernelError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct StatementTelemetry {
    observer: Arc<dyn StatementOperationObserver>,
}

impl StatementTelemetry {
    pub(crate) fn noop() -> Self {
        Self {
            observer: Arc::new(NoOpObserver),
        }
    }

    pub(super) fn new(
        connection: Arc<dyn ConnectionTelemetry>,
        statement_type: StatementType,
        operation_type: OperationType,
    ) -> Self {
        Self {
            observer: Arc::new(TelemetryObserver::new(
                connection,
                statement_type,
                operation_type,
            )),
        }
    }

    pub(crate) fn on_statement_submitted(&self, statement_id: &str) {
        self.observer.on_statement_submitted(statement_id);
    }

    pub(crate) fn on_result_metadata(
        &self,
        result_format: ExecutionResultFormat,
        is_compressed: bool,
    ) {
        self.observer
            .on_result_metadata(result_format, is_compressed);
    }

    pub(crate) fn record_poll(&self, elapsed: Duration) {
        self.observer.record_poll(elapsed);
    }

    pub(crate) fn record_result_batch(&self, rows: usize) {
        self.observer.record_result_batch(rows);
    }

    pub(crate) fn record_result_consumed(&self) {
        self.observer.record_result_consumed();
    }

    pub(crate) fn record_chunk_metrics(&self, metrics: ChunkMetrics) {
        self.observer.record_chunk_metrics(metrics);
    }

    pub(crate) fn finalize(&self, error: Option<&KernelError>) {
        self.observer.finalize(error);
    }
}

pub(super) trait StatementOperationObserver: Send + Sync {
    fn on_statement_submitted(&self, statement_id: &str);
    fn on_result_metadata(&self, result_format: ExecutionResultFormat, is_compressed: bool);
    fn record_poll(&self, elapsed: Duration);
    fn record_result_batch(&self, rows: usize);
    fn record_result_consumed(&self);
    fn record_chunk_metrics(&self, metrics: ChunkMetrics);
    fn finalize(&self, error: Option<&KernelError>);
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ChunkMetrics {
    pub total_chunks_present: Option<i32>,
    pub total_chunks_iterated: Option<i32>,
    pub initial_chunk_latency: Option<Duration>,
    pub slowest_chunk_latency: Option<Duration>,
    pub sum_chunks_download_time: Option<Duration>,
}

impl ChunkMetrics {
    pub(crate) fn into_details(self) -> ChunkDetails {
        ChunkDetails {
            initial_chunk_latency_millis: self.initial_chunk_latency.map(duration_millis),
            slowest_chunk_latency_millis: self.slowest_chunk_latency.map(duration_millis),
            total_chunks_present: self.total_chunks_present,
            total_chunks_iterated: self.total_chunks_iterated,
            sum_chunks_download_time_millis: self.sum_chunks_download_time.map(duration_millis),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StatementTelemetryContext {
    pub statement_id: Option<String>,
    pub statement_type: StatementType,
    pub operation_type: OperationType,
    pub result_format: Option<ExecutionResultFormat>,
    pub is_compressed: Option<bool>,
    pub retry_count: Option<i64>,
    pub is_internal_call: bool,
    pub poll_count: i32,
    pub poll_latency: Duration,
    pub first_batch_latency: Option<Duration>,
    pub consumption_latency: Option<Duration>,
    pub result_batch_count: i32,
    pub result_row_count: i64,
    pub chunk_metrics: Option<ChunkMetrics>,
}

struct TelemetryObserver {
    connection: Arc<dyn ConnectionTelemetry>,
    state: Mutex<StatementTelemetryState>,
    finalized: AtomicBool,
}

struct StatementTelemetryState {
    started_at: Instant,
    context: StatementTelemetryContext,
}

impl TelemetryObserver {
    fn new(
        connection: Arc<dyn ConnectionTelemetry>,
        statement_type: StatementType,
        operation_type: OperationType,
    ) -> Self {
        Self {
            connection,
            state: Mutex::new(StatementTelemetryState {
                started_at: Instant::now(),
                context: StatementTelemetryContext {
                    statement_id: None,
                    statement_type,
                    operation_type,
                    result_format: None,
                    is_compressed: None,
                    retry_count: None,
                    is_internal_call: false,
                    poll_count: 0,
                    poll_latency: Duration::ZERO,
                    first_batch_latency: None,
                    consumption_latency: None,
                    result_batch_count: 0,
                    result_row_count: 0,
                    chunk_metrics: None,
                },
            }),
            finalized: AtomicBool::new(false),
        }
    }
}

impl StatementOperationObserver for TelemetryObserver {
    fn on_statement_submitted(&self, statement_id: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.context.statement_id = Some(statement_id.to_string());
        }
    }

    fn on_result_metadata(&self, result_format: ExecutionResultFormat, is_compressed: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.context.result_format = Some(result_format);
            state.context.is_compressed = Some(is_compressed);
        }
    }

    fn record_poll(&self, elapsed: Duration) {
        if let Ok(mut state) = self.state.lock() {
            state.context.poll_count = state.context.poll_count.saturating_add(1);
            state.context.poll_latency += elapsed;
        }
    }

    fn record_result_batch(&self, rows: usize) {
        if let Ok(mut state) = self.state.lock() {
            if state.context.first_batch_latency.is_none() {
                state.context.first_batch_latency = Some(state.started_at.elapsed());
            }
            state.context.result_batch_count = state.context.result_batch_count.saturating_add(1);
            state.context.result_row_count = state
                .context
                .result_row_count
                .saturating_add(rows.try_into().unwrap_or(i64::MAX));
        }
    }

    fn record_result_consumed(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.context.consumption_latency = Some(state.started_at.elapsed());
        }
    }

    fn record_chunk_metrics(&self, metrics: ChunkMetrics) {
        if let Ok(mut state) = self.state.lock() {
            state.context.chunk_metrics = Some(metrics);
        }
    }

    fn finalize(&self, error: Option<&KernelError>) {
        if self.finalized.swap(true, Ordering::AcqRel) {
            return;
        }

        let Some((context, elapsed)) = self
            .state
            .lock()
            .ok()
            .map(|state| (state.context.clone(), state.started_at.elapsed()))
        else {
            return;
        };

        self.connection
            .emit_statement_operation(context, elapsed, error);
    }
}

#[derive(Debug)]
struct NoOpObserver;

impl StatementOperationObserver for NoOpObserver {
    fn on_statement_submitted(&self, _statement_id: &str) {}

    fn on_result_metadata(&self, _result_format: ExecutionResultFormat, _is_compressed: bool) {}

    fn record_poll(&self, _elapsed: Duration) {}

    fn record_result_batch(&self, _rows: usize) {}

    fn record_result_consumed(&self) {}

    fn record_chunk_metrics(&self, _metrics: ChunkMetrics) {}

    fn finalize(&self, _error: Option<&KernelError>) {}
}

fn duration_millis(duration: Duration) -> i64 {
    duration.as_millis().try_into().unwrap_or(i64::MAX)
}
