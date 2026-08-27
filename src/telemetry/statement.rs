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

    pub(crate) fn record_result_batch(&self) {
        self.observer.record_result_batch();
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
    fn record_result_batch(&self);
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

impl From<crate::reader::ResultReaderMetrics> for ChunkMetrics {
    fn from(metrics: crate::reader::ResultReaderMetrics) -> Self {
        Self {
            total_chunks_present: metrics.total_chunks_present,
            total_chunks_iterated: metrics.total_chunks_iterated,
            initial_chunk_latency: metrics.initial_chunk_latency,
            slowest_chunk_latency: metrics.slowest_chunk_latency,
            sum_chunks_download_time: metrics.sum_chunks_download_time,
        }
    }
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

    fn record_result_batch(&self) {
        if let Ok(mut state) = self.state.lock() {
            if state.context.first_batch_latency.is_none() {
                state.context.first_batch_latency = Some(state.started_at.elapsed());
            }
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

    fn record_result_batch(&self) {}

    fn record_result_consumed(&self) {}

    fn record_chunk_metrics(&self, _metrics: ChunkMetrics) {}

    fn finalize(&self, _error: Option<&KernelError>) {}
}

fn duration_millis(duration: Duration) -> i64 {
    duration.as_millis().try_into().unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel_error::ErrorCode;
    use crate::reader::test_utils::ErrorReader;
    use crate::reader::{ResultReader, ResultReaderMetrics};
    use crate::result_stream::ResultStream;
    use crate::telemetry::models::OperationType;
    use crate::telemetry::runtime::{ConnectionTelemetry, TelemetrySessionContext};
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use async_trait::async_trait;
    use std::future::Future;
    use std::pin::Pin;

    #[derive(Default)]
    struct CapturingConnectionTelemetry {
        statement_events: Mutex<Vec<CapturedStatementEvent>>,
    }

    struct CapturedStatementEvent {
        context: StatementTelemetryContext,
        error_code: Option<ErrorCode>,
        error_message: Option<String>,
    }

    impl ConnectionTelemetry for CapturingConnectionTelemetry {
        fn session(&self) -> Option<Arc<TelemetrySessionContext>> {
            None
        }

        fn emit_operation(
            &self,
            _operation_type: OperationType,
            _elapsed: Duration,
            _error: Option<&KernelError>,
        ) {
        }

        fn emit_statement_operation(
            &self,
            context: StatementTelemetryContext,
            _elapsed: Duration,
            error: Option<&KernelError>,
        ) {
            self.statement_events
                .lock()
                .expect("events mutex")
                .push(CapturedStatementEvent {
                    context,
                    error_code: error.map(|e| e.code),
                    error_message: error.map(|e| e.message.clone()),
                });
        }

        fn finish<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async {})
        }
    }

    struct MetricsReader {
        metrics: ResultReaderMetrics,
    }

    #[async_trait]
    impl ResultReader for MetricsReader {
        async fn schema(&self) -> crate::Result<crate::result_stream::SchemaRef> {
            Ok(Arc::new(Schema::empty()))
        }

        async fn next_batch(&mut self) -> crate::Result<Option<RecordBatch>> {
            Ok(None)
        }

        fn metrics(&self) -> Option<ResultReaderMetrics> {
            Some(self.metrics)
        }
    }

    #[tokio::test]
    async fn peek_first_batch_error_finalizes_statement_with_error_before_drop() {
        let connection = Arc::new(CapturingConnectionTelemetry::default());
        let telemetry = StatementTelemetry::new(
            connection.clone(),
            StatementType::Sql,
            OperationType::ExecuteStatement,
        );
        let reader = ErrorReader {
            schema: Arc::new(Schema::empty()),
        };
        let mut stream = ResultStream::from_reader(Box::new(reader))
            .await
            .expect("schema succeeds");
        stream.attach_telemetry(telemetry);

        let error = match stream.peek_first_batch().await {
            Ok(_) => panic!("peek should surface reader error"),
            Err(error) => error,
        };
        assert_eq!(error.code, ErrorCode::Internal);

        drop(stream);

        let events = connection.statement_events.lock().expect("events mutex");
        assert_eq!(
            events.len(),
            1,
            "peek error should finalize once; Drop must not overwrite it with success"
        );
        let event = &events[0];
        assert_eq!(event.context.statement_type, StatementType::Sql);
        assert_eq!(event.error_code, Some(ErrorCode::Internal));
        assert_eq!(event.error_message.as_deref(), Some("simulated read error"));
        assert!(
            event.context.consumption_latency.is_some(),
            "failed consumption should still record result-consumption latency"
        );
    }

    #[tokio::test]
    async fn dropping_result_stream_finalizes_with_reader_metrics() {
        let connection = Arc::new(CapturingConnectionTelemetry::default());
        let telemetry = StatementTelemetry::new(
            connection.clone(),
            StatementType::Sql,
            OperationType::ExecuteStatement,
        );
        let metrics = ResultReaderMetrics {
            total_chunks_present: Some(5),
            total_chunks_iterated: Some(2),
            initial_chunk_latency: Some(Duration::from_millis(11)),
            slowest_chunk_latency: Some(Duration::from_millis(29)),
            sum_chunks_download_time: Some(Duration::from_millis(40)),
        };
        let mut stream = ResultStream::from_reader(Box::new(MetricsReader { metrics }))
            .await
            .expect("schema succeeds");
        stream.attach_telemetry(telemetry);

        drop(stream);

        let events = connection.statement_events.lock().expect("events mutex");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        let chunk_metrics = event
            .context
            .chunk_metrics
            .expect("drop should include reader metrics");
        assert_eq!(chunk_metrics.total_chunks_present, Some(5));
        assert_eq!(chunk_metrics.total_chunks_iterated, Some(2));
        assert_eq!(
            chunk_metrics.initial_chunk_latency,
            Some(Duration::from_millis(11))
        );
        assert_eq!(
            chunk_metrics.slowest_chunk_latency,
            Some(Duration::from_millis(29))
        );
        assert_eq!(
            chunk_metrics.sum_chunks_download_time,
            Some(Duration::from_millis(40))
        );
        assert!(
            event.context.consumption_latency.is_none(),
            "drop without consumption must NOT stamp consumption latency \
             (would report caller think-time, not time to consume the result set)"
        );
    }
}
