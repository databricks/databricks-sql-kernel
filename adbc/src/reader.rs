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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::error::{Result, Status};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use databricks_sql_kernel as kernel;
use databricks_sql_kernel::ExecutedStatementHandle as _;

use crate::cancellation::OperationGuard;
use crate::error::adbc_error;
use crate::runtime::RuntimeExt as _;
use crate::statement::{execute_bound_row, ParameterStyle};

pub(crate) struct BoundRows {
    source: BoundSource,
    current: Option<RecordBatch>,
    row: usize,
}

pub(crate) struct SlicingReader {
    inner: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
    max_rows: usize,
    current: Option<RecordBatch>,
    offset: usize,
}

impl SlicingReader {
    pub(crate) fn new(inner: Box<dyn RecordBatchReader + Send>, max_rows: usize) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            max_rows,
            current: None,
            offset: 0,
        }
    }
}

impl Iterator for SlicingReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = &self.current {
            if self.offset < batch.num_rows() {
                let length = self.max_rows.min(batch.num_rows() - self.offset);
                let slice = batch.slice(self.offset, length);
                self.offset += length;
                return Some(Ok(slice));
            }
        }
        self.current = match self.inner.next()? {
            Ok(batch) => Some(batch),
            Err(error) => return Some(Err(error)),
        };
        self.offset = 0;
        let batch = self.current.as_ref().expect("batch was just installed");
        if batch.num_rows() == 0 {
            let empty = batch.clone();
            self.current = None;
            return Some(Ok(empty));
        }
        self.next()
    }
}

impl RecordBatchReader for SlicingReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

enum BoundSource {
    Batch(Option<RecordBatch>),
    Stream(Box<dyn RecordBatchReader + Send>),
}

impl BoundRows {
    pub(crate) fn from_batch(batch: RecordBatch) -> Self {
        Self {
            source: BoundSource::Batch(Some(batch)),
            current: None,
            row: 0,
        }
    }

    pub(crate) fn from_stream(reader: Box<dyn RecordBatchReader + Send>) -> Self {
        Self {
            source: BoundSource::Stream(reader),
            current: None,
            row: 0,
        }
    }

    pub(crate) fn next_row(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(batch) = &self.current {
                if self.row < batch.num_rows() {
                    let row = batch.slice(self.row, 1);
                    self.row += 1;
                    return Ok(Some(row));
                }
            }
            self.current = match &mut self.source {
                BoundSource::Batch(batch) => batch.take(),
                BoundSource::Stream(reader) => reader.next().transpose().map_err(|source| {
                    adbc_error(
                        format!("failed to read bound Arrow stream: {source}"),
                        Status::InvalidData,
                    )
                })?,
            };
            self.row = 0;
            if self.current.is_none() {
                return Ok(None);
            }
        }
    }
}

pub(crate) struct KernelReader {
    connection_operation: Option<OperationGuard>,
    executed: kernel::ExecutedStatement,
    // The executed handle must drop first so it can schedule cleanup while
    // the final runtime owner is still alive.
    runtime: Arc<tokio::runtime::Runtime>,
    schema: SchemaRef,
    lifecycle: ResultLifecycle,
}

pub(crate) struct AsyncKernelReader {
    connection_operation: Option<OperationGuard>,
    executed: kernel::ExecutedAsyncStatement,
    stream: kernel::ResultStream,
    runtime: Arc<tokio::runtime::Runtime>,
    schema: SchemaRef,
    lifecycle: ResultLifecycle,
}

fn is_native_geospatial_field(field: &arrow_schema::Field) -> bool {
    let DataType::Struct(children) = field.data_type() else {
        return false;
    };
    children.iter().any(|child| {
        child.name() == "wkb"
            && (child
                .metadata()
                .get("geometry")
                .is_some_and(|value| value == "true")
                || child
                    .metadata()
                    .get("geography")
                    .is_some_and(|value| value == "true"))
    })
}

fn normalize_result_schema(schema: SchemaRef) -> SchemaRef {
    let mut changed = false;
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if is_native_geospatial_field(field) && !field.is_nullable() {
                changed = true;
                Arc::new(field.as_ref().clone().with_nullable(true))
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    if changed {
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
    } else {
        schema
    }
}

fn normalize_result_batch(
    batch: &RecordBatch,
    expected_schema: &SchemaRef,
) -> std::result::Result<RecordBatch, ArrowError> {
    let normalized_batch_schema = normalize_result_schema(batch.schema());
    if normalized_batch_schema != *expected_schema {
        return Err(ArrowError::SchemaError(
            "kernel result batch schema differs from the advertised result schema".into(),
        ));
    }
    if batch.schema() == *expected_schema {
        return Ok(batch.clone());
    }
    RecordBatch::try_new(Arc::clone(expected_schema), batch.columns().to_vec())
}

struct ResultLifecycle {
    generation: Arc<AtomicU64>,
    expected_generation: u64,
    done: bool,
}

impl ResultLifecycle {
    fn new(generation: Arc<AtomicU64>, expected_generation: u64) -> Self {
        Self {
            generation,
            expected_generation,
            done: false,
        }
    }

    fn invalidated(&self) -> bool {
        self.generation.load(Ordering::Acquire) != self.expected_generation
    }

    fn should_cancel_on_drop(&self) -> bool {
        !self.done
    }

    fn release(&mut self) -> bool {
        let should_cancel = self.should_cancel_on_drop();
        self.done = true;
        should_cancel
    }
}

impl KernelReader {
    pub(crate) fn new(
        runtime: Arc<tokio::runtime::Runtime>,
        executed: kernel::ExecutedStatement,
        generation: Arc<AtomicU64>,
        expected_generation: u64,
        connection_operation: OperationGuard,
    ) -> Self {
        let schema = normalize_result_schema(executed.schema());
        Self {
            connection_operation: Some(connection_operation),
            executed,
            runtime,
            schema,
            lifecycle: ResultLifecycle::new(generation, expected_generation),
        }
    }

    fn close(&mut self) {
        // ExecutedStatement::Drop retries a failed close, so the explicit
        // close here provides deterministic cleanup without losing the
        // kernel's best-effort backstop.
        let _ = self.runtime.block_on_safe(self.executed.close());
    }

    fn cancel_and_close(&mut self) {
        let _ = self.runtime.block_on_safe(self.executed.cancel());
        self.close();
    }

    fn finish(&mut self) {
        self.lifecycle.done = true;
        self.connection_operation = None;
    }
}

impl Iterator for KernelReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.lifecycle.done {
            return None;
        }
        if self
            .connection_operation
            .as_ref()
            .is_some_and(OperationGuard::is_cancelled)
        {
            self.finish();
            self.close();
            return Some(Err(ArrowError::ComputeError(
                "operation was cancelled".into(),
            )));
        }
        if self.lifecycle.invalidated() {
            self.finish();
            self.cancel_and_close();
            return Some(Err(ArrowError::ComputeError(
                "statement was reused while its result was being consumed".into(),
            )));
        }
        match self.runtime.block_on_safe(self.executed.next_batch()) {
            Ok(Some(kernel::ResultBatch::Arrow(batch))) => {
                Some(normalize_result_batch(batch, &self.schema))
            }
            Ok(Some(_)) => {
                self.finish();
                self.close();
                Some(Err(ArrowError::NotYetImplemented(
                    "kernel returned an unsupported result format".to_string(),
                )))
            }
            Ok(None) => {
                self.finish();
                None
            }
            Err(source) => {
                self.finish();
                self.close();
                Some(Err(ArrowError::ExternalError(Box::new(source))))
            }
        }
    }
}

impl Drop for KernelReader {
    fn drop(&mut self) {
        if self.lifecycle.release() {
            self.cancel_and_close();
        }
        self.connection_operation = None;
    }
}

impl RecordBatchReader for KernelReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl AsyncKernelReader {
    pub(crate) fn new(
        runtime: Arc<tokio::runtime::Runtime>,
        executed: kernel::ExecutedAsyncStatement,
        stream: kernel::ResultStream,
        generation: Arc<AtomicU64>,
        expected_generation: u64,
        connection_operation: OperationGuard,
    ) -> Self {
        let schema = normalize_result_schema(stream.schema());
        Self {
            connection_operation: Some(connection_operation),
            executed,
            stream,
            runtime,
            schema,
            lifecycle: ResultLifecycle::new(generation, expected_generation),
        }
    }

    fn finish(&mut self) {
        self.lifecycle.done = true;
        self.connection_operation = None;
    }

    fn close(&mut self) {
        let _ = self.runtime.block_on_safe(self.executed.close());
    }

    fn cancel_and_close(&mut self) {
        let _ = self.runtime.block_on_safe(self.executed.cancel());
        self.close();
    }
}

impl Iterator for AsyncKernelReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.lifecycle.done {
            return None;
        }
        if self
            .connection_operation
            .as_ref()
            .is_some_and(OperationGuard::is_cancelled)
        {
            self.finish();
            self.close();
            return Some(Err(ArrowError::ComputeError(
                "operation was cancelled".into(),
            )));
        }
        if self.lifecycle.invalidated() {
            self.finish();
            self.cancel_and_close();
            return Some(Err(ArrowError::ComputeError(
                "statement was reused while its result was being consumed".into(),
            )));
        }
        match self.runtime.block_on_safe(self.stream.next_batch()) {
            Ok(Some(kernel::ResultBatch::Arrow(batch))) => {
                Some(normalize_result_batch(batch, &self.schema))
            }
            Ok(Some(_)) => {
                self.finish();
                self.close();
                Some(Err(ArrowError::NotYetImplemented(
                    "kernel returned an unsupported result format".to_string(),
                )))
            }
            Ok(None) => {
                self.finish();
                self.close();
                None
            }
            Err(source) => {
                self.finish();
                self.close();
                Some(Err(ArrowError::ExternalError(Box::new(source))))
            }
        }
    }
}

impl Drop for AsyncKernelReader {
    fn drop(&mut self) {
        if self.lifecycle.release() {
            self.cancel_and_close();
        }
        self.connection_operation = None;
    }
}

impl RecordBatchReader for AsyncKernelReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub(crate) struct SequenceReader {
    connection_operation: Option<OperationGuard>,
    inner: Arc<Mutex<kernel::Statement>>,
    runtime: Arc<tokio::runtime::Runtime>,
    lifecycle: ResultLifecycle,
    base_spec: kernel::StatementSpec,
    parameter_style: ParameterStyle,
    client_timeout: Option<Duration>,
    rows: BoundRows,
    current: Option<kernel::ExecutedStatement>,
    schema: SchemaRef,
}

impl SequenceReader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inner: Arc<Mutex<kernel::Statement>>,
        runtime: Arc<tokio::runtime::Runtime>,
        generation: Arc<AtomicU64>,
        expected_generation: u64,
        base_spec: kernel::StatementSpec,
        parameter_style: ParameterStyle,
        client_timeout: Option<Duration>,
        rows: BoundRows,
        executed: kernel::ExecutedStatement,
        connection_operation: OperationGuard,
    ) -> Self {
        let schema = normalize_result_schema(executed.schema());
        Self {
            connection_operation: Some(connection_operation),
            inner,
            runtime,
            lifecycle: ResultLifecycle::new(generation, expected_generation),
            base_spec,
            parameter_style,
            client_timeout,
            rows,
            current: Some(executed),
            schema,
        }
    }

    fn close_current(&mut self) {
        if let Some(executed) = &mut self.current {
            let _ = self.runtime.block_on_safe(executed.close());
        }
        self.current = None;
    }

    fn cancel_and_close_current(&mut self) {
        if let Some(executed) = &self.current {
            let _ = self.runtime.block_on_safe(executed.cancel());
        }
        self.close_current();
    }

    fn fail(&mut self, error: ArrowError) -> Option<std::result::Result<RecordBatch, ArrowError>> {
        self.lifecycle.done = true;
        self.close_current();
        self.connection_operation = None;
        Some(Err(error))
    }
}

impl Iterator for SequenceReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.lifecycle.done {
            return None;
        }
        if self
            .connection_operation
            .as_ref()
            .is_some_and(OperationGuard::is_cancelled)
        {
            self.lifecycle.done = true;
            self.cancel_and_close_current();
            self.connection_operation = None;
            return Some(Err(ArrowError::ComputeError(
                "operation was cancelled".into(),
            )));
        }
        if self.lifecycle.invalidated() {
            self.lifecycle.done = true;
            self.cancel_and_close_current();
            self.connection_operation = None;
            return Some(Err(ArrowError::ComputeError(
                "statement was reused while its result was being consumed".into(),
            )));
        }

        loop {
            if let Some(executed) = &mut self.current {
                match self.runtime.block_on_safe(executed.next_batch()) {
                    Ok(Some(kernel::ResultBatch::Arrow(batch))) => {
                        return Some(normalize_result_batch(batch, &self.schema));
                    }
                    Ok(Some(_)) => {
                        return self.fail(ArrowError::NotYetImplemented(
                            "kernel returned an unsupported result format".into(),
                        ));
                    }
                    Ok(None) => self.close_current(),
                    Err(source) => {
                        return self.fail(ArrowError::ExternalError(Box::new(source)));
                    }
                }
            }

            let row = match self.rows.next_row() {
                Ok(Some(row)) => row,
                Ok(None) => {
                    self.lifecycle.done = true;
                    self.connection_operation = None;
                    return None;
                }
                Err(source) => return self.fail(ArrowError::ExternalError(Box::new(source))),
            };
            let executed = match execute_bound_row(
                &self.inner,
                &self.runtime,
                &self.base_spec,
                &row,
                self.parameter_style,
                self.client_timeout,
            ) {
                Ok(executed) => executed,
                Err(source) => return self.fail(ArrowError::ExternalError(Box::new(source))),
            };
            if normalize_result_schema(executed.schema()) != self.schema {
                let executed = executed;
                let _ = self.runtime.block_on_safe(executed.close());
                return self.fail(ArrowError::SchemaError(
                    "parameter executions returned inconsistent schemas".into(),
                ));
            }
            self.current = Some(executed);
        }
    }
}

impl RecordBatchReader for SequenceReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for SequenceReader {
    fn drop(&mut self) {
        if self.lifecycle.release() {
            self.cancel_and_close_current();
        } else {
            self.close_current();
        }
        self.connection_operation = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BinaryArray, Int32Array, Int64Array, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::collections::VecDeque;

    struct SyntheticReader {
        schema: SchemaRef,
        batches: VecDeque<std::result::Result<RecordBatch, ArrowError>>,
    }

    impl Iterator for SyntheticReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.pop_front()
        }
    }

    impl RecordBatchReader for SyntheticReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    fn int_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn slicing_reader_preserves_empty_batches_and_continues() {
        let schema = int_batch(vec![]).schema();
        let inner = SyntheticReader {
            schema,
            batches: VecDeque::from([
                Ok(int_batch(vec![])),
                Ok(int_batch(vec![])),
                Ok(int_batch(vec![1, 2, 3])),
            ]),
        };
        let mut reader = SlicingReader::new(Box::new(inner), 2);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 2);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        assert!(reader.next().is_none());
    }

    #[test]
    fn slicing_reader_propagates_lazy_fetch_errors() {
        let schema = int_batch(vec![]).schema();
        let inner = SyntheticReader {
            schema,
            batches: VecDeque::from([
                Ok(int_batch(vec![1, 2])),
                Err(ArrowError::ComputeError("injected fetch failure".into())),
            ]),
        };
        let mut reader = SlicingReader::new(Box::new(inner), 1);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
        assert!(reader
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("injected fetch failure"));
    }

    #[test]
    fn bound_rows_slices_every_row_in_order() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))])
                .unwrap();
        let mut rows = BoundRows::from_batch(batch);
        for expected in [10, 20, 30] {
            let row = rows.next_row().unwrap().unwrap();
            assert_eq!(row.num_rows(), 1);
            let values = row.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.value(0), expected);
        }
        assert!(rows.next_row().unwrap().is_none());
    }

    #[test]
    fn native_geospatial_schema_is_conservatively_nullable_across_delivery_paths() {
        let children = vec![
            Arc::new(Field::new("srid", DataType::Int32, false)),
            Arc::new(
                Field::new("wkb", DataType::Binary, false)
                    .with_metadata(HashMap::from([("geometry".into(), "true".into())])),
            ),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "geom",
            DataType::Struct(children.clone().into()),
            false,
        )]));
        let values = StructArray::new(
            children.into(),
            vec![
                Arc::new(Int32Array::from(vec![4326])),
                Arc::new(BinaryArray::from(vec![Some(&[1_u8, 2][..])])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap();

        let normalized_schema = normalize_result_schema(schema);
        assert!(normalized_schema.field(0).is_nullable());
        let normalized_batch = normalize_result_batch(&batch, &normalized_schema).unwrap();
        assert_eq!(normalized_batch.schema(), normalized_schema);
        assert_eq!(normalized_batch.num_rows(), 1);
    }

    #[test]
    fn ordinary_result_schema_is_not_rewritten() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        assert!(Arc::ptr_eq(
            &schema,
            &normalize_result_schema(Arc::clone(&schema))
        ));
    }

    #[test]
    fn result_lifecycle_observes_statement_reuse() {
        let generation = Arc::new(AtomicU64::new(7));
        let lifecycle = ResultLifecycle::new(Arc::clone(&generation), 7);
        assert!(!lifecycle.invalidated());

        generation.fetch_add(1, Ordering::AcqRel);

        assert!(lifecycle.invalidated());
    }

    #[test]
    fn result_lifecycle_cancels_incomplete_release_exactly_once() {
        let generation = Arc::new(AtomicU64::new(0));
        let mut lifecycle = ResultLifecycle::new(generation, 0);
        assert!(lifecycle.release());
        assert!(!lifecycle.release());
    }

    #[test]
    fn result_lifecycle_does_not_cancel_fully_consumed_release() {
        let generation = Arc::new(AtomicU64::new(0));
        let mut lifecycle = ResultLifecycle::new(generation, 0);
        lifecycle.done = true;
        assert!(!lifecycle.release());
    }
}
