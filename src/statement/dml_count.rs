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

//! Derive `num_modified_rows` for SEA DML statements from the result set.
//!
//! Thrift surfaces `numModifiedRows` on the operation **status**
//! (`TGetOperationStatusResp`, populated by the server on the terminal
//! status of a DML). The SEA REST API does **not** put a row count on its
//! status envelope at all — it returns the count as the statement's
//! *result set*, a single row of `num_*_rows` counters:
//!
//! | Statement | Result columns                                                          |
//! |-----------|-------------------------------------------------------------------------|
//! | INSERT    | `num_affected_rows`, `num_inserted_rows`                                |
//! | UPDATE    | `num_affected_rows`                                                      |
//! | DELETE    | `num_affected_rows`                                                      |
//! | MERGE     | `num_affected_rows`, `num_updated_rows`, `num_deleted_rows`, `num_inserted_rows` |
//!
//! `num_affected_rows` is the total, matching Thrift's `numModifiedRows`.
//! Reading it here lets the kernel's `num_modified_rows()` accessor reach
//! Thrift parity. The read is **non-consuming** (via
//! [`ResultStream::peek_first_batch`]) so the count row still flows to the
//! caller's fetch exactly as it does on the Thrift path — this only adds
//! the status value, it does not change which rows are returned.

use crate::client::transport::ExecuteResult;
use crate::kernel_error::{Error, Result};
use crate::reader::InMemoryReader;
use crate::result_stream::{ResultBatch, ResultStream};
use crate::types::sea::ResultManifest;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, Int8Type};
use arrow_array::Array;
use arrow_schema::DataType;

/// Decode native batch counts without expanding repeated rows. Preserve the
/// count result for fetching; a decoding failure must never resubmit the write.
pub(crate) async fn decode_parameter_batch(
    mut result: ExecuteResult,
    parameter_set_count: usize,
) -> Result<ExecuteResult> {
    if parameter_set_count == 0 {
        return Ok(result);
    }
    let decoded = async {
        result.status.num_modified_rows = None;
        let schema = result.reader.schema().await?;
        if schema.fields().is_empty() {
            return Ok(());
        }
        let affected = schema
            .index_of(AFFECTED_ROWS_COLUMN)
            .map_err(|_| malformed_batch_count("missing num_affected_rows"))?;
        let repeat = schema.index_of("repeat").ok();
        let mut represented = 0usize;
        let mut total = Some(0i64);
        let mut batches = Vec::new();
        while let Some(mut batch) = result.reader.next_batch().await? {
            let actual_schema = batch.schema();
            let compatible = actual_schema.fields().len() == schema.fields().len()
                && actual_schema
                    .fields()
                    .iter()
                    .zip(schema.fields())
                    .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type());
            if !compatible {
                return Err(malformed_batch_count("inconsistent count schema"));
            }
            for row in 0..batch.num_rows() {
                let repetitions = match repeat {
                    Some(i) => batch_count_value(batch.column(i).as_ref(), row)?
                        .filter(|n| *n > 0)
                        .ok_or_else(|| malformed_batch_count("repeat must be positive"))?,
                    None => 1,
                };
                represented = usize::try_from(repetitions)
                    .ok()
                    .and_then(|n| represented.checked_add(n))
                    .filter(|n| *n <= parameter_set_count)
                    .ok_or_else(|| malformed_batch_count("too many parameter-set counts"))?;
                let count = batch_affected_value(batch.column(affected).as_ref(), row)?;
                total = match (total, count) {
                    (Some(sum), Some(n)) => Some(
                        n.checked_mul(repetitions)
                            .and_then(|n| sum.checked_add(n))
                            .ok_or_else(|| malformed_batch_count("affected count overflow"))?,
                    ),
                    _ => None,
                };
            }
            if actual_schema.fields() != schema.fields() {
                batch = arrow_array::RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
                    .map_err(|error| {
                        malformed_batch_count(&format!("inconsistent count nullability: {error}"))
                    })?;
            }
            batches.push(batch);
        }
        if represented != 0 && represented != parameter_set_count {
            return Err(malformed_batch_count("missing parameter-set counts"));
        }
        result.status.num_modified_rows = if represented == 0 { None } else { total };
        let metrics = result.reader.metrics();
        result.reader = Box::new(InMemoryReader::new(schema, batches).with_metrics(metrics));
        Ok(())
    }
    .await;
    decoded.map_err(|error: Error| {
        error
            .with_query_id(&result.statement_id)
            .with_retryable(false)
    })?;
    Ok(result)
}

fn malformed_batch_count(detail: &str) -> Error {
    Error::data_loss().message(format!(
        "Native parameter batch succeeded but its counts could not be decoded: {detail}. Rows may already be committed",
    ))
}

fn batch_affected_value(column: &dyn Array, row: usize) -> Result<Option<i64>> {
    match batch_count_value(column, row)? {
        // SUCCESS_NO_INFO means execution succeeded without a precise count.
        None | Some(-2) => Ok(None),
        Some(n) if n >= 0 => Ok(Some(n)),
        _ => Err(malformed_batch_count("invalid negative affected count")),
    }
}

fn batch_count_value(column: &dyn Array, row: usize) -> Result<Option<i64>> {
    if column.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match column.data_type() {
        DataType::Int64 => column.as_primitive::<Int64Type>().value(row),
        DataType::Int32 => column.as_primitive::<Int32Type>().value(row) as i64,
        DataType::Int16 => column.as_primitive::<Int16Type>().value(row) as i64,
        DataType::Int8 => column.as_primitive::<Int8Type>().value(row) as i64,
        _ => return Err(malformed_batch_count("count must be an integer")),
    }))
}

/// The exact set of column names a SEA DML count result is built from.
const DML_COUNT_COLUMNS: [&str; 4] = [
    "num_affected_rows",
    "num_inserted_rows",
    "num_updated_rows",
    "num_deleted_rows",
];

/// The total-affected column whose value maps to `num_modified_rows`.
const AFFECTED_ROWS_COLUMN: &str = "num_affected_rows";

/// Does this result `manifest` describe a DML row-count result?
///
/// Same strict signature as [`read_dml_affected_rows`], but checked against
/// the manifest's column metadata so a caller can decide whether the inline
/// result is worth decoding *before* building a reader — keeping SELECTs (real
/// columns) off the decode path entirely.
pub(crate) fn manifest_is_dml_count(manifest: &ResultManifest) -> bool {
    let cols = &manifest.schema.columns;
    !cols.is_empty()
        && cols
            .iter()
            .all(|c| DML_COUNT_COLUMNS.contains(&c.name.as_str()))
        && cols.iter().any(|c| c.name == AFFECTED_ROWS_COLUMN)
}

/// If `stream`'s result set is a DML row-count result, return its
/// `num_affected_rows` value; otherwise `None`.
///
/// Detection is deliberately strict — **every** column must be one of the
/// known `num_*_rows` counters and `num_affected_rows` must be present —
/// so a `SELECT 1 AS num_affected_rows` (a real query that merely aliases
/// a column to that name) is not mistaken for a DML result. Thrift never
/// faces this ambiguity because it reads the count from operation
/// metadata rather than the result columns; the strict signature keeps us
/// as close as the SEA wire shape allows.
///
/// Non-consuming: peeks the first batch and buffers it, so the caller's
/// subsequent fetch still sees the row.
pub(crate) async fn read_dml_affected_rows(stream: &mut ResultStream) -> Result<Option<i64>> {
    let schema = stream.schema();
    let fields = schema.fields();
    if fields.is_empty() {
        return Ok(None);
    }
    if !fields
        .iter()
        .all(|f| DML_COUNT_COLUMNS.contains(&f.name().as_str()))
    {
        return Ok(None);
    }
    let Some(col_idx) = fields.iter().position(|f| f.name() == AFFECTED_ROWS_COLUMN) else {
        return Ok(None);
    };

    let Some(ResultBatch::Arrow(batch)) = stream.peek_first_batch().await? else {
        return Ok(None);
    };
    // A DML count result is always exactly one summary row.
    if batch.num_rows() != 1 {
        return Ok(None);
    }
    let col = batch.column(col_idx);
    if col.is_null(0) {
        return Ok(None);
    }
    // The server types these BIGINT (Arrow Int64); accept the narrower
    // signed widths defensively, and ignore anything else rather than
    // mis-reporting.
    let value = match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().value(0),
        DataType::Int32 => col.as_primitive::<Int32Type>().value(0) as i64,
        DataType::Int16 => col.as_primitive::<Int16Type>().value(0) as i64,
        DataType::Int8 => col.as_primitive::<Int8Type>().value(0) as i64,
        _ => return Ok(None),
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{InMemoryReader, ResultReaderMetrics};
    use crate::telemetry::ExecutionResultFormat;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn batch_counts_preserve_reader_metrics() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            AFFECTED_ROWS_COLUMN,
            DataType::Int64,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![2, 3]))])
                .unwrap();
        let metrics = ResultReaderMetrics {
            total_chunks_present: Some(1),
            total_chunks_iterated: Some(1),
            sum_chunks_download_time: Some(std::time::Duration::from_millis(7)),
            ..Default::default()
        };
        let result = ExecuteResult {
            statement_id: "batch-metrics".into(),
            reader: Box::new(
                InMemoryReader::new(schema.clone(), vec![batch.clone()])
                    .with_metrics(Some(metrics)),
            ),
            manifest: None,
            telemetry_result_format: ExecutionResultFormat::InlineArrow,
            telemetry_is_compressed: false,
            status: serde_json::from_str(r#"{"state":"SUCCEEDED"}"#).unwrap(),
        };
        let result = decode_parameter_batch(result, 2).await.unwrap();
        assert_eq!(result.status.num_modified_rows, Some(5));
        let retained = result.reader.metrics().expect("source metrics retained");
        assert_eq!(retained.total_chunks_present, metrics.total_chunks_present);
        assert_eq!(
            retained.total_chunks_iterated,
            metrics.total_chunks_iterated
        );
        assert_eq!(
            retained.sum_chunks_download_time,
            metrics.sum_chunks_download_time
        );
    }

    /// Build a single-batch `ResultStream` from `(name, Int64 values)` columns.
    async fn count_stream(cols: &[(&str, &[i64])]) -> ResultStream {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, _)| Field::new(*n, DataType::Int64, false))
            .collect();
        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|(_, v)| Arc::new(Int64Array::from(v.to_vec())) as ArrayRef)
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap()
    }

    #[test]
    fn manifest_detection_gates_on_strict_signature() {
        let dml: ResultManifest = serde_json::from_str(
            r#"{"format":"ARROW_STREAM","schema":{"column_count":2,"columns":[
                {"name":"num_affected_rows","type_name":"LONG","type_text":"BIGINT","position":0},
                {"name":"num_inserted_rows","type_name":"LONG","type_text":"BIGINT","position":1}]},
                "total_row_count":1}"#,
        )
        .unwrap();
        assert!(manifest_is_dml_count(&dml));

        let select: ResultManifest = serde_json::from_str(
            r#"{"format":"ARROW_STREAM","schema":{"column_count":1,"columns":[
                {"name":"id","type_name":"INT","type_text":"INT","position":0}]},
                "total_row_count":3}"#,
        )
        .unwrap();
        assert!(!manifest_is_dml_count(&select));

        // Leads with the counter name but has a non-counter column → declined.
        let aliased: ResultManifest = serde_json::from_str(
            r#"{"format":"ARROW_STREAM","schema":{"column_count":2,"columns":[
                {"name":"num_affected_rows","type_name":"LONG","type_text":"BIGINT","position":0},
                {"name":"label","type_name":"STRING","type_text":"STRING","position":1}]},
                "total_row_count":1}"#,
        )
        .unwrap();
        assert!(!manifest_is_dml_count(&aliased));
    }

    #[tokio::test]
    async fn insert_shape_returns_affected_rows() {
        // INSERT → [num_affected_rows, num_inserted_rows].
        let mut s = count_stream(&[("num_affected_rows", &[3]), ("num_inserted_rows", &[3])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn update_delete_shape_returns_affected_rows() {
        let mut s = count_stream(&[("num_affected_rows", &[2])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn merge_shape_returns_affected_not_other_counters() {
        // MERGE → [num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows];
        // we report num_affected_rows (the total), not a sibling column.
        let mut s = count_stream(&[
            ("num_affected_rows", &[2]),
            ("num_updated_rows", &[1]),
            ("num_deleted_rows", &[0]),
            ("num_inserted_rows", &[1]),
        ])
        .await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn zero_affected_rows_is_a_real_value_not_none() {
        let mut s = count_stream(&[("num_affected_rows", &[0])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn peek_is_non_consuming_count_row_still_fetchable() {
        // The derive must not steal the row from the caller's fetch — parity
        // with Thrift, whose DML fetch also returns the count row.
        let mut s = count_stream(&[("num_affected_rows", &[7])]).await;
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), Some(7));
        // The single count row is still there for next_batch().
        let batch = s.next_batch().await.unwrap().expect("count row preserved");
        let ResultBatch::Arrow(rb) = batch;
        assert_eq!(rb.num_rows(), 1);
        assert_eq!(rb.column(0).as_primitive::<Int64Type>().value(0), 7);
        // And the stream is then exhausted.
        assert!(s.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn select_with_real_columns_is_not_a_dml_count() {
        // A genuine SELECT: not all columns are counters → None, and nothing
        // is peeked/consumed.
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut s = ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
        // Untouched: the real row is the first thing the caller fetches.
        assert!(s.next_batch().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn select_aliasing_a_counter_name_is_rejected_by_strict_match() {
        // `SELECT 1 AS num_affected_rows, 'x' AS label` — leads with a counter
        // name but `label` is not a counter, so the strict all-columns guard
        // declines rather than mis-reporting a row count.
        let fields = vec![
            Field::new("num_affected_rows", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut s = ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
    }

    #[tokio::test]
    async fn empty_schema_ddl_is_not_a_dml_count() {
        let schema = Arc::new(Schema::empty());
        let s_reader = InMemoryReader::new(schema.clone(), vec![]);
        let mut s = ResultStream::from_reader(Box::new(s_reader)).await.unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
    }

    #[tokio::test]
    async fn null_affected_rows_reports_none() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "num_affected_rows",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None as Option<i64>])) as ArrayRef],
        )
        .unwrap();
        let mut s = ResultStream::from_reader(Box::new(InMemoryReader::new(schema, vec![batch])))
            .await
            .unwrap();
        assert_eq!(read_dml_affected_rows(&mut s).await.unwrap(), None);
    }
}
