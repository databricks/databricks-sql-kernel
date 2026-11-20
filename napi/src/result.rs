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

//! Arrow IPC payload types crossed across the napi boundary.
//!
//! Per sea-design.md Layer 2: "The binding ships the batch across the
//! FFI as Arrow IPC bytes. The adapter converts those bytes into
//! JavaScript rows…" — so the napi boundary is intentionally narrow:
//! one envelope per bounded group of ready batches, one per schema.

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use databricks_sql_kernel::{
    result_stream::TryNextBatch, Error as KernelError, ResultBatch, ResultStream,
};
use napi::bindgen_prelude::Buffer;

use crate::error::napi_err_from_kernel;

/// A single Arrow IPC stream payload encoding one or more record batches (plus
/// the schema header so the JS-side reader is stateless).
#[napi(object)]
pub struct ArrowBatch {
    /// Arrow IPC stream payload (schema header + record-batch messages).
    /// Decode with `apache-arrow`'s `RecordBatchReader`.
    pub ipc_bytes: Buffer,
}

/// An Arrow IPC stream payload encoding just the result schema (no
/// record-batch messages). Returned by `Statement.schema()`.
#[napi(object)]
pub struct ArrowSchema {
    /// Arrow IPC stream payload (schema header only, no record-batch
    /// messages). Decode with `apache-arrow`'s `RecordBatchReader` —
    /// the reader will expose the schema and immediately end.
    pub ipc_bytes: Buffer,
}

/// Encode an Arrow schema (and an optional single record batch) as a
/// complete, self-contained Arrow IPC stream payload.
///
/// Shared single source of truth for every batch/schema that crosses
/// the napi boundary: the sync `Statement` path (`schema()` /
/// `fetchNextBatch()` in `statement.rs`) and the async
/// `AsyncResultHandle` path (`async_statement.rs`) both call this, so
/// the two emit byte-identical envelopes and route encode failures
/// through the same kernel error envelope (`napi_err_from_kernel`) —
/// the TS reviver sees a typed `KernelError(code='Internal')` rather
/// than a bare `GenericFailure` string. Lives here in `result.rs`
/// because this module already owns `ArrowBatch` / `ArrowSchema`, the
/// payload types these bytes are wrapped in.
///
/// Returning a self-contained IPC stream per call is wasteful
/// header-wise but lets the JS adapter stay stateless — it decodes
/// each `ipcBytes` independently via the same `apache-arrow`
/// `RecordBatchReader` path.
pub(crate) fn encode_ipc_stream(
    schema: &SchemaRef,
    batch: Option<&RecordBatch>,
) -> napi::Result<Vec<u8>> {
    encode_ipc_batches(schema, batch.map(std::slice::from_ref).unwrap_or_default())
}

fn encode_ipc_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> napi::Result<Vec<u8>> {
    fn encode_err(stage: &str, e: impl std::fmt::Display) -> napi::Error {
        napi_err_from_kernel(
            KernelError::internal().message(format!("Arrow IPC writer {stage} failed: {e}")),
        )
    }
    // Pre-size the buffer so a typical 500KB batch doesn't reallocate
    // ~17 times growing from `Vec::new()`'s default capacity of 0.
    // `get_array_memory_size` is the kernel-side memory footprint (close
    // enough to the IPC-encoded size for capacity-hint purposes — IPC
    // adds ~hundreds of bytes of framing per batch, not orders of
    // magnitude). 4 KiB slack covers the schema-header on the
    // schema-only path (`batch = None`) and the framing overhead on
    // the schema+batch path.
    let estimated = batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum::<usize>()
        + 4096;
    let mut buf: Vec<u8> = Vec::with_capacity(estimated);
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, schema).map_err(|e| encode_err("init", e))?;
        for rb in batches {
            writer.write(rb).map_err(|e| encode_err("write", e))?;
        }
        writer.finish().map_err(|e| encode_err("finish", e))?;
    }
    Ok(buf)
}

pub(crate) trait BatchSource: Send {
    fn next_batch(
        &mut self,
    ) -> impl std::future::Future<Output = databricks_sql_kernel::Result<Option<&ResultBatch>>> + Send;

    fn try_next_batch(&mut self) -> databricks_sql_kernel::Result<TryNextBatch<'_>>;
}

impl BatchSource for ResultStream {
    async fn next_batch(&mut self) -> databricks_sql_kernel::Result<Option<&ResultBatch>> {
        self.next_batch().await
    }

    fn try_next_batch(&mut self) -> databricks_sql_kernel::Result<TryNextBatch<'_>> {
        self.try_next_batch()
    }
}

pub(crate) struct IpcStream<Source> {
    pub(crate) source: Source,
    pending: Option<napi::Result<RecordBatch>>,
}

impl<Source: BatchSource> IpcStream<Source> {
    pub(crate) fn new(source: Source) -> Self {
        Self {
            source,
            pending: None,
        }
    }

    pub(crate) async fn next(
        &mut self,
        schema: &SchemaRef,
        binding_name: &str,
    ) -> napi::Result<Option<Vec<u8>>> {
        let first = match self.pending.take() {
            Some(batch) => batch?,
            None => {
                let Some(batch) = self
                    .source
                    .next_batch()
                    .await
                    .map_err(napi_err_from_kernel)?
                else {
                    return Ok(None);
                };
                clone_arrow_batch(batch, binding_name)?
            }
        };
        let mut rows = first.num_rows();
        let mut bytes = first.get_array_memory_size();
        let mut batches = vec![first];
        while rows < 16_384 && bytes < 4 * 1024 * 1024 && batches.len() < 16 {
            let candidate = match self.source.try_next_batch() {
                Ok(TryNextBatch::Ready(batch)) => clone_arrow_batch(batch, binding_name),
                Ok(TryNextBatch::Pending | TryNextBatch::End) => break,
                Err(error) => Err(napi_err_from_kernel(error)),
                Ok(_) => Err(napi_err_from_kernel(
                    KernelError::invalid_argument()
                        .message("unsupported non-blocking result state"),
                )),
            };
            let candidate = match candidate {
                Ok(batch) => batch,
                Err(error) => {
                    self.pending = Some(Err(error));
                    break;
                }
            };
            let candidate_bytes = candidate.get_array_memory_size();
            if candidate.num_rows() > 16_384 - rows || candidate_bytes > 4 * 1024 * 1024 - bytes {
                self.pending = Some(Ok(candidate));
                break;
            }
            rows += candidate.num_rows();
            bytes += candidate_bytes;
            batches.push(candidate);
        }
        encode_ipc_batches(schema, &batches).map(Some)
    }
}

fn clone_arrow_batch(batch: &ResultBatch, binding_name: &str) -> napi::Result<RecordBatch> {
    match batch {
        ResultBatch::Arrow(batch) => Ok(batch.clone()),
        _ => Err(napi_err_from_kernel(KernelError::invalid_argument().message(format!(
            "non-Arrow ResultBatch variant from kernel — {binding_name} v{} cannot decode this batch shape, upgrade the binding",
            env!("CARGO_PKG_VERSION"),
        )))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    #[test]
    fn encode_ipc_stream_schema_only_yields_zero_batches() {
        let schema = make_schema();
        let bytes = encode_ipc_stream(&schema, None).expect("encode succeeds");
        let reader = StreamReader::try_new(bytes.as_slice(), None).expect("reader opens");
        let decoded_schema = reader.schema();
        let batches: Vec<_> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("decode succeeds");
        assert_eq!(decoded_schema.fields(), schema.fields());
        assert_eq!(batches.len(), 0, "schema-only payload yields 0 batches");
    }

    #[test]
    fn encode_ipc_stream_schema_plus_batch_round_trips() {
        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch builds");
        let bytes = encode_ipc_stream(&schema, Some(&batch)).expect("encode succeeds");
        let reader = StreamReader::try_new(bytes.as_slice(), None).expect("reader opens");
        let decoded: Vec<_> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("decode succeeds");
        assert_eq!(decoded.len(), 1, "exactly one batch decoded");
        assert_eq!(decoded[0].num_rows(), 3);
        assert_eq!(decoded[0].schema().fields(), schema.fields());
    }

    struct Source {
        items: std::collections::VecDeque<databricks_sql_kernel::Result<Option<RecordBatch>>>,
        current: Option<ResultBatch>,
        awaits: usize,
        probes: usize,
    }

    impl Source {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                items: batches.into_iter().map(|batch| Ok(Some(batch))).collect(),
                current: None,
                awaits: 0,
                probes: 0,
            }
        }

        fn take(&mut self) -> databricks_sql_kernel::Result<TryNextBatch<'_>> {
            match self.items.pop_front() {
                Some(Ok(Some(batch))) => {
                    self.current = Some(ResultBatch::Arrow(batch));
                    Ok(TryNextBatch::Ready(self.current.as_ref().unwrap()))
                }
                Some(Ok(None)) => Ok(TryNextBatch::Pending),
                Some(Err(error)) => Err(error),
                None => Ok(TryNextBatch::End),
            }
        }
    }

    impl BatchSource for Source {
        async fn next_batch(&mut self) -> databricks_sql_kernel::Result<Option<&ResultBatch>> {
            self.awaits += 1;
            match self.take()? {
                TryNextBatch::Ready(batch) => Ok(Some(batch)),
                TryNextBatch::End => Ok(None),
                _ => panic!("packing must not await a pending batch"),
            }
        }

        fn try_next_batch(&mut self) -> databricks_sql_kernel::Result<TryNextBatch<'_>> {
            self.probes += 1;
            self.take()
        }
    }

    fn int_batch(rows: usize, value: i32) -> RecordBatch {
        RecordBatch::try_new(
            make_schema(),
            vec![Arc::new(Int32Array::from(vec![value; rows]))],
        )
        .unwrap()
    }

    fn decode(bytes: Vec<u8>) -> Vec<RecordBatch> {
        StreamReader::try_new(bytes.as_slice(), None)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    async fn next(stream: &mut IpcStream<Source>, schema: &SchemaRef) -> Vec<RecordBatch> {
        decode(stream.next(schema, "test").await.unwrap().unwrap())
    }

    #[tokio::test]
    async fn packing_stops_at_row_target_and_replays_lookahead() {
        let first = int_batch(12_000, 1);
        let second = int_batch(12_000, 2);
        let mut stream = IpcStream::new(Source::new(vec![first.clone(), second.clone()]));
        assert_eq!(next(&mut stream, &make_schema()).await, vec![first]);
        assert_eq!(next(&mut stream, &make_schema()).await, vec![second]);
        assert!(stream.next(&make_schema(), "test").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn packing_never_probes_past_full_or_oversized_first_batch() {
        for rows in [16_384, 20_000] {
            let batch = int_batch(rows, 9);
            let mut stream = IpcStream::new(Source::new(vec![batch.clone(), int_batch(1, 2)]));
            assert_eq!(next(&mut stream, &make_schema()).await, vec![batch]);
            assert_eq!(stream.source.probes, 0);
            assert_eq!(
                next(&mut stream, &make_schema()).await,
                vec![int_batch(1, 2)]
            );
        }
    }

    #[tokio::test]
    async fn packing_byte_limit_defers_instead_of_dropping_large_batch() {
        use arrow_array::StringArray;
        let array = Arc::new(StringArray::from(vec![
            Some("x".repeat(3 * 1024 * 1024)),
            None,
        ]));
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let mut stream = IpcStream::new(Source::new(vec![batch.clone(), batch.clone()]));
        let first = next(&mut stream, &schema).await;
        assert_eq!(first.len(), 1);
        assert!(first[0] == batch);
        let second = next(&mut stream, &schema).await;
        assert_eq!(second.len(), 1);
        assert!(second[0] == batch);
        assert!(stream.next(&schema, "test").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn oversized_byte_batch_does_not_probe() {
        use arrow_array::StringArray;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "x".repeat(5 * 1024 * 1024)
            ]))],
        )
        .unwrap();
        let mut stream = IpcStream::new(Source::new(vec![batch.clone(), batch.clone()]));
        assert!(next(&mut stream, &schema).await == vec![batch]);
        assert_eq!(stream.source.probes, 0);
    }

    #[tokio::test]
    async fn empty_batches_are_preserved_and_batch_count_is_bounded() {
        let batches = vec![int_batch(0, 0); 32];
        let mut stream = IpcStream::new(Source::new(batches));
        assert_eq!(next(&mut stream, &make_schema()).await.len(), 16);
        assert_eq!(next(&mut stream, &make_schema()).await.len(), 16);
        assert!(stream.next(&make_schema(), "test").await.unwrap().is_none());
        assert_eq!(stream.source.awaits, 3);
    }

    #[tokio::test]
    async fn pending_batch_returns_without_another_await() {
        let mut source = Source::new(vec![int_batch(1, 1)]);
        source.items.extend([Ok(None), Ok(Some(int_batch(1, 2)))]);
        let mut stream = IpcStream::new(source);
        assert_eq!(
            next(&mut stream, &make_schema()).await,
            vec![int_batch(1, 1)]
        );
        assert_eq!(stream.source.awaits, 1);
        assert_eq!(
            next(&mut stream, &make_schema()).await,
            vec![int_batch(1, 2)]
        );
    }

    #[tokio::test]
    async fn lookahead_error_is_replayed_after_already_available_rows() {
        let mut source = Source::new(vec![int_batch(1, 1)]);
        source
            .items
            .push_back(Err(KernelError::io().message("late download error")));
        let mut stream = IpcStream::new(source);
        assert_eq!(
            next(&mut stream, &make_schema()).await,
            vec![int_batch(1, 1)]
        );
        let error = stream.next(&make_schema(), "test").await.unwrap_err();
        assert!(error.reason.contains("late download error"));
        assert_eq!(stream.source.awaits, 1);
    }

    #[tokio::test]
    async fn full_group_round_trips_without_array_concatenation() {
        let batch = int_batch(4096, 13);
        let mut stream = IpcStream::new(Source::new(vec![batch.clone(); 5]));
        assert_eq!(
            next(&mut stream, &make_schema()).await,
            vec![batch.clone(); 4]
        );
        assert_eq!(stream.source.probes, 3);
        assert_eq!(next(&mut stream, &make_schema()).await, vec![batch]);
    }

    #[tokio::test]
    async fn nullable_arrow_types_and_metadata_round_trip_across_messages() {
        use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
        use arrow_array::*;
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("key");
        map.values().append_null();
        map.append(true).unwrap();
        map.append(false).unwrap();
        let structure = StructArray::from(vec![(
            Arc::new(Field::new("nested", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("value"), None])) as ArrayRef,
        )]);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Int8Array::from(vec![Some(-1), None])),
            Arc::new(Int16Array::from(vec![Some(-300), None])),
            Arc::new(Int32Array::from(vec![Some(-70_000), None])),
            Arc::new(Int64Array::from(vec![Some(9_007_199_254_740_993), None])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
            Arc::new(Float32Array::from(vec![Some(1.25), None])),
            Arc::new(Float64Array::from(vec![Some(-0.0), None])),
            Arc::new(StringArray::from(vec![Some("héllo"), None])),
            Arc::new(LargeStringArray::from(vec![Some("world"), None])),
            Arc::new(BinaryArray::from(vec![Some(b"\x00\xff".as_slice()), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), None])
                    .with_precision_and_scale(20, 3)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![Some(-1), None])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_234_567), None]).with_timezone("UTC"),
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1_234_567),
                None,
            ])),
            Arc::new(DurationSecondArray::from(vec![Some(-1), None])),
            Arc::new(DurationMillisecondArray::from(vec![Some(-1001), None])),
            Arc::new(DurationMicrosecondArray::from(vec![Some(1_000_001), None])),
            Arc::new(DurationNanosecondArray::from(vec![
                Some(-1_000_000_001),
                None,
            ])),
            Arc::new(IntervalYearMonthArray::from(vec![Some(15), None])),
            Arc::new(IntervalDayTimeArray::from(vec![
                Some(types::IntervalDayTimeType::make_value(-1, 2)),
                None,
            ])),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                Some(types::IntervalMonthDayNanoType::make_value(1, -2, 3)),
                None,
            ])),
            Arc::new(ListArray::from_iter_primitive::<types::Int32Type, _, _>([
                Some(vec![Some(1), None]),
                None,
            ])),
            Arc::new(NullArray::new(2)),
            Arc::new(map.finish()),
            Arc::new(structure),
            Arc::new(
                Decimal256Array::from(vec![
                    Some(
                        "123456789012345678901234567890123456789012"
                            .parse()
                            .unwrap(),
                    ),
                    None,
                ])
                .with_precision_and_scale(50, 12)
                .unwrap(),
            ),
        ];
        let fields = arrays
            .iter()
            .enumerate()
            .map(|(index, array)| {
                Field::new(format!("col{index}"), array.data_type().clone(), true).with_metadata(
                    std::collections::HashMap::from([(
                        "databricks.type_name".into(),
                        format!("type{index}"),
                    )]),
                )
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut stream = IpcStream::new(Source::new(vec![batch.clone(); 3]));
        assert_eq!(next(&mut stream, &schema).await, vec![batch; 3]);
    }

    #[tokio::test]
    async fn changing_dictionaries_keep_each_batch_values() {
        use arrow_array::{types::Int8Type, DictionaryArray};
        let batches = ["first", "second"].map(|value| {
            let array = DictionaryArray::<Int8Type>::from_iter([Some(value), None]);
            RecordBatch::try_from_iter([("dictionary", Arc::new(array) as arrow_array::ArrayRef)])
                .unwrap()
        });
        let schema = batches[0].schema();
        let mut stream = IpcStream::new(Source::new(batches.to_vec()));
        assert_eq!(next(&mut stream, &schema).await, batches);
    }
}
