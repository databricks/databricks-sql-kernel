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
use arrow_schema::{DataType, SchemaRef};
use databricks_sql_kernel::{
    result_stream::TryNextBatch, Error as KernelError, ResultBatch, ResultStream,
};
use napi::bindgen_prelude::Buffer;

use crate::error::napi_err_from_kernel;

const IPC_TARGET_BYTES: usize = 1024 * 1024;

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
/// Schema getters and `IpcStream` share the same writer and typed error
/// envelope. Each payload stays self-contained for stateless JS readers;
/// fetch paths amortize the schema header across ready record batches.
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
    // IPC-decoded arrays can share a much larger backing buffer. Reserve for
    // the slices being encoded, with slack for schema and message framing.
    // Unsupported sizing falls back to the writer's normal buffer growth.
    let estimated = batches
        .iter()
        .map(sliced_batch_bytes)
        .filter(|&bytes| bytes != usize::MAX)
        .fold(
            4096usize.saturating_add(batches.len().saturating_mul(1024)),
            usize::saturating_add,
        );
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

fn sliced_batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .try_fold(0usize, |bytes, array| {
            bytes.checked_add(array.to_data().get_slice_memory_size().ok()?)
        })
        .unwrap_or(usize::MAX)
}

fn contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_dictionary(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_dictionary(field.data_type())),
        DataType::RunEndEncoded(ends, values) => {
            contains_dictionary(ends.data_type()) || contains_dictionary(values.data_type())
        }
        _ => false,
    }
}

pub(crate) trait BatchSource: Send {
    fn next_batch(
        &mut self,
    ) -> impl std::future::Future<Output = databricks_sql_kernel::Result<Option<&ResultBatch>>> + Send;

    fn try_next_ready_batch(&mut self) -> databricks_sql_kernel::Result<TryNextBatch<'_>>;
}

impl BatchSource for ResultStream {
    async fn next_batch(&mut self) -> databricks_sql_kernel::Result<Option<&ResultBatch>> {
        ResultStream::next_batch(self).await
    }

    fn try_next_ready_batch(&mut self) -> databricks_sql_kernel::Result<TryNextBatch<'_>> {
        ResultStream::try_next_ready_batch(self)
    }
}

pub(crate) struct IpcStream<Source> {
    pub(crate) source: Source,
    pending: Option<napi::Result<RecordBatch>>,
    packing_allowed: Option<bool>,
}

impl<Source: BatchSource> IpcStream<Source> {
    pub(crate) fn new(source: Source) -> Self {
        Self {
            source,
            pending: None,
            packing_allowed: None,
        }
    }

    pub(crate) async fn next(
        &mut self,
        schema: &SchemaRef,
        binding_name: &str,
    ) -> napi::Result<Option<Vec<u8>>> {
        // apache-arrow 13 ignores dictionary replacements within a stream.
        let packing_allowed = *self.packing_allowed.get_or_insert_with(|| {
            !schema
                .fields()
                .iter()
                .any(|field| contains_dictionary(field.data_type()))
        });
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
        let mut bytes = sliced_batch_bytes(&first);
        let mut batches = vec![first];
        while packing_allowed && rows < 16_384 && bytes < IPC_TARGET_BYTES && batches.len() < 16 {
            let candidate = match self.source.try_next_ready_batch() {
                Ok(TryNextBatch::Ready(batch)) => clone_arrow_batch(batch, binding_name),
                Ok(TryNextBatch::Pending | TryNextBatch::End) => break,
                Err(error) => Err(napi_err_from_kernel(error)),
                Ok(_) => break,
            };
            let candidate = match candidate {
                Ok(batch) => batch,
                Err(error) => {
                    self.pending = Some(Err(error));
                    break;
                }
            };
            let candidate_bytes = sliced_batch_bytes(&candidate);
            if candidate.num_rows() > 16_384 - rows || candidate_bytes > IPC_TARGET_BYTES - bytes {
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

    #[test]
    fn oversized_owned_batch_does_not_retain_a_doubled_output_buffer() {
        use arrow_array::StringArray;
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(StringArray::from(vec![Some("x".repeat(1536 * 1024)), None]))
                as arrow_array::ArrayRef,
        )])
        .unwrap();
        let bytes = encode_ipc_stream(&batch.schema(), Some(&batch)).unwrap();
        assert!(bytes.len() > IPC_TARGET_BYTES);
        assert!(bytes.capacity() < bytes.len() * 11 / 10);
        assert_eq!(decode(bytes), vec![batch]);
    }

    #[test]
    fn message_framing_does_not_double_the_output_buffer() {
        use arrow_array::{ArrayRef, Int64Array};
        let batch = RecordBatch::try_from_iter((0..16).map(|column| {
            (
                format!("column{column}"),
                Arc::new(Int64Array::from_iter(
                    (0..480).map(|row| (row % 11 != 0).then_some(row + column)),
                )) as ArrayRef,
            )
        }))
        .unwrap();
        let batches = vec![batch; 16];
        let bytes = encode_ipc_batches(&batches[0].schema(), &batches).unwrap();
        assert!(bytes.capacity() < bytes.len() * 11 / 10);
        assert_eq!(decode(bytes), batches);
    }

    struct Source {
        items: std::collections::VecDeque<databricks_sql_kernel::Result<Option<RecordBatch>>>,
        current: Option<ResultBatch>,
        awaits: usize,
        probes: usize,
        deferred_error: Option<KernelError>,
    }

    impl Source {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                items: batches.into_iter().map(|batch| Ok(Some(batch))).collect(),
                current: None,
                awaits: 0,
                probes: 0,
                deferred_error: None,
            }
        }

        fn take(&mut self, speculative: bool) -> databricks_sql_kernel::Result<TryNextBatch<'_>> {
            if self.deferred_error.is_some() {
                if speculative {
                    return Ok(TryNextBatch::Pending);
                }
                return Err(self.deferred_error.take().unwrap());
            }
            match self.items.pop_front() {
                Some(Ok(Some(batch))) => {
                    self.current = Some(ResultBatch::Arrow(batch));
                    Ok(TryNextBatch::Ready(self.current.as_ref().unwrap()))
                }
                Some(Ok(None)) => Ok(TryNextBatch::Pending),
                Some(Err(error)) if speculative => {
                    self.deferred_error = Some(error);
                    Ok(TryNextBatch::Pending)
                }
                Some(Err(error)) => Err(error),
                None => Ok(TryNextBatch::End),
            }
        }
    }

    impl BatchSource for Source {
        async fn next_batch(&mut self) -> databricks_sql_kernel::Result<Option<&ResultBatch>> {
            self.awaits += 1;
            match self.take(false)? {
                TryNextBatch::Ready(batch) => Ok(Some(batch)),
                TryNextBatch::End => Ok(None),
                _ => panic!("packing must not await a pending batch"),
            }
        }

        fn try_next_ready_batch(&mut self) -> databricks_sql_kernel::Result<TryNextBatch<'_>> {
            self.probes += 1;
            self.take(true)
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
            Some("x".repeat(IPC_TARGET_BYTES * 3 / 4)),
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
        assert_eq!(stream.source.awaits, 2);
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
    async fn decoded_wide_slices_share_backing_memory_without_inflating_budget() {
        use arrow_array::{ArrayRef, Int64Array};
        let batch = RecordBatch::try_from_iter((0..8).map(|column| {
            (
                format!("column{column}"),
                Arc::new(Int64Array::from_iter(
                    (0..2840).map(|row| (row % 11 != 0).then_some(row + column)),
                )) as ArrayRef,
            )
        }))
        .unwrap();
        let bytes = encode_ipc_batches(&batch.schema(), &[batch]).unwrap();
        let decoded =
            StreamReader::try_new(std::io::BufReader::new(std::io::Cursor::new(bytes)), None)
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
        assert!(decoded.get_array_memory_size() > IPC_TARGET_BYTES);
        assert!(sliced_batch_bytes(&decoded) < 200_000);
        let schema = decoded.schema();
        let mut stream = IpcStream::new(Source::new(vec![decoded.clone(); 6]));
        let bytes = stream.next(&schema, "test").await.unwrap().unwrap();
        assert!(bytes.len() < IPC_TARGET_BYTES);
        assert_eq!(decode(bytes), vec![decoded.clone(); 5]);
        assert_eq!(next(&mut stream, &schema).await, vec![decoded]);
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
    async fn dictionary_schemas_never_probe_or_pack_batches() {
        use arrow_array::{types::Int8Type, ArrayRef, DictionaryArray, StructArray};
        for nested in [false, true] {
            let batches = ["first", "second"].map(|value| {
                let dictionary =
                    Arc::new(DictionaryArray::<Int8Type>::from_iter([Some(value), None]))
                        as ArrayRef;
                let array = if nested {
                    Arc::new(StructArray::from(vec![(
                        Arc::new(Field::new("nested", dictionary.data_type().clone(), true)),
                        dictionary,
                    )])) as ArrayRef
                } else {
                    dictionary
                };
                RecordBatch::try_from_iter([("dictionary", array)]).unwrap()
            });
            let schema = batches[0].schema();
            let mut stream = IpcStream::new(Source::new(batches.to_vec()));
            for index in 0..2 {
                let bytes = stream.next(&schema, "test").await.unwrap().unwrap();
                if let Ok(directory) = std::env::var("DICT_FIXTURE_DIR") {
                    let directory = std::path::PathBuf::from(directory);
                    std::fs::create_dir_all(&directory).unwrap();
                    std::fs::write(directory.join(format!("{nested}-{index}.arrow")), &bytes)
                        .unwrap();
                }
                assert_eq!(
                    decode(bytes).len(),
                    1,
                    "dictionary payloads must be independent"
                );
                assert_eq!(
                    stream.source.probes, 0,
                    "dictionary data must not be speculatively consumed"
                );
            }
            assert!(stream.next(&schema, "test").await.unwrap().is_none());
        }
    }

    #[test]
    fn dictionary_guard_covers_every_nested_container() {
        use arrow_schema::{UnionFields, UnionMode};
        let field = Arc::new(Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ));
        let containers = vec![
            field.data_type().clone(),
            DataType::List(field.clone()),
            DataType::LargeList(field.clone()),
            DataType::ListView(field.clone()),
            DataType::LargeListView(field.clone()),
            DataType::FixedSizeList(field.clone(), 2),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            field.clone(),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Struct(vec![field.clone()].into()),
            DataType::Union(
                UnionFields::try_new([0], [field.clone()]).unwrap(),
                UnionMode::Dense,
            ),
            DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int16, false)), field),
        ];
        for data_type in containers {
            assert!(contains_dictionary(&data_type), "{data_type}");
            let nested =
                DataType::Struct(vec![Arc::new(Field::new("outer", data_type, true))].into());
            assert!(contains_dictionary(&nested));
        }
        assert!(!contains_dictionary(&DataType::Struct(
            vec![Arc::new(Field::new("plain", DataType::Utf8, true))].into()
        )));
    }
}
