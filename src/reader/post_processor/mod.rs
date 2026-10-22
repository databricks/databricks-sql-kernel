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

//! Consolidated post-processing for result batches.
//!
//! The kernel performs a small amount of work between the wire reader
//! and the consumer:
//!
//! - **Manifest augmentation**: copy server-reported per-column
//!   metadata (`databricks.type_name`, precision, scale, interval
//!   kind, …) onto the Arrow schema so consumers reading
//!   `batch.schema()` see it.
//! - **Complex-type JSON stringification** (optional, off by
//!   default): replace `List` / `Map` / `Struct` columns with `Utf8`
//!   columns of compact JSON text, matching the Thrift wire
//!   `complexTypesAsArrow=false` output. Opt in per session via
//!   [`ResultConfig::complex_types_as_json`](crate::config::ResultConfig::complex_types_as_json).
//!
//! Both transforms share one wrapper so the hot path is a single
//! `next_batch` indirection regardless of how many transforms are
//! active, and so the "no work" case is genuinely zero-cost.
//!
//! See `docs/designs/complex-types-json-stringify.md` for the design.

mod geospatial;
mod json;

use crate::config::GeospatialResultRepresentation;
use crate::kernel_error::{Error, Result};
use crate::reader::{ResultReader, ResultReaderAdapter, ResultReaderMetrics, TryNextBatch};
use crate::types::sea::ResultManifest;
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::sync::Arc;

/// Optional per-batch transforms the post-processor applies.
#[derive(Debug, Clone, Default)]
pub(crate) struct PostProcessOptions {
    /// When `true`, complex columns (`List` / `LargeList` /
    /// `FixedSizeList` / `Map` / `Struct`) are replaced with `Utf8`
    /// columns containing compact JSON text per row. See
    /// [`ResultConfig::complex_types_as_json`](crate::config::ResultConfig::complex_types_as_json).
    pub complex_types_as_json: bool,
    /// When `true`, `Interval(*)` and `Duration(*)` columns are
    /// replaced with `Utf8` columns containing the canonical
    /// Databricks text form (e.g. `"2-6"` for YEAR-MONTH,
    /// `"3 12:30:15.000000000"` for DAY-TIME).
    ///
    /// Motivation: pyarrow's Python bindings can't decode Arrow's
    /// `month_interval` type (id 21) at all — `.as_py()` / `.to_pylist()` /
    /// `cast(string)` / `to_pandas()` all fail. The Python connector
    /// requests this flag so interval results are usable from
    /// Python; non-Python consumers can leave it `false` if they
    /// have a working interval decoder.
    pub intervals_as_string: bool,
    /// Client-facing physical representation for logical GEOMETRY and
    /// GEOGRAPHY columns. The structured logical manifest schema is reconciled
    /// with the physical `struct<srid,wkb>` attachment, then exposed with
    /// canonical tags or recursively rendered as EWKT.
    pub geospatial_result_representation: GeospatialResultRepresentation,
}

/// Wrapper that runs all per-batch post-processing the kernel does
/// today on the way out to consumers, in a single pass.
///
/// Consolidates what was previously a `ManifestAugmentingReader` (PR
/// #35) and reserves capacity for future transforms (e.g. interval
/// stringification, GEOMETRY normalisation) without growing a new
/// wrapper layer per transform.
///
/// The constructor inspects the upstream schema + manifest + options
/// once, decides what — if anything — needs to happen per batch, and
/// stores a single `Option<BatchTransform>` closure. When the closure
/// is `None`, `next_batch` is true pass-through (no allocation, no
/// rebuild); when it's `Some`, the closure runs every active
/// transform in one fused walk so we never pay multiple
/// `RecordBatch::try_new` rebuilds for the same upstream batch.
pub(crate) struct ResultPostProcessor {
    inner: Box<dyn ResultReader + Send>,
    schema: SchemaRef,
    transform: Option<BatchTransform>,
}

type BatchTransform = Box<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>;

impl ResultPostProcessor {
    fn process_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.transform {
            Some(transform) => transform(&batch),
            None => Ok(batch),
        }
    }

    /// Build a post-processor around `inner`.
    ///
    /// `manifest` is the SEA `ResultManifest` carrying server-reported
    /// per-column metadata; `None` for upstreams that don't have one
    /// (e.g. legacy paths). `options` toggles optional transforms
    /// (today: complex-type JSON stringification).
    pub(crate) async fn new(
        inner: Box<dyn ResultReader + Send>,
        manifest: Option<&ResultManifest>,
        options: PostProcessOptions,
    ) -> Result<Self> {
        let inner_schema = inner.schema().await?;

        // 1. Compute the output schema after manifest augmentation
        //    (no-op when manifest is None).
        let augmented_schema = match manifest {
            Some(m) => ResultReaderAdapter::augment_schema_with_manifest(&inner_schema, m),
            None => inner_schema.clone(),
        };
        let logical_schema = match manifest {
            Some(manifest) => Some(
                super::ResultReaderFactory::extract_logical_schema_from_manifest(Some(manifest))?,
            ),
            None => None,
        };

        // 2. Compute the normalized schema and the set of per-column
        //    rewrites. Geo normalization runs first because a geo leaf may be
        //    nested inside a complex column that is subsequently rendered as
        //    JSON. Binary geo mode may rebuild array containers to attach
        //    metadata, but it reuses the existing child/value buffers.
        let mut json_columns: Vec<usize> = Vec::new();
        let mut interval_columns: Vec<usize> = Vec::new();
        let mut null_columns: Vec<usize> = Vec::new();
        let mut geospatial_columns: Vec<usize> = Vec::new();
        let mut normalized_fields: Vec<Field> = Vec::with_capacity(augmented_schema.fields().len());
        let mut output_fields: Vec<Field> = Vec::with_capacity(augmented_schema.fields().len());
        let mut schema_changed = false;
        for (i, field) in augmented_schema.fields().iter().enumerate() {
            let logical_field = logical_schema
                .as_ref()
                .and_then(|schema| schema.fields().get(i))
                .map(|field| field.as_ref());
            let geo_plan = geospatial::plan(
                field,
                logical_field,
                options.geospatial_result_representation,
            )?;
            let (normalized_field, is_top_level_geo) = match geo_plan {
                Some(plan) => {
                    if plan.needs_rewrite {
                        schema_changed = true;
                        geospatial_columns.push(i);
                    }
                    (
                        field.as_ref().clone().with_data_type(plan.output_type),
                        plan.is_top_level_geo,
                    )
                }
                None => (field.as_ref().clone(), false),
            };

            let normalized_type = normalized_field.data_type();
            let output_field = if options.complex_types_as_json
                && !is_top_level_geo
                && json::is_complex_type(normalized_type)
            {
                json_columns.push(i);
                schema_changed = true;
                normalized_field.clone().with_data_type(DataType::Utf8)
            } else if options.intervals_as_string && is_interval_or_duration_type(normalized_type) {
                interval_columns.push(i);
                schema_changed = true;
                normalized_field.clone().with_data_type(DataType::Utf8)
            } else if matches!(normalized_type, DataType::Null) {
                // Untyped NULL (`SELECT NULL` / VOID) arrives as an Arrow
                // `Null`-typed column, but the server reports it as STRING
                // (the manifest carries `type_name=NULL`/`type_text=VOID`,
                // whose canonical Arrow surface is Utf8) and the Thrift /
                // JDBC reference exposes it as STRING — identically to a
                // `CAST(NULL AS STRING)` sibling in the same query. Surface
                // the server type: rewrite the column to an all-null `Utf8`.
                // Always on (not option-gated); `DataType::Null` never
                // carries values, so this is lossless. See issue #203
                // (DATATYPE-041).
                null_columns.push(i);
                schema_changed = true;
                normalized_field.clone().with_data_type(DataType::Utf8)
            } else {
                normalized_field.clone()
            };
            normalized_fields.push(normalized_field);
            output_fields.push(output_field);
        }
        let normalized_schema: SchemaRef = if geospatial_columns.is_empty() {
            augmented_schema.clone()
        } else {
            Arc::new(Schema::new_with_metadata(
                normalized_fields,
                augmented_schema.metadata().clone(),
            ))
        };
        let output_schema: SchemaRef = if schema_changed {
            Arc::new(Schema::new_with_metadata(
                output_fields,
                augmented_schema.metadata().clone(),
            ))
        } else {
            augmented_schema.clone()
        };

        // 3. Decide whether we need a per-batch transform at all.
        //
        // Cases:
        //  - `inner_schema == output_schema` (Arc-eq or
        //    structurally identical) and no column rewrites: no work
        //    needed.
        //  - Output schema differs only in field metadata (manifest
        //    augmentation, no column rewrites): a single
        //    `RecordBatch::try_new` re-project onto the augmented
        //    schema.
        //  - Any rewrites present: fused walk that re-projects
        //    primitives and emits new Utf8 arrays for the rewritten
        //    columns.
        let needs_rebuild = !schemas_field_equal(&inner_schema, &output_schema);
        let has_rewrites = !geospatial_columns.is_empty()
            || !json_columns.is_empty()
            || !interval_columns.is_empty()
            || !null_columns.is_empty();
        let transform: Option<BatchTransform> = if !needs_rebuild && !has_rewrites {
            None
        } else if !has_rewrites {
            // Manifest-only path: cheap re-project. Reuses upstream
            // ArrayRefs unchanged.
            let target_schema = output_schema.clone();
            Some(Box::new(move |batch: &RecordBatch| {
                let columns: Vec<ArrayRef> = batch.columns().to_vec();
                RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
                    Error::internal().message(format!(
                        "ResultPostProcessor: upstream batch shape disagrees with the schema it advertised: {e}"
                    ))
                })
            }))
        } else {
            // Fused walk: re-project primitives, stringify complex
            // columns (JSON path), stringify intervals/durations
            // (interval path). Captures only what each closure
            // invocation actually needs.
            let target_schema = output_schema.clone();
            let normalized = normalized_schema.clone();
            let json_cols: Vec<usize> = json_columns;
            let interval_cols: Vec<usize> = interval_columns;
            let null_cols: Vec<usize> = null_columns;
            let geo_cols: Vec<usize> = geospatial_columns;
            Some(Box::new(move |batch: &RecordBatch| {
                build_rewritten_batch(
                    batch,
                    &normalized,
                    &target_schema,
                    &geo_cols,
                    &json_cols,
                    &interval_cols,
                    &null_cols,
                )
            }))
        };

        Ok(Self {
            inner,
            schema: output_schema,
            transform,
        })
    }
}

#[async_trait]
impl ResultReader for ResultPostProcessor {
    async fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.inner.next_batch().await? else {
            return Ok(None);
        };
        Ok(Some(self.process_batch(batch)?))
    }

    fn try_next_batch(&mut self) -> Result<TryNextBatch> {
        let next = self.inner.try_next_batch()?;
        next.map_ready(|batch| self.process_batch(batch))
    }

    fn metrics(&self) -> Option<ResultReaderMetrics> {
        self.inner.metrics()
    }
}

/// Compare schemas by their field sequence including per-field
/// metadata, names, and nullability. `Field::eq` also covers rewritten types.
fn schemas_field_equal(a: &SchemaRef, b: &SchemaRef) -> bool {
    if Arc::ptr_eq(a, b) {
        return true;
    }
    if a.fields().len() != b.fields().len() {
        return false;
    }
    a.fields()
        .iter()
        .zip(b.fields().iter())
        .all(|(x, y)| x == y)
        && a.metadata() == b.metadata()
}

/// Build a `RecordBatch` whose tagged geo values are first recursively
/// normalized, complex columns (indexed by `json_cols`) are replaced with
/// `Utf8` arrays of compact JSON text, interval / duration columns (indexed by
/// `interval_cols`) are replaced with canonical Databricks text, and untyped
/// NULL columns (indexed by `null_cols`) become all-null `Utf8` arrays.
///
/// All index sets are sorted. `geo_cols` may overlap `json_cols` when a
/// complex column contains a geo leaf; geo normalization intentionally runs
/// first so JSON sees the normalized nested schema and values. The other
/// rewrite sets are pairwise disjoint.
fn build_rewritten_batch(
    batch: &RecordBatch,
    normalized: &SchemaRef,
    output: &SchemaRef,
    geo_cols: &[usize],
    json_cols: &[usize],
    interval_cols: &[usize],
    null_cols: &[usize],
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    let mut next_geo_idx = 0;
    let mut next_json_idx = 0;
    let mut next_interval_idx = 0;
    let mut next_null_idx = 0;
    for (col_idx, column) in batch.columns().iter().enumerate() {
        let normalized_field = normalized.fields().get(col_idx).ok_or_else(|| {
            Error::internal().message(format!(
                "ResultPostProcessor: upstream batch has {} columns but normalized schema only has {}",
                batch.num_columns(),
                normalized.fields().len()
            ))
        })?;
        let is_geo = geo_cols
            .get(next_geo_idx)
            .copied()
            .is_some_and(|target| target == col_idx);
        let is_json = json_cols
            .get(next_json_idx)
            .copied()
            .is_some_and(|target| target == col_idx);
        let is_interval = interval_cols
            .get(next_interval_idx)
            .copied()
            .is_some_and(|target| target == col_idx);
        let is_null_col = null_cols
            .get(next_null_idx)
            .copied()
            .is_some_and(|target| target == col_idx);

        let column = if is_geo {
            next_geo_idx += 1;
            geospatial::rewrite(
                column,
                normalized_field.data_type(),
                normalized_field.name(),
            )?
        } else {
            column.clone()
        };

        if is_json {
            next_json_idx += 1;
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            let mut scratch = String::new();
            for row in 0..num_rows {
                if column.is_null(row) {
                    builder.append_null();
                    continue;
                }
                scratch.clear();
                json::append_json(
                    &mut scratch,
                    column.as_ref(),
                    row,
                    normalized_field.as_ref(),
                )?;
                builder.append_value(&scratch);
            }
            columns.push(Arc::new(builder.finish()));
        } else if is_interval {
            next_interval_idx += 1;
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            let dt = column.data_type();
            for row in 0..num_rows {
                if column.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let s = match dt {
                    DataType::Interval(_) => json::format_interval(column.as_ref(), row)?,
                    DataType::Duration(_) => json::format_duration(column.as_ref(), row)?,
                    other => {
                        return Err(Error::internal().message(format!(
                            "intervals_as_string: column {col_idx} expected Interval/Duration, got {other:?}"
                        )));
                    }
                };
                builder.append_value(&s);
            }
            columns.push(Arc::new(builder.finish()));
        } else if is_null_col {
            next_null_idx += 1;
            // `DataType::Null` carries no values, so every row is null:
            // emit an all-null `Utf8` array of the same length. Matches
            // the server-reported STRING type for untyped NULL / VOID.
            columns.push(arrow_array::new_null_array(&DataType::Utf8, num_rows));
        } else {
            columns.push(column);
        }
    }

    RecordBatch::try_new(output.clone(), columns).map_err(|e| {
        Error::internal().message(format!(
            "ResultPostProcessor: failed to assemble rewritten batch: {e}"
        ))
    })
}

/// True iff `dt` is an Arrow `Interval(*)` or `Duration(*)` — the
/// types the `intervals_as_string` post-processor stringifies.
fn is_interval_or_duration_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Interval(_) | DataType::Duration(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::metadata_keys;
    use crate::reader::test_utils::MockReader;
    use crate::reader::ResultReader;
    use crate::types::sea::{ColumnInfo, ResultSchema};
    use arrow_array::builder::{
        BinaryBuilder, Int32Builder, ListBuilder, MapBuilder, StringBuilder,
    };
    use arrow_array::{
        Array, BinaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::Fields;

    fn make_manifest(columns: Vec<ColumnInfo>) -> ResultManifest {
        ResultManifest {
            format: "ARROW_STREAM".to_string(),
            schema: ResultSchema {
                column_count: columns.len() as i32,
                columns,
            },
            arrow_schema: None,
            total_chunk_count: None,
            total_row_count: None,
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
            is_native_metadata_result: None,
        }
    }

    fn column_info(name: &str, type_name: &str, type_text: &str, position: i32) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            type_text: type_text.to_string(),
            position,
            type_precision: None,
            type_scale: None,
            type_interval_type: None,
            type_json: None,
            nullable: None,
        }
    }

    const POINT_1_2_WKB: &[u8] = &[
        1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
    ];

    fn untagged_geo_values(row_srids: &[Option<i32>]) -> ArrayRef {
        let fields = Fields::from(vec![
            Field::new("srid", DataType::Int32, false),
            Field::new("wkb", DataType::Binary, false),
        ]);
        let mut srids = Int32Builder::new();
        let mut wkbs = BinaryBuilder::new();
        let mut validity = Vec::with_capacity(row_srids.len());
        for row_srid in row_srids {
            match row_srid {
                Some(srid) => {
                    srids.append_value(*srid);
                    wkbs.append_value(POINT_1_2_WKB);
                    validity.push(true);
                }
                None => {
                    // Non-null children carry placeholders masked by the null
                    // outer struct, matching Reyden's encoding.
                    srids.append_value(0);
                    wkbs.append_value([]);
                    validity.push(false);
                }
            }
        }
        Arc::new(StructArray::new(
            fields,
            vec![Arc::new(srids.finish()), Arc::new(wkbs.finish())],
            Some(NullBuffer::from(validity)),
        ))
    }

    #[tokio::test]
    async fn pass_through_when_no_manifest_and_json_off() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["hi"])) as _],
        )
        .unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let mut pp = ResultPostProcessor::new(inner, None, PostProcessOptions::default())
            .await
            .unwrap();
        // No transform should have been installed.
        assert!(pp.transform.is_none());
        let out = pp.next_batch().await.unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.schema().field(0).metadata().len(), 0);
    }

    #[tokio::test]
    async fn manifest_only_attaches_metadata_to_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["v"])) as _],
        )
        .unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![column_info("name", "VARIANT", "VARIANT", 0)]);

        let mut pp =
            ResultPostProcessor::new(inner, Some(&manifest), PostProcessOptions::default())
                .await
                .unwrap();
        assert!(pp.transform.is_some());

        let out = pp.next_batch().await.unwrap().unwrap();
        assert_eq!(
            out.schema()
                .field(0)
                .metadata()
                .get(metadata_keys::TYPE_NAME),
            Some(&"VARIANT".to_string())
        );
        assert_eq!(
            out.schema()
                .field(0)
                .metadata()
                .get(metadata_keys::SPARK_SQL_NAME),
            Some(&"VARIANT".to_string())
        );
    }

    #[tokio::test]
    async fn manifest_nullability_survives_type_conversions() {
        for nullable in [false, true] {
            let mut lists = ListBuilder::new(Int32Builder::new());
            lists.values().append_value(42);
            lists.append(true);
            let lists = lists.finish();
            let intervals = arrow_array::DurationMicrosecondArray::from(vec![1_000_000]);
            let schema = Arc::new(Schema::new(vec![
                Field::new("col_0", lists.data_type().clone(), !nullable),
                Field::new("col_1", intervals.data_type().clone(), !nullable),
            ]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(lists) as _, Arc::new(intervals) as _])
                    .unwrap();
            let inner = Box::new(MockReader::new(vec![batch.clone(), batch]));
            let mut columns = vec![
                column_info("values", "ARRAY", "ARRAY<INT>", 0),
                column_info("duration", "INTERVAL", "INTERVAL DAY TO SECOND", 1),
            ];
            for column in &mut columns {
                column.nullable = Some(nullable);
            }
            let manifest = make_manifest(columns);
            let options = PostProcessOptions {
                complex_types_as_json: true,
                intervals_as_string: true,
                ..Default::default()
            };
            let mut processor = ResultPostProcessor::new(inner, Some(&manifest), options)
                .await
                .unwrap();
            let schema = processor.schema().await.unwrap();
            for field in schema.fields() {
                assert_eq!(field.data_type(), &DataType::Utf8);
                assert_eq!(field.is_nullable(), nullable);
            }
            let batch = processor.next_batch().await.unwrap().unwrap();
            assert_eq!(batch.schema(), schema);
            let TryNextBatch::Ready(batch) = processor.try_next_batch().unwrap() else {
                panic!("expected a buffered batch");
            };
            assert_eq!(batch.schema(), schema);
            assert!(processor.next_batch().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn manifest_overlays_real_names_onto_positional_columns_and_batches() {
        // Real names must appear on both the advertised schema and the
        // yielded batch (rebuilt onto the renamed schema), not the col_N
        // names from the Arrow payload.
        let positional = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Int32, true),
            Field::new("col_1", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            positional.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as _,
                Arc::new(StringArray::from(vec!["abc"])) as _,
            ],
        )
        .unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![
            column_info("id", "INT", "INT", 0),
            column_info("varchar_column", "STRING", "STRING", 1),
        ]);

        let mut pp =
            ResultPostProcessor::new(inner, Some(&manifest), PostProcessOptions::default())
                .await
                .unwrap();

        // Advertised schema carries the manifest names.
        let advertised = pp.schema().await.unwrap();
        assert_eq!(advertised.field(0).name(), "id");
        assert_eq!(advertised.field(1).name(), "varchar_column");

        // The actual yielded batch carries them too (not the col_N names),
        // and the data is untouched.
        let out = pp.next_batch().await.unwrap().unwrap();
        assert_eq!(out.schema().field(0).name(), "id");
        assert_eq!(out.schema().field(1).name(), "varchar_column");
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            out.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "abc"
        );
    }

    #[tokio::test]
    async fn untyped_null_column_rewritten_to_utf8() {
        // Regression for issue #203 (DATATYPE-041): an untyped NULL
        // (`SELECT NULL` / VOID) arrives as an Arrow `Null`-typed column;
        // the post-processor must surface the server-reported STRING type
        // as an all-null `Utf8` column — always on, no option needed.
        let schema = Arc::new(Schema::new(vec![
            Field::new("untyped_null", DataType::Null, true),
            Field::new("typed_null_string", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::NullArray::new(1)) as _,
                Arc::new(StringArray::from(vec![None::<&str>])) as _,
            ],
        )
        .unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        // Default options (no complex/interval rewrite): the Null→Utf8
        // coercion is unconditional.
        let mut pp = ResultPostProcessor::new(inner, None, PostProcessOptions::default())
            .await
            .unwrap();

        // Both the advertised schema and the yielded batch report Utf8 for
        // the untyped-NULL column, identical to the CAST(NULL AS STRING)
        // sibling.
        let advertised = pp.schema().await.unwrap();
        assert_eq!(advertised.field(0).data_type(), &DataType::Utf8);
        assert_eq!(advertised.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            advertised.field(0).data_type(),
            advertised.field(1).data_type()
        );

        let out = pp.next_batch().await.unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(out.num_rows(), 1);
        // The value is still NULL in the rewritten column.
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("untyped_null rewritten to StringArray");
        assert_eq!(col.len(), 1);
        assert!(col.is_null(0));
    }

    #[tokio::test]
    async fn untyped_null_carries_manifest_metadata_and_real_name() {
        // The Null→Utf8 rewrite must compose with manifest augmentation:
        // the rewritten field keeps the manifest's real name and the
        // server-reported `databricks.type_name`/`type_text` (NULL/VOID).
        let positional = Arc::new(Schema::new(vec![Field::new("col_0", DataType::Null, true)]));
        let batch = RecordBatch::try_new(
            positional,
            vec![Arc::new(arrow_array::NullArray::new(2)) as _],
        )
        .unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![column_info("untyped_null", "NULL", "VOID", 0)]);

        let mut pp =
            ResultPostProcessor::new(inner, Some(&manifest), PostProcessOptions::default())
                .await
                .unwrap();

        let out = pp.next_batch().await.unwrap().unwrap();
        let field = out.schema().field(0).clone();
        assert_eq!(field.name(), "untyped_null");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_NAME),
            Some(&"NULL".to_string())
        );
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_TEXT),
            Some(&"VOID".to_string())
        );
        assert_eq!(out.num_rows(), 2);
        assert!(out.column(0).is_null(0) && out.column(0).is_null(1));
    }

    #[tokio::test]
    async fn json_path_rewrites_list_column_to_utf8() {
        // Build a one-row batch with a list<int32> column.
        let mut list_b = ListBuilder::new(arrow_array::builder::Int32Builder::new());
        list_b.values().append_value(1);
        list_b.values().append_value(2);
        list_b.values().append_value(3);
        list_b.append(true);
        let list_arr = list_b.finish();

        let list_field = Field::new(
            "vals",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        );
        let other_field = Field::new("n", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![list_field, other_field]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(list_arr) as _,
                Arc::new(Int32Array::from(vec![7])) as _,
            ],
        )
        .unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let opts = PostProcessOptions {
            complex_types_as_json: true,
            ..Default::default()
        };
        let mut pp = ResultPostProcessor::new(inner, None, opts).await.unwrap();

        // Output schema should expose Utf8 for the complex column.
        let out_schema = pp.schema().await.unwrap();
        assert_eq!(out_schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(out_schema.field(1).data_type(), &DataType::Int32);

        let out_batch = pp.next_batch().await.unwrap().unwrap();
        let arr = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "[1,2,3]");
        // Primitive column reprojected unchanged.
        let other = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(other.value(0), 7);
    }

    #[tokio::test]
    async fn json_path_handles_null_list_value() {
        let mut list_b = ListBuilder::new(arrow_array::builder::Int32Builder::new());
        list_b.append(false); // null
        list_b.values().append_value(1);
        list_b.append(true);
        let list_arr = list_b.finish();

        let list_field = Field::new(
            "vals",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        );
        let schema = Arc::new(Schema::new(vec![list_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_arr) as _]).unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let opts = PostProcessOptions {
            complex_types_as_json: true,
            ..Default::default()
        };
        let mut pp = ResultPostProcessor::new(inner, None, opts).await.unwrap();
        let out_batch = pp.next_batch().await.unwrap().unwrap();
        let arr = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(arr.is_null(0));
        assert_eq!(arr.value(1), "[1]");
    }

    #[tokio::test]
    async fn json_path_struct_renders_fields_in_order() {
        let fields: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]
        .into();
        let struct_arr = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["foo"])) as _,
                Arc::new(Int32Array::from(vec![1])) as _,
            ],
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(fields),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_arr) as _]).unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let opts = PostProcessOptions {
            complex_types_as_json: true,
            ..Default::default()
        };
        let mut pp = ResultPostProcessor::new(inner, None, opts).await.unwrap();
        let out = pp.next_batch().await.unwrap().unwrap();
        let arr = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "{\"a\":\"foo\",\"b\":1}");
    }

    #[tokio::test]
    async fn json_path_map_renders_object() {
        let mut b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        b.keys().append_value("k");
        b.values().append_value("v");
        b.append(true).unwrap();
        let map_arr = b.finish();
        let map_dt = map_arr.data_type().clone();

        let schema = Arc::new(Schema::new(vec![Field::new("m", map_dt, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(map_arr) as _]).unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let opts = PostProcessOptions {
            complex_types_as_json: true,
            ..Default::default()
        };
        let mut pp = ResultPostProcessor::new(inner, None, opts).await.unwrap();
        let out = pp.next_batch().await.unwrap().unwrap();
        let arr = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "{\"k\":\"v\"}");
    }

    #[tokio::test]
    async fn json_off_leaves_complex_columns_intact() {
        let mut list_b = ListBuilder::new(arrow_array::builder::Int32Builder::new());
        list_b.values().append_value(1);
        list_b.append(true);
        let list_arr = list_b.finish();
        let list_dt = list_arr.data_type().clone();

        let schema = Arc::new(Schema::new(vec![Field::new("vals", list_dt.clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_arr) as _]).unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let mut pp = ResultPostProcessor::new(inner, None, PostProcessOptions::default())
            .await
            .unwrap();
        assert_eq!(pp.schema().await.unwrap().field(0).data_type(), &list_dt);
        let out = pp.next_batch().await.unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &list_dt);
    }

    #[tokio::test]
    async fn geospatial_string_mode_converts_geometry_geography_any_and_nulls() {
        let geometry = untagged_geo_values(&[Some(3857), None]);
        let geography = untagged_geo_values(&[Some(4326), None]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_0", geometry.data_type().clone(), true),
            Field::new("col_1", geography.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![geometry, geography]).unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![
            column_info("geom", "GEOMETRY", "GEOMETRY(ANY)", 0),
            column_info("geog", "GEOGRAPHY", "GEOGRAPHY(4326)", 1),
        ]);

        let options = PostProcessOptions {
            geospatial_result_representation: GeospatialResultRepresentation::String,
            ..Default::default()
        };
        let mut pp = ResultPostProcessor::new(inner, Some(&manifest), options)
            .await
            .unwrap();
        assert_eq!(
            pp.schema().await.unwrap().field(0).data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            pp.schema().await.unwrap().field(1).data_type(),
            &DataType::Utf8
        );

        let out = pp.next_batch().await.unwrap().unwrap();
        let geometry = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let geography = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(geometry.value(0), "SRID=3857;POINT(1 2)");
        assert_eq!(geography.value(0), "SRID=4326;POINT(1 2)");
        assert!(geometry.is_null(1));
        assert!(geography.is_null(1));
        assert_eq!(
            out.schema()
                .field(0)
                .metadata()
                .get(metadata_keys::TYPE_TEXT)
                .map(String::as_str),
            Some("GEOMETRY(ANY)")
        );
    }

    #[tokio::test]
    async fn geospatial_binary_mode_attaches_tags_and_reuses_child_arrays() {
        let geometry = untagged_geo_values(&[Some(4326)]);
        let original = geometry
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .columns()
            .to_vec();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            geometry.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![geometry]).unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![column_info("geom", "GEOMETRY", "GEOMETRY(ANY)", 0)]);
        let options = PostProcessOptions {
            geospatial_result_representation: GeospatialResultRepresentation::Binary,
            ..Default::default()
        };

        let mut pp = ResultPostProcessor::new(inner, Some(&manifest), options)
            .await
            .unwrap();
        let out = pp.next_batch().await.unwrap().unwrap();
        let values = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(Arc::ptr_eq(values.column(0), &original[0]));
        assert!(Arc::ptr_eq(values.column(1), &original[1]));
        let schema = out.schema();
        let DataType::Struct(fields) = schema.field(0).data_type() else {
            panic!("binary geometry must remain a struct")
        };
        assert_eq!(
            fields[1].metadata().get("geometry").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            fields[1].metadata().get("srid").map(String::as_str),
            Some("-1")
        );
    }

    #[tokio::test]
    async fn nested_geo_is_converted_before_complex_json() {
        let list_values = untagged_geo_values(&[Some(4326), None]);
        let list_item = Arc::new(Field::new("item", list_values.data_type().clone(), true));
        let list = ListArray::new(
            list_item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
            list_values,
            None,
        );

        let struct_values = untagged_geo_values(&[Some(0)]);
        let struct_fields = Fields::from(vec![Field::new(
            "g",
            struct_values.data_type().clone(),
            true,
        )]);
        let struct_array = StructArray::new(struct_fields, vec![struct_values], None);

        let map_values = untagged_geo_values(&[Some(4326)]);
        let entry_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", map_values.data_type().clone(), true),
        ]);
        let entries = StructArray::new(
            entry_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["k"])), map_values],
            None,
        );
        let map = MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
            entries,
            None,
            false,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("col_0", list.data_type().clone(), true),
            Field::new("col_1", struct_array.data_type().clone(), true),
            Field::new("col_2", map.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(list), Arc::new(struct_array), Arc::new(map)],
        )
        .unwrap();
        let inner: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![
            column_info("arr", "ARRAY", "ARRAY<GEOMETRY(ANY)>", 0),
            column_info("s", "STRUCT", "STRUCT<g:GEOMETRY(0)>", 1),
            column_info("m", "MAP", "MAP<STRING,GEOGRAPHY(4326)>", 2),
        ]);
        let options = PostProcessOptions {
            complex_types_as_json: true,
            geospatial_result_representation: GeospatialResultRepresentation::String,
            ..Default::default()
        };

        let mut pp = ResultPostProcessor::new(inner, Some(&manifest), options)
            .await
            .unwrap();
        let out = pp.next_batch().await.unwrap().unwrap();
        let value = |index| {
            out.column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
        };
        assert_eq!(value(0), "[\"SRID=4326;POINT(1 2)\",null]");
        assert_eq!(value(1), "{\"g\":\"POINT(1 2)\"}");
        assert_eq!(value(2), "{\"k\":\"SRID=4326;POINT(1 2)\"}");
    }

    #[tokio::test]
    async fn manifest_not_null_geo_text_is_converted_without_changing_siblings() {
        let blob: ArrayRef = Arc::new(BinaryArray::from(vec![&b"\x00\x80\xff"[..]; 2]));
        let timestamp: ArrayRef = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![1234567_i64, 7654321])
                .with_timezone("Etc/UTC"),
        );
        let fields = Fields::from(vec![
            Field::new("geom", DataType::Utf8, false),
            Field::new("blob", DataType::Binary, false),
            Field::new("ts", timestamp.data_type().clone(), false),
        ]);
        let values = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "POINT(1 2)",
                    "SRID=3857;POINT(1 2)",
                ])),
                blob.clone(),
                timestamp.clone(),
            ],
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            DataType::Struct(fields),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
        let manifest = make_manifest(vec![column_info(
            "value",
            "STRUCT",
            "STRUCT<geom: GEOMETRY(ANY) NOT NULL, blob: BINARY NOT NULL, ts: TIMESTAMP NOT NULL>",
            0,
        )]);
        let mut pp = ResultPostProcessor::new(
            Box::new(MockReader::new(vec![batch])),
            Some(&manifest),
            PostProcessOptions::default(),
        )
        .await
        .unwrap();
        let advertised = pp.schema().await.unwrap();
        let out = pp.next_batch().await.unwrap().unwrap();
        assert_eq!(out.schema(), advertised);
        let values = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let geo = values
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let srids = geo.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(srids.values(), &[0, 3857]);
        let wkb = geo
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(wkb.value(0), POINT_1_2_WKB);
        assert_eq!(wkb.value(1), POINT_1_2_WKB);
        assert!(Arc::ptr_eq(values.column(1), &blob));
        assert!(Arc::ptr_eq(values.column(2), &timestamp));
        assert!(!values.fields()[0].is_nullable());
        assert!(pp.next_batch().await.unwrap().is_none());
    }
}
