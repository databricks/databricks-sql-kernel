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

mod json;

use crate::kernel_error::{Error, Result};
use crate::reader::{ResultReader, ResultReaderAdapter};
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

        // 2. JSON-stringification rewrite: identify complex columns
        //    in the augmented schema and turn them into nullable Utf8
        //    while preserving the Databricks metadata.
        let mut json_columns: Vec<usize> = Vec::new();
        let output_schema: SchemaRef = if options.complex_types_as_json {
            let mut new_fields: Vec<Field> = Vec::with_capacity(augmented_schema.fields().len());
            for (i, field) in augmented_schema.fields().iter().enumerate() {
                if json::is_complex_type(field.data_type()) {
                    json_columns.push(i);
                    let metadata = field.metadata().clone();
                    new_fields.push(
                        Field::new(field.name(), DataType::Utf8, true).with_metadata(metadata),
                    );
                } else {
                    new_fields.push(field.as_ref().clone());
                }
            }
            Arc::new(Schema::new_with_metadata(
                new_fields,
                augmented_schema.metadata().clone(),
            ))
        } else {
            augmented_schema.clone()
        };

        // 3. Decide whether we need a per-batch transform at all.
        //
        // Cases:
        //  - `inner_schema == output_schema` (Arc-eq or
        //    structurally identical): no work needed.
        //  - Output schema differs only in field metadata (manifest
        //    augmentation, no JSON rewrite): a single
        //    `RecordBatch::try_new` re-project onto the augmented
        //    schema.
        //  - JSON columns present: fused walk that re-projects
        //    primitives and emits new Utf8 arrays for complex
        //    columns.
        let needs_rebuild = !schemas_field_equal(&inner_schema, &output_schema);
        let transform: Option<BatchTransform> = if !needs_rebuild && json_columns.is_empty() {
            None
        } else if json_columns.is_empty() {
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
            // columns. Captures only what each closure invocation
            // actually needs.
            let target_schema = output_schema.clone();
            let augmented = augmented_schema.clone();
            let json_cols: Vec<usize> = json_columns;
            Some(Box::new(move |batch: &RecordBatch| {
                build_json_batch(batch, &augmented, &target_schema, &json_cols)
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
        match &self.transform {
            Some(t) => Ok(Some(t(&batch)?)),
            None => Ok(Some(batch)),
        }
    }
}

/// Compare schemas by their field sequence including per-field
/// metadata. Faster than serialising both sides; sufficient because
/// the augmenter only rewrites `Field::metadata` (and the JSON path
/// rewrites field type, which `Field::eq` already covers).
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

/// Build a `RecordBatch` whose primitive columns are reprojected
/// from `batch` and whose complex columns (indexed by `json_cols`)
/// are replaced with `Utf8` arrays of compact JSON text.
fn build_json_batch(
    batch: &RecordBatch,
    augmented: &SchemaRef,
    output: &SchemaRef,
    json_cols: &[usize],
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    let mut next_json_idx = 0;
    for (col_idx, column) in batch.columns().iter().enumerate() {
        let is_json = json_cols
            .get(next_json_idx)
            .copied()
            .is_some_and(|target| target == col_idx);
        if is_json {
            next_json_idx += 1;
            // Use the augmented field (carries databricks.* metadata)
            // so the per-row encoder can see VARIANT / GEOMETRY /
            // GEOGRAPHY hints if they ever apply to a complex column.
            // Guard against an upstream batch that has more columns
            // than the schema it advertised — `Schema::field` would
            // panic; we want a typed `Error::internal` instead.
            let field = augmented.fields().get(col_idx).ok_or_else(|| {
                Error::internal().message(format!(
                    "ResultPostProcessor: upstream batch has {} columns but advertised schema only has {}",
                    batch.num_columns(),
                    augmented.fields().len()
                ))
            })?;
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
            let mut scratch = String::new();
            for row in 0..num_rows {
                if column.is_null(row) {
                    builder.append_null();
                    continue;
                }
                scratch.clear();
                json::append_json(&mut scratch, column.as_ref(), row, field.as_ref())?;
                builder.append_value(&scratch);
            }
            columns.push(Arc::new(builder.finish()));
        } else {
            columns.push(column.clone());
        }
    }

    RecordBatch::try_new(output.clone(), columns).map_err(|e| {
        Error::internal().message(format!(
            "ResultPostProcessor: failed to assemble JSON-rewritten batch: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::metadata_keys;
    use crate::reader::test_utils::MockReader;
    use crate::reader::ResultReader;
    use crate::types::sea::{ColumnInfo, ResultSchema};
    use arrow_array::builder::{ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::{Array, Int32Array, StringArray, StructArray};
    use arrow_schema::Fields;

    fn make_manifest(columns: Vec<ColumnInfo>) -> ResultManifest {
        ResultManifest {
            format: "ARROW_STREAM".to_string(),
            schema: ResultSchema {
                column_count: columns.len() as i32,
                columns,
            },
            total_chunk_count: None,
            total_row_count: None,
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
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
        }
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
}
