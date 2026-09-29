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

//! Reconciliation of Reyden GEOMETRY / GEOGRAPHY Arrow results.
//!
//! Reyden encodes each value as
//! `struct<srid:int32 not null,wkb:binary not null>`. Reyder's structured
//! manifest retains the logical `GEOMETRY(...)` / `GEOGRAPHY(...)` type and is
//! the source of truth for interpreting result attachments. An attachment can
//! contain the same struct without the canonical Arrow tags, so this module
//! reconstructs them from the logical schema without copying child buffers.

use crate::kernel_error::{Error, Result};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, LargeListViewArray, ListArray,
    ListViewArray, MapArray, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeoFamily {
    Geometry,
    Geography,
}

impl GeoFamily {
    fn manifest_name(self) -> &'static str {
        match self {
            Self::Geometry => "GEOMETRY",
            Self::Geography => "GEOGRAPHY",
        }
    }
}

/// Schema decision for a top-level result column.
#[derive(Debug)]
pub(super) struct GeoPlan {
    /// Type after attaching canonical geo metadata recursively.
    pub(super) output_type: DataType,
    /// Whether the values need local tag attachment.
    pub(super) needs_rewrite: bool,
    /// Whether the column itself is a logical geo value. This distinguishes a
    /// native geo struct from an ordinary complex column for JSON rendering.
    pub(super) is_top_level_geo: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GeoTag {
    family: GeoFamily,
    srid: i32,
}

#[derive(Debug)]
struct TypePlan {
    output_type: DataType,
    contains_geo: bool,
    is_root_geo: bool,
}

/// Reconcile an attachment field with its structured-manifest logical field.
///
/// `logical_field` is reconstructed from the structured SEA manifest. It is
/// the authority for deciding whether an otherwise ordinary-looking
/// `struct<srid,wkb>` is geospatial. A plain user struct remains a struct unless
/// the logical schema declares it geo. Canonical attachment tags are used as a
/// fallback only when no logical field is available.
pub(super) fn plan(field: &Field, logical_field: Option<&Field>) -> Result<Option<GeoPlan>> {
    let type_plan = plan_data_type(
        field.data_type(),
        logical_field.map(Field::data_type),
        field.name(),
    )?;
    if !type_plan.contains_geo {
        return Ok(None);
    }
    Ok(Some(GeoPlan {
        needs_rewrite: &type_plan.output_type != field.data_type(),
        output_type: type_plan.output_type,
        is_top_level_geo: type_plan.is_root_geo,
    }))
}

/// Recursively reconcile geo leaves in `array` with `output_type`.
///
/// Only container array objects whose field metadata must be tagged are
/// rebuilt; value buffers and child arrays remain shared.
pub(super) fn rewrite(
    array: &ArrayRef,
    output_type: &DataType,
    column_name: &str,
) -> Result<ArrayRef> {
    rewrite_array(array, output_type, column_name)
}

fn plan_data_type(physical: &DataType, logical: Option<&DataType>, path: &str) -> Result<TypePlan> {
    let logical_tag = logical.map(native_geo_tag).transpose()?.flatten();
    let physical_tag = native_geo_tag(physical)?;

    if let Some(expected) = logical_tag {
        // DBR/legacy SEA paths serialize geo values as text even though the
        // structured manifest still declares GEOMETRY / GEOGRAPHY. Preserve
        // that already-public representation. Native Reyden is identified by
        // its `struct<srid,wkb>` attachment and is reconciled below.
        if matches!(physical, DataType::Utf8 | DataType::LargeUtf8) {
            return Ok(TypePlan {
                output_type: physical.clone(),
                contains_geo: false,
                is_root_geo: false,
            });
        }
        validate_geo_source(physical, expected, path)?;
        // The logical schema is canonical and contains the family and
        // type-level SRID tags that can be missing from the payload.
        let output_type = logical.expect("logical_tag implies a logical type").clone();
        return Ok(TypePlan {
            output_type,
            contains_geo: true,
            is_root_geo: true,
        });
    }

    if let Some(actual) = physical_tag {
        if let Some(logical) = logical {
            return Err(Error::data_loss().message(format!(
                "geospatial value '{path}' is tagged {}({}) in the attachment but its logical manifest type is {logical:?}",
                actual.family.manifest_name(),
                actual.srid,
            )));
        }
        return Ok(TypePlan {
            output_type: physical.clone(),
            contains_geo: true,
            is_root_geo: true,
        });
    }

    match physical {
        DataType::Struct(fields) => {
            let logical_fields = match logical {
                Some(DataType::Struct(fields)) => Some(fields),
                Some(other) if contains_tagged_geo(other)? => {
                    return logical_shape_mismatch(path, physical, other);
                }
                _ => None,
            };
            if let (Some(logical_fields), Some(logical)) = (logical_fields, logical) {
                if logical_fields.len() != fields.len() && contains_tagged_geo(logical)? {
                    return logical_shape_mismatch(path, physical, logical);
                }
            }

            let mut contains_geo = false;
            let mut output_fields = Vec::with_capacity(fields.len());
            for (index, field) in fields.iter().enumerate() {
                let logical_field = logical_fields.and_then(|fields| fields.get(index));
                let child_path = format!("{path}.{}", field.name());
                let child = plan_data_type(
                    field.data_type(),
                    logical_field.map(|field| field.data_type()),
                    &child_path,
                )?;
                contains_geo |= child.contains_geo;
                output_fields.push(if &child.output_type == field.data_type() {
                    field.clone()
                } else {
                    Arc::new(field.as_ref().clone().with_data_type(child.output_type))
                });
            }
            Ok(TypePlan {
                output_type: DataType::Struct(Fields::from(output_fields)),
                contains_geo,
                is_root_geo: false,
            })
        }
        DataType::List(field) => plan_nested_field(
            field,
            logical.and_then(|logical| match logical {
                DataType::List(field) => Some(field.as_ref()),
                _ => None,
            }),
            path,
            DataType::List,
            logical,
        ),
        DataType::LargeList(field) => plan_nested_field(
            field,
            logical.and_then(|logical| match logical {
                DataType::LargeList(field) => Some(field.as_ref()),
                _ => None,
            }),
            path,
            DataType::LargeList,
            logical,
        ),
        DataType::ListView(field) => plan_nested_field(
            field,
            logical.and_then(|logical| match logical {
                DataType::ListView(field) => Some(field.as_ref()),
                _ => None,
            }),
            path,
            DataType::ListView,
            logical,
        ),
        DataType::LargeListView(field) => plan_nested_field(
            field,
            logical.and_then(|logical| match logical {
                DataType::LargeListView(field) => Some(field.as_ref()),
                _ => None,
            }),
            path,
            DataType::LargeListView,
            logical,
        ),
        DataType::FixedSizeList(field, size) => {
            let logical_field = logical.and_then(|logical| match logical {
                DataType::FixedSizeList(field, logical_size) if logical_size == size => {
                    Some(field.as_ref())
                }
                _ => None,
            });
            plan_nested_field(
                field,
                logical_field,
                path,
                |field| DataType::FixedSizeList(field, *size),
                logical,
            )
        }
        DataType::Map(field, ordered) => {
            let logical_field = logical.and_then(|logical| match logical {
                DataType::Map(field, logical_ordered) if logical_ordered == ordered => {
                    Some(field.as_ref())
                }
                _ => None,
            });
            plan_nested_field(
                field,
                logical_field,
                path,
                |field| DataType::Map(field, *ordered),
                logical,
            )
        }
        _ => {
            if let Some(logical) = logical {
                if contains_tagged_geo(logical)? {
                    return logical_shape_mismatch(path, physical, logical);
                }
            }
            if contains_tagged_geo(physical)? {
                return unsupported_container(physical);
            }
            Ok(TypePlan {
                output_type: physical.clone(),
                contains_geo: false,
                is_root_geo: false,
            })
        }
    }
}

fn plan_nested_field(
    physical_field: &FieldRef,
    logical_field: Option<&Field>,
    path: &str,
    constructor: impl FnOnce(FieldRef) -> DataType,
    logical_container: Option<&DataType>,
) -> Result<TypePlan> {
    if let Some(logical_container) = logical_container {
        if logical_field.is_none() && contains_tagged_geo(logical_container)? {
            return logical_shape_mismatch(path, physical_field.data_type(), logical_container);
        }
    }
    let child = plan_data_type(
        physical_field.data_type(),
        logical_field.map(Field::data_type),
        path,
    )?;
    let output_field = if &child.output_type == physical_field.data_type() {
        physical_field.clone()
    } else {
        Arc::new(
            physical_field
                .as_ref()
                .clone()
                .with_data_type(child.output_type),
        )
    };
    Ok(TypePlan {
        output_type: constructor(output_field),
        contains_geo: child.contains_geo,
        is_root_geo: false,
    })
}

fn logical_shape_mismatch<T>(path: &str, physical: &DataType, logical: &DataType) -> Result<T> {
    Err(Error::data_loss().message(format!(
        "geospatial value '{path}' has attachment type {physical:?}, which does not match logical type {logical:?}"
    )))
}

fn contains_tagged_geo(data_type: &DataType) -> Result<bool> {
    if native_geo_tag(data_type)?.is_some() {
        return Ok(true);
    }
    match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                if contains_tagged_geo(field.data_type())? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                if contains_tagged_geo(field.data_type())? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains_tagged_geo(field.data_type()),
        DataType::Dictionary(_, value) => contains_tagged_geo(value),
        _ => Ok(false),
    }
}

fn unsupported_container<T>(data_type: &DataType) -> Result<T> {
    Err(Error::not_implemented().message(format!(
        "geospatial metadata reconciliation is not implemented for Arrow container {data_type:?}"
    )))
}

fn validate_geo_source(data_type: &DataType, expected: GeoTag, path: &str) -> Result<()> {
    if !is_geo_struct_shape(data_type) {
        return Err(Error::data_loss().message(format!(
            "geospatial value '{path}' is declared {} but its attachment type is {data_type:?}; expected struct<srid:int32,wkb:binary>",
            expected.family.manifest_name(),
        )));
    }
    if let Some(actual) = native_geo_tag(data_type)? {
        if actual != expected {
            return Err(Error::data_loss().message(format!(
                "geospatial value '{path}' is declared {}({}) but its attachment is tagged {}({})",
                expected.family.manifest_name(),
                expected.srid,
                actual.family.manifest_name(),
                actual.srid,
            )));
        }
    }
    Ok(())
}

fn is_geo_struct_shape(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    fields.len() == 2
        && fields[0].name() == "srid"
        && fields[0].data_type() == &DataType::Int32
        && fields[1].name() == "wkb"
        && fields[1].data_type() == &DataType::Binary
}

/// Recognize the canonical geo tag when present. An exact untagged
/// `struct<srid,wkb>` returns `None`; any partial or conflicting tag is an
/// invalid wire schema.
fn native_geo_tag(data_type: &DataType) -> Result<Option<GeoTag>> {
    if !is_geo_struct_shape(data_type) {
        return Ok(None);
    }
    let DataType::Struct(fields) = data_type else {
        unreachable!("shape check guarantees a struct")
    };

    let metadata = fields[1].metadata();
    let geometry = metadata.get("geometry").map(String::as_str) == Some("true");
    let geography = metadata.get("geography").map(String::as_str) == Some("true");
    let has_geo_metadata = metadata.contains_key("geometry")
        || metadata.contains_key("geography")
        || metadata.contains_key("srid");
    if !has_geo_metadata {
        return Ok(None);
    }
    if geometry == geography {
        return Err(Error::data_loss().message(
            "malformed geospatial Arrow tag: expected exactly one of geometry=true or geography=true",
        ));
    }
    let srid = metadata
        .get("srid")
        .ok_or_else(|| {
            Error::data_loss().message("malformed geospatial Arrow tag: missing type-level srid")
        })?
        .parse::<i32>()
        .map_err(|error| {
            Error::data_loss().message(format!(
                "malformed geospatial Arrow tag: invalid type-level srid: {error}"
            ))
        })?;

    Ok(Some(GeoTag {
        family: if geometry {
            GeoFamily::Geometry
        } else {
            GeoFamily::Geography
        },
        srid,
    }))
}

fn rewrite_array(array: &ArrayRef, output_type: &DataType, path: &str) -> Result<ArrayRef> {
    if let Some(output_tag) = native_geo_tag(output_type)? {
        validate_geo_source(array.data_type(), output_tag, path)?;
        if array.data_type() == output_type {
            return Ok(array.clone());
        }
        let values = downcast::<StructArray>(array, path, "StructArray")?;
        let DataType::Struct(output_fields) = output_type else {
            unreachable!("a geo tag is only valid on a struct")
        };
        let output = StructArray::try_new(
            output_fields.clone(),
            values.columns().to_vec(),
            values.nulls().cloned(),
        )
        .map_err(|error| rebuild_error(path, error))?;
        return Ok(Arc::new(output));
    }

    match (array.data_type(), output_type) {
        (DataType::Struct(source_fields), DataType::Struct(output_fields)) => {
            let values = downcast::<StructArray>(array, path, "StructArray")?;
            let columns = values
                .columns()
                .iter()
                .zip(source_fields.iter())
                .zip(output_fields.iter())
                .map(|((column, source), output)| {
                    rewrite_array(
                        column,
                        output.data_type(),
                        &format!("{path}.{}", source.name()),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let output =
                StructArray::try_new(output_fields.clone(), columns, values.nulls().cloned())
                    .map_err(|error| rebuild_error(path, error))?;
            Ok(Arc::new(output))
        }
        (DataType::List(_), DataType::List(output_field)) => {
            let values = downcast::<ListArray>(array, path, "ListArray")?;
            let child = rewrite_array(values.values(), output_field.data_type(), path)?;
            let output = ListArray::try_new(
                output_field.clone(),
                values.offsets().clone(),
                child,
                values.nulls().cloned(),
            )
            .map_err(|error| rebuild_error(path, error))?;
            Ok(Arc::new(output))
        }
        (DataType::LargeList(_), DataType::LargeList(output_field)) => {
            let values = downcast::<LargeListArray>(array, path, "LargeListArray")?;
            let child = rewrite_array(values.values(), output_field.data_type(), path)?;
            let output = LargeListArray::try_new(
                output_field.clone(),
                values.offsets().clone(),
                child,
                values.nulls().cloned(),
            )
            .map_err(|error| rebuild_error(path, error))?;
            Ok(Arc::new(output))
        }
        (DataType::ListView(_), DataType::ListView(output_field)) => {
            let values = downcast::<ListViewArray>(array, path, "ListViewArray")?;
            let child = rewrite_array(values.values(), output_field.data_type(), path)?;
            let output = ListViewArray::try_new(
                output_field.clone(),
                values.offsets().clone(),
                values.sizes().clone(),
                child,
                values.nulls().cloned(),
            )
            .map_err(|error| rebuild_error(path, error))?;
            Ok(Arc::new(output))
        }
        (DataType::LargeListView(_), DataType::LargeListView(output_field)) => {
            let values = downcast::<LargeListViewArray>(array, path, "LargeListViewArray")?;
            let child = rewrite_array(values.values(), output_field.data_type(), path)?;
            let output = LargeListViewArray::try_new(
                output_field.clone(),
                values.offsets().clone(),
                values.sizes().clone(),
                child,
                values.nulls().cloned(),
            )
            .map_err(|error| rebuild_error(path, error))?;
            Ok(Arc::new(output))
        }
        (
            DataType::FixedSizeList(_, source_size),
            DataType::FixedSizeList(output_field, output_size),
        ) if source_size == output_size => {
            let values = downcast::<FixedSizeListArray>(array, path, "FixedSizeListArray")?;
            let child = rewrite_array(values.values(), output_field.data_type(), path)?;
            let output = FixedSizeListArray::try_new(
                output_field.clone(),
                *output_size,
                child,
                values.nulls().cloned(),
            )
            .map_err(|error| rebuild_error(path, error))?;
            Ok(Arc::new(output))
        }
        (DataType::Map(_, source_ordered), DataType::Map(output_field, output_ordered))
            if source_ordered == output_ordered =>
        {
            let values = downcast::<MapArray>(array, path, "MapArray")?;
            let entries: ArrayRef = Arc::new(values.entries().clone());
            let entries = rewrite_array(&entries, output_field.data_type(), path)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    Error::internal().message(format!(
                        "geospatial map rewrite for '{path}' did not produce StructArray entries"
                    ))
                })?
                .clone();
            let output = MapArray::try_new(
                output_field.clone(),
                values.offsets().clone(),
                entries,
                values.nulls().cloned(),
                *output_ordered,
            )
            .map_err(|error| rebuild_error(path, error))?;
            Ok(Arc::new(output))
        }
        (source, output) if source == output => Ok(array.clone()),
        (source, output) => Err(Error::internal().message(format!(
            "geospatial rewrite for '{path}' cannot map Arrow type {source:?} to {output:?}"
        ))),
    }
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, path: &str, expected: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::data_loss().message(format!(
            "geospatial value '{path}' advertised {expected} but supplied {:?}",
            array.data_type()
        ))
    })
}

fn rebuild_error(path: &str, error: arrow_schema::ArrowError) -> Error {
    Error::data_loss().message(format!(
        "failed to rebuild Arrow container '{path}' after geospatial reconciliation: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{BinaryBuilder, Int32Builder};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use std::collections::HashMap;

    const POINT_1_2_WKB: &[u8] = &[
        1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
    ];

    fn geo_type(family: GeoFamily, type_srid: i32) -> DataType {
        let family_key = match family {
            GeoFamily::Geometry => "geometry",
            GeoFamily::Geography => "geography",
        };
        DataType::Struct(Fields::from(vec![
            Field::new("srid", DataType::Int32, false),
            Field::new("wkb", DataType::Binary, false).with_metadata(HashMap::from([
                (family_key.to_string(), "true".to_string()),
                ("srid".to_string(), type_srid.to_string()),
            ])),
        ]))
    }

    fn plain_geo_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("srid", DataType::Int32, false),
            Field::new("wkb", DataType::Binary, false),
        ]))
    }

    fn nullable_plain_geo_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("srid", DataType::Int32, true),
            Field::new("wkb", DataType::Binary, true),
        ]))
    }

    fn geo_array(family: GeoFamily, type_srid: i32, row_srid: i32) -> ArrayRef {
        let DataType::Struct(fields) = geo_type(family, type_srid) else {
            unreachable!()
        };
        let mut srids = Int32Builder::new();
        srids.append_value(row_srid);
        srids.append_value(0);
        let mut wkbs = BinaryBuilder::new();
        wkbs.append_value(POINT_1_2_WKB);
        wkbs.append_value([]);
        Arc::new(StructArray::new(
            fields,
            vec![Arc::new(srids.finish()), Arc::new(wkbs.finish())],
            Some(NullBuffer::from(vec![true, false])),
        ))
    }

    #[test]
    fn recognizes_only_canonical_tagged_structs() {
        assert_eq!(
            native_geo_tag(&geo_type(GeoFamily::Geometry, -1))
                .unwrap()
                .map(|tag| tag.family),
            Some(GeoFamily::Geometry)
        );
        assert_eq!(
            native_geo_tag(&geo_type(GeoFamily::Geography, 4326))
                .unwrap()
                .map(|tag| tag.family),
            Some(GeoFamily::Geography)
        );

        let plain = plain_geo_type();
        assert_eq!(native_geo_tag(&plain).unwrap(), None);
    }

    #[test]
    fn logical_geo_identifies_an_untagged_payload_and_validates_existing_tags() {
        let physical = Field::new("g", plain_geo_type(), true);
        let logical = Field::new("g", geo_type(GeoFamily::Geometry, -1), true);

        let binary = plan(&physical, Some(&logical)).unwrap().unwrap();
        assert!(binary.needs_rewrite);
        assert_eq!(binary.output_type, logical.data_type().clone());

        let already_tagged = Field::new("g", geo_type(GeoFamily::Geometry, -1), true);
        let binary = plan(&already_tagged, Some(&logical)).unwrap().unwrap();
        assert!(!binary.needs_rewrite);
        assert_eq!(binary.output_type, logical.data_type().clone());

        let fallback = plan(&already_tagged, None).unwrap().unwrap();
        assert!(!fallback.needs_rewrite);
        assert_eq!(fallback.output_type, already_tagged.data_type().clone());

        let wrong_family = Field::new("g", geo_type(GeoFamily::Geography, -1), true);
        let error = plan(&wrong_family, Some(&logical)).unwrap_err();
        assert!(error.message.contains("tagged GEOGRAPHY"));

        let logical_plain = Field::new("g", plain_geo_type(), true);
        let error = plan(&already_tagged, Some(&logical_plain)).unwrap_err();
        assert!(error.message.contains("logical manifest type"));

        let bad_shape = Field::new("g", DataType::Utf8, true);
        assert!(plan(&bad_shape, Some(&logical)).unwrap().is_none());

        assert!(plan(&physical, None).unwrap().is_none());
    }

    #[test]
    fn binary_rewrite_attaches_logical_tags_without_copying_children() {
        let tagged = geo_array(GeoFamily::Geometry, -1, 4326);
        let tagged = tagged.as_any().downcast_ref::<StructArray>().unwrap();
        let DataType::Struct(plain_fields) = nullable_plain_geo_type() else {
            unreachable!()
        };
        let plain: ArrayRef = Arc::new(StructArray::new(
            plain_fields,
            tagged.columns().to_vec(),
            tagged.nulls().cloned(),
        ));
        let output_type = geo_type(GeoFamily::Geometry, -1);
        let output = rewrite(&plain, &output_type, "g").unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(Arc::ptr_eq(output.column(0), tagged.column(0)));
        assert!(Arc::ptr_eq(output.column(1), tagged.column(1)));
        assert_eq!(output.data_type(), &output_type);

        let DataType::Struct(output_fields) = output.data_type() else {
            unreachable!()
        };
        assert!(!output_fields[0].is_nullable());
        assert!(!output_fields[1].is_nullable());
    }

    #[test]
    fn nested_geo_rewrite_attaches_tags_and_preserves_buffers_and_nulls() {
        let tagged = geo_array(GeoFamily::Geometry, -1, 4326);
        let tagged = tagged.as_any().downcast_ref::<StructArray>().unwrap();
        let DataType::Struct(plain_fields) = plain_geo_type() else {
            unreachable!()
        };
        let plain_values: ArrayRef = Arc::new(StructArray::new(
            plain_fields,
            tagged.columns().to_vec(),
            tagged.nulls().cloned(),
        ));
        let list = ListArray::new(
            Arc::new(Field::new("item", plain_geo_type(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
            plain_values,
            None,
        );
        let physical = Field::new("values", list.data_type().clone(), true);
        let logical_type = DataType::List(Arc::new(Field::new(
            "item",
            geo_type(GeoFamily::Geometry, -1),
            true,
        )));
        let logical = Field::new("values", logical_type, true);

        let plan = plan(&physical, Some(&logical)).unwrap().unwrap();
        assert!(plan.needs_rewrite);
        let output = rewrite(&(Arc::new(list) as ArrayRef), &plan.output_type, "values").unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        let values = output
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let DataType::Struct(fields) = values.data_type() else {
            panic!("nested geometry must remain a struct")
        };
        assert_eq!(
            fields[1].metadata().get("geometry").map(String::as_str),
            Some("true")
        );
        assert!(Arc::ptr_eq(values.column(0), tagged.column(0)));
        assert!(Arc::ptr_eq(values.column(1), tagged.column(1)));
        assert!(values.is_null(1));
    }
}
