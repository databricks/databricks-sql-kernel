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

//! Reconciliation of GEOMETRY / GEOGRAPHY Arrow results.
//!
//! Reyden encodes each value as `struct<srid:int32 not null,wkb:binary not
//! null>`, while the standard DBSQL path encodes it as WKT / EWKT in Arrow
//! `Utf8`. Reyder's structured manifest retains the logical `GEOMETRY(...)` /
//! `GEOGRAPHY(...)` type and is the source of truth for interpreting either
//! result attachment. This module recursively normalizes both physical inputs
//! to the client-requested representation.

use crate::config::GeospatialResultRepresentation;
use crate::kernel_error::{Error, Result};
use arrow_array::builder::{BinaryBuilder, Int32Builder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeListArray, Int32Array, LargeListArray,
    LargeListViewArray, LargeStringArray, ListArray, ListViewArray, MapArray, StringArray,
    StructArray,
};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{DataType, Field, FieldRef, Fields};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use std::borrow::Cow;
use std::fmt::Write as _;
use std::str::FromStr;
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
    /// Type after applying the requested geo representation recursively.
    pub(super) output_type: DataType,
    /// Whether the values need either local tag attachment or EWKT rendering.
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
pub(super) fn plan(
    field: &Field,
    logical_field: Option<&Field>,
    target: GeospatialResultRepresentation,
) -> Result<Option<GeoPlan>> {
    let type_plan = plan_data_type(
        field.data_type(),
        logical_field.map(Field::data_type),
        target,
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
/// Binary mode rebuilds only container array objects whose field metadata must
/// be tagged; value buffers and child arrays remain shared. String mode renders
/// each declared geo leaf to a new UTF-8 array.
pub(super) fn rewrite(
    array: &ArrayRef,
    output_type: &DataType,
    column_name: &str,
) -> Result<ArrayRef> {
    rewrite_array(array, output_type, column_name)
}

fn plan_data_type(
    physical: &DataType,
    logical: Option<&DataType>,
    target: GeospatialResultRepresentation,
    path: &str,
) -> Result<TypePlan> {
    let logical_tag = logical.map(native_geo_tag).transpose()?.flatten();
    let physical_tag = native_geo_tag(physical)?;

    if let Some(expected) = logical_tag {
        // The standard DBSQL path serializes geo values as WKT / EWKT even
        // though the structured manifest declares GEOMETRY / GEOGRAPHY. Keep
        // that physical type in string mode, or parse it into the same
        // canonical native struct that Reyden returns in binary mode.
        if matches!(physical, DataType::Utf8 | DataType::LargeUtf8) {
            let output_type = match target {
                GeospatialResultRepresentation::String => physical.clone(),
                GeospatialResultRepresentation::Binary => {
                    logical.expect("logical_tag implies a logical type").clone()
                }
            };
            return Ok(TypePlan {
                output_type,
                contains_geo: true,
                is_root_geo: true,
            });
        }
        validate_geo_source(physical, expected, path)?;
        let output_type = match target {
            GeospatialResultRepresentation::String => DataType::Utf8,
            GeospatialResultRepresentation::Binary => {
                // The logical schema is canonical and contains the family and
                // type-level SRID tags that can be missing from the payload.
                logical.expect("logical_tag implies a logical type").clone()
            }
        };
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
        let output_type = match target {
            GeospatialResultRepresentation::String => DataType::Utf8,
            GeospatialResultRepresentation::Binary => physical.clone(),
        };
        return Ok(TypePlan {
            output_type,
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
                    target,
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
            target,
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
            target,
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
            target,
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
            target,
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
                target,
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
                target,
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
    target: GeospatialResultRepresentation,
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
        target,
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
        "geospatial EWKT conversion is not implemented for Arrow container {data_type:?}"
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

fn native_geo_family(data_type: &DataType) -> Result<Option<GeoFamily>> {
    Ok(native_geo_tag(data_type)?.map(|tag| tag.family))
}

fn rewrite_array(array: &ArrayRef, output_type: &DataType, path: &str) -> Result<ArrayRef> {
    if let Some(output_tag) = native_geo_tag(output_type)? {
        if matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return text_to_native(array, output_type, path);
        }
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

    if output_type == &DataType::Utf8 && is_geo_struct_shape(array.data_type()) {
        return native_to_ewkt(array, path);
    }

    if native_geo_family(array.data_type())?.is_some() {
        return Err(Error::internal().message(format!(
            "geospatial rewrite for '{path}' planned unexpected output type {output_type:?}"
        )));
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

fn text_to_native(array: &ArrayRef, output_type: &DataType, path: &str) -> Result<ArrayRef> {
    let DataType::Struct(output_fields) = output_type else {
        unreachable!("a geo tag is only valid on a struct")
    };

    match array.data_type() {
        DataType::Utf8 => {
            let values = downcast::<StringArray>(array, path, "StringArray")?;
            ewkt_values_to_native(values.iter(), values.len(), output_fields, path)
        }
        DataType::LargeUtf8 => {
            let values = downcast::<LargeStringArray>(array, path, "LargeStringArray")?;
            ewkt_values_to_native(values.iter(), values.len(), output_fields, path)
        }
        other => Err(Error::internal().message(format!(
            "geospatial text conversion for '{path}' received unexpected Arrow type {other:?}"
        ))),
    }
}

fn ewkt_values_to_native<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    len: usize,
    output_fields: &Fields,
    path: &str,
) -> Result<ArrayRef> {
    let mut srids = Int32Builder::with_capacity(len);
    let mut wkbs = BinaryBuilder::with_capacity(len, len.saturating_mul(32));
    let mut nulls = NullBufferBuilder::new(len);

    for (row, value) in values.enumerate() {
        let Some(value) = value else {
            // Geo struct children are non-nullable. Use ignored placeholders
            // beneath a null outer struct, matching Reyden's representation.
            srids.append_value(0);
            wkbs.append_value([]);
            nulls.append_null();
            continue;
        };

        let (srid, wkt_body) = split_ewkt(value)
            .map_err(|detail| malformed_text(path, row, &format!("has invalid EWKT: {detail}")))?;
        let normalized = normalize_multipoint_empty_members(wkt_body);
        let geometry = wkt::Wkt::<f64>::from_str(&normalized).map_err(|error| {
            malformed_text(path, row, &format!("contains invalid WKT: {error}"))
        })?;
        let mut wkb = Vec::with_capacity(wkb::writer::geometry_wkb_size(&geometry));
        wkb::writer::write_geometry(&mut wkb, &geometry, &Default::default()).map_err(|error| {
            malformed_text(
                path,
                row,
                &format!("could not be encoded as OGC WKB: {error}"),
            )
        })?;

        srids.append_value(srid);
        wkbs.append_value(wkb);
        nulls.append_non_null();
    }

    let output = StructArray::try_new(
        output_fields.clone(),
        vec![Arc::new(srids.finish()), Arc::new(wkbs.finish())],
        nulls.finish(),
    )
    .map_err(|error| rebuild_error(path, error))?;
    Ok(Arc::new(output))
}

/// The `wkt` parser currently rejects empty point members inside a
/// `MULTIPOINT`, even though Databricks emits valid values such as
/// `MULTIPOINT(EMPTY,(1 2))`. WKB represents an empty point as an all-NaN
/// coordinate, so substitute parseable NaN coordinates before handing the
/// value to the generic WKT-to-WKB pipeline.
fn normalize_multipoint_empty_members(value: &str) -> Cow<'_, str> {
    let bytes = value.as_bytes();
    let mut replacements = Vec::new();
    let mut cursor = 0;

    while cursor < bytes.len() {
        if !bytes[cursor].is_ascii_alphabetic() {
            cursor += 1;
            continue;
        }

        let word_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_alphabetic() {
            cursor += 1;
        }
        let word = &bytes[word_start..cursor];
        let (mut dimension, mut body_start) = if word.eq_ignore_ascii_case(b"MULTIPOINT") {
            (2, skip_ascii_whitespace(bytes, cursor))
        } else if word.eq_ignore_ascii_case(b"MULTIPOINTZ")
            || word.eq_ignore_ascii_case(b"MULTIPOINTM")
        {
            (3, skip_ascii_whitespace(bytes, cursor))
        } else if word.eq_ignore_ascii_case(b"MULTIPOINTZM") {
            (4, skip_ascii_whitespace(bytes, cursor))
        } else {
            continue;
        };

        if dimension == 2 {
            let dimension_end = bytes[body_start..]
                .iter()
                .position(|byte| !byte.is_ascii_alphabetic())
                .map(|offset| body_start + offset)
                .unwrap_or(bytes.len());
            let dimension_word = &bytes[body_start..dimension_end];
            if dimension_word.eq_ignore_ascii_case(b"Z")
                || dimension_word.eq_ignore_ascii_case(b"M")
            {
                dimension = 3;
                body_start = skip_ascii_whitespace(bytes, dimension_end);
            } else if dimension_word.eq_ignore_ascii_case(b"ZM") {
                dimension = 4;
                body_start = skip_ascii_whitespace(bytes, dimension_end);
            }
        }

        // `MULTIPOINT EMPTY` describes an empty collection. Only a parenthesized
        // body can contain empty point members that need normalization.
        if bytes.get(body_start) != Some(&b'(') {
            continue;
        }

        let mut depth = 1;
        let mut index = body_start + 1;
        while index < bytes.len() && depth != 0 {
            match bytes[index] {
                b'(' => {
                    depth += 1;
                    index += 1;
                }
                b')' => {
                    depth -= 1;
                    index += 1;
                }
                byte if byte.is_ascii_alphabetic() => {
                    let start = index;
                    while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
                        index += 1;
                    }
                    if bytes[start..index].eq_ignore_ascii_case(b"EMPTY") {
                        replacements.push((start, index, dimension));
                    }
                }
                _ => index += 1,
            }
        }
        cursor = index;
    }

    if replacements.is_empty() {
        return Cow::Borrowed(value);
    }

    let mut output = String::with_capacity(value.len() + replacements.len() * 16);
    let mut copied_through = 0;
    for (start, end, dimension) in replacements {
        output.push_str(&value[copied_through..start]);
        for ordinate in 0..dimension {
            if ordinate != 0 {
                output.push(' ');
            }
            // A leading `+` makes `NaN` number-like to the `wkt` tokenizer.
            output.push_str("+NaN");
        }
        copied_through = end;
    }
    output.push_str(&value[copied_through..]);
    Cow::Owned(output)
}

fn skip_ascii_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    index
}

/// Split a standard-warehouse WKT / EWKT value into its row SRID and WKT
/// body. Databricks omits the EWKT prefix for SRID 0, including values in a
/// `GEOMETRY(ANY)` column, so an absent prefix deliberately maps to zero.
fn split_ewkt(value: &str) -> std::result::Result<(i32, &str), String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("value is empty".to_string());
    }

    let Some(prefix) = value.get(..5) else {
        return Ok((0, value));
    };
    if !prefix.eq_ignore_ascii_case("SRID=") {
        return Ok((0, value));
    }

    let rest = &value[5..];
    let separator = rest
        .find(';')
        .ok_or_else(|| "SRID prefix is missing ';'".to_string())?;
    let srid_text = rest[..separator].trim();
    let srid = srid_text
        .parse::<i32>()
        .map_err(|error| format!("invalid SRID '{srid_text}': {error}"))?;
    let wkt_body = rest[separator + 1..].trim();
    if wkt_body.is_empty() {
        return Err("WKT body is empty".to_string());
    }
    Ok((srid, wkt_body))
}

fn native_to_ewkt(array: &ArrayRef, path: &str) -> Result<ArrayRef> {
    let values = downcast::<StructArray>(array, path, "StructArray")?;
    let srids = values
        .column_by_name("srid")
        .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
        .ok_or_else(|| malformed_native(path, "missing Int32 child 'srid'"))?;
    let wkbs = values
        .column_by_name("wkb")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| malformed_native(path, "missing Binary child 'wkb'"))?;

    let mut output = StringBuilder::with_capacity(values.len(), values.len() * 32);
    for row in 0..values.len() {
        if values.is_null(row) {
            output.append_null();
            continue;
        }
        if srids.is_null(row) || wkbs.is_null(row) {
            return Err(malformed_native(
                path,
                &format!("row {row} has a null child under a non-null struct"),
            ));
        }

        let geometry = wkb::reader::read_wkb(wkbs.value(row)).map_err(|error| {
            Error::data_loss().message(format!(
                "geospatial value '{path}' row {row} contains invalid WKB: {error}"
            ))
        })?;
        let mut ewkt = String::new();
        let srid = srids.value(row);
        if srid != 0 {
            write!(&mut ewkt, "SRID={srid};").expect("writing to String is infallible");
        }
        write_ogc_wkt(&mut ewkt, &geometry).map_err(|error| {
            Error::data_loss().message(format!(
                "geospatial value '{path}' row {row} could not be rendered as WKT: {error}"
            ))
        })?;
        output.append_value(ewkt);
    }
    Ok(Arc::new(output.finish()) as ArrayRef)
}

/// Error from rendering an already-validated WKB geometry as OGC WKT.
///
/// WKB only contains the seven Simple Features geometry families handled
/// below. The additional `geo_traits` variants are rejected explicitly so a
/// future parser change cannot silently emit a non-OGC representation.
#[derive(Debug)]
enum WktRenderError {
    UnsupportedDimension(Dimensions),
    CoordinateDimension {
        geometry: Dimensions,
        coordinate: Dimensions,
    },
    MissingCoordinate(usize),
    UnsupportedGeometry(&'static str),
}

impl std::fmt::Display for WktRenderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedDimension(dimension) => {
                write!(formatter, "unsupported coordinate dimension {dimension:?}")
            }
            Self::CoordinateDimension {
                geometry,
                coordinate,
            } => write!(
                formatter,
                "coordinate dimension {coordinate:?} does not match geometry dimension {geometry:?}"
            ),
            Self::MissingCoordinate(index) => {
                write!(formatter, "coordinate is missing ordinate {index}")
            }
            Self::UnsupportedGeometry(kind) => {
                write!(formatter, "unsupported non-WKB geometry kind {kind}")
            }
        }
    }
}

type WktRenderResult = std::result::Result<(), WktRenderError>;

/// Render the OGC/ISO WKB geometry as WKT without allocating an intermediate
/// geometry tree.
///
/// The `wkt` crate's generic writer currently assumes that members of multi
/// geometries are never empty. Databricks permits values such as
/// `MULTIPOINT(EMPTY,(1 2))` and `MULTIPOLYGON(EMPTY,...)`, so walking the
/// `geo_traits` view directly is both allocation-free and preserves those
/// valid values instead of panicking.
fn write_ogc_wkt(output: &mut String, geometry: &impl GeometryTrait<T = f64>) -> WktRenderResult {
    match geometry.as_type() {
        GeometryType::Point(point) => write_point(output, point),
        GeometryType::LineString(line) => write_line_string(output, line),
        GeometryType::Polygon(polygon) => write_polygon(output, polygon),
        GeometryType::MultiPoint(points) => write_multi_point(output, points),
        GeometryType::MultiLineString(lines) => write_multi_line_string(output, lines),
        GeometryType::MultiPolygon(polygons) => write_multi_polygon(output, polygons),
        GeometryType::GeometryCollection(collection) => {
            write_geometry_collection(output, collection)
        }
        GeometryType::Rect(_) => Err(WktRenderError::UnsupportedGeometry("RECT")),
        GeometryType::Triangle(_) => Err(WktRenderError::UnsupportedGeometry("TRIANGLE")),
        GeometryType::Line(_) => Err(WktRenderError::UnsupportedGeometry("LINE")),
    }
}

fn write_point(output: &mut String, point: &impl PointTrait<T = f64>) -> WktRenderResult {
    let dimension = point.dim();
    write_type_name(output, "POINT", dimension)?;
    match point.coord() {
        Some(coordinate) => {
            write_body_start(output, dimension);
            write_coordinate(output, &coordinate, dimension)?;
            output.push(')');
        }
        None => output.push_str(" EMPTY"),
    }
    Ok(())
}

fn write_line_string(output: &mut String, line: &impl LineStringTrait<T = f64>) -> WktRenderResult {
    let dimension = line.dim();
    write_type_name(output, "LINESTRING", dimension)?;
    if line.num_coords() == 0 {
        output.push_str(" EMPTY");
    } else {
        write_body_start(output, dimension);
        write_coordinates(output, line, dimension)?;
        output.push(')');
    }
    Ok(())
}

fn write_polygon(output: &mut String, polygon: &impl PolygonTrait<T = f64>) -> WktRenderResult {
    let dimension = polygon.dim();
    write_type_name(output, "POLYGON", dimension)?;
    match polygon.exterior() {
        Some(exterior) if exterior.num_coords() != 0 => {
            write_body_start(output, dimension);
            write_polygon_body(output, &exterior, polygon.interiors(), dimension)?;
            output.push(')');
        }
        _ => output.push_str(" EMPTY"),
    }
    Ok(())
}

fn write_multi_point(
    output: &mut String,
    points: &impl MultiPointTrait<T = f64>,
) -> WktRenderResult {
    let dimension = points.dim();
    write_type_name(output, "MULTIPOINT", dimension)?;
    if points.num_points() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }

    write_body_start(output, dimension);
    for (index, point) in points.points().enumerate() {
        write_separator(output, index);
        if let Some(coordinate) = point.coord() {
            output.push('(');
            write_coordinate(output, &coordinate, dimension)?;
            output.push(')');
        } else {
            output.push_str("EMPTY");
        }
    }
    output.push(')');
    Ok(())
}

fn write_multi_line_string(
    output: &mut String,
    lines: &impl MultiLineStringTrait<T = f64>,
) -> WktRenderResult {
    let dimension = lines.dim();
    write_type_name(output, "MULTILINESTRING", dimension)?;
    if lines.num_line_strings() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }

    write_body_start(output, dimension);
    for (index, line) in lines.line_strings().enumerate() {
        write_separator(output, index);
        if line.num_coords() == 0 {
            output.push_str("EMPTY");
        } else {
            output.push('(');
            write_coordinates(output, &line, dimension)?;
            output.push(')');
        }
    }
    output.push(')');
    Ok(())
}

fn write_multi_polygon(
    output: &mut String,
    polygons: &impl MultiPolygonTrait<T = f64>,
) -> WktRenderResult {
    let dimension = polygons.dim();
    write_type_name(output, "MULTIPOLYGON", dimension)?;
    if polygons.num_polygons() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }

    write_body_start(output, dimension);
    for (index, polygon) in polygons.polygons().enumerate() {
        write_separator(output, index);
        match polygon.exterior() {
            Some(exterior) if exterior.num_coords() != 0 => {
                output.push('(');
                write_polygon_body(output, &exterior, polygon.interiors(), dimension)?;
                output.push(')');
            }
            _ => output.push_str("EMPTY"),
        }
    }
    output.push(')');
    Ok(())
}

fn write_geometry_collection(
    output: &mut String,
    collection: &impl GeometryCollectionTrait<T = f64>,
) -> WktRenderResult {
    let dimension = collection.dim();
    write_type_name(output, "GEOMETRYCOLLECTION", dimension)?;
    if collection.num_geometries() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }

    write_body_start(output, dimension);
    for (index, geometry) in collection.geometries().enumerate() {
        write_separator(output, index);
        write_ogc_wkt(output, &geometry)?;
    }
    output.push(')');
    Ok(())
}

fn write_polygon_body<R, I>(
    output: &mut String,
    exterior: &R,
    interiors: I,
    dimension: Dimensions,
) -> WktRenderResult
where
    R: LineStringTrait<T = f64>,
    I: Iterator<Item = R>,
{
    output.push('(');
    write_coordinates(output, exterior, dimension)?;
    output.push(')');
    for interior in interiors {
        output.push_str(",(");
        write_coordinates(output, &interior, dimension)?;
        output.push(')');
    }
    Ok(())
}

fn write_coordinates(
    output: &mut String,
    line: &impl LineStringTrait<T = f64>,
    dimension: Dimensions,
) -> WktRenderResult {
    for (index, coordinate) in line.coords().enumerate() {
        write_separator(output, index);
        write_coordinate(output, &coordinate, dimension)?;
    }
    Ok(())
}

fn write_coordinate(
    output: &mut String,
    coordinate: &impl CoordTrait<T = f64>,
    dimension: Dimensions,
) -> WktRenderResult {
    if coordinate.dim().size() != dimension.size() {
        return Err(WktRenderError::CoordinateDimension {
            geometry: dimension,
            coordinate: coordinate.dim(),
        });
    }
    for index in 0..dimension.size() {
        if index != 0 {
            output.push(' ');
        }
        let ordinate = coordinate
            .nth(index)
            .ok_or(WktRenderError::MissingCoordinate(index))?;
        write!(output, "{ordinate}").expect("writing to String is infallible");
    }
    Ok(())
}

fn write_type_name(output: &mut String, type_name: &str, dimension: Dimensions) -> WktRenderResult {
    output.push_str(type_name);
    match dimension {
        Dimensions::Xy | Dimensions::Unknown(2) => {}
        Dimensions::Xyz | Dimensions::Unknown(3) => output.push_str(" Z"),
        Dimensions::Xym => output.push_str(" M"),
        Dimensions::Xyzm | Dimensions::Unknown(4) => output.push_str(" ZM"),
        unsupported => return Err(WktRenderError::UnsupportedDimension(unsupported)),
    }
    Ok(())
}

fn write_body_start(output: &mut String, dimension: Dimensions) {
    if matches!(dimension, Dimensions::Xy | Dimensions::Unknown(2)) {
        output.push('(');
    } else {
        // Databricks' canonical higher-dimensional spelling includes a
        // space between the Z/M qualifier and the coordinate body.
        output.push_str(" (");
    }
}

fn write_separator(output: &mut String, index: usize) {
    if index != 0 {
        output.push(',');
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

fn malformed_native(path: &str, detail: &str) -> Error {
    Error::data_loss().message(format!(
        "geospatial value '{path}' has malformed native Arrow data: {detail}"
    ))
}

fn malformed_text(path: &str, row: usize, detail: &str) -> Error {
    Error::data_loss().message(format!(
        "geospatial value '{path}' row {row} has malformed text data: {detail}"
    ))
}

fn rebuild_error(path: &str, error: arrow_schema::ArrowError) -> Error {
    Error::data_loss().message(format!(
        "failed to rebuild Arrow container '{path}' after geospatial conversion: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn wkt_from_hex(value: &str) -> String {
        let bytes = hex::decode(value).unwrap();
        let geometry = wkb::reader::read_wkb(&bytes).unwrap();
        let mut output = String::new();
        write_ogc_wkt(&mut output, &geometry).unwrap();
        output
    }

    #[test]
    fn recognizes_only_canonical_tagged_structs() {
        assert_eq!(
            native_geo_family(&geo_type(GeoFamily::Geometry, -1)).unwrap(),
            Some(GeoFamily::Geometry)
        );
        assert_eq!(
            native_geo_family(&geo_type(GeoFamily::Geography, 4326)).unwrap(),
            Some(GeoFamily::Geography)
        );

        let plain = plain_geo_type();
        assert_eq!(native_geo_family(&plain).unwrap(), None);
    }

    #[test]
    fn logical_geo_identifies_an_untagged_payload_and_validates_existing_tags() {
        let physical = Field::new("g", plain_geo_type(), true);
        let logical = Field::new("g", geo_type(GeoFamily::Geometry, -1), true);

        let binary = plan(
            &physical,
            Some(&logical),
            GeospatialResultRepresentation::Binary,
        )
        .unwrap()
        .unwrap();
        assert!(binary.needs_rewrite);
        assert_eq!(binary.output_type, logical.data_type().clone());

        let already_tagged = Field::new("g", geo_type(GeoFamily::Geometry, -1), true);
        let binary = plan(
            &already_tagged,
            Some(&logical),
            GeospatialResultRepresentation::Binary,
        )
        .unwrap()
        .unwrap();
        assert!(!binary.needs_rewrite);
        assert_eq!(binary.output_type, logical.data_type().clone());

        let fallback = plan(
            &already_tagged,
            None,
            GeospatialResultRepresentation::Binary,
        )
        .unwrap()
        .unwrap();
        assert!(!fallback.needs_rewrite);
        assert_eq!(fallback.output_type, already_tagged.data_type().clone());

        let string = plan(
            &physical,
            Some(&logical),
            GeospatialResultRepresentation::String,
        )
        .unwrap()
        .unwrap();
        assert_eq!(string.output_type, DataType::Utf8);

        let wrong_family = Field::new("g", geo_type(GeoFamily::Geography, -1), true);
        let error = plan(
            &wrong_family,
            Some(&logical),
            GeospatialResultRepresentation::Binary,
        )
        .unwrap_err();
        assert!(error.message.contains("tagged GEOGRAPHY"));

        let logical_plain = Field::new("g", plain_geo_type(), true);
        let error = plan(
            &already_tagged,
            Some(&logical_plain),
            GeospatialResultRepresentation::Binary,
        )
        .unwrap_err();
        assert!(error.message.contains("logical manifest type"));

        for text_type in [DataType::Utf8, DataType::LargeUtf8] {
            let text = Field::new("g", text_type.clone(), true);
            let string = plan(
                &text,
                Some(&logical),
                GeospatialResultRepresentation::String,
            )
            .unwrap()
            .unwrap();
            assert!(!string.needs_rewrite);
            assert_eq!(string.output_type, text_type);

            let binary = plan(
                &text,
                Some(&logical),
                GeospatialResultRepresentation::Binary,
            )
            .unwrap()
            .unwrap();
            assert!(binary.needs_rewrite);
            assert_eq!(binary.output_type, logical.data_type().clone());
        }

        assert!(
            plan(&physical, None, GeospatialResultRepresentation::String)
                .unwrap()
                .is_none()
        );
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
    fn native_geometry_converts_to_ewkt_and_preserves_outer_null() {
        let array = geo_array(GeoFamily::Geometry, -1, 4326);
        let output = rewrite(&array, &DataType::Utf8, "g").unwrap();
        let output = output
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(output.value(0), "SRID=4326;POINT(1 2)");
        assert!(output.is_null(1));
    }

    #[test]
    fn srid_zero_uses_plain_wkt() {
        let array = geo_array(GeoFamily::Geometry, 0, 0);
        let output = rewrite(&array, &DataType::Utf8, "g").unwrap();
        let output = output
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(output.value(0), "POINT(1 2)");
    }

    #[test]
    fn text_geometry_converts_to_native_and_preserves_outer_null() {
        for array in [
            Arc::new(StringArray::from(vec![
                Some("SRID=4326;POINT (1 2)"),
                Some("POINT ZM (1 2 3 4)"),
                None,
            ])) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some("SRID=4326;POINT (1 2)"),
                Some("POINT ZM (1 2 3 4)"),
                None,
            ])) as ArrayRef,
        ] {
            let output_type = geo_type(GeoFamily::Geometry, -1);
            let output = rewrite(&array, &output_type, "g").unwrap();
            let output = output.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(output.data_type(), &output_type);
            assert_eq!(output.null_count(), 1);
            assert!(output.is_null(2));

            let srids = output
                .column_by_name("srid")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(srids.values(), &[4326, 0, 0]);
            assert_eq!(srids.null_count(), 0);

            let wkbs = output
                .column_by_name("wkb")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            assert_eq!(wkbs.value(0), POINT_1_2_WKB);
            assert_eq!(wkbs.null_count(), 0);
            let geometry = wkb::reader::read_wkb(wkbs.value(1)).unwrap();
            let mut rendered = String::new();
            write_ogc_wkt(&mut rendered, &geometry).unwrap();
            assert_eq!(rendered, "POINT ZM (1 2 3 4)");
        }
    }

    #[test]
    fn text_geometry_converts_to_native_inside_nested_container() {
        let values: ArrayRef =
            Arc::new(StringArray::from(vec![Some("SRID=3857;POINT(1 2)"), None]));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 2])),
            values,
            Some(NullBuffer::from(vec![true, false])),
        ));
        let output_type = DataType::List(Arc::new(Field::new(
            "item",
            geo_type(GeoFamily::Geometry, -1),
            true,
        )));

        let output = rewrite(&list, &output_type, "nested_geometry").unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.data_type(), &output_type);
        assert!(output.is_null(1));

        let values = output
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(values.is_null(1));
        let srids = values
            .column_by_name("srid")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let wkbs = values
            .column_by_name("wkb")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(srids.value(0), 3857);
        assert_eq!(wkbs.value(0), POINT_1_2_WKB);
    }

    #[test]
    fn text_to_native_handles_every_ogc_family_dimensions_and_empties() {
        let cases = [
            "POINT EMPTY",
            "POINT Z (1 2 3)",
            "LINESTRING M (0 1 2,3 4 5)",
            "POLYGON ZM ((0 0 1 2,0 1 2 3,1 0 3 4,0 0 1 2))",
            "MULTIPOINT((1 2),EMPTY,(3 4))",
            "MULTIPOINT Z ((1 2 3),EMPTY,(4 5 6))",
            "MULTIPOINT M (EMPTY,(1 2 3),EMPTY)",
            "MULTIPOINT ZM ((1 2 3 4),EMPTY,(5 6 7 8))",
            "MULTILINESTRING(EMPTY,(1 2,3 4))",
            "MULTIPOLYGON(EMPTY,((0 0,0 1,1 0,0 0)))",
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY)",
            "GEOMETRYCOLLECTION ZM (POINT ZM EMPTY,MULTIPOINT ZM (EMPTY,(1 2 3 4)),GEOMETRYCOLLECTION ZM (MULTIPOINT ZM ((5 6 7 8),EMPTY)))",
        ];

        for expected in cases {
            let array: ArrayRef = Arc::new(StringArray::from(vec![expected]));
            let output = rewrite(&array, &geo_type(GeoFamily::Geometry, -1), "geometry")
                .unwrap_or_else(|error| panic!("failed to convert {expected}: {error:?}"));
            let output = output.as_any().downcast_ref::<StructArray>().unwrap();
            let wkbs = output
                .column_by_name("wkb")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let geometry = wkb::reader::read_wkb(wkbs.value(0)).unwrap();
            let mut actual = String::new();
            write_ogc_wkt(&mut actual, &geometry).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn malformed_text_is_reported_as_data_loss_without_echoing_the_value() {
        for value in [
            "SRID=invalid;POINT(1 2)",
            "SRID=4326POINT(1 2)",
            "SRID=4326;",
            "not sensitive result contents",
        ] {
            let array: ArrayRef = Arc::new(StringArray::from(vec![value]));
            let error =
                rewrite(&array, &geo_type(GeoFamily::Geometry, -1), "geometry").unwrap_err();
            assert_eq!(error.code, crate::kernel_error::ErrorCode::DataLoss);
            assert!(error.message.contains("'geometry' row 0"), "{error:?}");
            assert!(!error.message.contains(value), "{error:?}");
        }
    }

    #[test]
    fn invalid_wkb_is_reported_as_data_loss() {
        let DataType::Struct(fields) = geo_type(GeoFamily::Geometry, 0) else {
            unreachable!()
        };
        let array: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(BinaryArray::from(vec![&b"not-wkb"[..]])),
            ],
            None,
        ));

        let error = rewrite(&array, &DataType::Utf8, "g").unwrap_err();
        assert!(error.message.contains("contains invalid WKB"));
    }

    #[test]
    fn wkb_renderer_handles_dimensions_collections_and_empty_members() {
        let cases = [
            (
                "0101000000000000000000f87f000000000000f87f",
                "POINT EMPTY",
            ),
            (
                "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
                "POINT ZM (1 2 3 4)",
            ),
            (
                "010100000050fc1873d79a5ec0d0d556ec2fe34240",
                "POINT(-122.4194 37.7749)",
            ),
            (
                "0104000000040000000101000000000000000000f03f00000000000000400101000000000000000000f87f000000000000f87f0101000000000000000000f87f000000000000f87f010100000000000000000008400000000000001040",
                "MULTIPOINT((1 2),EMPTY,EMPTY,(3 4))",
            ),
            (
                "010500000003000000010200000000000000010200000002000000000000000000f03f000000000000004000000000000008400000000000001040010200000000000000",
                "MULTILINESTRING(EMPTY,(1 2,3 4),EMPTY)",
            ),
            (
                "010600000001000000010300000000000000",
                "MULTIPOLYGON(EMPTY)",
            ),
            (
                "0107000000020000000101000000000000000000f03f0000000000000040010200000000000000",
                "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY)",
            ),
        ];

        for (wkb, expected) in cases {
            assert_eq!(wkt_from_hex(wkb), expected);
        }
    }
}
