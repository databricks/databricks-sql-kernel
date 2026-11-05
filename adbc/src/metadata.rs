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

//! ADBC metadata adaptation over the kernel's flat metadata surface.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};

use adbc_core::error::{Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth};
use adbc_core::schemas::{
    COLUMN_SCHEMA, GET_INFO_SCHEMA, GET_OBJECTS_SCHEMA, GET_TABLE_TYPES_SCHEMA,
    OBJECTS_DB_SCHEMA_SCHEMA, TABLE_SCHEMA,
};
use arrow_array::cast::AsArray;
use arrow_array::{
    new_null_array, Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, ListArray,
    RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, StructArray, UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema};
use databricks_sql_kernel as kernel;

use crate::error::{adbc_error, map_kernel_error};
use crate::runtime::RuntimeExt as _;

type Reader = Box<dyn RecordBatchReader + Send + 'static>;

#[derive(Clone, Debug)]
struct TableRow {
    catalog: String,
    schema: String,
    name: String,
    table_type: String,
}

#[derive(Clone, Debug)]
struct ColumnRow {
    catalog: String,
    schema: String,
    table: String,
    name: String,
    ordinal: Option<i32>,
    remarks: Option<String>,
    data_type: Option<i16>,
    type_name: Option<String>,
    column_size: Option<i32>,
    decimal_digits: Option<i16>,
    radix: Option<i16>,
    nullable: Option<i16>,
    column_default: Option<String>,
    sql_data_type: Option<i16>,
    datetime_sub: Option<i16>,
    char_octet_length: Option<i32>,
    is_nullable: Option<String>,
    scope_catalog: Option<String>,
    scope_schema: Option<String>,
    scope_table: Option<String>,
    is_auto_increment: Option<bool>,
}

#[derive(Clone, Debug)]
struct TableNode {
    row: TableRow,
    columns: Option<Vec<ColumnRow>>,
}

#[derive(Clone, Debug)]
struct SchemaNode {
    name: String,
    tables: Option<Vec<TableNode>>,
}

#[derive(Clone, Debug)]
struct CatalogNode {
    name: String,
    schemas: Option<Vec<SchemaNode>>,
}

fn invalid(message: impl Into<String>) -> adbc_core::error::Error {
    adbc_error(message, Status::InvalidData)
}

fn reader(schema: Arc<Schema>, batch: RecordBatch) -> Reader {
    Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

fn batches_reader(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Reader {
    Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    ))
}

fn drain(
    runtime: &tokio::runtime::Runtime,
    mut stream: kernel::ResultStream,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    loop {
        match runtime
            .block_on_safe(stream.next_batch())
            .map_err(map_kernel_error)?
        {
            Some(kernel::ResultBatch::Arrow(batch)) => batches.push(batch.clone()),
            Some(_) => return Err(invalid("kernel metadata returned a non-Arrow batch")),
            None => return Ok(batches),
        }
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| invalid(format!("kernel metadata omitted required column {name}")))?;
    Ok(batch.column(index))
}

fn required_string(batch: &RecordBatch, name: &str, row: usize) -> Result<String> {
    optional_string(batch, name, row)?
        .ok_or_else(|| invalid(format!("kernel metadata returned null {name}")))
}

fn optional_string(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<String>> {
    let value = column(batch, name)?;
    if value.is_null(row) {
        return Ok(None);
    }
    match value.data_type() {
        DataType::Utf8 => Ok(Some(value.as_string::<i32>().value(row).to_owned())),
        DataType::LargeUtf8 => Ok(Some(value.as_string::<i64>().value(row).to_owned())),
        other => Err(invalid(format!(
            "kernel metadata column {name} is {other}, expected UTF-8"
        ))),
    }
}

fn optional_i32(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<i32>> {
    let value = column(batch, name)?;
    if value.is_null(row) {
        return Ok(None);
    }
    value
        .as_any()
        .downcast_ref::<Int32Array>()
        .map(|array| Some(array.value(row)))
        .ok_or_else(|| invalid(format!("kernel metadata column {name} is not Int32")))
}

fn optional_i16(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<i16>> {
    optional_i32(batch, name, row)?
        .map(|value| {
            i16::try_from(value)
                .map_err(|_| invalid(format!("kernel metadata column {name} overflows Int16")))
        })
        .transpose()
}

fn optional_yes_no(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<bool>> {
    Ok(optional_string(batch, name, row)?.and_then(|value| {
        match value.to_ascii_lowercase().as_str() {
            "yes" | "true" => Some(true),
            "no" | "false" => Some(false),
            _ => None,
        }
    }))
}

fn pattern(value: Option<&str>) -> Result<Option<kernel::LikePattern<'_>>> {
    value
        .map(kernel::LikePattern::new)
        .transpose()
        .map_err(map_kernel_error)
}

fn identifier(value: Option<&str>) -> Result<Option<kernel::Identifier<'_>>> {
    value
        .map(kernel::Identifier::new)
        .transpose()
        .map_err(map_kernel_error)
}

fn escaped_pattern(value: Option<&str>, escape_wildcards: bool) -> Option<String> {
    value.map(|value| {
        if escape_wildcards {
            value
                .replace('\\', "\\\\")
                .replace('_', "\\_")
                .replace('%', "\\%")
        } else {
            value.to_owned()
        }
    })
}

fn escape_catalog_underscores(value: Option<&str>, enabled: bool) -> Option<String> {
    value.map(|value| {
        if !enabled {
            return value.to_owned();
        }
        let mut escaped = String::with_capacity(value.len());
        let mut consecutive_backslashes = 0usize;
        for character in value.chars() {
            if character == '_' && consecutive_backslashes.is_multiple_of(2) {
                escaped.push('\\');
            }
            escaped.push(character);
            if character == '\\' {
                consecutive_backslashes += 1;
            } else {
                consecutive_backslashes = 0;
            }
        }
        escaped
    })
}

fn metadata_base_type(type_name: &str) -> String {
    let normalized = type_name.trim().to_ascii_uppercase();
    if matches!(normalized.as_str(), "INT" | "INTEGER") {
        return "INTEGER".into();
    }
    if matches!(
        normalized.as_str(),
        "TIMESTAMP" | "TIMESTAMP_LTZ" | "TIMESTAMP_NTZ"
    ) {
        return "TIMESTAMP".into();
    }
    if normalized.starts_with("INTERVAL ") {
        return "INTERVAL".into();
    }
    let end = normalized
        .char_indices()
        .find(|(_, character)| matches!(character, '<' | '('))
        .map_or(normalized.len(), |(index, _)| index);
    normalized[..end].trim().to_owned()
}

fn table_type_filter(value: Option<&str>) -> Option<Vec<&str>> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| value.split(',').collect())
}

fn append_base_type(batch: RecordBatch, schema: Arc<Schema>) -> Result<RecordBatch> {
    let mut column_sizes = Vec::with_capacity(batch.num_rows());
    let mut decimal_digits = Vec::with_capacity(batch.num_rows());
    let mut ordinals = Vec::with_capacity(batch.num_rows());
    let mut values = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let type_name = required_string(&batch, "TYPE_NAME", row)?;
        let original_size = optional_i32(&batch, "COLUMN_SIZE", row)?.unwrap_or_default();
        let original_scale = optional_i32(&batch, "DECIMAL_DIGITS", row)?.unwrap_or_default();
        let (size, scale) = csharp_precision_scale(&type_name, original_size, original_scale);
        column_sizes.push(Some(size));
        decimal_digits.push(Some(scale));
        ordinals.push(
            optional_i32(&batch, "ORDINAL_POSITION", row)?.map(|ordinal| ordinal.saturating_sub(1)),
        );
        values.push(metadata_base_type(&type_name));
    }
    let mut columns = batch.columns().to_vec();
    columns[schema.index_of("COLUMN_SIZE")?] = Arc::new(Int32Array::from(column_sizes));
    columns[schema.index_of("DECIMAL_DIGITS")?] = Arc::new(Int32Array::from(decimal_digits));
    columns[schema.index_of("ORDINAL_POSITION")?] = Arc::new(Int32Array::from(ordinals));
    columns.push(Arc::new(StringArray::from(values)));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn type_arguments(type_name: &str) -> Option<Vec<i32>> {
    let (_, arguments) = type_name.split_once('(')?;
    let arguments = arguments
        .split_once(')')
        .map_or(arguments, |(value, _)| value);
    arguments
        .split(',')
        .map(|value| value.trim().parse::<i32>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()
}

fn csharp_precision_scale(type_name: &str, original_size: i32, original_scale: i32) -> (i32, i32) {
    let base_type = metadata_base_type(type_name);
    match base_type.as_str() {
        "STRING" | "VARCHAR" | "LONGVARCHAR" | "LONGNVARCHAR" | "NVARCHAR" => (
            type_arguments(type_name)
                .and_then(|values| values.first().copied())
                .unwrap_or(i32::MAX),
            original_scale,
        ),
        "CHAR" | "NCHAR" => (
            type_arguments(type_name)
                .and_then(|values| values.first().copied())
                .unwrap_or(255),
            original_scale,
        ),
        "DECIMAL" | "NUMERIC" => {
            let arguments = type_arguments(type_name).unwrap_or_else(|| vec![10, 0]);
            (
                arguments.first().copied().unwrap_or(10),
                arguments.get(1).copied().unwrap_or_default(),
            )
        }
        _ => (original_size, original_scale),
    }
}

fn append_global_temp_schema(batches: &mut Vec<RecordBatch>, schema: &Arc<Schema>) -> Result<()> {
    for batch in batches.iter() {
        for row in 0..batch.num_rows() {
            if optional_string(batch, "TABLE_SCHEM", row)?.as_deref() == Some("global_temp") {
                return Ok(());
            }
        }
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| match (field.name().as_str(), field.data_type()) {
            ("TABLE_SCHEM", DataType::Utf8) => {
                Ok(Arc::new(StringArray::from(vec!["global_temp"])) as ArrayRef)
            }
            ("TABLE_CATALOG", DataType::Utf8) => {
                Ok(Arc::new(StringArray::from(vec![""])) as ArrayRef)
            }
            ("TABLE_SCHEM", DataType::LargeUtf8) => {
                Ok(Arc::new(arrow_array::LargeStringArray::from(vec!["global_temp"])) as ArrayRef)
            }
            ("TABLE_CATALOG", DataType::LargeUtf8) => {
                Ok(Arc::new(arrow_array::LargeStringArray::from(vec![""])) as ArrayRef)
            }
            (name, data_type) => Err(invalid(format!(
                "unexpected GetSchemas field {name} with type {data_type}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    batches.push(RecordBatch::try_new(Arc::clone(schema), columns)?);
    Ok(())
}

fn csharp_key_schema(schema: &Arc<Schema>) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "KEY_SEQ" {
                    Arc::new(field.as_ref().clone().with_name("KEQ_SEQ"))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

#[allow(clippy::too_many_arguments)] // Mirrors the C# metadata-statement option surface.
pub(crate) fn execute_statement_command(
    metadata: &kernel::Metadata,
    runtime: &tokio::runtime::Runtime,
    command: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: Option<&str>,
    column_name: Option<&str>,
    table_types: Option<&str>,
    foreign_catalog: Option<&str>,
    foreign_schema: Option<&str>,
    foreign_table: Option<&str>,
    escape_wildcards: bool,
    escape_catalog_name: bool,
) -> Result<Reader> {
    let exact_catalog = catalog;
    let exact_schema = schema;
    let exact_table = table;
    let catalog = if escape_wildcards {
        escaped_pattern(catalog, true)
    } else {
        escape_catalog_underscores(catalog, escape_catalog_name)
    };
    let schema = escaped_pattern(schema, escape_wildcards);
    let table = escaped_pattern(table, escape_wildcards);
    let column_name = escaped_pattern(column_name, escape_wildcards);
    let table_types = table_type_filter(table_types);
    let stream = match command.to_ascii_lowercase().as_str() {
        "getcatalogs" => runtime
            .block_on_safe(metadata.list_catalogs())
            .map_err(map_kernel_error)?,
        "getschemas" => runtime
            .block_on_safe(
                metadata.list_schemas(identifier(catalog.as_deref())?, pattern(schema.as_deref())?),
            )
            .map_err(map_kernel_error)?,
        "gettables" => runtime
            .block_on_safe(metadata.list_tables(
                pattern(catalog.as_deref())?,
                pattern(schema.as_deref())?,
                pattern(table.as_deref())?,
                table_types.as_deref(),
            ))
            .map_err(map_kernel_error)?,
        "getcolumns" => runtime
            .block_on_safe(metadata.list_columns(
                identifier(catalog.as_deref())?,
                pattern(schema.as_deref())?,
                pattern(table.as_deref())?,
                pattern(column_name.as_deref())?,
            ))
            .map_err(map_kernel_error)?,
        "getprimarykeys" => {
            let table = exact_table.ok_or_else(|| {
                adbc_error(
                    "GetPrimaryKeys requires target_table",
                    Status::InvalidArguments,
                )
            })?;
            runtime
                .block_on_safe(metadata.primary_keys(
                    identifier(exact_catalog)?,
                    identifier(exact_schema)?,
                    kernel::Identifier::new(table).map_err(map_kernel_error)?,
                ))
                .map_err(map_kernel_error)?
        }
        "getcrossreference" => runtime
            .block_on_safe(metadata.foreign_keys(
                identifier(exact_catalog)?,
                identifier(exact_schema)?,
                identifier(exact_table)?,
                identifier(foreign_catalog)?,
                identifier(foreign_schema)?,
                identifier(foreign_table)?,
            ))
            .map_err(map_kernel_error)?,
        "" => {
            return Err(adbc_error(
                "metadata command is not set",
                Status::InvalidState,
            ));
        }
        _ => {
            return Err(adbc_error(
                format!("unsupported metadata command {command:?}"),
                Status::NotImplemented,
            ));
        }
    };
    let source_schema = stream.schema();
    let batches = drain(runtime, stream)?;
    if command.eq_ignore_ascii_case("GetPrimaryKeys")
        || command.eq_ignore_ascii_case("GetCrossReference")
    {
        let output_schema = csharp_key_schema(&source_schema);
        let batches = batches
            .into_iter()
            .map(|batch| {
                RecordBatch::try_new(Arc::clone(&output_schema), batch.columns().to_vec())
                    .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(batches_reader(output_schema, batches));
    }
    if command.eq_ignore_ascii_case("GetSchemas") {
        let mut batches = batches;
        if schema
            .as_deref()
            .is_none_or(|schema| like_match(schema, "global_temp"))
        {
            append_global_temp_schema(&mut batches, &source_schema)?;
        }
        return Ok(batches_reader(source_schema, batches));
    }
    if !command.eq_ignore_ascii_case("GetColumns") {
        return Ok(batches_reader(source_schema, batches));
    }

    let mut fields = source_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new("BASE_TYPE_NAME", DataType::Utf8, true)));
    let output_schema = Arc::new(Schema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    let batches = batches
        .into_iter()
        .map(|batch| append_base_type(batch, Arc::clone(&output_schema)))
        .collect::<Result<Vec<_>>>()?;
    Ok(batches_reader(output_schema, batches))
}

fn catalog_schema_hierarchy(
    metadata: &kernel::Metadata,
    runtime: &tokio::runtime::Runtime,
    catalog: Option<&str>,
    schema: Option<&str>,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let catalog_stream = runtime
        .block_on_safe(metadata.list_catalogs())
        .map_err(map_kernel_error)?;
    let mut hierarchy = BTreeMap::new();
    for batch in drain(runtime, catalog_stream)? {
        for row in 0..batch.num_rows() {
            let catalog_name = required_string(&batch, "TABLE_CAT", row)?;
            if !catalog.is_none_or(|value| like_match(value, &catalog_name)) {
                continue;
            }
            let schema_stream = runtime
                .block_on_safe(metadata.list_schemas(
                    Some(kernel::Identifier::new(&catalog_name).map_err(map_kernel_error)?),
                    pattern(schema)?,
                ))
                .map_err(map_kernel_error)?;
            let mut schemas = BTreeSet::new();
            for schema_batch in drain(runtime, schema_stream)? {
                for schema_row in 0..schema_batch.num_rows() {
                    schemas.insert(required_string(&schema_batch, "TABLE_SCHEM", schema_row)?);
                }
            }
            hierarchy.insert(catalog_name, schemas);
        }
    }
    Ok(hierarchy)
}

fn initialize_table_hierarchy(
    hierarchy: BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, BTreeMap<String, Vec<TableNode>>> {
    hierarchy
        .into_iter()
        .map(|(catalog_name, schemas)| {
            (
                catalog_name,
                schemas
                    .into_iter()
                    .map(|schema_name| (schema_name, Vec::new()))
                    .collect(),
            )
        })
        .collect()
}

fn union_fields() -> arrow_schema::UnionFields {
    match GET_INFO_SCHEMA.field(1).data_type() {
        DataType::Union(fields, _) => fields.clone(),
        _ => unreachable!("ADBC get-info schema must contain a union"),
    }
}

enum InfoValue {
    String(Option<String>),
    Bool(bool),
    Int64(i64),
}

fn current_vendor_version(
    session: &kernel::Session,
    runtime: &tokio::runtime::Runtime,
) -> Result<String> {
    let mut statement = session.statement();
    statement.spec().sql("SELECT current_version()");
    let mut executed = runtime
        .block_on_safe(statement.execute())
        .map_err(map_kernel_error)?;
    let value = match runtime
        .block_on_safe(executed.next_batch())
        .map_err(map_kernel_error)?
    {
        Some(kernel::ResultBatch::Arrow(batch))
            if batch.num_rows() > 0 && batch.num_columns() > 0 =>
        {
            let values = batch.column(0);
            current_version_value(values.as_ref())?
        }
        Some(_) => return Err(invalid("current_version() returned an empty result")),
        None => return Err(invalid("current_version() returned no result")),
    };
    while runtime
        .block_on_safe(executed.next_batch())
        .map_err(map_kernel_error)?
        .is_some()
    {}
    // The server returns JSON. Avoid coupling metadata to a JSON dependency for two optional keys.
    for key in ["dbsql_version", "dbr_version"] {
        let marker = format!("\"{key}\"");
        if let Some(after_key) = value.split_once(&marker).map(|(_, rest)| rest) {
            if let Some(after_colon) = after_key.split_once(':').map(|(_, rest)| rest.trim_start())
            {
                if let Some(quoted) = after_colon.strip_prefix('"') {
                    if let Some((version, _)) = quoted.split_once('"') {
                        if !version.is_empty() {
                            return Ok(version.to_owned());
                        }
                    }
                }
            }
        }
    }
    Ok("unknown".to_owned())
}

fn current_version_value(values: &dyn Array) -> Result<String> {
    match values.data_type() {
        DataType::Utf8 => Ok(values.as_string::<i32>().value(0).to_owned()),
        DataType::LargeUtf8 => Ok(values.as_string::<i64>().value(0).to_owned()),
        DataType::Struct(_) => {
            let values = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("current_version() returned an invalid struct"))?;
            for name in ["dbsql_version", "dbr_version"] {
                let Some(column) = values.column_by_name(name) else {
                    continue;
                };
                let version = match column.data_type() {
                    DataType::Utf8 if !column.is_null(0) => {
                        Some(column.as_string::<i32>().value(0))
                    }
                    DataType::LargeUtf8 if !column.is_null(0) => {
                        Some(column.as_string::<i64>().value(0))
                    }
                    _ => None,
                };
                if let Some(version) = version.filter(|version| !version.is_empty()) {
                    return Ok(format!("{{\"{name}\":\"{version}\"}}"));
                }
            }
            Ok("unknown".to_owned())
        }
        other => Err(invalid(format!(
            "current_version() returned {other}, expected UTF-8 or Struct"
        ))),
    }
}

pub(crate) fn get_info(
    session: &kernel::Session,
    runtime: &tokio::runtime::Runtime,
    vendor_version_cache: &Mutex<Option<String>>,
    codes: Option<HashSet<InfoCode>>,
) -> Result<Reader> {
    let requested = |code: InfoCode| codes.as_ref().is_none_or(|codes| codes.contains(&code));
    let vendor_version = if requested(InfoCode::VendorVersion) {
        let cached = vendor_version_cache
            .lock()
            .map_err(|_| invalid("vendor-version cache lock is poisoned"))?
            .clone();
        if let Some(version) = cached {
            Some(version)
        } else {
            let version = current_vendor_version(session, runtime)?;
            *vendor_version_cache
                .lock()
                .map_err(|_| invalid("vendor-version cache lock is poisoned"))? =
                Some(version.clone());
            Some(version)
        }
    } else {
        None
    };
    let mut rows = Vec::<(u32, InfoValue)>::new();
    let mut add = |code: InfoCode, value: InfoValue| {
        if requested(code) {
            rows.push((u32::from(&code), value));
        }
    };
    add(
        InfoCode::VendorName,
        InfoValue::String(Some("Databricks".into())),
    );
    if let Some(version) = vendor_version {
        add(InfoCode::VendorVersion, InfoValue::String(Some(version)));
    }
    add(
        InfoCode::VendorArrowVersion,
        InfoValue::String(Some("59".into())),
    );
    add(InfoCode::VendorSql, InfoValue::Bool(true));
    add(InfoCode::VendorSubstrait, InfoValue::Bool(false));
    add(InfoCode::VendorSubstraitMinVersion, InfoValue::String(None));
    add(InfoCode::VendorSubstraitMaxVersion, InfoValue::String(None));
    add(
        InfoCode::DriverName,
        InfoValue::String(Some("Databricks Rust ADBC Driver".into())),
    );
    add(
        InfoCode::DriverVersion,
        InfoValue::String(Some(env!("CARGO_PKG_VERSION").into())),
    );
    add(
        InfoCode::DriverArrowVersion,
        InfoValue::String(Some("59".into())),
    );
    add(InfoCode::DriverAdbcVersion, InfoValue::Int64(1_001_000));
    rows.sort_by_key(|(code, _)| *code);

    build_info(rows)
}

fn build_info(rows: Vec<(u32, InfoValue)>) -> Result<Reader> {
    let mut names = Vec::with_capacity(rows.len());
    let mut type_ids = Vec::with_capacity(rows.len());
    let mut offsets = Vec::with_capacity(rows.len());
    let mut strings = Vec::new();
    let mut bools = Vec::new();
    let mut ints = Vec::new();
    for (name, value) in rows {
        names.push(name);
        match value {
            InfoValue::String(value) => {
                type_ids.push(0);
                offsets.push(strings.len() as i32);
                strings.push(value);
            }
            InfoValue::Bool(value) => {
                type_ids.push(1);
                offsets.push(bools.len() as i32);
                bools.push(value);
            }
            InfoValue::Int64(value) => {
                type_ids.push(2);
                offsets.push(ints.len() as i32);
                ints.push(value);
            }
        }
    }
    let fields = union_fields();
    let union = UnionArray::try_new(
        union_fields(),
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        vec![
            Arc::new(StringArray::from(strings)),
            Arc::new(BooleanArray::from(bools)),
            Arc::new(Int64Array::from(ints)),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            new_null_array(fields.iter().nth(4).unwrap().1.data_type(), 0),
            new_null_array(fields.iter().nth(5).unwrap().1.data_type(), 0),
        ],
    )
    .map_err(|error| invalid(format!("failed to build get-info union: {error}")))?;
    let batch = RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![
            Arc::new(arrow_array::UInt32Array::from(names)),
            Arc::new(union),
        ],
    )?;
    Ok(reader(GET_INFO_SCHEMA.clone(), batch))
}

pub(crate) fn get_table_types(
    session: &kernel::Session,
    runtime: &tokio::runtime::Runtime,
) -> Result<Reader> {
    let stream = runtime
        .block_on_safe(session.metadata().table_types())
        .map_err(map_kernel_error)?;
    let mut values = Vec::new();
    for batch in drain(runtime, stream)? {
        for row in 0..batch.num_rows() {
            values.push(required_string(&batch, "TABLE_TYPE", row)?);
        }
    }
    let batch = RecordBatch::try_new(
        GET_TABLE_TYPES_SCHEMA.clone(),
        vec![Arc::new(StringArray::from(values))],
    )?;
    Ok(reader(GET_TABLE_TYPES_SCHEMA.clone(), batch))
}

pub(crate) fn get_table_schema(
    session: &kernel::Session,
    runtime: &tokio::runtime::Runtime,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
) -> Result<Schema> {
    if catalog == Some("") || schema == Some("") {
        return Err(adbc_error("table not found", Status::NotFound));
    }
    let stream = runtime
        .block_on_safe(session.metadata().list_columns(
            identifier(catalog)?,
            pattern(schema)?,
            pattern(Some(table))?,
            None,
        ))
        .map_err(map_kernel_error)?;
    let mut fields = Vec::new();
    let mut has_geospatial_type = false;
    for batch in drain(runtime, stream)? {
        for row in 0..batch.num_rows() {
            if optional_string(&batch, "TABLE_CAT", row)?.as_deref() != catalog && catalog.is_some()
            {
                continue;
            }
            if optional_string(&batch, "TABLE_SCHEM", row)?.as_deref() != schema && schema.is_some()
            {
                continue;
            }
            if required_string(&batch, "TABLE_NAME", row)? != table {
                continue;
            }
            let name = required_string(&batch, "COLUMN_NAME", row)?;
            let type_name = required_string(&batch, "TYPE_NAME", row)?;
            let normalized_type = type_name.to_ascii_uppercase();
            has_geospatial_type |=
                normalized_type.contains("GEOMETRY") || normalized_type.contains("GEOGRAPHY");
            let nullable = !matches!(
                optional_string(&batch, "IS_NULLABLE", row)?.as_deref(),
                Some("NO")
            );
            let field = Field::new(
                name,
                kernel::metadata::type_mapping::databricks_type_to_arrow(&type_name),
                nullable,
            );
            fields.push((optional_i32(&batch, "ORDINAL_POSITION", row)?, field));
        }
    }
    fields.sort_by_key(|(ordinal, _)| *ordinal);
    if fields.is_empty() {
        return Err(adbc_error(
            format!("table not found: {table}"),
            Status::NotFound,
        ));
    }
    if has_geospatial_type {
        return query_table_schema(session, runtime, catalog, schema, table);
    }
    Ok(Schema::new(
        fields
            .into_iter()
            .map(|(_, field)| field)
            .collect::<Vec<_>>(),
    ))
}

fn query_table_schema(
    session: &kernel::Session,
    runtime: &tokio::runtime::Runtime,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
) -> Result<Schema> {
    fn quoted(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    let qualified = catalog
        .into_iter()
        .chain(schema)
        .chain(std::iter::once(table))
        .map(quoted)
        .collect::<Vec<_>>()
        .join(".");
    let mut statement = session.statement();
    statement
        .spec()
        .sql(format!("SELECT * FROM {qualified} WHERE 1 = 0"));
    let mut executed = runtime
        .block_on_safe(statement.execute())
        .map_err(map_kernel_error)?;
    let result_schema = executed.schema();
    while runtime
        .block_on_safe(executed.next_batch())
        .map_err(map_kernel_error)?
        .is_some()
    {}
    Ok(result_schema.as_ref().clone())
}

fn parse_tables(batches: Vec<RecordBatch>) -> Result<Vec<TableRow>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(TableRow {
                catalog: required_string(&batch, "TABLE_CAT", row)?,
                schema: required_string(&batch, "TABLE_SCHEM", row)?,
                name: required_string(&batch, "TABLE_NAME", row)?,
                table_type: required_string(&batch, "TABLE_TYPE", row)?,
            });
        }
    }
    Ok(rows)
}

fn normalize_objects_column_metadata(
    data_type: Option<i16>,
    type_name: Option<String>,
    sql_data_type: Option<i16>,
) -> (Option<String>, Option<i16>) {
    let type_name = type_name.map(|type_name| {
        if type_name.eq_ignore_ascii_case("INT") {
            "INTEGER".to_owned()
        } else {
            type_name
        }
    });
    (type_name, sql_data_type.or(data_type))
}

fn parse_columns(batches: Vec<RecordBatch>) -> Result<Vec<ColumnRow>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let data_type = optional_i16(&batch, "DATA_TYPE", row)?;
            let (type_name, sql_data_type) = normalize_objects_column_metadata(
                data_type,
                optional_string(&batch, "TYPE_NAME", row)?,
                optional_i16(&batch, "SQL_DATA_TYPE", row)?,
            );
            rows.push(ColumnRow {
                catalog: required_string(&batch, "TABLE_CAT", row)?,
                schema: required_string(&batch, "TABLE_SCHEM", row)?,
                table: required_string(&batch, "TABLE_NAME", row)?,
                name: required_string(&batch, "COLUMN_NAME", row)?,
                ordinal: optional_i32(&batch, "ORDINAL_POSITION", row)?,
                remarks: optional_string(&batch, "REMARKS", row)?,
                data_type,
                type_name,
                column_size: optional_i32(&batch, "COLUMN_SIZE", row)?,
                decimal_digits: optional_i16(&batch, "DECIMAL_DIGITS", row)?,
                radix: optional_i16(&batch, "NUM_PREC_RADIX", row)?,
                nullable: optional_i16(&batch, "NULLABLE", row)?,
                column_default: optional_string(&batch, "COLUMN_DEF", row)?,
                sql_data_type,
                datetime_sub: optional_i16(&batch, "SQL_DATETIME_SUB", row)?,
                char_octet_length: optional_i32(&batch, "CHAR_OCTET_LENGTH", row)?,
                is_nullable: optional_string(&batch, "IS_NULLABLE", row)?,
                scope_catalog: optional_string(&batch, "SCOPE_CATALOG", row)?,
                scope_schema: optional_string(&batch, "SCOPE_SCHEMA", row)?,
                scope_table: optional_string(&batch, "SCOPE_TABLE", row)?,
                is_auto_increment: optional_yes_no(&batch, "IS_AUTO_INCREMENT", row)?,
            });
        }
    }
    Ok(rows)
}

fn like_match(pattern: &str, value: &str) -> bool {
    fn inner(pattern: &[char], value: &[char]) -> bool {
        match pattern {
            [] => value.is_empty(),
            ['\\', literal, rest @ ..] => {
                !value.is_empty() && value[0] == *literal && inner(rest, &value[1..])
            }
            ['%', rest @ ..] => {
                inner(rest, value) || (!value.is_empty() && inner(pattern, &value[1..]))
            }
            ['_', rest @ ..] => !value.is_empty() && inner(rest, &value[1..]),
            [literal, rest @ ..] => {
                !value.is_empty() && value[0] == *literal && inner(rest, &value[1..])
            }
        }
    }
    inner(
        &pattern.chars().collect::<Vec<_>>(),
        &value.chars().collect::<Vec<_>>(),
    )
}

fn struct_fields(data_type: &DataType) -> Fields {
    match data_type {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    }
}

fn build_columns(tables: &[TableNode]) -> Result<ArrayRef> {
    let columns = tables
        .iter()
        .flat_map(|table| table.columns.iter().flatten())
        .cloned()
        .collect::<Vec<_>>();
    let optional_strings = |get: fn(&ColumnRow) -> Option<String>| {
        Arc::new(StringArray::from(
            columns.iter().map(get).collect::<Vec<_>>(),
        )) as ArrayRef
    };
    let values = vec![
        Arc::new(StringArray::from(
            columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int32Array::from(
            columns.iter().map(|c| c.ordinal).collect::<Vec<_>>(),
        )) as ArrayRef,
        optional_strings(|c| c.remarks.clone()),
        Arc::new(Int16Array::from(
            columns.iter().map(|c| c.data_type).collect::<Vec<_>>(),
        )) as ArrayRef,
        optional_strings(|c| c.type_name.clone()),
        Arc::new(Int32Array::from(
            columns.iter().map(|c| c.column_size).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int16Array::from(
            columns.iter().map(|c| c.decimal_digits).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int16Array::from(
            columns.iter().map(|c| c.radix).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int16Array::from(
            columns.iter().map(|c| c.nullable).collect::<Vec<_>>(),
        )) as ArrayRef,
        optional_strings(|c| c.column_default.clone()),
        Arc::new(Int16Array::from(
            columns.iter().map(|c| c.sql_data_type).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int16Array::from(
            columns.iter().map(|c| c.datetime_sub).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int32Array::from(
            columns
                .iter()
                .map(|c| c.char_octet_length)
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        optional_strings(|c| c.is_nullable.clone()),
        optional_strings(|c| c.scope_catalog.clone()),
        optional_strings(|c| c.scope_schema.clone()),
        optional_strings(|c| c.scope_table.clone()),
        Arc::new(BooleanArray::from(
            columns
                .iter()
                .map(|c| c.is_auto_increment)
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(BooleanArray::from(vec![None; columns.len()])) as ArrayRef,
    ];
    Ok(Arc::new(StructArray::new(
        struct_fields(&COLUMN_SCHEMA),
        values,
        None,
    )))
}

fn build_objects(catalogs: Vec<CatalogNode>) -> Result<Reader> {
    let catalog_names = Arc::new(StringArray::from(
        catalogs.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
    )) as ArrayRef;
    let schemas = catalogs
        .iter()
        .flat_map(|c| c.schemas.iter().flatten())
        .cloned()
        .collect::<Vec<_>>();
    let schema_names = Arc::new(StringArray::from(
        schemas.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
    )) as ArrayRef;
    let tables = schemas
        .iter()
        .flat_map(|s| s.tables.iter().flatten())
        .cloned()
        .collect::<Vec<_>>();
    let table_names = Arc::new(StringArray::from(
        tables
            .iter()
            .map(|t| t.row.name.as_str())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let table_types = Arc::new(StringArray::from(
        tables
            .iter()
            .map(|t| t.row.table_type.as_str())
            .collect::<Vec<_>>(),
    )) as ArrayRef;

    let column_values = build_columns(&tables)?;
    let column_offsets = OffsetBuffer::from_lengths(
        tables
            .iter()
            .map(|t| t.columns.as_ref().map_or(0, Vec::len)),
    );
    let column_nulls = NullBuffer::from(
        tables
            .iter()
            .map(|t| t.columns.is_some())
            .collect::<Vec<_>>(),
    );
    let column_lists = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", COLUMN_SCHEMA.clone(), true)),
            column_offsets,
            column_values,
            Some(column_nulls),
        )
        .map_err(|e| invalid(e.to_string()))?,
    ) as ArrayRef;
    // Match the established ADBC driver behavior: GetObjects exposes the
    // constraint field but leaves it null. Applications retrieve keys through
    // the dedicated primary-key and cross-reference metadata operations.
    let constraint_lists =
        new_null_array(struct_fields(&TABLE_SCHEMA)[3].data_type(), tables.len());
    let table_struct = Arc::new(StructArray::new(
        struct_fields(&TABLE_SCHEMA),
        vec![table_names, table_types, column_lists, constraint_lists],
        None,
    )) as ArrayRef;
    let table_offsets = OffsetBuffer::from_lengths(
        schemas
            .iter()
            .map(|s| s.tables.as_ref().map_or(0, Vec::len)),
    );
    let table_nulls = NullBuffer::from(
        schemas
            .iter()
            .map(|s| s.tables.is_some())
            .collect::<Vec<_>>(),
    );
    let table_lists = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true)),
            table_offsets,
            table_struct,
            Some(table_nulls),
        )
        .map_err(|e| invalid(e.to_string()))?,
    ) as ArrayRef;
    let schema_struct = Arc::new(StructArray::new(
        struct_fields(&OBJECTS_DB_SCHEMA_SCHEMA),
        vec![schema_names, table_lists],
        None,
    )) as ArrayRef;
    let schema_offsets = OffsetBuffer::from_lengths(
        catalogs
            .iter()
            .map(|c| c.schemas.as_ref().map_or(0, Vec::len)),
    );
    let schema_nulls = NullBuffer::from(
        catalogs
            .iter()
            .map(|c| c.schemas.is_some())
            .collect::<Vec<_>>(),
    );
    let schema_lists = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", OBJECTS_DB_SCHEMA_SCHEMA.clone(), true)),
            schema_offsets,
            schema_struct,
            Some(schema_nulls),
        )
        .map_err(|e| invalid(e.to_string()))?,
    ) as ArrayRef;
    let batch = RecordBatch::try_new(
        GET_OBJECTS_SCHEMA.clone(),
        vec![catalog_names, schema_lists],
    )?;
    Ok(reader(GET_OBJECTS_SCHEMA.clone(), batch))
}

#[allow(clippy::too_many_arguments)] // Mirrors the ADBC GetObjects filter surface plus kernel context.
pub(crate) fn get_objects(
    session: &kernel::Session,
    runtime: &tokio::runtime::Runtime,
    depth: ObjectDepth,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: Option<&str>,
    table_types: Option<Vec<&str>>,
    column_name: Option<&str>,
    escape_catalog_name: bool,
) -> Result<Reader> {
    // Databricks identifiers are case-insensitive. Lowercasing search patterns
    // before sending metadata requests matches the established connector
    // contract while preserving SQL-LIKE wildcards and escapes.
    let catalog = escape_catalog_underscores(catalog, escape_catalog_name)
        .map(|catalog| catalog.to_lowercase());
    let schema = schema.map(str::to_lowercase);
    let table = table.map(str::to_lowercase);
    let column_name = column_name.map(str::to_lowercase);
    let catalog = catalog.as_deref();
    let schema = schema.as_deref();
    let table = table.as_deref();
    let column_name = column_name.as_deref();
    let metadata = session.metadata();
    if depth == ObjectDepth::Catalogs {
        let stream = runtime
            .block_on_safe(metadata.list_catalogs())
            .map_err(map_kernel_error)?;
        let mut catalogs = Vec::new();
        for batch in drain(runtime, stream)? {
            for row in 0..batch.num_rows() {
                let name = required_string(&batch, "TABLE_CAT", row)?;
                if catalog.is_none_or(|pattern| like_match(pattern, &name)) {
                    catalogs.push(CatalogNode {
                        name,
                        schemas: None,
                    });
                }
            }
        }
        return build_objects(catalogs);
    }

    if depth == ObjectDepth::Schemas {
        let catalogs = catalog_schema_hierarchy(&metadata, runtime, catalog, schema)?
            .into_iter()
            .map(|(name, schemas)| CatalogNode {
                name,
                schemas: Some(
                    schemas
                        .into_iter()
                        .map(|name| SchemaNode { name, tables: None })
                        .collect(),
                ),
            })
            .collect();
        return build_objects(catalogs);
    }

    let hierarchy = catalog_schema_hierarchy(&metadata, runtime, catalog, schema)?;

    let table_stream = runtime
        .block_on_safe(metadata.list_tables(
            pattern(catalog)?,
            pattern(schema)?,
            pattern(table)?,
            table_types.as_deref(),
        ))
        .map_err(map_kernel_error)?;
    let tables = parse_tables(drain(runtime, table_stream)?)?;
    let table_keys = tables
        .iter()
        .map(|t| (t.catalog.clone(), t.schema.clone(), t.name.clone()))
        .collect::<BTreeSet<_>>();
    let mut columns_by_table: BTreeMap<(String, String, String), Vec<ColumnRow>> = BTreeMap::new();
    if matches!(depth, ObjectDepth::All | ObjectDepth::Columns) {
        for catalog_name in tables
            .iter()
            .map(|t| t.catalog.clone())
            .collect::<BTreeSet<_>>()
        {
            let stream = runtime
                .block_on_safe(metadata.list_columns(
                    Some(kernel::Identifier::new(&catalog_name).map_err(map_kernel_error)?),
                    pattern(schema)?,
                    pattern(table)?,
                    pattern(column_name)?,
                ))
                .map_err(map_kernel_error)?;
            for column in parse_columns(drain(runtime, stream)?)? {
                let key = (
                    column.catalog.clone(),
                    column.schema.clone(),
                    column.table.clone(),
                );
                if table_keys.contains(&key) {
                    columns_by_table.entry(key).or_default().push(column);
                }
            }
        }
        for columns in columns_by_table.values_mut() {
            columns.sort_by_key(|c| c.ordinal);
        }
    }

    let include_children = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
    let mut grouped = initialize_table_hierarchy(hierarchy);
    for table_row in tables {
        let key = (
            table_row.catalog.clone(),
            table_row.schema.clone(),
            table_row.name.clone(),
        );
        grouped
            .entry(table_row.catalog.clone())
            .or_default()
            .entry(table_row.schema.clone())
            .or_default()
            .push(TableNode {
                row: table_row,
                columns: include_children
                    .then(|| columns_by_table.remove(&key).unwrap_or_default()),
            });
    }
    let catalogs = grouped
        .into_iter()
        .map(|(name, schemas)| CatalogNode {
            name,
            schemas: Some(
                schemas
                    .into_iter()
                    .map(|(name, tables)| SchemaNode {
                        name,
                        tables: Some(tables),
                    })
                    .collect(),
            ),
        })
        .collect();
    build_objects(catalogs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_uses_exact_adbc_schema_and_filters() {
        let rows = vec![
            (
                u32::from(&InfoCode::DriverName),
                InfoValue::String(Some("driver".into())),
            ),
            (u32::from(&InfoCode::VendorSql), InfoValue::Bool(true)),
        ];
        let mut reader = build_info(rows).unwrap();
        assert_eq!(reader.schema(), GET_INFO_SCHEMA.clone());
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn current_version_accepts_string_and_struct_results() {
        let json = StringArray::from(vec![Some(r#"{"dbsql_version":"2026.40"}"#)]);
        assert_eq!(current_version_value(&json).unwrap(), json.value(0));

        let fields = Fields::from(vec![
            Field::new("dbr_version", DataType::Utf8, true),
            Field::new("dbsql_version", DataType::Utf8, true),
        ]);
        let values = StructArray::new(
            fields,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("2026.40")])) as ArrayRef,
            ],
            None,
        );
        assert_eq!(
            current_version_value(&values).unwrap(),
            r#"{"dbsql_version":"2026.40"}"#
        );
    }

    #[test]
    fn like_patterns_support_wildcards_and_escapes() {
        assert!(like_match("m%", "main"));
        assert!(like_match("m_in", "main"));
        assert!(like_match(r"a\_b", "a_b"));
        assert!(!like_match(r"a\_b", "axb"));
    }

    #[test]
    fn table_hierarchy_preserves_matching_parents_without_deeper_matches() {
        let hierarchy = BTreeMap::from([
            ("catalog_with_no_schema_match".into(), BTreeSet::new()),
            (
                "catalog_with_schema_match".into(),
                BTreeSet::from(["schema_with_no_table_match".into()]),
            ),
        ]);

        let grouped = initialize_table_hierarchy(hierarchy);
        assert!(grouped["catalog_with_no_schema_match"].is_empty());
        assert!(grouped["catalog_with_schema_match"]["schema_with_no_table_match"].is_empty());
    }

    #[test]
    fn metadata_statement_filters_match_csharp_contract() {
        for (input, expected) in [
            ("a_b%", r"a\_b\%"),
            (r"a\b", r"a\\b"),
            (r"a\_b", r"a\\\_b"),
            (r"a\\b", r"a\\\\b"),
        ] {
            assert_eq!(escaped_pattern(Some(input), false).as_deref(), Some(input));
            assert_eq!(
                escaped_pattern(Some(input), true).as_deref(),
                Some(expected)
            );
            assert!(like_match(expected, input));
        }
        assert!(!like_match(r"a\_b\%", "axb-suffix"));
        assert!(!like_match(r"a\\b", "axb"));
        assert_eq!(table_type_filter(None), None);
        assert_eq!(table_type_filter(Some("")), None);
        assert_eq!(
            table_type_filter(Some("TABLE,VIEW")),
            Some(vec!["TABLE", "VIEW"])
        );
    }

    #[test]
    fn catalog_underscore_escape_preserves_existing_like_intent() {
        for (input, expected) in [
            ("catalog_name", r"catalog\_name"),
            (r"catalog\_name", r"catalog\_name"),
            ("catalog%name", "catalog%name"),
            (r"catalog\name", r"catalog\name"),
            (r"catalog\\_name", r"catalog\\\_name"),
            (r"catalog\\\_name", r"catalog\\\_name"),
        ] {
            assert_eq!(
                escape_catalog_underscores(Some(input), true).as_deref(),
                Some(expected),
                "input {input:?}"
            );
            assert_eq!(
                escape_catalog_underscores(Some(input), false).as_deref(),
                Some(input),
                "disabled input {input:?}"
            );
        }
        assert_eq!(escape_catalog_underscores(None, true), None);
    }

    #[test]
    fn csharp_key_metadata_uses_legacy_sequence_spelling() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("TABLE_NAME", DataType::Utf8, true),
            Field::new("KEY_SEQ", DataType::Int32, true),
        ]));
        let schema = csharp_key_schema(&schema);
        assert_eq!(schema.field(0).name(), "TABLE_NAME");
        assert_eq!(schema.field(1).name(), "KEQ_SEQ");
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);
    }

    #[test]
    fn objects_columns_match_csharp_type_metadata() {
        assert_eq!(
            normalize_objects_column_metadata(Some(4), Some("INT".into()), None),
            (Some("INTEGER".into()), Some(4))
        );
        assert_eq!(
            normalize_objects_column_metadata(Some(12), Some("STRING".into()), None),
            (Some("STRING".into()), Some(12))
        );
        assert_eq!(
            normalize_objects_column_metadata(Some(3), Some("DECIMAL(10,2)".into()), Some(3)),
            (Some("DECIMAL(10,2)".into()), Some(3))
        );
    }

    #[test]
    fn get_columns_extension_appends_canonical_base_type_names() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("TYPE_NAME", DataType::Utf8, false),
            Field::new("COLUMN_SIZE", DataType::Int32, true),
            Field::new("DECIMAL_DIGITS", DataType::Int32, true),
            Field::new("ORDINAL_POSITION", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "INT",
                    "STRING",
                    "DECIMAL(10,2)",
                    "CHAR",
                    "VARCHAR(20)",
                    "TIMESTAMP_NTZ",
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(4),
                    None,
                    None,
                    None,
                    None,
                    None,
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(0),
                    None,
                    None,
                    None,
                    None,
                    None,
                ])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            ],
        )
        .unwrap();
        let mut fields = source_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new("BASE_TYPE_NAME", DataType::Utf8, true)));
        let output_schema = Arc::new(Schema::new(fields));
        let output = append_base_type(batch, output_schema).unwrap();
        let values = output
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>(),
            vec![
                "INTEGER",
                "STRING",
                "DECIMAL",
                "CHAR",
                "VARCHAR",
                "TIMESTAMP",
            ]
        );
        assert_eq!(
            output
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![
                Some(4),
                Some(i32::MAX),
                Some(10),
                Some(255),
                Some(20),
                Some(0)
            ]
        );
        assert_eq!(
            output
                .column(2)
                .as_primitive::<arrow_array::types::Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), Some(0), Some(2), Some(0), Some(0), Some(0)]
        );
        assert_eq!(
            output
                .column(3)
                .as_primitive::<arrow_array::types::Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]
        );
    }

    #[test]
    fn get_schemas_extension_appends_global_temp_once() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("TABLE_SCHEM", DataType::Utf8, false),
            Field::new("TABLE_CATALOG", DataType::Utf8, false),
        ]));
        let mut batches = vec![RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["default"])),
                Arc::new(StringArray::from(vec!["main"])),
            ],
        )
        .unwrap()];
        append_global_temp_schema(&mut batches, &schema).unwrap();
        append_global_temp_schema(&mut batches, &schema).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            required_string(&batches[1], "TABLE_SCHEM", 0).unwrap(),
            "global_temp"
        );
        assert_eq!(
            required_string(&batches[1], "TABLE_CATALOG", 0).unwrap(),
            ""
        );
    }

    #[test]
    fn every_objects_depth_has_exact_schema() {
        for catalogs in [
            vec![CatalogNode {
                name: "main".into(),
                schemas: None,
            }],
            vec![CatalogNode {
                name: "main".into(),
                schemas: Some(vec![SchemaNode {
                    name: "default".into(),
                    tables: None,
                }]),
            }],
        ] {
            let reader = build_objects(catalogs).unwrap();
            assert_eq!(reader.schema(), GET_OBJECTS_SCHEMA.clone());
        }
    }

    #[test]
    fn objects_depth_distinguishes_null_from_empty_lists() {
        let table = TableNode {
            row: TableRow {
                catalog: "main".into(),
                schema: "default".into(),
                name: "t".into(),
                table_type: "TABLE".into(),
            },
            columns: Some(Vec::new()),
        };
        let cases = [
            (None, None),
            (Some(None), None),
            (Some(Some(None)), None),
            (Some(Some(Some(table))), Some(true)),
        ];
        for (hierarchy, columns_valid) in cases {
            let schemas = hierarchy.clone().map(|tables| {
                vec![SchemaNode {
                    name: "default".into(),
                    tables: tables.map(|table| table.into_iter().collect()),
                }]
            });
            let mut output = build_objects(vec![CatalogNode {
                name: "main".into(),
                schemas,
            }])
            .unwrap();
            let batch = output.next().unwrap().unwrap();
            let schema_lists = batch.column(1).as_list::<i32>();
            assert_eq!(schema_lists.is_valid(0), hierarchy.is_some());
            if let Some(tables) = hierarchy {
                let schema_structs = schema_lists.values().as_struct();
                let table_lists = schema_structs.column(1).as_list::<i32>();
                assert_eq!(table_lists.is_valid(0), tables.is_some());
                if tables.flatten().is_some() {
                    let table_structs = table_lists.values().as_struct();
                    assert_eq!(table_structs.column(2).is_valid(0), columns_valid.unwrap());
                    assert!(table_structs.column(3).is_null(0));
                }
            }
        }
    }

    #[test]
    fn objects_zero_matches_is_an_empty_batch_with_exact_schema() {
        let mut output = build_objects(Vec::new()).unwrap();
        let batch = output.next().unwrap().unwrap();
        assert_eq!(batch.schema(), GET_OBJECTS_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn objects_leave_constraints_null_for_every_table() {
        let tables = ["parent", "child"]
            .into_iter()
            .map(|name| TableNode {
                row: TableRow {
                    catalog: "main".into(),
                    schema: "default".into(),
                    name: name.into(),
                    table_type: "TABLE".into(),
                },
                columns: Some(Vec::new()),
            })
            .collect();
        let mut output = build_objects(vec![CatalogNode {
            name: "main".into(),
            schemas: Some(vec![SchemaNode {
                name: "default".into(),
                tables: Some(tables),
            }]),
        }])
        .unwrap();
        let batch = output.next().unwrap().unwrap();
        let schema_lists = batch.column(1).as_list::<i32>();
        let schema_structs = schema_lists.values().as_struct();
        let table_lists = schema_structs.column(1).as_list::<i32>();
        let table_structs = table_lists.values().as_struct();
        let constraints = table_structs.column(3);
        assert_eq!(constraints.len(), 2);
        assert_eq!(constraints.null_count(), 2);
    }
}
