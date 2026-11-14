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

//! Small results as Python rows, without pyarrow.
//!
//! Handing a one-row result to Python as a `pyarrow.RecordBatch` costs far
//! more than the row itself: the C data interface export and import of the
//! batch and its schema, then `to_pylist()` per column. `rows_to_py` builds
//! the rows' Python values directly, matching what pyarrow's `to_pylist()`
//! returns for each supported type, and declines (`None`) everything else so
//! callers fall back to the Arrow path.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampSecondType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrowPrimitiveType, PrimitiveArray, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};
use chrono::{Datelike, NaiveDate};
use pyo3::exceptions::{PyImportError, PyKeyError, PyOverflowError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyList, PyString, PyTuple};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

static DECIMAL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DATE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DATETIME: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static UTC: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static STRING_TO_TZINFO: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Days from 0001-01-01 (day 1 of the proleptic Gregorian calendar) to
/// 1970-01-01.
const UNIX_EPOCH_DAYS_FROM_CE: i64 = 719_163;
const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

fn cached<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyAny>>,
    module: &str,
    path: &[&str],
) -> PyResult<Bound<'py, PyAny>> {
    cell.get_or_try_init(py, || {
        let mut value = py.import(module)?.into_any();
        for name in path {
            value = value.getattr(*name)?;
        }
        Ok::<_, PyErr>(value.unbind())
    })
    .map(|value| value.bind(py).clone())
}

/// Whether `rows_to_py` converts columns of this type.
pub(crate) fn is_supported(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32 => true,
        // pyarrow returns pandas Timestamps for nanoseconds when pandas is
        // installed.
        DataType::Timestamp(unit, _) => *unit != TimeUnit::Nanosecond,
        _ => false,
    }
}

pub(crate) fn schema_is_supported(schema: &Schema) -> bool {
    schema.fields().iter().all(|f| is_supported(f.data_type()))
}

/// `fetch_all_rows`'s `max_rows`, rejecting negatives as `KernelError`
/// (a `usize` parameter would raise `OverflowError` instead).
pub(crate) fn max_rows(py: Python<'_>, max_rows: i64) -> PyResult<usize> {
    usize::try_from(max_rows).map_err(|_| {
        crate::errors::to_py_err(
            py,
            databricks_sql_kernel::Error::invalid_argument().message("max_rows must be >= 0"),
        )
    })
}

pub(crate) fn pending_rows(pending: &VecDeque<RecordBatch>) -> usize {
    pending.iter().map(RecordBatch::num_rows).sum()
}

/// The rows in `pending` if the stream `ended` with at most `max_rows` of
/// them and they convert, clearing `pending`; otherwise `None`, leaving
/// `pending` for the Arrow fetches.
pub(crate) fn take_rows<'py>(
    py: Python<'py>,
    schema: &Schema,
    ended: bool,
    pending: &mut VecDeque<RecordBatch>,
    max_rows: usize,
    nan_ok: bool,
) -> PyResult<Option<Bound<'py, PyList>>> {
    if !ended || pending_rows(pending) > max_rows {
        return Ok(None);
    }
    let rows = rows_to_py(py, schema, pending.make_contiguous(), nan_ok)?;
    if rows.is_some() {
        pending.clear();
    }
    Ok(rows)
}

/// The rows of `batches` as a list of tuples, or `None` if a column can't be
/// converted here: an unsupported type, a batch whose column types differ
/// from `schema`'s, a value that Python's `date`/`datetime` can't hold, or
/// (unless `nan_ok`) a NaN float, whose conversion depends on the caller.
pub(crate) fn rows_to_py<'py>(
    py: Python<'py>,
    schema: &Schema,
    batches: &[RecordBatch],
    nan_ok: bool,
) -> PyResult<Option<Bound<'py, PyList>>> {
    let num_columns = schema.fields().len();
    let num_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut columns: Vec<Vec<Bound<'py, PyAny>>> = (0..num_columns)
        .map(|_| Vec::with_capacity(num_rows))
        .collect();
    let mut converter = Converter::new(py, nan_ok);
    for batch in batches {
        if batch.num_columns() != num_columns {
            return Ok(None);
        }
        for ((field, array), out) in schema
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(columns.iter_mut())
        {
            if array.data_type() != field.data_type() || !converter.convert(array.as_ref(), out)? {
                return Ok(None);
            }
        }
    }
    let mut values: Vec<_> = columns.into_iter().map(Vec::into_iter).collect();
    let rows = (0..num_rows)
        .map(|_| {
            PyTuple::new(
                py,
                values
                    .iter_mut()
                    .map(|column| column.next().expect("one value per row")),
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(Some(PyList::new(py, rows)?))
}

macro_rules! ints {
    ($self:expr, $array:expr, $out:expr, $type:ty) => {{
        let py = $self.py;
        let a = $array.as_primitive::<$type>();
        $self.each(a, $out, |i| {
            Ok(Some(a.value(i).into_pyobject(py)?.into_any()))
        })
    }};
}

struct Converter<'py> {
    py: Python<'py>,
    nan_ok: bool,
    none: Bound<'py, PyAny>,
    /// Zones resolved so far, by name.
    zones: HashMap<String, Bound<'py, PyAny>>,
}

/// Python `date` fields for days since the Unix epoch, if in `date`'s range
/// (years 1 through 9999).
fn ymd(days: i64) -> Option<(i32, u32, u32)> {
    let days = i32::try_from(days.checked_add(UNIX_EPOCH_DAYS_FROM_CE)?).ok()?;
    let date = NaiveDate::from_num_days_from_ce_opt(days)?;
    (1..=9999)
        .contains(&date.year())
        .then(|| (date.year(), date.month(), date.day()))
}

impl<'py> Converter<'py> {
    fn new(py: Python<'py>, nan_ok: bool) -> Self {
        Self {
            py,
            nan_ok,
            none: py.None().into_bound(py),
            zones: HashMap::new(),
        }
    }

    /// Append the Python values of `array` to `out`; `false` if it can't be
    /// converted here.
    fn convert(&mut self, array: &dyn Array, out: &mut Vec<Bound<'py, PyAny>>) -> PyResult<bool> {
        let py = self.py;
        match array.data_type() {
            // A NullArray has no validity buffer; every slot is null.
            DataType::Null => {
                out.extend((0..array.len()).map(|_| self.none.clone()));
                Ok(true)
            }
            DataType::Boolean => {
                let a = array.as_boolean();
                self.each(a, out, |i| {
                    Ok(Some(PyBool::new(py, a.value(i)).to_owned().into_any()))
                })
            }
            DataType::Int8 => ints!(self, array, out, Int8Type),
            DataType::Int16 => ints!(self, array, out, Int16Type),
            DataType::Int32 => ints!(self, array, out, Int32Type),
            DataType::Int64 => ints!(self, array, out, Int64Type),
            DataType::UInt8 => ints!(self, array, out, UInt8Type),
            DataType::UInt16 => ints!(self, array, out, UInt16Type),
            DataType::UInt32 => ints!(self, array, out, UInt32Type),
            DataType::UInt64 => ints!(self, array, out, UInt64Type),
            DataType::Float32 => {
                let a = array.as_primitive::<Float32Type>();
                self.floats(a, out, |i| f64::from(a.value(i)))
            }
            DataType::Float64 => {
                let a = array.as_primitive::<Float64Type>();
                self.floats(a, out, |i| a.value(i))
            }
            DataType::Utf8 => {
                let a = array.as_string::<i32>();
                self.each(a, out, |i| {
                    Ok(Some(PyString::new(py, a.value(i)).into_any()))
                })
            }
            DataType::LargeUtf8 => {
                let a = array.as_string::<i64>();
                self.each(a, out, |i| {
                    Ok(Some(PyString::new(py, a.value(i)).into_any()))
                })
            }
            DataType::Binary => {
                let a = array.as_binary::<i32>();
                self.each(a, out, |i| {
                    Ok(Some(PyBytes::new(py, a.value(i)).into_any()))
                })
            }
            DataType::LargeBinary => {
                let a = array.as_binary::<i64>();
                self.each(a, out, |i| {
                    Ok(Some(PyBytes::new(py, a.value(i)).into_any()))
                })
            }
            DataType::FixedSizeBinary(_) => {
                let a = array.as_fixed_size_binary();
                self.each(a, out, |i| {
                    Ok(Some(PyBytes::new(py, a.value(i)).into_any()))
                })
            }
            DataType::Decimal128(_, scale) => {
                let a = array.as_primitive::<Decimal128Type>();
                self.decimals(a, *scale, out)
            }
            DataType::Decimal256(_, scale) => {
                let a = array.as_primitive::<Decimal256Type>();
                self.decimals(a, *scale, out)
            }
            DataType::Date32 => {
                let a = array.as_primitive::<Date32Type>();
                let date = cached(py, &DATE, "datetime", &["date"])?;
                self.each(a, out, |i| {
                    let Some((y, m, d)) = ymd(i64::from(a.value(i))) else {
                        return Ok(None);
                    };
                    Ok(Some(date.call1((y, m, d))?))
                })
            }
            DataType::Timestamp(unit, tz) => {
                let (values, micros_per_unit): (&[i64], i64) = match unit {
                    TimeUnit::Second => (
                        array.as_primitive::<TimestampSecondType>().values(),
                        MICROS_PER_SECOND,
                    ),
                    TimeUnit::Millisecond => (
                        array.as_primitive::<TimestampMillisecondType>().values(),
                        1_000,
                    ),
                    TimeUnit::Microsecond => {
                        (array.as_primitive::<TimestampMicrosecondType>().values(), 1)
                    }
                    TimeUnit::Nanosecond => return Ok(false),
                };
                self.timestamps(array, values, micros_per_unit, tz.as_deref(), out)
            }
            _ => Ok(false),
        }
    }

    /// Append `value(i)` for each valid slot of `array` and `None` for each
    /// null; `false` (and stop) as soon as `value` declines a slot.
    fn each(
        &self,
        array: &dyn Array,
        out: &mut Vec<Bound<'py, PyAny>>,
        mut value: impl FnMut(usize) -> PyResult<Option<Bound<'py, PyAny>>>,
    ) -> PyResult<bool> {
        for i in 0..array.len() {
            if array.is_null(i) {
                out.push(self.none.clone());
                continue;
            }
            match value(i) {
                Ok(Some(v)) => out.push(v),
                Ok(None) => return Ok(false),
                Err(e) if out_of_range(self.py, &e) => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }

    fn floats<T: ArrowPrimitiveType>(
        &self,
        array: &PrimitiveArray<T>,
        out: &mut Vec<Bound<'py, PyAny>>,
        value: impl Fn(usize) -> f64,
    ) -> PyResult<bool> {
        let py = self.py;
        let nan_ok = self.nan_ok;
        self.each(array, out, |i| {
            let v = value(i);
            if v.is_nan() && !nan_ok {
                return Ok(None);
            }
            Ok(Some(PyFloat::new(py, v).into_any()))
        })
    }

    /// `decimal.Decimal` from `<unscaled>E<-scale>`, which carries the same
    /// digits and exponent as the string pyarrow builds it from.
    fn decimals<T>(
        &self,
        array: &PrimitiveArray<T>,
        scale: i8,
        out: &mut Vec<Bound<'py, PyAny>>,
    ) -> PyResult<bool>
    where
        T: ArrowPrimitiveType,
        T::Native: std::fmt::Display,
    {
        let decimal = cached(self.py, &DECIMAL, "decimal", &["Decimal"])?;
        let exponent = -i32::from(scale);
        self.each(array, out, |i| {
            Ok(Some(decimal.call1((format!(
                "{}E{}",
                array.value(i),
                exponent
            ),))?))
        })
    }

    /// The tzinfo pyarrow's `string_to_tzinfo` returns for `name`, looked up
    /// once per call. `None` (declined) without pyarrow, or for a zone it
    /// doesn't know (KeyError from zoneinfo and pytz) or can't parse
    /// (ValueError).
    fn zone(&mut self, name: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
        let py = self.py;
        if let Some(zone) = self.zones.get(name) {
            return Ok(Some(zone.clone()));
        }
        let to_tzinfo = match cached(py, &STRING_TO_TZINFO, "pyarrow.lib", &["string_to_tzinfo"]) {
            Ok(f) => f,
            Err(e) if e.is_instance_of::<PyImportError>(py) => return Ok(None),
            Err(e) => return Err(e),
        };
        match to_tzinfo.call1((name,)) {
            Ok(zone) => {
                self.zones.insert(name.to_owned(), zone.clone());
                Ok(Some(zone))
            }
            Err(e)
                if e.is_instance_of::<PyKeyError>(py) || e.is_instance_of::<PyValueError>(py) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Like pyarrow: a naive `datetime` for a timestamp without a time zone;
    /// otherwise the UTC instant converted with `astimezone` to the zone
    /// pyarrow's `string_to_tzinfo` returns for its name.
    fn timestamps(
        &mut self,
        array: &dyn Array,
        values: &[i64],
        micros_per_unit: i64,
        tz: Option<&str>,
        out: &mut Vec<Bound<'py, PyAny>>,
    ) -> PyResult<bool> {
        let py = self.py;
        let datetime = cached(py, &DATETIME, "datetime", &["datetime"])?;
        let zone = match tz {
            None => None,
            Some(name) => match self.zone(name)? {
                Some(zone) => Some((zone, cached(py, &UTC, "datetime", &["timezone", "utc"])?)),
                None => return Ok(false),
            },
        };
        for (i, &value) in values.iter().enumerate() {
            if array.is_null(i) {
                out.push(self.none.clone());
                continue;
            }
            let Some(micros) = value.checked_mul(micros_per_unit) else {
                return Ok(false);
            };
            let seconds = micros.div_euclid(MICROS_PER_SECOND);
            let micro = micros.rem_euclid(MICROS_PER_SECOND);
            let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
            let Some((y, mo, d)) = ymd(seconds.div_euclid(SECONDS_PER_DAY)) else {
                return Ok(false);
            };
            let (h, mi, s) = (
                second_of_day / 3600,
                second_of_day % 3600 / 60,
                second_of_day % 60,
            );
            let converted = match &zone {
                None => datetime.call1((y, mo, d, h, mi, s, micro)),
                Some((zone, utc)) => datetime
                    .call1((y, mo, d, h, mi, s, micro, utc))
                    .and_then(|dt| dt.call_method1("astimezone", (zone,))),
            };
            match converted {
                Ok(v) => out.push(v),
                // E.g. a UTC instant near year 1 shifted into a western zone.
                Err(e) if out_of_range(py, &e) => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
}

/// Whether `error` is a `date`/`datetime` range error, which the caller's
/// fallback reports (or handles) as it does without this module.
fn out_of_range(py: Python<'_>, error: &PyErr) -> bool {
    error.is_instance_of::<PyOverflowError>(py) || error.is_instance_of::<PyValueError>(py)
}

/// Hashable stand-in for a result schema, equal to another key exactly when
/// the schemas are equal (field names, types, nullability and metadata).
/// Lets callers cache what they derive from a schema without converting it
/// to pyarrow, whose `Schema.equals(check_metadata=True)` fingerprints both
/// sides on every call.
#[pyclass(name = "SchemaKey", module = "databricks_sql_kernel", frozen)]
pub(crate) struct SchemaKey {
    schema: Arc<Schema>,
    /// Computed on first `hash()`: comparing two keys doesn't need it.
    hash: OnceLock<u64>,
}

impl SchemaKey {
    pub(crate) fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            hash: OnceLock::new(),
        }
    }
}

#[pymethods]
impl SchemaKey {
    /// The key of a `pyarrow.Schema`.
    #[new]
    fn py_new(schema: &Bound<'_, PyAny>) -> PyResult<Self> {
        use arrow_pyarrow::FromPyArrow;
        Ok(Self::new(Arc::new(Schema::from_pyarrow_bound(schema)?)))
    }

    fn __hash__(&self) -> u64 {
        *self.hash.get_or_init(|| {
            let mut hasher = DefaultHasher::new();
            self.schema.hash(&mut hasher);
            hasher.finish()
        })
    }

    /// `NotImplemented` (via the failed extraction) for other types.
    fn __eq__(&self, other: &Bound<'_, Self>) -> bool {
        let other = other.get();
        Arc::ptr_eq(&self.schema, &other.schema) || self.schema == other.schema
    }

    fn __repr__(&self) -> String {
        let names: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        format!("SchemaKey({names:?})")
    }
}

/// The rows of pyarrow record batches as `fetch_all_rows` would return them,
/// for testing the conversion against pyarrow's own. `None` where
/// `fetch_all_rows` would decline. Not part of the public API.
#[pyfunction]
#[pyo3(name = "_rows_from_arrow", signature = (batches, nan_ok = false))]
pub(crate) fn rows_from_arrow<'py>(
    py: Python<'py>,
    batches: Vec<Bound<'py, PyAny>>,
    nan_ok: bool,
) -> PyResult<Option<Bound<'py, PyList>>> {
    use arrow_pyarrow::FromPyArrow;
    let batches = batches
        .iter()
        .map(RecordBatch::from_pyarrow_bound)
        .collect::<PyResult<Vec<_>>>()?;
    let Some(first) = batches.first() else {
        return Ok(Some(PyList::empty(py)));
    };
    let schema = first.schema();
    if !schema_is_supported(&schema) {
        return Ok(None);
    }
    rows_to_py(py, &schema, &batches, nan_ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Date32Array, Float64Array, Int32Array, StringArray};
    use arrow_schema::Field;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).unwrap()
    }

    fn repr(rows: Option<Bound<'_, PyList>>) -> Option<String> {
        rows.map(|r| r.repr().unwrap().to_string())
    }

    #[test]
    fn ymd_covers_python_date_range_only() {
        assert_eq!(ymd(0), Some((1970, 1, 1)));
        assert_eq!(ymd(-719_162), Some((1, 1, 1)));
        assert_eq!(ymd(2_932_896), Some((9999, 12, 31)));
        assert_eq!(ymd(-719_163), None);
        assert_eq!(ymd(2_932_897), None);
        assert_eq!(ymd(i64::MAX), None);
    }

    #[test]
    fn rows_are_tuples_with_none_for_nulls() {
        Python::initialize();
        Python::attach(|py| {
            let b = batch(vec![
                (
                    "n",
                    Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
                ),
                ("s", Arc::new(StringArray::from(vec![Some("a"), None]))),
            ]);
            let rows = rows_to_py(py, &b.schema(), &[b.clone(), b.slice(1, 1)], false);
            assert_eq!(
                repr(rows.unwrap()).as_deref(),
                Some("[(1, 'a'), (None, None), (None, None)]")
            );
        });
    }

    #[test]
    fn unconvertible_values_and_mismatched_batches_decline() {
        Python::initialize();
        Python::attach(|py| {
            let nan = batch(vec![(
                "f",
                Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef,
            )]);
            let nans = std::slice::from_ref(&nan);
            assert!(rows_to_py(py, &nan.schema(), nans, false)
                .unwrap()
                .is_none());
            assert_eq!(
                repr(rows_to_py(py, &nan.schema(), nans, true).unwrap()).as_deref(),
                Some("[(nan,)]")
            );
            let late = batch(vec![(
                "d",
                Arc::new(Date32Array::from(vec![2_932_897])) as ArrayRef,
            )]);
            assert!(rows_to_py(py, &late.schema(), &[late], false)
                .unwrap()
                .is_none());
            let other = Schema::new(vec![Field::new("f", DataType::Float32, true)]);
            assert!(rows_to_py(py, &other, &[nan], true).unwrap().is_none());
        });
    }

    #[test]
    fn take_rows_leaves_batches_it_does_not_convert() {
        Python::initialize();
        Python::attach(|py| {
            let b = batch(vec![(
                "n",
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            )]);
            let schema = b.schema();
            let mut pending = VecDeque::from([b.clone()]);
            // Stream not ended, or more rows than allowed: nothing taken.
            assert!(take_rows(py, &schema, false, &mut pending, 10, false)
                .unwrap()
                .is_none());
            assert!(take_rows(py, &schema, true, &mut pending, 2, false)
                .unwrap()
                .is_none());
            assert_eq!(pending.len(), 1);
            let rows = take_rows(py, &schema, true, &mut pending, 3, false).unwrap();
            assert_eq!(repr(rows).as_deref(), Some("[(1,), (2,), (3,)]"));
            assert!(pending.is_empty());
            // An empty, ended stream is zero rows.
            let rows = take_rows(py, &schema, true, &mut pending, 0, false).unwrap();
            assert_eq!(repr(rows).as_deref(), Some("[]"));
        });
    }

    #[test]
    fn schema_keys_compare_schemas_including_metadata() {
        let field = Field::new("n", DataType::Int32, true);
        let with_meta = field
            .clone()
            .with_metadata([("databricks.type_name".to_string(), "INT".to_string())].into());
        let key = |f: &Field| SchemaKey::new(Arc::new(Schema::new(vec![f.clone()])));
        let same = |a: &SchemaKey, b: &SchemaKey| a.schema == b.schema;
        assert!(same(&key(&with_meta), &key(&with_meta)));
        assert!(!same(&key(&with_meta), &key(&field)));
        assert!(!same(&key(&field), &key(&field.clone().with_name("m"))));
        assert!(!same(
            &key(&field),
            &key(&field.clone().with_nullable(false))
        ));
        assert_eq!(key(&with_meta).__hash__(), key(&with_meta).__hash__());
    }
}
