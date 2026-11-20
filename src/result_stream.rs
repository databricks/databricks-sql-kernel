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

//! `ResultStream` — paginated result returned by `execute()` /
//! `await_result()` / `fetch_result()`.
//!
//! Per `docs/spec-v0.md` "ResultStream", the kernel abstracts both
//! inline and CloudFetch result delivery behind one type. The API is
//! split across an *async* batch-fetch boundary and a *sync* row-pull
//! cursor, so a hot per-row loop never goes through a per-row Future
//! or a per-row heap allocation.
//!
//! ## Why batch-async + row-sync?
//!
//! For a 10M-row scan, the cost of returning each row through
//! `async fn next_row(&mut self) -> Future<Owned Row>` is real:
//!
//! - Each owned `Row` would require at least one heap allocation per
//!   row (the column buffer Vec); variable-width columns add more.
//!   A ~5-string-column query against 10M rows ≈ 70M heap allocations.
//! - Each `Future` is a state machine; even when the body never
//!   actually awaits, the API leaves the door open for the compiler to
//!   keep one alive per row.
//!
//! Splitting the API removes both costs. `next_batch().await` does
//! the network work once per ~10K-row batch; `next_row_in_batch()`
//! walks the already-fetched batch with O(1) array indexing and no
//! allocation. `Row<'a>` is a borrowed view (one pointer + one index)
//! into the currently-buffered batch.
//!
//! ## Mixing batch and row APIs
//!
//! Callers may mix `next_batch().await` with `next_row_in_batch()` —
//! `next_batch()` resets the row cursor to the start of the new
//! batch.

use crate::kernel_error::{Error, Result};
use crate::reader::{ResultReader, ResultReaderMetrics};
use crate::session::SessionInner;
use crate::telemetry::StatementTelemetry;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, DecimalType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use std::collections::VecDeque;
use std::sync::Arc;

/// Cheap-clone reference to a result schema. The kernel hands one
/// `Arc<Schema>` per result stream out of the wrapped reader; callers
/// clone the `Arc` to keep it alive past the stream when needed (the
/// FFI layer in particular needs to keep the schema alive across an
/// `extern "C"` boundary, which a borrowed `&Schema` cannot express).
pub type SchemaRef = Arc<Schema>;

/// Format-neutral envelope for one batch of result data.
///
/// v0 only ever yields `Arrow`. The enum is `#[non_exhaustive]` so v1+
/// non-Arrow variants (`ColumnarThrift`, `JsonRows`, …) are additive
/// — wrappers writing `match` against `ResultBatch` get a forced
/// `_ =>` arm that keeps compiling.
#[derive(Debug)]
#[non_exhaustive]
pub enum ResultBatch {
    /// Arrow `RecordBatch`.
    Arrow(RecordBatch),
    // Future: ColumnarThrift(...), JsonRows(...)
}

/// Outcome of a non-blocking result-stream read.
#[derive(Debug)]
#[non_exhaustive]
pub enum TryNextBatch<'a> {
    Ready(&'a ResultBatch),
    Pending,
    End,
}

type Prefetched<T> = VecDeque<std::result::Result<T, Box<Error>>>;

// `ResultBatch::into_arrow()` was deliberately removed: the
// `#[non_exhaustive]` envelope is the right escape hatch already.
// Wrappers `match` directly on `ResultBatch::Arrow(rb)` and choose
// their fallback (panic, error, future-format support) at compile
// time. A runtime `into_arrow()` -> Result<RecordBatch> would
// undermine that — non-Arrow variants would surface as runtime
// `InvalidArgument` errors in production rather than at the `match`
// site at compile time. Kept for the historical record because
// rounds 1-3 kept proposing it.

/// Borrowed cursor into the currently-buffered batch. One pointer +
/// one index — no heap allocation per row.
///
/// Returned by [`ResultStream::next_row_in_batch`]. The lifetime
/// borrows from the `ResultStream` (and transitively from the batch
/// it is currently iterating); the row is invalidated when the next
/// `next_batch()` call replaces the buffer.
///
/// Per-column accessors (`get_str`, `get_i64`, …) return `Option`
/// (SQL NULL → `None`) and borrow data from the batch where possible
/// (e.g. UTF-8 columns return `&'a str`, byte columns return
/// `&'a [u8]`). PR 5 fleshes out the full per-Arrow-type set.
pub struct Row<'a> {
    batch: &'a RecordBatch,
    row_idx: usize,
}

impl<'a> Row<'a> {
    /// Number of columns in the row.
    pub fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    // No `Row::schema()` accessor: the schema lives on the parent
    // `ResultStream` (callers reach it via `stream.schema()`). A
    // convenience method here would borrow `&'a Schema` from the
    // batch, which entangles `Row<'a>`'s lifetime with the schema's
    // in a way that adds bookkeeping cost without saving the caller a
    // meaningful step.

    fn column(&self, col: usize) -> Result<&'a dyn Array> {
        if col >= self.batch.num_columns() {
            return Err(Error::invalid_argument().message(format!(
                "column index {col} out of range (batch has {} columns)",
                self.batch.num_columns()
            )));
        }
        Ok(self.batch.column(col).as_ref())
    }

    /// UTF-8 string column. Returns `None` for SQL NULL; borrows from
    /// the underlying batch.
    pub fn get_str(&self, col: usize) -> Result<Option<&'a str>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Utf8 => Ok(Some(arr.as_string::<i32>().value(self.row_idx))),
            DataType::LargeUtf8 => Ok(Some(arr.as_string::<i64>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Utf8/LargeUtf8", other)),
        }
    }

    /// 64-bit signed integer column. Widens `Int8`/`Int16`/`Int32`.
    pub fn get_i64(&self, col: usize) -> Result<Option<i64>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Int8 => Ok(Some(
                arr.as_primitive::<Int8Type>().value(self.row_idx) as i64
            )),
            DataType::Int16 => Ok(Some(
                arr.as_primitive::<Int16Type>().value(self.row_idx) as i64
            )),
            DataType::Int32 => Ok(Some(
                arr.as_primitive::<Int32Type>().value(self.row_idx) as i64
            )),
            DataType::Int64 => Ok(Some(arr.as_primitive::<Int64Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Int{8,16,32,64}", other)),
        }
    }

    /// 32-bit signed integer column. Widens `Int8`/`Int16`. Fails
    /// with `InvalidArgument` for `Int64` to avoid silent truncation.
    pub fn get_i32(&self, col: usize) -> Result<Option<i32>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Int8 => Ok(Some(
                arr.as_primitive::<Int8Type>().value(self.row_idx) as i32
            )),
            DataType::Int16 => Ok(Some(
                arr.as_primitive::<Int16Type>().value(self.row_idx) as i32
            )),
            DataType::Int32 => Ok(Some(arr.as_primitive::<Int32Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Int{8,16,32}", other)),
        }
    }

    /// 16-bit signed integer column. Widens `Int8`.
    pub fn get_i16(&self, col: usize) -> Result<Option<i16>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Int8 => Ok(Some(
                arr.as_primitive::<Int8Type>().value(self.row_idx) as i16
            )),
            DataType::Int16 => Ok(Some(arr.as_primitive::<Int16Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Int{8,16}", other)),
        }
    }

    /// 8-bit signed integer column.
    pub fn get_i8(&self, col: usize) -> Result<Option<i8>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Int8 => Ok(Some(arr.as_primitive::<Int8Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Int8", other)),
        }
    }

    /// 64-bit unsigned integer column. Widens `UInt8`/`UInt16`/`UInt32`.
    pub fn get_u64(&self, col: usize) -> Result<Option<u64>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::UInt8 => Ok(Some(
                arr.as_primitive::<UInt8Type>().value(self.row_idx) as u64
            )),
            DataType::UInt16 => Ok(Some(
                arr.as_primitive::<UInt16Type>().value(self.row_idx) as u64
            )),
            DataType::UInt32 => Ok(Some(
                arr.as_primitive::<UInt32Type>().value(self.row_idx) as u64
            )),
            DataType::UInt64 => Ok(Some(arr.as_primitive::<UInt64Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "UInt{8,16,32,64}", other)),
        }
    }

    /// 32-bit unsigned integer column. Widens `UInt8`/`UInt16`.
    pub fn get_u32(&self, col: usize) -> Result<Option<u32>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::UInt8 => Ok(Some(
                arr.as_primitive::<UInt8Type>().value(self.row_idx) as u32
            )),
            DataType::UInt16 => Ok(Some(
                arr.as_primitive::<UInt16Type>().value(self.row_idx) as u32
            )),
            DataType::UInt32 => Ok(Some(arr.as_primitive::<UInt32Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "UInt{8,16,32}", other)),
        }
    }

    /// 16-bit unsigned integer column. Widens `UInt8`.
    pub fn get_u16(&self, col: usize) -> Result<Option<u16>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::UInt8 => Ok(Some(
                arr.as_primitive::<UInt8Type>().value(self.row_idx) as u16
            )),
            DataType::UInt16 => Ok(Some(arr.as_primitive::<UInt16Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "UInt{8,16}", other)),
        }
    }

    /// 8-bit unsigned integer column.
    pub fn get_u8(&self, col: usize) -> Result<Option<u8>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::UInt8 => Ok(Some(arr.as_primitive::<UInt8Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "UInt8", other)),
        }
    }

    /// 64-bit float column. Widens `Float32`.
    pub fn get_f64(&self, col: usize) -> Result<Option<f64>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Float32 => Ok(Some(
                arr.as_primitive::<Float32Type>().value(self.row_idx) as f64
            )),
            DataType::Float64 => Ok(Some(arr.as_primitive::<Float64Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Float{32,64}", other)),
        }
    }

    /// 32-bit float column.
    pub fn get_f32(&self, col: usize) -> Result<Option<f32>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Float32 => Ok(Some(arr.as_primitive::<Float32Type>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Float32", other)),
        }
    }

    /// Boolean column.
    pub fn get_bool(&self, col: usize) -> Result<Option<bool>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Boolean => Ok(Some(arr.as_boolean().value(self.row_idx))),
            other => Err(type_mismatch(col, "Boolean", other)),
        }
    }

    /// Binary column. Returns `None` for SQL NULL; borrows from the
    /// underlying batch.
    pub fn get_bytes(&self, col: usize) -> Result<Option<&'a [u8]>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Binary => Ok(Some(arr.as_binary::<i32>().value(self.row_idx))),
            DataType::LargeBinary => Ok(Some(arr.as_binary::<i64>().value(self.row_idx))),
            other => Err(type_mismatch(col, "Binary/LargeBinary", other)),
        }
    }

    /// Decimal column rendered in canonical decimal string form
    /// (e.g. `"-123.456"`). Accepts both `Decimal128` and `Decimal256`
    /// — Databricks decimals are capped at precision 38 today (fits
    /// `Decimal128`), but the kernel handles `Decimal256` too so
    /// future precision bumps don't surface as `InvalidArgument`.
    /// Returning a `String` keeps `rust_decimal` out of the public
    /// surface.
    pub fn get_decimal(&self, col: usize) -> Result<Option<String>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Decimal128(p, s) => {
                let raw = arr.as_primitive::<Decimal128Type>().value(self.row_idx);
                Ok(Some(Decimal128Type::format_decimal(raw, *p, *s)))
            }
            DataType::Decimal256(p, s) => {
                let raw = arr.as_primitive::<Decimal256Type>().value(self.row_idx);
                Ok(Some(Decimal256Type::format_decimal(raw, *p, *s)))
            }
            other => Err(type_mismatch(col, "Decimal128/Decimal256", other)),
        }
    }

    /// Date column as `chrono::NaiveDate`.
    pub fn get_date(&self, col: usize) -> Result<Option<NaiveDate>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Date32 => Ok(arr.as_primitive::<Date32Type>().value_as_date(self.row_idx)),
            DataType::Date64 => Ok(arr.as_primitive::<Date64Type>().value_as_date(self.row_idx)),
            other => Err(type_mismatch(col, "Date32/Date64", other)),
        }
    }

    /// Timestamp column as `chrono::NaiveDateTime`. Drops any
    /// timezone — the caller must layer a TZ on top if they need
    /// one.
    pub fn get_timestamp(&self, col: usize) -> Result<Option<NaiveDateTime>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Timestamp(unit, _tz) => Ok(match unit {
                TimeUnit::Second => arr
                    .as_primitive::<TimestampSecondType>()
                    .value_as_datetime(self.row_idx),
                TimeUnit::Millisecond => arr
                    .as_primitive::<TimestampMillisecondType>()
                    .value_as_datetime(self.row_idx),
                TimeUnit::Microsecond => arr
                    .as_primitive::<TimestampMicrosecondType>()
                    .value_as_datetime(self.row_idx),
                TimeUnit::Nanosecond => arr
                    .as_primitive::<TimestampNanosecondType>()
                    .value_as_datetime(self.row_idx),
            }),
            other => Err(type_mismatch(col, "Timestamp", other)),
        }
    }

    /// Interval column in canonical SQL string form. Databricks
    /// emits intervals as UTF-8 in v0, so this is a thin wrapper
    /// around [`get_str`](Self::get_str) that also accepts Arrow's
    /// native `Interval` types (returned as a `Debug`-formatted
    /// string for now).
    pub fn get_interval(&self, col: usize) -> Result<Option<String>> {
        let arr = self.column(col)?;
        if arr.is_null(self.row_idx) {
            return Ok(None);
        }
        match arr.data_type() {
            DataType::Utf8 => Ok(Some(arr.as_string::<i32>().value(self.row_idx).to_owned())),
            DataType::LargeUtf8 => Ok(Some(arr.as_string::<i64>().value(self.row_idx).to_owned())),
            DataType::Interval(_) => {
                // Arrow's native Interval types are rarely used by
                // Databricks results — fall back to a Debug-formatted
                // representation so callers see *something*. Tracked
                // as a §7 follow-up to render properly.
                Ok(Some(format!("{:?}", arr.slice(self.row_idx, 1))))
            }
            other => Err(type_mismatch(col, "Utf8/Interval", other)),
        }
    }
}

/// Build a typed-error for `Row` accessor type mismatches.
fn type_mismatch(col: usize, expected: &str, actual: &DataType) -> Error {
    Error::invalid_argument().message(format!(
        "column {col} type mismatch: expected {expected}, got {actual:?}"
    ))
}

/// Paginated result stream.
///
/// Internally wraps a `Box<dyn ResultReader>` (kernel-internal trait
/// — see `src/reader/mod.rs`). Wrappers see one concrete public type;
/// the polymorphism is hidden.
#[non_exhaustive]
pub struct ResultStream {
    /// Wrapped async reader. The reader's `next_batch` is `async fn`
    /// — for in-memory readers the future is immediately ready; for
    /// CloudFetch it drives the underlying `StreamingCloudFetchProvider`
    /// directly. No `block_on` is performed anywhere on this path, so
    /// `ResultStream::next_batch` is safe to `.await` from any tokio
    /// runtime (including napi-rs's).
    reader: Option<Box<dyn ResultReader>>,

    /// Cached schema. Filled in eagerly by
    /// [`ResultStream::from_reader`] (which is `async` because pulling
    /// the schema from a CloudFetch reader forces the first chunk to
    /// download) so [`ResultStream::schema`] is non-fallible and
    /// synchronous.
    schema: SchemaRef,

    /// The most recent batch returned by [`ResultStream::next_batch`].
    /// Owned by the stream so the borrowed `&ResultBatch` stays valid
    /// until the next call to `next_batch` replaces it.
    current: Option<ResultBatch>,

    /// Buffered outcomes retained for subsequent reads after inspection.
    prefetched: Prefetched<ResultBatch>,

    /// Row cursor inside [`current`](Self::current). Reset to `0`
    /// every time `next_batch` populates a fresh batch.
    /// `next_row_in_batch` reads and increments.
    current_row_idx: usize,

    /// Flips to `true` once the wrapped reader returns its first
    /// `None`. Subsequent `next_batch` calls short-circuit to
    /// `Ok(None)` without re-entering the reader — saves a wasted
    /// call on readers whose `next_batch` is non-trivial (CloudFetch
    /// in particular checks state and may take a runtime tick).
    exhausted: bool,

    /// Optional fire-and-forget `CloseStatement` on Drop.
    ///
    /// `Some` only for the **metadata** path (`Metadata::list_*`),
    /// which produces a bare `ResultStream` with no owning
    /// `ExecutedStatement` to reap the server-side statement. Without
    /// this, every metadata call leaked a server statement until the
    /// session closed. The data path leaves this `None` — there the
    /// owning `ExecutedStatement` (or its `Drop`) issues the close, so
    /// a second guard here would double-fire.
    close_guard: Option<StatementCloseGuard>,

    telemetry: Option<ResultStreamTelemetry>,
    volume: Option<Arc<crate::volume::VolumeState>>,
}

struct ResultStreamTelemetry {
    telemetry: StatementTelemetry,
    finalized: bool,
    consumed_recorded: bool,
}

impl ResultStreamTelemetry {
    fn new(telemetry: StatementTelemetry) -> Self {
        Self {
            telemetry,
            finalized: false,
            consumed_recorded: false,
        }
    }

    fn record_batch(&self, batch: &ResultBatch) {
        match batch {
            ResultBatch::Arrow(_) => self.record_result_batch(),
        }
    }

    fn record_result_batch(&self) {
        self.telemetry.record_result_batch();
    }

    /// Finalize telemetry exactly once.
    ///
    /// `consumed` gates the consumption-latency stamp: it must be
    /// `true` only on a terminal read path — the stream reaching its
    /// natural end (`Ok(None)`) or erroring — and `false` on the
    /// `Drop` path. `record_result_consumed` sets
    /// `consumption_latency = started_at.elapsed()`, so stamping it on
    /// a stream that was abandoned (or only partially read) and then
    /// dropped would report caller think-time from statement start to
    /// handle drop rather than the latency to actually consume the
    /// result set, skewing `result_set_consumption_latency_millis`.
    /// Chunk metrics and the terminal `finalize` still fire on every
    /// path so a dropped stream is always reported.
    fn finish(
        &mut self,
        metrics: Option<ResultReaderMetrics>,
        error: Option<&Error>,
        consumed: bool,
    ) {
        if self.finalized {
            return;
        }
        if let Some(metrics) = metrics {
            self.telemetry.record_chunk_metrics(metrics.into());
        }
        if consumed && !self.consumed_recorded {
            self.telemetry.record_result_consumed();
            self.consumed_recorded = true;
        }
        self.telemetry.finalize(error);
        self.finalized = true;
    }
}

/// Fire-and-forget `CloseStatement`-on-Drop for a server statement that
/// no other handle owns (the metadata path). Mirrors the
/// `ExecutedStatement` Drop cleanup, scoped to the bare-stream case.
struct StatementCloseGuard {
    session: Arc<SessionInner>,
    statement_id: String,
}

impl Drop for StatementCloseGuard {
    fn drop(&mut self) {
        let transport = Arc::clone(self.session.transport());
        let runtime = self.session.runtime_handle().clone();
        let stmt_id = std::mem::take(&mut self.statement_id);
        if stmt_id.is_empty() {
            return;
        }
        runtime.spawn(async move {
            if let Err(e) = transport.close_statement(&stmt_id).await {
                crate::klog::warn!(
                    "fire-and-forget close_statement for metadata stream {} failed: {} \
                     (server reclaims on session close)",
                    stmt_id,
                    e
                );
            }
        });
    }
}

impl ResultStream {
    /// Build a `ResultStream` over a kernel-internal `ResultReader`.
    ///
    /// `pub(crate)` — wrappers never call this directly. They go
    /// through `Metadata::*` (PR 3) or `ExecutedStatement::result_stream`
    /// (PR 4).
    ///
    /// Pulls the reader's schema eagerly so `ResultStream::schema`
    /// is non-fallible and available before any batches are
    /// requested. `async` because the CloudFetch reader resolves its
    /// schema by forcing the first chunk to download; in-memory and
    /// inline readers resolve immediately. v0 only ever wraps
    /// Arrow-producing readers, so every batch is wrapped in
    /// [`ResultBatch::Arrow`].
    pub(crate) async fn from_reader(reader: Box<dyn ResultReader>) -> Result<Self> {
        let schema = reader.schema().await?;
        Ok(Self {
            reader: Some(reader),
            schema,
            current: None,
            prefetched: VecDeque::new(),
            current_row_idx: 0,
            exhausted: false,
            close_guard: None,
            telemetry: None,
            volume: None,
        })
    }

    /// Like [`from_reader`](Self::from_reader), but the stream owns a
    /// fire-and-forget `CloseStatement` for `statement_id`, fired when
    /// the stream is dropped.
    ///
    /// For the **metadata** path only: `Metadata::list_*` returns a
    /// bare `ResultStream` with no owning `ExecutedStatement`, so
    /// nothing else reaps the server statement. The data path uses
    /// `from_reader` (no guard) because `ExecutedStatement` owns the
    /// close there.
    pub(crate) async fn from_reader_with_close(
        reader: Box<dyn ResultReader>,
        session: Arc<SessionInner>,
        statement_id: String,
    ) -> Result<Self> {
        let mut stream = Self::from_reader(reader).await?;
        // Skip the guard for an empty id (inline fast-path results the
        // server already auto-closed) — nothing to reap, and an empty
        // CloseStatement would be a wasted/invalid RPC.
        if !statement_id.is_empty() {
            stream.close_guard = Some(StatementCloseGuard {
                session,
                statement_id,
            });
        }
        Ok(stream)
    }

    pub(crate) fn attach_volume(&mut self, marked: bool, state: Arc<crate::volume::VolumeState>) {
        if marked {
            self.volume = Some(state);
        }
    }

    /// Whether this execution has server-authorized Volume completion.
    pub fn is_volume_operation(&self) -> bool {
        self.volume.is_some()
    }

    /// Complete a server-marked file transfer while retaining its instruction rows.
    ///
    /// Ordinary results and read-only attachments have no transfer authority.
    /// `NeedsInputStream` requests owned bytes for the server's stream sentinel;
    /// supplying an empty buffer is a valid upload.
    pub async fn complete_volume_operation(
        &mut self,
        options: &crate::volume::VolumeOptions,
        input_bytes: Option<Vec<u8>>,
    ) -> Result<crate::volume::VolumeCompletion> {
        let Some(state) = self.volume.clone() else {
            return Ok(crate::volume::VolumeCompletion::NotVolume);
        };
        if !state.completion.initialized()
            && (self.current.is_some() || (self.exhausted && self.prefetched.is_empty()))
        {
            return Err(Error::invalid_state()
                .message("complete_volume_operation must be called before reading result rows"));
        }
        let result = state
            .complete(
                async {
                    let batch = loop {
                        let batch = self.peek_first_batch().await?.ok_or_else(|| {
                            Error::invalid_argument().message("Missing Volume instruction row")
                        })?;
                        let ResultBatch::Arrow(batch) = batch;
                        if batch.num_rows() != 0 {
                            break batch.clone();
                        }
                        self.prefetched.pop_front();
                    };
                    // Later batches cannot authorize a partial transfer of a multi-row result.
                    while !self.exhausted {
                        match self
                            .reader
                            .as_mut()
                            .expect("Volume completion retains its reader")
                            .next_batch()
                            .await
                        {
                            Err(error) => {
                                self.prefetched.push_back(Err(Box::new(error.clone())));
                                return Err(error);
                            }
                            Ok(Some(batch)) if batch.num_rows() != 0 => {
                                self.prefetched.push_back(Ok(ResultBatch::Arrow(batch)));
                                return Err(Error::invalid_argument()
                                    .message("Expected one Volume instruction row"));
                            }
                            Ok(Some(_)) => {}
                            Ok(None) => self.exhausted = true,
                        }
                    }
                    Ok(batch)
                },
                options,
                input_bytes,
            )
            .await;
        if let Err(error) = &result {
            self.finalize_telemetry(Some(error));
        }
        result
    }

    pub(crate) fn volume_state(&self) -> Option<Arc<crate::volume::VolumeState>> {
        self.volume.as_ref().map(Arc::clone)
    }

    pub(crate) fn cancel_volume(&self) {
        if let Some(state) = &self.volume {
            state.cancel();
        }
    }

    pub(crate) fn attach_telemetry(&mut self, telemetry: StatementTelemetry) {
        self.telemetry = Some(ResultStreamTelemetry::new(telemetry));
    }

    fn reader_metrics(&self) -> Option<ResultReaderMetrics> {
        self.reader.as_ref().and_then(|reader| reader.metrics())
    }

    pub(crate) fn finish_telemetry(&mut self) {
        let metrics = self.reader_metrics();
        if let Some(telemetry) = &mut self.telemetry {
            telemetry.finish(metrics, None, false);
        }
    }

    /// Finalize telemetry once, passing the reader's terminal metrics.
    /// `reader` and `telemetry` are disjoint fields, so this borrows
    /// cleanly under `&mut self`. `ResultStreamTelemetry::finish` is
    /// idempotent, so redundant calls are no-ops.
    ///
    /// This is the **read-path** finalizer — it fires when the stream
    /// reaches its natural end or errors, both of which count as
    /// consuming the result set, so it always stamps consumption
    /// latency. The `Drop` path calls `finish` directly with
    /// `consumed = false` so an abandoned stream doesn't report
    /// think-time as consumption latency.
    fn finalize_telemetry(&mut self, error: Option<&Error>) {
        let metrics = self.reader_metrics();
        if let Some(telemetry) = &mut self.telemetry {
            telemetry.finish(metrics, error, true);
        }
    }

    /// Install `batch` as the current batch: buffer it, reset the row
    /// cursor, and record it to telemetry exactly once. Consolidates the
    /// prefetch-replay and fresh-fetch arms of [`next_batch`](Self::next_batch)
    /// so the telemetry/cursor bookkeeping lives in one place.
    fn set_current_batch(&mut self, batch: ResultBatch) {
        self.current = Some(batch);
        self.current_row_idx = 0;
        if let (Some(telemetry), Some(batch)) = (&self.telemetry, self.current.as_ref()) {
            telemetry.record_batch(batch);
        }
    }

    /// Result schema. Available before any batches have been pulled.
    /// Returns a cheap-clone `Arc<Schema>` so callers can hold the
    /// schema independently of the stream (the FFI layer needs this
    /// to keep the schema alive across an `extern "C"` boundary).
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Pull the next batch into the kernel's buffer and return a
    /// borrowed reference. Drives polling, CloudFetch, decompression
    /// internally. `None` when the stream is exhausted.
    ///
    /// Calling this resets the row cursor: the next
    /// [`next_row_in_batch`](Self::next_row_in_batch) returns the
    /// first row of the new batch.
    ///
    /// Return type is **borrowed and format-neutral** —
    /// `&ResultBatch` preserves the per-batch zero-allocation
    /// property and keeps the `#[non_exhaustive] enum ResultBatch`
    /// envelope as the v1 escape hatch (additional variants in v1+
    /// are non-breaking).
    ///
    /// **v0 implementation note.** The wrapped `ResultReader` is
    /// async-native — `next_batch` awaits the underlying provider
    /// (CloudFetch / inline / in-memory) directly. No `block_on` is
    /// performed on this path, so calling from a tokio runtime
    /// (napi-rs, PyO3 async) is safe.
    pub async fn next_batch(&mut self) -> Result<Option<&ResultBatch>> {
        // Replay inspected batches first so DML counts and Volume validation
        // do not consume callers' result rows.
        if let Some(batch) = self.prefetched.pop_front() {
            self.set_current_batch(batch.map_err(|error| *error)?);
            return Ok(self.current.as_ref());
        }
        if self.exhausted {
            self.finalize_telemetry(None);
            return Ok(None);
        }
        let reader = self
            .reader
            .as_mut()
            .expect("ResultStream reader is present until into_owned_batches consumes it");
        match reader.next_batch().await {
            Ok(Some(rb)) => {
                self.set_current_batch(ResultBatch::Arrow(rb));
                Ok(self.current.as_ref())
            }
            Ok(None) => {
                self.current = None;
                self.current_row_idx = 0;
                self.exhausted = true;
                self.finalize_telemetry(None);
                Ok(None)
            }
            Err(e) => {
                self.finalize_telemetry(Some(&e));
                Err(e)
            }
        }
    }

    /// Return the next batch if it is already available without waiting for
    /// network or background-prefetch progress.
    pub fn try_next_batch(&mut self) -> Result<TryNextBatch<'_>> {
        if let Some(batch) = self.prefetched.pop_front() {
            self.set_current_batch(batch.map_err(|error| *error)?);
            return Ok(TryNextBatch::Ready(
                self.current.as_ref().expect("batch was just installed"),
            ));
        }
        if self.exhausted {
            self.finalize_telemetry(None);
            return Ok(TryNextBatch::End);
        }

        let reader = self
            .reader
            .as_mut()
            .expect("ResultStream reader is present until into_owned_batches consumes it");
        match reader.try_next_batch() {
            Ok(crate::reader::TryNextBatch::Ready(batch)) => {
                self.set_current_batch(ResultBatch::Arrow(batch));
                Ok(TryNextBatch::Ready(
                    self.current.as_ref().expect("batch was just installed"),
                ))
            }
            Ok(crate::reader::TryNextBatch::Pending) => Ok(TryNextBatch::Pending),
            Ok(crate::reader::TryNextBatch::End) => {
                self.current = None;
                self.current_row_idx = 0;
                self.exhausted = true;
                self.finalize_telemetry(None);
                Ok(TryNextBatch::End)
            }
            Err(error) => {
                self.finalize_telemetry(Some(&error));
                Err(error)
            }
        }
    }

    /// Non-consuming peek at the next buffered batch.
    ///
    /// Pulls a batch or error when none is current or buffered and the reader is not
    /// exhausted. A subsequent [`next_batch`](Self::next_batch) still returns
    /// the buffered outcome.
    pub(crate) async fn peek_first_batch(&mut self) -> Result<Option<&ResultBatch>> {
        if self.prefetched.is_empty() && self.current.is_none() && !self.exhausted {
            let reader = self
                .reader
                .as_mut()
                .expect("ResultStream reader is present until into_owned_batches consumes it");
            match reader.next_batch().await {
                Ok(Some(rb)) => {
                    self.prefetched.push_back(Ok(ResultBatch::Arrow(rb)));
                    // Stamp first-batch (result-set-ready) latency at fetch
                    // time, not when the caller later consumes the prefetched
                    // batch via `next_batch`. `record_result_batch` is
                    // idempotent (only stamps while unset), so the replay in
                    // `next_batch` is a no-op. This keeps
                    // `result_set_ready_latency_millis` reflecting when the
                    // result set became ready rather than the caller's
                    // think-time before the first `next_batch` pull.
                    if let (Some(telemetry), Some(Ok(batch))) =
                        (&self.telemetry, self.prefetched.front())
                    {
                        telemetry.record_batch(batch);
                    }
                }
                Ok(None) => self.exhausted = true,
                Err(e) => {
                    self.finalize_telemetry(Some(&e));
                    self.prefetched.push_back(Err(Box::new(e.clone())));
                    return Err(e);
                }
            }
        }
        match self.prefetched.front() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(error)) => Err((**error).clone()),
            None => Ok(None),
        }
    }

    /// The batch most recently buffered by
    /// [`next_batch`](Self::next_batch), without advancing the stream
    /// or doing any I/O. `None` before the first `next_batch` or after
    /// end-of-stream.
    ///
    /// Used by [`ExecutedStatement::next_batch`](crate::statement::executed::ExecutedStatement::next_batch)
    /// to re-borrow the just-fetched batch after a borrow-scoped
    /// exhaustion check, so it can fire the auto-close on `None`.
    pub fn current(&self) -> Option<&ResultBatch> {
        self.current.as_ref()
    }

    /// Pull the next row from the currently-buffered batch.
    ///
    /// **Sync.** Walks within the batch the most recent
    /// [`next_batch`](Self::next_batch).await populated; does no I/O.
    /// Returns `None` when the current batch is exhausted — call
    /// `next_batch().await` to load the next batch and resume.
    pub fn next_row_in_batch(&mut self) -> Option<Row<'_>> {
        let ResultBatch::Arrow(batch) = self.current.as_ref()?;
        if self.current_row_idx >= batch.num_rows() {
            return None;
        }
        let row = Row {
            batch,
            row_idx: self.current_row_idx,
        };
        self.current_row_idx += 1;
        Some(row)
    }

    // A row-stream adapter (`into_rows`, `into_stream`, etc.) is
    // intentionally **not** in v0. A signature like
    // `impl Stream<Item = Result<Row<'_>>>` would be a "lending
    // stream" — `Stream::Item` cannot depend on `&mut self` of the
    // producer and the standard `futures::Stream` cannot express it.
    // The right adapter is either a new public trait (own design
    // exercise) or `OwnedRow` (re-introduces the per-row allocation
    // we removed). v0 ships the dual `next_batch` /
    // `next_row_in_batch` API; revisit when consumers ask.

    /// Free client-side resources held by the stream — buffered
    /// chunks, the link prefetcher, in-flight downloaders.
    ///
    /// Usually local-only, **except for the metadata path**: a stream
    /// built via [`from_reader_with_close`](Self::from_reader_with_close)
    /// (`Metadata::list_*`) carries a close-on-drop guard that fires a
    /// fire-and-forget `CloseStatement` for its server statement when
    /// the stream drops. The data path (`from_reader`) issues no RPC
    /// here — the owning `ExecutedStatement` reaps the statement
    /// (auto-close-on-drain / its own `Drop`); to cancel a still-running
    /// statement, call `cancel()` on the executed handle.
    ///
    /// Consuming `self` makes use-after-close a compile error.
    /// `Drop` performs the same cleanup (including the metadata guard's
    /// close) if `close` was never called explicitly.
    pub fn close(self) {
        // Drops `self` (and the wrapped reader / current batch / any
        // metadata close-guard). Background tasks owned by the reader
        // stop on `Drop`.
    }

    /// Consume the stream and yield **owned** `RecordBatch`es as a
    /// sync iterator.
    ///
    /// The native async batch/row API is the recommended path for
    /// Rust callers — borrowing per-batch avoids the heap
    /// allocation per row that owned streaming would force. This
    /// adapter exists for FFI / non-async consumers (PR 6's C ABI,
    /// PyO3 `pyclass` wrappers, napi-rs resource wrappers, anyone
    /// who'd otherwise drop down to `arrow::RecordBatchReader`)
    /// where carrying a borrow across a host-language boundary
    /// isn't expressible.
    ///
    /// The wrapped reader is async; `next()` drives it via
    /// [`crate::util::runtime::block_on_current_runtime`], which uses
    /// `tokio::task::block_in_place` + the current runtime handle when
    /// called from inside a multi-thread tokio runtime, and falls back
    /// to `futures::executor::block_on` when no runtime is on the
    /// thread (e.g. plain Rust callers, the C ABI shim). This keeps
    /// CloudFetch readers (which await tokio primitives internally)
    /// safe from the nested-`block_on` panic that motivated issue #19,
    /// while still letting in-memory readers iterate without a tokio
    /// runtime at all. Errors are surfaced once; subsequent calls
    /// after an `Err` keep returning `None`.
    pub fn into_owned_batches(mut self) -> OwnedBatchIter {
        // Inspected batches were pulled from the reader; yield them first to preserve row order.
        let prefetched = std::mem::take(&mut self.prefetched)
            .into_iter()
            .map(|outcome| outcome.map(|ResultBatch::Arrow(rb)| rb))
            .collect();
        OwnedBatchIter {
            reader: self
                .reader
                .take()
                .expect("ResultStream reader is present until into_owned_batches consumes it"),
            prefetched,
            telemetry: self.telemetry.take(),
            done: false,
        }
    }
}

impl Drop for ResultStream {
    fn drop(&mut self) {
        // Drop is not a consumption path: the stream may have been
        // abandoned before exhaustion (or never read at all), so pass
        // `consumed = false` to avoid stamping caller think-time as
        // consumption latency. `finish` still records chunk metrics
        // and finalizes, so a dropped stream is always reported.
        let metrics = self.reader_metrics();
        if let Some(telemetry) = &mut self.telemetry {
            telemetry.finish(metrics, None, false);
        }
    }
}

/// Iterator over owned `RecordBatch`es produced by
/// [`ResultStream::into_owned_batches`].
///
/// `Iterator::Item` is `Result<RecordBatch>`. The first error stops
/// iteration; subsequent `next()` calls return `None`. Stops at the
/// reader's natural end-of-stream the same way.
pub struct OwnedBatchIter {
    reader: Box<dyn ResultReader>,
    /// Inspected batches must precede the reader's remaining rows.
    prefetched: Prefetched<RecordBatch>,
    telemetry: Option<ResultStreamTelemetry>,
    done: bool,
}

impl Iterator for OwnedBatchIter {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(outcome) = self.prefetched.pop_front() {
            return match outcome {
                Ok(batch) => {
                    if let Some(telemetry) = &self.telemetry {
                        telemetry.record_result_batch();
                    }
                    Some(Ok(batch))
                }
                Err(error) => {
                    self.done = true;
                    Some(Err(*error))
                }
            };
        }
        // Runtime-aware blocking poll: uses block_in_place + the
        // current Handle when there's a tokio runtime on this thread
        // (the case for CloudFetch readers, which await tokio
        // primitives); falls back to futures::executor::block_on for
        // plain-thread callers driving in-memory readers. Prevents
        // the nested-block_on panic from issue #19 on the sync
        // iterator path.
        match crate::util::runtime::block_on_current_runtime(self.reader.next_batch()) {
            Ok(Some(batch)) => {
                if let Some(telemetry) = &self.telemetry {
                    telemetry.record_result_batch();
                }
                Some(Ok(batch))
            }
            Ok(None) => {
                self.done = true;
                // Natural end of stream — the result set was fully
                // consumed, so stamp consumption latency.
                if let Some(telemetry) = &mut self.telemetry {
                    telemetry.finish(self.reader.metrics(), None, true);
                }
                None
            }
            Err(e) => {
                self.done = true;
                // Terminal error path also counts as consumption
                // ending, so stamp consumption latency.
                if let Some(telemetry) = &mut self.telemetry {
                    telemetry.finish(self.reader.metrics(), Some(&e), true);
                }
                Some(Err(e))
            }
        }
    }
}

impl Drop for OwnedBatchIter {
    fn drop(&mut self) {
        // Drop without draining the iterator is not a consumption
        // path (see `ResultStream`'s Drop): pass `consumed = false`.
        // If iteration reached its natural end / error, `next` already
        // finalized with `consumed = true` and this is a no-op.
        if let Some(telemetry) = &mut self.telemetry {
            telemetry.finish(self.reader.metrics(), None, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel_error::ErrorCode;
    use crate::reader::test_utils::ErrorThenEndReader;
    use crate::reader::InMemoryReader;
    use arrow_array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        TimestampMicrosecondArray, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_schema::{Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn schema_for(fields: Vec<Field>) -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(fields))
    }

    async fn stream_with(batch: RecordBatch) -> ResultStream {
        let schema = batch.schema();
        let reader = InMemoryReader::new(schema, vec![batch]);
        ResultStream::from_reader(Box::new(reader))
            .await
            .expect("from_reader")
    }

    async fn stream_with_one_shot_error() -> ResultStream {
        let schema = schema_for(vec![Field::new("a", DataType::Int32, false)]);
        ResultStream::from_reader(Box::new(ErrorThenEndReader {
            schema,
            error_pending: true,
        }))
        .await
        .expect("from_reader")
    }

    async fn advance_to_first_row(stream: &mut ResultStream) {
        let _ = stream
            .next_batch()
            .await
            .expect("next_batch")
            .expect("at least one batch");
    }

    fn first_row(stream: &mut ResultStream) -> Row<'_> {
        stream.next_row_in_batch().expect("a row")
    }

    #[tokio::test]
    async fn get_str_returns_value_and_none_for_null() {
        let schema = schema_for(vec![Field::new("s", DataType::Utf8, true)]);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("hi"), None]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;

        let r = first_row(&mut stream);
        assert_eq!(r.get_str(0).unwrap(), Some("hi"));
        let r = first_row(&mut stream);
        assert_eq!(r.get_str(0).unwrap(), None);
    }

    #[tokio::test]
    async fn get_i64_widens_smaller_ints() {
        let schema = schema_for(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int16, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int8Array::from(vec![1])),
                Arc::new(Int16Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int64Array::from(vec![4])),
            ],
        )
        .unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        assert_eq!(r.get_i64(0).unwrap(), Some(1));
        assert_eq!(r.get_i64(1).unwrap(), Some(2));
        assert_eq!(r.get_i64(2).unwrap(), Some(3));
        assert_eq!(r.get_i64(3).unwrap(), Some(4));
    }

    #[tokio::test]
    async fn get_i32_rejects_int64_to_avoid_truncation() {
        let schema = schema_for(vec![Field::new("a", DataType::Int64, false)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        let err = r.get_i32(0).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn get_u_unsigned_accessors() {
        let schema = schema_for(vec![
            Field::new("a", DataType::UInt8, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt8Array::from(vec![7])),
                Arc::new(UInt32Array::from(vec![1_000_000])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        assert_eq!(r.get_u8(0).unwrap(), Some(7));
        assert_eq!(r.get_u32(1).unwrap(), Some(1_000_000));
        assert_eq!(r.get_u64(2).unwrap(), Some(u64::MAX));
    }

    #[tokio::test]
    async fn get_f64_widens_f32() {
        let schema = schema_for(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float32Array::from(vec![1.5_f32])),
                Arc::new(Float64Array::from(vec![2.5_f64])),
            ],
        )
        .unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        assert!((r.get_f64(0).unwrap().unwrap() - 1.5).abs() < f64::EPSILON);
        assert!((r.get_f64(1).unwrap().unwrap() - 2.5).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn get_bool_and_bytes() {
        let schema = schema_for(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("blob", DataType::Binary, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"hello"),
                    Some(b"world"),
                ])),
            ],
        )
        .unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        assert_eq!(r.get_bool(0).unwrap(), Some(true));
        assert_eq!(r.get_bytes(1).unwrap(), Some(&b"hello"[..]));
        let r = first_row(&mut stream);
        assert_eq!(r.get_bool(0).unwrap(), Some(false));
        assert_eq!(r.get_bytes(1).unwrap(), Some(&b"world"[..]));
    }

    #[tokio::test]
    async fn get_decimal_renders_canonical_string() {
        let schema = schema_for(vec![Field::new("d", DataType::Decimal128(10, 2), false)]);
        let arr: ArrayRef = Arc::new(
            Decimal128Array::from(vec![-12345_i128, 100_i128])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        assert_eq!(r.get_decimal(0).unwrap().as_deref(), Some("-123.45"));
        let r = first_row(&mut stream);
        assert_eq!(r.get_decimal(0).unwrap().as_deref(), Some("1.00"));
    }

    #[tokio::test]
    async fn get_decimal_handles_decimal256() {
        // Databricks doesn't emit Decimal256 today, but the kernel
        // formats it just as well as Decimal128 — so future precision
        // bumps don't regress as InvalidArgument.
        use arrow_buffer::i256;
        let schema = schema_for(vec![Field::new("d", DataType::Decimal256(50, 3), false)]);
        let arr: ArrayRef = Arc::new(
            Decimal256Array::from(vec![i256::from_i128(-123_456_i128), i256::from_i128(1000)])
                .with_precision_and_scale(50, 3)
                .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        assert_eq!(r.get_decimal(0).unwrap().as_deref(), Some("-123.456"));
        let r = first_row(&mut stream);
        assert_eq!(r.get_decimal(0).unwrap().as_deref(), Some("1.000"));
    }

    #[tokio::test]
    async fn get_date_and_timestamp() {
        let schema = schema_for(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);
        // 2026-01-01 ↔ days since UNIX epoch (1970-01-01) = 20454.
        let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let days = (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
        let dt = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let micros = dt.and_utc().timestamp_micros();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(days)])),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)])),
            ],
        )
        .unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        assert_eq!(r.get_date(0).unwrap(), Some(date));
        assert_eq!(r.get_timestamp(1).unwrap(), Some(dt));
    }

    #[tokio::test]
    async fn type_mismatch_returns_invalid_argument() {
        let schema = schema_for(vec![Field::new("s", DataType::Utf8, false)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        let r = first_row(&mut stream);
        let err = r.get_i64(0).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("type mismatch"), "{}", err.message);
    }

    #[tokio::test]
    async fn next_row_in_batch_walks_all_rows_then_none() {
        let schema = schema_for(vec![Field::new("a", DataType::Int32, false)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let mut stream = stream_with(batch).await;
        advance_to_first_row(&mut stream).await;
        assert_eq!(
            stream
                .next_row_in_batch()
                .unwrap()
                .get_i32(0)
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(
            stream
                .next_row_in_batch()
                .unwrap()
                .get_i32(0)
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(
            stream
                .next_row_in_batch()
                .unwrap()
                .get_i32(0)
                .unwrap()
                .unwrap(),
            3
        );
        assert!(stream.next_row_in_batch().is_none());
    }

    #[tokio::test]
    async fn next_batch_resets_row_cursor() {
        let schema = schema_for(vec![Field::new("a", DataType::Int32, false)]);
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![30]))]).unwrap();
        let reader = InMemoryReader::new(b1.schema(), vec![b1, b2]);
        let mut stream = ResultStream::from_reader(Box::new(reader)).await.unwrap();

        stream.next_batch().await.unwrap().unwrap();
        assert!(stream.next_row_in_batch().is_some());
        assert!(stream.next_row_in_batch().is_some());
        assert!(stream.next_row_in_batch().is_none());

        stream.next_batch().await.unwrap().unwrap();
        // Cursor reset — first row of the new batch is available.
        assert_eq!(
            stream
                .next_row_in_batch()
                .unwrap()
                .get_i32(0)
                .unwrap()
                .unwrap(),
            30
        );
        assert!(stream.next_row_in_batch().is_none());
    }

    #[tokio::test]
    async fn exhausted_stream_short_circuits() {
        let schema = schema_for(vec![Field::new("a", DataType::Int32, false)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let mut stream = stream_with(batch).await;
        // Pull and exhaust.
        stream.next_batch().await.unwrap();
        assert!(stream.next_batch().await.unwrap().is_none());
        // Subsequent calls keep returning None without crashing.
        assert!(stream.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn next_batch_replays_peek_error() {
        for nonblocking in [false, true] {
            let mut stream = stream_with_one_shot_error().await;
            assert!(stream.peek_first_batch().await.is_err());
            let error = if nonblocking {
                stream.try_next_batch().unwrap_err()
            } else {
                stream.next_batch().await.unwrap_err()
            };
            assert_eq!(error.message, "simulated one-shot read error");
            assert!(stream.next_batch().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn into_owned_batches_replays_peek_error() {
        let mut stream = stream_with_one_shot_error().await;
        assert!(stream.peek_first_batch().await.is_err());

        let mut iter = stream.into_owned_batches();
        assert_eq!(
            iter.next().unwrap().unwrap_err().message,
            "simulated one-shot read error"
        );
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn into_owned_batches_yields_then_terminates() {
        // Stream construction is async (schema pulled via async fn);
        // the iterator itself is sync and drives in-memory readers via
        // `futures::executor::block_on`.
        let schema = schema_for(vec![Field::new("a", DataType::Int32, false)]);
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
            .unwrap();
        let b2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![3]))])
            .unwrap();
        let b3 = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![4]))]).unwrap();
        let reader = InMemoryReader::new(b1.schema(), vec![b1.clone(), b2.clone(), b3.clone()]);
        let mut stream = ResultStream::from_reader(Box::new(reader)).await.unwrap();
        assert!(stream.peek_first_batch().await.unwrap().is_some());

        let mut iter = stream.into_owned_batches();
        let first = iter.next().unwrap().unwrap();
        assert_eq!(first, b1);
        let second = iter.next().unwrap().unwrap();
        assert_eq!(second, b2);
        assert_eq!(iter.next().unwrap().unwrap(), b3);
        assert!(iter.next().is_none(), "exhausted iterator returns None");
        assert!(iter.next().is_none(), "still None on subsequent calls");
    }

    #[tokio::test]
    async fn into_owned_batches_stops_after_error() {
        // ErrorReader yields one Err then would yield None; iterator
        // surfaces the error once and terminates.
        use crate::reader::test_utils::ErrorReader;
        let schema = schema_for(vec![Field::new("a", DataType::Int32, false)]);
        let reader = ErrorReader { schema };
        let stream = ResultStream::from_reader(Box::new(reader)).await.unwrap();

        let mut iter = stream.into_owned_batches();
        let first = iter.next().unwrap();
        assert!(first.is_err());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn into_owned_batches_yields_batch_then_error_then_none() {
        // BatchThenErrorReader: one Ok(Some(batch)), then Err.
        // Iterator must yield the batch, surface the error, then
        // fuse to None forever.
        use crate::reader::test_utils::BatchThenErrorReader;
        let schema = schema_for(vec![Field::new("a", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 200]))],
        )
        .unwrap();
        let reader = BatchThenErrorReader {
            schema: batch.schema(),
            batch: Some(batch),
        };
        let stream = ResultStream::from_reader(Box::new(reader)).await.unwrap();

        let mut iter = stream.into_owned_batches();
        let first = iter.next().expect("first yields a batch");
        let first = first.expect("first is Ok(batch)");
        assert_eq!(first.num_rows(), 2);

        let second = iter.next().expect("second yields an error");
        assert!(second.is_err(), "second is Err");

        assert!(iter.next().is_none(), "iterator fuses to None after Err");
        assert!(iter.next().is_none(), "remains None on subsequent calls");
    }
}
