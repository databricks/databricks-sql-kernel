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

use crate::kernel_error::Result;
use crate::reader::ResultReader;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
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
#[non_exhaustive]
pub enum ResultBatch {
    /// Arrow `RecordBatch`.
    Arrow(RecordBatch),
    // Future: ColumnarThrift(...), JsonRows(...)
}

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
    // PR 5: batch reference + row index.
    //   batch: &'a RecordBatch,
    //   row_idx: usize,
    _phantom: std::marker::PhantomData<&'a ()>,
}

#[allow(
    dead_code,
    reason = "row accessors land in PR 5; PR 3 only ships the batch-level API for Metadata"
)]
impl<'a> Row<'a> {
    /// Number of columns in the row.
    pub fn num_columns(&self) -> usize {
        unimplemented!("PR 5")
    }

    // No `Row::schema()` accessor: the schema lives on the parent
    // `ResultStream` (callers reach it via `stream.schema()`). A
    // convenience method here would borrow `&'a Schema` from the
    // batch, which entangles `Row<'a>`'s lifetime with the schema's
    // in a way that adds bookkeeping cost without saving the caller a
    // meaningful step.

    /// UTF-8 string column. Returns `None` for SQL NULL; borrows from
    /// the underlying batch.
    pub fn get_str(&self, _col: usize) -> Result<Option<&'a str>> {
        unimplemented!("PR 5")
    }

    /// 64-bit signed integer column.
    pub fn get_i64(&self, _col: usize) -> Result<Option<i64>> {
        unimplemented!("PR 5")
    }

    /// 64-bit float column.
    pub fn get_f64(&self, _col: usize) -> Result<Option<f64>> {
        unimplemented!("PR 5")
    }

    /// Boolean column.
    pub fn get_bool(&self, _col: usize) -> Result<Option<bool>> {
        unimplemented!("PR 5")
    }

    /// Binary column. Returns `None` for SQL NULL; borrows from the
    /// underlying batch.
    pub fn get_bytes(&self, _col: usize) -> Result<Option<&'a [u8]>> {
        unimplemented!("PR 5")
    }

    // PR 5: get_decimal, get_date, get_timestamp, get_interval, …
    //
    // Implementation note for PR 5: hand-rolling 14+ accessors invites
    // inconsistency in NULL handling, type-mismatch error policy, and
    // lifetime threading. Land these as a macro-derived set keyed on
    // Arrow primitive types — single source of truth for the
    // `Result<Option<T>>` shape and a single place to thread the `'a`
    // lifetime against the underlying `RecordBatch`.
}

/// Paginated result stream.
///
/// Internally wraps a `Box<dyn ResultReader>` (kernel-internal trait
/// — see `src/reader/mod.rs`). Wrappers see one concrete public type;
/// the polymorphism is hidden.
#[non_exhaustive]
pub struct ResultStream {
    /// Wrapped synchronous reader. The reader's `next_batch` is
    /// already sync and does no I/O for in-memory readers; for
    /// CloudFetch / inline readers it `block_on`s the runtime
    /// internally. PR 5 generalises this to drive truly-async
    /// fetches without `block_on`.
    reader: Box<dyn ResultReader>,

    /// Cached schema. Filled in eagerly by [`ResultStream::from_reader`]
    /// so [`ResultStream::schema`] is non-fallible.
    schema: SchemaRef,

    /// The most recent batch returned by [`ResultStream::next_batch`].
    /// Owned by the stream so the borrowed `&ResultBatch` stays valid
    /// until the next call to `next_batch` replaces it.
    current: Option<ResultBatch>,

    /// Flips to `true` once the wrapped reader returns its first
    /// `None`. Subsequent `next_batch` calls short-circuit to
    /// `Ok(None)` without re-entering the reader — saves a wasted
    /// call on readers whose `next_batch` is non-trivial (CloudFetch
    /// in particular checks state and may take a runtime tick).
    exhausted: bool,
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
    /// requested. v0 only ever wraps Arrow-producing readers, so
    /// every batch is wrapped in [`ResultBatch::Arrow`].
    pub(crate) fn from_reader(reader: Box<dyn ResultReader>) -> Result<Self> {
        let schema = reader.schema()?;
        Ok(Self {
            reader,
            schema,
            current: None,
            exhausted: false,
        })
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
    /// synchronous (it `block_on`s internally for the
    /// network-driving variants). The `async fn` signature is the
    /// **forward-compatible** API the spec mandates; PR 5 swaps the
    /// body for a real async path without breaking callers.
    pub async fn next_batch(&mut self) -> Result<Option<&ResultBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        match self.reader.next_batch()? {
            Some(rb) => {
                self.current = Some(ResultBatch::Arrow(rb));
                Ok(self.current.as_ref())
            }
            None => {
                self.current = None;
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    /// Pull the next row from the currently-buffered batch.
    ///
    /// **Sync.** Walks within the batch the most recent
    /// [`next_batch`](Self::next_batch).await populated; does no I/O.
    /// Returns `None` when the current batch is exhausted — call
    /// `next_batch().await` to load the next batch and resume.
    pub fn next_row_in_batch(&mut self) -> Option<Row<'_>> {
        unimplemented!("PR 5: implement next_row_in_batch()")
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
    /// Local-only; does not issue any server-side RPC. To cancel the
    /// underlying statement, call `cancel()` on the executed handle.
    ///
    /// Consuming `self` makes use-after-close a compile error.
    /// `Drop` performs the same cleanup if `close` was never called
    /// explicitly.
    pub fn close(self) {
        // Drops `self` (and the wrapped reader / current batch).
        // Background tasks owned by the reader stop on `Drop`.
    }
}
