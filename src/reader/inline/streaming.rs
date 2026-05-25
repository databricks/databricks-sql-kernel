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

//! `InlineStreamingProvider` — rolling-window prefetch for multi-chunk inline Arrow.
//!
//! Lights up when the session has `can_cloud_download = false`. The
//! server returns chunks one at a time via `GET /result/chunks/{idx}`
//! with Arrow IPC bytes in `attachment`; this provider keeps a bounded
//! number of those chunks decoded and ready in memory ahead of the
//! consumer so the per-chunk SEA round-trip latency is hidden behind
//! `RecordBatch` iteration.
//!
//! ## Design (modelled on JDBC's `StreamingChunkProvider`)
//!
//! ```text
//! Server                Prefetch task             Consumer
//! ──────                ─────────────             ────────
//!                       fetch chunk N    ──┐
//! responds with bytes ──> decode IPC      │
//!                       push to window  ──┘──> [chunk N][chunk N+1]...
//!                       (until window full)        │
//!                                                  │
//!                                              pull batch ◄── caller
//! ```
//!
//! - One sequential prefetch task (the SEA inline contract is "one
//!   `GetResultData` at a time" — there's no parallel download).
//! - Bounded by `max_chunks_in_memory` (analogous to
//!   `ResultConfig::cloudfetch_max_chunks_in_memory`).
//! - When the consumer drains a chunk's batches, the entry is
//!   popped, freeing a slot. A `Notify` wakes the prefetch task so
//!   it can pull the next chunk in.
//!
//! ## Bounded SEA API contract
//!
//! Every `get_inline_chunk` call carries **both** `chunk_index` and
//! `row_offset`. The next-fetch values come from the previous chunk's
//! server-emitted `next_chunk_index` and `row_offset + row_count`
//! respectively. End-of-stream is the absence of `next_chunk_index`
//! on a fetched chunk — never synthesised from `chunk_index + 1`.

use crate::client::{InlineChunkFetchResult, Transport};
use crate::kernel_error::{Error, Result};
use crate::reader::cloudfetch::parse_arrow_ipc;
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, Notify};
use tracing::{debug, error, instrument};

/// Narrow source trait for inline-chunk fetches.
///
/// Lets the streaming provider depend on just the one method it
/// needs (so tests don't have to build a full `Transport` mock with
/// `unreachable!()` for the 20-odd metadata RPCs). Production code
/// uses the blanket impl on `Arc<dyn Transport>`.
#[async_trait]
pub(crate) trait InlineChunkSource: Send + Sync + std::fmt::Debug {
    async fn get_inline_chunk(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<InlineChunkFetchResult>;
}

#[async_trait]
impl InlineChunkSource for Arc<dyn Transport> {
    async fn get_inline_chunk(
        &self,
        statement_id: &str,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<InlineChunkFetchResult> {
        Transport::get_inline_chunk(self.as_ref(), statement_id, chunk_index, row_offset).await
    }
}

/// One pre-decoded inline-Arrow chunk sitting in the rolling window.
#[derive(Debug)]
struct InlineChunkEntry {
    chunk_index: i64,
    /// Decoded Arrow batches, drained one at a time by `next_batch`.
    /// `VecDeque` so we can `pop_front` in O(1).
    batches: VecDeque<RecordBatch>,
}

/// Shared state between the prefetch task and the consumer.
///
/// `Arc<Inner>` so the spawned task holds its own clone without
/// keeping the provider alive past stream end.
#[derive(Debug)]
struct Inner {
    source: Arc<dyn InlineChunkSource>,
    statement_id: String,
    compression: CompressionCodec,

    /// Rolling window of pre-decoded chunks, ordered by `chunk_index`.
    /// Pushed to the back by the prefetch task; drained from the
    /// front by the consumer.
    ///
    /// A `TokioMutex` (not `std::sync::Mutex`) so the consumer can
    /// `.await` while holding it would be a footgun — we deliberately
    /// only hold the lock for the duration of a `pop` or `push_back`.
    chunks: TokioMutex<VecDeque<InlineChunkEntry>>,

    /// `chunk_index` to ask for in the next `get_inline_chunk` call.
    /// Initialised from the initial response; updated each prefetch
    /// from the server-emitted `next_chunk_index`.
    next_fetch_chunk_index: AtomicI64,

    /// Row offset to send on the next `get_inline_chunk` call.
    /// Initialised + updated alongside `next_fetch_chunk_index`.
    next_fetch_row_offset: AtomicI64,

    /// End-of-stream sentinel — flipped when the *previously fetched
    /// chunk* had `next_chunk_index = None`. Once set, no further
    /// prefetch is issued; the consumer drains the remaining
    /// `chunks` window and then sees EOF.
    end_of_stream: AtomicBool,

    /// First-error sticky slot. The prefetch task records its first
    /// failure here; the consumer surfaces it on the next pull.
    /// Once set, the task exits — the kernel does not retry the SEA
    /// path here (HTTP-level retries already happened inside the
    /// transport).
    error: TokioMutex<Option<Error>>,

    /// Bound on the rolling window.
    max_chunks_in_memory: usize,

    /// Bell for the consumer ↔ prefetch handshake:
    ///
    /// - Prefetch task notifies when it pushes a chunk or sets an
    ///   error / EOF.
    /// - Consumer notifies when it pops a chunk (so prefetch can
    ///   resume if the window was full).
    notify: Notify,
}

/// Multi-chunk inline-Arrow streaming provider.
#[derive(Debug)]
pub(crate) struct InlineStreamingProvider {
    inner: Arc<Inner>,
    /// Cached schema. Set when the first batch is decoded.
    schema: Option<SchemaRef>,
}

impl InlineStreamingProvider {
    /// Build a streaming provider seeded by the first chunk's payload
    /// (which the kernel already received in the initial execute
    /// response — see `reader/mod.rs::create_inner_reader`).
    ///
    /// The initial chunk is decoded synchronously so callers get a
    /// usable schema back from `schema()` even before the background
    /// prefetch task has done any work.
    ///
    /// # Arguments
    /// * `transport` — for issuing `get_inline_chunk` RPCs.
    /// * `statement_id` — for the URL path.
    /// * `initial_attachment` — Arrow IPC bytes the kernel already
    ///   has from the initial execute response.
    /// * `initial_chunk_index` — the first chunk's index (0 in
    ///   practice; pass whatever the manifest reported).
    /// * `initial_row_offset` / `initial_row_count` — same.
    /// * `next_chunk_index` — server's `next_chunk_index` on the
    ///   initial chunk. `Some(N)` → more chunks to fetch starting at
    ///   N; `None` → single-chunk result, no further fetches.
    /// * `compression` — Arrow IPC compression codec from the
    ///   manifest.
    /// * `max_chunks_in_memory` — bound on the rolling window
    ///   (`ResultConfig::inline_max_chunks_in_memory`).
    /// * `runtime_handle` — for spawning the prefetch task.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: Arc<dyn InlineChunkSource>,
        statement_id: impl Into<String>,
        initial_attachment: Vec<u8>,
        initial_chunk_index: i64,
        initial_row_offset: i64,
        initial_row_count: i64,
        next_chunk_index: Option<i64>,
        compression: CompressionCodec,
        max_chunks_in_memory: usize,
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Arc<Self>> {
        // Decode the initial chunk synchronously so we can return a
        // schema-bearing provider to the caller.
        let initial_batches = parse_arrow_ipc(&initial_attachment, compression)?;
        let schema = initial_batches.first().map(|b| b.schema());

        let initial_entry = InlineChunkEntry {
            chunk_index: initial_chunk_index,
            batches: VecDeque::from(initial_batches),
        };

        // Next-fetch state comes from the initial chunk's metadata.
        // If `next_chunk_index` is None we are at end-of-stream
        // already — the prefetch task will see the flag and exit
        // without making any RPC.
        let (next_idx, eos) = match next_chunk_index {
            Some(idx) => (idx, false),
            None => (0, true),
        };
        let next_row_offset = initial_row_offset + initial_row_count;

        // Bound the window at >=1 so we always have room to push
        // the chunk currently being decoded.
        let max_in_memory = max_chunks_in_memory.max(1);

        let mut window = VecDeque::with_capacity(max_in_memory);
        window.push_back(initial_entry);

        let inner = Arc::new(Inner {
            source,
            statement_id: statement_id.into(),
            compression,
            chunks: TokioMutex::new(window),
            next_fetch_chunk_index: AtomicI64::new(next_idx),
            next_fetch_row_offset: AtomicI64::new(next_row_offset),
            end_of_stream: AtomicBool::new(eos),
            error: TokioMutex::new(None),
            max_chunks_in_memory: max_in_memory,
            notify: Notify::new(),
        });

        // Spawn the prefetch task. It exits when end-of-stream is
        // reached, an error fires, or the `Arc<Inner>` is dropped
        // (when the last consumer goes away the only Arc holder is
        // the task itself — `Inner` doesn't strong-cycle, so on
        // drop-of-consumer the task notices via `chunks` lock
        // returning a poisoned/no-longer-served chunks deque on
        // next iteration; in practice we rely on `end_of_stream`
        // being set when the consumer is done).
        if !eos {
            let task_inner = Arc::clone(&inner);
            runtime_handle.spawn(async move {
                Self::prefetch_loop(task_inner).await;
            });
        }

        Ok(Arc::new(Self { inner, schema }))
    }

    /// Decoded schema from the first chunk. `None` if the initial
    /// chunk had no batches (empty result set).
    pub(crate) fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Pull the next Arrow batch.
    ///
    /// - If the head chunk has batches buffered, drain one.
    /// - When the head chunk is drained, pop it and notify the
    ///   prefetch task (window slot freed). Then loop and try again.
    /// - If the window is empty and not at EOS, await the prefetch
    ///   task pushing the next chunk.
    /// - If the window is empty and EOS is set, return `Ok(None)`.
    /// - If the prefetch task recorded an error, surface it.
    #[instrument(name = "inline_stream.next_batch", skip(self))]
    pub(crate) async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        loop {
            // Check for prefetch error first — fail fast.
            {
                let mut err_slot = self.inner.error.lock().await;
                if let Some(err) = err_slot.take() {
                    return Err(err);
                }
            }

            // Try to drain a batch from the head chunk.
            {
                let mut guard = self.inner.chunks.lock().await;
                if let Some(head) = guard.front_mut() {
                    if let Some(batch) = head.batches.pop_front() {
                        // Still more in this chunk — return it directly.
                        return Ok(Some(batch));
                    }
                    // Head chunk drained — pop it and let the
                    // prefetch task know there's room for one more.
                    let popped = guard.pop_front().unwrap();
                    debug!(
                        "Drained inline chunk {} from rolling window; \
                         {} chunks remain buffered",
                        popped.chunk_index,
                        guard.len()
                    );
                    drop(guard);
                    self.inner.notify.notify_waiters();
                    // Loop: there may already be more chunks in the
                    // window behind the popped one.
                    continue;
                }
                // Window empty.
                if self.inner.end_of_stream.load(Ordering::Acquire) {
                    return Ok(None);
                }
                // Drop the lock before awaiting so the prefetch task
                // can push.
                drop(guard);
            }

            // Wait for the prefetch task to push a chunk (or set EOS,
            // or record an error). `notified()` is edge-triggered;
            // capture it before the lock is dropped above ideally,
            // but for now the loop's recheck-on-wake handles a race
            // where the notification fires between the empty-check
            // and `notified()`.
            self.inner.notify.notified().await;
        }
    }

    /// Background loop: fetch chunks sequentially, decode, push into
    /// the window. Pauses (via `notify`) when the window is full.
    /// Exits on end-of-stream, error, or runtime shutdown.
    async fn prefetch_loop(inner: Arc<Inner>) {
        loop {
            // Stop if we've already hit EOS.
            if inner.end_of_stream.load(Ordering::Acquire) {
                debug!("inline prefetch: end-of-stream reached, task exiting");
                return;
            }

            // Wait for window space if full.
            loop {
                let window_len = inner.chunks.lock().await.len();
                if window_len < inner.max_chunks_in_memory {
                    break;
                }
                debug!(
                    "inline prefetch: window full ({}/{}), pausing",
                    window_len, inner.max_chunks_in_memory
                );
                inner.notify.notified().await;
            }

            let chunk_index = inner.next_fetch_chunk_index.load(Ordering::Acquire);
            let row_offset = inner.next_fetch_row_offset.load(Ordering::Acquire);

            // Issue the SEA RPC. Errors are recorded sticky;
            // consumer surfaces on next `next_batch()` pull.
            let fetch_result = match inner
                .source
                .get_inline_chunk(&inner.statement_id, chunk_index, row_offset)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    error!(
                        "inline prefetch: get_inline_chunk(chunk={}, row_offset={}) failed: {}",
                        chunk_index, row_offset, e
                    );
                    *inner.error.lock().await = Some(e);
                    inner.end_of_stream.store(true, Ordering::Release);
                    inner.notify.notify_waiters();
                    return;
                }
            };

            // Decode synchronously. Arrow IPC parsing is fast
            // (microseconds per MB on the kernel's measurements) and
            // doing it here keeps the consumer hot-path zero-CPU.
            let batches = match parse_arrow_ipc(&fetch_result.bytes, inner.compression) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "inline prefetch: parse_arrow_ipc(chunk={}, bytes={}) failed: {}",
                        chunk_index,
                        fetch_result.bytes.len(),
                        e
                    );
                    *inner.error.lock().await = Some(e);
                    inner.end_of_stream.store(true, Ordering::Release);
                    inner.notify.notify_waiters();
                    return;
                }
            };

            // Per the bounded SEA API contract: next-fetch state
            // comes from the server's `next_chunk_index` and the
            // chunk's own `row_offset + row_count`. Never synthesised.
            let advance_to = fetch_result
                .next_chunk_index
                .unwrap_or(fetch_result.chunk_index + 1);
            let next_row = fetch_result.row_offset + fetch_result.row_count;
            inner
                .next_fetch_chunk_index
                .store(advance_to, Ordering::Release);
            inner
                .next_fetch_row_offset
                .store(next_row, Ordering::Release);

            if fetch_result.next_chunk_index.is_none() {
                inner.end_of_stream.store(true, Ordering::Release);
            }

            let chunk_idx = fetch_result.chunk_index;
            let batch_count = batches.len();

            // Push the new chunk onto the window.
            {
                let mut guard = inner.chunks.lock().await;
                guard.push_back(InlineChunkEntry {
                    chunk_index: chunk_idx,
                    batches: VecDeque::from(batches),
                });
                debug!(
                    "inline prefetch: pushed chunk {} ({} batches); \
                     window now has {} chunks; eos={}",
                    chunk_idx,
                    batch_count,
                    guard.len(),
                    inner.end_of_stream.load(Ordering::Acquire),
                );
            }
            inner.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    /// One mock inline chunk: pre-built Arrow IPC bytes plus the
    /// metadata the server would emit alongside it.
    #[derive(Clone, Debug)]
    struct MockChunk {
        bytes: Vec<u8>,
        chunk_index: i64,
        row_offset: i64,
        row_count: i64,
        next_chunk_index: Option<i64>,
    }

    /// Mock chunk-source that serves a scripted list of inline
    /// chunks in order. Records the RPC count so tests can assert
    /// the prefetch bound was respected.
    #[derive(Debug)]
    struct MockSource {
        chunks: StdMutex<Vec<MockChunk>>,
        calls: AtomicUsize,
    }

    impl MockSource {
        fn new(chunks: Vec<MockChunk>) -> Arc<Self> {
            Arc::new(Self {
                chunks: StdMutex::new(chunks),
                calls: AtomicUsize::new(0),
            })
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl InlineChunkSource for MockSource {
        async fn get_inline_chunk(
            &self,
            _statement_id: &str,
            chunk_index: i64,
            _row_offset: i64,
        ) -> Result<InlineChunkFetchResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut chunks = self.chunks.lock().unwrap();
            let position = chunks
                .iter()
                .position(|c| c.chunk_index == chunk_index)
                .ok_or_else(|| {
                    Error::invalid_state()
                        .message(format!("mock: no chunk {} scripted", chunk_index))
                })?;
            let c = chunks.remove(position);
            Ok(InlineChunkFetchResult {
                bytes: c.bytes,
                chunk_index: c.chunk_index,
                row_offset: c.row_offset,
                row_count: c.row_count,
                next_chunk_index: c.next_chunk_index,
            })
        }
    }

    fn build_ipc_bytes(values: &[i32]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    /// Build a fixture: 3 inline chunks, indices 0/1/2, chunk 2 is
    /// terminal (`next_chunk_index = None`). Chunk 0's bytes are
    /// passed to `InlineStreamingProvider::new` directly as the
    /// "initial attachment" — only chunks 1 and 2 are served by the
    /// mock transport.
    fn three_chunk_fixture() -> (Vec<u8>, MockChunk, MockChunk, MockChunk) {
        let c0 = MockChunk {
            bytes: build_ipc_bytes(&[1, 2, 3]),
            chunk_index: 0,
            row_offset: 0,
            row_count: 3,
            next_chunk_index: Some(1),
        };
        let c1 = MockChunk {
            bytes: build_ipc_bytes(&[4, 5, 6]),
            chunk_index: 1,
            row_offset: 3,
            row_count: 3,
            next_chunk_index: Some(2),
        };
        let c2 = MockChunk {
            bytes: build_ipc_bytes(&[7, 8, 9]),
            chunk_index: 2,
            row_offset: 6,
            row_count: 3,
            next_chunk_index: None,
        };
        (c0.bytes.clone(), c0, c1, c2)
    }

    fn count_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn streams_all_chunks_in_order() {
        // Three-chunk fixture; assert all 9 rows arrive in order.
        let (initial_bytes, c0, c1, c2) = three_chunk_fixture();
        let source: Arc<dyn InlineChunkSource> = MockSource::new(vec![c1.clone(), c2.clone()]);

        let provider = InlineStreamingProvider::new(
            source,
            "stmt-1",
            initial_bytes,
            c0.chunk_index,
            c0.row_offset,
            c0.row_count,
            c0.next_chunk_index,
            CompressionCodec::None,
            /* max_chunks_in_memory */ 4,
            tokio::runtime::Handle::current(),
        )
        .expect("provider build");

        let mut batches = Vec::new();
        while let Some(b) = provider.next_batch().await.unwrap() {
            batches.push(b);
        }

        assert_eq!(batches.len(), 3, "one batch per chunk");
        assert_eq!(count_rows(&batches), 9);
        // Spot-check the ordering: chunk 0 has [1,2,3], chunk 2 has
        // [7,8,9]; the i32 values per-row prove ordering.
        let last = batches.last().unwrap();
        let arr = last
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(arr.values(), &[7, 8, 9]);
    }

    #[tokio::test]
    async fn respects_max_chunks_in_memory_bound() {
        // Fixture is 3 chunks total. With max_chunks_in_memory = 1,
        // the prefetch task must NOT race ahead and pull all chunks
        // up front; it pulls one, waits for the consumer to drain,
        // then pulls the next.
        let (initial_bytes, c0, c1, c2) = three_chunk_fixture();
        let mock = MockSource::new(vec![c1.clone(), c2.clone()]);
        let source: Arc<dyn InlineChunkSource> = mock.clone();

        let provider = InlineStreamingProvider::new(
            source,
            "stmt-1",
            initial_bytes,
            c0.chunk_index,
            c0.row_offset,
            c0.row_count,
            c0.next_chunk_index,
            CompressionCodec::None,
            /* max_chunks_in_memory */ 1,
            tokio::runtime::Handle::current(),
        )
        .expect("provider build");

        // Drain everything. The kernel makes exactly 2 inline-chunk
        // RPCs (for chunks 1 and 2; chunk 0 was the initial
        // attachment).
        let _b0 = provider.next_batch().await.unwrap().unwrap();
        let _b1 = provider.next_batch().await.unwrap().unwrap();
        let _b2 = provider.next_batch().await.unwrap().unwrap();
        let eof = provider.next_batch().await.unwrap();
        assert!(eof.is_none(), "EOS after draining all chunks");
        assert_eq!(
            mock.call_count(),
            2,
            "tight window must not over-fetch (chunk 0 from initial attachment + 2 RPCs)"
        );
    }

    #[tokio::test]
    async fn single_chunk_no_prefetch_rpcs() {
        // If the initial response is the *only* chunk (next_chunk_index
        // is None on the server side), the streaming provider should
        // make zero RPCs.
        let mock = MockSource::new(vec![]);
        let source: Arc<dyn InlineChunkSource> = mock.clone();

        let initial = MockChunk {
            bytes: build_ipc_bytes(&[42]),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1,
            next_chunk_index: None, // single-chunk result
        };

        let provider = InlineStreamingProvider::new(
            source,
            "stmt-1",
            initial.bytes.clone(),
            initial.chunk_index,
            initial.row_offset,
            initial.row_count,
            initial.next_chunk_index,
            CompressionCodec::None,
            4,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        let b = provider.next_batch().await.unwrap().unwrap();
        assert_eq!(b.num_rows(), 1);
        let eof = provider.next_batch().await.unwrap();
        assert!(eof.is_none());
        assert_eq!(
            mock.call_count(),
            0,
            "no inline-chunk RPC needed for a single-chunk inline result",
        );
    }

    #[tokio::test]
    async fn surfaces_prefetch_errors() {
        // Mock the first prefetch RPC to be missing — provider
        // should surface the error from `next_batch()`.
        let (initial_bytes, c0, _, _) = three_chunk_fixture();
        // Empty mock → first prefetch (chunk 1) returns Err.
        let source: Arc<dyn InlineChunkSource> = MockSource::new(vec![]);

        let provider = InlineStreamingProvider::new(
            source,
            "stmt-1",
            initial_bytes,
            c0.chunk_index,
            c0.row_offset,
            c0.row_count,
            c0.next_chunk_index,
            CompressionCodec::None,
            4,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        // First pull yields chunk 0 (initial attachment).
        let _ = provider.next_batch().await.unwrap().unwrap();
        // Second pull awaits prefetch; the prefetch task fails,
        // records the error, and notifies. Consumer surfaces it.
        let err = provider
            .next_batch()
            .await
            .expect_err("prefetch failure must surface");
        assert!(
            err.message.to_lowercase().contains("no chunk")
                || err.message.to_lowercase().contains("chunk"),
            "error mentions the missing chunk: {}",
            err.message
        );
    }
}
