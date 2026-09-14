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
//! Lights up when CloudFetch is disabled (`cloudfetch_enabled = false`,
//! i.e. the execute request was sent with SEA `disposition = INLINE`).
//! The server returns chunks one at a time via `GET /result/chunks/{idx}`
//! with Arrow IPC bytes in `attachment`; this provider keeps a bounded
//! number of those chunks decoded and ready in memory ahead of the
//! consumer so the per-chunk SEA round-trip latency is hidden behind
//! `RecordBatch` iteration.
//!
//! ## Coordination
//!
//! The producer (background prefetch task) and the consumer (kernel
//! caller pulling batches) communicate through a bounded
//! `tokio::sync::mpsc` channel:
//!
//! - Sender: prefetch task pushes one `Result<InlineChunkEntry>` per
//!   chunk. Channel capacity gives the rolling-window bound.
//! - Receiver: consumer awaits one entry at a time, then walks its
//!   batches synchronously.
//!
//! Why mpsc and not `Notify`: `tokio::sync::Notify::notify_waiters()`
//! does NOT buffer permits. If the consumer drops a lock and the
//! prefetch pushes+notifies before the consumer calls
//! `.notified().await`, the wake is lost and the consumer hangs
//! forever. mpsc's `send`/`recv` handle the wake correctly out of the
//! box and add natural backpressure when the channel fills.
//!
//! Why one channel of `Result<...>` instead of separate data/error
//! signals: errors become sticky for free — the prefetch task drops
//! its sender after sending the error, so the receiver gets the
//! `Err`, and every subsequent `recv()` returns `None` (channel
//! closed) which we map to a recorded sticky error so consumers can't
//! "scroll past" a transport failure.
//!
//! ## Cancellation / cleanup
//!
//! The provider stores the `JoinHandle` of its prefetch task and
//! aborts it in `Drop`. Dropping the receiver also makes
//! `Sender::send().await` return `Err`, causing the task's next loop
//! iteration to exit naturally; the explicit `abort()` covers the
//! case where the task is blocked on the SEA RPC itself.
//!
//! ## Bounded SEA API contract
//!
//! Every `get_inline_chunk` call carries **both** `chunk_index` and
//! `row_offset`. The next-fetch values come from the previous chunk's
//! server-emitted `next_chunk_index` and `row_offset + row_count`
//! respectively. End-of-stream is the absence of `next_chunk_index`
//! on a fetched chunk — never synthesised from `chunk_index + 1`.

use crate::client::InlineChunkFetchResult;
use crate::kernel_error::{Error, Result};
use crate::klog::{debug, error};
use crate::reader::cloudfetch::parse_arrow_ipc;
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tracing::instrument;

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

/// One pre-decoded inline-Arrow chunk sitting in the rolling window.
#[derive(Debug)]
struct InlineChunkEntry {
    chunk_index: i64,
    /// Decoded Arrow batches. Iterating left to right; we drain by
    /// `.into_iter()` once we own the entry on the consumer side, so
    /// no shared interior mutability is needed.
    batches: Vec<RecordBatch>,
}

/// Multi-chunk inline-Arrow streaming provider.
///
/// Holds the receiver for the prefetch task's chunk channel and a
/// sticky-error slot for surfacing prefetch failures to callers that
/// have already observed at least one error.
pub(crate) struct InlineStreamingProvider {
    /// Consumer state. `TokioMutex` because `next_batch(&self)` takes
    /// `&self` for ergonomics (the kernel's `ResultReader` trait takes
    /// `&mut self`, but we wrap this in an `Arc<InlineStreamingProvider>`
    /// for spawned-task safety on the prefetch side; consumers are
    /// single-threaded in practice).
    consumer: TokioMutex<ConsumerState>,

    /// Cached schema. Set on construction from either the initial
    /// chunk's first batch OR — when the initial chunk is empty —
    /// the manifest. `None` only when both sources are empty (legal
    /// for a truly empty result set).
    schema: Option<SchemaRef>,

    /// JoinHandle of the prefetch task. `Some(handle)` while the
    /// task is running; `take()`n and `abort()`ed in `Drop`.
    prefetch_handle: TokioMutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for InlineStreamingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineStreamingProvider")
            .field("schema_present", &self.schema.is_some())
            .finish_non_exhaustive()
    }
}

/// Consumer-side mutable state. Wrapped in `TokioMutex` so
/// `next_batch(&self)` can mutate.
struct ConsumerState {
    /// Channel of chunks pushed by the prefetch task.
    rx: mpsc::Receiver<Result<InlineChunkEntry>>,
    /// The current chunk we're walking through (if any).
    current: Option<InlineChunkEntry>,
    /// Index into `current.batches` for the next batch to yield.
    cursor: usize,
    /// Sticky-error slot. Once the prefetch task surfaces an error
    /// (or the channel closes prematurely), this is set and every
    /// subsequent `next_batch()` call returns the same error rather
    /// than silently transitioning to "successful EOS". Cloned for
    /// each surfacing so the slot itself survives.
    sticky_error: Option<Error>,
}

impl InlineStreamingProvider {
    /// Build a streaming provider seeded by the first chunk's payload
    /// (which the kernel already received in the initial execute
    /// response — see `reader/mod.rs::create_inner_reader`).
    ///
    /// # Arguments
    /// * `source` — for issuing `get_inline_chunk` RPCs.
    /// * `statement_id` — for the URL path.
    /// * `initial_attachment` — Arrow IPC bytes the kernel already
    ///   has from the initial execute response.
    /// * `initial_chunk_index` / `initial_row_offset` /
    ///   `initial_row_count` — initial chunk metadata.
    /// * `next_chunk_index` — server's `next_chunk_index` on the
    ///   initial chunk. `Some(N)` → more chunks to fetch starting at
    ///   N; `None` → single-chunk result, no further fetches.
    /// * `compression` — Arrow IPC compression codec from the
    ///   manifest.
    /// * `max_chunks_in_memory` — bound on the rolling window.
    /// * `manifest_schema` — fallback schema extracted from the
    ///   result manifest; used when the initial chunk decodes to
    ///   zero batches (legal on paginated results where the first
    ///   chunk is intentionally tiny). Pass `None` if no manifest
    ///   is available.
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
        manifest_schema: Option<SchemaRef>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Arc<Self>> {
        // Decode the initial chunk synchronously so we can return a
        // schema-bearing provider to the caller.
        let initial_batches = parse_arrow_ipc(&initial_attachment, compression)?;

        // Schema source priority:
        // 1. The first batch of the initial chunk (most-authoritative,
        //    matches what consumers will actually iterate).
        // 2. The manifest-derived schema (load-bearing for
        //    paginated results where the initial chunk has zero
        //    batches — without this fallback,
        //    `ResultReaderAdapter::new` rejects the stream).
        let schema = initial_batches
            .first()
            .map(|b| b.schema())
            .or(manifest_schema);

        let initial_entry = InlineChunkEntry {
            chunk_index: initial_chunk_index,
            batches: initial_batches,
        };

        // Bound the window at >=1 so the channel can always accept
        // at least one chunk from the prefetch task even if the
        // caller passed 0 by mistake.
        let max_in_memory = max_chunks_in_memory.max(1);

        // Channel capacity = max_in_memory - 1: the consumer always
        // holds one chunk in `current`, so total chunks in memory =
        // 1 (current) + channel.len() ≤ max_in_memory. mpsc requires
        // capacity ≥ 1, so saturate at 1.
        let channel_cap = max_in_memory.saturating_sub(1).max(1);
        let (tx, rx) = mpsc::channel::<Result<InlineChunkEntry>>(channel_cap);

        let consumer = ConsumerState {
            rx,
            current: Some(initial_entry),
            cursor: 0,
            sticky_error: None,
        };

        let prefetch_handle = if let Some(initial_next_idx) = next_chunk_index {
            let statement_id = statement_id.into();
            let initial_next_row = initial_row_offset.saturating_add(initial_row_count);
            let task = PrefetchTask {
                source,
                statement_id,
                compression,
                tx,
                next_chunk_index: initial_next_idx,
                next_row_offset: initial_next_row,
            };
            Some(runtime_handle.spawn(async move {
                task.run().await;
            }))
        } else {
            // Single-chunk result: no prefetch task to spawn. Drop
            // the sender so the receiver sees channel-closed
            // immediately when it tries to recv after draining the
            // initial chunk.
            drop(tx);
            None
        };

        Ok(Arc::new(Self {
            consumer: TokioMutex::new(consumer),
            schema,
            prefetch_handle: TokioMutex::new(prefetch_handle),
        }))
    }

    /// Decoded schema. `None` only when the initial chunk had zero
    /// batches AND no manifest schema was provided — the truly-empty
    /// result case.
    pub(crate) fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Pull the next Arrow batch.
    ///
    /// Semantics:
    /// - Drains batches from the current chunk first.
    /// - When current is drained, awaits the next chunk from the
    ///   prefetch channel.
    /// - If the prefetch task surfaced an error (via the channel),
    ///   the error is stored sticky: every subsequent
    ///   `next_batch()` call returns the same error rather than
    ///   silently transitioning to `Ok(None)`. This matches JDBC's
    ///   `StreamingChunkProvider` semantics.
    /// - End-of-stream → `Ok(None)`.
    #[instrument(name = "inline_stream.next_batch", skip(self))]
    pub(crate) async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        let mut state = self.consumer.lock().await;

        loop {
            // Drain a batch from the current chunk first — even if
            // the prefetch task has set a sticky error, the
            // already-buffered batches were successfully fetched
            // and the consumer should receive them before the
            // error surfaces. Only when no more buffered data is
            // available do we consult the sticky-error slot or the
            // channel.
            if let Some(ref current) = state.current {
                if state.cursor < current.batches.len() {
                    let batch = current.batches[state.cursor].clone();
                    state.cursor += 1;
                    return Ok(Some(batch));
                }
                // Current chunk fully drained; advance.
                debug!(
                    "Drained inline chunk {} ({} batches)",
                    current.chunk_index,
                    current.batches.len(),
                );
                state.current = None;
                state.cursor = 0;
            }

            // Buffered chunk is drained. Sticky-error short-
            // circuit: once we've surfaced an error, every
            // subsequent call returns the same error. Consumers
            // cannot scroll past a transport failure.
            if let Some(ref e) = state.sticky_error {
                return Err(e.clone());
            }

            // Need to recv the next chunk from the prefetch task.
            match state.rx.recv().await {
                Some(Ok(entry)) => {
                    state.current = Some(entry);
                    // Loop back to drain.
                }
                Some(Err(e)) => {
                    // Store sticky and surface on this call.
                    state.sticky_error = Some(e.clone());
                    return Err(e);
                }
                None => {
                    // Channel closed: clean EOS.
                    return Ok(None);
                }
            }
        }
    }
}

impl Drop for InlineStreamingProvider {
    fn drop(&mut self) {
        // Abort the prefetch task so it stops issuing RPCs the
        // consumer can no longer observe. The task may have already
        // exited (EOS, error, or runtime shutdown); `abort()` on a
        // finished JoinHandle is a no-op.
        //
        // Note: this `Drop` is sync, so we can't `.lock().await`. We
        // use `try_lock` which succeeds because no other task can
        // hold the lock at drop time (the provider's `Arc` count is
        // zero, otherwise we wouldn't be in `Drop`).
        if let Ok(mut guard) = self.prefetch_handle.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

/// Self-contained state for the background prefetch task. Holds
/// everything the task needs so the loop body has no shared
/// state with the consumer beyond the `tx` channel — easy to
/// reason about, easy to abort.
struct PrefetchTask {
    source: Arc<dyn InlineChunkSource>,
    statement_id: String,
    compression: CompressionCodec,
    tx: mpsc::Sender<Result<InlineChunkEntry>>,
    next_chunk_index: i64,
    next_row_offset: i64,
}

impl PrefetchTask {
    async fn run(mut self) {
        loop {
            // The send path naturally pauses when the channel is
            // full — that's our rolling-window backpressure. If
            // the receiver has been dropped (consumer gone),
            // `send().await` returns `Err` and we exit cleanly.
            let chunk_index = self.next_chunk_index;
            let row_offset = self.next_row_offset;

            let fetch_result = match self
                .source
                .get_inline_chunk(&self.statement_id, chunk_index, row_offset)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    error!(
                        "inline prefetch: get_inline_chunk(chunk={}, row_offset={}) failed: {}",
                        chunk_index, row_offset, e
                    );
                    let _ = self.tx.send(Err(e)).await;
                    return;
                }
            };

            // Validate the server response. Self-loops and
            // missing `next_chunk_index` on a non-terminal chunk
            // would otherwise spin the prefetch loop forever.
            //
            // Strict-monotonic continuation: server-provided
            // `next_chunk_index` MUST be > current chunk_index.
            // Equal or less means the server emitted a self-loop
            // (a server bug). Surface as a hard error sticky on
            // the channel; do not synthesise our way past it.
            if let Some(next) = fetch_result.next_chunk_index {
                if next <= fetch_result.chunk_index {
                    let err = Error::data_loss().message(format!(
                        "Server returned non-monotonic next_chunk_index: \
                         chunk={} next_chunk_index={}; refusing to spin",
                        fetch_result.chunk_index, next,
                    ));
                    error!("inline prefetch: {}", err);
                    let _ = self.tx.send(Err(err)).await;
                    return;
                }
            }

            // Decode synchronously. Arrow IPC parsing is fast and
            // doing it here keeps the consumer hot-path zero-CPU.
            //
            // **Performance note:** on the current-thread tokio
            // runtime, large LZ4-compressed chunks could starve
            // the consumer task during decode. Today's perf
            // measurements (per PR #67) show ~1-5 ms per 20 MB
            // uncompressed Arrow IPC chunk — well inside the
            // typical 10 ms responsiveness budget. If LZ4 push
            // that into the tens-of-ms range, wrap this in
            // `tokio::task::spawn_blocking` (the cost is one
            // extra mpsc-channel-hop's worth of latency). Defer
            // pending measurements that justify the complexity.
            let batches = match parse_arrow_ipc(&fetch_result.bytes, self.compression) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "inline prefetch: parse_arrow_ipc(chunk={}, bytes={}) failed: {}",
                        chunk_index,
                        fetch_result.bytes.len(),
                        e
                    );
                    let _ = self.tx.send(Err(e)).await;
                    return;
                }
            };

            let chunk_idx = fetch_result.chunk_index;
            let batch_count = batches.len();
            let server_next_idx = fetch_result.next_chunk_index;

            // Send the chunk to the consumer. If the consumer has
            // dropped (Provider::drop ran), `send` returns Err
            // and we exit.
            if self
                .tx
                .send(Ok(InlineChunkEntry {
                    chunk_index: chunk_idx,
                    batches,
                }))
                .await
                .is_err()
            {
                debug!(
                    "inline prefetch: consumer dropped while sending chunk {}; \
                     task exiting",
                    chunk_idx,
                );
                return;
            }

            debug!(
                "inline prefetch: pushed chunk {} ({} batches); \
                 next_chunk_index={:?}",
                chunk_idx, batch_count, server_next_idx,
            );

            // Advance next-fetch cursors. Per the bounded SEA API
            // contract: the kernel MUST NOT synthesise `chunk_index
            // + 1` as the continuation. We use the server-emitted
            // `next_chunk_index` directly and stop on `None`.
            match server_next_idx {
                Some(idx) => {
                    self.next_chunk_index = idx;
                    self.next_row_offset = fetch_result
                        .row_offset
                        .saturating_add(fetch_result.row_count);
                }
                None => {
                    // End-of-stream signal from server. Drop the
                    // sender so the receiver gets channel-closed
                    // on its next recv() call — clean EOS.
                    debug!(
                        "inline prefetch: end-of-stream after chunk {}; closing channel",
                        chunk_idx,
                    );
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    #[derive(Clone, Debug)]
    struct MockChunk {
        bytes: Vec<u8>,
        chunk_index: i64,
        row_offset: i64,
        row_count: i64,
        next_chunk_index: Option<i64>,
    }

    /// Mock chunk-source. Records the RPC count so tests can assert
    /// the bound was respected.
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

    fn build_empty_ipc_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

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
            4,
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        let mut batches = Vec::new();
        while let Some(b) = provider.next_batch().await.unwrap() {
            batches.push(b);
        }

        assert_eq!(batches.len(), 3, "one batch per chunk");
        assert_eq!(count_rows(&batches), 9);
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
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

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
        let mock = MockSource::new(vec![]);
        let source: Arc<dyn InlineChunkSource> = mock.clone();

        let initial = MockChunk {
            bytes: build_ipc_bytes(&[42]),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1,
            next_chunk_index: None,
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
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        let b = provider.next_batch().await.unwrap().unwrap();
        assert_eq!(b.num_rows(), 1);
        let eof = provider.next_batch().await.unwrap();
        assert!(eof.is_none());
        assert_eq!(mock.call_count(), 0);
    }

    #[tokio::test]
    async fn surfaces_prefetch_errors() {
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
            None,
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
            err.message.to_lowercase().contains("chunk"),
            "error mentions the missing chunk: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn sticky_error_persists_across_calls() {
        // P1 #3 regression guard. After an error is surfaced once,
        // subsequent next_batch() calls must return the same error,
        // NOT silently transition to Ok(None) (which would lie to
        // consumers about result completeness).
        let (initial_bytes, c0, _, _) = three_chunk_fixture();
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
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Drain chunk 0.
        let _ = provider.next_batch().await.unwrap().unwrap();
        // First failed call.
        let err1 = provider.next_batch().await.unwrap_err();
        // Subsequent calls must keep failing.
        let err2 = provider.next_batch().await.unwrap_err();
        let err3 = provider.next_batch().await.unwrap_err();
        assert_eq!(err1.message, err2.message);
        assert_eq!(err2.message, err3.message);
    }

    #[tokio::test]
    async fn rejects_non_monotonic_next_chunk_index() {
        // P1 #4 regression guard. If the server emits a self-loop
        // (`next_chunk_index <= chunk_index`), the prefetch task
        // would otherwise spin forever. Must surface as a sticky
        // data_loss error.
        let (initial_bytes, c0, _, _) = three_chunk_fixture();
        let bad_chunk = MockChunk {
            bytes: build_ipc_bytes(&[10]),
            chunk_index: 1,
            row_offset: 3,
            row_count: 1,
            next_chunk_index: Some(1), // self-loop
        };
        let source: Arc<dyn InlineChunkSource> = MockSource::new(vec![bad_chunk]);

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
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Drain chunk 0 (initial attachment) — succeeds.
        let _ = provider.next_batch().await.unwrap().unwrap();
        // The prefetch task validates `next_chunk_index >
        // chunk_index` BEFORE sending the chunk through the
        // channel — a self-loop is fail-fast, the consumer never
        // sees chunk 1's data. The next pull surfaces the error.
        let err = provider.next_batch().await.unwrap_err();
        assert!(
            err.message
                .to_lowercase()
                .contains("non-monotonic next_chunk_index"),
            "error should call out the self-loop: {}",
            err.message,
        );
    }

    #[tokio::test]
    async fn manifest_schema_fallback_when_initial_chunk_empty() {
        // P1 #8 regression guard. When the initial chunk decodes to
        // zero batches (legal on paginated results), the provider
        // must fall back to the manifest-derived schema. Without
        // the fallback, schema() returns None forever and
        // ResultReaderAdapter::new rejects the stream.
        let manifest_schema = Arc::new(Schema::new(vec![
            Field::new("colA", DataType::Int32, false),
            Field::new("colB", DataType::Int32, true),
        ]));

        let empty_bytes = build_empty_ipc_bytes();
        let source: Arc<dyn InlineChunkSource> = MockSource::new(vec![]);

        let provider = InlineStreamingProvider::new(
            source,
            "stmt-1",
            empty_bytes,
            0,
            0,
            0,
            None, // single-chunk empty result
            CompressionCodec::None,
            4,
            Some(manifest_schema.clone()),
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        let schema = provider
            .schema()
            .expect("manifest fallback must produce a schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "colA");
        assert_eq!(schema.field(1).name(), "colB");
    }

    #[tokio::test]
    async fn prefetch_task_exits_when_consumer_drops() {
        // P0 #2 regression guard. The prefetch task must not keep
        // RPC-ing after the consumer has dropped the provider.
        // mpsc-based design gives us this for free: when the
        // receiver drops, the next `tx.send().await` returns Err
        // and the task exits.
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
            /* max_chunks_in_memory = */ 1,
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Pull only the first batch (initial attachment); the
        // prefetch task is now blocked trying to send chunk 1 into
        // the bounded channel.
        let _ = provider.next_batch().await.unwrap().unwrap();
        // Drop the provider. The Drop impl aborts the prefetch
        // task; the mpsc channel close path would also exit it on
        // the next loop iteration.
        drop(provider);

        // Give tokio a moment to actually run the abort. Without
        // this yield the test would be flaky because abort() is
        // asynchronous (it sets a cancellation flag the task
        // checks at await points).
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // The prefetch task at most made one more RPC (the one in
        // flight when we dropped). It must not have made more.
        let calls_after_drop = mock.call_count();
        // Give a generous timeout — if the task were really still
        // running, it would have made ALL the remaining calls (2
        // in this fixture) in the next few millis.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let calls_after_wait = mock.call_count();
        assert!(
            calls_after_wait <= calls_after_drop + 1,
            "prefetch task kept RPC-ing after consumer drop: \
             {} calls right after drop, {} after a 50ms wait",
            calls_after_drop,
            calls_after_wait,
        );
    }

    #[tokio::test]
    async fn lost_wake_race_does_not_hang() {
        // P0 #1 regression guard. With the old Notify-based
        // implementation, a tight push/pop loop could lose the
        // wake-up between the consumer's empty-check and the
        // notified().await. With mpsc, recv() handles the wake
        // correctly and this test completes cleanly.
        //
        // We exercise the race by setting max_chunks_in_memory = 1
        // (channel capacity 1) and draining as fast as possible.
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
            1,
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        let drain = async {
            let mut total = 0;
            while let Some(b) = provider.next_batch().await.unwrap() {
                total += b.num_rows();
            }
            total
        };

        // 5s timeout — old impl could hang forever; new impl
        // completes in microseconds.
        let total = tokio::time::timeout(std::time::Duration::from_secs(5), drain)
            .await
            .expect("must complete within 5s — lost-wake hang detected");
        assert_eq!(total, 9);
    }

    /// Backpressure regression guard (PR #71 round-2, B-1).
    ///
    /// Pulls one batch (draining the initial chunk's only batch),
    /// then sleeps long enough for the prefetch task to fill the
    /// channel and block on `tx.send().await`. Asserts that the
    /// RPC count plateaus at the expected bound *and stays there*
    /// during the pause — proving the bounded channel is gating
    /// the prefetch task, not letting it race ahead. Then resumes
    /// draining and verifies the remaining chunks arrive.
    ///
    /// Without backpressure, the prefetch task would speedrun the
    /// fixture during the sleep and `call_count` would be the
    /// total chunks, not the bound.
    #[tokio::test]
    async fn slow_consumer_observes_backpressure_plateau() {
        // Five-chunk fixture so there's room for multiple prefetched
        // chunks to fit in a small channel and several to be left
        // pending behind it.
        let initial_bytes = build_ipc_bytes(&[1]);
        let mut server_chunks: Vec<MockChunk> = Vec::new();
        for i in 1..5 {
            server_chunks.push(MockChunk {
                bytes: build_ipc_bytes(&[(i as i32) + 1]),
                chunk_index: i,
                row_offset: i,
                row_count: 1,
                next_chunk_index: if i < 4 { Some(i + 1) } else { None },
            });
        }
        let mock = MockSource::new(server_chunks);
        let source: Arc<dyn InlineChunkSource> = mock.clone();

        // max_chunks_in_memory = 2 → channel capacity = 1. So at
        // any moment the prefetch task has at most: 1 chunk in
        // the consumer's `current`, 1 chunk in the channel, 1
        // chunk being prepared (sent but the sender call is what
        // blocks). After the consumer drains the first chunk and
        // pauses, prefetch makes 2 RPCs and then blocks on
        // `tx.send().await` for the 3rd.
        let provider = InlineStreamingProvider::new(
            source,
            "stmt-1",
            initial_bytes,
            0, // initial_chunk_index
            0, // initial_row_offset
            1, // initial_row_count
            Some(1),
            CompressionCodec::None,
            /* max_chunks_in_memory = */ 2,
            None,
            tokio::runtime::Handle::current(),
        )
        .unwrap();

        // Drain the initial chunk (1 batch). After this, the
        // prefetch task can start filling the channel.
        let _b0 = provider.next_batch().await.unwrap().unwrap();

        // Sleep long enough for the prefetch task to fill the
        // channel and block on send. The channel capacity is 1, so
        // after this the task should have made exactly 1 RPC (chunk
        // 1, currently sitting in the channel) and be blocked
        // trying to send chunk 2.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let calls_at_plateau = mock.call_count();

        // Sleep again. If the channel were truly bounded, the RPC
        // count should NOT increase further during this pause.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let calls_after_more_sleep = mock.call_count();

        assert_eq!(
            calls_at_plateau, calls_after_more_sleep,
            "backpressure regression: RPC count climbed during \
             consumer pause ({} → {}) — channel is unbounded or \
             capacity exceeds the configured max_chunks_in_memory",
            calls_at_plateau, calls_after_more_sleep,
        );
        // And the count is bounded: max 2 RPCs queued (one in the
        // channel, one being prepared to send). Anything more
        // means we leaked the bound.
        assert!(
            calls_at_plateau <= 2,
            "prefetch task raced past the channel bound: \
             {} RPCs while consumer paused (expected ≤ 2)",
            calls_at_plateau,
        );

        // Resume draining; everything should still arrive
        // correctly. This proves backpressure didn't cause a
        // permanent stall.
        let mut total_batches = 1; // already drained chunk 0
        while let Some(_b) = provider.next_batch().await.unwrap() {
            total_batches += 1;
        }
        assert_eq!(total_batches, 5, "all 5 chunks must arrive after resume");
        assert_eq!(mock.call_count(), 4, "exactly 4 RPCs total (chunks 1-4)");
    }
}
