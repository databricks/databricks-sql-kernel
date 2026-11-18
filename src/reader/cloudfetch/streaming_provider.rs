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

//! StreamingCloudFetchProvider for orchestrating CloudFetch downloads.
//!
//! This is the main component that coordinates:
//! - On-demand link fetching from the link fetcher (which handles prefetching)
//! - Parallel chunk downloads from cloud storage
//! - Memory management via chunk limits
//! - Consumer API via RecordBatchReader trait

use crate::kernel_error::{Error, ErrorCode, Result};
use crate::klog::{debug, error, info, warn};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::reader::ResultReaderMetrics;
use crate::types::cloudfetch::{
    ChunkEntry, ChunkState, CloudFetchConfig, DEFAULT_CHUNK_READY_TIMEOUT_SECS,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use dashmap::{mapref::entry::Entry, DashMap};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, TryLockError};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Merge a list of record batches into larger batches up to `target_rows` each.
///
/// Accumulates consecutive batches until the target row count is reached, then
/// starts a new accumulator. Returns the merged batches in order.
///
/// - If input is empty, returns an empty vec.
/// - A single batch that already exceeds the target is passed through as-is.
/// - The last merged batch may be smaller than the target.
fn merge_batches_to_target(
    batches: Vec<RecordBatch>,
    target_rows: usize,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    if batches.len() == 1 {
        return Ok(batches);
    }

    let schema = batches[0].schema();
    let mut result: Vec<RecordBatch> = Vec::new();
    let mut pending: Vec<RecordBatch> = Vec::new();
    let mut accumulated_rows: usize = 0;

    for batch in batches {
        accumulated_rows += batch.num_rows();
        pending.push(batch);

        if accumulated_rows >= target_rows {
            let merged = flush_pending(&schema, &mut pending)?;
            result.push(merged);
            accumulated_rows = 0;
        }
    }

    // Flush any remaining batches
    if !pending.is_empty() {
        let merged = flush_pending(&schema, &mut pending)?;
        result.push(merged);
    }

    debug!(
        "Batch merge: produced {} batches (target {} rows/batch)",
        result.len(),
        target_rows
    );

    Ok(result)
}

/// Concatenate pending batches into a single batch and clear the pending vec.
fn flush_pending(schema: &SchemaRef, pending: &mut Vec<RecordBatch>) -> Result<RecordBatch> {
    if pending.len() == 1 {
        return Ok(pending.drain(..).next().unwrap());
    }
    let merged = concat_batches(schema, &*pending)
        .map_err(|e| Error::invalid_state().message(format!("Failed to merge batches: {}", e)))?;
    pending.clear();
    Ok(merged)
}

/// Orchestrates link fetching and chunk downloading for CloudFetch.
///
/// This provider manages:
/// - Background link prefetching to stay ahead of consumption
/// - Parallel downloads with memory-bounded concurrency
/// - Proper ordering of result batches
/// - Error propagation and cancellation
///
/// Python callers can hold the GIL during `try_next_batch`, so contention on
/// the batch buffer, chunk map or terminal error must yield `Pending` rather
/// than wait for a worker that may acquire the GIL through a log callback.
pub struct StreamingCloudFetchProvider {
    // Dependencies (injected)
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    chunk_downloader: Arc<ChunkDownloader>,
    config: CloudFetchConfig,

    // Schema - extracted from first Arrow batch
    schema: OnceLock<SchemaRef>,

    // Consumer state
    current_chunk_index: AtomicI64,
    current_batch_buffer: Mutex<VecDeque<RecordBatch>>,

    // Download scheduling state
    next_download_index: AtomicI64,
    end_of_stream: AtomicBool,

    // Storage - unified chunks map with state and data
    // Wrapped in Arc so spawned tasks share the same map
    chunks: Arc<DashMap<i64, ChunkEntry>>,

    // Memory control
    chunks_in_memory: AtomicUsize,
    max_chunks_in_memory: usize,

    // Coordination signals
    chunk_state_changed: Arc<Notify>,

    // First terminal provider failure, if any.
    //
    // Link-fetch failures (initial or incremental) surface here
    // once the transport's retry budget is exhausted. Unlike a
    // per-chunk download/processing failure — which lives on the
    // `ChunkEntry` a waiter can find in `chunks` — a link fetch
    // fails *before* any entry for the requested chunk exists, so
    // there is nothing in `chunks` for `wait_for_chunk` to key
    // off. Without a provider-level record, a woken waiter finds
    // neither data nor a terminal state (`end_of_stream` is also
    // still unset) and loops forever. Persisting the first such
    // error here gives every current and future waiter a
    // deterministic terminal result. First writer wins so the
    // original error context (SQLSTATE, source, etc.) is
    // preserved; later failures are dropped.
    terminal_error: Mutex<Option<Error>>,

    // Cancellation
    cancel_token: CancellationToken,

    // Tokio runtime handle for spawning tasks
    runtime_handle: tokio::runtime::Handle,

    // Statement identifier (when available from the link
    // fetcher) for log correlation. Operators filtering
    // production logs by SEA `statement_id` need this attached
    // to refresh-path log lines.
    statement_id: Option<Arc<str>>,

    // Coordinator for batched link-refresh.
    //
    // Several chunks expiring concurrently (the common shape: a
    // burst of presigned URLs all near the 1h boundary) would each
    // try to refresh the link cache. The coordinator ensures only
    // one refresh RPC is in flight at a time; other callers either
    // see the fresh link in cache after the RPC completes, or wait
    // on a `Notify` for the in-flight refresh to finish.
    //
    // **Critical**: we do NOT hold a lock across the RPC itself.
    // The SEA HTTP layer retries up to 900 seconds (see
    // `src/client/retry.rs`), and holding a lock for that duration
    // would block consumer drops, statement cancels, and any
    // task that hit an expired URL while the RPC was in flight.
    // Instead, the coordinator uses a CAS-style claim on an
    // `AtomicBool`: a single task wins the claim and issues the
    // RPC; concurrent callers see the claim is taken and wait on
    // the `Notify` for the RPC to complete (or the cancel-token
    // to fire), then re-check the cache.
    //
    // `Arc<...>` so spawned download tasks can hold their own clone
    // without keeping the whole provider alive past stream end.
    refresh_coordinator: Arc<RefreshCoordinator>,

    metrics: Arc<CloudFetchMetrics>,
}

/// Coordinates concurrent batched link-refresh attempts.
///
/// See the doc on `StreamingCloudFetchProvider::refresh_coordinator`
/// for the design rationale (specifically: we cannot hold a lock
/// across the SEA RPC because of the 900s retry budget).
#[derive(Debug, Default)]
struct RefreshCoordinator {
    /// Set to `true` while a refresh RPC is in flight. Concurrent
    /// callers see this and wait on `done` rather than racing into
    /// the same RPC.
    in_progress: AtomicBool,
    /// Notified each time a refresh completes (successfully or
    /// not). Used by waiters to re-check the cache.
    done: Notify,
}

/// All the wiring `download_chunk_with_retry` needs.
///
/// Bundles what was an 8-argument function signature into a
/// single struct so the spawn-site (and any future helper that
/// drives downloads) doesn't have to thread each Arc through by
/// hand. Cloned cheaply: every field is already an `Arc` /
/// `CancellationToken` / `Copy`.
#[derive(Clone)]
struct DownloadCtx {
    downloader: Arc<ChunkDownloader>,
    link_fetcher: Arc<dyn ChunkLinkFetcher>,
    chunks: Arc<DashMap<i64, ChunkEntry>>,
    refresh_coordinator: Arc<RefreshCoordinator>,
    cancel_token: CancellationToken,
    statement_id: Option<Arc<str>>,
    metrics: Arc<CloudFetchMetrics>,
    max_retries: u32,
    retry_delay: std::time::Duration,
    /// Absolute end-to-end wall-clock budget for downloading one chunk,
    /// spanning every retry, the backoff sleeps, and each HTTP attempt.
    /// See [`CloudFetchConfig::download_timeout`]. Enforced in
    /// [`StreamingCloudFetchProvider::download_chunk_with_retry`].
    download_timeout: std::time::Duration,
}

#[derive(Debug)]
struct CloudFetchMetrics {
    total_chunks_present: Option<i32>,
    total_chunks_iterated: AtomicI32,
    chunks_downloaded: AtomicI32,
    initial_chunk_latency_ms: AtomicI64,
    slowest_chunk_latency_ms: AtomicI64,
    sum_chunks_download_time_ms: AtomicI64,
}

impl CloudFetchMetrics {
    fn new(total_chunks_present: Option<i32>) -> Self {
        Self {
            total_chunks_present,
            total_chunks_iterated: AtomicI32::new(0),
            chunks_downloaded: AtomicI32::new(0),
            initial_chunk_latency_ms: AtomicI64::new(-1),
            slowest_chunk_latency_ms: AtomicI64::new(0),
            sum_chunks_download_time_ms: AtomicI64::new(0),
        }
    }

    fn record_chunk_download(&self, chunk_index: i64, elapsed: std::time::Duration) {
        let millis = elapsed.as_millis().try_into().unwrap_or(i64::MAX);
        // `initial_chunk_latency` is the time to download chunk 0
        // specifically, not the first download to *complete*. Under
        // concurrency the fastest/smallest chunk often finishes first,
        // so a first-to-complete gate would report a misleading value.
        // Gate strictly on `chunk_index == 0` to match the JDBC/C#
        // drivers' `initialChunkLatency` semantic.
        if chunk_index == 0 {
            self.initial_chunk_latency_ms
                .store(millis, Ordering::Release);
        }
        self.slowest_chunk_latency_ms
            .fetch_max(millis, Ordering::AcqRel);
        self.sum_chunks_download_time_ms
            .fetch_add(millis, Ordering::AcqRel);
        // Count completed downloads so `snapshot` can distinguish a
        // real ~0ms download from "no download ever completed". Both
        // `slowest`/`sum` atomics initialize to 0, so without this
        // counter a stream dropped/errored before any download finished
        // would report `Some(0)` — indistinguishable from a genuine
        // 0ms download and skewing server-side latency aggregates.
        self.chunks_downloaded.fetch_add(1, Ordering::AcqRel);
    }

    fn record_chunk_iterated(&self) {
        self.total_chunks_iterated.fetch_add(1, Ordering::AcqRel);
    }

    fn snapshot(&self) -> ResultReaderMetrics {
        let initial = self.initial_chunk_latency_ms.load(Ordering::Acquire);
        let iterated = self.total_chunks_iterated.load(Ordering::Acquire);
        // Only emit `slowest`/`sum` once at least one download has
        // completed. Both atomics initialize to 0, so gating on this
        // counter keeps a no-download snapshot reporting `None` rather
        // than a misleading `Some(0)` — mirroring the `-1` sentinel /
        // `None` treatment of `initial_chunk_latency` above.
        let downloaded = self.chunks_downloaded.load(Ordering::Acquire) > 0;
        ResultReaderMetrics {
            total_chunks_present: self.total_chunks_present,
            total_chunks_iterated: Some(iterated),
            initial_chunk_latency: (initial >= 0)
                .then(|| std::time::Duration::from_millis(initial as u64)),
            slowest_chunk_latency: downloaded.then(|| {
                std::time::Duration::from_millis(
                    self.slowest_chunk_latency_ms.load(Ordering::Acquire) as u64,
                )
            }),
            sum_chunks_download_time: downloaded.then(|| {
                std::time::Duration::from_millis(
                    self.sum_chunks_download_time_ms.load(Ordering::Acquire) as u64,
                )
            }),
        }
    }
}

impl std::fmt::Debug for StreamingCloudFetchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingCloudFetchProvider")
            .field("current_chunk_index", &self.current_chunk_index)
            .field("next_download_index", &self.next_download_index)
            .field("end_of_stream", &self.end_of_stream)
            .field("chunks_in_memory", &self.chunks_in_memory)
            .finish()
    }
}

/// Spawn a download future on `runtime_handle` and await its `JoinHandle`,
/// converting a task panic (`JoinError`) into an ordinary error.
///
/// tokio turns a panic inside a spawned task into a `JoinError`, but only if the
/// handle is awaited. A detached (fire-and-forget) download task that panics is
/// silently swallowed — its chunk then stays `Downloading` forever and the whole
/// stream wedges (PECOBLR-4026). Routing every chunk download through this helper
/// guarantees a panic surfaces as a failed chunk instead of an indefinite hang.
///
/// Note: this spawns a second (inner) task and awaits its handle, so each chunk
/// download uses two tasks — the outer scheduling task and this inner one. That
/// decouples the download from the outer task: if the outer task were aborted
/// mid-`await`, the inner download would keep running detached. This is not a leak
/// today because nothing `abort()`s these tasks — cancellation is cooperative via
/// `cancel_token`, checked inside `download_chunk_with_retry`. The extra task is
/// the price of capturing a `JoinError`; keep this invariant if adding `abort()`.
async fn join_download_task<F>(
    runtime_handle: &tokio::runtime::Handle,
    fut: F,
) -> Result<Vec<RecordBatch>>
where
    F: std::future::Future<Output = Result<Vec<RecordBatch>>> + Send + 'static,
{
    match runtime_handle.spawn(fut).await {
        Ok(result) => result,
        Err(join_err) => {
            error!("CloudFetch download task panicked: {}", join_err);
            Err(Error::io().message(format!("download task panicked: {}", join_err)))
        }
    }
}

impl StreamingCloudFetchProvider {
    /// Create a new provider.
    ///
    /// # Arguments
    /// * `config` - CloudFetch configuration
    /// * `link_fetcher` - Trait object for fetching chunk links (should be a SeaChunkLinkFetcherHandle
    ///   that already has initial links cached and handles prefetching)
    /// * `chunk_downloader` - For downloading chunks from cloud storage
    /// * `runtime_handle` - Tokio runtime handle for spawning background tasks
    pub(crate) fn new(
        config: CloudFetchConfig,
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        chunk_downloader: Arc<ChunkDownloader>,
        total_chunks_present: Option<i32>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let statement_id = link_fetcher.statement_id().map(Arc::from);
        let provider = Arc::new(Self {
            link_fetcher,
            chunk_downloader,
            config: config.clone(),
            schema: OnceLock::new(),
            current_chunk_index: AtomicI64::new(0),
            current_batch_buffer: Mutex::new(VecDeque::new()),
            next_download_index: AtomicI64::new(0),
            end_of_stream: AtomicBool::new(false),
            chunks: Arc::new(DashMap::new()),
            chunks_in_memory: AtomicUsize::new(0),
            max_chunks_in_memory: config.max_chunks_in_memory,
            chunk_state_changed: Arc::new(Notify::new()),
            terminal_error: Mutex::new(None),
            cancel_token: CancellationToken::new(),
            runtime_handle,
            statement_id,
            refresh_coordinator: Arc::new(RefreshCoordinator::default()),
            metrics: Arc::new(CloudFetchMetrics::new(total_chunks_present)),
        });

        // Start background initialization that fetches links and schedules downloads
        let provider_clone = Arc::clone(&provider);
        provider.runtime_handle.spawn(async move {
            provider_clone.initialize().await;
        });

        provider
    }

    /// Test-only constructor: build a provider around a pre-seeded
    /// `chunks` map WITHOUT spawning the background `initialize`
    /// task. Lets consumer-path tests (`wait_for_chunk`) observe the
    /// exact `ChunkState` they seed. The injected `link_fetcher` /
    /// `chunk_downloader` are never exercised by these tests.
    #[cfg(test)]
    fn for_test_with_chunks(
        link_fetcher: Arc<dyn ChunkLinkFetcher>,
        chunk_downloader: Arc<ChunkDownloader>,
        chunks: Arc<DashMap<i64, ChunkEntry>>,
        end_of_stream: bool,
    ) -> Arc<Self> {
        let config = CloudFetchConfig::default();
        Arc::new(Self {
            link_fetcher,
            chunk_downloader,
            max_chunks_in_memory: config.max_chunks_in_memory,
            config,
            schema: OnceLock::new(),
            current_chunk_index: AtomicI64::new(0),
            current_batch_buffer: Mutex::new(VecDeque::new()),
            next_download_index: AtomicI64::new(0),
            end_of_stream: AtomicBool::new(end_of_stream),
            chunks,
            chunks_in_memory: AtomicUsize::new(0),
            chunk_state_changed: Arc::new(Notify::new()),
            terminal_error: Mutex::new(None),
            cancel_token: CancellationToken::new(),
            runtime_handle: tokio::runtime::Handle::current(),
            statement_id: None,
            refresh_coordinator: Arc::new(RefreshCoordinator::default()),
            metrics: Arc::new(CloudFetchMetrics::new(None)),
        })
    }

    /// Initialize the provider by fetching initial links and starting downloads.
    ///
    /// This runs as a background task immediately after creation to:
    /// 1. Fetch initial links from the link fetcher (which has them cached)
    /// 2. Populate the chunks map
    /// 3. Schedule downloads to start immediately
    async fn initialize(self: &Arc<Self>) {
        // Fetch initial batch of links (should be instant if cached by link fetcher)
        debug!("Initializing provider: fetching initial links");

        let result = tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => return,
            result = self.link_fetcher.fetch_links(0, 0) => result,
        };
        match result {
            Ok(result) => {
                // Store fetched links
                for link in result.links {
                    let chunk_index = link.chunk_index;
                    self.chunks
                        .entry(chunk_index)
                        .or_insert_with(|| ChunkEntry::with_link(link));
                }

                if !result.has_more {
                    self.end_of_stream.store(true, Ordering::Release);
                }

                debug!(
                    "Provider initialized: {} links cached, end_of_stream={}",
                    self.chunks.len(),
                    !result.has_more
                );

                // Now schedule downloads - chunks map has links ready
                self.schedule_downloads().await;
            }
            Err(e) => {
                error!("Failed to fetch initial links: {}", e);
                // Persist as a terminal provider failure: no chunk
                // entry exists for the initial links, so a waiter has
                // nothing in `chunks` to observe. Without this it
                // would loop forever (see `terminal_error`).
                self.set_terminal_error(e);
            }
        }
    }

    /// Get next record batch. Main consumer interface.
    ///
    /// Blocks until the next batch is available or end of stream.
    /// First drains current_batch_buffer, then fetches next chunk.
    ///
    /// When `batch_merge_target_rows > 0`, batches within each chunk are
    /// pre-merged during download (in background tasks), so each chunk
    /// typically yields a single large batch.
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>> {
        // Check for cancellation
        if self.cancel_token.is_cancelled() {
            return Err(Error::invalid_state().message("Operation cancelled"));
        }

        // Drain batch buffer first
        if let Some(batch) = self.take_buffered_batch() {
            return Ok(Some(batch));
        }

        // Buffer empty - need next chunk
        let chunk_index = self.current_chunk_index.load(Ordering::Acquire);

        // Check if we've reached end of stream
        if self.is_at_end(chunk_index) {
            debug!("End of stream reached at chunk {}", chunk_index);
            return Ok(None);
        }

        // Wait for the chunk to be downloaded
        self.wait_for_chunk(chunk_index).await?;

        // Get the chunk and move batches to buffer
        if self.release_downloaded_chunk(chunk_index) {
            // Schedule more downloads
            self.schedule_downloads().await;
            if self.cancel_token.is_cancelled() {
                return Err(Error::invalid_state().message("Operation cancelled"));
            }

            // Return first batch from buffer
            if let Some(batch) = self.take_buffered_batch() {
                return Ok(Some(batch));
            }
        }

        // Check end of stream again
        if self.end_of_stream.load(Ordering::Acquire) {
            return Ok(None);
        }

        // A terminal link-fetch failure that landed after the wait
        // returned takes precedence over the generic "not found"
        // message, so callers see the real cause deterministically.
        if let Some(err) = self.terminal_error() {
            return Err(err);
        }

        Err(Error::invalid_state().message(format!("Chunk {} not found after wait", chunk_index)))
    }

    /// Return the next batch if it is already downloaded, without waiting for
    /// chunk state to change or for a contended provider-state lock. Download
    /// and link-fetch tasks continue while this returns `Pending` to the caller.
    pub fn try_next_batch(self: &Arc<Self>) -> Result<crate::reader::TryNextBatch> {
        use crate::reader::TryNextBatch;

        let mut released = false;
        let outcome = loop {
            if self.cancel_token.is_cancelled() {
                break Err(Error::invalid_state().message("Operation cancelled"));
            }

            let mut buffer = match self.current_batch_buffer.try_lock() {
                Err(TryLockError::WouldBlock) => break Ok(TryNextBatch::Pending),
                result => result.unwrap(),
            };
            if let Some(batch) = buffer.pop_front() {
                drop(buffer);
                let _ = self.schema.get_or_init(|| batch.schema());
                break Ok(TryNextBatch::Ready(batch));
            }

            let chunk_index = self.current_chunk_index.load(Ordering::Acquire);
            match self.chunks.try_entry(chunk_index) {
                None => break Ok(TryNextBatch::Pending),
                Some(Entry::Vacant(_)) if self.end_of_stream.load(Ordering::Acquire) => {
                    break Ok(TryNextBatch::End);
                }
                Some(Entry::Occupied(entry)) => match &entry.get().state {
                    ChunkState::Downloaded => {
                        // Both guards are acquired before removing data so a
                        // contended probe leaves every batch available to retry.
                        if let Some(batches) = entry.remove().batches {
                            buffer.extend(batches);
                        }
                        self.advance_chunk();
                        released = true;
                        continue;
                    }
                    ChunkState::DownloadFailed(error) => break Err(error.clone()),
                    ChunkState::ProcessingFailed(error) => {
                        break Err(Error::io().message(error.clone()))
                    }
                    ChunkState::Cancelled => {
                        break Err(Error::invalid_state().message("Operation cancelled"))
                    }
                    _ => {}
                },
                Some(Entry::Vacant(_)) => {}
            }
            drop(buffer);
            let terminal_error = match self.terminal_error.try_lock() {
                Err(TryLockError::WouldBlock) => break Ok(TryNextBatch::Pending),
                result => result.unwrap(),
            };
            if let Some(error) = terminal_error.as_ref() {
                break Err(error.clone());
            }
            break Ok(TryNextBatch::Pending);
        };

        // Refill the rolling window once, after the loop settles, rather than
        // once per skipped empty chunk. Any released chunk freed a window slot;
        // schedule_downloads early-exits when the window is full and the current
        // batch (if any) is already buffered, so this must not block.
        if released {
            debug!(
                "Buffered downloaded chunks: chunks_in_memory={}/{}, consumer_at={}",
                self.chunks_in_memory.load(Ordering::Acquire),
                self.max_chunks_in_memory,
                self.current_chunk_index.load(Ordering::Acquire),
            );
            // Detached spawn (vs. the blocking path's inline await) means two
            // rapid try_next_batch calls can overlap this refill, and a mixed
            // try_next_batch + next_batch caller can run it alongside next_batch's
            // inline refill. That is safe because schedule_downloads is
            // reentrant against itself: the load of next_download_index is only
            // a hint. The actual claim is the UrlFetched -> Downloading
            // transition performed under the DashMap get_mut write guard, so at
            // most one runner ever claims a given chunk. A runner that loses the
            // race sees a non-UrlFetched state, fails the claim, and breaks
            // without advancing next_download_index (only the winner fetch_adds
            // it), so no index is double-scheduled or skipped.
            let provider = Arc::clone(self);
            self.runtime_handle.spawn(async move {
                provider.schedule_downloads().await;
            });
        }

        outcome
    }

    /// Cancel all pending operations.
    pub fn cancel(&self) {
        debug!("Cancelling CloudFetch provider");
        self.cancel_token.cancel();
        self.chunk_state_changed.notify_one();
    }

    // --- Internal methods ---

    fn take_buffered_batch(&self) -> Option<RecordBatch> {
        let batch = self.current_batch_buffer.lock().unwrap().pop_front()?;
        let _ = self.schema.get_or_init(|| batch.schema());
        Some(batch)
    }

    fn is_at_end(&self, chunk_index: i64) -> bool {
        self.end_of_stream.load(Ordering::Acquire) && !self.chunks.contains_key(&chunk_index)
    }

    /// Move one downloaded chunk into the consumer buffer and advance the
    /// shared cursor. Scheduling the replacement download remains caller-owned
    /// because the blocking path awaits it while the non-blocking path spawns it.
    fn release_downloaded_chunk(&self, chunk_index: i64) -> bool {
        let Some((_, mut entry)) = self.chunks.remove(&chunk_index) else {
            return false;
        };
        if let Some(batches) = entry.batches.take() {
            self.current_batch_buffer.lock().unwrap().extend(batches);
        }
        let (new_count, next_chunk) = self.advance_chunk();
        debug!(
            "Released chunk {}: chunks_in_memory={}/{}, consumer advancing to {}",
            chunk_index, new_count, self.max_chunks_in_memory, next_chunk
        );
        true
    }

    fn advance_chunk(&self) -> (usize, i64) {
        let new_count = self.chunks_in_memory.fetch_sub(1, Ordering::Release) - 1;
        let next_chunk = self.current_chunk_index.fetch_add(1, Ordering::Release) + 1;
        self.metrics.record_chunk_iterated();
        (new_count, next_chunk)
    }

    /// Record the first terminal link-fetch failure and wake every waiter.
    ///
    /// Called from the link-fetch error paths once the transport's
    /// retry budget is exhausted. First writer wins, so the original
    /// error (with its SQLSTATE / source chain intact) is preserved
    /// and any later failure is dropped. Uses `notify_waiters()`
    /// rather than `notify_one()` so *every* consumer currently
    /// blocked in `wait_for_chunk` is released, not just one — a
    /// terminal failure must not leave sibling waiters stuck.
    fn set_terminal_error(&self, error: Error) {
        let first = {
            let mut slot = self.terminal_error.lock().unwrap();
            if slot.is_none() {
                *slot = Some(error.clone());
                true
            } else {
                false
            }
        };
        // Wake all current waiters so they observe the terminal state.
        // A waiter that registers *after* this checks `terminal_error`
        // on entry, so it never misses the signal.
        self.chunk_state_changed.notify_waiters();
        // Host log callbacks can acquire locks held by result consumers.
        if first {
            error!("CloudFetch provider entering terminal error state: {error}");
        }
    }

    /// Return a clone of the terminal link-fetch error, if one is set.
    fn terminal_error(&self) -> Option<Error> {
        self.terminal_error.lock().unwrap().clone()
    }

    /// Fetch links from the link fetcher and store them in the chunks map.
    ///
    /// Returns whether the requested `start_index` is now available in the map.
    async fn fetch_and_store_links(&self, start_index: i64) -> bool {
        let result = tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => return false,
            result = self.link_fetcher.fetch_links(start_index, 0) => result,
        };
        match result {
            Ok(result) => {
                for link in result.links {
                    let chunk_index = link.chunk_index;
                    self.chunks
                        .entry(chunk_index)
                        .or_insert_with(|| ChunkEntry::with_link(link));
                }

                if !result.has_more {
                    self.end_of_stream.store(true, Ordering::Release);
                }

                self.chunks.contains_key(&start_index)
            }
            Err(e) => {
                error!("Failed to fetch links for chunk {}: {}", start_index, e);
                // Persist as a terminal provider failure. The chunk
                // whose link we failed to fetch has no entry in
                // `chunks`, so a consumer blocked in `wait_for_chunk`
                // on `start_index` would otherwise loop forever
                // (see `terminal_error`).
                self.set_terminal_error(e);
                false
            }
        }
    }

    /// Spawn download tasks for available links up to concurrency limit.
    async fn schedule_downloads(&self) {
        loop {
            // Check if we have room for more downloads
            let current_in_memory = self.chunks_in_memory.load(Ordering::Acquire);
            if current_in_memory >= self.max_chunks_in_memory {
                debug!(
                    "Memory limit reached: {}/{} chunks in memory, pausing downloads",
                    current_in_memory, self.max_chunks_in_memory
                );
                break;
            }

            // Get next chunk index to download
            let next_index = self.next_download_index.load(Ordering::Acquire);

            // Check if we have a link for this chunk
            let should_download = {
                if let Some(entry) = self.chunks.get(&next_index) {
                    matches!(entry.state, ChunkState::UrlFetched)
                } else {
                    false
                }
            };

            if !should_download {
                // Link not in map yet — try to pull from the fetcher (cache or server)
                if self.end_of_stream.load(Ordering::Acquire) {
                    break;
                }
                if !self.fetch_and_store_links(next_index).await {
                    break;
                }
                // Re-check after fetching
                let ready = self
                    .chunks
                    .get(&next_index)
                    .is_some_and(|e| matches!(e.state, ChunkState::UrlFetched));
                if !ready {
                    break;
                }
            }

            // Proactive expiry check: if the cached link for this
            // chunk is already expired (e.g. it sat in the queue
            // longer than the presigned URL's TTL), refresh the
            // whole forward batch before kicking off the download.
            // Avoids the wasted-RTT failure-then-refresh path inside
            // the download task.
            let expired = self
                .chunks
                .get(&next_index)
                .and_then(|e| e.link.as_ref().map(|l| l.is_expired()))
                .unwrap_or(false);
            if expired {
                let sid = self.statement_id.as_deref().unwrap_or("<unknown>");
                debug!(
                    statement_id = %sid,
                    "Proactive refresh: cached link for chunk {} already expired",
                    next_index,
                );
                if let Err(e) = Self::refresh_links_batch(
                    next_index,
                    &self.link_fetcher,
                    &self.chunks,
                    &self.refresh_coordinator,
                    &self.cancel_token,
                    self.statement_id.as_deref(),
                )
                .await
                {
                    warn!(
                        statement_id = %sid,
                        "Proactive batched refresh failed for chunk {}: {}; \
                         will fall through to per-download retry path",
                        next_index, e,
                    );
                    // Don't abort scheduling — the download task's
                    // own refresh attempt will surface the error
                    // (and retry) when it actually tries to fetch.
                }
            }

            // Try to claim this chunk for download
            let claimed = {
                if let Some(mut entry) = self.chunks.get_mut(&next_index) {
                    if matches!(entry.state, ChunkState::UrlFetched) {
                        entry.state = ChunkState::Downloading;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };

            if !claimed {
                break;
            }

            // Increment counters
            let new_count = self.chunks_in_memory.fetch_add(1, Ordering::Release) + 1;
            self.next_download_index.fetch_add(1, Ordering::Release);
            self.link_fetcher.note_scheduled(next_index);

            debug!(
                "Scheduling download for chunk {}: chunks_in_memory={}/{}, consumer_at={}",
                next_index,
                new_count,
                self.max_chunks_in_memory,
                self.current_chunk_index.load(Ordering::Acquire)
            );

            // Spawn download task. All the per-download wiring
            // lives in `DownloadCtx` so the spawn-site doesn't have
            // to re-thread 8 parameters and the function signature
            // stays clean.
            let chunk_index = next_index;
            let ctx = DownloadCtx {
                downloader: Arc::clone(&self.chunk_downloader),
                link_fetcher: Arc::clone(&self.link_fetcher),
                chunks: self.chunks.clone(),
                refresh_coordinator: Arc::clone(&self.refresh_coordinator),
                cancel_token: self.cancel_token.clone(),
                statement_id: self.statement_id.clone(),
                metrics: Arc::clone(&self.metrics),
                max_retries: self.config.max_retries,
                retry_delay: self.config.retry_delay,
                download_timeout: self.config.download_timeout,
            };
            let chunk_state_changed = Arc::clone(&self.chunk_state_changed);
            let chunks = self.chunks.clone();
            let batch_merge_target_rows = self.config.batch_merge_target_rows;

            // Run the download on its own task and AWAIT its JoinHandle here.
            // tokio turns a panic inside a spawned task into a `JoinError` — but
            // only if the handle is awaited. The prior fire-and-forget spawn dropped
            // the handle, so a panic in the download/decompress path (e.g. the
            // twox-hash overflow in LZ4 block-checksum verification — PECOBLR-4026)
            // was silently swallowed, leaving the chunk stuck in `Downloading`
            // forever and wedging the whole stream. Surfacing the panic as a
            // download error lets the reader fail cleanly instead of hanging.
            let rt = self.runtime_handle.clone();
            self.runtime_handle.spawn(async move {
                let result = join_download_task(&rt, async move {
                    Self::download_chunk_with_retry(chunk_index, &ctx).await
                })
                .await;

                match result {
                    Ok(batches) => {
                        // When batch merging is enabled, merge small batches within
                        // this chunk into larger ones up to the target row count.
                        // This runs on the download task so the merge cost is
                        // hidden behind download latency.
                        let batches = if batch_merge_target_rows > 0 {
                            match merge_batches_to_target(batches, batch_merge_target_rows) {
                                Ok(merged) => merged,
                                Err(e) => {
                                    error!(
                                        "Failed to merge batches for chunk {}: {}",
                                        chunk_index, e
                                    );
                                    if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                                        entry.state = ChunkState::ProcessingFailed(e.to_string());
                                    }
                                    chunk_state_changed.notify_one();
                                    return;
                                }
                            }
                        } else {
                            batches
                        };

                        if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                            entry.batches = Some(batches);
                            entry.state = ChunkState::Downloaded;
                        }
                    }
                    Err(e) => {
                        error!("Failed to download chunk {}: {}", chunk_index, e);
                        if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                            // Preserve the full typed error in the state so
                            // its `ErrorCode` (e.g. `Timeout` for a deadline)
                            // survives to the consumer path — see H05. Keep a
                            // stringified copy in `last_failure_reason` for the
                            // operator breadcrumb that outlives the state.
                            entry.last_failure_reason = Some(e.to_string());
                            entry.state = ChunkState::DownloadFailed(e);
                        }
                    }
                }

                chunk_state_changed.notify_one();
            });
        }
    }

    /// Stable terminal error for a chunk whose end-to-end download budget
    /// ([`CloudFetchConfig::download_timeout`]) was exhausted. Classified as
    /// [`Error::timeout`] so consumers (and the C ABI / ODBC bridge) get a
    /// consistent, retryable timeout classification rather than an opaque
    /// I/O error (audit H05).
    fn download_deadline_error(chunk_index: i64, budget: std::time::Duration) -> Error {
        Error::timeout().message(format!(
            "CloudFetch chunk {} download exceeded the {:.1}s end-to-end deadline",
            chunk_index,
            budget.as_secs_f64()
        ))
    }

    /// Download a single chunk with retry and link refresh on expiry,
    /// bounded end-to-end by [`CloudFetchConfig::download_timeout`].
    async fn download_chunk_with_retry(
        chunk_index: i64,
        ctx: &DownloadCtx,
    ) -> Result<Vec<RecordBatch>> {
        let mut attempts = 0;

        // Absolute end-to-end deadline for this chunk. Bounds the whole
        // retry loop — every HTTP attempt, link refresh, and backoff sleep
        // — so a permanently-stalled chunk surfaces a terminal timeout in
        // bounded time instead of stacking `max_retries × request_timeout`
        // into minutes (audit H05). A zero budget disables the deadline
        // (falls back to attempt-count bounding only).
        let deadline = (!ctx.download_timeout.is_zero())
            .then(|| std::time::Instant::now() + ctx.download_timeout);

        loop {
            if ctx.cancel_token.is_cancelled() {
                return Err(Error::invalid_state().message("Download cancelled"));
            }

            // Hard deadline check at the top of every attempt: if the
            // budget is spent, stop promptly with a stable timeout error
            // rather than starting another attempt or refresh.
            if let Some(deadline) = deadline {
                if std::time::Instant::now() >= deadline {
                    return Err(Self::download_deadline_error(
                        chunk_index,
                        ctx.download_timeout,
                    ));
                }
            }

            // Get link (may need to refresh the whole forward batch
            // if expired). We always go via the batched refresh path:
            // refreshing all in-flight chunks in one RPC matches the
            // shape of how SEA expires links (the whole presigned-URL
            // batch tends to share an expiry window), and avoids N
            // separate `refetch_link` calls when a burst of chunks
            // expires concurrently.
            let link = {
                // Clone the link and release the DashMap guard before any
                // refresh awaits; refresh_links_batch accesses the same map.
                let stored_link = ctx.chunks.get(&chunk_index).and_then(|e| e.link.clone());

                match stored_link {
                    Some(link) if !link.is_expired() => link,
                    _ => {
                        debug!(
                            "Link for chunk {} missing or expired; \
                             triggering batched refresh",
                            chunk_index
                        );
                        // Bound the refresh RPC by the remaining chunk
                        // budget so a stalled refresh can't overshoot the
                        // end-to-end deadline by its own (much larger)
                        // per-request `request_timeout` (audit H05). This
                        // keeps the deadline genuinely end-to-end across
                        // the link-refresh step, matching the loop-level
                        // comment above.
                        let refresh = Self::refresh_links_batch(
                            chunk_index,
                            &ctx.link_fetcher,
                            &ctx.chunks,
                            &ctx.refresh_coordinator,
                            &ctx.cancel_token,
                            ctx.statement_id.as_deref(),
                        );
                        match deadline {
                            Some(d) => tokio::time::timeout(
                                d.saturating_duration_since(std::time::Instant::now()),
                                refresh,
                            )
                            .await
                            .map_err(|_| {
                                Self::download_deadline_error(chunk_index, ctx.download_timeout)
                            })?,
                            None => refresh.await,
                        }
                        .map_err(|e| {
                            // Wrap with chunk_index breadcrumb so the
                            // downstream `error!("Failed to download
                            // chunk N: …")` log clearly distinguishes
                            // refresh-RPC failures from
                            // download-RPC failures.
                            Error::io()
                                .message(format!(
                                    "Link refresh RPC failed for chunk {}",
                                    chunk_index
                                ))
                                .with_cause(e)
                        })?;
                        // After a successful refresh, the link
                        // MUST exist and be fresh — `refresh_links_batch`
                        // returns Err otherwise (missing chunk in
                        // response, or stale-after-refresh =
                        // clock-skew). If we somehow get here with
                        // no link cached, treat it as an invariant
                        // breach.
                        let cached = ctx
                            .chunks
                            .get(&chunk_index)
                            .and_then(|e| e.link.clone())
                            .ok_or_else(|| {
                                Error::invalid_state().message(format!(
                                    "Invariant: link for chunk {} absent after successful \
                                     batched refresh — refresh_links_batch should have \
                                     errored before reaching here",
                                    chunk_index
                                ))
                            })?;
                        if cached.is_expired() {
                            // The link is still expired after the
                            // refresh call returned `Ok(())`. Two
                            // distinct root causes, surfaced with
                            // distinct errors so operators don't
                            // get a misleading clock-skew message
                            // for the benign concurrent-disjoint
                            // case.
                            //
                            // (a) **Clock skew / server bug**: a
                            //     refresh ran *for our chunk* but
                            //     the link is still flagged
                            //     expired. The URL is the new one
                            //     from the server. The local-vs-
                            //     server clock or `LINK_EXPIRY_BUFFER_SECS`
                            //     is off, or the server emitted a
                            //     stale link.
                            // (b) **Concurrent disjoint window**:
                            //     a refresh ran but didn't cover
                            //     our chunk (loser-of-claim path
                            //     returned Ok). The URL is
                            //     unchanged — same one we started
                            //     with. Surfaces as `Unavailable`
                            //     so the caller can retry without
                            //     conflating with the clock-skew
                            //     diagnosis.
                            //
                            // `refresh_links_batch` itself now
                            // returns Err for the
                            // MAX_CLAIM_RETRIES exhaustion case,
                            // so reaching this branch with an
                            // unchanged URL means we were in a
                            // disjoint-window race that resolved
                            // within the retry budget but still
                            // didn't refresh our chunk (e.g. the
                            // winner's window was strictly before
                            // our chunk_index).
                            let url_unchanged = stored_link
                                .as_ref()
                                .is_some_and(|prev| prev.url == cached.url);
                            return Err(if url_unchanged {
                                Error::unavailable().message(format!(
                                    "Link for chunk {} not refreshed by the concurrent \
                                     refresh window (URL unchanged); retry would help",
                                    chunk_index,
                                ))
                            } else {
                                Error::invalid_state().message(format!(
                                    "Link for chunk {} is still expired after refresh \
                                     (URL changed but `is_expired()` still true — \
                                     likely client/server clock skew or a server bug)",
                                    chunk_index,
                                ))
                            });
                        }
                        cached
                    }
                }
            };

            // Attempt download. Cap this attempt at the remaining budget so
            // a stalled connect / headers / body can't outrun the deadline
            // (reqwest's per-request timeout spans all three). The downloader
            // applies `min(request_timeout, remaining)` so the static
            // per-request safety net is never loosened. `None` when no
            // deadline is configured.
            let download_started = std::time::Instant::now();
            let attempt_timeout = deadline.map(|d| d.saturating_duration_since(download_started));
            match ctx.downloader.download(&link, attempt_timeout).await {
                Ok(batches) => {
                    ctx.metrics
                        .record_chunk_download(chunk_index, download_started.elapsed());
                    return Ok(batches);
                }
                Err(e) => {
                    // Refreshing a signed URL cannot repair certificate verification failures.
                    if e.code == ErrorCode::NetworkError && !e.retryable {
                        return Err(e);
                    }
                    // Every completed download failure consumes the hard attempt
                    // budget, even when its link became stale while in flight.
                    attempts += 1;
                    if attempts >= ctx.max_retries {
                        return Err(e);
                    }
                    // Stale-snapshot detection. Two ways a download
                    // can fail against a URL that became stale while
                    // the request was in flight:
                    //
                    // 1. **Concurrent refresh** updated the cached
                    //    link while we were `.await`-ing on
                    //    `download()`. We compare on URL: a fresh
                    //    batch from the server emits new presigned
                    //    URLs.
                    // 2. **Passive expiry** — the URL we tried (and
                    //    every consumer of the cache shared) just
                    //    crossed its TTL while in flight. S3 may
                    //    return 403 a hair earlier than
                    //    `LINK_EXPIRY_BUFFER_SECS` predicts; clock
                    //    skew can also push us past expiry
                    //    silently. In either case, the link we
                    //    *tried* is now expired per the local
                    //    clock; the next loop iteration will
                    //    refresh and pick up a fresh URL.
                    //
                    // (Edge case for case 1: if the server happened
                    // to emit the same URL on refresh — extremely
                    // unlikely for presigned URLs but possible in
                    // theory — we'd count the attempt under
                    // case-1 detection. The retry budget still
                    // bounds total work.)
                    let cached_url = ctx
                        .chunks
                        .get(&chunk_index)
                        .and_then(|e| e.link.as_ref().map(|l| l.url.clone()));
                    let was_refreshed_concurrently = cached_url
                        .as_ref()
                        .is_some_and(|cached| *cached != link.url);
                    let attempted_link_now_expired = link.is_expired();
                    if was_refreshed_concurrently || attempted_link_now_expired {
                        debug!(
                            "Chunk {} download failed against a link the cache now \
                             considers stale ({}); refreshing before retry: {}",
                            chunk_index,
                            if was_refreshed_concurrently {
                                "refreshed concurrently"
                            } else {
                                "passively expired in-flight"
                            },
                            e,
                        );
                        // Don't sleep. The next iteration refreshes or reads the
                        // latest URL; the failure was charged above.
                        continue;
                    }

                    warn!(
                        "Chunk {} download failed (attempt {}/{}): {}, retrying...",
                        chunk_index, attempts, ctx.max_retries, e
                    );

                    // The `DownloadRetry` state itself carries no
                    // failure reason; the reason lives in
                    // `ChunkEntry::last_failure_reason` so an
                    // operator inspecting the chunks map during
                    // the retry-sleep window still has the
                    // correlation breadcrumb. Keeping
                    // `DownloadRetry` distinct from
                    // `DownloadFailed` is intentional — it
                    // signals "task is alive and sleeping" to
                    // other code paths (notably
                    // `refresh_links_batch`'s "don't double-spawn"
                    // guard).
                    if let Some(mut entry) = ctx.chunks.get_mut(&chunk_index) {
                        entry.state = ChunkState::DownloadRetry;
                        entry.last_failure_reason = Some(e.to_string());
                    }

                    // Never sleep past the deadline: cap the backoff at the
                    // remaining budget so the next iteration's deadline check
                    // fires promptly instead of after a full `retry_delay`
                    // overshoot.
                    let sleep_for = match deadline {
                        Some(d) => ctx
                            .retry_delay
                            .min(d.saturating_duration_since(std::time::Instant::now())),
                        None => ctx.retry_delay,
                    };
                    tokio::time::sleep(sleep_for).await;
                }
            }
        }
    }

    pub(crate) fn metrics(&self) -> Option<ResultReaderMetrics> {
        let snapshot = self.metrics.snapshot();
        // Surface metrics whenever *any* datum was captured, not just
        // once a chunk has been iterated (released from the buffer).
        // `total_chunks_present` is known from the manifest at
        // construction and `initial_chunk_latency` is stamped at
        // chunk-0 download completion — both fire before the first
        // `record_chunk_iterated`. Gating strictly on `iterated > 0`
        // would drop this data for a stream that downloads but is
        // dropped/errors before releasing any chunk, losing useful
        // telemetry for partial/failed consumption.
        let has_data = snapshot.total_chunks_present.is_some()
            || snapshot
                .total_chunks_iterated
                .is_some_and(|count| count > 0)
            || snapshot.initial_chunk_latency.is_some()
            || snapshot.slowest_chunk_latency.is_some();
        has_data.then_some(snapshot)
    }

    /// Refresh presigned-URL links for `chunk_index` and every
    /// subsequent chunk whose download is still pending or in flight.
    ///
    /// ## Coordination
    ///
    /// Uses [`RefreshCoordinator`] — an `AtomicBool` + `Notify`
    /// instead of a `TokioMutex`. The mutex was held across the
    /// SEA RPC, which can retry for up to 900 s (see
    /// `src/client/retry.rs`); a stuck retry would block consumer
    /// drops and any other download task that hit an expired URL.
    ///
    /// New flow:
    /// 1. CAS-claim `in_progress: false → true`. If another task
    ///    already holds the claim, wait on `done.notified()`
    ///    (cooperatively, in parallel with the `cancel_token`)
    ///    and then re-check the cache without ourselves issuing
    ///    an RPC.
    /// 2. If we won the claim, issue the RPC with `select!`
    ///    against `cancel_token.cancelled()` so a provider drop
    ///    surfaces immediately.
    /// 3. Apply the refresh batch to the chunks map, then
    ///    `notify_waiters()` so others can re-check.
    /// 4. Always release the claim (even on error).
    ///
    /// ## Per-chunk state reconciliation
    ///
    /// - `UrlFetched`: link replaced; state stays `UrlFetched`
    ///   so `schedule_downloads` picks the chunk up with the
    ///   fresh URL.
    /// - `Pending`: forward-compatibility arm. No `Pending` entry
    ///   exists in the chunks map today after initialization
    ///   (every insertion uses `ChunkEntry::with_link`), but if
    ///   a future change introduces a link-less `Pending` within
    ///   the refresh window, this arm promotes it to
    ///   `UrlFetched` once a fresh link arrives.
    /// - `Downloading` / `DownloadFailed` / `DownloadRetry`:
    ///   **link replaced only**. A task owns the state machine
    ///   for the chunk; resetting state would let
    ///   `schedule_downloads` spawn a *second* download task while
    ///   the first one is still working, double-counting
    ///   `chunks_in_memory` and racing both tasks to write into
    ///   `entry.batches`. The running task re-reads `entry.link`
    ///   on its next retry iteration and picks up the fresh URL
    ///   transparently.
    /// - `Downloaded` / `ProcessingFailed` / `Cancelled` /
    ///   `Released`: link replaced for bookkeeping; no other
    ///   change. Bytes / terminal state are already final.
    async fn refresh_links_batch(
        start_chunk_index: i64,
        link_fetcher: &Arc<dyn ChunkLinkFetcher>,
        chunks: &Arc<DashMap<i64, ChunkEntry>>,
        coordinator: &Arc<RefreshCoordinator>,
        cancel_token: &CancellationToken,
        statement_id: Option<&str>,
    ) -> Result<()> {
        // Display helper: `"<unknown>"` is loud enough for ops
        // to notice missing statement_id in log filters.
        let sid = statement_id.unwrap_or("<unknown>");
        // Snapshot the URL the caller saw expired *before* we
        // attempt anything. The skip-after-refresh checks below
        // compare against this snapshot: any URL change proves a
        // refresh landed, regardless of what `is_expired()` says
        // about the new URL (so the dedupe contract holds even if
        // a future server emits short-TTL URLs ≤ LINK_EXPIRY_BUFFER_SECS).
        let pre_attempt_url = chunks
            .get(&start_chunk_index)
            .and_then(|e| e.link.as_ref().map(|l| l.url.clone()));

        // Disjoint-window retry budget. The loser-of-claim path
        // (below) waits on `done` for whoever owns the claim. If
        // the winner's refresh window didn't cover *our* chunk
        // (e.g. two download tasks for disjoint chunk ranges hit
        // expiry within the same lock window), the wait resolves
        // with our chunk still stale. Without this loop we'd
        // return `Ok(())` and the caller's post-refresh
        // `is_expired()` check would surface a misleading
        // "still expired — clock skew" error for what is
        // actually a benign disjoint-window race.
        //
        // Three iterations covers the realistic worst case (two
        // concurrent disjoint-window refreshers + ours); a hard
        // bound avoids pathological churn if the server keeps
        // emitting non-overlapping refresh batches.
        const MAX_CLAIM_RETRIES: u32 = 3;

        for claim_attempt in 0..MAX_CLAIM_RETRIES {
            // Acquire-style CAS to claim "I'm the one refreshing".
            // `compare_exchange` returns `Ok(prev)` only when prev was
            // exactly `false` — i.e. nobody else was refreshing.
            let won_claim = coordinator
                .in_progress
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();

            if won_claim {
                break;
            }

            // Another task is mid-refresh. Wait for its `done`
            // signal — cancel-aware, so a provider drop unblocks
            // us. Then re-check the cache; the other refresh may
            // have already refreshed our chunk.
            debug!(
                statement_id = %sid,
                claim_attempt,
                "Batched refresh for chunk {} deferred — another task is refreshing; \
                 waiting on done signal",
                start_chunk_index,
            );
            let notified = coordinator.done.notified();
            tokio::select! {
                _ = notified => {}
                _ = cancel_token.cancelled() => {
                    return Err(Error::invalid_state()
                        .message("Refresh wait cancelled"));
                }
            }
            // Re-check: did the other refresh land a fresh link
            // for *our* chunk? "Fresh" means either (a) the URL
            // changed from what the caller saw — proves a refresh
            // landed even if absolute TTL would still flag it, or
            // (b) `is_expired()` reports false. Either signal is
            // enough to skip our own RPC.
            if let Some(entry) = chunks.get(&start_chunk_index) {
                if let Some(ref link) = entry.link {
                    let url_changed = pre_attempt_url
                        .as_ref()
                        .is_some_and(|prev| *prev != link.url);
                    if url_changed || !link.is_expired() {
                        drop(entry);
                        debug!(
                            statement_id = %sid,
                            "Concurrent refresh delivered a {} link for chunk {}; \
                             skipping our own RPC",
                            if url_changed { "new" } else { "fresh" },
                            start_chunk_index,
                        );
                        return Ok(());
                    }
                }
            }
            // Disjoint-window case: the winning refresh ran but
            // didn't cover our chunk. Loop back to retry the CAS
            // claim ourselves. If we never win after
            // MAX_CLAIM_RETRIES, surface a typed error so the
            // caller doesn't conflate this with the clock-skew
            // path.
            if claim_attempt + 1 == MAX_CLAIM_RETRIES {
                return Err(Error::unavailable().message(format!(
                    "Concurrent refresh contention: claim CAS lost {} times for chunk {} \
                     and each winning refresh covered a disjoint window. Likely \
                     pathological concurrent expiry across very different chunk ranges; \
                     consider lowering the per-task fan-out or filing a bug.",
                    MAX_CLAIM_RETRIES, start_chunk_index,
                )));
            }
        }

        // We hold the claim. Make sure we release it on any exit
        // path (success, error, RPC cancel). A small RAII guard
        // is the cleanest way to do that.
        struct ReleaseOnDrop<'a>(&'a RefreshCoordinator);
        impl Drop for ReleaseOnDrop<'_> {
            fn drop(&mut self) {
                self.0.in_progress.store(false, Ordering::Release);
                self.0.done.notify_waiters();
            }
        }
        let _release = ReleaseOnDrop(coordinator.as_ref());

        // Re-check after winning the claim: between the
        // expired-link check at the caller and our CAS, some
        // other path may have already refreshed our chunk. Same
        // dual signal as above: URL change OR `is_expired()=false`.
        if let Some(entry) = chunks.get(&start_chunk_index) {
            if let Some(ref link) = entry.link {
                let url_changed = pre_attempt_url
                    .as_ref()
                    .is_some_and(|prev| *prev != link.url);
                if url_changed || !link.is_expired() {
                    drop(entry);
                    debug!(
                        statement_id = %sid,
                        "Batched refresh for chunk {} skipped after claim — \
                         link became {} between expired-check and claim",
                        start_chunk_index,
                        if url_changed { "new" } else { "fresh" },
                    );
                    return Ok(());
                }
            }
        }

        // Derive `row_offset` from the existing cached link if any.
        // The bounded SEA API contract requires `row_offset` on
        // CloudFetch fetches; honour it here so the server-side
        // cursor lands on the right row.
        let row_offset = chunks
            .get(&start_chunk_index)
            .and_then(|e| e.link.as_ref().map(|l| l.row_offset))
            .unwrap_or(0);

        // Promoted to `info!`: refreshes are rare and
        // operationally interesting. Operators want to see them
        // without enabling debug logging across the kernel.
        info!(
            statement_id = %sid,
            "Issuing batched link refresh from chunk {} (row_offset={})",
            start_chunk_index, row_offset,
        );
        // Cancel-aware RPC: the SEA HTTP layer can retry for
        // hundreds of seconds; we want a provider drop or
        // statement cancel to unblock us promptly.
        let refreshed = tokio::select! {
            r = link_fetcher.refetch_links_from(start_chunk_index, row_offset) => r?,
            _ = cancel_token.cancelled() => {
                return Err(Error::invalid_state()
                    .message("Refresh RPC cancelled"));
            }
        };

        // Sanity cap on response size. A well-behaved server bounds
        // a single refresh page; a pathological server could
        // return millions of links and blow the chunks map up.
        // The cap is loose — any realistic page is well under it,
        // and exceeding it almost certainly signals a bug or a
        // misconfigured server.
        const MAX_REFRESH_RESPONSE_LINKS: usize = 10_000;
        if refreshed.len() > MAX_REFRESH_RESPONSE_LINKS {
            return Err(Error::invalid_state().message(format!(
                "Refresh response is implausibly large: {} links exceeds cap of {} \
                 (likely a server bug or misconfigured page size)",
                refreshed.len(),
                MAX_REFRESH_RESPONSE_LINKS,
            )));
        }

        // Bad-server defences:
        // 1. Refresh batch must include the chunk we asked about.
        //    Without this we'd loop refresh forever, never getting
        //    a fresh URL for our chunk.
        // 2. The contiguous range starting at `start_chunk_index`
        //    must have no interior holes. SEA pages chunks
        //    contiguously; a hole signals a partial response, and
        //    silently accepting it would leave the held-back
        //    chunk with a still-stale link → refresh loop forever
        //    on *that* chunk while siblings download cleanly.
        //
        // We do NOT also check `is_expired()` here — clock-skew
        // detection lives in `download_chunk_with_retry` after
        // this returns (the caller fetches the cached link and
        // sees still-expired ⇒ surfaces the same error). A single
        // fail-fast site is enough.
        let start_chunk_link = refreshed
            .iter()
            .find(|l| l.chunk_index == start_chunk_index);
        if start_chunk_link.is_none() {
            return Err(Error::invalid_state().message(format!(
                "Refresh response did not include chunk {} (server returned {} links: {:?}). \
                 Cannot proceed without a fresh link for the requested chunk.",
                start_chunk_index,
                refreshed.len(),
                refreshed.iter().map(|l| l.chunk_index).collect::<Vec<_>>(),
            )));
        }
        // Interior-hole check: assume contiguous from
        // `start_chunk_index`. Find the max chunk_index in the
        // response and verify every index in [start, max] is
        // present.
        if let Some(max_idx) = refreshed.iter().map(|l| l.chunk_index).max() {
            let present: std::collections::HashSet<i64> =
                refreshed.iter().map(|l| l.chunk_index).collect();
            let mut missing: Vec<i64> = Vec::new();
            for ci in start_chunk_index..=max_idx {
                if !present.contains(&ci) {
                    missing.push(ci);
                    if missing.len() >= 10 {
                        // Cap reported list — error message stays readable.
                        break;
                    }
                }
            }
            if !missing.is_empty() {
                return Err(Error::invalid_state().message(format!(
                    "Refresh response has interior holes in range [{}, {}]: \
                     missing chunks {:?}{}. Refusing to accept a partial window — \
                     held-back chunks would loop refresh forever.",
                    start_chunk_index,
                    max_idx,
                    missing,
                    if missing.len() == 10 {
                        " (truncated)"
                    } else {
                        ""
                    },
                )));
            }
        }

        // Apply the refresh to the chunks map. Per the doc on
        // this function: `Downloading | DownloadFailed |
        // DownloadRetry` get link-only updates so the in-flight
        // task continues to own the state machine.
        for fresh in refreshed {
            let ci = fresh.chunk_index;
            if let Some(mut entry) = chunks.get_mut(&ci) {
                match entry.state {
                    // No download task is active for `UrlFetched`;
                    // refresh updates the link in place so
                    // `schedule_downloads` picks the chunk up with
                    // the fresh URL.
                    //
                    // `Pending` is included for forward-compatibility:
                    // `ChunkEntry::pending()` always sets `link: None`,
                    // so no `Pending` entry exists in the chunks map
                    // today after initialization completes (every
                    // insertion uses `ChunkEntry::with_link`). If a
                    // future change introduces a link-less `Pending`
                    // entry within the refresh window, this arm
                    // promotes it to `UrlFetched` the moment a fresh
                    // link arrives — same as a normal `UrlFetched`
                    // entry getting its link replaced.
                    ChunkState::UrlFetched | ChunkState::Pending => {
                        entry.link = Some(fresh);
                        entry.state = ChunkState::UrlFetched;
                    }
                    // A task owns the state machine here.
                    // Update the link only — the task re-reads
                    // `entry.link` on its next retry and picks
                    // up the fresh URL transparently. Critically,
                    // we do NOT reset to `UrlFetched`: that would
                    // let `schedule_downloads` spawn a second
                    // task for the same chunk.
                    ChunkState::Downloading
                    | ChunkState::DownloadFailed(_)
                    | ChunkState::DownloadRetry => {
                        entry.link = Some(fresh);
                    }
                    // Terminal / already-have-bytes states. Keep
                    // link metadata current (future bookkeeping
                    // may read `entry.link.row_offset`) but no
                    // state change, no re-download.
                    ChunkState::Downloaded
                    | ChunkState::ProcessingFailed(_)
                    | ChunkState::Cancelled
                    | ChunkState::Released => {
                        entry.link = Some(fresh);
                    }
                }
            } else {
                // Brand-new chunk in the refresh batch — outside
                // the current consumer window. Insert with the
                // link so `schedule_downloads` can pick it up
                // when the consumer reaches it.
                chunks.insert(ci, ChunkEntry::with_link(fresh));
            }
        }

        Ok(())
    }

    /// Wait for a specific chunk to be ready (Downloaded state).
    async fn wait_for_chunk(&self, chunk_index: i64) -> Result<()> {
        loop {
            // Register the state-change waiter *before* the readiness /
            // terminal-error checks below. `notify_waiters()` (used by
            // `set_terminal_error` and the download tasks) only wakes
            // waiters that are already registered when it fires, so a
            // future constructed inline in the `select!` — i.e. after the
            // checks — could miss a wakeup that lands in that window and
            // stall until `chunk_ready_timeout`. `enable()` registers the
            // future with the `Notify` now, so any notification arriving
            // after the checks is captured and resolves the arm
            // immediately on the next poll. Mirrors the pre-registration
            // done for the refresh-coordinator wait.
            let state_changed = self.chunk_state_changed.notified();
            tokio::pin!(state_changed);
            state_changed.as_mut().enable();

            // Check cancellation
            if self.cancel_token.is_cancelled() {
                return Err(Error::invalid_state().message("Operation cancelled"));
            }

            // Check if the requested chunk is ready FIRST. A terminal
            // link-fetch failure only ever fails a page at or beyond
            // the download frontier, which runs ahead of the consumer
            // by up to `max_chunks_in_memory` chunks. Chunks between
            // the consumer's current position and the failed page may
            // already be downloaded and ready — checking readiness
            // before the terminal error preserves in-order delivery of
            // that buffered data, matching the per-chunk failure path
            // (which is keyed on the specific chunk entry). The
            // consumer only observes the terminal error once it reaches
            // the chunk whose link failed, at which point no entry
            // exists in `chunks` and the check below returns it.
            if let Some(entry) = self.chunks.get(&chunk_index) {
                match &entry.state {
                    ChunkState::Downloaded => return Ok(()),
                    // Return the preserved typed error verbatim so the
                    // consumer (including the C ABI / ODBC bridge) sees the
                    // original `ErrorCode` — e.g. `Timeout` for a deadline —
                    // rather than an opaque re-wrapped `Io` error (H05).
                    ChunkState::DownloadFailed(e) => return Err(e.clone()),
                    ChunkState::ProcessingFailed(e) => return Err(Error::io().message(e.clone())),
                    ChunkState::Cancelled => {
                        return Err(Error::invalid_state().message("Operation cancelled"))
                    }
                    _ => {} // Keep waiting
                }
            } else if self.end_of_stream.load(Ordering::Acquire) {
                // Chunk doesn't exist and we're at end of stream
                return Err(
                    Error::invalid_state().message(format!("Chunk {} not available", chunk_index))
                );
            } else if let Some(err) = self.terminal_error() {
                // The requested chunk has no entry (its link fetch is
                // what failed) and we are not at a clean end of stream:
                // surface the persisted terminal error rather than
                // looping forever. Placed after the ready-chunk check
                // so already-downloaded in-order chunks are still
                // delivered before the failure is reported.
                return Err(err);
            }

            // Wait for any chunk state change with timeout
            let timeout =
                self.config
                    .chunk_ready_timeout
                    .unwrap_or(std::time::Duration::from_secs(
                        DEFAULT_CHUNK_READY_TIMEOUT_SECS,
                    ));

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    return Err(Error::invalid_state().message("Operation cancelled"));
                }
                _ = &mut state_changed => {
                    // State changed, loop to check
                }
                _ = tokio::time::sleep(timeout) => {
                    // Timeout - check state again and continue
                    debug!("Timeout waiting for chunk {}, continuing...", chunk_index);
                }
            }
        }
    }

    /// Get the schema, waiting if necessary.
    pub async fn get_schema(&self) -> Result<SchemaRef> {
        if let Some(schema) = self.schema.get() {
            return Ok(schema.clone());
        }

        // Need to peek the first batch to get schema
        let chunk_index = 0i64;

        // Ensure chunk 0 is being downloaded
        self.schedule_downloads().await;

        // Wait for chunk 0
        self.wait_for_chunk(chunk_index).await?;

        // Get schema from the first batch
        if let Some(entry) = self.chunks.get(&chunk_index) {
            if let Some(ref batches) = entry.batches {
                if let Some(batch) = batches.first() {
                    let schema = batch.schema();
                    let _ = self.schema.get_or_init(|| schema.clone());
                    return Ok(schema);
                }
            }
        }

        Err(Error::invalid_state().message("Unable to determine schema"))
    }
}

impl Drop for StreamingCloudFetchProvider {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChunkLinkFetchResult;
    use crate::types::cloudfetch::CloudFetchLink;
    use std::collections::HashMap;

    /// Mock ChunkLinkFetcher for testing
    #[allow(dead_code)]
    #[derive(Debug)]
    struct MockLinkFetcher {
        links_by_index: HashMap<i64, CloudFetchLink>,
        total_chunks: i64,
        scheduled: Mutex<Vec<i64>>,
        fetch_gate: Option<Arc<Notify>>,
    }

    impl MockLinkFetcher {
        #[allow(dead_code)]
        fn new(total_chunks: i64) -> Self {
            let mut links_by_index = HashMap::new();
            for i in 0..total_chunks {
                links_by_index.insert(i, create_test_link(i));
            }
            Self {
                links_by_index,
                total_chunks,
                scheduled: Mutex::new(Vec::new()),
                fetch_gate: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        fn note_scheduled(&self, chunk_index: i64) {
            self.scheduled.lock().unwrap().push(chunk_index);
        }

        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            if let Some(gate) = &self.fetch_gate {
                gate.notified().await;
            }
            if start_chunk_index >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let links: Vec<CloudFetchLink> = (start_chunk_index..self.total_chunks)
                .take(3) // Return up to 3 links at a time
                .filter_map(|i| self.links_by_index.get(&i).cloned())
                .collect();

            let has_more = start_chunk_index + (links.len() as i64) < self.total_chunks;
            let next = if has_more {
                Some(start_chunk_index + links.len() as i64)
            } else {
                None
            };

            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: next,
                next_row_offset: None,
            })
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            self.links_by_index
                .get(&chunk_index)
                .cloned()
                .ok_or_else(|| {
                    Error::invalid_state()
                        .message(format!("Link not found for chunk {}", chunk_index))
                })
        }

        async fn refetch_links_from(
            &self,
            chunk_index: i64,
            _row_offset: i64,
        ) -> Result<Vec<CloudFetchLink>> {
            // Test mock: return all links at chunk_index and beyond,
            // ordered by chunk_index. Real SEA returns a server-
            // decided batch starting at the requested index; this is
            // good enough for state-machine tests.
            let mut links: Vec<_> = self
                .links_by_index
                .iter()
                .filter(|(idx, _)| **idx >= chunk_index)
                .map(|(_, link)| link.clone())
                .collect();
            links.sort_by_key(|l| l.chunk_index);
            Ok(links)
        }
    }

    fn create_test_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            url: format!("https://storage.example.com/chunk{}", chunk_index),
            chunk_index,
            row_offset: chunk_index * 1000,
            row_count: 1000,
            byte_count: 50000,
            expiration: chrono::Utc::now() + chrono::Duration::hours(1),
            http_headers: HashMap::new(),
            next_chunk_index: Some(chunk_index + 1),
        }
    }

    #[tokio::test]
    async fn cancellation_interrupts_link_fetch_during_refill_and_initialization() {
        for initial in [false, true] {
            let mut fetcher = MockLinkFetcher::new(2);
            fetcher.fetch_gate = Some(Arc::new(Notify::new()));
            let chunks = Arc::new(DashMap::new());
            let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
            chunks.insert(
                0,
                ChunkEntry {
                    link: Some(create_test_link(0)),
                    state: ChunkState::Downloaded,
                    batches: Some(vec![batch]),
                    ..ChunkEntry::with_link(create_test_link(0))
                },
            );
            let provider = StreamingCloudFetchProvider::for_test_with_chunks(
                Arc::new(fetcher),
                noop_downloader(),
                chunks,
                false,
            );
            provider.chunks_in_memory.store(1, Ordering::Release);
            provider.next_download_index.store(1, Ordering::Release);
            if initial {
                let mut initialize = Box::pin(provider.initialize());
                assert!(futures::poll!(initialize.as_mut()).is_pending());
                provider.cancel();
                tokio::time::timeout(NO_HANG, initialize).await.unwrap();
            } else {
                let mut read = Box::pin(provider.next_batch());
                assert!(futures::poll!(read.as_mut()).is_pending());
                assert_eq!(provider.current_chunk_index.load(Ordering::Acquire), 1);
                provider.cancel();
                assert!(tokio::time::timeout(NO_HANG, read)
                    .await
                    .unwrap()
                    .unwrap_err()
                    .message
                    .contains("cancel"));
            }
            assert!(provider.terminal_error().is_none());
            assert!(provider
                .next_batch()
                .await
                .unwrap_err()
                .message
                .contains("cancel"));
        }
    }

    #[tokio::test]
    async fn bounded_downloads_notify_each_scheduled_chunk_in_order() {
        use crate::client::retry::{build_retry_configs, RetryConfig};
        use crate::client::{DatabricksHttpClient, HttpClientConfig};
        use crate::types::sea::CompressionCodec;
        use arrow_ipc::writer::StreamWriter;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut fetcher = MockLinkFetcher::new(8);
        for i in 0..8 {
            let batch = make_batch(&[i as i32]);
            let mut bytes = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            let route = format!("/chunk-{i}");
            Mock::given(method("GET"))
                .and(path(&route))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
                .expect(1)
                .mount(&server)
                .await;
            fetcher.links_by_index.get_mut(&i).unwrap().url = format!("{}{route}", server.uri());
        }
        let fetcher = Arc::new(fetcher);
        let client = DatabricksHttpClient::new(
            HttpClientConfig::default(),
            build_retry_configs(
                &RetryConfig {
                    max_retries: 0,
                    ..RetryConfig::default()
                },
                &HashMap::new(),
            ),
        )
        .unwrap();
        let provider = StreamingCloudFetchProvider::new(
            CloudFetchConfig {
                max_chunks_in_memory: 2,
                ..CloudFetchConfig::default()
            },
            fetcher.clone(),
            Arc::new(ChunkDownloader::new(
                Arc::new(client),
                CompressionCodec::None,
                0.1,
            )),
            Some(8),
            tokio::runtime::Handle::current(),
        );
        let mut values = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(batch) = provider.next_batch().await.unwrap() {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                values.extend_from_slice(ids.values());
                assert!(provider.chunks_in_memory.load(Ordering::Acquire) <= 2);
            }
        })
        .await
        .expect("all eight chunks must drain with a two-chunk window");
        assert_eq!(values, (0..8).collect::<Vec<i32>>());
        assert_eq!(
            *fetcher.scheduled.lock().unwrap(),
            (0..8).collect::<Vec<i64>>()
        );
    }

    /// Regression (PECOBLR-4026): a panicking download task must surface as an
    /// error rather than being silently swallowed — a swallowed panic leaves the
    /// chunk stuck `Downloading` and wedges the stream forever. Verifies
    /// `join_download_task` converts a task panic into an error.
    #[tokio::test]
    async fn panicking_download_task_surfaces_as_error() {
        let handle = tokio::runtime::Handle::current();
        let result = join_download_task(&handle, async {
            panic!("simulated download-task panic");
        })
        .await;
        let err = result.expect_err("a panicking download task must yield an error, not Ok");
        assert!(
            err.to_string().contains("download task panicked"),
            "error should identify a panicked download task, got: {err}"
        );
    }

    #[tokio::test]
    async fn persistent_download_failure_is_bounded_when_links_expire_in_flight() {
        for status in [403, 500] {
            assert_persistent_download_failure_is_bounded(status).await;
        }
    }

    async fn assert_persistent_download_failure_is_bounded(status: u16) {
        use crate::client::retry::{build_retry_configs, RetryConfig};
        use crate::client::{DatabricksHttpClient, HttpClientConfig};
        use crate::types::cloudfetch::LINK_EXPIRY_BUFFER_SECS;
        use crate::types::sea::CompressionCodec;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const RESPONSE_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
        const MAX_DOWNLOAD_ATTEMPTS: u32 = 2;

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/chunk-0.arrow"))
            .respond_with(ResponseTemplate::new(status).set_delay(RESPONSE_DELAY))
            .mount(&server)
            .await;

        // Disable the HTTP client's own retries so this test isolates the
        // provider-level download budget.
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig::default(),
                build_retry_configs(&retry, &HashMap::new()),
            )
            .unwrap(),
        );
        let downloader = Arc::new(ChunkDownloader::new(
            http_client,
            CompressionCodec::None,
            0.1,
        ));

        #[derive(Debug)]
        struct BoundaryRefreshFetcher {
            url: String,
            refresh_calls: AtomicUsize,
        }

        impl BoundaryRefreshFetcher {
            fn link(&self) -> CloudFetchLink {
                CloudFetchLink {
                    url: self.url.clone(),
                    expiration: chrono::Utc::now()
                        + chrono::Duration::seconds(LINK_EXPIRY_BUFFER_SECS)
                        + chrono::Duration::milliseconds(50),
                    ..create_test_link(0)
                }
            }
        }

        #[async_trait::async_trait]
        impl ChunkLinkFetcher for BoundaryRefreshFetcher {
            async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
                unreachable!()
            }

            async fn refetch_link(&self, _: i64, _: i64) -> Result<CloudFetchLink> {
                unreachable!()
            }

            async fn refetch_links_from(&self, _: i64, _: i64) -> Result<Vec<CloudFetchLink>> {
                self.refresh_calls.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(vec![self.link()])
            }
        }

        let fetcher = Arc::new(BoundaryRefreshFetcher {
            url: format!("{}/chunk-0.arrow", server.uri()),
            refresh_calls: AtomicUsize::new(0),
        });
        let chunks = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(fetcher.link(), ChunkState::Downloading));
        let ctx = DownloadCtx {
            downloader,
            link_fetcher: fetcher,
            chunks,
            refresh_coordinator: Arc::new(RefreshCoordinator::default()),
            cancel_token: CancellationToken::new(),
            statement_id: None,
            metrics: Arc::new(CloudFetchMetrics::new(Some(1))),
            max_retries: MAX_DOWNLOAD_ATTEMPTS,
            retry_delay: std::time::Duration::ZERO,
            // This test isolates the attempt-count budget; disable the
            // wall-clock deadline (0 = disabled) so it doesn't interfere.
            download_timeout: std::time::Duration::ZERO,
        };

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(700),
            StreamingCloudFetchProvider::download_chunk_with_retry(0, &ctx),
        )
        .await
        .expect("persistent failures must exhaust the download attempt budget");

        let error = result.expect_err("persistent HTTP failure must be terminal");
        assert!(
            error.message.contains(&format!("HTTP {status}")),
            "unexpected error: {error}"
        );
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            MAX_DOWNLOAD_ATTEMPTS as usize,
            "in-flight link expiry must not bypass the download attempt budget",
        );
    }

    // ─── End-to-end download deadline (issue #321 / audit H05) ───────────
    //
    // These tests exercise `download_chunk_with_retry`'s absolute
    // `download_timeout` deadline: a stalled connection / headers / body
    // and repeated failures must not let a single chunk outrun the budget.
    // Each drives the real `ChunkDownloader` against a `wiremock` server so
    // reqwest's per-request timeout (the actual cap) is genuinely exercised,
    // not mocked away.
    mod download_deadline {
        use super::*;
        use crate::client::retry::{build_retry_configs, RetryConfig};
        use crate::client::{DatabricksHttpClient, HttpClientConfig};
        use crate::kernel_error::ErrorCode;
        use crate::types::sea::CompressionCodec;
        use std::time::{Duration, Instant};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// A link fetcher that only ever serves chunk 0's link pointing at
        /// `url`. Refresh paths are unreachable — the link never expires
        /// (1h out), so these tests isolate the download deadline from any
        /// refresh behaviour.
        #[derive(Debug)]
        struct SingleLinkFetcher {
            url: String,
        }

        #[async_trait::async_trait]
        impl ChunkLinkFetcher for SingleLinkFetcher {
            async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
                unreachable!("deadline tests seed the chunks map directly")
            }
            async fn refetch_link(&self, _: i64, _: i64) -> Result<CloudFetchLink> {
                unreachable!("link never expires in these tests")
            }
            async fn refetch_links_from(&self, _: i64, _: i64) -> Result<Vec<CloudFetchLink>> {
                unreachable!("link never expires in these tests")
            }
        }

        impl SingleLinkFetcher {
            fn link(&self) -> CloudFetchLink {
                CloudFetchLink {
                    url: self.url.clone(),
                    ..create_test_link(0)
                }
            }
        }

        /// Build a `DownloadCtx` whose downloader targets `server`, with the
        /// given end-to-end `download_timeout` and (large) `max_retries` so
        /// the wall-clock deadline — not the attempt count — is what bounds
        /// the loop. The inner HTTP client's own retries are disabled so the
        /// provider is the sole retry authority (mirrors production).
        fn ctx_for(
            server: &MockServer,
            download_timeout: Duration,
            cancel_token: CancellationToken,
        ) -> DownloadCtx {
            let http_client = Arc::new(
                DatabricksHttpClient::new(
                    HttpClientConfig::default(),
                    build_retry_configs(
                        &RetryConfig {
                            max_retries: 0,
                            ..RetryConfig::default()
                        },
                        &HashMap::new(),
                    ),
                )
                .unwrap(),
            );
            let downloader = Arc::new(ChunkDownloader::new(
                http_client,
                CompressionCodec::None,
                0.1,
            ));
            let fetcher = Arc::new(SingleLinkFetcher {
                url: format!("{}/chunk-0.arrow", server.uri()),
            });
            let chunks = Arc::new(DashMap::new());
            chunks.insert(0, entry_with_state(fetcher.link(), ChunkState::Downloading));
            DownloadCtx {
                downloader,
                link_fetcher: fetcher,
                chunks,
                refresh_coordinator: Arc::new(RefreshCoordinator::default()),
                cancel_token,
                statement_id: None,
                metrics: Arc::new(CloudFetchMetrics::new(Some(1))),
                // Large attempt budget so ONLY the wall-clock deadline can
                // stop the loop — proves the deadline, not the counter.
                max_retries: 10_000,
                retry_delay: Duration::from_millis(20),
                download_timeout,
            }
        }

        /// A stalled response body (headers arrive, body never completes)
        /// must be cut off by the deadline, not the static request_timeout.
        /// Without the per-attempt cap this hangs for the client's
        /// `request_timeout` (default 120s) per attempt.
        #[tokio::test]
        async fn stalled_body_is_bounded_by_deadline() {
            let server = MockServer::start().await;
            // Delay far beyond the budget: stands in for a body that never
            // makes progress. The per-attempt timeout must fire first.
            Mock::given(method("GET"))
                .and(path("/chunk-0.arrow"))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
                .mount(&server)
                .await;

            let budget = Duration::from_millis(300);
            let ctx = ctx_for(&server, budget, CancellationToken::new());

            let started = Instant::now();
            // Outer guard is ~10x the budget: if the deadline is NOT
            // enforced this trips and fails loudly instead of hanging CI.
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                StreamingCloudFetchProvider::download_chunk_with_retry(0, &ctx),
            )
            .await
            .expect("a stalled body must be bounded by the download deadline, not hang");
            let elapsed = started.elapsed();

            let err = result.expect_err("a stalled body must surface a terminal error");
            assert_eq!(
                err.code,
                ErrorCode::Timeout,
                "deadline expiry must be classified as a timeout, got: {err}"
            );
            assert!(
                err.message.contains("deadline"),
                "unexpected error message: {err}"
            );
            // Elapsed stays within a documented scheduling tolerance of the
            // budget (≤ budget + generous slack for scheduler/backoff).
            assert!(
                elapsed < budget + Duration::from_secs(1),
                "elapsed {elapsed:?} should stay within tolerance of the {budget:?} budget"
            );
        }

        /// Repeated failures (persistent HTTP 500, each returned promptly)
        /// must stop at the wall-clock deadline rather than running the full
        /// attempt count. Proves the deadline bounds the retry/backoff loop.
        #[tokio::test]
        async fn retry_exhaustion_is_bounded_by_deadline() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/chunk-0.arrow"))
                .respond_with(ResponseTemplate::new(500))
                .mount(&server)
                .await;

            let budget = Duration::from_millis(250);
            let ctx = ctx_for(&server, budget, CancellationToken::new());

            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                StreamingCloudFetchProvider::download_chunk_with_retry(0, &ctx),
            )
            .await
            .expect("retry loop must stop at the deadline, not run 10k attempts");
            let elapsed = started.elapsed();

            let err = result.expect_err("persistent 500 within the budget must be terminal");
            assert_eq!(
                err.code,
                ErrorCode::Timeout,
                "budget exhaustion must be classified as a timeout, got: {err}"
            );
            assert!(
                elapsed < budget + Duration::from_secs(1),
                "elapsed {elapsed:?} should stay within tolerance of the {budget:?} budget"
            );
            // Far fewer than max_retries attempts — the deadline, not the
            // 10k counter, stopped the loop.
            let attempts = server.received_requests().await.unwrap().len();
            assert!(
                attempts < 100,
                "deadline should stop the loop well before the 10k attempt cap, got {attempts}"
            );
        }

        /// A cancellation while a chunk is retrying under a live deadline
        /// must be observed promptly (at the next loop boundary) and win —
        /// the loop must not run out the remaining budget once cancelled.
        /// The chunk keeps failing fast (HTTP 500 returned immediately), so
        /// attempts turn over quickly and the loop-top cancel check fires
        /// well before the multi-second budget would.
        #[tokio::test]
        async fn cancel_under_live_deadline_returns_promptly() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/chunk-0.arrow"))
                .respond_with(ResponseTemplate::new(500))
                .mount(&server)
                .await;

            // Long budget: only cancellation, not the deadline, should end
            // this loop. If cancel is NOT observed, the loop would spin on
            // fast 500s until the 10s budget — the outer 3s guard catches
            // that regression.
            let cancel = CancellationToken::new();
            let ctx = ctx_for(&server, Duration::from_secs(10), cancel.clone());

            let canceller = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                canceller.cancel();
            });

            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                StreamingCloudFetchProvider::download_chunk_with_retry(0, &ctx),
            )
            .await
            .expect("cancellation must be observed promptly, not run out the budget");
            let elapsed = started.elapsed();

            let err = result.expect_err("a cancelled download must be an error");
            // The "Download cancelled" arm uses `Error::invalid_state()`,
            // which maps to `ErrorCode::InvalidArgument`.
            assert_eq!(
                err.code,
                ErrorCode::InvalidArgument,
                "cancel should win over the (far-off) deadline, got: {err}"
            );
            assert!(
                err.message.contains("cancelled"),
                "unexpected error message: {err}"
            );
            // Returned near the cancel time, nowhere near the 10s budget.
            assert!(
                elapsed < Duration::from_secs(1),
                "cancel should return promptly (~50ms), got {elapsed:?}"
            );
        }

        /// H05 end-to-end: the deadline error's `ErrorCode::Timeout`
        /// classification must survive the chunk-state machine and reach
        /// the *consumer* path (`wait_for_chunk`), not just the direct
        /// return of `download_chunk_with_retry`. Before the fix,
        /// `ChunkState::DownloadFailed(String)` dropped the code and
        /// `wait_for_chunk` re-wrapped it as `Error::io()`
        /// (`ErrorCode::Internal`) — the very "opaque I/O error" the doc
        /// promised consumers would not see.
        #[tokio::test]
        async fn deadline_timeout_classification_survives_to_consumer() {
            // Minimal dummy deps — never exercised, `wait_for_chunk`
            // only reads the seeded chunk state.
            let http_client = Arc::new(
                DatabricksHttpClient::new(
                    HttpClientConfig::default(),
                    build_retry_configs(&RetryConfig::default(), &HashMap::new()),
                )
                .unwrap(),
            );
            let downloader = Arc::new(ChunkDownloader::new(
                http_client,
                CompressionCodec::None,
                0.1,
            ));
            let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(SingleLinkFetcher {
                url: "https://unused.example.com/chunk-0.arrow".to_string(),
            });

            // Seed chunk 0 exactly as the download task would on a
            // deadline expiry: DownloadFailed carrying the typed
            // timeout error.
            let deadline_err =
                StreamingCloudFetchProvider::download_deadline_error(0, Duration::from_millis(300));
            assert_eq!(deadline_err.code, ErrorCode::Timeout);
            let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
            chunks.insert(
                0,
                ChunkEntry {
                    link: None,
                    state: ChunkState::DownloadFailed(deadline_err),
                    batches: None,
                    last_failure_reason: None,
                },
            );

            let provider = StreamingCloudFetchProvider::for_test_with_chunks(
                fetcher, downloader, chunks, true,
            );

            let err = provider
                .wait_for_chunk(0)
                .await
                .expect_err("a deadline-failed chunk must surface an error to the consumer");
            assert_eq!(
                err.code,
                ErrorCode::Timeout,
                "consumer path must preserve the Timeout classification, got: {err}"
            );
            assert!(
                err.retryable,
                "a preserved timeout error must keep its retryable=true flag"
            );
            assert!(
                err.message.contains("deadline"),
                "the original deadline message must be preserved: {err}"
            );
        }
    }

    #[test]
    fn test_chunk_link_fetch_result_end_of_stream() {
        let result = ChunkLinkFetchResult::end_of_stream();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
        assert!(result.next_chunk_index.is_none());
    }

    // Note: Full streaming provider tests require integration test setup
    // with mock HTTP responses for the chunk downloader

    // --- merge_batches_to_target unit tests ---

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn make_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from(values.to_vec());
        let names = StringArray::from(
            values
                .iter()
                .map(|v| format!("row_{}", v))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    #[test]
    fn test_merge_empty() {
        let result = merge_batches_to_target(vec![], 100).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_merge_single_batch_passthrough() {
        let batch = make_batch(&[1, 2, 3]);
        let result = merge_batches_to_target(vec![batch], 100).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_merge_all_fit_in_one_target() {
        // 3 batches totaling 9 rows, target=100 → all merge into 1
        let b1 = make_batch(&[1, 2, 3]);
        let b2 = make_batch(&[4, 5]);
        let b3 = make_batch(&[6, 7, 8, 9]);

        let result = merge_batches_to_target(vec![b1, b2, b3], 100).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 9);

        // Verify data order is preserved
        let ids = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual: Vec<i32> = (0..ids.len()).map(|i| ids.value(i)).collect();
        assert_eq!(actual, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_merge_splits_at_target() {
        // 5 batches of 3 rows each (15 total), target=7 → expect 3 merged batches
        // Batch 1-3: 3+3+3=9 rows (>=7, flush) → 9 rows
        // Batch 4-5: 3+3=6 rows (end, flush) → 6 rows
        let batches: Vec<RecordBatch> = (0..5)
            .map(|i| {
                let start = i * 3 + 1;
                make_batch(&[start, start + 1, start + 2])
            })
            .collect();

        let result = merge_batches_to_target(batches, 7).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 9); // first 3 batches
        assert_eq!(result[1].num_rows(), 6); // last 2 batches

        // Verify ordering across merged batches
        let ids0 = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ids1 = result[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let all: Vec<i32> = (0..ids0.len())
            .map(|i| ids0.value(i))
            .chain((0..ids1.len()).map(|i| ids1.value(i)))
            .collect();
        let expected: Vec<i32> = (1..=15).collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn test_merge_single_large_batch_exceeds_target() {
        // A single batch with 200 rows and target=100 → passed through as-is
        let values: Vec<i32> = (1..=200).collect();
        let batch = make_batch(&values);
        let result = merge_batches_to_target(vec![batch], 100).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 200);
    }

    #[test]
    fn test_merge_preserves_schema() {
        let b1 = make_batch(&[1]);
        let b2 = make_batch(&[2]);
        let original_schema = b1.schema();

        let result = merge_batches_to_target(vec![b1, b2], 100).unwrap();
        assert_eq!(result[0].schema(), original_schema);
    }

    // --- refresh_links_batch unit tests ---
    //
    // These test the static method directly. They construct a tiny
    // chunks DashMap by hand, prime it with entries in various
    // states, then call `StreamingCloudFetchProvider::refresh_links_batch`
    // and assert the right state transitions + RPC count.

    use crate::types::cloudfetch::{ChunkEntry, ChunkState};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Mock fetcher that records how many `refetch_links_from`
    /// calls were made. Returns synthetic refreshed links with
    /// distinct URLs so the test can tell new from stale.
    #[derive(Debug)]
    struct RefreshCountingMock {
        refresh_calls: AtomicUsize,
        // Range of chunk_indices the mock will return on each
        // refresh, starting at the requested start_chunk_index.
        window: i64,
    }

    impl RefreshCountingMock {
        fn new(window: i64) -> Arc<Self> {
            Arc::new(Self {
                refresh_calls: AtomicUsize::new(0),
                window,
            })
        }

        fn fresh_link(chunk_index: i64) -> CloudFetchLink {
            CloudFetchLink {
                url: format!("https://storage.example.com/REFRESHED/chunk{}", chunk_index),
                chunk_index,
                row_offset: chunk_index * 1000,
                row_count: 1000,
                byte_count: 50000,
                expiration: chrono::Utc::now() + chrono::Duration::hours(1),
                http_headers: HashMap::new(),
                next_chunk_index: Some(chunk_index + 1),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for RefreshCountingMock {
        async fn fetch_links(
            &self,
            _start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            unreachable!("not used by these tests")
        }

        async fn refetch_link(
            &self,
            _chunk_index: i64,
            _row_offset: i64,
        ) -> Result<CloudFetchLink> {
            unreachable!("not used by these tests")
        }

        async fn refetch_links_from(
            &self,
            chunk_index: i64,
            _row_offset: i64,
        ) -> Result<Vec<CloudFetchLink>> {
            self.refresh_calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok((chunk_index..chunk_index + self.window)
                .map(Self::fresh_link)
                .collect())
        }
    }

    /// Build a CloudFetchLink whose `expiration` is in the past.
    fn expired_link(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            expiration: chrono::Utc::now() - chrono::Duration::seconds(60),
            ..create_test_link(chunk_index)
        }
    }

    fn entry_with_state(link: CloudFetchLink, state: ChunkState) -> ChunkEntry {
        ChunkEntry {
            link: Some(link),
            state,
            batches: None,
            last_failure_reason: None,
        }
    }

    /// Build a downloader that is never exercised by the non-blocking
    /// consumer-path tests below (they seed chunk state directly and set
    /// `end_of_stream`, so the spawned `schedule_downloads` refill exits
    /// without issuing an RPC). Points at an unroutable URL and disables
    /// retries so any accidental download fails fast rather than hanging.
    fn unused_downloader() -> Arc<ChunkDownloader> {
        use crate::client::retry::{build_retry_configs, RetryConfig};
        use crate::client::{DatabricksHttpClient, HttpClientConfig};
        use crate::types::sea::CompressionCodec;

        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig::default(),
                build_retry_configs(&retry, &HashMap::new()),
            )
            .unwrap(),
        );
        Arc::new(ChunkDownloader::new(
            http_client,
            CompressionCodec::None,
            0.1,
        ))
    }

    /// `try_next_batch` must return `Pending` (without blocking) while the
    /// head chunk is not yet `Downloaded`, then `Ready` once it flips, and
    /// finally `End` after the last chunk is consumed. This exercises the
    /// non-blocking chunk-state machine that diverges from `next_batch`.
    #[tokio::test]
    async fn try_next_batch_pending_then_ready_then_end() {
        use crate::reader::TryNextBatch;

        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        // Chunk 0 present but still downloading — not ready to yield.
        chunks.insert(
            0,
            entry_with_state(create_test_link(0), ChunkState::Downloading),
        );
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(1));
        let provider = StreamingCloudFetchProvider::for_test_with_chunks(
            fetcher,
            unused_downloader(),
            Arc::clone(&chunks),
            /* end_of_stream = */ true,
        );
        // One chunk resident: `release_downloaded_chunk` decrements this.
        provider.chunks_in_memory.store(1, Ordering::Release);

        // Not downloaded yet → Pending, and it must NOT block.
        assert!(
            matches!(provider.try_next_batch().unwrap(), TryNextBatch::Pending),
            "a not-yet-Downloaded head chunk must report Pending"
        );

        // Flip the chunk to Downloaded with real batches, as a download
        // task would.
        {
            let mut entry = chunks.get_mut(&0).unwrap();
            entry.state = ChunkState::Downloaded;
            entry.batches = Some(vec![make_batch(&[1, 2, 3])]);
        }

        match provider.try_next_batch().unwrap() {
            TryNextBatch::Ready(batch) => assert_eq!(batch.num_rows(), 3),
            _ => panic!("expected Ready after chunk flipped to Downloaded"),
        }

        // Head chunk consumed; end_of_stream + no more chunks → End.
        assert!(
            matches!(provider.try_next_batch().unwrap(), TryNextBatch::End),
            "after the only chunk is drained the stream must report End"
        );
    }

    #[tokio::test]
    async fn try_next_batch_yields_on_lock_contention() {
        use crate::reader::TryNextBatch;

        fn assert_pending<G>(provider: &Arc<StreamingCloudFetchProvider>, guard: G) {
            let (sender, receiver) = std::sync::mpsc::channel();
            let provider = Arc::clone(provider);
            let probe = std::thread::spawn(move || {
                sender.send(provider.try_next_batch()).unwrap();
            });
            let outcome = receiver.recv_timeout(NO_HANG);
            // Release before joining so a blocking regression fails by timeout.
            drop(guard);
            probe.join().unwrap();
            assert!(matches!(
                outcome.expect("probe waited for a held lock").unwrap(),
                TryNextBatch::Pending
            ));
        }

        enum HeldLock {
            Buffer,
            ChunkRead,
            ChunkWrite,
            EmptyShard,
            TerminalError,
            TerminalErrorAfterEmptyChunk,
        }

        for held in [
            HeldLock::Buffer,
            HeldLock::ChunkRead,
            HeldLock::ChunkWrite,
            HeldLock::EmptyShard,
            HeldLock::TerminalError,
            HeldLock::TerminalErrorAfterEmptyChunk,
        ] {
            let terminal = matches!(
                held,
                HeldLock::TerminalError | HeldLock::TerminalErrorAfterEmptyChunk
            );
            let skipped = i64::from(matches!(held, HeldLock::TerminalErrorAfterEmptyChunk));
            let chunks = Arc::new(DashMap::new());
            if skipped > 0 {
                chunks.insert(
                    0,
                    entry_with_state(create_test_link(0), ChunkState::Downloaded),
                );
            }
            if !matches!(held, HeldLock::EmptyShard) {
                let mut entry = entry_with_state(
                    create_test_link(skipped),
                    if terminal {
                        ChunkState::Downloading
                    } else {
                        ChunkState::Downloaded
                    },
                );
                entry.batches = Some(vec![make_batch(&[1]), make_batch(&[2])]);
                chunks.insert(skipped, entry);
            }
            let provider = StreamingCloudFetchProvider::for_test_with_chunks(
                Arc::new(MockLinkFetcher::new(2)),
                unused_downloader(),
                Arc::clone(&chunks),
                true,
            );
            provider
                .chunks_in_memory
                .store(chunks.len(), Ordering::Release);
            if terminal {
                *provider.terminal_error.lock().unwrap() =
                    Some(Error::unavailable().message("link fetch failed"));
            }
            if matches!(held, HeldLock::Buffer) {
                provider
                    .current_batch_buffer
                    .lock()
                    .unwrap()
                    .push_back(make_batch(&[0]));
            }

            match held {
                HeldLock::Buffer => {
                    assert_pending(&provider, provider.current_batch_buffer.lock().unwrap())
                }
                HeldLock::ChunkRead => assert_pending(&provider, chunks.get(&0).unwrap()),
                HeldLock::ChunkWrite => assert_pending(&provider, chunks.get_mut(&0).unwrap()),
                HeldLock::EmptyShard => assert_pending(&provider, chunks.entry(0)),
                HeldLock::TerminalError | HeldLock::TerminalErrorAfterEmptyChunk => {
                    assert_pending(&provider, provider.terminal_error.lock().unwrap());
                }
            }
            assert_eq!(
                provider.current_chunk_index.load(Ordering::Acquire),
                skipped
            );
            assert_eq!(
                provider.chunks_in_memory.load(Ordering::Acquire),
                chunks.len()
            );
            assert_eq!(
                provider
                    .metrics
                    .total_chunks_iterated
                    .load(Ordering::Acquire),
                skipped as i32
            );
            if terminal {
                let error = provider
                    .try_next_batch()
                    .expect_err("terminal error must survive contention");
                assert_eq!(error.code, ErrorCode::Unavailable);
                assert_eq!(error.message, "link fetch failed");
            } else {
                let expected = match held {
                    HeldLock::Buffer => vec![0, 1, 2],
                    HeldLock::EmptyShard => vec![],
                    _ => vec![1, 2],
                };
                for value in expected {
                    match provider.try_next_batch().unwrap() {
                        TryNextBatch::Ready(batch) => assert_eq!(batch, make_batch(&[value])),
                        _ => panic!("buffered batches must survive contention in order"),
                    }
                }
                assert!(matches!(
                    provider.try_next_batch().unwrap(),
                    TryNextBatch::End
                ));
            }
        }
    }

    /// A `Downloaded` chunk that decoded to zero batches must be skipped
    /// (the cursor advancing past it) so the caller sees the *next*
    /// chunk's data — not a spurious `Pending`/`End`. Guards the
    /// empty-downloaded-chunk skip in the non-blocking loop.
    #[tokio::test]
    async fn try_next_batch_skips_empty_downloaded_chunk() {
        use crate::reader::TryNextBatch;

        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        // Chunk 0: Downloaded but empty (legal on paginated results).
        chunks.insert(
            0,
            ChunkEntry {
                link: Some(create_test_link(0)),
                state: ChunkState::Downloaded,
                batches: Some(vec![]),
                last_failure_reason: None,
            },
        );
        // Chunk 1: Downloaded with real data.
        chunks.insert(
            1,
            ChunkEntry {
                link: Some(create_test_link(1)),
                state: ChunkState::Downloaded,
                batches: Some(vec![make_batch(&[7, 8])]),
                last_failure_reason: None,
            },
        );
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(2));
        let provider = StreamingCloudFetchProvider::for_test_with_chunks(
            fetcher,
            unused_downloader(),
            Arc::clone(&chunks),
            /* end_of_stream = */ true,
        );
        provider.chunks_in_memory.store(2, Ordering::Release);

        // The empty chunk 0 must not stall the stream: the loop advances
        // past it and yields chunk 1's batch in a single call.
        match provider.try_next_batch().unwrap() {
            TryNextBatch::Ready(batch) => assert_eq!(batch.num_rows(), 2),
            _ => panic!("empty chunk must be skipped to reach chunk 1"),
        }
        assert!(
            matches!(provider.try_next_batch().unwrap(), TryNextBatch::End),
            "both chunks consumed → End"
        );
    }

    /// A terminal per-chunk failure at the head must surface as `Err`
    /// (not `Pending`/`End`) so the non-blocking caller sees the real
    /// cause deterministically. `DownloadFailed` carries a typed error
    /// that must be returned verbatim; `ProcessingFailed` carries a
    /// message that is re-wrapped as `Error::io()`.
    #[tokio::test]
    async fn try_next_batch_surfaces_terminal_chunk_errors() {
        use crate::kernel_error::ErrorCode;

        // DownloadFailed → the carried typed error is returned as-is,
        // preserving its classification and retryable flag.
        {
            let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
            let download_err = Error::network_error().message("chunk 0 download blew up");
            assert_eq!(download_err.code, ErrorCode::NetworkError);
            chunks.insert(
                0,
                entry_with_state(
                    create_test_link(0),
                    ChunkState::DownloadFailed(download_err),
                ),
            );
            let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(1));
            let provider = StreamingCloudFetchProvider::for_test_with_chunks(
                fetcher,
                unused_downloader(),
                Arc::clone(&chunks),
                /* end_of_stream = */ true,
            );

            let err = match provider.try_next_batch() {
                Err(err) => err,
                Ok(_) => panic!("a DownloadFailed head chunk must surface an error"),
            };
            assert_eq!(
                err.code,
                ErrorCode::NetworkError,
                "DownloadFailed must return its carried error verbatim, got: {err}"
            );
            assert!(
                err.message.contains("chunk 0 download blew up"),
                "the original download error message must be preserved: {err}"
            );
        }

        // ProcessingFailed → the carried message is re-wrapped as an
        // I/O error (ErrorCode::Internal).
        {
            let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
            chunks.insert(
                0,
                entry_with_state(
                    create_test_link(0),
                    ChunkState::ProcessingFailed("arrow decode failed".to_string()),
                ),
            );
            let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(MockLinkFetcher::new(1));
            let provider = StreamingCloudFetchProvider::for_test_with_chunks(
                fetcher,
                unused_downloader(),
                Arc::clone(&chunks),
                /* end_of_stream = */ true,
            );

            let err = match provider.try_next_batch() {
                Err(err) => err,
                Ok(_) => panic!("a ProcessingFailed head chunk must surface an error"),
            };
            assert_eq!(
                err.code,
                ErrorCode::Internal,
                "ProcessingFailed must be re-wrapped as an I/O error, got: {err}"
            );
            assert!(
                err.message.contains("arrow decode failed"),
                "the original processing error message must be preserved: {err}"
            );
        }
    }

    #[tokio::test]
    async fn refresh_batch_updates_link_for_urlfetched_chunks() {
        // Chunks 0-2 all expired, all in UrlFetched state. One
        // refresh covers them all. After: each has a non-expired
        // URL with the REFRESHED prefix.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        for i in 0..3 {
            chunks.insert(i, entry_with_state(expired_link(i), ChunkState::UrlFetched));
        }

        let mock = RefreshCountingMock::new(3);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .expect("refresh must succeed");

        assert_eq!(
            mock.refresh_calls.load(AtomicOrdering::SeqCst),
            1,
            "exactly one batched refresh RPC for the window"
        );
        for i in 0..3 {
            let entry = chunks.get(&i).unwrap();
            assert!(matches!(entry.state, ChunkState::UrlFetched));
            assert!(
                entry.link.as_ref().unwrap().url.contains("REFRESHED"),
                "chunk {} link was not refreshed",
                i
            );
            assert!(!entry.link.as_ref().unwrap().is_expired());
        }
    }

    #[tokio::test]
    async fn refresh_batch_preserves_downloaded_state() {
        // Chunk 0 is Downloaded with its bytes in memory. The
        // refresh updates its link (bookkeeping) but must not reset
        // state to UrlFetched — that would force a re-download of
        // bytes we already have.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        let already_downloaded = ChunkEntry {
            link: Some(expired_link(0)),
            state: ChunkState::Downloaded,
            // Real downloaded chunks carry batches; using None here
            // keeps the test focussed on state-machine reconciliation.
            batches: Some(vec![]),
            last_failure_reason: None,
        };
        chunks.insert(0, already_downloaded);
        chunks.insert(1, entry_with_state(expired_link(1), ChunkState::UrlFetched));

        let mock = RefreshCountingMock::new(2);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .unwrap();

        let c0 = chunks.get(&0).unwrap();
        assert!(
            matches!(c0.state, ChunkState::Downloaded),
            "Downloaded state must be preserved"
        );
        assert!(c0.batches.is_some(), "Downloaded bytes must not be cleared");
        assert!(
            c0.link.as_ref().unwrap().url.contains("REFRESHED"),
            "link was still updated for bookkeeping"
        );

        let c1 = chunks.get(&1).unwrap();
        assert!(matches!(c1.state, ChunkState::UrlFetched));
        assert!(c1.link.as_ref().unwrap().url.contains("REFRESHED"));
    }

    #[tokio::test]
    async fn refresh_batch_leaves_downloading_state_intact() {
        // A chunk in Downloading state means an in-flight task owns
        // the state machine. Refresh must NOT reset state to
        // UrlFetched (which would let `schedule_downloads` race in
        // and start a second download). Only the link is replaced;
        // the in-flight task will see the fresh URL on its next
        // retry-iteration of `download_chunk_with_retry`.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(
            5,
            entry_with_state(expired_link(5), ChunkState::Downloading),
        );

        let mock = RefreshCountingMock::new(1);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(
            5,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .unwrap();

        let c5 = chunks.get(&5).unwrap();
        assert!(
            matches!(c5.state, ChunkState::Downloading),
            "Downloading state must be preserved — the in-flight task owns it"
        );
        assert!(c5.link.as_ref().unwrap().url.contains("REFRESHED"));
    }

    #[tokio::test]
    async fn refresh_batch_does_not_reset_in_flight_states() {
        // F3 regression guard. A chunk in `DownloadFailed` or
        // `DownloadRetry` is owned by an in-flight download task
        // — that task is either about to retry (`DownloadRetry`)
        // or just failed and the spawn-site is updating state
        // (`DownloadFailed` is also a transient retry state set
        // by `download_chunk_with_retry` between attempts in
        // some refactors).
        //
        // Refresh MUST NOT reset these to `UrlFetched`. Doing so
        // would let `schedule_downloads` claim and spawn a second
        // download task for the same chunk while the first is
        // still working — double-counting `chunks_in_memory` and
        // racing both tasks to write to `entry.batches`. The
        // in-flight task picks up the fresh link on its next
        // retry iteration via `entry.link`; refresh only needs
        // to update the link field.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(
            7,
            entry_with_state(
                expired_link(7),
                ChunkState::DownloadFailed(Error::network_error().message("network glitch")),
            ),
        );
        chunks.insert(
            8,
            entry_with_state(expired_link(8), ChunkState::DownloadRetry),
        );

        // Mock window covers chunks 7..=9 so we refresh across
        // both our test entries.
        let mock = RefreshCountingMock::new(3);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(
            7,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .unwrap();

        // States must be UNCHANGED.
        let c7 = chunks.get(&7).unwrap();
        let c8 = chunks.get(&8).unwrap();
        assert!(
            matches!(c7.state, ChunkState::DownloadFailed(_)),
            "DownloadFailed state must be preserved — the in-flight task owns it"
        );
        assert!(
            matches!(c8.state, ChunkState::DownloadRetry),
            "DownloadRetry state must be preserved — the in-flight task owns it"
        );
        // But the links must be updated, so the in-flight task
        // picks them up on its next retry iteration.
        assert!(c7.link.as_ref().unwrap().url.contains("REFRESHED"));
        assert!(c8.link.as_ref().unwrap().url.contains("REFRESHED"));
    }

    // `#[tokio::test]` defaults to current-thread (tasks are
    // cooperatively scheduled), so concurrent calls would not race.
    // The multi-thread flavor genuinely runs the two
    // `refresh_links_batch` calls on different worker threads, so
    // the TokioMutex dedupe path is exercised under real contention.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_batch_dedups_concurrent_calls() {
        // Two concurrent calls — only one RPC should fire. The
        // second caller takes the mutex after the first releases,
        // sees the now-fresh link, and skips its own refetch.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));

        let mock = RefreshCountingMock::new(1);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();

        let fetcher_a: Arc<dyn ChunkLinkFetcher> = mock.clone();
        let chunks_a = Arc::clone(&chunks);
        let coordinator_a = Arc::clone(&coordinator);
        let cancel_a = cancel.clone();
        let h1 = tokio::spawn(async move {
            StreamingCloudFetchProvider::refresh_links_batch(
                0,
                &fetcher_a,
                &chunks_a,
                &coordinator_a,
                &cancel_a,
                None,
            )
            .await
        });

        let fetcher_b: Arc<dyn ChunkLinkFetcher> = mock.clone();
        let chunks_b = Arc::clone(&chunks);
        let coordinator_b = Arc::clone(&coordinator);
        let cancel_b = cancel.clone();
        let h2 = tokio::spawn(async move {
            StreamingCloudFetchProvider::refresh_links_batch(
                0,
                &fetcher_b,
                &chunks_b,
                &coordinator_b,
                &cancel_b,
                None,
            )
            .await
        });

        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();

        // The first holder did the work; the second holder
        // re-checked, found the link non-expired, and skipped.
        assert_eq!(
            mock.refresh_calls.load(AtomicOrdering::SeqCst),
            1,
            "concurrent refreshes must dedup down to one RPC"
        );
    }

    #[tokio::test]
    async fn refresh_skip_logs_release_chunk_guard() {
        use tracing_subscriber::prelude::*;

        struct CheckLogLock {
            chunks: Arc<DashMap<i64, ChunkEntry>>,
            logged: Arc<AtomicBool>,
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CheckLogLock {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                event.record(
                    &mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                        if field.name() != "message" {
                            return;
                        }
                        let message = format!("{value:?}");
                        if message.starts_with("Concurrent refresh delivered")
                            || message.contains("skipped after claim")
                        {
                            assert!(
                                self.chunks.try_get_mut(&0).is_present(),
                                "refresh log callback must be able to write the chunk: {message}"
                            );
                            self.logged.store(true, Ordering::Release);
                        }
                    },
                );
            }
        }

        for contended in [false, true] {
            let chunks = Arc::new(DashMap::new());
            let link = if contended {
                expired_link(0)
            } else {
                create_test_link(0)
            };
            chunks.insert(0, entry_with_state(link, ChunkState::UrlFetched));
            let mock = RefreshCountingMock::new(1);
            let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();
            let coordinator = Arc::new(RefreshCoordinator::default());
            coordinator.in_progress.store(contended, Ordering::Release);
            let cancel = CancellationToken::new();
            let logged = Arc::new(AtomicBool::new(false));
            let subscriber = tracing_subscriber::registry().with(CheckLogLock {
                chunks: Arc::clone(&chunks),
                logged: Arc::clone(&logged),
            });
            let _guard = tracing::subscriber::set_default(subscriber);

            let mut refresh = Box::pin(StreamingCloudFetchProvider::refresh_links_batch(
                0,
                &fetcher,
                &chunks,
                &coordinator,
                &cancel,
                None,
            ));
            if contended {
                assert!(futures::poll!(refresh.as_mut()).is_pending());
                chunks.insert(
                    0,
                    entry_with_state(create_test_link(0), ChunkState::UrlFetched),
                );
                coordinator.in_progress.store(false, Ordering::Release);
                coordinator.done.notify_waiters();
            }
            tokio::time::timeout(NO_HANG, refresh)
                .await
                .unwrap()
                .unwrap();
            assert!(logged.load(Ordering::Acquire), "contended={contended}");
            assert_eq!(mock.refresh_calls.load(AtomicOrdering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn refresh_batch_disjoint_window_loser_retries_claim() {
        // Loser-of-claim disjoint-window gap (round-3 follow-up):
        // when Task A wins the CAS and refreshes a window that
        // does NOT cover Task B's chunk, Task B must loop back to
        // retry the CAS itself rather than returning `Ok(())` and
        // letting the caller surface a misleading "still expired
        // — clock skew" error.
        //
        // Mock semantics: returns ONLY the chunk_index it was
        // asked about (window = 1). So a refresh starting at
        // chunk 0 covers chunk 0 only; a refresh starting at
        // chunk 50 covers chunk 50 only.
        //
        // Test setup: two concurrent refresh requests for chunks
        // 0 and 50. Both starting links are expired. After both
        // complete:
        // - Both chunks must have fresh URLs in the cache.
        // - Exactly TWO RPC calls were made (one per disjoint
        //   window).
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));
        chunks.insert(
            50,
            entry_with_state(expired_link(50), ChunkState::UrlFetched),
        );

        // window=1 → each refresh returns ONE link (only the
        // chunk_index it was asked about). So a refresh for
        // chunk 0 cannot cover chunk 50 and vice versa.
        let mock = RefreshCountingMock::new(1);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();

        let fetcher_a: Arc<dyn ChunkLinkFetcher> = mock.clone();
        let chunks_a = Arc::clone(&chunks);
        let coordinator_a = Arc::clone(&coordinator);
        let cancel_a = cancel.clone();
        let h1 = tokio::spawn(async move {
            StreamingCloudFetchProvider::refresh_links_batch(
                0,
                &fetcher_a,
                &chunks_a,
                &coordinator_a,
                &cancel_a,
                None,
            )
            .await
        });

        let fetcher_b: Arc<dyn ChunkLinkFetcher> = mock.clone();
        let chunks_b = Arc::clone(&chunks);
        let coordinator_b = Arc::clone(&coordinator);
        let cancel_b = cancel.clone();
        let h2 = tokio::spawn(async move {
            StreamingCloudFetchProvider::refresh_links_batch(
                50,
                &fetcher_b,
                &chunks_b,
                &coordinator_b,
                &cancel_b,
                None,
            )
            .await
        });

        // Both calls must succeed — no "still expired" error
        // should surface for the disjoint-window case.
        h1.await.unwrap().expect("chunk 0 refresh must succeed");
        h2.await.unwrap().expect("chunk 50 refresh must succeed");

        // Both chunks have been refreshed (URLs contain
        // "REFRESHED" from the mock).
        for ci in [0, 50] {
            let cached = chunks
                .get(&ci)
                .and_then(|e| e.link.as_ref().map(|l| l.url.clone()))
                .expect("chunk must be cached");
            assert!(
                cached.contains("REFRESHED"),
                "chunk {} URL must be refreshed after disjoint-window retry; got {}",
                ci,
                cached,
            );
        }

        // Exactly two RPCs — one per disjoint window. The loser's
        // retry-claim path is what makes the second RPC happen;
        // without it, the loser would return `Ok(())` after
        // waiting on `done` and the caller would surface a
        // bogus clock-skew error.
        assert_eq!(
            mock.refresh_calls.load(AtomicOrdering::SeqCst),
            2,
            "disjoint windows must each get their own RPC \
             (the loser retries CAS instead of returning Ok)",
        );
    }

    // ─── Coverage for the batched link-refresh edge cases ──────────

    /// Mock fetcher with configurable failure modes for the
    /// coverage-gap tests below. Defaults to "return all chunks
    /// asked for"; can be configured to return a narrower window
    /// or to fail with an error.
    #[derive(Debug)]
    struct ConfigurableMock {
        // Indices the mock will actually emit on refresh, regardless
        // of which `chunk_index` was requested. `None` = "echo the
        // requested chunk + window".
        emit_indices: StdMutex<Option<Vec<i64>>>,
        // When set, refetch_links_from returns this error.
        force_error: StdMutex<Option<&'static str>>,
        refresh_calls: AtomicUsize,
    }

    impl ConfigurableMock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                emit_indices: StdMutex::new(None),
                force_error: StdMutex::new(None),
                refresh_calls: AtomicUsize::new(0),
            })
        }

        fn emit_only(self: &Arc<Self>, indices: Vec<i64>) {
            *self.emit_indices.lock().unwrap() = Some(indices);
        }

        fn fail_with(self: &Arc<Self>, msg: &'static str) {
            *self.force_error.lock().unwrap() = Some(msg);
        }
    }

    use std::sync::Mutex as StdMutex;

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for ConfigurableMock {
        async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
            unreachable!()
        }
        async fn refetch_link(&self, _: i64, _: i64) -> Result<CloudFetchLink> {
            unreachable!()
        }
        async fn refetch_links_from(
            &self,
            chunk_index: i64,
            _: i64,
        ) -> Result<Vec<CloudFetchLink>> {
            self.refresh_calls.fetch_add(1, AtomicOrdering::SeqCst);
            if let Some(msg) = *self.force_error.lock().unwrap() {
                return Err(Error::network_error().message(msg));
            }
            let indices = self.emit_indices.lock().unwrap().clone();
            let to_emit: Vec<i64> = match indices {
                Some(v) => v,
                None => (chunk_index..chunk_index + 3).collect(),
            };
            Ok(to_emit
                .into_iter()
                .map(RefreshCountingMock::fresh_link)
                .collect())
        }
    }

    #[tokio::test]
    async fn refresh_batch_preserves_cancelled_and_released_states() {
        // Coverage for the `Cancelled` and `Released` state arms in
        // `refresh_links_batch`. Both must keep their state intact
        // (terminal); link metadata may be refreshed for
        // bookkeeping but no download is re-triggered.
        //
        // Start the refresh at chunk 0 so the refresh window
        // (chunks 0, 1, 2) covers our Cancelled/Released entries.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::Cancelled));
        chunks.insert(1, entry_with_state(expired_link(1), ChunkState::Released));
        chunks.insert(2, entry_with_state(expired_link(2), ChunkState::UrlFetched));

        let mock = RefreshCountingMock::new(3);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .unwrap();

        // Terminal states stay terminal.
        assert!(matches!(
            chunks.get(&0).unwrap().state,
            ChunkState::Cancelled
        ));
        assert!(matches!(
            chunks.get(&1).unwrap().state,
            ChunkState::Released
        ));
        assert!(matches!(
            chunks.get(&2).unwrap().state,
            ChunkState::UrlFetched
        ));
        // All three are in the refresh window, so all three get
        // their link metadata updated (including terminal states,
        // for bookkeeping).
        for ci in 0..3 {
            assert!(
                chunks
                    .get(&ci)
                    .unwrap()
                    .link
                    .as_ref()
                    .unwrap()
                    .url
                    .contains("REFRESHED"),
                "chunk {} link metadata must be refreshed for bookkeeping",
                ci,
            );
        }
    }

    #[tokio::test]
    async fn refresh_batch_inserts_new_chunk_for_window_expansion() {
        // Coverage for the "no entry yet" branch in
        // `refresh_links_batch` (the `else` arm that inserts a
        // brand-new ChunkEntry). Happens when the refresh response
        // covers a chunk_index we hadn't materialised yet.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));
        // Note: chunks 1 and 2 are NOT yet in the map.

        let mock = RefreshCountingMock::new(3); // emits chunks 0, 1, 2
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .unwrap();

        // Brand-new entries materialised for 1 and 2.
        assert!(chunks.get(&1).is_some(), "chunk 1 must be inserted");
        assert!(chunks.get(&2).is_some(), "chunk 2 must be inserted");
        assert!(matches!(
            chunks.get(&1).unwrap().state,
            ChunkState::UrlFetched
        ));
        assert!(matches!(
            chunks.get(&2).unwrap().state,
            ChunkState::UrlFetched
        ));
    }

    #[tokio::test]
    async fn refresh_batch_errors_when_server_truncates_window() {
        // Coverage for the "refresh response is missing the
        // requested chunk_index" defence. If the server returns
        // a batch that doesn't include the chunk we asked about,
        // we'd otherwise spin in a refresh-loop forever.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(5, entry_with_state(expired_link(5), ChunkState::UrlFetched));

        let mock = ConfigurableMock::new();
        // Refresh asks for chunk 5; mock returns chunks 0..2
        // instead (truncated window). Must surface as a hard error
        // rather than spin.
        mock.emit_only(vec![0, 1, 2]);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        let err = StreamingCloudFetchProvider::refresh_links_batch(
            5,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .expect_err("truncated window must surface as Err");
        assert!(
            err.message.contains("did not include chunk 5"),
            "error should pinpoint the missing chunk: {}",
            err.message,
        );
    }

    #[tokio::test]
    async fn refresh_batch_errors_on_interior_holes() {
        // F8: server returns chunks [0, 1, 3] starting from 0 —
        // chunk 2 is a hole. We refuse this rather than silently
        // accept and leave chunk 2 with a stale link (refresh-loop
        // forever on chunk 2 while siblings download cleanly).
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));

        let mock = ConfigurableMock::new();
        mock.emit_only(vec![0, 1, 3]);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        let err = StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .expect_err("interior hole must surface as Err");
        assert!(
            err.message.contains("interior holes"),
            "error should mention interior holes: {}",
            err.message,
        );
        assert!(
            err.message.contains("[2]"),
            "error should list the missing chunk index: {}",
            err.message,
        );
    }

    #[tokio::test]
    async fn refresh_batch_propagates_rpc_error() {
        // Coverage for the error path: `refetch_links_from` returns
        // Err, `refresh_links_batch` must propagate it (not swallow
        // and silently return Ok).
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));

        let mock = ConfigurableMock::new();
        mock.fail_with("network unreachable");
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        let err = StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .expect_err("RPC error must surface");
        assert!(
            err.message.contains("network unreachable"),
            "transport error must be propagated verbatim: {}",
            err.message,
        );
    }

    #[tokio::test]
    async fn refresh_batch_stale_after_refresh_leaves_caller_to_detect() {
        // F7 (duplicated stale-after-refresh defence) — the
        // check is now exclusively in `download_chunk_with_retry`
        // (the caller), not in `refresh_links_batch` itself.
        // This means `refresh_links_batch` accepts a stale server
        // response without erroring; the caller's post-refresh
        // `is_expired()` check is what surfaces the clock-skew
        // error.
        //
        // This test verifies the new contract: even when the
        // server returns a stale link, `refresh_links_batch`
        // returns Ok and writes the (still-stale) link to the
        // cache. The caller's responsibility from there.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));

        #[derive(Debug)]
        struct StaleEmittingMock;
        #[async_trait::async_trait]
        impl ChunkLinkFetcher for StaleEmittingMock {
            async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
                unreachable!()
            }
            async fn refetch_link(&self, _: i64, _: i64) -> Result<CloudFetchLink> {
                unreachable!()
            }
            async fn refetch_links_from(
                &self,
                chunk_index: i64,
                _: i64,
            ) -> Result<Vec<CloudFetchLink>> {
                Ok(vec![CloudFetchLink {
                    expiration: chrono::Utc::now() - chrono::Duration::seconds(60),
                    ..create_test_link(chunk_index)
                }])
            }
        }

        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(StaleEmittingMock);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();

        // refresh_links_batch returns Ok — it no longer
        // duplicates the staleness check.
        StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .expect("refresh_links_batch must accept the (stale) server response");

        // ...but the cached link is the stale one the server
        // emitted. The caller (download_chunk_with_retry) is
        // expected to detect this and surface a clear error.
        let cached = chunks.get(&0).unwrap();
        assert!(
            cached.link.as_ref().unwrap().is_expired(),
            "stale server link must land in the cache as-is",
        );
    }

    #[tokio::test]
    async fn refresh_batch_accepts_boundary_link_within_expiry_buffer() {
        // F16: a "fresh" link that lands within the
        // `LINK_EXPIRY_BUFFER_SECS` window is technically marked
        // `is_expired() == true` by our safety margin, but
        // `refresh_links_batch` itself doesn't enforce freshness.
        // Verify the contract: batch refresh accepts the link
        // as-is; the caller's post-refresh `is_expired()` check
        // is what eventually drives a re-refresh.
        //
        // The test uses `LINK_EXPIRY_BUFFER_SECS - 1` to land
        // squarely inside the buffer regardless of how the
        // constant is later tuned. A future maintainer who bumps
        // the buffer should expect this test to keep passing
        // without edits.
        use crate::types::cloudfetch::LINK_EXPIRY_BUFFER_SECS;
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));

        #[derive(Debug)]
        struct BoundaryMock {
            ttl_secs: i64,
        }
        #[async_trait::async_trait]
        impl ChunkLinkFetcher for BoundaryMock {
            async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
                unreachable!()
            }
            async fn refetch_link(&self, _: i64, _: i64) -> Result<CloudFetchLink> {
                unreachable!()
            }
            async fn refetch_links_from(
                &self,
                chunk_index: i64,
                _: i64,
            ) -> Result<Vec<CloudFetchLink>> {
                // Expires inside the buffer — externally looks
                // "valid for <ttl_secs> more seconds" but
                // `is_expired()` returns true.
                Ok(vec![CloudFetchLink {
                    expiration: chrono::Utc::now() + chrono::Duration::seconds(self.ttl_secs),
                    ..create_test_link(chunk_index)
                }])
            }
        }

        // TTL just inside the buffer: `is_expired()` flags it,
        // raw `expiration` is still in the future.
        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(BoundaryMock {
            ttl_secs: LINK_EXPIRY_BUFFER_SECS - 1,
        });
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();

        // refresh_links_batch returns Ok and accepts the
        // boundary link. The caller is responsible for
        // re-checking `is_expired()`.
        StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .expect("refresh must accept a boundary link");

        let cached = chunks.get(&0).unwrap();
        let link = cached.link.as_ref().unwrap();
        // Critical invariant: `is_expired()` reports true even
        // though raw `expiration` is still in the future. The
        // safety buffer is what catches the clock-skew edge.
        assert!(
            link.is_expired(),
            "boundary link (ttl={}s, buffer={}s) must report is_expired=true",
            LINK_EXPIRY_BUFFER_SECS - 1,
            LINK_EXPIRY_BUFFER_SECS,
        );
        assert!(
            link.expiration > chrono::Utc::now(),
            "raw expiration is still in the future — the buffer is what flags it",
        );

        // Mirror check: TTL just outside the buffer must report
        // `is_expired() == false`. This is what the dedupe path
        // would treat as "no refresh needed."
        let fresh_link = CloudFetchLink {
            expiration: chrono::Utc::now() + chrono::Duration::seconds(LINK_EXPIRY_BUFFER_SECS + 1),
            ..create_test_link(0)
        };
        assert!(
            !fresh_link.is_expired(),
            "link with ttl={}s (just outside buffer={}s) must report is_expired=false",
            LINK_EXPIRY_BUFFER_SECS + 1,
            LINK_EXPIRY_BUFFER_SECS,
        );
    }

    #[tokio::test]
    async fn refresh_batch_cancellation_unblocks_immediately() {
        // F1: refresh must be cancel-aware. Verify that
        // cancelling the token while the refresh RPC is in
        // flight surfaces a clean error without waiting for the
        // RPC's full retry budget.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));

        // Mock that hangs forever on refetch_links_from.
        #[derive(Debug)]
        struct HangingMock;
        #[async_trait::async_trait]
        impl ChunkLinkFetcher for HangingMock {
            async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
                unreachable!()
            }
            async fn refetch_link(&self, _: i64, _: i64) -> Result<CloudFetchLink> {
                unreachable!()
            }
            async fn refetch_links_from(&self, _: i64, _: i64) -> Result<Vec<CloudFetchLink>> {
                std::future::pending::<()>().await;
                unreachable!()
            }
        }

        let fetcher: Arc<dyn ChunkLinkFetcher> = Arc::new(HangingMock);
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();

        // Spawn the refresh and cancel after a short delay.
        let cancel_clone = cancel.clone();
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let result = StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await;

        cancel_task.await.unwrap();
        let err = result.expect_err("refresh must error on cancel, not hang on the RPC");
        assert!(
            err.message.to_lowercase().contains("cancel"),
            "error should mention cancellation: {}",
            err.message,
        );

        // Critical: the `in_progress` flag must be cleared even
        // on the cancel path. Otherwise subsequent refreshes
        // would queue indefinitely behind a flag that was never
        // released. The `ReleaseOnDrop` guard handles this.
        assert!(
            !coordinator.in_progress.load(Ordering::Acquire),
            "in_progress flag must be cleared on cancel via the RAII guard",
        );
    }

    #[tokio::test]
    async fn refresh_does_not_disturb_concurrent_downloading_task() {
        // F2: the static-state "Downloading" test
        // (`refresh_batch_leaves_downloading_state_intact`)
        // only proves the match arm doesn't reset state — it
        // doesn't prove the dynamic invariant: when a download
        // task is *actually running* and the chunks map says
        // `Downloading`, a concurrent refresh must not (a)
        // change the task's view of the state machine, (b)
        // resurrect the chunk into `UrlFetched` (which would
        // let `schedule_downloads` double-spawn), or (c)
        // disturb the link such that the task can't observe
        // the fresh URL on its next iteration.
        //
        // This test simulates a real in-flight download via
        // `tokio::spawn` gated on a `Notify`. The "task"
        // observes the link, awaits the gate (which we hold
        // open during the refresh), then re-reads the link
        // and reports what it saw.
        use tokio::sync::Notify;

        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        // The download task's starting link (a stale URL it's
        // already "downloading against").
        let initial_link = expired_link(0);
        let initial_url = initial_link.url.clone();
        chunks.insert(0, entry_with_state(initial_link, ChunkState::Downloading));

        // Gate that the fake download task waits on. We open it
        // after the refresh completes so the task is guaranteed
        // to observe the post-refresh cache.
        let proceed = Arc::new(Notify::new());

        // The fake download task: re-reads `entry.link` after
        // the gate opens (mirroring `download_chunk_with_retry`'s
        // retry-loop behaviour) and reports both the URL it saw
        // and the chunk's state at that moment.
        let chunks_clone = Arc::clone(&chunks);
        let proceed_clone = Arc::clone(&proceed);
        let task = tokio::spawn(async move {
            proceed_clone.notified().await;
            let entry = chunks_clone.get(&0).expect("chunk 0 must exist");
            let observed_url = entry.link.as_ref().map(|l| l.url.clone());
            let observed_state = entry.state.clone();
            (observed_url, observed_state)
        });

        // While the fake task is parked at the gate, fire a
        // refresh against the same chunks map. The refresh mock
        // emits a *new* URL for chunk 0.
        let mock = RefreshCountingMock::new(1); // emits chunks 0..0
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();
        let coordinator = Arc::new(RefreshCoordinator::default());
        let cancel = CancellationToken::new();

        StreamingCloudFetchProvider::refresh_links_batch(
            0,
            &fetcher,
            &chunks,
            &coordinator,
            &cancel,
            None,
        )
        .await
        .expect("refresh must succeed");

        // Invariant 1: state is *still* Downloading. The refresh
        // must not have reset it to UrlFetched (which would
        // double-spawn). This is the dynamic version of the
        // F3 invariant.
        let post_refresh_state = chunks.get(&0).unwrap().state.clone();
        assert!(
            matches!(post_refresh_state, ChunkState::Downloading),
            "concurrent refresh must not change Downloading state; got {:?}",
            post_refresh_state,
        );

        // Invariant 2: the URL was actually swapped in the
        // cache. Without this, the in-flight task has no way
        // to pick up the fresh link on its next iteration.
        let post_refresh_url = chunks
            .get(&0)
            .and_then(|e| e.link.as_ref().map(|l| l.url.clone()))
            .expect("link must still be present");
        assert_ne!(
            post_refresh_url, initial_url,
            "refresh must update the cached URL even while state remains Downloading",
        );
        assert!(
            post_refresh_url.contains("REFRESHED"),
            "cached URL must be the refresh-mock's new URL; got {}",
            post_refresh_url,
        );

        // Now release the fake task and verify it observes the
        // post-refresh state.
        proceed.notify_one();
        let (observed_url, observed_state) =
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .expect("task must complete")
                .expect("task must not panic");

        // Invariant 3: the in-flight task, on its next read of
        // `entry.link`, picks up the fresh URL transparently —
        // exactly the contract the PR claims.
        assert_eq!(
            observed_url.as_deref(),
            Some(post_refresh_url.as_str()),
            "in-flight task must see the refreshed URL on next read",
        );
        assert!(
            matches!(observed_state, ChunkState::Downloading),
            "in-flight task observes the chunk as still Downloading (owns the state machine)",
        );
    }

    // --- Terminal link-fetch failure propagation (issue #319 / audit H03) ---
    //
    // A CloudFetch link-fetch failure fails *before* any `ChunkEntry`
    // for the requested chunk exists and leaves `end_of_stream`
    // unset, so `wait_for_chunk` has nothing to observe. Before the
    // fix a woken consumer looped forever. These tests inject
    // link-fetch faults and prove the provider now surfaces a
    // deterministic terminal error to every current and future
    // waiter without hanging.

    /// A `ChunkDownloader` that never actually gets exercised in these
    /// tests (link fetch fails first), but `StreamingCloudFetchProvider::new`
    /// requires one. Points at an unroutable URL so any accidental
    /// download attempt fails fast rather than reaching the network.
    fn noop_downloader() -> Arc<ChunkDownloader> {
        use crate::client::retry::{build_retry_configs, RetryConfig};
        use crate::client::{DatabricksHttpClient, HttpClientConfig};
        use crate::types::sea::CompressionCodec;

        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig::default(),
                build_retry_configs(&retry, &HashMap::new()),
            )
            .unwrap(),
        );
        Arc::new(ChunkDownloader::new(
            http_client,
            CompressionCodec::None,
            0.1,
        ))
    }

    /// Configurable link fetcher that fails `fetch_links` for a chosen
    /// `start_chunk_index` and otherwise serves a fixed set of links.
    ///
    /// Failure is keyed on the requested `start_chunk_index`, not a
    /// call counter, so it is deterministic no matter how many times a
    /// page is (re)fetched:
    ///
    /// - `fail_at_start_index == 0`: the initial link fetch in
    ///   `initialize` (which requests `start_chunk_index == 0`) fails.
    /// - `fail_at_start_index == n > 0`: earlier pages serve
    ///   `links_per_call` links each; the fetch that would supply
    ///   chunk `n` fails — modelling an incremental / pagination-refill
    ///   failure.
    #[derive(Debug)]
    struct FailingLinkFetcher {
        total_chunks: i64,
        links_per_call: i64,
        fail_at_start_index: i64,
        error_message: String,
        /// When set, served links point at `{url_base}/chunk-{i}.arrow`
        /// so a real downloader can fetch them; otherwise the default
        /// unroutable `create_test_link` URL is used.
        url_base: Option<String>,
    }

    impl FailingLinkFetcher {
        fn new(total_chunks: i64, links_per_call: i64, fail_at_start_index: i64) -> Arc<Self> {
            Arc::new(Self {
                total_chunks,
                links_per_call,
                fail_at_start_index,
                error_message: "injected link-fetch failure".to_string(),
                url_base: None,
            })
        }

        fn with_url_base(
            total_chunks: i64,
            links_per_call: i64,
            fail_at_start_index: i64,
            url_base: String,
        ) -> Arc<Self> {
            Arc::new(Self {
                total_chunks,
                links_per_call,
                fail_at_start_index,
                error_message: "injected link-fetch failure".to_string(),
                url_base: Some(url_base),
            })
        }

        fn link_for(&self, chunk_index: i64) -> CloudFetchLink {
            match &self.url_base {
                Some(base) => CloudFetchLink {
                    url: format!("{base}/chunk-{chunk_index}.arrow"),
                    ..create_test_link(chunk_index)
                },
                None => create_test_link(chunk_index),
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for FailingLinkFetcher {
        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            if start_chunk_index == self.fail_at_start_index {
                // Preserve a distinctive SQLSTATE so the test can
                // assert the original error context survives. Keyed on
                // the requested index so retries of the same page fail
                // deterministically.
                return Err(Error::io()
                    .message(self.error_message.clone())
                    .with_sql_state("08001"));
            }

            if start_chunk_index >= self.total_chunks {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let links: Vec<CloudFetchLink> = (start_chunk_index
                ..(start_chunk_index + self.links_per_call).min(self.total_chunks))
                .map(|i| self.link_for(i))
                .collect();
            let next_index = start_chunk_index + links.len() as i64;
            let has_more = next_index < self.total_chunks;
            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: has_more.then_some(next_index),
                next_row_offset: None,
            })
        }

        async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
            Ok(create_test_link(chunk_index))
        }

        async fn refetch_links_from(
            &self,
            chunk_index: i64,
            _row_offset: i64,
        ) -> Result<Vec<CloudFetchLink>> {
            Ok(vec![create_test_link(chunk_index)])
        }
    }

    /// Build a provider wired to a failing fetcher, with a short
    /// chunk-ready timeout so a *regression* (a hang) still resolves
    /// within the outer `tokio::time::timeout` guard rather than
    /// wedging the whole test run.
    fn provider_with_failing_fetcher(
        fetcher: Arc<FailingLinkFetcher>,
        total_chunks_present: Option<i32>,
    ) -> Arc<StreamingCloudFetchProvider> {
        let config = CloudFetchConfig {
            chunk_ready_timeout: Some(std::time::Duration::from_millis(50)),
            ..CloudFetchConfig::default()
        };
        StreamingCloudFetchProvider::new(
            config,
            fetcher,
            noop_downloader(),
            total_chunks_present,
            tokio::runtime::Handle::current(),
        )
    }

    /// The outer guard: any `next_batch` that has not resolved within
    /// this window is treated as the indefinite-hang regression.
    const NO_HANG: std::time::Duration = std::time::Duration::from_secs(5);

    /// Initial link fetch fails → the first `next_batch` returns the
    /// terminal error instead of hanging, and the original error
    /// context (message + SQLSTATE) is preserved.
    #[tokio::test]
    async fn initial_link_fetch_failure_is_terminal() {
        let fetcher = FailingLinkFetcher::new(4, 2, /* fail_at_start_index */ 0);
        let provider = provider_with_failing_fetcher(fetcher, Some(4));

        let err = tokio::time::timeout(NO_HANG, provider.next_batch())
            .await
            .expect("next_batch must not hang after an initial link-fetch failure")
            .expect_err("a terminal link-fetch failure must surface as an error");

        assert!(
            err.message.contains("injected link-fetch failure"),
            "original error message must be preserved; got: {err}"
        );
        assert_eq!(
            err.sql_state.as_deref(),
            Some("08001"),
            "original SQLSTATE context must be preserved",
        );
    }

    /// Incremental link fetch fails after the first page succeeds and
    /// is consumed. This test isolates the incremental link-fetch path:
    /// chunk 0's link is served and its bytes download successfully
    /// (from a mock HTTP server), so the *only* failure source is the
    /// second `fetch_links` call. It asserts both that (a) chunk 0's
    /// rows are delivered in order before the failure — proving the
    /// terminal error does not discard already-downloaded chunks — and
    /// (b) the surfaced error carries the injected incremental
    /// link-fetch message + SQLSTATE, not some other failure.
    #[tokio::test]
    async fn incremental_link_fetch_failure_is_terminal() {
        use crate::client::retry::{build_retry_configs, RetryConfig};
        use crate::client::{DatabricksHttpClient, HttpClientConfig};
        use crate::types::sea::CompressionCodec;
        use arrow_array::Int32Array;
        use arrow_ipc::writer::StreamWriter;
        use arrow_schema::{DataType, Field, Schema};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // A valid single-column Arrow IPC stream for chunk 0.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut ipc_bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/chunk-0.arrow"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc_bytes))
            .mount(&server)
            .await;

        // Serve 1 link per call so chunk 0's link comes back on the
        // first fetch and its bytes download from the mock server;
        // the fetch supplying chunk 1 fails (deterministically, on
        // every retry of that page).
        let fetcher =
            FailingLinkFetcher::with_url_base(4, 1, /* fail_at_start_index */ 1, server.uri());

        let http_client = Arc::new(
            DatabricksHttpClient::new(
                HttpClientConfig::default(),
                build_retry_configs(
                    &RetryConfig {
                        max_retries: 0,
                        ..RetryConfig::default()
                    },
                    &HashMap::new(),
                ),
            )
            .unwrap(),
        );
        let downloader = Arc::new(ChunkDownloader::new(
            http_client,
            CompressionCodec::None,
            0.1,
        ));
        let config = CloudFetchConfig {
            chunk_ready_timeout: Some(std::time::Duration::from_millis(50)),
            ..CloudFetchConfig::default()
        };
        let provider = StreamingCloudFetchProvider::new(
            config,
            fetcher,
            downloader,
            Some(4),
            tokio::runtime::Handle::current(),
        );

        // First read must deliver chunk 0's rows — the terminal error
        // from the failed chunk-1 link fetch must not discard the
        // already-downloaded chunk 0.
        let first = tokio::time::timeout(NO_HANG, provider.next_batch())
            .await
            .expect("first read must not hang")
            .expect("chunk 0 downloaded successfully; its rows must be delivered")
            .expect("chunk 0 must yield a batch, not clean EOS");
        assert_eq!(
            first.num_rows(),
            3,
            "chunk 0's rows must be delivered in order"
        );

        // The next read reaches chunk 1, whose link fetch failed, and
        // must surface the injected incremental link-fetch error.
        let err = tokio::time::timeout(NO_HANG, provider.next_batch())
            .await
            .expect("next_batch must not hang after an incremental link-fetch failure")
            .expect_err("the incremental link-fetch failure must be terminal");
        assert!(
            err.message.contains("injected link-fetch failure"),
            "surfaced error must be the incremental link-fetch failure, not a download error; got: {err}"
        );
        assert_eq!(
            err.sql_state.as_deref(),
            Some("08001"),
            "incremental link-fetch error must preserve its SQLSTATE context",
        );
    }

    /// A waiter already parked in `next_batch` when the initial link
    /// fetch fails is woken and returns the terminal error — proving
    /// `set_terminal_error` wakes pre-registered waiters.
    #[tokio::test]
    async fn waiter_registered_before_failure_is_woken() {
        // A fetcher whose initial fetch blocks briefly, then fails —
        // guaranteeing the consumer is parked in `wait_for_chunk`
        // before the terminal state is set.
        #[derive(Debug)]
        struct SlowFailFetcher {
            gate: Arc<Notify>,
        }
        #[async_trait::async_trait]
        impl ChunkLinkFetcher for SlowFailFetcher {
            async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
                // Wait until the test signals the consumer is parked.
                self.gate.notified().await;
                Err(Error::io().message("delayed link-fetch failure"))
            }
            async fn refetch_link(&self, i: i64, _: i64) -> Result<CloudFetchLink> {
                Ok(create_test_link(i))
            }
            async fn refetch_links_from(&self, i: i64, _: i64) -> Result<Vec<CloudFetchLink>> {
                Ok(vec![create_test_link(i)])
            }
        }

        let gate = Arc::new(Notify::new());
        let config = CloudFetchConfig {
            chunk_ready_timeout: Some(std::time::Duration::from_secs(30)),
            ..CloudFetchConfig::default()
        };
        let provider = StreamingCloudFetchProvider::new(
            config,
            Arc::new(SlowFailFetcher {
                gate: Arc::clone(&gate),
            }),
            noop_downloader(),
            Some(1),
            tokio::runtime::Handle::current(),
        );

        // Start the consumer; it will park in wait_for_chunk because
        // the fetch is blocked on the gate.
        let consumer = {
            let p = Arc::clone(&provider);
            tokio::spawn(async move { p.next_batch().await })
        };

        // Give the consumer a moment to reach the wait loop, then
        // release the fetch so it fails.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        gate.notify_waiters();

        let result = tokio::time::timeout(NO_HANG, consumer)
            .await
            .expect("a pre-registered waiter must be woken by the terminal failure")
            .expect("consumer task must not panic");
        let err = result.expect_err("the woken waiter must return the terminal error");
        assert!(
            err.message.contains("delayed link-fetch failure"),
            "woken waiter must see the original error; got: {err}"
        );
    }

    /// A read started *after* the provider is already terminal returns
    /// the error immediately (no wait, no hang) — future consumers see
    /// the persisted terminal state.
    #[tokio::test]
    async fn read_started_after_failure_returns_terminal_error() {
        let fetcher = FailingLinkFetcher::new(4, 2, 0);
        let provider = provider_with_failing_fetcher(fetcher, Some(4));

        // First read drives the provider into the terminal state.
        let _ = tokio::time::timeout(NO_HANG, provider.next_batch())
            .await
            .expect("first next_batch must not hang")
            .expect_err("first read establishes the terminal error");

        // A subsequent read must also return the error, promptly.
        let err =
            tokio::time::timeout(std::time::Duration::from_millis(200), provider.next_batch())
                .await
                .expect("a read after the terminal state must return immediately, not wait")
                .expect_err("future reads must keep returning the terminal error");
        assert!(
            err.message.contains("injected link-fetch failure"),
            "future reads must see the same persisted error; got: {err}"
        );
    }

    /// Cancellation racing with a terminal link-fetch failure: whichever
    /// fires, no waiter stays blocked and `next_batch` returns an error
    /// rather than hanging.
    #[tokio::test]
    async fn cancel_racing_terminal_failure_leaves_no_waiter_blocked() {
        let fetcher = FailingLinkFetcher::new(4, 2, 0);
        let provider = provider_with_failing_fetcher(fetcher, Some(4));

        // Cancel concurrently with the failing initialize().
        let canceller = {
            let p = Arc::clone(&provider);
            tokio::spawn(async move {
                p.cancel();
            })
        };

        let err = tokio::time::timeout(NO_HANG, provider.next_batch())
            .await
            .expect("cancel racing a terminal failure must not leave the waiter blocked")
            .expect_err("next_batch must return an error (cancelled or terminal)");
        // Either outcome is acceptable; the invariant is "no hang".
        let msg = err.message.to_lowercase();
        assert!(
            msg.contains("cancel") || msg.contains("injected link-fetch failure"),
            "error must be the cancellation or the terminal link-fetch failure; got: {err}"
        );

        canceller.await.expect("canceller task must not panic");
    }

    /// The first terminal error wins: a later, different link-fetch
    /// failure does not overwrite the originally-persisted error.
    #[tokio::test]
    async fn first_terminal_error_wins() {
        use tracing_subscriber::prelude::*;

        struct CheckLogLock {
            provider: Arc<StreamingCloudFetchProvider>,
            logged: Arc<AtomicBool>,
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CheckLogLock {
            fn on_event(
                &self,
                _: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                assert!(self.provider.terminal_error.try_lock().is_ok());
                self.logged.store(true, Ordering::Release);
            }
        }

        let fetcher = FailingLinkFetcher::new(4, 2, 0);
        let provider = provider_with_failing_fetcher(fetcher, Some(4));
        let logged = Arc::new(AtomicBool::new(false));
        let subscriber = tracing_subscriber::registry().with(CheckLogLock {
            provider: Arc::clone(&provider),
            logged: Arc::clone(&logged),
        });
        let _guard = tracing::subscriber::set_default(subscriber);

        provider.set_terminal_error(Error::io().message("first error"));
        provider.set_terminal_error(Error::io().message("second error"));
        assert!(logged.load(Ordering::Acquire));

        let stored = provider
            .terminal_error()
            .expect("a terminal error must be recorded");
        assert!(
            stored.message.contains("first error"),
            "first writer must win; got: {stored}"
        );
    }
}
