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

use crate::kernel_error::{Error, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::ChunkLinkFetcher;
use crate::types::cloudfetch::{
    ChunkEntry, ChunkState, CloudFetchConfig, DEFAULT_CHUNK_READY_TIMEOUT_SECS,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

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

    // Cancellation
    cancel_token: CancellationToken,

    // Tokio runtime handle for spawning tasks
    runtime_handle: tokio::runtime::Handle,

    // Mutex around the batched link-refresh path.
    //
    // Several chunks expiring concurrently (the common shape: a
    // burst of presigned URLs all near the 1h boundary) would each
    // try to refresh the link cache. With this mutex, the first
    // caller takes the lock and issues exactly one
    // `refetch_links_from` RPC that covers the entire forward
    // window; subsequent callers await the same lock, then each
    // pulls the fresh link from the cache without a second
    // round-trip.
    //
    // `Arc<...>` so spawned download tasks can hold their own clone
    // without keeping the whole provider alive past stream end.
    refresh_lock: Arc<TokioMutex<()>>,
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
        runtime_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
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
            cancel_token: CancellationToken::new(),
            runtime_handle,
            refresh_lock: Arc::new(TokioMutex::new(())),
        });

        // Start background initialization that fetches links and schedules downloads
        let provider_clone = Arc::clone(&provider);
        provider.runtime_handle.spawn(async move {
            provider_clone.initialize().await;
        });

        provider
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

        match self.link_fetcher.fetch_links(0, 0).await {
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
                self.chunk_state_changed.notify_one();
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
        if let Some(batch) = self.current_batch_buffer.lock().unwrap().pop_front() {
            // Capture schema from first batch
            let _ = self.schema.get_or_init(|| batch.schema());
            return Ok(Some(batch));
        }

        // Buffer empty - need next chunk
        let chunk_index = self.current_chunk_index.load(Ordering::Acquire);

        // Check if we've reached end of stream
        if self.end_of_stream.load(Ordering::Acquire) && !self.chunks.contains_key(&chunk_index) {
            debug!("End of stream reached at chunk {}", chunk_index);
            return Ok(None);
        }

        // Wait for the chunk to be downloaded
        self.wait_for_chunk(chunk_index).await?;

        // Get the chunk and move batches to buffer
        if let Some((_, mut entry)) = self.chunks.remove(&chunk_index) {
            if let Some(batches) = entry.batches.take() {
                let mut buffer = self.current_batch_buffer.lock().unwrap();
                for batch in batches {
                    buffer.push_back(batch);
                }
            }

            // Mark chunk as released and decrement counter
            let new_count = self.chunks_in_memory.fetch_sub(1, Ordering::Release) - 1;

            // Move to next chunk
            let next_chunk = self.current_chunk_index.fetch_add(1, Ordering::Release) + 1;

            debug!(
                "Released chunk {}: chunks_in_memory={}/{}, consumer advancing to {}",
                chunk_index, new_count, self.max_chunks_in_memory, next_chunk
            );

            // Schedule more downloads
            self.schedule_downloads().await;

            // Return first batch from buffer
            if let Some(batch) = self.current_batch_buffer.lock().unwrap().pop_front() {
                let _ = self.schema.get_or_init(|| batch.schema());
                return Ok(Some(batch));
            }
        }

        // Check end of stream again
        if self.end_of_stream.load(Ordering::Acquire) {
            return Ok(None);
        }

        Err(Error::invalid_state().message(format!("Chunk {} not found after wait", chunk_index)))
    }

    /// Cancel all pending operations.
    pub fn cancel(&self) {
        debug!("Cancelling CloudFetch provider");
        self.cancel_token.cancel();
        self.chunk_state_changed.notify_one();
    }

    // --- Internal methods ---

    /// Fetch links from the link fetcher and store them in the chunks map.
    ///
    /// Returns whether the requested `start_index` is now available in the map.
    async fn fetch_and_store_links(&self, start_index: i64) -> bool {
        match self.link_fetcher.fetch_links(start_index, 0).await {
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
                self.chunk_state_changed.notify_one();
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
                debug!(
                    "Proactive refresh: cached link for chunk {} already expired",
                    next_index
                );
                if let Err(e) = Self::refresh_links_batch(
                    next_index,
                    &self.link_fetcher,
                    &self.chunks,
                    &self.refresh_lock,
                )
                .await
                {
                    warn!(
                        "Proactive batched refresh failed for chunk {}: {}; \
                         will fall through to per-download retry path",
                        next_index, e
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

            debug!(
                "Scheduling download for chunk {}: chunks_in_memory={}/{}, consumer_at={}",
                next_index,
                new_count,
                self.max_chunks_in_memory,
                self.current_chunk_index.load(Ordering::Acquire)
            );

            // Spawn download task
            let chunk_index = next_index;
            let downloader = Arc::clone(&self.chunk_downloader);
            let link_fetcher = Arc::clone(&self.link_fetcher);
            let chunks = self.chunks.clone();
            let refresh_lock = Arc::clone(&self.refresh_lock);
            let chunk_state_changed = Arc::clone(&self.chunk_state_changed);
            let cancel_token = self.cancel_token.clone();
            let max_retries = self.config.max_retries;
            let retry_delay = self.config.retry_delay;
            let batch_merge_target_rows = self.config.batch_merge_target_rows;

            self.runtime_handle.spawn(async move {
                let result = Self::download_chunk_with_retry(
                    chunk_index,
                    &downloader,
                    &link_fetcher,
                    &chunks,
                    &refresh_lock,
                    max_retries,
                    retry_delay,
                    &cancel_token,
                )
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
                            entry.state = ChunkState::DownloadFailed(e.to_string());
                        }
                    }
                }

                chunk_state_changed.notify_one();
            });
        }
    }

    /// Download a single chunk with retry and link refresh on expiry.
    #[allow(clippy::too_many_arguments)]
    async fn download_chunk_with_retry(
        chunk_index: i64,
        downloader: &ChunkDownloader,
        link_fetcher: &Arc<dyn ChunkLinkFetcher>,
        chunks: &Arc<DashMap<i64, ChunkEntry>>,
        refresh_lock: &Arc<TokioMutex<()>>,
        max_retries: u32,
        retry_delay: std::time::Duration,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        let mut attempts = 0;

        loop {
            if cancel_token.is_cancelled() {
                return Err(Error::invalid_state().message("Download cancelled"));
            }

            // Get link (may need to refresh the whole forward batch
            // if expired). We always go via the batched refresh path:
            // refreshing all in-flight chunks in one RPC matches the
            // shape of how SEA expires links (the whole presigned-URL
            // batch tends to share an expiry window), and avoids N
            // separate `refetch_link` calls when a burst of chunks
            // expires concurrently.
            let link = {
                let entry = chunks.get(&chunk_index);
                let stored_link = entry.as_ref().and_then(|e| e.link.clone());

                match stored_link {
                    Some(link) if !link.is_expired() => link,
                    _ => {
                        debug!(
                            "Link for chunk {} missing or expired; \
                             triggering batched refresh",
                            chunk_index
                        );
                        Self::refresh_links_batch(chunk_index, link_fetcher, chunks, refresh_lock)
                            .await?;
                        chunks
                            .get(&chunk_index)
                            .and_then(|e| e.link.clone())
                            .ok_or_else(|| {
                                Error::invalid_state().message(format!(
                                    "Link for chunk {} still missing after batched refresh",
                                    chunk_index
                                ))
                            })?
                    }
                }
            };

            // Attempt download
            match downloader.download(&link).await {
                Ok(batches) => return Ok(batches),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(e);
                    }
                    warn!(
                        "Chunk {} download failed (attempt {}/{}): {}, retrying...",
                        chunk_index, attempts, max_retries, e
                    );

                    // Update state to retry
                    if let Some(mut entry) = chunks.get_mut(&chunk_index) {
                        entry.state = ChunkState::DownloadRetry;
                    }

                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }

    /// Refresh presigned-URL links for `chunk_index` and every
    /// subsequent chunk whose download is still pending or in flight.
    ///
    /// One RPC; per-chunk state reconciliation:
    /// - `UrlFetched | DownloadFailed | DownloadRetry`: link replaced
    ///   in cache. `schedule_downloads` will pick the chunk up using
    ///   the fresh URL on its next loop.
    /// - `Downloading`: link replaced in cache. The active download
    ///   task will see the new link on its next retry iteration
    ///   (this function is called from inside that task — see
    ///   `download_chunk_with_retry`).
    /// - `Downloaded | ProcessingFailed | Cancelled | Released`:
    ///   bytes (or terminal state) are already in memory. We update
    ///   the cached link for bookkeeping but do nothing else; the
    ///   chunk doesn't need to be re-downloaded.
    /// - `Pending` (no link cached): not in our refresh window, skip.
    ///
    /// The `refresh_lock` is taken before the RPC fires. Multiple
    /// concurrent download tasks hitting expired URLs at the same
    /// time will queue on the lock; only the first holder issues the
    /// RPC, the rest find their now-fresh link already in cache when
    /// they re-check.
    ///
    /// `start_chunk_index` is the chunk whose download just failed
    /// (or is about to fail) — its server-side row offset is read
    /// from the cached link if any so the RPC carries the right
    /// `row_offset` for the bounded SEA API contract.
    async fn refresh_links_batch(
        start_chunk_index: i64,
        link_fetcher: &Arc<dyn ChunkLinkFetcher>,
        chunks: &Arc<DashMap<i64, ChunkEntry>>,
        refresh_lock: &Arc<TokioMutex<()>>,
    ) -> Result<()> {
        let _guard = refresh_lock.lock().await;

        // Re-check after taking the lock: another task may have just
        // refreshed our chunk. Skip the RPC if so.
        if let Some(entry) = chunks.get(&start_chunk_index) {
            if let Some(ref link) = entry.link {
                if !link.is_expired() {
                    debug!(
                        "Batched refresh for chunk {} skipped — already refreshed by another task",
                        start_chunk_index
                    );
                    return Ok(());
                }
            }
        }

        // Derive `row_offset` from the existing cached link if any.
        // The bounded SEA API contract requires `row_offset` on
        // CloudFetch fetches (see PR #67 + PR #68); honour it here so
        // the server-side cursor lands on the right row.
        let row_offset = chunks
            .get(&start_chunk_index)
            .and_then(|e| e.link.as_ref().map(|l| l.row_offset))
            .unwrap_or(0);

        debug!(
            "Issuing batched link refresh from chunk {} (row_offset={})",
            start_chunk_index, row_offset
        );
        let refreshed = link_fetcher
            .refetch_links_from(start_chunk_index, row_offset)
            .await?;

        for fresh in refreshed {
            let ci = fresh.chunk_index;
            if let Some(mut entry) = chunks.get_mut(&ci) {
                match entry.state {
                    // Pending links to download — replace and let
                    // schedule_downloads pick up on next loop.
                    ChunkState::UrlFetched
                    | ChunkState::DownloadFailed(_)
                    | ChunkState::DownloadRetry => {
                        entry.link = Some(fresh);
                        entry.state = ChunkState::UrlFetched;
                    }
                    // Download already in flight — the in-flight
                    // task re-reads `entry.link` on each retry, so
                    // updating the link in place is enough. Don't
                    // touch the state machine; the running task
                    // owns it.
                    ChunkState::Downloading => {
                        entry.link = Some(fresh);
                    }
                    // Terminal-ish or already-have-bytes states.
                    // Keep the link metadata current (cheap; future
                    // bookkeeping may read `entry.link.row_offset`
                    // etc.) but do not change state or trigger any
                    // download.
                    ChunkState::Downloaded
                    | ChunkState::ProcessingFailed(_)
                    | ChunkState::Cancelled
                    | ChunkState::Released => {
                        entry.link = Some(fresh);
                    }
                    // Pending without a link — refresh response
                    // covered a chunk we hadn't asked about yet.
                    // Drop a link in and mark as ready to schedule.
                    ChunkState::Pending => {
                        entry.link = Some(fresh);
                        entry.state = ChunkState::UrlFetched;
                    }
                }
            } else {
                // No entry yet — the refresh response covered a
                // chunk we haven't seen at all. Create a fresh entry
                // with the link; schedule_downloads will pick it up
                // when it's in the consumer window.
                chunks.insert(ci, ChunkEntry::with_link(fresh));
            }
        }

        Ok(())
    }

    /// Wait for a specific chunk to be ready (Downloaded state).
    async fn wait_for_chunk(&self, chunk_index: i64) -> Result<()> {
        loop {
            // Check cancellation
            if self.cancel_token.is_cancelled() {
                return Err(Error::invalid_state().message("Operation cancelled"));
            }

            // Check if chunk is ready
            if let Some(entry) = self.chunks.get(&chunk_index) {
                match &entry.state {
                    ChunkState::Downloaded => return Ok(()),
                    ChunkState::DownloadFailed(e) => return Err(Error::io().message(e.clone())),
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
                _ = self.chunk_state_changed.notified() => {
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
            }
        }
    }

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for MockLinkFetcher {
        async fn fetch_links(
            &self,
            start_chunk_index: i64,
            _start_row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
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
        let lock = Arc::new(TokioMutex::new(()));
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(0, &fetcher, &chunks, &lock)
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
        };
        chunks.insert(0, already_downloaded);
        chunks.insert(1, entry_with_state(expired_link(1), ChunkState::UrlFetched));

        let mock = RefreshCountingMock::new(2);
        let lock = Arc::new(TokioMutex::new(()));
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(0, &fetcher, &chunks, &lock)
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
        let lock = Arc::new(TokioMutex::new(()));
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(5, &fetcher, &chunks, &lock)
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
    async fn refresh_batch_resets_download_failed_to_urlfetched() {
        // A chunk in DownloadFailed (transient) should be reset to
        // UrlFetched with the fresh URL so schedule_downloads can
        // pick it up on next loop.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(
            7,
            entry_with_state(
                expired_link(7),
                ChunkState::DownloadFailed("network glitch".into()),
            ),
        );

        let mock = RefreshCountingMock::new(1);
        let lock = Arc::new(TokioMutex::new(()));
        let fetcher: Arc<dyn ChunkLinkFetcher> = mock.clone();

        StreamingCloudFetchProvider::refresh_links_batch(7, &fetcher, &chunks, &lock)
            .await
            .unwrap();

        let c7 = chunks.get(&7).unwrap();
        assert!(matches!(c7.state, ChunkState::UrlFetched));
        assert!(c7.link.as_ref().unwrap().url.contains("REFRESHED"));
    }

    #[tokio::test]
    async fn refresh_batch_dedups_concurrent_calls() {
        // Two concurrent calls — only one RPC should fire. The
        // second caller takes the mutex after the first releases,
        // sees the now-fresh link, and skips its own refetch.
        let chunks: Arc<DashMap<i64, ChunkEntry>> = Arc::new(DashMap::new());
        chunks.insert(0, entry_with_state(expired_link(0), ChunkState::UrlFetched));

        let mock = RefreshCountingMock::new(1);
        let lock = Arc::new(TokioMutex::new(()));

        let fetcher_a: Arc<dyn ChunkLinkFetcher> = mock.clone();
        let chunks_a = Arc::clone(&chunks);
        let lock_a = Arc::clone(&lock);
        let h1 = tokio::spawn(async move {
            StreamingCloudFetchProvider::refresh_links_batch(0, &fetcher_a, &chunks_a, &lock_a)
                .await
        });

        let fetcher_b: Arc<dyn ChunkLinkFetcher> = mock.clone();
        let chunks_b = Arc::clone(&chunks);
        let lock_b = Arc::clone(&lock);
        let h2 = tokio::spawn(async move {
            StreamingCloudFetchProvider::refresh_links_batch(0, &fetcher_b, &chunks_b, &lock_b)
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
}
