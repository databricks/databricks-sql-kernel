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
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
    max_retries: u32,
    retry_delay: std::time::Duration,
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
            cancel_token: CancellationToken::new(),
            runtime_handle,
            statement_id,
            refresh_coordinator: Arc::new(RefreshCoordinator::default()),
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
                max_retries: self.config.max_retries,
                retry_delay: self.config.retry_delay,
            };
            let chunk_state_changed = Arc::clone(&self.chunk_state_changed);
            let chunks = self.chunks.clone();
            let batch_merge_target_rows = self.config.batch_merge_target_rows;

            self.runtime_handle.spawn(async move {
                let result = Self::download_chunk_with_retry(chunk_index, &ctx).await;

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
                            let msg = e.to_string();
                            entry.state = ChunkState::DownloadFailed(msg.clone());
                            entry.last_failure_reason = Some(msg);
                        }
                    }
                }

                chunk_state_changed.notify_one();
            });
        }
    }

    /// Download a single chunk with retry and link refresh on expiry.
    async fn download_chunk_with_retry(
        chunk_index: i64,
        ctx: &DownloadCtx,
    ) -> Result<Vec<RecordBatch>> {
        let mut attempts = 0;

        loop {
            if ctx.cancel_token.is_cancelled() {
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
                let entry = ctx.chunks.get(&chunk_index);
                let stored_link = entry.as_ref().and_then(|e| e.link.clone());

                match stored_link {
                    Some(link) if !link.is_expired() => link,
                    _ => {
                        debug!(
                            "Link for chunk {} missing or expired; \
                             triggering batched refresh",
                            chunk_index
                        );
                        Self::refresh_links_batch(
                            chunk_index,
                            &ctx.link_fetcher,
                            &ctx.chunks,
                            &ctx.refresh_coordinator,
                            &ctx.cancel_token,
                            ctx.statement_id.as_deref(),
                        )
                        .await
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

            // Attempt download
            match ctx.downloader.download(&link).await {
                Ok(batches) => return Ok(batches),
                Err(e) => {
                    // Stale-snapshot detection. Two ways a download
                    // can fail against an already-stale URL through
                    // no fault of the retry budget:
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
                             considers stale ({}); not counting against retry budget: {}",
                            chunk_index,
                            if was_refreshed_concurrently {
                                "refreshed concurrently"
                            } else {
                                "passively expired in-flight"
                            },
                            e,
                        );
                        // Don't increment `attempts`; don't sleep.
                        // Loop body re-reads `entry.link` and picks
                        // up the fresh URL on the next iteration.
                        continue;
                    }

                    attempts += 1;
                    if attempts >= ctx.max_retries {
                        return Err(e);
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

                    tokio::time::sleep(ctx.retry_delay).await;
                }
            }
        }
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
            last_failure_reason: None,
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
                ChunkState::DownloadFailed("network glitch".into()),
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
}
