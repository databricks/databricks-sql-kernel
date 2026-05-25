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

//! ChunkLinkFetcher trait and implementations.
//!
//! This module provides a statement-scoped abstraction for fetching chunk links.
//! The `ChunkLinkFetcher` trait wraps a `Transport` and binds it to a
//! specific statement, providing a simpler interface for `StreamingCloudFetchProvider`.
//!
//! ## Prefetch Architecture
//!
//! The `SeaChunkLinkFetcher` implements proactive link prefetching similar to the
//! JDBC driver. It maintains a prefetch window ahead of the consumer position:
//!
//! ```text
//! Consumer Position     Prefetch Window (128 chunks)
//!       |                      |
//!       v                      v
//! [0][1][2]...[current]...[current + 128]
//!       ^                      ^
//!       |                      |
//!  Links cached          Prefetch triggers when
//!  and available         we approach this boundary
//! ```
//!
//! When a link is requested:
//! 1. Return immediately from cache if available
//! 2. Update consumer position tracking
//! 3. Check if prefetch should be triggered (async, non-blocking)
//! 4. Background task fetches links and caches them

use crate::client::{ChunkLinkFetchResult, Transport};
use crate::kernel_error::{Error, Result};
use crate::types::cloudfetch::CloudFetchLink;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, error};

/// Statement-scoped abstraction for fetching chunk links.
///
/// This trait wraps a `Transport` and binds it to a specific statement,
/// providing a simpler interface for `StreamingCloudFetchProvider`. Each
/// implementation delegates to `Transport::get_result_chunks()` but
/// handles protocol-specific details (e.g., which continuation parameter to use).
///
/// # Design Note
///
/// Different backends use different continuation mechanisms:
/// - SEA uses `chunk_index`
/// - Thrift uses `row_offset`
///
/// Both parameters are passed to methods; implementations use the relevant one.
#[async_trait]
pub(crate) trait ChunkLinkFetcher: Send + Sync + std::fmt::Debug {
    /// Fetch the next batch of chunk links.
    ///
    /// # Parameters
    /// * `start_chunk_index` - Used by chunk-index based backends (SEA)
    /// * `start_row_offset` - Used by row-offset based backends (Thrift)
    ///
    /// Each implementation uses the parameter relevant to its protocol.
    async fn fetch_links(
        &self,
        start_chunk_index: i64,
        start_row_offset: i64,
    ) -> Result<ChunkLinkFetchResult>;

    /// Refetch a specific chunk link (used when link expires before download).
    async fn refetch_link(&self, chunk_index: i64, row_offset: i64) -> Result<CloudFetchLink>;
}

/// SEA implementation of ChunkLinkFetcher with caching and proactive prefetching.
///
/// This fetcher:
/// - Makes API calls to the SEA get_result_chunks endpoint
/// - Caches fetched links in a concurrent DashMap
/// - Tracks the highest chunk index fetched to avoid redundant API calls
/// - Uses each link's server-emitted `next_chunk_index` to advance and
///   to detect end-of-stream — per the bounded SEA API contract for
///   OSS drivers, `total_chunk_count` from the manifest is deprecated
///   and no longer consulted by the kernel.
/// - **Proactively prefetches links** when consumer position approaches the
///   prefetch boundary (similar to JDBC's `linkPrefetchWindow`)
///
/// ## Prefetch Behavior
///
/// When `fetch_links()` is called:
/// 1. Consumer position is updated
/// 2. If `next_server_fetch_index <= consumer_position + link_prefetch_window`,
///    a background prefetch task is spawned
/// 3. The prefetch task fetches links until caught up with the window
/// 4. This ensures links are ready before they're needed
///
/// Uses `chunk_index` for pagination (ignores `row_offset`).
pub(crate) struct SeaChunkLinkFetcher {
    /// Databricks client for making API calls
    client: Arc<dyn Transport>,
    /// Statement ID this fetcher is bound to
    statement_id: String,
    /// Cached links by chunk index
    links: DashMap<i64, CloudFetchLink>,
    /// Next chunk index to ask the server for. Initialised from the
    /// last initial link's `next_chunk_index` (the server tells us
    /// where the stream continues). Updated after each batch from the
    /// same field on the most-recently-fetched link.
    next_server_fetch_index: AtomicI64,
    /// End-of-stream sentinel. Flips to `true` the moment the server
    /// returns a fetch whose last link has `next_chunk_index = None`.
    /// Per the bounded SEA API contract for OSS drivers, this is the
    /// authoritative end-of-stream signal — `total_chunk_count` is
    /// deprecated for OSS-driver use and not consulted here.
    all_links_fetched: AtomicBool,
    /// Current consumer position (highest chunk index requested)
    current_consumer_index: AtomicI64,
    /// Number of chunks to prefetch ahead of consumer position
    link_prefetch_window: i64,
    /// Whether a prefetch task is currently running
    prefetch_in_progress: AtomicBool,
    /// Tokio runtime handle for spawning prefetch tasks
    runtime_handle: tokio::runtime::Handle,
    /// Lock for coordinating prefetch operations
    prefetch_lock: TokioMutex<()>,
    /// Next row offset for pagination (used by some backends)
    next_row_offset: AtomicI64,
}

impl std::fmt::Debug for SeaChunkLinkFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeaChunkLinkFetcher")
            .field("statement_id", &self.statement_id)
            .field("links_count", &self.links.len())
            .field(
                "next_server_fetch_index",
                &self.next_server_fetch_index.load(Ordering::Relaxed),
            )
            .field(
                "all_links_fetched",
                &self.all_links_fetched.load(Ordering::Relaxed),
            )
            .field(
                "current_consumer_index",
                &self.current_consumer_index.load(Ordering::Relaxed),
            )
            .field("link_prefetch_window", &self.link_prefetch_window)
            .field(
                "prefetch_in_progress",
                &self.prefetch_in_progress.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl SeaChunkLinkFetcher {
    /// Create a new SEA link fetcher with caching and prefetching.
    ///
    /// End-of-stream and "where does the next fetch start" are both
    /// derived from the last initial link's server-provided
    /// `next_chunk_index` and `row_offset + row_count`. Per the
    /// bounded SEA API contract for OSS drivers, `total_chunk_count`
    /// from the manifest is deprecated for driver use; the server-
    /// emitted `next_chunk_index` on each link is the authoritative
    /// continuation signal.
    ///
    /// # Arguments
    /// * `client` - The Databricks client to use for API calls
    /// * `statement_id` - The statement ID to fetch links for
    /// * `initial_links` - Links from the initial execute response.
    ///   Must be ordered by `chunk_index`; the last link's
    ///   `next_chunk_index` decides whether more chunks remain.
    /// * `link_prefetch_window` - Number of chunks to prefetch ahead of consumer
    /// * `runtime_handle` - Tokio runtime handle for spawning prefetch tasks
    pub(crate) fn new(
        client: Arc<dyn Transport>,
        statement_id: impl Into<String>,
        initial_links: &[CloudFetchLink],
        link_prefetch_window: i64,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let statement_id = statement_id.into();
        let links = DashMap::new();

        // Store initial links.
        for link in initial_links {
            links.insert(link.chunk_index, link.clone());
        }

        // The last link's `next_chunk_index` is the server's
        // continuation token. Absent → end-of-stream; present →
        // that's where the next fetch starts.
        //
        // When the caller passes no initial links at all we start
        // at chunk 0 with `all_links_fetched=false` and let the
        // first server fetch (sync fallback or async prefetch)
        // discover the rest. An empty `initial_links` is the
        // "kernel constructed before the first server response"
        // shape used in tests; it does *not* mean end-of-stream.
        let last_link = initial_links.last();
        let (next_server_fetch_index, all_links_fetched, next_row_offset) = match last_link {
            Some(link) => match link.next_chunk_index {
                Some(idx) => (idx, false, link.row_offset + link.row_count),
                None => (link.chunk_index + 1, true, link.row_offset + link.row_count),
            },
            None => (0, false, 0),
        };

        debug!(
            "SeaChunkLinkFetcher: {} initial links, next_server_fetch={}, \
             next_row_offset={}, all_fetched={}, prefetch_window={}",
            initial_links.len(),
            next_server_fetch_index,
            next_row_offset,
            all_links_fetched,
            link_prefetch_window
        );

        Self {
            client,
            statement_id,
            links,
            next_server_fetch_index: AtomicI64::new(next_server_fetch_index),
            all_links_fetched: AtomicBool::new(all_links_fetched),
            current_consumer_index: AtomicI64::new(0),
            link_prefetch_window,
            prefetch_in_progress: AtomicBool::new(false),
            runtime_handle,
            prefetch_lock: TokioMutex::new(()),
            next_row_offset: AtomicI64::new(next_row_offset),
        }
    }

    /// Check if a link is cached.
    #[allow(dead_code, reason = "wired into the v0 path in PR 2")]
    pub(crate) fn has_link(&self, chunk_index: i64) -> bool {
        self.links.contains_key(&chunk_index)
    }

    /// Get a cached link if available.
    #[allow(dead_code, reason = "wired into the v0 path in PR 2")]
    pub(crate) fn get_link(&self, chunk_index: i64) -> Option<CloudFetchLink> {
        self.links.get(&chunk_index).map(|r| r.clone())
    }

    /// Check if all links have been fetched.
    #[allow(dead_code, reason = "wired into the v0 path in PR 2")]
    pub(crate) fn all_links_fetched(&self) -> bool {
        self.all_links_fetched.load(Ordering::Acquire)
    }

    /// Check if prefetch should be triggered and spawn background task if needed.
    ///
    /// Prefetch is triggered when:
    /// - Not all links have been fetched yet
    /// - No prefetch is currently in progress
    /// - `next_server_fetch_index <= current_consumer_index + link_prefetch_window`
    fn maybe_trigger_prefetch(self: &Arc<Self>) {
        // Quick checks without locks
        if self.all_links_fetched.load(Ordering::Acquire) {
            return;
        }

        // Try to claim prefetch (compare_exchange returns Ok if we won)
        if self
            .prefetch_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Another prefetch is already in progress
            return;
        }

        let consumer_pos = self.current_consumer_index.load(Ordering::Acquire);
        let server_fetch_idx = self.next_server_fetch_index.load(Ordering::Acquire);
        let target_index = consumer_pos + self.link_prefetch_window;

        // Check if we need to prefetch
        if server_fetch_idx > target_index {
            // We're already ahead of the window, release the flag
            self.prefetch_in_progress.store(false, Ordering::Release);
            return;
        }

        debug!(
            "Triggering async prefetch: consumer={}, server_fetch={}, target={}, window={}",
            consumer_pos, server_fetch_idx, target_index, self.link_prefetch_window
        );

        // Spawn background prefetch task
        let fetcher = Arc::clone(self);
        self.runtime_handle.spawn(async move {
            fetcher.run_prefetch_loop().await;
        });
    }

    /// Background prefetch loop - fetches links until caught up with window.
    async fn run_prefetch_loop(self: Arc<Self>) {
        // Acquire the prefetch lock to prevent concurrent prefetch operations
        let _guard = self.prefetch_lock.lock().await;

        loop {
            // Check if we're done
            if self.all_links_fetched.load(Ordering::Acquire) {
                debug!("Prefetch loop: all links fetched, stopping");
                break;
            }

            let consumer_pos = self.current_consumer_index.load(Ordering::Acquire);
            let server_fetch_idx = self.next_server_fetch_index.load(Ordering::Acquire);
            let target_index = consumer_pos + self.link_prefetch_window;

            // Check if we're caught up
            if server_fetch_idx > target_index {
                debug!(
                    "Prefetch loop: caught up (server_fetch={} > target={}), stopping",
                    server_fetch_idx, target_index
                );
                break;
            }

            // Fetch next batch
            let row_offset = self.next_row_offset.load(Ordering::Acquire);
            debug!(
                "Prefetch loop: fetching from index {}, row_offset={}",
                server_fetch_idx, row_offset
            );

            match self
                .client
                .get_result_chunks(&self.statement_id, server_fetch_idx, row_offset)
                .await
            {
                Ok(result) => {
                    // Cache the new links.
                    for link in &result.links {
                        self.links.insert(link.chunk_index, link.clone());
                    }

                    // The server-emitted `next_chunk_index` on the
                    // last link in the batch is the continuation
                    // signal. Absent (`None`) → no more chunks;
                    // present → that's the next chunk_index to ask
                    // for. We never synthesise `chunk_index + 1` —
                    // the bounded SEA API contract makes the server
                    // the source of truth for chunk boundaries.
                    //
                    // Note on parallel downloads: setting
                    // `all_links_fetched = true` here just stops the
                    // *prefetch loop*. In-flight chunk downloads
                    // continue independently (see streaming_provider
                    // and its `end_of_stream` flag, which it
                    // explicitly checks alongside whether the
                    // current chunk is still in its in-memory cache
                    // before returning end-of-stream to the caller).
                    if let Some(last_link) = result.links.last() {
                        let next_row_offset = last_link.row_offset + last_link.row_count;
                        self.next_row_offset
                            .store(next_row_offset, Ordering::Release);
                        match last_link.next_chunk_index {
                            Some(idx) => {
                                self.next_server_fetch_index
                                    .fetch_max(idx, Ordering::Release);
                                debug!(
                                    "Prefetch loop: fetched {} links (last chunk {}), \
                                     next_server_fetch={}, next_row_offset={}",
                                    result.links.len(),
                                    last_link.chunk_index,
                                    idx,
                                    next_row_offset,
                                );
                            }
                            None => {
                                debug!(
                                    "Prefetch loop: end-of-stream marker on link {} \
                                     (next_chunk_index absent); link discovery done",
                                    last_link.chunk_index,
                                );
                                self.all_links_fetched.store(true, Ordering::Release);
                                break;
                            }
                        }
                    } else if !result.has_more {
                        // Defensive: server returned no links and
                        // signalled no more. Treat as end-of-stream
                        // for link discovery.
                        debug!("Prefetch loop: empty response with has_more=false");
                        self.all_links_fetched.store(true, Ordering::Release);
                        break;
                    } else {
                        // Empty batch but `has_more` — server hiccup
                        // or the prefetcher got rate-limited. Bail
                        // and let the consumer-driven path retry.
                        debug!("Prefetch loop: empty batch with has_more=true; bailing");
                        break;
                    }
                }
                Err(e) => {
                    error!("Prefetch loop: failed to fetch links: {}", e);
                    // Don't propagate error - let the main fetch_links handle it
                    break;
                }
            }
        }

        // Release prefetch flag
        self.prefetch_in_progress.store(false, Ordering::Release);
    }
}

/// Wrapper to allow calling `maybe_trigger_prefetch` from the trait implementation.
///
/// Since the trait takes `&self`, we need a way to get an `Arc<Self>` to spawn
/// the background prefetch task. This wrapper holds the Arc internally.
pub(crate) struct SeaChunkLinkFetcherHandle {
    inner: Arc<SeaChunkLinkFetcher>,
}

impl std::fmt::Debug for SeaChunkLinkFetcherHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl SeaChunkLinkFetcherHandle {
    /// Create a new handle wrapping a SeaChunkLinkFetcher.
    ///
    /// Immediately triggers async prefetch to start fetching links proactively,
    /// giving the prefetch task a head start before the consumer starts reading.
    pub(crate) fn new(fetcher: SeaChunkLinkFetcher) -> Self {
        let handle = Self {
            inner: Arc::new(fetcher),
        };

        // Trigger prefetch immediately on creation - don't wait for first next_batch()
        handle.inner.maybe_trigger_prefetch();

        handle
    }
}

#[async_trait]
impl ChunkLinkFetcher for SeaChunkLinkFetcherHandle {
    async fn fetch_links(
        &self,
        start_chunk_index: i64,
        start_row_offset: i64,
    ) -> Result<ChunkLinkFetchResult> {
        let fetcher = &self.inner;

        // Update consumer position (highest chunk index requested)
        fetcher
            .current_consumer_index
            .fetch_max(start_chunk_index, Ordering::Release);

        // Trigger async prefetch if needed (non-blocking)
        self.inner.maybe_trigger_prefetch();

        // If we already have all links, return what we have from cache.
        // Walk contiguously from `start_chunk_index` until we hit a
        // gap — that handles both the known case (server returned
        // contiguous chunk_index = 0,1,2,...) and any future hole
        // without depending on `total_chunk_count`.
        if fetcher.all_links_fetched.load(Ordering::Acquire) {
            let mut links = Vec::new();
            let mut i = start_chunk_index;
            while let Some(link) = fetcher.links.get(&i) {
                links.push(link.clone());
                i += 1;
            }
            return Ok(ChunkLinkFetchResult {
                links,
                has_more: false,
                next_chunk_index: None,
                next_row_offset: None,
            });
        }

        // Check if we need to fetch from server
        let server_fetch_index = fetcher.next_server_fetch_index.load(Ordering::Acquire);

        // If request is for links we should already have, return from cache
        if start_chunk_index < server_fetch_index {
            let mut links = Vec::new();
            for i in start_chunk_index..server_fetch_index {
                if let Some(link) = fetcher.links.get(&i) {
                    links.push(link.clone());
                }
            }
            let has_more = !fetcher.all_links_fetched.load(Ordering::Acquire);
            return Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: if has_more {
                    Some(server_fetch_index)
                } else {
                    None
                },
                next_row_offset: None,
            });
        }

        // Fetch from server (synchronous fallback if prefetch hasn't caught up)
        debug!(
            "SeaChunkLinkFetcher: cache miss for chunk {}, fetching synchronously",
            start_chunk_index
        );
        let result = fetcher
            .client
            .get_result_chunks(&fetcher.statement_id, start_chunk_index, start_row_offset)
            .await?;

        // Cache the new links.
        for link in &result.links {
            fetcher.links.insert(link.chunk_index, link.clone());
        }

        // Advance `next_server_fetch_index` / `next_row_offset` from
        // the *last link's* server-emitted metadata. End-of-stream
        // (for link discovery) is the absence of `next_chunk_index`
        // on the last link in the batch — never derived from
        // `total_chunk_count` or `chunk_index + 1` arithmetic.
        let next_chunk_index = match result.links.last() {
            Some(last_link) => {
                let next_row_offset = last_link.row_offset + last_link.row_count;
                fetcher
                    .next_row_offset
                    .store(next_row_offset, Ordering::Release);
                match last_link.next_chunk_index {
                    Some(idx) => {
                        fetcher
                            .next_server_fetch_index
                            .fetch_max(idx, Ordering::Release);
                        Some(idx)
                    }
                    None => {
                        fetcher.all_links_fetched.store(true, Ordering::Release);
                        None
                    }
                }
            }
            None => {
                // Empty response. Trust the server's `has_more`.
                if !result.has_more {
                    fetcher.all_links_fetched.store(true, Ordering::Release);
                }
                None
            }
        };

        let has_more = !fetcher.all_links_fetched.load(Ordering::Acquire);
        Ok(ChunkLinkFetchResult {
            links: result.links,
            has_more,
            next_chunk_index: if has_more { next_chunk_index } else { None },
            next_row_offset: Some(fetcher.next_row_offset.load(Ordering::Acquire)),
        })
    }

    async fn refetch_link(&self, chunk_index: i64, _row_offset: i64) -> Result<CloudFetchLink> {
        let result = self
            .inner
            .client
            .get_result_chunks(&self.inner.statement_id, chunk_index, 0)
            .await?;

        let link = result
            .links
            .into_iter()
            .find(|l| l.chunk_index == chunk_index)
            .ok_or_else(|| {
                Error::invalid_state()
                    .message(format!("Link not found for chunk_index {}", chunk_index))
            })?;

        // Update cache with fresh link
        self.inner.links.insert(chunk_index, link.clone());
        Ok(link)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ExecuteResult, SessionInfo};
    use crate::reader::EmptyReader;
    use crate::types::sea::{ExecuteParams, StatementState};
    use arrow_schema::Schema;
    use std::collections::HashMap;
    use std::sync::Arc as StdArc;

    /// Mock Transport for testing
    #[derive(Debug)]
    struct MockDatabricksClient {
        chunks: Vec<Vec<CloudFetchLink>>,
    }

    impl MockDatabricksClient {
        fn new(chunks: Vec<Vec<CloudFetchLink>>) -> Self {
            Self { chunks }
        }
    }

    #[async_trait]
    impl Transport for MockDatabricksClient {
        async fn create_session(
            &self,
            _catalog: Option<&str>,
            _schema: Option<&str>,
            _session_config: HashMap<String, String>,
        ) -> Result<SessionInfo> {
            Ok(SessionInfo {
                session_id: "mock-session".to_string(),
            })
        }

        async fn delete_session(&self, _session_id: &str) -> Result<()> {
            Ok(())
        }

        async fn execute_statement(
            &self,
            _session_id: &str,
            _sql: &str,
            _params: &ExecuteParams,
        ) -> Result<ExecuteResult> {
            Ok(ExecuteResult {
                statement_id: "mock-statement".to_string(),
                reader: Box::new(EmptyReader::new(StdArc::new(Schema::empty()))),
                manifest: None,
                server_state: StatementState::Succeeded,
            })
        }

        async fn submit_statement(
            &self,
            _session_id: &str,
            _sql: &str,
            _params: &ExecuteParams,
        ) -> Result<crate::client::ExecuteResponse> {
            unimplemented!("submit_statement not used by the link-fetcher mock")
        }

        async fn fetch_completed_result(
            &self,
            _statement_id: &str,
        ) -> Result<Option<ExecuteResult>> {
            unimplemented!("fetch_completed_result not used by the link-fetcher mock")
        }

        async fn statement_status(
            &self,
            _statement_id: &str,
        ) -> Result<crate::client::ExecuteResponse> {
            unimplemented!("statement_status not used by the link-fetcher mock")
        }

        async fn get_result_chunks(
            &self,
            _statement_id: &str,
            chunk_index: i64,
            _row_offset: i64,
        ) -> Result<ChunkLinkFetchResult> {
            let index = chunk_index as usize;
            if index >= self.chunks.len() {
                return Ok(ChunkLinkFetchResult::end_of_stream());
            }

            let links = self.chunks[index].clone();
            let has_more = index + 1 < self.chunks.len();
            let next_chunk_index = if has_more {
                Some((index + 1) as i64)
            } else {
                None
            };

            Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index,
                next_row_offset: None,
            })
        }

        fn poll_config(&self) -> crate::client::TransportConfig {
            crate::client::TransportConfig::default()
        }

        async fn cancel_statement(&self, _statement_id: &str) -> Result<()> {
            Ok(())
        }

        async fn close_statement(&self, _statement_id: &str) -> Result<()> {
            Ok(())
        }

        async fn list_catalogs(&self, _session_id: &str) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_schemas(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_tables(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _table_types: Option<&[&str]>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_columns(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _table_pattern: Option<&str>,
            _column_pattern: Option<&str>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_procedures(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _procedure_pattern: Option<&str>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_functions(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _function_pattern: Option<&str>,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_primary_keys(
            &self,
            _session_id: &str,
            _catalog: &str,
            _schema: &str,
            _table: &str,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        async fn list_foreign_keys(
            &self,
            _session_id: &str,
            _parent_catalog: Option<&str>,
            _parent_schema: Option<&str>,
            _parent_table: Option<&str>,
            _foreign_catalog: &str,
            _foreign_schema: &str,
            _foreign_table: &str,
        ) -> Result<ExecuteResult> {
            self.execute_statement("", "", &ExecuteParams::default())
                .await
        }

        fn list_table_types(&self) -> Vec<String> {
            vec!["TABLE".to_string()]
        }
    }

    /// Test link with `next_chunk_index = Some(chunk_index + 1)`.
    /// Use [`create_test_link_terminal`] for the final link of a
    /// stream so the bounded-API end-of-stream sentinel is set
    /// correctly.
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

    /// Final link in a stream. `next_chunk_index = None` marks
    /// end-of-stream for link discovery (the kernel's contract per
    /// the bounded SEA API doc).
    fn create_test_link_terminal(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            next_chunk_index: None,
            ..create_test_link(chunk_index)
        }
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_fetch_links() {
        // Mock server returns chunks 0, 1, 2 with 2 being terminal.
        // Note: because the handle's constructor immediately triggers
        // an async prefetch, the cache may already be fully
        // populated by the time we call `fetch_links` below. So we
        // assert on the *first* link of the returned set rather than
        // a strict length: prefetch-completed and prefetch-pending
        // both produce correct kernel behaviour, just different
        // batch sizes.
        let links = vec![
            vec![create_test_link(0)],
            vec![create_test_link(1)],
            vec![create_test_link_terminal(2)],
        ];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[], // no initial links
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Fetch starting at chunk 0: first link in the result should
        // be chunk 0, and there is more to come.
        let result = handle.fetch_links(0, 0).await.unwrap();
        assert!(!result.links.is_empty());
        assert_eq!(result.links[0].chunk_index, 0);

        // Fetching from the last chunk index: only that link, no more.
        let result = handle.fetch_links(2, 0).await.unwrap();
        assert_eq!(result.links.len(), 1);
        assert_eq!(result.links[0].chunk_index, 2);
        assert!(!result.has_more);
        assert_eq!(result.next_chunk_index, None);
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_with_initial_links() {
        let links = vec![
            vec![create_test_link(0)],
            vec![create_test_link(1)],
            vec![create_test_link_terminal(2)],
        ];
        let client = Arc::new(MockDatabricksClient::new(links));

        // Initial link points at chunk 1 next; not terminal so the
        // fetcher knows more is coming.
        let initial_links = vec![create_test_link(0)];
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &initial_links,
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Chunk 0 should be cached
        assert!(handle.inner.has_link(0));

        // Fetch chunk 0 should return from cache
        let result = handle.fetch_links(0, 0).await.unwrap();
        assert_eq!(result.links.len(), 1);
        assert_eq!(result.links[0].chunk_index, 0);
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_fetch_links_end_of_stream() {
        // Single terminal chunk.
        let links = vec![vec![create_test_link_terminal(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Fetch beyond available chunks
        let result = handle.fetch_links(5, 0).await.unwrap();
        assert!(result.links.is_empty());
        assert!(!result.has_more);
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_link() {
        // Structure: chunks is indexed by chunk_index, each element contains links for that chunk
        // When refetch_link(chunk_index, _) is called, it fetches chunk[chunk_index] from the server
        // and searches for a link with matching chunk_index
        let links = vec![
            vec![create_test_link(0)],          // non-terminal
            vec![create_test_link_terminal(1)], // terminal
        ];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Refetch specific link (calls get_result_chunks(1, _) which returns chunk 1)
        let link = handle.refetch_link(1, 0).await.unwrap();
        assert_eq!(link.chunk_index, 1);

        // Link should now be cached
        assert!(handle.inner.has_link(1));
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_link_not_found() {
        let links = vec![vec![create_test_link_terminal(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Try to refetch non-existent link
        let result = handle.refetch_link(5, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_all_links_fetched_flag() {
        let links = vec![vec![create_test_link_terminal(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));

        // Initial link is terminal — fetcher should detect
        // all_links_fetched at construction time without ever
        // hitting the mock server.
        let initial_links = vec![create_test_link_terminal(0)];
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &initial_links,
            128,
            tokio::runtime::Handle::current(),
        );

        // Should be marked as all fetched since we have all chunks
        assert!(fetcher.all_links_fetched());
    }
}
