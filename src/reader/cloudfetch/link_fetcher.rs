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
use crate::klog::{debug, error};
use crate::types::cloudfetch::CloudFetchLink;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

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

    /// Refetch a specific chunk link.
    ///
    /// The current SEA streaming path uses [`Self::refetch_links_from`]
    /// (batched refresh) on expiry. This single-link variant is kept
    /// for callers that genuinely want a single link (e.g. tests, or
    /// future Thrift transports where each link refresh is its own
    /// RPC by construction).
    #[allow(
        dead_code,
        reason = "preserved for single-link callers; SEA path uses refetch_links_from"
    )]
    async fn refetch_link(&self, chunk_index: i64, row_offset: i64) -> Result<CloudFetchLink>;

    /// Refetch the link batch starting at `chunk_index`.
    ///
    /// SEA's `get_result_chunks` returns links for the requested
    /// chunk plus a server-decided forward window. When a single
    /// presigned URL has expired, the URLs for subsequent chunks are
    /// typically also near-expiry — refreshing the whole batch in
    /// one RPC instead of N single-link refetches is the contract
    /// we want.
    ///
    /// Returns the full list of links the server emitted. Callers
    /// are responsible for walking the result and reconciling each
    /// link with the current per-chunk state (e.g. skipping the
    /// download re-trigger for chunks already in `Downloaded`).
    ///
    /// Implementations also refresh their internal cache from the
    /// batch so subsequent `fetch_links` / `refetch_link` calls see
    /// the new URLs without an extra round trip.
    async fn refetch_links_from(
        &self,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<Vec<CloudFetchLink>>;

    /// Identifier of the statement this fetcher is bound to, if
    /// known. Used by downstream consumers (e.g. the streaming
    /// provider) to attach a `statement_id` field to log lines so
    /// operators can filter by query.
    ///
    /// Default implementation returns `None` — mock fetchers and
    /// non-SEA transports don't need to surface anything.
    fn statement_id(&self) -> Option<&str> {
        None
    }
}

/// SEA implementation of ChunkLinkFetcher with caching and proactive prefetching.
///
/// This fetcher:
/// - Makes API calls to the SEA get_result_chunks endpoint
/// - Caches fetched links in a concurrent DashMap
/// - Tracks the highest chunk index fetched to avoid redundant API calls
/// - Uses each link's server-emitted `next_chunk_index` to advance and
///   to detect end-of-stream — per the bounded SEA API contract for
///   OSS drivers. `total_chunk_count` from the manifest is deprecated
///   but is still consulted **as a transition-period guard** for
///   servers that haven't rolled out the per-link field; see
///   [`decode_continuation`] for the bridge logic.
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
    /// authoritative end-of-stream signal. We also consult the
    /// deprecated `total_chunk_count` manifest hint **only** as a
    /// transition-period safety net: if a server hasn't rolled out
    /// the per-link field yet, `next_chunk_index` would always be
    /// `None` and the kernel would silently truncate after the first
    /// batch. The hint lets us detect that case and keep fetching.
    /// See [`decode_continuation`].
    all_links_fetched: AtomicBool,
    /// **Transition-period only.** The deprecated manifest
    /// `total_chunk_count`, if any. Used purely to detect servers
    /// that haven't rolled out `external_links[].next_chunk_index`
    /// yet: when `next_chunk_index = None` AND `total_chunk_count`
    /// says more chunks remain, we fall back to
    /// `last_chunk.chunk_index + 1` and log a warning so the rollout
    /// asymmetry is visible. Will be removed once the bounded SEA
    /// API is live on all server versions the kernel targets.
    total_chunk_count_hint: Option<i64>,
    /// Sticky one-shot flag for the warning above. Avoids spamming
    /// logs on every batch.
    fallback_warned: AtomicBool,
    /// Sticky one-shot flag: server emitted neither
    /// `next_chunk_index` nor `total_chunk_count`. The kernel
    /// assumes end-of-stream in that case (only safe default),
    /// but operationally this is a contract regression worth
    /// surfacing.
    silent_eos_warned: AtomicBool,
    /// Sticky one-shot flag: server emitted a non-monotonic
    /// `next_chunk_index` (pointing backwards or at an
    /// already-cached chunk) and the kernel clamped to forward
    /// progress. An operator triaging "results look wrong" needs
    /// this breadcrumb to correlate with a server bug.
    non_monotonic_warned: AtomicBool,
    /// Sticky one-shot flag: prefetch loop has advanced past a
    /// sanity ceiling. If the server keeps emitting
    /// `next_chunk_index = Some(N)` indefinitely, the prefetch
    /// loop will issue unbounded RPCs and grow the link cache
    /// without bound (memory is bounded for *downloads* via
    /// `max_chunks_in_memory` but NOT for link metadata).
    runaway_prefetch_warned: AtomicBool,
    /// Sticky one-shot flag: server returned an empty batch with
    /// `has_more = true`. The kernel forces forward progress to
    /// avoid spinning, but the empty-with-has_more shape is a
    /// server bug worth surfacing in production logs.
    empty_with_has_more_warned: AtomicBool,
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

/// Outcome of decoding `(last_link, total_chunk_count_hint)` into
/// the next-fetch continuation state.
///
/// The bounded SEA API contract makes the per-link
/// `external_links[].next_chunk_index` the authoritative
/// continuation signal. `total_chunk_count` is deprecated for OSS-
/// driver use, but it remains populated by current SEA servers and
/// the kernel uses it **only as a transition-period guard**: a
/// server that hasn't rolled out the per-link field yet would
/// always emit `next_chunk_index = None`, which (without this
/// guard) the kernel would interpret as immediate end-of-stream
/// after the first batch — silently truncating multi-batch
/// results.
#[derive(Debug)]
struct Continuation {
    /// `chunk_index` to ask the server for next, OR a placeholder
    /// when at end-of-stream (`eos == true`).
    next_fetch_index: i64,
    /// Whether the kernel has discovered all the chunks the server
    /// will ever produce for this statement.
    eos: bool,
    /// True if the kernel reached its decision using the deprecated
    /// `total_chunk_count` hint rather than the per-link field. The
    /// caller emits a one-shot warning so the rollout asymmetry is
    /// visible in production logs.
    used_total_chunk_count_fallback: bool,
    /// True when *both* signals were absent — `next_chunk_index =
    /// None` AND `total_chunk_count = None`. The kernel assumes
    /// end-of-stream in this case (the only safe default), but a
    /// pre-rollout server emitting neither signal silently
    /// truncates multi-batch results. The caller emits a one-shot
    /// warning so post-rollout regressions are visible.
    used_silent_eos: bool,
}

/// Combine the server-emitted per-link `next_chunk_index` with the
/// deprecated `total_chunk_count` manifest hint.
///
/// Branches:
///
/// 1. `last_link.next_chunk_index = Some(N)` — trust the bounded
///    SEA API contract. Continue at N, eos = false.
/// 2. `last_link.next_chunk_index = None`, `total_chunk_count`
///    present and says we're at-or-past the last chunk — trust the
///    per-link signal. eos = true.
/// 3. `last_link.next_chunk_index = None`, `total_chunk_count`
///    says more chunks remain — **fallback path**: the server
///    hasn't rolled out the new field yet. Continue at
///    `last_chunk.chunk_index + 1`, eos = false, log a warning.
/// 4. `last_link.next_chunk_index = None`, `total_chunk_count =
///    None` — **silent-EOS path**: neither signal present. Assume
///    end-of-stream (the only safe default — we have no way to
///    know whether more chunks exist), but flag the case so the
///    caller can warn. A bounded-API server should always emit
///    at least one of the two; reaching this branch indicates a
///    pre-rollout server or a contract regression.
fn decode_continuation(last_link: &CloudFetchLink, total_chunk_count: Option<i64>) -> Continuation {
    let next_after = last_link.chunk_index.saturating_add(1);
    match (last_link.next_chunk_index, total_chunk_count) {
        (Some(idx), _) => Continuation {
            next_fetch_index: idx,
            eos: false,
            used_total_chunk_count_fallback: false,
            used_silent_eos: false,
        },
        (None, Some(total)) if next_after < total => Continuation {
            next_fetch_index: next_after,
            eos: false,
            used_total_chunk_count_fallback: true,
            used_silent_eos: false,
        },
        (None, Some(_)) => Continuation {
            next_fetch_index: next_after,
            eos: true,
            used_total_chunk_count_fallback: false,
            used_silent_eos: false,
        },
        (None, None) => Continuation {
            next_fetch_index: next_after,
            eos: true,
            used_total_chunk_count_fallback: false,
            used_silent_eos: true,
        },
    }
}

impl SeaChunkLinkFetcher {
    /// Create a new SEA link fetcher with caching and prefetching.
    ///
    /// End-of-stream and "where does the next fetch start" come
    /// from [`decode_continuation`], which combines the per-link
    /// `next_chunk_index` (authoritative per the bounded SEA API
    /// contract) with the deprecated `total_chunk_count` manifest
    /// hint (used **only** as a transition-period guard against
    /// servers that haven't rolled out the per-link field).
    ///
    /// # Arguments
    /// * `client` - The Databricks client to use for API calls
    /// * `statement_id` - The statement ID to fetch links for
    /// * `initial_links` - Links from the initial execute response.
    ///   Must be ordered by `chunk_index`; the last link's
    ///   `next_chunk_index` decides whether more chunks remain.
    /// * `total_chunk_count` - Transition-period guard hint from
    ///   the manifest. Passed `None` if absent or if the caller is
    ///   confident the bounded-API server is live.
    /// * `link_prefetch_window` - Number of chunks to prefetch ahead of consumer
    /// * `runtime_handle` - Tokio runtime handle for spawning prefetch tasks
    pub(crate) fn new(
        client: Arc<dyn Transport>,
        statement_id: impl Into<String>,
        initial_links: &[CloudFetchLink],
        total_chunk_count: Option<i64>,
        link_prefetch_window: i64,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let statement_id = statement_id.into();
        let links = DashMap::new();

        // Store initial links.
        for link in initial_links {
            links.insert(link.chunk_index, link.clone());
        }

        // Compute continuation. Highest chunk_index in the initial
        // batch is the floor for `next_server_fetch_index` — guards
        // against a hypothetical (or buggy) server response with a
        // non-monotonic `next_chunk_index` that points backwards.
        let max_initial_chunk = initial_links
            .iter()
            .map(|l| l.chunk_index)
            .max()
            .unwrap_or(-1);
        let initial_floor = max_initial_chunk.saturating_add(1);

        // Compute continuation from the last initial link, if any.
        // The constructor can't use `apply_continuation` directly
        // (no `&self` yet — we're building it) so it inlines the
        // same sequence and emits the warnings post-build.
        let (
            next_server_fetch_index,
            all_links_fetched,
            next_row_offset,
            fallback_used,
            non_monotonic_clamp,
        ) = match initial_links.last() {
            Some(link) => {
                let cont = decode_continuation(link, total_chunk_count);
                // Clamp to floor: if the server emitted a
                // `next_chunk_index` ≤ already-cached
                // `chunk_index`, force forward progress so the
                // prefetch loop can't re-issue the same RPC.
                let next_idx = cont.next_fetch_index.max(initial_floor);
                let clamp_fired = !cont.eos && cont.next_fetch_index < initial_floor;
                (
                    next_idx,
                    cont.eos,
                    link.row_offset_after(),
                    cont.used_total_chunk_count_fallback,
                    clamp_fired.then_some((cont.next_fetch_index, link.chunk_index)),
                )
            }
            // No initial links → "kernel constructed before the
            // first server response" shape used by tests. Start
            // at chunk 0; eos = false so the first server fetch
            // can populate the cache.
            None => (0, false, 0, false, None),
        };

        debug!(
            "SeaChunkLinkFetcher: {} initial links, next_server_fetch={}, \
             next_row_offset={}, all_fetched={}, prefetch_window={}, total_chunk_count={:?}",
            initial_links.len(),
            next_server_fetch_index,
            next_row_offset,
            all_links_fetched,
            link_prefetch_window,
            total_chunk_count,
        );

        let fetcher = Self {
            client,
            statement_id,
            links,
            next_server_fetch_index: AtomicI64::new(next_server_fetch_index),
            all_links_fetched: AtomicBool::new(all_links_fetched),
            total_chunk_count_hint: total_chunk_count,
            fallback_warned: AtomicBool::new(false),
            silent_eos_warned: AtomicBool::new(false),
            non_monotonic_warned: AtomicBool::new(false),
            runaway_prefetch_warned: AtomicBool::new(false),
            empty_with_has_more_warned: AtomicBool::new(false),
            current_consumer_index: AtomicI64::new(0),
            link_prefetch_window,
            prefetch_in_progress: AtomicBool::new(false),
            runtime_handle,
            prefetch_lock: TokioMutex::new(()),
            next_row_offset: AtomicI64::new(next_row_offset),
        };
        if fallback_used {
            fetcher.warn_total_chunk_count_fallback("initial response");
        }
        // Capture any silent-EOS signal so a pre-rollout server is
        // visible from the very first batch.
        if let Some(link) = initial_links.last() {
            if link.next_chunk_index.is_none() && total_chunk_count.is_none() {
                fetcher.warn_silent_eos("initial response");
            }
        }
        // Capture any non-monotonic-clamp event for the same
        // reason — server bug visibility from batch 1.
        if let Some((server_emitted, last_chunk)) = non_monotonic_clamp {
            fetcher.warn_non_monotonic_clamp(
                "initial response",
                server_emitted,
                initial_floor,
                last_chunk,
            );
        }
        fetcher
    }

    /// Emit a one-shot warning when the kernel falls back to the
    /// deprecated `total_chunk_count` continuation signal because
    /// the server hasn't populated `external_links[].next_chunk_index`
    /// yet. Production telemetry can grep for this to know when to
    /// remove the fallback path.
    fn warn_total_chunk_count_fallback(&self, site: &str) {
        if !self.fallback_warned.swap(true, Ordering::AcqRel) {
            crate::klog::warn!(
                statement_id = %self.statement_id,
                total_chunk_count = ?self.total_chunk_count_hint,
                "Server did not populate `external_links[].next_chunk_index` ({site}); \
                 falling back to `total_chunk_count` for continuation. Remove the \
                 fallback path once the bounded-SEA-API rollout is complete.",
            );
        }
    }

    /// One-shot warning for the silent-EOS branch — server emitted
    /// neither `next_chunk_index` nor `total_chunk_count`. The
    /// kernel can only assume end-of-stream (no signal to do
    /// otherwise). A bounded-API server should always emit at
    /// least one signal; reaching this branch indicates a
    /// pre-rollout server or a contract regression that silently
    /// truncates multi-batch results.
    fn warn_silent_eos(&self, site: &str) {
        if !self.silent_eos_warned.swap(true, Ordering::AcqRel) {
            crate::klog::warn!(
                statement_id = %self.statement_id,
                "Server emitted neither `next_chunk_index` nor `total_chunk_count` ({site}); \
                 assuming end-of-stream. If this query has more than one batch of links, \
                 results are silently truncated — likely a pre-rollout server or contract \
                 regression.",
            );
        }
    }

    /// One-shot warning when the server emits a non-monotonic
    /// `next_chunk_index` (pointing at or behind a chunk the
    /// kernel has already cached). The kernel clamps to forward
    /// progress, but the operator triaging "results look wrong"
    /// needs the breadcrumb to correlate with a server bug.
    fn warn_non_monotonic_clamp(
        &self,
        site: &str,
        server_emitted: i64,
        clamped_to: i64,
        last_chunk_index: i64,
    ) {
        if !self.non_monotonic_warned.swap(true, Ordering::AcqRel) {
            crate::klog::warn!(
                statement_id = %self.statement_id,
                server_next_chunk_index = server_emitted,
                last_chunk_index,
                clamped_to,
                "Server emitted non-monotonic `next_chunk_index` ({site}); \
                 clamping forward to avoid re-issuing the same RPC. \
                 Likely a server bug — kernel result-ordering is unaffected, \
                 but downstream consumers may see latency from re-fetches.",
            );
        }
    }

    /// One-shot warning when the prefetch loop has advanced past
    /// a sanity ceiling. Without this, a server emitting
    /// `next_chunk_index = Some(N)` indefinitely would cause
    /// unbounded growth in the link-metadata cache (downloads are
    /// bounded by `max_chunks_in_memory`; metadata is not).
    fn warn_runaway_prefetch(&self, site: &str, current_index: i64, ceiling: i64) {
        if !self.runaway_prefetch_warned.swap(true, Ordering::AcqRel) {
            crate::klog::warn!(
                statement_id = %self.statement_id,
                next_server_fetch_index = current_index,
                ceiling,
                total_chunk_count = ?self.total_chunk_count_hint,
                "Prefetch loop has advanced past the runaway-detection ceiling ({site}); \
                 link-cache memory will grow unbounded if the server keeps emitting \
                 `next_chunk_index = Some(N)`. Likely a server bug.",
            );
        }
    }

    /// One-shot warning when the server returns an empty batch
    /// with `has_more = true`. The kernel forces forward progress
    /// to avoid spinning, but the empty-with-has_more shape is a
    /// server bug we want surfaced in production logs.
    fn warn_empty_with_has_more(&self, site: &str, advanced_from: i64, advanced_to: i64) {
        if !self.empty_with_has_more_warned.swap(true, Ordering::AcqRel) {
            crate::klog::warn!(
                statement_id = %self.statement_id,
                advanced_from,
                advanced_to,
                "Server returned an empty batch with has_more=true ({site}); \
                 forcing forward progress to avoid an infinite loop. Likely a server bug.",
            );
        }
    }

    /// Sanity ceiling for runaway-prefetch detection.
    ///
    /// `10 × total_chunk_count_hint` when known (so a server that
    /// overshoots its own claimed chunk count by 10× is still
    /// flagged), else a hard floor of 10,000. Either way: if the
    /// prefetch loop crosses this threshold without hitting EOS,
    /// something is wrong upstream.
    fn runaway_prefetch_ceiling(&self) -> i64 {
        self.total_chunk_count_hint
            .map(|n| n.saturating_mul(10))
            .unwrap_or(10_000)
            .max(10_000)
    }

    /// Apply a `decode_continuation` result to the fetcher's
    /// atomic state, emitting any one-shot warnings required.
    ///
    /// Three sites share this invariant — constructor, prefetch
    /// loop, and sync fallback — and a previous round of review
    /// (F8) called out that hand-rolling the sequence at each
    /// site is exactly the kind of code that rots. This helper
    /// is the single source of truth for the order of
    /// operations:
    ///
    /// 1. Update `next_row_offset` via `fetch_max` (must not
    ///    regress — sync-fallback and prefetch run concurrently).
    /// 2. Emit `total_chunk_count` fallback warning if relevant.
    /// 3. Emit silent-EOS warning if relevant.
    /// 4. Either flip `all_links_fetched` (eos case) or
    ///    `fetch_max` the new `next_server_fetch_index` (forward
    ///    progress). The forward-progress case also clamps to
    ///    `last_chunk.chunk_index + 1` so a non-monotonic server
    ///    response can't drive the cursor backwards.
    /// 5. Emit runaway-prefetch warning if the new cursor has
    ///    crossed the sanity ceiling.
    ///
    /// Returns the post-update `next_server_fetch_index` (for
    /// callers that need to report it, e.g. building a
    /// `ChunkLinkFetchResult`).
    fn apply_continuation(&self, last_link: &CloudFetchLink, site: &str) -> i64 {
        // 1. Row-offset cursor — `fetch_max` so a slow sync-fallback
        //    completing late can never regress a faster prefetch.
        let next_row = last_link.row_offset_after();
        self.next_row_offset.fetch_max(next_row, Ordering::Release);

        // 2/3. Decode + warn.
        let cont = decode_continuation(last_link, self.total_chunk_count_hint);
        if cont.used_total_chunk_count_fallback {
            self.warn_total_chunk_count_fallback(site);
        }
        if cont.used_silent_eos {
            self.warn_silent_eos(site);
        }

        // 4. Either EOS or forward progress (with clamp).
        if cont.eos {
            self.all_links_fetched.store(true, Ordering::Release);
            return self.next_server_fetch_index.load(Ordering::Acquire);
        }
        let clamp_floor = last_link.chunk_index.saturating_add(1);
        let advance_to = cont.next_fetch_index.max(clamp_floor);
        if cont.next_fetch_index < clamp_floor {
            self.warn_non_monotonic_clamp(
                site,
                cont.next_fetch_index,
                advance_to,
                last_link.chunk_index,
            );
        }
        self.next_server_fetch_index
            .fetch_max(advance_to, Ordering::Release);

        // 5. Runaway-prefetch sanity check.
        let ceiling = self.runaway_prefetch_ceiling();
        if advance_to > ceiling {
            self.warn_runaway_prefetch(site, advance_to, ceiling);
        }

        advance_to
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

                    // The continuation signal comes from
                    // `decode_continuation`: prefer the per-link
                    // `next_chunk_index` (bounded SEA API
                    // contract); fall back to `total_chunk_count`
                    // only when the server hasn't rolled out the
                    // new field. Forward progress is guarded —
                    // `fetch_max` and a separate
                    // `max(chunk_index+1)` floor prevent a
                    // non-monotonic server response from driving
                    // `next_server_fetch_index` backwards.
                    //
                    // Setting `all_links_fetched = true` here just
                    // stops the prefetch loop; in-flight chunk
                    // downloads continue independently.
                    if let Some(last_link) = result.links.last() {
                        let last_chunk_index = last_link.chunk_index;
                        let links_count = result.links.len();
                        let advance_to = self.apply_continuation(last_link, "prefetch loop");
                        if self.all_links_fetched.load(Ordering::Acquire) {
                            debug!("Prefetch loop: end-of-stream on link {}", last_chunk_index,);
                            break;
                        }
                        debug!(
                            "Prefetch loop: fetched {} links (last chunk {}), \
                             next_server_fetch={}",
                            links_count, last_chunk_index, advance_to,
                        );
                    } else if !result.has_more {
                        // Defensive: server returned no links and
                        // signalled no more. Treat as end-of-stream
                        // for link discovery.
                        debug!("Prefetch loop: empty response with has_more=false");
                        self.all_links_fetched.store(true, Ordering::Release);
                        break;
                    } else {
                        // Empty batch but `has_more` is true. Force
                        // forward progress so the loop can't spin
                        // re-issuing the same RPC. Two strategies:
                        // 1. If the server gave us a
                        //    `next_chunk_index` hint, use it.
                        // 2. Otherwise bump past
                        //    `server_fetch_idx` ourselves.
                        // Either way, on the next iteration we'll
                        // ask for a *different* chunk index, which
                        // breaks the loop. If the server keeps
                        // returning empty-with-has_more, that's a
                        // server bug we surface via the
                        // `current_consumer_index` heuristic in
                        // streaming_provider (consumer never
                        // advances → chunks_in_memory stays full
                        // → prefetch pauses) AND via the
                        // `empty_with_has_more_warned` one-shot.
                        let advance_to = result
                            .next_chunk_index
                            .unwrap_or_else(|| server_fetch_idx.saturating_add(1));
                        self.next_server_fetch_index
                            .fetch_max(advance_to, Ordering::Release);
                        self.warn_empty_with_has_more(
                            "prefetch loop",
                            server_fetch_idx,
                            advance_to,
                        );
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
                i = i.saturating_add(1);
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

        // If request is for links we should already have, return
        // from cache. Walk contiguously and STOP at the first hole
        // — a `for i in start..server_fetch_index` with silent
        // `is_none()` skip would return a non-contiguous slice and
        // hang the streaming provider's wait_for_chunk on the
        // first missing index.
        if start_chunk_index < server_fetch_index {
            let mut links = Vec::new();
            let mut i = start_chunk_index;
            while i < server_fetch_index {
                let Some(link) = fetcher.links.get(&i) else {
                    break;
                };
                links.push(link.clone());
                i = i.saturating_add(1);
            }
            // `i` is now either `server_fetch_index` (no holes) or
            // the first missing chunk_index — in either case it's
            // where the next continuation should pick up.
            let has_more = !fetcher.all_links_fetched.load(Ordering::Acquire);
            return Ok(ChunkLinkFetchResult {
                links,
                has_more,
                next_chunk_index: if has_more { Some(i) } else { None },
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

        // Advance via the shared `apply_continuation` helper so
        // prefetch-loop and sync-fallback share the same bridge
        // logic for the total_chunk_count rollout, the same
        // forward-progress clamp, and the same one-shot warnings.
        let next_chunk_index = match result.links.last() {
            Some(last_link) => {
                let advance_to = fetcher.apply_continuation(last_link, "sync fallback");
                if fetcher.all_links_fetched.load(Ordering::Acquire) {
                    None
                } else {
                    Some(advance_to)
                }
            }
            None => {
                // Empty response. Honour the server's has_more
                // signal AND defend against an empty-with-has_more
                // spin: force forward progress on
                // next_server_fetch_index so the next call asks
                // for a different chunk index. The server is
                // expected to either yield links or signal
                // end-of-stream — a perpetual "I have more but
                // here are zero links" loop is a server bug we
                // refuse to amplify into an infinite local retry.
                if !result.has_more {
                    fetcher.all_links_fetched.store(true, Ordering::Release);
                } else {
                    let advance_to = result
                        .next_chunk_index
                        .unwrap_or_else(|| start_chunk_index.saturating_add(1));
                    fetcher
                        .next_server_fetch_index
                        .fetch_max(advance_to, Ordering::Release);
                    fetcher.warn_empty_with_has_more(
                        "sync fallback",
                        start_chunk_index,
                        advance_to,
                    );
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

    async fn refetch_links_from(
        &self,
        chunk_index: i64,
        row_offset: i64,
    ) -> Result<Vec<CloudFetchLink>> {
        let result = self
            .inner
            .client
            .get_result_chunks(&self.inner.statement_id, chunk_index, row_offset)
            .await?;

        // The SEA contract today caps the number of links in a
        // single `get_result_chunks` response to ~empirical bounds
        // (≤ a few hundred chunks per page). If SEA ever changes
        // and a page returns thousands of links, the downstream
        // reconciliation loop (which mutates a DashMap entry per
        // link) is a quiet O(N) hotspot. `debug_assert!` makes the
        // assumption loud in debug builds without paying for it in
        // release.
        debug_assert!(
            result.links.len() <= 10_000,
            "implausibly large refresh response ({} links) — \
             SEA contract may have changed; review the cap",
            result.links.len(),
        );

        // Refresh the internal link cache with every link the server
        // returned. The caller will reconcile per-chunk state in the
        // streaming provider (which knows which chunks are already
        // downloaded vs in-flight).
        for link in &result.links {
            self.inner.links.insert(link.chunk_index, link.clone());
        }

        // Mirror the continuation cursor updates from `fetch_links`:
        // a refresh response is a normal `get_result_chunks`
        // response, just used for the expiry-recovery path. If we
        // drop its continuation metadata on the floor (as the
        // initial implementation did), the prefetch loop happily
        // keeps firing past the actual end of the result set when a
        // refresh response carries `next_chunk_index = None`. Use
        // `fetch_max` so an early-window refresh (start_chunk_index
        // < current cursor) doesn't rewind progress.
        if let Some(last_link) = result.links.last() {
            let next_row_offset = last_link.row_offset.saturating_add(last_link.row_count);
            self.inner
                .next_row_offset
                .fetch_max(next_row_offset, Ordering::Release);
            let cont = decode_continuation(last_link, self.inner.total_chunk_count_hint);
            if cont.used_total_chunk_count_fallback {
                self.inner.warn_total_chunk_count_fallback("link refresh");
            }
            if cont.eos {
                self.inner.all_links_fetched.store(true, Ordering::Release);
            } else {
                let advance_to = cont
                    .next_fetch_index
                    .max(last_link.chunk_index.saturating_add(1));
                self.inner
                    .next_server_fetch_index
                    .fetch_max(advance_to, Ordering::Release);
            }
        }

        Ok(result.links)
    }

    fn statement_id(&self) -> Option<&str> {
        Some(&self.inner.statement_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ExecuteResult, SessionInfo};
    use crate::reader::EmptyReader;
    use crate::types::sea::{ExecuteParams, StatementState, StatementStatus};
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
                status: StatementStatus {
                    state: StatementState::Succeeded,
                    error: None,
                    num_modified_rows: None,
                    display_message: None,
                    diagnostic_info: None,
                    error_details_json: None,
                },
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
        async fn execute_statement_direct(
            &self,
            _session_id: &str,
            _sql: &str,
            _params: &ExecuteParams,
        ) -> Result<crate::client::DirectOutcome> {
            unimplemented!("execute_statement_direct not used by the link-fetcher mock")
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

        async fn get_inline_chunk(
            &self,
            _statement_id: &str,
            _chunk_index: i64,
            _row_offset: i64,
        ) -> Result<crate::client::InlineChunkFetchResult> {
            unimplemented!("get_inline_chunk not used by the link-fetcher mock")
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

        async fn list_procedure_columns(
            &self,
            _session_id: &str,
            _catalog: Option<&str>,
            _schema_pattern: Option<&str>,
            _procedure_pattern: Option<&str>,
            _column_pattern: Option<&str>,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Try to refetch non-existent link
        let result = handle.refetch_link(5, 0).await;
        assert!(result.is_err());
    }

    // ──────────────────────────────────────────────────────────────
    // refetch_links_from coverage — exercises the batched-refresh
    // entry point used by the streaming provider's expiry path.
    // The SEA impl writes the cache, advances the row-offset and
    // server-fetch cursors, and reconciles EOS — none of which
    // was tested directly before (only via mocks at the trait
    // level inside the streaming provider).
    // ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_links_from_writes_cache() {
        // After `refetch_links_from`, every link in the response
        // must be in the internal cache so subsequent
        // `fetch_links` / `refetch_link` calls see the new URLs
        // without an extra round trip.
        let links = vec![
            vec![create_test_link(0)],
            vec![create_test_link(1)],
            vec![create_test_link_terminal(2)],
        ];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            None,
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Request a refresh batch from chunk 1 onward.
        let refreshed = handle.refetch_links_from(1, 1000).await.unwrap();

        // Mock returns one link per `get_result_chunks` call —
        // the chunk at the requested index. Refresh response is
        // a single link for chunk 1.
        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].chunk_index, 1);

        // Cache was updated.
        assert!(handle.inner.has_link(1));
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_links_from_advances_eos() {
        // When `refetch_links_from` returns a terminal link
        // (`next_chunk_index = None`), the `all_links_fetched`
        // flag must flip so subsequent `fetch_links` calls
        // short-circuit instead of hitting the server again.
        let links = vec![vec![create_test_link_terminal(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            None,
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Pre-condition: not yet at EOS.
        assert!(!handle.inner.all_links_fetched.load(Ordering::Acquire));

        let _ = handle.refetch_links_from(0, 0).await.unwrap();

        // Post-condition: EOS flipped on after the terminal link
        // landed in the refresh response.
        assert!(
            handle.inner.all_links_fetched.load(Ordering::Acquire),
            "terminal link in refresh response must flip all_links_fetched",
        );
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_links_from_advances_cursor() {
        // `refetch_links_from` mirrors `fetch_links`'s cursor
        // advancement (`next_server_fetch_index` via `fetch_max`,
        // `next_row_offset` via `fetch_max`). A subsequent
        // prefetch must NOT re-issue the chunk we just refreshed.
        let links = vec![
            vec![create_test_link(0)],
            vec![create_test_link(1)],
            vec![create_test_link_terminal(2)],
        ];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            None,
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Pre-condition: cursor at 0.
        assert_eq!(
            handle.inner.next_server_fetch_index.load(Ordering::Acquire),
            0
        );
        assert_eq!(handle.inner.next_row_offset.load(Ordering::Acquire), 0);

        // Refresh from chunk 0. The non-terminal link's
        // continuation should advance the cursor past 0.
        let _ = handle.refetch_links_from(0, 0).await.unwrap();

        // `next_server_fetch_index` advances past the refreshed
        // index — refresh of chunk 0 should leave it at 1 (or
        // higher, depending on the test link's `next_chunk_index`
        // hint).
        let new_idx = handle.inner.next_server_fetch_index.load(Ordering::Acquire);
        assert!(
            new_idx >= 1,
            "next_server_fetch_index must advance past refreshed chunk; got {}",
            new_idx,
        );

        // `next_row_offset` advances past chunk 0's rows
        // (row_offset=0 + row_count=1000 = 1000).
        let new_offset = handle.inner.next_row_offset.load(Ordering::Acquire);
        assert_eq!(
            new_offset, 1000,
            "next_row_offset must advance to row_offset + row_count",
        );
    }

    #[tokio::test]
    async fn test_sea_link_fetcher_refetch_links_from_empty_response() {
        // A `get_result_chunks` call past end-of-stream returns
        // `ChunkLinkFetchResult::end_of_stream()` (empty links,
        // `has_more = false`). `refetch_links_from` must handle
        // this gracefully — no panic on empty `.last()`, no
        // bogus cursor update.
        let links = vec![vec![create_test_link_terminal(0)]];
        let client = Arc::new(MockDatabricksClient::new(links));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "test-statement",
            &[],
            None,
            128,
            tokio::runtime::Handle::current(),
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Ask for a chunk past the end — mock returns
        // end_of_stream (empty links).
        let refreshed = handle.refetch_links_from(99, 0).await.unwrap();
        assert!(
            refreshed.is_empty(),
            "past-EOS request should return empty vec, not error",
        );
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
            None,
            128,
            tokio::runtime::Handle::current(),
        );

        // Should be marked as all fetched since we have all chunks
        assert!(fetcher.all_links_fetched());
    }

    // ──────────────────────────────────────────────────────────────
    // Continuation tests.
    //
    // These exercise the bounded-SEA-API control flow directly,
    // bypassing the async prefetch race so a regression in
    // `next_chunk_index` advancement / `row_offset` arithmetic /
    // EOS detection actually fails the test.
    //
    // `link_prefetch_window = 0` keeps the prefetch loop from
    // racing ahead of the consumer-driven `fetch_links` call.
    // ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_terminal_on_first_batch_single_chunk() {
        // Single terminal link in the initial response → fetcher
        // must initialise with `all_links_fetched = true` and zero
        // server-fetch RPCs are needed.
        let client = Arc::new(MockDatabricksClient::new(vec![]));
        let initial = vec![create_test_link_terminal(0)];
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "stmt",
            &initial,
            None,
            0,
            tokio::runtime::Handle::current(),
        );
        assert!(
            fetcher.all_links_fetched(),
            "terminal initial link must flip all_links_fetched",
        );
    }

    #[tokio::test]
    async fn test_multi_batch_continuation() {
        // 2-batch fixture: initial = chunk 0 (non-terminal,
        // next_chunk_index = 1). Mock server returns chunk 1
        // (terminal) when asked for `chunk_index = 1`. Drive sync
        // `fetch_links` and assert state after each call.
        //
        // MockDatabricksClient indexes the response Vec by
        // `chunk_index`, so position 1 = chunk 1's response.
        let server_chunks = vec![
            vec![],                             // index 0: never asked
            vec![create_test_link_terminal(1)], // index 1: chunk 1, terminal
        ];
        let client = Arc::new(MockDatabricksClient::new(server_chunks));

        // Use a non-terminal initial chunk so the kernel knows to
        // continue.
        let initial = vec![create_test_link(0)];
        let fetcher = SeaChunkLinkFetcher::new(
            Arc::clone(&client) as Arc<dyn Transport>,
            "stmt",
            &initial,
            None,
            /* link_prefetch_window = */ 0,
            tokio::runtime::Handle::current(),
        );
        // Wrap directly in the handle without triggering the prefetch
        // bell — `link_prefetch_window=0` keeps it idle anyway.
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // After construction the fetcher has only chunk 0.
        assert!(handle.inner.has_link(0));
        assert!(!handle.inner.has_link(1));
        assert!(!handle.inner.all_links_fetched());

        // Drive the next-batch fetch. Mock returns chunk 1
        // (terminal), so the fetcher must flip EOS.
        let result = handle.fetch_links(1, 0).await.unwrap();
        assert!(
            result.links.iter().any(|l| l.chunk_index == 1),
            "second fetch must surface chunk 1",
        );
        assert!(
            handle.inner.has_link(1),
            "chunk 1 must be cached after fetch",
        );
        assert!(
            handle.inner.all_links_fetched(),
            "terminal link must flip EOS for link discovery",
        );
        // `next_chunk_index` of the returned result must be None on
        // the terminal batch.
        assert!(
            !result.has_more,
            "has_more must be false after terminal batch",
        );
        assert!(result.next_chunk_index.is_none());
    }

    #[tokio::test]
    async fn test_row_offset_advancement() {
        // Initial chunk: row_offset=0, row_count=1000 →
        // next_row_offset must be 1000. After a continuation that
        // returns a chunk with row_offset=1000, row_count=500, the
        // fetcher's tracked next_row_offset must be 1500.
        let server_chunk = CloudFetchLink {
            url: "https://example.com/chunk1".to_string(),
            chunk_index: 1,
            row_offset: 1000,
            row_count: 500,
            byte_count: 50000,
            expiration: chrono::Utc::now() + chrono::Duration::hours(1),
            http_headers: HashMap::new(),
            next_chunk_index: None, // terminal
        };
        // Server-side Vec indexed by chunk_index — pad slot 0 with
        // an empty response (never asked).
        let client = Arc::new(MockDatabricksClient::new(vec![vec![], vec![server_chunk]]));

        let initial = vec![create_test_link(0)]; // row_offset=0, row_count=1000
        let fetcher = SeaChunkLinkFetcher::new(
            Arc::clone(&client) as Arc<dyn Transport>,
            "stmt",
            &initial,
            None,
            /* link_prefetch_window = */ 0,
            tokio::runtime::Handle::current(),
        );
        assert_eq!(
            fetcher.next_row_offset.load(Ordering::Acquire),
            1000,
            "initial next_row_offset must be initial_link.row_offset + row_count",
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        let _ = handle.fetch_links(1, 1000).await.unwrap();

        assert_eq!(
            handle.inner.next_row_offset.load(Ordering::Acquire),
            1500,
            "next_row_offset must advance to last_link.row_offset + row_count",
        );
    }

    // ──────────────────────────────────────────────────────────────
    // total_chunk_count transition-period fallback.
    // ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_total_chunk_count_fallback_keeps_fetching() {
        // **P0 regression guard.** Today's SEA servers don't yet
        // populate `external_links[].next_chunk_index`. Without the
        // total_chunk_count fallback, the kernel would interpret
        // `next_chunk_index = None` on a non-terminal chunk as
        // end-of-stream and silently drop chunks 1..N.
        //
        // Setup: initial link emits NO per-link next_chunk_index
        // (`create_test_link_no_next`), but manifest says
        // total_chunk_count = 2 (so chunk 1 must still be fetched).
        // Mock returns chunk 1, also without per-link next, but
        // total_chunk_count says it's the last one.
        let server_chunk_1 = create_test_link_no_next(1);
        let client = Arc::new(MockDatabricksClient::new(vec![
            vec![], // index 0: never asked
            vec![server_chunk_1],
        ]));

        let initial = vec![create_test_link_no_next(0)];
        let fetcher = SeaChunkLinkFetcher::new(
            Arc::clone(&client) as Arc<dyn Transport>,
            "stmt",
            &initial,
            /* total_chunk_count = */ Some(2),
            0,
            tokio::runtime::Handle::current(),
        );
        // Crucial: fetcher must NOT mark all_links_fetched at
        // construction even though the per-link signal would say so.
        assert!(
            !fetcher.all_links_fetched(),
            "fallback path must keep fetching when total_chunk_count says more remain",
        );
        assert_eq!(
            fetcher.next_server_fetch_index.load(Ordering::Acquire),
            1,
            "next fetch must target chunk 1 (initial last_chunk + 1) via fallback",
        );
        let handle = SeaChunkLinkFetcherHandle::new(fetcher);

        // Drive the continuation fetch.
        let result = handle.fetch_links(1, 0).await.unwrap();
        assert!(
            result.links.iter().any(|l| l.chunk_index == 1),
            "second fetch must surface chunk 1",
        );
        // Per-link signal still None on chunk 1, but
        // total_chunk_count = 2 → last_chunk + 1 == 2 == total →
        // EOS reached.
        assert!(
            handle.inner.all_links_fetched(),
            "fallback path must flip EOS once last_chunk + 1 == total_chunk_count",
        );
    }

    /// Helper: link whose `next_chunk_index` is `None` (simulates a
    /// pre-rollout SEA server that doesn't emit the per-link field).
    fn create_test_link_no_next(chunk_index: i64) -> CloudFetchLink {
        CloudFetchLink {
            next_chunk_index: None,
            ..create_test_link(chunk_index)
        }
    }

    #[test]
    fn test_decode_continuation_per_link_wins_when_present() {
        // Bounded-API path: per-link `next_chunk_index = Some` →
        // used directly, no fallback.
        let link = create_test_link(0); // next_chunk_index = Some(1)
        let cont = decode_continuation(&link, /* total */ Some(100));
        assert_eq!(cont.next_fetch_index, 1);
        assert!(!cont.eos);
        assert!(!cont.used_total_chunk_count_fallback);
    }

    #[test]
    fn test_decode_continuation_fallback_when_per_link_missing_and_total_more() {
        // Pre-rollout path: per-link None, total says more.
        let link = create_test_link_no_next(0);
        let cont = decode_continuation(&link, Some(3));
        assert_eq!(cont.next_fetch_index, 1, "fallback continues from chunk+1");
        assert!(!cont.eos);
        assert!(cont.used_total_chunk_count_fallback);
    }

    #[test]
    fn test_decode_continuation_eos_when_per_link_none_and_total_agrees() {
        // Bounded-API path on terminal chunk: per-link None,
        // total_chunk_count says we're at-or-past the end. EOS.
        let link = create_test_link_no_next(2); // chunk 2 of 3
        let cont = decode_continuation(&link, Some(3));
        assert!(cont.eos);
        assert!(!cont.used_total_chunk_count_fallback);
    }

    #[test]
    fn test_decode_continuation_eos_when_per_link_none_and_no_total() {
        // Silent-EOS branch: neither signal present. Decode falls
        // through to "assume end-of-stream" (the only safe default)
        // but flags the case via `used_silent_eos` so the caller
        // can fire a one-shot warning.
        let link = create_test_link_no_next(0);
        let cont = decode_continuation(&link, None);
        assert!(cont.eos);
        assert!(!cont.used_total_chunk_count_fallback);
        assert!(
            cont.used_silent_eos,
            "(None, None) must surface as silent-EOS so caller can warn",
        );
    }

    #[test]
    fn test_decode_continuation_used_silent_eos_false_in_other_branches() {
        // The silent-EOS flag must be exclusive — it should NOT
        // fire in any of the other three branches.

        // 1. per-link Some → no silent-EOS
        let link_with_next = create_test_link(0);
        let cont = decode_continuation(&link_with_next, None);
        assert!(
            !cont.used_silent_eos,
            "per-link Some must not flag silent-EOS"
        );

        // 2. per-link None + total says more remaining → fallback path
        let link_no_next = create_test_link_no_next(0);
        let cont = decode_continuation(&link_no_next, Some(5));
        assert!(
            !cont.used_silent_eos,
            "fallback path must not flag silent-EOS"
        );
        assert!(cont.used_total_chunk_count_fallback);

        // 3. per-link None + total says at-end → trust per-link
        let cont = decode_continuation(&link_no_next, Some(1));
        assert!(
            !cont.used_silent_eos,
            "explicit-EOS via total must not flag silent-EOS"
        );
        assert!(cont.eos);
    }

    #[tokio::test]
    async fn test_constructor_clamps_non_monotonic_next_chunk_index() {
        // Defensive: if the server emits `next_chunk_index <
        // chunk_index` (a server bug), the fetcher must not let
        // `next_server_fetch_index` regress and cause the prefetch
        // loop to re-issue the same RPC. AND it must fire the
        // one-shot `non_monotonic_warned` flag so an operator
        // can correlate "results look wrong" with the server bug.
        let bad_link = CloudFetchLink {
            chunk_index: 5,
            next_chunk_index: Some(3), // backwards
            ..create_test_link(5)
        };
        let client = Arc::new(MockDatabricksClient::new(vec![]));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "stmt",
            &[bad_link],
            None,
            0,
            tokio::runtime::Handle::current(),
        );
        assert!(
            fetcher.next_server_fetch_index.load(Ordering::Acquire) >= 6,
            "non-monotonic next_chunk_index must be clamped to max(chunk_index) + 1",
        );
        assert!(
            fetcher.non_monotonic_warned.load(Ordering::Acquire),
            "non-monotonic clamp must fire the sticky warning flag",
        );
    }

    #[tokio::test]
    async fn test_constructor_warns_silent_eos_when_neither_signal_present() {
        // G1: a pre-rollout server that emits neither
        // `next_chunk_index` nor `total_chunk_count` causes the
        // kernel to assume EOS. Verify the sticky warning fires.
        let link_neither = CloudFetchLink {
            chunk_index: 0,
            next_chunk_index: None,
            ..create_test_link(0)
        };
        let client = Arc::new(MockDatabricksClient::new(vec![]));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "stmt",
            &[link_neither],
            None, // total_chunk_count also absent
            0,
            tokio::runtime::Handle::current(),
        );
        assert!(
            fetcher.silent_eos_warned.load(Ordering::Acquire),
            "silent-EOS branch must fire the sticky warning flag",
        );
    }

    #[tokio::test]
    async fn test_constructor_does_not_warn_silent_eos_when_total_present() {
        // Mirror: if `total_chunk_count` is present, we're either
        // in the "fallback" path or the "explicit EOS via total"
        // path — neither is silent-EOS.
        let link_neither = CloudFetchLink {
            chunk_index: 0,
            next_chunk_index: None,
            ..create_test_link(0)
        };
        let client = Arc::new(MockDatabricksClient::new(vec![]));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "stmt",
            &[link_neither],
            Some(1), // total says we're at the last chunk
            0,
            tokio::runtime::Handle::current(),
        );
        assert!(
            !fetcher.silent_eos_warned.load(Ordering::Acquire),
            "explicit-EOS via total must NOT trip silent-EOS warning",
        );
    }

    #[tokio::test]
    async fn test_warn_helpers_are_sticky_one_shot() {
        // Every `warn_*` helper uses `swap(true)` so subsequent
        // calls are no-ops. Verify this contract for each of the
        // four new warnings; otherwise a chatty server would spam
        // production logs.
        let client = Arc::new(MockDatabricksClient::new(vec![]));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "stmt",
            &[],
            None,
            0,
            tokio::runtime::Handle::current(),
        );

        // Each warn helper takes the swap-true path on first
        // call (returns prior false), no-op afterwards. We
        // observe the side effect via the AtomicBool field.
        for _ in 0..3 {
            fetcher.warn_silent_eos("test");
            fetcher.warn_non_monotonic_clamp("test", 3, 6, 5);
            fetcher.warn_runaway_prefetch("test", 100_000, 10_000);
            fetcher.warn_empty_with_has_more("test", 0, 1);
        }

        assert!(fetcher.silent_eos_warned.load(Ordering::Acquire));
        assert!(fetcher.non_monotonic_warned.load(Ordering::Acquire));
        assert!(fetcher.runaway_prefetch_warned.load(Ordering::Acquire));
        assert!(fetcher.empty_with_has_more_warned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_runaway_prefetch_ceiling_uses_hint_or_floor() {
        // Ceiling = max(10 × total_chunk_count_hint, 10_000).
        // A small known total still gives us the 10k floor;
        // a large known total gives us 10×.
        let client = Arc::new(MockDatabricksClient::new(vec![]));

        let with_small_total = SeaChunkLinkFetcher::new(
            Arc::clone(&client) as Arc<dyn Transport>,
            "stmt",
            &[],
            Some(50),
            0,
            tokio::runtime::Handle::current(),
        );
        // 10 × 50 = 500, but floor = 10_000 wins
        assert_eq!(with_small_total.runaway_prefetch_ceiling(), 10_000);

        let with_large_total = SeaChunkLinkFetcher::new(
            Arc::clone(&client) as Arc<dyn Transport>,
            "stmt",
            &[],
            Some(5_000),
            0,
            tokio::runtime::Handle::current(),
        );
        // 10 × 5_000 = 50_000 beats the floor
        assert_eq!(with_large_total.runaway_prefetch_ceiling(), 50_000);

        let without_total = SeaChunkLinkFetcher::new(
            client as Arc<dyn Transport>,
            "stmt",
            &[],
            None,
            0,
            tokio::runtime::Handle::current(),
        );
        // No hint → hard floor only
        assert_eq!(without_total.runaway_prefetch_ceiling(), 10_000);
    }

    #[tokio::test]
    async fn test_apply_continuation_uses_fetch_max_for_row_offset() {
        // G2: `next_row_offset` must use `fetch_max`, not `store`,
        // so a late sync-fallback finishing after a faster
        // prefetch can't regress the cursor.
        let client = Arc::new(MockDatabricksClient::new(vec![]));
        let fetcher = SeaChunkLinkFetcher::new(
            client,
            "stmt",
            &[],
            None,
            0,
            tokio::runtime::Handle::current(),
        );

        // Pre-seed the cursor to a high value (simulates a fast
        // prefetch that already ran).
        fetcher.next_row_offset.store(5_000, Ordering::Release);

        // Now have `apply_continuation` see a link with a *lower*
        // row_offset_after (simulates a late sync-fallback).
        let late_link = CloudFetchLink {
            chunk_index: 1,
            row_offset: 1_000,
            row_count: 1_000,
            ..create_test_link(1)
        };
        let _ = fetcher.apply_continuation(&late_link, "test");

        // Cursor MUST stay at 5_000 — `fetch_max` keeps the
        // higher value.
        assert_eq!(
            fetcher.next_row_offset.load(Ordering::Acquire),
            5_000,
            "late sync-fallback must not regress next_row_offset (fetch_max contract)",
        );
    }
}
