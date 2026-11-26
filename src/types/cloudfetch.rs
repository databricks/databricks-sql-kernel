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

//! CloudFetch-specific types for streaming downloads.
//!
//! These types are used by `StreamingCloudFetchProvider` and related components
//! for downloading Arrow data from cloud storage via presigned URLs.

use crate::kernel_error::{Error, Result};
use crate::types::sea::ExternalLink;
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::time::Duration;

/// Safety buffer (in seconds) before link expiration.
/// Links are considered expired this many seconds before their actual expiration time
/// to avoid race conditions during download.
pub const LINK_EXPIRY_BUFFER_SECS: i64 = 30;

/// Default timeout (in seconds) for waiting for a chunk to be ready.
/// Used as a fallback when `chunk_ready_timeout` is not configured.
pub const DEFAULT_CHUNK_READY_TIMEOUT_SECS: u64 = 30;

/// Configuration for CloudFetch streaming.
#[derive(Debug, Clone)]
pub struct CloudFetchConfig {
    /// Extra header-only downloads ahead of the decoded window; 0 disables.
    pub download_ahead: usize,
    /// Number of chunk links to prefetch ahead of consumption.
    pub link_prefetch_window: usize,
    /// Maximum number of chunks to hold in memory (controls parallelism).
    pub max_chunks_in_memory: usize,
    /// Maximum number of retry attempts for failed downloads.
    pub max_retries: u32,
    /// Delay between retry attempts.
    pub retry_delay: Duration,
    /// Absolute end-to-end wall-clock budget for downloading a single
    /// chunk, spanning every retry, the backoff sleeps between them,
    /// and each individual HTTP attempt (connect, response headers, and
    /// response body).
    ///
    /// Enforced as a hard deadline in
    /// `StreamingCloudFetchProvider::download_chunk_with_retry`: the
    /// remaining budget caps each HTTP attempt (via reqwest's
    /// per-request timeout) so a stalled connection, stalled headers, or
    /// stalled body can never exceed it, and the loop stops retrying
    /// once the deadline passes — returning a stable timeout error
    /// promptly rather than blocking the consumer for minutes.
    ///
    /// Without this bound the per-attempt HTTP timeout (`request_timeout`,
    /// default 120s) multiplied by `max_retries` let a permanently-stuck
    /// chunk block for ~10 minutes on a nominal budget (audit H05).
    pub download_timeout: Duration,
    /// Timeout for waiting for a chunk to be ready.
    pub chunk_ready_timeout: Option<Duration>,
    /// Log warning if download speed falls below this threshold (MB/s).
    pub speed_threshold_mbps: f64,
    /// Whether CloudFetch is enabled.
    pub enabled: bool,
    /// Target number of rows per merged batch. 0 = disabled (pass through as-is).
    /// When > 0, consecutive small batches are concatenated into larger batches
    /// of approximately this many rows before being served to consumers.
    pub batch_merge_target_rows: usize,
    /// Rolling-window bound for multi-chunk inline Arrow results. Analogue of
    /// [`Self::max_chunks_in_memory`] but for sequential inline
    /// prefetches rather than parallel cloud downloads.
    pub inline_max_chunks_in_memory: usize,
}

impl Default for CloudFetchConfig {
    fn default() -> Self {
        Self {
            download_ahead: 0,
            // Match JDBC default: prefetch 128 chunks ahead of consumer
            link_prefetch_window: 128,
            // Match JDBC default: cloudFetchThreadPoolSize = 16
            max_chunks_in_memory: 16,
            max_retries: 5,
            retry_delay: Duration::from_millis(1500),
            // End-to-end per-chunk download budget. Matches the CloudFetch
            // HTTP-retry `overall_timeout` set in
            // `session::build_retry_configs_for_session`, so the provider
            // retry loop and the inner HTTP client share one budget instead
            // of stacking multiplicatively (audit H05). `session` re-projects
            // this from the same source so the two can't drift.
            download_timeout: Duration::from_secs(20),
            chunk_ready_timeout: Some(Duration::from_secs(30)),
            speed_threshold_mbps: 0.1,
            enabled: true,
            batch_merge_target_rows: 0,
            inline_max_chunks_in_memory: 4,
        }
    }
}

/// Parsed external link with validated expiration timestamp.
///
/// This is the internal representation of a CloudFetch link, converted from
/// the SEA API's `ExternalLink` with the expiration parsed into a proper
/// `DateTime<Utc>`.
#[derive(Debug, Clone)]
pub struct CloudFetchLink {
    /// Pre-signed URL for downloading the chunk.
    pub url: String,
    /// Index of this chunk in the result set.
    pub chunk_index: i64,
    /// Row offset of this chunk in the result set.
    pub row_offset: i64,
    /// Number of rows in this chunk.
    pub row_count: i64,
    /// Size of this chunk in bytes (compressed if applicable).
    pub byte_count: i64,
    /// When this link expires.
    pub expiration: DateTime<Utc>,
    /// Optional HTTP headers to include in the download request.
    pub http_headers: HashMap<String, String>,
    /// Index of the next chunk, if there are more.
    pub next_chunk_index: Option<i64>,
}

impl CloudFetchLink {
    /// Check if link is expired (with safety buffer).
    ///
    /// Uses `LINK_EXPIRY_BUFFER_SECS` as a safety margin to avoid race conditions
    /// where a link expires during download.
    pub fn is_expired(&self) -> bool {
        Utc::now() + chrono::Duration::seconds(LINK_EXPIRY_BUFFER_SECS) >= self.expiration
    }

    /// Row offset of the next chunk after this one (i.e. the
    /// cursor where the next page of results should start).
    ///
    /// Computed as `row_offset + row_count` with `saturating_add`
    /// so a pathological server response can't cause arithmetic
    /// overflow when the kernel uses this to advance its
    /// continuation cursor.
    pub fn row_offset_after(&self) -> i64 {
        self.row_offset.saturating_add(self.row_count)
    }

    /// Convert from SEA API response type.
    pub fn from_external_link(link: &ExternalLink) -> Result<Self> {
        let expiration = DateTime::parse_from_rfc3339(&link.expiration)
            .map_err(|e| {
                Error::invalid_state().message(format!("Invalid expiration timestamp: {}", e))
            })?
            .with_timezone(&Utc);

        Ok(Self {
            url: link.external_link.clone(),
            chunk_index: link.chunk_index,
            row_offset: link.row_offset,
            row_count: link.row_count,
            byte_count: link.byte_count,
            expiration,
            http_headers: link.http_headers.clone().unwrap_or_default(),
            next_chunk_index: link.next_chunk_index,
        })
    }
}

/// State of a chunk in the download pipeline.
///
/// State transitions:
/// ```text
///   Pending -> UrlFetched (link available)
///   UrlFetched -> Downloading (download started)
///   Downloading -> Downloaded (success)
///   Downloading -> DownloadFailed (error)
///   DownloadFailed -> DownloadRetry (retrying)
///   DownloadRetry -> Downloading (retry started)
///   Downloaded -> ProcessingFailed (Arrow parse error)
///   Downloaded -> Released (consumed by client)
///   * -> Cancelled (user cancellation)
/// ```
#[derive(Debug, Clone)]
pub enum ChunkState {
    /// Initial state, no link yet.
    Pending,
    /// Link available, not yet downloading.
    UrlFetched,
    /// Download in progress.
    Downloading,
    /// Downloaded successfully.
    Downloaded,
    /// Download failed (will retry). Carries the full [`Error`] —
    /// not a stringified message — so the terminal classification
    /// (`ErrorCode`, `retryable`, cause chain) survives to the
    /// consumer path (`wait_for_chunk`). Stringifying here would
    /// collapse an `ErrorCode::Timeout` deadline error into an
    /// opaque `ErrorCode::Internal` when the consumer re-wrapped it
    /// (audit H05).
    DownloadFailed(Error),
    /// Retrying after failure.
    DownloadRetry,
    /// Arrow parse failed (terminal).
    ProcessingFailed(String),
    /// Cancelled by user (terminal).
    Cancelled,
    /// Memory released (terminal).
    Released,
}

/// Entry for a chunk in the chunks map.
#[derive(Clone)]
pub struct ChunkEntry {
    /// Link for this chunk (set when state is UrlFetched or later).
    pub link: Option<CloudFetchLink>,
    /// Current state of this chunk.
    pub state: ChunkState,
    /// Parsed record batches (populated when state is Downloaded).
    pub batches: Option<Vec<RecordBatch>>,
    /// Most recent failure reason, preserved across state
    /// transitions that erase it from `ChunkState` itself.
    ///
    /// `ChunkState::DownloadFailed(reason)` carries the reason
    /// while it's set, but the retry loop transitions
    /// `DownloadFailed → DownloadRetry` (where the reason no
    /// longer lives in the state). Without this field an operator
    /// can't correlate the chunk's failure history during the
    /// retry-sleep window. Last writer wins — only the most
    /// recent reason is preserved.
    pub last_failure_reason: Option<String>,
}

impl ChunkEntry {
    /// Create a new pending entry with no link.
    pub fn pending() -> Self {
        Self {
            link: None,
            state: ChunkState::Pending,
            batches: None,
            last_failure_reason: None,
        }
    }

    /// Create a new entry with a fetched link.
    pub fn with_link(link: CloudFetchLink) -> Self {
        Self {
            link: Some(link),
            state: ChunkState::UrlFetched,
            batches: None,
            last_failure_reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloudfetch_config_default() {
        let config = CloudFetchConfig::default();
        assert_eq!(config.link_prefetch_window, 128); // Matches JDBC default
        assert_eq!(config.max_chunks_in_memory, 16); // Matches JDBC cloudFetchThreadPoolSize
        assert_eq!(config.max_retries, 5);
        assert!(config.enabled);
        assert_eq!(config.batch_merge_target_rows, 0); // Disabled by default
                                                       // End-to-end per-chunk download budget (audit H05). Matches the
                                                       // CloudFetch HTTP-retry overall_timeout so the two layers can't drift.
        assert_eq!(config.download_timeout, Duration::from_secs(20));
    }

    #[test]
    fn test_cloudfetch_link_from_external_link() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "2099-01-01T12:00:00Z".to_string(),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: Some(HashMap::from([(
                "x-custom".to_string(),
                "value".to_string(),
            )])),
            next_chunk_index: Some(1),
        };

        let link = CloudFetchLink::from_external_link(&external).unwrap();
        assert_eq!(link.chunk_index, 0);
        assert_eq!(link.next_chunk_index, Some(1));
        assert_eq!(
            link.http_headers.get("x-custom"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn test_cloudfetch_link_invalid_expiration() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "not-a-valid-timestamp".to_string(),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: None,
            next_chunk_index: None,
        };

        let result = CloudFetchLink::from_external_link(&external);
        assert!(result.is_err());
    }

    #[test]
    fn test_cloudfetch_link_is_expired() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "2000-01-01T12:00:00Z".to_string(), // Way in the past
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: None,
            next_chunk_index: None,
        };

        let link = CloudFetchLink::from_external_link(&external).unwrap();
        assert!(link.is_expired());
    }

    #[test]
    fn test_cloudfetch_link_not_expired() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "2099-01-01T12:00:00Z".to_string(), // Way in the future
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: None,
            next_chunk_index: None,
        };

        let link = CloudFetchLink::from_external_link(&external).unwrap();
        assert!(!link.is_expired());
    }

    #[test]
    fn test_chunk_entry_pending() {
        let entry = ChunkEntry::pending();
        assert!(entry.link.is_none());
        assert!(entry.batches.is_none());
        assert!(matches!(entry.state, ChunkState::Pending));
    }

    #[test]
    fn test_chunk_entry_with_link() {
        let external = ExternalLink {
            external_link: "https://storage.example.com/chunk0".to_string(),
            expiration: "2099-01-01T12:00:00Z".to_string(),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            http_headers: None,
            next_chunk_index: None,
        };
        let link = CloudFetchLink::from_external_link(&external).unwrap();
        let entry = ChunkEntry::with_link(link);

        assert!(entry.link.is_some());
        assert!(entry.batches.is_none());
        assert!(matches!(entry.state, ChunkState::UrlFetched));
    }
}
