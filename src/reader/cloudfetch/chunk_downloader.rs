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

//! ChunkDownloader for downloading Arrow data from cloud storage.
//!
//! This module handles downloading chunk data from presigned URLs,
//! including HTTP header handling, speed monitoring, decompression,
//! and Arrow IPC parsing.

use crate::client::retry::RequestType;
use crate::client::DatabricksHttpClient;
use crate::kernel_error::{Error, Result};
use crate::klog::{debug, warn};
use crate::reader::cloudfetch::arrow_parser::parse_arrow_ipc;
use crate::types::cloudfetch::CloudFetchLink;
use crate::types::sea::CompressionCodec;
use arrow_array::RecordBatch;
use reqwest::Method;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) struct PreparedDownload {
    response: reqwest::Response,
    link: CloudFetchLink,
    started: Instant,
    headers_elapsed: Duration,
    timeout: Duration,
}

impl PreparedDownload {
    pub(crate) fn matches(&self, link: &CloudFetchLink) -> bool {
        self.link.url == link.url
            && self.link.http_headers == link.http_headers
            && self.started.elapsed() < self.timeout
    }
}

/// Downloads Arrow data from cloud storage presigned URLs.
///
/// Handles:
/// - HTTP GET to presigned URL with custom headers
/// - LZ4 decompression (if compression enabled)
/// - Arrow IPC stream parsing into RecordBatches
/// - Download speed monitoring
#[derive(Debug)]
pub struct ChunkDownloader {
    http_client: Arc<DatabricksHttpClient>,
    compression: CompressionCodec,
    speed_threshold_mbps: f64,
}

impl ChunkDownloader {
    /// Create a new chunk downloader.
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for making requests
    /// * `compression` - Compression codec used by the server
    /// * `speed_threshold_mbps` - Log warning if speed falls below this (MB/s)
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        compression: CompressionCodec,
        speed_threshold_mbps: f64,
    ) -> Self {
        Self {
            http_client,
            compression,
            speed_threshold_mbps,
        }
    }

    /// Download a chunk from the given link.
    ///
    /// # Arguments
    /// * `link` - CloudFetch link containing URL and headers
    /// * `attempt_timeout` - Optional hard cap for this single HTTP
    ///   attempt, applied via reqwest's per-request timeout. It bounds
    ///   the whole attempt — connect, waiting for response headers, and
    ///   consuming the response body — so a stall in any phase can't
    ///   exceed it. Because reqwest's per-request timeout *overrides* the
    ///   client-wide one, we apply `min(request_timeout, attempt_timeout)`
    ///   so the static per-request safety net is never loosened. `None`
    ///   falls back to the HTTP client's static `request_timeout`. The
    ///   caller ([`download_chunk_with_retry`](crate::reader::cloudfetch))
    ///   passes the remaining end-to-end chunk budget here so no attempt
    ///   outruns the deadline (audit H05).
    ///
    /// # Returns
    /// Parsed RecordBatches from the downloaded Arrow IPC data
    pub async fn download(
        &self,
        link: &CloudFetchLink,
        attempt_timeout: Option<Duration>,
    ) -> Result<Vec<RecordBatch>> {
        let prepared = self.prepare(link, attempt_timeout).await?;
        let elapsed = prepared.headers_elapsed;
        let start = Instant::now();
        let bytes = prepared.response.bytes().await.map_err(|error| {
            Error::io().message(format!("Failed to read download response: {}", error))
        })?;
        self.decode(link, &bytes, elapsed + start.elapsed())
    }

    pub(crate) async fn prepare(
        &self,
        link: &CloudFetchLink,
        attempt_timeout: Option<Duration>,
    ) -> Result<PreparedDownload> {
        let start = Instant::now();

        debug!(
            "Downloading chunk {} ({} bytes expected, attempt_timeout {:?})",
            link.chunk_index, link.byte_count, attempt_timeout
        );

        // Build request with custom headers from the link
        let mut request_builder = self.http_client.inner().request(Method::GET, &link.url);

        for (key, value) in &link.http_headers {
            request_builder = request_builder.header(key, value);
        }

        // Cap this single attempt at the remaining chunk budget. reqwest's
        // per-request timeout spans connect → headers → body, so a stall at
        // any phase is bounded rather than the static request_timeout. It
        // *overrides* (does not compose with) the client-wide `.timeout()`,
        // so take `min(request_timeout, remaining)`: the remaining budget is
        // the tighter bound with today's defaults, but the min preserves the
        // static per-request safety net if `download_timeout` is ever
        // configured larger than `request_timeout`.
        if let Some(timeout) = attempt_timeout {
            let request_timeout = self.http_client.config().read_timeout;
            request_builder = request_builder.timeout(timeout.min(request_timeout));
        }

        let request = request_builder
            .build()
            .map_err(|e| Error::io().message(format!("Failed to build download request: {}", e)))?;

        // Execute without auth (presigned URLs have their own auth)
        let response = self
            .http_client
            .execute_without_auth(request, RequestType::CloudFetchDownload)
            .await?;

        Ok(PreparedDownload {
            response,
            link: link.clone(),
            started: start,
            headers_elapsed: start.elapsed(),
            timeout: attempt_timeout
                .unwrap_or(self.http_client.config().read_timeout)
                .min(self.http_client.config().read_timeout),
        })
    }

    pub(crate) async fn download_prepared(
        &self,
        link: &CloudFetchLink,
        prepared: PreparedDownload,
        attempt_timeout: Option<Duration>,
    ) -> Result<(Vec<RecordBatch>, Duration)> {
        let headers_elapsed = prepared.headers_elapsed;
        let start = Instant::now();
        match prepared.response.bytes().await {
            Ok(bytes) => self
                .decode(link, &bytes, headers_elapsed + start.elapsed())
                .map(|batches| (batches, headers_elapsed)),
            Err(_) => self
                .download(
                    link,
                    attempt_timeout.map(|timeout| timeout.saturating_sub(start.elapsed())),
                )
                .await
                .map(|batches| (batches, Duration::ZERO)),
        }
    }

    fn decode(
        &self,
        link: &CloudFetchLink,
        bytes: &[u8],
        elapsed: Duration,
    ) -> Result<Vec<RecordBatch>> {
        let size_mb = bytes.len() as f64 / 1024.0 / 1024.0;
        let speed_mbps = size_mb / elapsed.as_secs_f64();

        debug!(
            "Downloaded chunk {}: {:.2} MB in {:.2}s ({:.2} MB/s)",
            link.chunk_index,
            size_mb,
            elapsed.as_secs_f64(),
            speed_mbps
        );

        if speed_mbps < self.speed_threshold_mbps {
            warn!(
                "CloudFetch download slower than threshold: {:.2} MB/s (threshold: {:.2} MB/s)",
                speed_mbps, self.speed_threshold_mbps
            );
        }

        // Parse Arrow IPC data
        let batches = parse_arrow_ipc(bytes, self.compression)?;

        debug!(
            "Parsed chunk {}: {} batches, {} total rows",
            link.chunk_index,
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        // Trim the decoded chunk down to the link's declared row_count.
        //
        // Under a `row_limit=N` query the SEA server can hand back a
        // chunk that decodes to more rows than the external link's
        // declared `row_count` (a +1 server look-ahead row, or more).
        // Mirror the Python SEA driver's
        // `CloudFetchQueue._create_table_at_offset` (utils.py), which
        // slices each downloaded chunk to its own external-link
        // `row_count`. The inline-Arrow path clamps via the manifest's
        // `total_row_count`; this is the CloudFetch-side equivalent so
        // both transports return exactly N rows.
        let batches = Self::trim_to_row_count(batches, link.row_count);

        Ok(batches)
    }

    /// Trim a decoded chunk's batches so their total row count does not
    /// exceed `row_count` (the external link's declared row count).
    ///
    /// Mirrors Python's per-chunk `arrow_table.slice(0, row_count)`:
    /// only ever shrinks, never expands. A negative `row_count` (server
    /// never sends this for a real link) is treated as "no trim". When
    /// the decoded data already fits within `row_count` the batches are
    /// returned untouched. Otherwise batches are kept whole until the
    /// budget is reached and the boundary batch is sliced; any trailing
    /// batches beyond the budget are dropped.
    fn trim_to_row_count(batches: Vec<RecordBatch>, row_count: i64) -> Vec<RecordBatch> {
        if row_count < 0 {
            return batches;
        }
        let limit = row_count as usize;

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total <= limit {
            return batches;
        }

        let mut trimmed = Vec::with_capacity(batches.len());
        let mut remaining = limit;
        for batch in batches {
            if remaining == 0 {
                break;
            }
            let rows = batch.num_rows();
            if rows <= remaining {
                remaining -= rows;
                trimmed.push(batch);
            } else {
                // Slice the boundary batch down to the remaining budget.
                trimmed.push(batch.slice(0, remaining));
                remaining = 0;
            }
        }

        debug!(
            "Trimmed CloudFetch chunk from {} to {} rows (link row_count={})",
            total, limit, row_count
        );

        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::HttpClientConfig;
    use std::collections::HashMap;

    #[allow(dead_code)]
    fn create_test_link() -> CloudFetchLink {
        CloudFetchLink {
            url: "https://storage.example.com/chunk0".to_string(),
            chunk_index: 0,
            row_offset: 0,
            row_count: 1000,
            byte_count: 50000,
            expiration: chrono::Utc::now() + chrono::Duration::hours(1),
            http_headers: HashMap::from([("x-custom".to_string(), "value".to_string())]),
            next_chunk_index: Some(1),
        }
    }

    #[test]
    fn test_chunk_downloader_creation() {
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).unwrap(),
        );
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        http_client.set_auth_provider(auth);
        let downloader = ChunkDownloader::new(http_client, CompressionCodec::None, 0.1);

        assert_eq!(downloader.speed_threshold_mbps, 0.1);
    }

    // Note: Full download tests require mocking the HTTP layer
    // Integration tests should be added in a separate test module

    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    /// Build a single-column Int32 RecordBatch with `num_rows` rows.
    fn make_batch(start: i32, num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let ids: Vec<i32> = (start..start + num_rows as i32).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[test]
    fn test_trim_to_row_count_single_batch_overrun() {
        // A chunk that decoded to N+1 rows but the link only declares N.
        // Mirrors the SEA `row_limit` look-ahead overrun.
        let batch = make_batch(0, 101);
        let trimmed = ChunkDownloader::trim_to_row_count(vec![batch], 100);
        assert_eq!(total_rows(&trimmed), 100);
        assert_eq!(trimmed.len(), 1);
        assert_eq!(trimmed[0].num_rows(), 100);
    }

    #[test]
    fn test_trim_to_row_count_multiple_batches_trims_across_chunk() {
        // Three batches (40 + 40 + 40 = 120 rows) but link declares 100.
        // Expect the first two batches whole and the third sliced to 20.
        let batches = vec![make_batch(0, 40), make_batch(40, 40), make_batch(80, 40)];
        let trimmed = ChunkDownloader::trim_to_row_count(batches, 100);
        assert_eq!(total_rows(&trimmed), 100);
        assert_eq!(trimmed.len(), 3);
        assert_eq!(trimmed[0].num_rows(), 40);
        assert_eq!(trimmed[1].num_rows(), 40);
        assert_eq!(trimmed[2].num_rows(), 20);
    }

    #[test]
    fn test_trim_to_row_count_drops_trailing_batches() {
        // Budget is exhausted within the first batch; the trailing batch
        // must be dropped entirely, not expanded.
        let batches = vec![make_batch(0, 100), make_batch(100, 50)];
        let trimmed = ChunkDownloader::trim_to_row_count(batches, 100);
        assert_eq!(total_rows(&trimmed), 100);
        assert_eq!(trimmed.len(), 1);
    }

    #[test]
    fn test_trim_to_row_count_no_overrun_passthrough() {
        // Decoded rows fit within the declared row_count → never expand,
        // batches returned untouched.
        let batches = vec![make_batch(0, 30), make_batch(30, 20)];
        let trimmed = ChunkDownloader::trim_to_row_count(batches, 100);
        assert_eq!(total_rows(&trimmed), 50);
        assert_eq!(trimmed.len(), 2);
    }
}
