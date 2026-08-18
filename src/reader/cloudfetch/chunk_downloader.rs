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
    download_timeout: Duration,
}

impl ChunkDownloader {
    /// Create a new chunk downloader.
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for making requests
    /// * `compression` - Compression codec used by the server
    /// * `speed_threshold_mbps` - Log warning if speed falls below this (MB/s)
    /// * `download_timeout` - Per-chunk download deadline (HTTP GET + body
    ///   read); a stall that exceeds it fails with a retriable timeout rather
    ///   than hanging.
    pub fn new(
        http_client: Arc<DatabricksHttpClient>,
        compression: CompressionCodec,
        speed_threshold_mbps: f64,
        download_timeout: Duration,
    ) -> Self {
        Self {
            http_client,
            compression,
            speed_threshold_mbps,
            download_timeout,
        }
    }

    /// Download a chunk from the given link.
    ///
    /// # Arguments
    /// * `link` - CloudFetch link containing URL and headers
    ///
    /// # Returns
    /// Parsed RecordBatches from the downloaded Arrow IPC data
    pub async fn download(&self, link: &CloudFetchLink) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();

        debug!(
            "Downloading chunk {} from {} ({} bytes expected)",
            link.chunk_index, link.url, link.byte_count
        );

        // Build request with custom headers from the link
        let mut request_builder = self.http_client.inner().request(Method::GET, &link.url);

        for (key, value) in &link.http_headers {
            request_builder = request_builder.header(key, value);
        }

        let request = request_builder
            .build()
            .map_err(|e| Error::io().message(format!("Failed to build download request: {}", e)))?;

        // Bound the whole HTTP GET + body read with a per-chunk deadline so a
        // stalled cloud-storage response fails with a retriable timeout rather
        // than hanging the result stream. reqwest's client-level timeout only
        // covers `execute()` (the header phase); the separately-awaited
        // `response.bytes()` below is where a wedged blob read would otherwise
        // block indefinitely (the reyden nightly hang). Mirrors the ADBC C#
        // driver's per-download deadline.
        let download_fut = async {
            // Execute without auth (presigned URLs have their own auth)
            let response = self
                .http_client
                .execute_without_auth(request, RequestType::CloudFetchDownload)
                .await?;

            // Read response body
            response.bytes().await.map_err(|e| {
                Error::io().message(format!("Failed to read download response: {}", e))
            })
        };

        let bytes = match tokio::time::timeout(self.download_timeout, download_fut).await {
            Ok(result) => result?,
            Err(_elapsed) => {
                return Err(Error::timeout().message(format!(
                    "CloudFetch chunk {} download exceeded {:?} timeout (url {})",
                    link.chunk_index, self.download_timeout, link.url
                )));
            }
        };

        let elapsed = start.elapsed();
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
        let batches = parse_arrow_ipc(&bytes, self.compression)?;

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
        let downloader = ChunkDownloader::new(
            http_client,
            CompressionCodec::None,
            0.1,
            Duration::from_secs(300),
        );

        assert_eq!(downloader.speed_threshold_mbps, 0.1);
    }

    /// A stalled cloud-storage response must fail with a timeout within the
    /// configured per-chunk `download_timeout`, not hang. Regression for the
    /// reyden nightly where a fault-injected CloudFetch stall wedged
    /// `response.bytes()` for ~52 min until the CI job cap killed it.
    #[tokio::test]
    async fn download_times_out_when_response_stalls() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // Respond far later than the downloader's per-chunk timeout,
        // simulating a wedged blob read.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/chunk0"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(10)))
            .mount(&server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).unwrap(),
        );
        http_client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token".to_string())));

        // Tiny per-chunk timeout so the test resolves quickly.
        let downloader = ChunkDownloader::new(
            http_client,
            CompressionCodec::None,
            0.1,
            Duration::from_millis(200),
        );

        let link = CloudFetchLink {
            url: format!("{}/chunk0", server.uri()),
            ..create_test_link()
        };

        let start = Instant::now();
        let result = downloader.download(&link).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "a stalled download must error, not hang");
        assert!(
            elapsed < Duration::from_secs(3),
            "download must give up near its 200ms timeout, took {:?}",
            elapsed
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("timeout")
                || err.to_string().to_lowercase().contains("timed out"),
            "error should indicate a timeout, got: {err}"
        );
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
