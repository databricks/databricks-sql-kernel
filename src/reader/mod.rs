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

//! Result readers for fetching query results from Databricks.
//!
//! This module provides:
//! - `ResultReaderFactory`: Creates appropriate reader based on response type
//! - `CloudFetchReader`: Streams Arrow data via cloud storage downloads
//! - `InlineArrowProvider`: Handles inline Arrow data embedded in responses

pub mod cloudfetch;
pub mod inline;
pub mod post_processor;
#[cfg(test)]
pub(crate) mod test_utils;

use crate::client::{DatabricksHttpClient, ExecuteResponse, Transport};
use crate::kernel_error::{Error, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::{SeaChunkLinkFetcher, SeaChunkLinkFetcherHandle};
use crate::reader::cloudfetch::streaming_provider::StreamingCloudFetchProvider;
use crate::reader::inline::{InlineArrowProvider, InlineStreamingProvider};
use crate::reader::post_processor::{PostProcessOptions, ResultPostProcessor};
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use crate::types::sea::{CompressionCodec, ResultManifest, StatementState};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::sync::Arc;

pub use cloudfetch::StreamingCloudFetchProvider as CloudFetchReader;

/// Factory that creates the appropriate reader based on the response type.
///
/// Encapsulates the decision of CloudFetch vs inline vs other result formats.
/// Uses `Transport` trait for backend flexibility - works with SeaRestTransport
/// or any future backend implementation.
#[derive(Debug)]
pub(crate) struct ResultReaderFactory {
    client: Arc<dyn Transport>,
    http_client: Arc<DatabricksHttpClient>,
    config: CloudFetchConfig,
    post_process_options: PostProcessOptions,
    runtime_handle: tokio::runtime::Handle,
}

impl ResultReaderFactory {
    /// Create a new result reader factory.
    pub(crate) fn new(
        client: Arc<dyn Transport>,
        http_client: Arc<DatabricksHttpClient>,
        config: CloudFetchConfig,
        post_process_options: PostProcessOptions,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            client,
            http_client,
            config,
            post_process_options,
            runtime_handle,
        }
    }

    /// Create a reader from a statement execution response.
    ///
    /// Automatically selects CloudFetch, inline, or other reader based
    /// on response. The returned reader is wrapped in a
    /// [`ResultPostProcessor`] that owns all per-batch post-processing
    /// the kernel does on the way out to consumers:
    ///
    /// - manifest-driven metadata attachment (`databricks.type_name`,
    ///   `databricks.type_text`, precision / scale / interval kind);
    /// - optional complex-type JSON stringification when the session
    ///   has `complex_types_as_json = true`.
    ///
    /// The post-processor is a single insertion point — every caller
    /// of `create_reader` benefits automatically.
    ///
    /// There is one other augmentation site for Arrow-FFI consumers:
    /// [`ResultReaderAdapter::new`] also applies the same
    /// [`augment_schema_with_manifest`](ResultReaderAdapter::augment_schema_with_manifest)
    /// to the wrapped reader's schema. Both sites share the same helper,
    /// so the metadata-attachment behaviour is consistent across the
    /// two surfaces.
    pub(crate) async fn create_reader(
        &self,
        statement_id: &str,
        response: &ExecuteResponse,
    ) -> Result<Box<dyn ResultReader + Send>> {
        let inner = self.create_inner_reader(statement_id, response)?;
        Ok(Box::new(
            ResultPostProcessor::new(
                inner,
                response.manifest.as_ref(),
                self.post_process_options.clone(),
            )
            .await?,
        ))
    }

    /// Pick the right concrete reader for `response`: CloudFetch,
    /// inline Arrow, or a typed empty stream. Split from
    /// [`create_reader`] so the manifest-augmentation wrap lives at
    /// exactly one site.
    fn create_inner_reader(
        &self,
        statement_id: &str,
        response: &ExecuteResponse,
    ) -> Result<Box<dyn ResultReader + Send>> {
        // Log manifest info for debugging
        if let Some(ref manifest) = response.manifest {
            crate::klog::debug!(
                statement_id,
                "Result manifest: total_chunks={:?}, total_rows={:?}, total_bytes={:?}, truncated={}",
                manifest.total_chunk_count,
                manifest.total_row_count,
                manifest.total_byte_count,
                manifest.truncated
            );
        }

        // Determine compression from manifest
        let compression = response
            .manifest
            .as_ref()
            .and_then(|m| m.result_compression.as_deref())
            .map(|s| CompressionCodec::from_manifest(Some(s)))
            .unwrap_or_default();

        // Manifest-authoritative empty-result short-circuit.
        //
        // When the SEA manifest reports `total_chunk_count = 0`, the
        // result set is empty regardless of whatever `result` payload
        // the server may have attached (external_links, inline-Arrow
        // attachment, or internal-link). Returning an `EmptyReader`
        // built from the manifest schema avoids walking the chunk-
        // fetch path for a result we know has no rows, and mirrors
        // the JDBC driver's manifest-first empty-result handling.
        //
        // Only fires when the field is explicitly `Some(0)`. Absent
        // or non-zero values fall through to the existing branch
        // dispatch — the manifest field is deprecated on newer
        // servers and we must not infer "empty" from its absence.
        //
        // Caller contract: returning here before the `status.state`
        // match arms below is safe only because every caller of
        // `create_inner_reader` has already verified a terminal-success
        // outcome (`StatementOutcome::Done` in `src/client/sea.rs`).
        // A future caller that invokes us on a Failed / Canceled /
        // Pending / Running state with `total_chunk_count == Some(0)`
        // would silently lose the error signal — re-gate here if that
        // ever changes.
        if matches!(
            response.manifest.as_ref().and_then(|m| m.total_chunk_count),
            Some(0)
        ) {
            crate::klog::info!(
                statement_id,
                "Manifest reports total_chunk_count=0; short-circuiting to empty reader",
            );
            let schema = Self::extract_schema_from_manifest(response.manifest.as_ref())?;
            return Ok(Box::new(EmptyReader::new(schema)));
        }

        // Check result data for CloudFetch or inline results
        if let Some(ref result) = response.result {
            // Log what result data we have for debugging the decision path
            crate::klog::debug!(
                statement_id,
                "Result data present: external_links={}, inline_arrow_data={} bytes, next_chunk_internal_link={}",
                result.external_links.as_ref().map(|l| l.len()).unwrap_or(0),
                result.inline_arrow_data.as_ref().map(|d| d.len()).unwrap_or(0),
                result.next_chunk_internal_link.is_some()
            );

            // Priority 1: CloudFetch (external_links present)
            if let Some(ref external_links) = result.external_links {
                if !external_links.is_empty() {
                    // `total_chunk_count` is forwarded purely as a
                    // transition-period guard. The link fetcher uses
                    // it only when the server hasn't yet rolled out
                    // `external_links[].next_chunk_index` (the
                    // bounded SEA API authoritative signal). See
                    // `SeaChunkLinkFetcher::decode_continuation` for
                    // the bridge logic.
                    let total_chunk_count =
                        response.manifest.as_ref().and_then(|m| m.total_chunk_count);
                    crate::klog::info!(
                        statement_id,
                        "Using CloudFetch reader: {} initial external links, total_chunks={:?}",
                        external_links.len(),
                        total_chunk_count,
                    );

                    return self.create_cloudfetch_reader(
                        statement_id,
                        external_links,
                        total_chunk_count,
                        compression,
                    );
                }
            }

            // Priority 2: Inline Arrow (attachment present)
            if let Some(ref inline_data) = result.inline_arrow_data {
                if !inline_data.is_empty() {
                    // Multi-chunk inline-Arrow streaming path: kicks
                    // in when the server emitted a `next_chunk_index`
                    // alongside the initial inline attachment. The
                    // streaming provider holds a rolling window of
                    // pre-decoded chunks ahead of the consumer
                    // (bounded by `inline_max_chunks_in_memory`); each
                    // continuation `get_inline_chunk` carries both
                    // chunk_index (= prev `next_chunk_index`) and
                    // row_offset (= prev `row_offset + row_count`)
                    // per the bounded SEA API contract.
                    //
                    // Rollout-skew guard: a pre-rollout server may
                    // emit a multi-chunk inline-Arrow result without
                    // populating `next_chunk_index` on the
                    // attachment-bearing response. The manifest's
                    // (deprecated but still emitted) `total_chunk_count`
                    // distinguishes that case from a genuinely
                    // single-chunk result. When the per-link signal
                    // is absent but `total_chunk_count > 1`, we
                    // refuse to silently truncate — instead surface
                    // a clear error so the rollout asymmetry is
                    // visible. Mirrors the PR #68 transition-period
                    // fix on CloudFetch.
                    let total_chunks = response.manifest.as_ref().and_then(|m| m.total_chunk_count);
                    if result.inline_next_chunk_index.is_none() {
                        if let Some(total) = total_chunks {
                            if total > 1 {
                                return Err(Error::data_loss().message(format!(
                                    "Inline-Arrow response is missing per-chunk \
                                     next_chunk_index but the manifest reports \
                                     total_chunk_count={}. Likely cause: server has not \
                                     rolled out the bounded SEA API per-chunk \
                                     continuation field. Refusing to silently truncate \
                                     a multi-chunk result.",
                                    total,
                                )));
                            }
                        }
                    }
                    if let Some(next_idx) = result.inline_next_chunk_index {
                        // Strict: when the server has opted into the
                        // multi-chunk streaming path (signalled by
                        // emitting `next_chunk_index`), the per-chunk
                        // metadata MUST be present too. Defaulting
                        // `row_count` to 0 would silently corrupt the
                        // next-fetch row_offset (we compute
                        // `row_offset + row_count` for the
                        // continuation). Symmetric with the strict
                        // handling in `sea.rs::get_inline_chunk` for
                        // continuation responses.
                        let inline_chunk_index = result.inline_chunk_index.ok_or_else(|| {
                            Error::data_loss().message(
                                "Inline-Arrow initial response has \
                                 next_chunk_index but is missing chunk_index — \
                                 server contract violation",
                            )
                        })?;
                        let inline_row_offset = result.inline_row_offset.ok_or_else(|| {
                            Error::data_loss().message(
                                "Inline-Arrow initial response has \
                                 next_chunk_index but is missing row_offset — \
                                 server contract violation",
                            )
                        })?;
                        let inline_row_count = result.inline_row_count.ok_or_else(|| {
                            Error::data_loss().message(
                                "Inline-Arrow initial response has \
                                 next_chunk_index but is missing row_count — \
                                 cannot derive next-fetch row_offset safely",
                            )
                        })?;
                        crate::klog::info!(
                            statement_id,
                            "Using inline-Arrow streaming reader: {} bytes initial chunk, \
                             next_chunk_index={}",
                            inline_data.len(),
                            next_idx,
                        );
                        // Pass the manifest-derived schema as a
                        // fallback for the case where the initial
                        // chunk decodes to zero batches (paginated
                        // results sometimes ship an empty leading
                        // chunk). Without this, `schema()` would
                        // return `None` and downstream wrappers
                        // would reject the stream.
                        let manifest_schema = response.manifest.as_ref().and_then(|m| {
                            match Self::extract_schema_from_manifest(Some(m)) {
                                Ok(s) => Some(s),
                                Err(e) => {
                                    crate::klog::warn!(
                                        statement_id,
                                        "Failed to extract manifest schema for inline-Arrow \
                                             streaming fallback; proceeding without it: {e}"
                                    );
                                    None
                                }
                            }
                        });
                        return self.create_inline_streaming_reader(
                            statement_id,
                            inline_data.clone(),
                            inline_chunk_index,
                            inline_row_offset,
                            inline_row_count,
                            Some(next_idx),
                            compression,
                            manifest_schema,
                        );
                    }

                    // Single-chunk inline: legacy fast path, parses
                    // the whole result up front.
                    crate::klog::info!(
                        statement_id,
                        "Using inline Arrow reader: {} bytes of Arrow IPC data",
                        inline_data.len()
                    );

                    // Pass the manifest's authoritative row count so the
                    // inline provider clamps off any look-ahead row the SEA
                    // server appends when a row_limit truncates the result.
                    let total_row_count =
                        response.manifest.as_ref().and_then(|m| m.total_row_count);
                    return self.create_inline_reader(
                        statement_id,
                        inline_data,
                        compression,
                        total_row_count,
                    );
                }
            }

            // Internal links without external = CloudFetch not available
            if result.next_chunk_internal_link.is_some() {
                return Err(Error::not_implemented()
                    .message("CloudFetch not available. Internal links not supported."));
            }
        }

        // No result data - check if this is a valid empty result or an error state
        match response.status.state {
            StatementState::Succeeded | StatementState::Closed => {
                // Valid empty result set - extract schema from manifest
                // Note: Closed state is valid for inline results where the server delivers
                // the data (or empty result) and immediately closes the statement.
                crate::klog::info!(
                    statement_id,
                    "Using empty reader: no result data present for {:?} statement",
                    response.status.state
                );
                let schema = Self::extract_schema_from_manifest(response.manifest.as_ref())?;
                Ok(Box::new(EmptyReader::new(schema)))
            }
            StatementState::Pending | StatementState::Running => Err(Error::invalid_state()
                .message("Statement is still executing. Poll for completion first.")),
            StatementState::Failed => {
                let error_msg = response
                    .status
                    .error
                    .as_ref()
                    .and_then(|e| e.message.as_deref())
                    .unwrap_or("Unknown error");
                Err(Error::io().message(format!("Statement failed: {}", error_msg)))
            }
            StatementState::Canceled => Err(Error::io().message("Statement was canceled")),
        }
    }

    /// Extract Arrow schema from the result manifest.
    ///
    /// Falls back to an empty schema if no manifest is available.
    fn extract_schema_from_manifest(manifest: Option<&ResultManifest>) -> Result<SchemaRef> {
        let Some(manifest) = manifest else {
            crate::klog::warn!("No manifest available for empty result set, using empty schema");
            return Ok(Arc::new(Schema::empty()));
        };

        let fields: Vec<Field> = manifest
            .schema
            .columns
            .iter()
            .map(|col| {
                // Pass `type_text` (e.g. `"DECIMAL(10,2)"` or
                // `"INTERVAL YEAR TO MONTH"`) rather than `type_name`
                // (e.g. `"DECIMAL"` or `"INTERVAL"`) so the shared mapper
                // can recover precision/scale / interval qualifier. The
                // non-empty result path uses the Arrow schema embedded in
                // the wire payload (which already encodes these); only
                // this empty-result path has to reconstruct the Arrow type
                // from the manifest, and reconstructing without those
                // produces wrong defaults that diverge from the Thrift
                // backend's wire schema for the same query.
                //
                // `type_interval_type` (`"YEAR_MONTH"` / `"DAY_TIME"`) is
                // the canonical hint for INTERVAL columns; passing it
                // through lets the mapper choose
                // `IntervalUnit::YearMonth` vs `IntervalUnit::DayTime`
                // without re-parsing the `type_text`.
                let data_type =
                    crate::metadata::type_mapping::databricks_type_to_arrow_with_interval(
                        &col.type_text,
                        col.type_interval_type.as_deref(),
                    );
                Field::new(&col.name, data_type, true) // nullable by default
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    fn create_cloudfetch_reader(
        &self,
        statement_id: &str,
        external_links: &[CloudFetchLink],
        total_chunk_count: Option<i64>,
        compression: CompressionCodec,
    ) -> Result<Box<dyn ResultReader + Send>> {
        crate::klog::debug!(
            statement_id,
            "Creating CloudFetch reader: {} initial links, prefetch_window={}, total_chunks={:?}",
            external_links.len(),
            self.config.link_prefetch_window,
            total_chunk_count,
        );

        // Create link fetcher with caching and prefetching built-in.
        // End-of-stream is derived from per-link `next_chunk_index`
        // (the bounded SEA API authoritative signal). The
        // `total_chunk_count` parameter is forwarded purely as a
        // transition-period guard for servers that haven't rolled
        // out the per-link field yet — see
        // `SeaChunkLinkFetcher::decode_continuation`.
        let fetcher = SeaChunkLinkFetcher::new(
            self.client.clone(),
            statement_id.to_string(),
            external_links,
            total_chunk_count,
            self.config.link_prefetch_window as i64,
            self.runtime_handle.clone(),
        );
        let link_fetcher: Arc<dyn crate::reader::cloudfetch::ChunkLinkFetcher> =
            Arc::new(SeaChunkLinkFetcherHandle::new(fetcher));

        let chunk_downloader = Arc::new(ChunkDownloader::new(
            self.http_client.clone(),
            compression,
            self.config.speed_threshold_mbps,
        ));

        let provider = StreamingCloudFetchProvider::new(
            self.config.clone(),
            link_fetcher,
            chunk_downloader,
            self.runtime_handle.clone(),
        );

        Ok(Box::new(CloudFetchResultReader::new(provider)))
    }

    fn create_inline_reader(
        &self,
        statement_id: &str,
        inline_data: &[u8],
        compression: CompressionCodec,
        total_row_count: Option<i64>,
    ) -> Result<Box<dyn ResultReader + Send>> {
        crate::klog::debug!(
            statement_id,
            "Creating inline Arrow reader: {} bytes, compression={:?}, total_row_count={:?}",
            inline_data.len(),
            compression,
            total_row_count
        );

        let provider = InlineArrowProvider::new(
            statement_id,
            inline_data.to_vec(),
            compression,
            total_row_count,
        )?;

        Ok(Box::new(InlineArrowReader::new(provider)))
    }

    /// Build a multi-chunk inline-Arrow streaming reader.
    ///
    /// Wraps the [`InlineStreamingProvider`] in a [`ResultReader`]
    /// adapter so the rest of the kernel can treat the inline
    /// streaming path identically to CloudFetch.
    ///
    /// `manifest_schema` is forwarded to the provider as a fallback
    /// for the case where the initial chunk decodes to zero
    /// batches. See the comment at the call-site for context.
    #[allow(clippy::too_many_arguments)]
    fn create_inline_streaming_reader(
        &self,
        statement_id: &str,
        initial_attachment: Vec<u8>,
        initial_chunk_index: i64,
        initial_row_offset: i64,
        initial_row_count: i64,
        next_chunk_index: Option<i64>,
        compression: CompressionCodec,
        manifest_schema: Option<SchemaRef>,
    ) -> Result<Box<dyn ResultReader + Send>> {
        crate::klog::debug!(
            "Creating inline-Arrow streaming reader: stmt={}, initial_chunk={}, \
             next_chunk={:?}, max_in_memory={}",
            statement_id,
            initial_chunk_index,
            next_chunk_index,
            self.config.inline_max_chunks_in_memory,
        );

        // Note: `Arc::new(self.client.clone())` produces
        // `Arc<Arc<dyn Transport>>` because the blanket impl of
        // `InlineChunkSource` is on `Arc<dyn Transport>` (not on
        // `dyn Transport`), and trait-objects of different traits
        // don't share a vtable so direct coercion
        // `Arc<dyn Transport>` → `Arc<dyn InlineChunkSource>`
        // isn't legal. The outer `Arc::new` is the price of using a
        // narrow trait so test mocks don't have to stub the full
        // 20-method `Transport` surface; net cost is one extra
        // pointer + one extra deref per RPC, negligible vs the SEA
        // round-trip.
        let source: Arc<dyn crate::reader::inline::InlineChunkSource> =
            Arc::new(self.client.clone());
        let provider = InlineStreamingProvider::new(
            source,
            statement_id.to_string(),
            initial_attachment,
            initial_chunk_index,
            initial_row_offset,
            initial_row_count,
            next_chunk_index,
            compression,
            self.config.inline_max_chunks_in_memory,
            manifest_schema,
            self.runtime_handle.clone(),
        )?;

        Ok(Box::new(InlineStreamingReader::new(provider)))
    }
}

/// Trait for result readers.
///
/// Readers are `Send + Sync` so a `Box<dyn ResultReader>` can live
/// inside a `Send + !Sync` public handle (`ResultStream` /
/// `ExecutedStatement`) and still satisfy the `&self`-future-is-Send
/// expectation of `async_trait` methods on the surrounding
/// `ExecutedStatementHandle`. Practically every existing impl is
/// already Sync (their state is `Arc`s or owned `Vec`s).
///
/// Both methods are `async` so the CloudFetch implementation can
/// `.await` its underlying async provider directly — no internal
/// `block_on`. This is what makes the public `ResultStream::next_batch`
/// truly async and safe to call from a tokio runtime (e.g. napi-rs).
///
/// **External implementors**: this trait is rewritten by `async_trait`
/// into a `Pin<Box<dyn Future + Send>>` return shape. Any external
/// `impl ResultReader for MyType { async fn schema(&self) -> ... }`
/// must be annotated with `#[async_trait::async_trait]`, otherwise the
/// compiler will emit an opaque type-mismatch error. The same goes for
/// the bound on `Box<dyn ResultReader + Send>` containers — readers
/// constructed outside the kernel need to thread `async_trait` through
/// their definitions.
#[async_trait]
pub trait ResultReader: Send + Sync {
    /// Get the schema of the result.
    async fn schema(&self) -> Result<SchemaRef>;

    /// Get the next record batch, or None if end of results.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Wrapper for StreamingCloudFetchProvider that implements ResultReader.
struct CloudFetchResultReader {
    provider: Arc<StreamingCloudFetchProvider>,
}

impl CloudFetchResultReader {
    fn new(provider: Arc<StreamingCloudFetchProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ResultReader for CloudFetchResultReader {
    async fn schema(&self) -> Result<SchemaRef> {
        self.provider.get_schema().await
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.provider.next_batch().await
    }
}

/// Reader for inline Arrow results.
///
/// Wraps `InlineArrowProvider` to implement the `ResultReader` trait.
struct InlineArrowReader {
    provider: InlineArrowProvider,
    /// Cached schema (extracted from first batch on creation)
    schema: Option<SchemaRef>,
}

impl InlineArrowReader {
    fn new(provider: InlineArrowProvider) -> Self {
        let schema = provider.schema().cloned();
        Self { provider, schema }
    }
}

#[async_trait]
impl ResultReader for InlineArrowReader {
    async fn schema(&self) -> Result<SchemaRef> {
        self.schema
            .clone()
            .ok_or_else(|| Error::invalid_state().message("Schema not available for inline result"))
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.provider.next_batch()
    }
}

/// Reader for multi-chunk inline-Arrow streaming.
///
/// Wraps [`InlineStreamingProvider`] to implement the [`ResultReader`]
/// trait. The provider's prefetch task fills a bounded rolling
/// window of decoded chunks; `next_batch` pulls one Arrow batch at
/// a time from the head of that window.
struct InlineStreamingReader {
    provider: Arc<InlineStreamingProvider>,
    /// Cached schema (extracted from the initial chunk).
    schema: Option<SchemaRef>,
}

impl InlineStreamingReader {
    fn new(provider: Arc<InlineStreamingProvider>) -> Self {
        let schema = provider.schema().cloned();
        Self { provider, schema }
    }
}

#[async_trait]
impl ResultReader for InlineStreamingReader {
    async fn schema(&self) -> Result<SchemaRef> {
        self.schema.clone().ok_or_else(|| {
            Error::invalid_state().message("Schema not available for inline-Arrow streaming result")
        })
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.provider.next_batch().await
    }
}

/// In-memory result reader.
///
/// Yields a fixed `Vec<RecordBatch>` from memory, one batch per call
/// to [`InMemoryReader::next_batch`]. Backs the static
/// `Metadata::type_info` and `Metadata::table_types` streams — both
/// return data without ever touching the wire.
pub struct InMemoryReader {
    schema: SchemaRef,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl InMemoryReader {
    /// Build a reader over the given batches. The schema is taken
    /// from the caller (rather than inferred from the first batch)
    /// so an empty `batches` slice still produces a typed empty
    /// stream.
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
        }
    }
}

#[async_trait]
impl ResultReader for InMemoryReader {
    async fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

/// Empty reader for queries with no results.
///
/// Used for valid queries that return zero rows (e.g., `SELECT * WHERE 1=0`).
/// The schema is preserved from the query's manifest.
pub struct EmptyReader {
    schema: SchemaRef,
}

impl EmptyReader {
    /// Create an empty reader that returns no batches.
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl ResultReader for EmptyReader {
    async fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

/// Arrow field metadata keys for Databricks column information.
///
/// These propagate SEA manifest metadata through the Arrow C Data Interface
/// so downstream consumers (ODBC driver) can access server-provided metadata
/// without reverse-engineering from Arrow type IDs.
///
/// Keys use the `databricks.` prefix to avoid conflicts with other metadata
/// conventions (e.g., Arrow's own `ARROW:extension:name`).
pub mod metadata_keys {
    pub const TYPE_NAME: &str = "databricks.type_name";
    pub const TYPE_TEXT: &str = "databricks.type_text";
    pub const TYPE_PRECISION: &str = "databricks.type_precision";
    pub const TYPE_SCALE: &str = "databricks.type_scale";
    pub const TYPE_INTERVAL_TYPE: &str = "databricks.type_interval_type";
}

/// Adapter to make ResultReader work as arrow's RecordBatchReader.
pub struct ResultReaderAdapter {
    inner: Box<dyn ResultReader + Send>,
    schema: SchemaRef,
}

impl ResultReaderAdapter {
    /// Create a new adapter wrapping a ResultReader, optionally augmenting the
    /// schema with Databricks column metadata from the SEA manifest.
    ///
    /// `async` because [`ResultReader::schema`] is async (the CloudFetch
    /// reader forces the first chunk to download to extract the schema).
    /// In-memory readers resolve their schema synchronously.
    pub async fn new(
        inner: Box<dyn ResultReader + Send>,
        manifest: Option<&ResultManifest>,
    ) -> Result<Self> {
        let schema = inner.schema().await?;
        let schema = match manifest {
            Some(m) => Self::augment_schema_with_manifest(&schema, m),
            None => schema,
        };
        Ok(Self { inner, schema })
    }

    /// Augment an Arrow schema with column info from the manifest.
    ///
    /// For each field with a matching `ColumnInfo` (by position):
    /// - overwrite the field name with `ColumnInfo.name` — SEA sends
    ///   positional placeholders (`col_0`, …) in the Arrow payload; the
    ///   authoritative names live in the manifest;
    /// - attach `databricks.*` type metadata (type_name, type_text,
    ///   precision, scale, interval_type) for the Arrow C Data FFI boundary.
    ///
    /// Fields with no matching `ColumnInfo` are left unchanged.
    pub(crate) fn augment_schema_with_manifest(
        schema: &SchemaRef,
        manifest: &ResultManifest,
    ) -> SchemaRef {
        use std::collections::HashMap;

        let col_by_pos: HashMap<usize, &crate::types::sea::ColumnInfo> = manifest
            .schema
            .columns
            .iter()
            .filter_map(|c| usize::try_from(c.position).ok().map(|pos| (pos, c)))
            .collect();

        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let col_info = col_by_pos.get(&i).copied();

                match col_info {
                    Some(info) => {
                        let mut metadata = field.metadata().clone();
                        metadata
                            .insert(metadata_keys::TYPE_NAME.to_string(), info.type_name.clone());
                        metadata
                            .insert(metadata_keys::TYPE_TEXT.to_string(), info.type_text.clone());
                        if let Some(prec) = info.type_precision {
                            metadata.insert(
                                metadata_keys::TYPE_PRECISION.to_string(),
                                prec.to_string(),
                            );
                        }
                        if let Some(scale) = info.type_scale {
                            metadata
                                .insert(metadata_keys::TYPE_SCALE.to_string(), scale.to_string());
                        }
                        if let Some(ref interval) = info.type_interval_type {
                            metadata.insert(
                                metadata_keys::TYPE_INTERVAL_TYPE.to_string(),
                                interval.clone(),
                            );
                        }
                        // Overlay the real manifest name (Arrow payload has
                        // positional `col_N`) and attach the type metadata.
                        field
                            .as_ref()
                            .clone()
                            .with_name(info.name.clone())
                            .with_metadata(metadata)
                    }
                    None => field.as_ref().clone(),
                }
            })
            .collect();

        Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ))
    }
}

impl arrow_array::RecordBatchReader for ResultReaderAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for ResultReaderAdapter {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Sync `Iterator` adapter for arrow's `RecordBatchReader`.
        // `block_on_current_runtime` uses block_in_place + the
        // current runtime Handle when one exists (the case for
        // CloudFetch readers), and falls back to
        // `futures::executor::block_on` when there is no runtime
        // (in-memory readers in plain-thread tests / non-tokio
        // callers). Prevents the issue-#19 nested-block_on panic
        // on this sync iterator path.
        match crate::util::runtime::block_on_current_runtime(self.inner.next_batch()) {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => Some(Err(ArrowError::ExternalError(Box::new(
                std::io::Error::other(e.to_string()),
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::test_utils::{ErrorReader, MockReader, SchemaErrorReader};
    use crate::types::sea::{ColumnInfo, ResultManifest, ResultSchema};
    use arrow_array::StringArray;
    use arrow_schema::DataType;

    fn make_test_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    // --- ResultReaderAdapter tests ---

    #[tokio::test]
    async fn test_adapter_schema() {
        let batch = make_test_batch(vec!["a"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let adapter = ResultReaderAdapter::new(reader, None).await.unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&adapter);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "name");
    }

    #[tokio::test]
    async fn test_adapter_iterates_batches() {
        let batch1 = make_test_batch(vec!["a", "b"]);
        let batch2 = make_test_batch(vec!["c"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch1, batch2]));
        let mut adapter = ResultReaderAdapter::new(reader, None).await.unwrap();

        let first = adapter.next().unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);

        let second = adapter.next().unwrap().unwrap();
        assert_eq!(second.num_rows(), 1);

        assert!(adapter.next().is_none());
    }

    #[tokio::test]
    async fn test_adapter_empty_reader() {
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![]));
        let mut adapter = ResultReaderAdapter::new(reader, None).await.unwrap();
        assert!(adapter.next().is_none());
    }

    #[tokio::test]
    async fn test_adapter_propagates_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let reader: Box<dyn ResultReader + Send> = Box::new(ErrorReader { schema });
        let mut adapter = ResultReaderAdapter::new(reader, None).await.unwrap();

        let result = adapter.next().unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_adapter_schema_error() {
        let reader: Box<dyn ResultReader + Send> = Box::new(SchemaErrorReader);
        let result = ResultReaderAdapter::new(reader, None).await;
        assert!(result.is_err());
    }

    // --- EmptyReader tests ---

    #[tokio::test]
    async fn test_empty_reader_with_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut reader = EmptyReader::new(schema.clone());

        // Schema should be preserved
        let result_schema = reader.schema().await.unwrap();
        assert_eq!(result_schema.fields().len(), 2);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(1).name(), "name");

        // Should return no batches
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_empty_reader_with_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let mut reader = EmptyReader::new(schema);

        assert_eq!(reader.schema().await.unwrap().fields().len(), 0);
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    #[test]
    fn test_extract_schema_from_manifest_primitives() {
        // The simple-primitive cases the previous private mapper covered.
        // Verifies the shared `databricks_type_to_arrow` still produces
        // the same Arrow types for these so behaviour didn't regress when
        // the duplicate was removed.
        let manifest = make_manifest(vec![
            make_column_info("a", "BOOLEAN", "BOOLEAN", 0, None, None, None),
            make_column_info("b", "INT", "INT", 1, None, None, None),
            make_column_info("c", "BIGINT", "BIGINT", 2, None, None, None),
            make_column_info("d", "STRING", "STRING", 3, None, None, None),
            make_column_info("e", "DOUBLE", "DOUBLE", 4, None, None, None),
        ]);
        let schema = ResultReaderFactory::extract_schema_from_manifest(Some(&manifest)).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_extract_schema_from_manifest_decimal_carries_precision_scale() {
        // Regression test for the comparator bucket-C cause #1:
        // empty-result schema for a DECIMAL column was previously
        // `Utf8` (because the local mapper fell through to the catch-all
        // and ignored type_text/type_precision/type_scale), producing
        // a divergence vs Thrift's `Decimal128(p, s)`.
        let manifest = make_manifest(vec![make_column_info(
            "decimal_column",
            "DECIMAL",
            "DECIMAL(10,2)",
            0,
            Some(10),
            Some(2),
            None,
        )]);
        let schema = ResultReaderFactory::extract_schema_from_manifest(Some(&manifest)).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_extract_schema_from_manifest_interval_void() {
        // INTERVAL and VOID were both falling through to Utf8 in the
        // previous private mapper — bucket-C cause #1 sub-cases.
        //
        // Canonical SEA `type_text` for INTERVAL columns is
        // `"INTERVAL YEAR TO MONTH"` / `"INTERVAL DAY TO SECOND"` —
        // the parser must split on whitespace to recognise the
        // `INTERVAL` base. `type_interval_type` (`"YEAR_MONTH"` /
        // `"DAY_TIME"`) is the canonical disambiguator and is
        // preferred over the keyword-match fallback.
        let manifest = make_manifest(vec![
            make_column_info(
                "ym",
                "INTERVAL",
                "INTERVAL YEAR TO MONTH",
                0,
                None,
                None,
                Some("YEAR_MONTH"),
            ),
            make_column_info(
                "ds",
                "INTERVAL",
                "INTERVAL DAY TO SECOND",
                1,
                None,
                None,
                Some("DAY_TIME"),
            ),
            make_column_info("v", "VOID", "VOID", 2, None, None, None),
        ]);
        let schema = ResultReaderFactory::extract_schema_from_manifest(Some(&manifest)).unwrap();
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
            "INTERVAL YEAR TO MONTH must map to IntervalUnit::YearMonth"
        );
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Interval(arrow_schema::IntervalUnit::DayTime),
            "INTERVAL DAY TO SECOND must map to IntervalUnit::DayTime"
        );
        // VOID surfaces as `Utf8` — the server reports untyped NULL / VOID
        // as STRING, so an empty-result VOID column reports the same type
        // as a `CAST(NULL AS STRING)` sibling (issue #203, DATATYPE-041).
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_extract_schema_from_manifest_interval_keyword_fallback() {
        // When `type_interval_type` is absent, the mapper falls back to
        // keyword-matching the `type_text`. Bare `"INTERVAL"` (no
        // qualifier) historically meant DAY_TIME; preserve that.
        let manifest = make_manifest(vec![
            make_column_info(
                "ym",
                "INTERVAL",
                "INTERVAL YEAR TO MONTH",
                0,
                None,
                None,
                None,
            ),
            make_column_info(
                "ds",
                "INTERVAL",
                "INTERVAL DAY TO SECOND",
                1,
                None,
                None,
                None,
            ),
            make_column_info("bare", "INTERVAL", "INTERVAL", 2, None, None, None),
        ]);
        let schema = ResultReaderFactory::extract_schema_from_manifest(Some(&manifest)).unwrap();
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Interval(arrow_schema::IntervalUnit::YearMonth)
        );
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Interval(arrow_schema::IntervalUnit::DayTime)
        );
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Interval(arrow_schema::IntervalUnit::DayTime)
        );
    }

    #[test]
    fn test_extract_schema_from_manifest_complex_types_parse_into_nested_arrow() {
        // ARRAY / MAP / STRUCT carry their structure in the manifest's
        // `type_text` field; the empty-result schema now reconstructs the
        // matching nested Arrow type so the empty-result path agrees
        // with the non-empty wire-payload schema. The actual parsing
        // lives in `metadata::type_mapping::databricks_type_to_arrow`;
        // this asserts the factory plumbs `type_text` through correctly.
        let manifest = make_manifest(vec![
            make_column_info("a", "ARRAY", "ARRAY<INT>", 0, None, None, None),
            make_column_info("m", "MAP", "MAP<STRING,INT>", 1, None, None, None),
            make_column_info("s", "STRUCT", "STRUCT<x:INT>", 2, None, None, None),
        ]);
        let schema = ResultReaderFactory::extract_schema_from_manifest(Some(&manifest)).unwrap();
        assert!(matches!(schema.field(0).data_type(), DataType::List(_)));
        assert!(matches!(schema.field(1).data_type(), DataType::Map(_, _)));
        assert!(matches!(schema.field(2).data_type(), DataType::Struct(_)));
    }

    #[test]
    fn test_extract_schema_from_manifest_no_manifest_returns_empty() {
        // Pre-existing fallback path: no manifest at all → empty schema,
        // no warning, no panic. Lock in.
        let schema = ResultReaderFactory::extract_schema_from_manifest(None).unwrap();
        assert_eq!(schema.fields().len(), 0);
    }

    // --- Schema augmentation tests ---

    fn make_manifest(columns: Vec<ColumnInfo>) -> ResultManifest {
        ResultManifest {
            format: "ARROW_STREAM".to_string(),
            schema: ResultSchema {
                column_count: columns.len() as i32,
                columns,
            },
            total_chunk_count: None,
            total_row_count: None,
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
        }
    }

    fn make_column_info(
        name: &str,
        type_name: &str,
        type_text: &str,
        position: i32,
        precision: Option<i64>,
        scale: Option<i64>,
        interval_type: Option<&str>,
    ) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            type_text: type_text.to_string(),
            position,
            type_precision: precision,
            type_scale: scale,
            type_interval_type: interval_type.map(|s| s.to_string()),
        }
    }

    /// Overlay must replace the Arrow payload's positional names
    /// (`col_0`, …) with the manifest's real names. The other augment
    /// tests use fixtures where the names already match, so this is the
    /// only one that catches a broken overlay.
    #[test]
    fn test_augment_schema_overlays_manifest_names_over_positional() {
        // Arrow payload = positional placeholders; manifest = real names.
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Int32, true),
            Field::new("col_1", DataType::Utf8, true),
            Field::new("col_2", DataType::Boolean, true),
        ]));
        let manifest = make_manifest(vec![
            make_column_info("id", "INT", "INT", 0, None, None, None),
            make_column_info("varchar_column", "STRING", "STRING", 1, None, None, None),
            make_column_info("boolean_column", "BOOLEAN", "BOOLEAN", 2, None, None, None),
        ]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        // Names come from the manifest, not the Arrow payload.
        assert_eq!(augmented.field(0).name(), "id");
        assert_eq!(augmented.field(1).name(), "varchar_column");
        assert_eq!(augmented.field(2).name(), "boolean_column");
        // Data types are preserved unchanged.
        assert_eq!(augmented.field(0).data_type(), &DataType::Int32);
        assert_eq!(augmented.field(1).data_type(), &DataType::Utf8);
        assert_eq!(augmented.field(2).data_type(), &DataType::Boolean);
        // Type metadata still attached.
        assert_eq!(
            augmented.field(0).metadata().get(metadata_keys::TYPE_NAME),
            Some(&"INT".to_string())
        );
    }

    /// A field with no matching `ColumnInfo` keeps its original Arrow name.
    #[test]
    fn test_augment_schema_keeps_arrow_name_when_no_manifest_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Int32, true),
            Field::new("col_1", DataType::Utf8, true),
        ]));
        // Manifest only describes position 0.
        let manifest = make_manifest(vec![make_column_info(
            "id", "INT", "INT", 0, None, None, None,
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        assert_eq!(augmented.field(0).name(), "id", "manifest name overlaid");
        assert_eq!(
            augmented.field(1).name(),
            "col_1",
            "no manifest column at position 1 → original Arrow name kept"
        );
    }

    #[test]
    fn test_augment_schema_basic_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let manifest = make_manifest(vec![
            make_column_info("id", "INT", "INT", 0, None, None, None),
            make_column_info("name", "STRING", "STRING", 1, None, None, None),
            make_column_info("active", "BOOLEAN", "BOOLEAN", 2, None, None, None),
        ]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        assert_eq!(augmented.fields().len(), 3);
        // Check type_name and type_text are set
        assert_eq!(
            augmented.field(0).metadata().get(metadata_keys::TYPE_NAME),
            Some(&"INT".to_string())
        );
        assert_eq!(
            augmented.field(0).metadata().get(metadata_keys::TYPE_TEXT),
            Some(&"INT".to_string())
        );
        assert_eq!(
            augmented.field(1).metadata().get(metadata_keys::TYPE_NAME),
            Some(&"STRING".to_string())
        );
        // Precision/scale should not be present
        assert!(augmented
            .field(0)
            .metadata()
            .get(metadata_keys::TYPE_PRECISION)
            .is_none());
    }

    #[test]
    fn test_augment_schema_decimal_with_precision_scale() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let manifest = make_manifest(vec![make_column_info(
            "amount",
            "DECIMAL",
            "DECIMAL(10,2)",
            0,
            Some(10),
            Some(2),
            None,
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        let field = augmented.field(0);
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_NAME),
            Some(&"DECIMAL".to_string())
        );
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_TEXT),
            Some(&"DECIMAL(10,2)".to_string())
        );
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_PRECISION),
            Some(&"10".to_string())
        );
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_SCALE),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn test_augment_schema_interval_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "duration",
            DataType::Utf8,
            true,
        )]));
        let manifest = make_manifest(vec![make_column_info(
            "duration",
            "INTERVAL",
            "INTERVAL YEAR TO MONTH",
            0,
            None,
            None,
            Some("YEAR_MONTH"),
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        let field = augmented.field(0);
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_INTERVAL_TYPE),
            Some(&"YEAR_MONTH".to_string())
        );
    }

    #[test]
    fn test_augment_schema_missing_column_info() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("known", DataType::Int32, true),
            Field::new("unknown", DataType::Utf8, true),
        ]));
        // Only provide metadata for position 0, not position 1
        let manifest = make_manifest(vec![make_column_info(
            "known", "INT", "INT", 0, None, None, None,
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        // First field should have metadata
        assert!(augmented
            .field(0)
            .metadata()
            .contains_key(metadata_keys::TYPE_NAME));
        // Second field should have no databricks metadata
        assert!(!augmented
            .field(1)
            .metadata()
            .contains_key(metadata_keys::TYPE_NAME));
    }

    #[test]
    fn test_augment_schema_preserves_existing_metadata() {
        let mut existing_metadata = std::collections::HashMap::new();
        existing_metadata.insert("custom.key".to_string(), "custom_value".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("col", DataType::Int32, true).with_metadata(existing_metadata)
        ]));
        let manifest = make_manifest(vec![make_column_info(
            "col", "INT", "INT", 0, None, None, None,
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        let field = augmented.field(0);
        // Existing metadata preserved
        assert_eq!(
            field.metadata().get("custom.key"),
            Some(&"custom_value".to_string())
        );
        // New metadata added
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_NAME),
            Some(&"INT".to_string())
        );
    }

    #[tokio::test]
    async fn test_adapter_with_manifest() {
        let batch = make_test_batch(vec!["hello"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![make_column_info(
            "name", "STRING", "STRING", 0, None, None, None,
        )]);

        let adapter = ResultReaderAdapter::new(reader, Some(&manifest))
            .await
            .unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&adapter);
        assert_eq!(
            schema.field(0).metadata().get(metadata_keys::TYPE_NAME),
            Some(&"STRING".to_string())
        );
    }

    #[tokio::test]
    async fn test_adapter_without_manifest() {
        let batch = make_test_batch(vec!["hello"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let adapter = ResultReaderAdapter::new(reader, None).await.unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&adapter);
        // No databricks metadata when manifest is None
        assert!(!schema
            .field(0)
            .metadata()
            .contains_key(metadata_keys::TYPE_NAME));
    }

    // ManifestAugmentingReader behaviour is now covered by
    // `post_processor::tests` — the wrapper itself was folded into
    // `ResultPostProcessor`.

    // ──────────────────────────────────────────────────────────────
    // ResultReaderFactory::create_inner_reader — rollout-guard and
    // initial-response strictness tests (PR #71 round-2 feedback
    // B-2, B-3).
    //
    // The rollout-guard fires before any Transport method is
    // called, so the test only needs a `Transport` impl that
    // compiles. We use a panicking stub.
    // ──────────────────────────────────────────────────────────────

    use crate::client::{
        ChunkLinkFetchResult, DatabricksHttpClient, ExecuteResponse, ExecuteResult,
        ExecuteResultData, HttpClientConfig, InlineChunkFetchResult, SessionInfo, TransportConfig,
    };
    use crate::kernel_error::ErrorCode;
    use crate::reader::post_processor::PostProcessOptions;
    use crate::types::sea::{ExecuteParams, ServiceError, StatementStatus};
    use std::collections::HashMap;

    /// Transport stub that panics on every method. Adequate for
    /// tests that exercise the *factory's* synchronous control
    /// flow before any RPC fires (e.g. the rollout-skew guard).
    #[derive(Debug)]
    struct PanickingTransport;

    #[async_trait]
    impl Transport for PanickingTransport {
        async fn create_session(
            &self,
            _: Option<&str>,
            _: Option<&str>,
            _: HashMap<String, String>,
        ) -> Result<SessionInfo> {
            unreachable!("PanickingTransport: not used by these tests")
        }
        async fn delete_session(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn execute_statement(
            &self,
            _: &str,
            _: &str,
            _: &ExecuteParams,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn submit_statement(
            &self,
            _: &str,
            _: &str,
            _: &ExecuteParams,
        ) -> Result<ExecuteResponse> {
            unreachable!()
        }
        async fn execute_statement_direct(
            &self,
            _: &str,
            _: &str,
            _: &ExecuteParams,
        ) -> Result<crate::client::DirectOutcome> {
            unreachable!()
        }
        async fn fetch_completed_result(&self, _: &str) -> Result<Option<ExecuteResult>> {
            unreachable!()
        }
        async fn statement_status(&self, _: &str) -> Result<ExecuteResponse> {
            unreachable!()
        }
        async fn get_result_chunks(&self, _: &str, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
            unreachable!()
        }
        async fn get_inline_chunk(
            &self,
            _: &str,
            _: i64,
            _: i64,
        ) -> Result<InlineChunkFetchResult> {
            unreachable!()
        }
        fn poll_config(&self) -> TransportConfig {
            TransportConfig::default()
        }
        async fn cancel_statement(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn close_statement(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn list_catalogs(&self, _: &str) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn list_schemas(
            &self,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn list_tables(
            &self,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&[&str]>,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn list_columns(
            &self,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&str>,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn list_procedures(
            &self,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&str>,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn list_functions(
            &self,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&str>,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn list_primary_keys(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        async fn list_foreign_keys(
            &self,
            _: &str,
            _: Option<&str>,
            _: Option<&str>,
            _: Option<&str>,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<ExecuteResult> {
            unreachable!()
        }
        fn list_table_types(&self) -> Vec<String> {
            unreachable!()
        }
    }

    /// Build a `ResultReaderFactory` against the panicking stub
    /// transport. Used by control-flow tests that don't fire any
    /// RPC.
    fn factory_for_control_flow_test() -> ResultReaderFactory {
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).unwrap(),
        );
        let runtime = tokio::runtime::Handle::current();
        ResultReaderFactory::new(
            Arc::new(PanickingTransport),
            http_client,
            CloudFetchConfig::default(),
            PostProcessOptions::default(),
            runtime,
        )
    }

    fn ok_status() -> StatementStatus {
        StatementStatus {
            state: StatementState::Succeeded,
            error: None::<ServiceError>,
            num_modified_rows: None,
            display_message: None,
            diagnostic_info: None,
            error_details_json: None,
        }
    }

    fn minimal_manifest(total_chunk_count: Option<i64>) -> ResultManifest {
        ResultManifest {
            format: "ARROW_STREAM".to_string(),
            schema: ResultSchema {
                column_count: 0,
                columns: vec![],
            },
            total_chunk_count,
            total_row_count: None,
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
        }
    }

    /// B-2: the rollout-guard branch. Server emits multi-chunk
    /// inline-Arrow (manifest says total_chunk_count = 2) but
    /// without the per-chunk `next_chunk_index` field. The factory
    /// MUST surface `data_loss` rather than silently truncate the
    /// result after the first chunk.
    #[tokio::test]
    async fn rollout_guard_errors_when_total_chunks_disagrees_with_next_chunk_index() {
        let factory = factory_for_control_flow_test();
        let response = ExecuteResponse {
            statement_id: "stmt-1".to_string(),
            status: ok_status(),
            manifest: Some(minimal_manifest(Some(2))),
            result: Some(ExecuteResultData {
                next_chunk_internal_link: None,
                external_links: None,
                // Non-empty inline payload, but per-chunk
                // continuation is missing.
                inline_arrow_data: Some(vec![1u8, 2, 3]),
                inline_chunk_index: Some(0),
                inline_row_offset: Some(0),
                inline_row_count: Some(0),
                inline_next_chunk_index: None,
            }),
        };

        let err = match factory.create_inner_reader("stmt-1", &response) {
            Ok(_) => panic!("rollout-skew must surface as Err"),
            Err(e) => e,
        };
        assert_eq!(err.code, ErrorCode::DataLoss);
        assert!(
            err.message.contains("total_chunk_count"),
            "error must call out the total_chunk_count discrepancy: {}",
            err.message,
        );
    }

    /// B-2 negative: when total_chunk_count says 1 (genuinely
    /// single-chunk), the rollout guard MUST NOT fire. The
    /// single-chunk inline path is taken instead, which would only
    /// error if the bytes don't parse as Arrow IPC — but the
    /// rollout guard runs BEFORE the parse, so it suffices to
    /// confirm the error (if any) is NOT a `data_loss` mentioning
    /// `total_chunk_count`.
    #[tokio::test]
    async fn rollout_guard_does_not_fire_on_genuinely_single_chunk() {
        let factory = factory_for_control_flow_test();
        let response = ExecuteResponse {
            statement_id: "stmt-1".to_string(),
            status: ok_status(),
            manifest: Some(minimal_manifest(Some(1))),
            result: Some(ExecuteResultData {
                next_chunk_internal_link: None,
                external_links: None,
                // Garbage bytes — the rollout guard won't reach the
                // IPC parse step, so this is fine for the guard
                // test. If we ever hit `data_loss` mentioning
                // `total_chunk_count`, the guard erroneously fired.
                inline_arrow_data: Some(vec![1u8, 2, 3]),
                inline_chunk_index: Some(0),
                inline_row_offset: Some(0),
                inline_row_count: Some(1),
                inline_next_chunk_index: None,
            }),
        };

        match factory.create_inner_reader("stmt-1", &response) {
            // OK or any non-rollout-guard error is acceptable —
            // we're proving the guard is *not* the failure mode.
            Ok(_) => {}
            Err(e) => assert!(
                !e.message.contains("total_chunk_count"),
                "rollout guard erroneously fired for single-chunk: {}",
                e.message,
            ),
        }
    }

    /// B-3: initial execute response must be strict about
    /// `row_count` when `next_chunk_index.is_some()`. Defaulting
    /// to 0 would silently corrupt the next-fetch row_offset.
    #[tokio::test]
    async fn initial_response_requires_row_count_when_streaming() {
        let factory = factory_for_control_flow_test();
        let response = ExecuteResponse {
            statement_id: "stmt-1".to_string(),
            status: ok_status(),
            manifest: Some(minimal_manifest(Some(2))),
            result: Some(ExecuteResultData {
                next_chunk_internal_link: None,
                external_links: None,
                inline_arrow_data: Some(vec![1u8, 2, 3]),
                inline_chunk_index: Some(0),
                inline_row_offset: Some(0),
                // Critical: row_count missing on an
                // attachment-bearing response that opts into
                // streaming (next_chunk_index is Some).
                inline_row_count: None,
                inline_next_chunk_index: Some(1),
            }),
        };

        let err = match factory.create_inner_reader("stmt-1", &response) {
            Ok(_) => panic!("missing row_count on streaming response must surface as Err"),
            Err(e) => e,
        };
        assert_eq!(err.code, ErrorCode::DataLoss);
        assert!(
            err.message.to_lowercase().contains("row_count"),
            "error must mention the missing field: {}",
            err.message,
        );
    }

    #[tokio::test]
    async fn initial_response_requires_chunk_index_when_streaming() {
        let factory = factory_for_control_flow_test();
        let response = ExecuteResponse {
            statement_id: "stmt-1".to_string(),
            status: ok_status(),
            manifest: Some(minimal_manifest(Some(2))),
            result: Some(ExecuteResultData {
                next_chunk_internal_link: None,
                external_links: None,
                inline_arrow_data: Some(vec![1u8, 2, 3]),
                inline_chunk_index: None,
                inline_row_offset: Some(0),
                inline_row_count: Some(1),
                inline_next_chunk_index: Some(1),
            }),
        };

        let err = match factory.create_inner_reader("stmt-1", &response) {
            Ok(_) => panic!("missing chunk_index on streaming response must surface as Err"),
            Err(e) => e,
        };
        assert_eq!(err.code, ErrorCode::DataLoss);
        assert!(
            err.message.to_lowercase().contains("chunk_index"),
            "error must mention the missing field: {}",
            err.message,
        );
    }

    // ──────────────────────────────────────────────────────────────
    // total_chunk_count == 0 manifest short-circuit.
    //
    // The SEA manifest is authoritative when it explicitly reports
    // zero chunks: the result has no rows, regardless of any stray
    // `result` payload the server may have attached. Absent or
    // non-zero values must fall through to the existing branch
    // dispatch (the field is deprecated on newer servers).
    // ──────────────────────────────────────────────────────────────

    /// Zero-chunk manifest must produce an `EmptyReader` even when a
    /// non-empty inline-Arrow attachment is also present. The factory
    /// must trust the manifest and never reach the IPC parser. Bytes
    /// that wouldn't parse as Arrow IPC are deliberately used so that
    /// an accidental fall-through would surface as an `io` error
    /// rather than a silent pass.
    #[tokio::test]
    async fn zero_chunk_count_short_circuits_to_empty_reader() {
        let factory = factory_for_control_flow_test();
        let response = ExecuteResponse {
            statement_id: "stmt-1".to_string(),
            status: ok_status(),
            manifest: Some(minimal_manifest(Some(0))),
            result: Some(ExecuteResultData {
                next_chunk_internal_link: None,
                external_links: None,
                inline_arrow_data: Some(vec![1u8, 2, 3]),
                inline_chunk_index: Some(0),
                inline_row_offset: Some(0),
                inline_row_count: Some(0),
                inline_next_chunk_index: None,
            }),
        };

        let mut reader = factory
            .create_inner_reader("stmt-1", &response)
            .expect("zero-chunk manifest must succeed without parsing inline bytes");
        assert!(
            reader.next_batch().await.unwrap().is_none(),
            "zero-chunk reader must yield no batches",
        );
    }

    /// Absent `total_chunk_count` (older servers / non-SEA path) must
    /// NOT trip the short-circuit. With a `Succeeded` state and no
    /// result payload, the factory falls through to the existing
    /// state-based empty branch — also an `EmptyReader`, but reached
    /// via a different code path. The test guards that the absence
    /// of the field is not mis-read as `Some(0)`.
    #[tokio::test]
    async fn absent_total_chunk_count_falls_through() {
        let factory = factory_for_control_flow_test();
        let response = ExecuteResponse {
            statement_id: "stmt-1".to_string(),
            status: ok_status(),
            manifest: Some(minimal_manifest(None)),
            result: None,
        };

        let mut reader = factory
            .create_inner_reader("stmt-1", &response)
            .expect("absent total_chunk_count + Succeeded state must yield empty reader");
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    /// Non-zero `total_chunk_count` must NOT trip the short-circuit
    /// even when the result payload is empty in shape. With
    /// `total_chunk_count = 1` and a non-empty inline attachment of
    /// garbage bytes, the factory takes the single-chunk inline path
    /// and fails parsing — proving the short-circuit didn't fire
    /// (which would have returned `Ok` with no batches). The exact
    /// error code isn't part of the contract; we only assert that
    /// it's not the manifest-driven `EmptyReader` path.
    #[tokio::test]
    async fn non_zero_total_chunk_count_does_not_short_circuit() {
        let factory = factory_for_control_flow_test();
        let response = ExecuteResponse {
            statement_id: "stmt-1".to_string(),
            status: ok_status(),
            manifest: Some(minimal_manifest(Some(1))),
            result: Some(ExecuteResultData {
                next_chunk_internal_link: None,
                external_links: None,
                inline_arrow_data: Some(vec![1u8, 2, 3]),
                inline_chunk_index: Some(0),
                inline_row_offset: Some(0),
                inline_row_count: Some(1),
                inline_next_chunk_index: None,
            }),
        };

        // Single-chunk inline path on garbage bytes — must surface as
        // a parse error. If the short-circuit had fired, this would
        // return `Ok` and we'd never reach the IPC parser.
        let err = factory
            .create_inner_reader("stmt-1", &response)
            .err()
            .expect("non-zero total_chunk_count + garbage inline must surface parse error");
        assert!(
            !err.message.contains("total_chunk_count=0"),
            "short-circuit must not have fired: {}",
            err.message,
        );
    }
}
