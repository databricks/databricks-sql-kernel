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

//! Inline Arrow results support.
//!
//! This module handles inline Arrow results that are embedded directly
//! in the API response (via the `attachment` field) rather than being
//! downloaded from cloud storage via CloudFetch.
//!
//! Two paths live here:
//!
//! - [`InlineArrowProvider`] (in [`provider`]) — single-chunk case.
//!   The whole result fits in one `attachment`; we parse it up front
//!   and yield batches synchronously. Used when the initial execute
//!   response embeds the full result and `next_chunk_index` is
//!   absent.
//!
//! - [`InlineStreamingProvider`] (in [`streaming`]) — multi-chunk
//!   case. The server returns one chunk at a time via
//!   `GET /result/chunks/{idx}?row_offset=N`; the kernel maintains a
//!   bounded rolling window of pre-decoded chunks ahead of the
//!   consumer. Used when CloudFetch is disabled (the execute request
//!   was sent with SEA `disposition = INLINE`) and the result is
//!   large enough that the server paginates it.

mod provider;
mod streaming;

pub use provider::InlineArrowProvider;
pub(crate) use streaming::{InlineChunkSource, InlineStreamingProvider};
