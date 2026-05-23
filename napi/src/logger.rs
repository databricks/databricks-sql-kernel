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

//! `tracing` → JS `DBSQLLogger` bridge via `ThreadsafeFunction`.
//!
//! **Round 3 work — empty in Round 1b.** This file is intentionally a
//! stub: the `pub(crate) mod logger;` declaration in `lib.rs` is kept
//! so the Round 3 PR landing the implementation can do so without a
//! `mod` reshuffle that ripples through every downstream stack branch.
//!
//! ## Round 3 design sketch
//!
//! The kernel emits ~46 `tracing::` events on hot paths (per-statement
//! lifecycle, retries, CloudFetch download progress). Forwarding each
//! event individually to JS via `ThreadsafeFunction::call` (≈10-50µs
//! per call) would eat the recent SELECT-1 latency wins; the bridge
//! has to batch.
//!
//! Planned shape:
//!
//! - **Kernel side:** a `tracing::Subscriber` Layer pushes events into
//!   a bounded `tokio::sync::mpsc::channel` (capacity ~1024).
//! - **Drain task:** one dedicated tokio task drains the channel in
//!   batches at ~100Hz and forwards each batch via a single
//!   `ThreadsafeFunction::call` with
//!   `ThreadsafeFunctionCallMode::NonBlocking`.
//! - **Backpressure:** drop-oldest on full channel (logs are
//!   advisory; never block a kernel-side hot path on JS-side
//!   back-pressure). A `dropped_log_count` counter is exposed for
//!   observability.
//! - **Default level:** Subscriber-level filtering defaults to
//!   `WARN` so the channel is only loaded when a JS consumer opts in
//!   (`DBSQLLogger.setLevel('debug')`).
//! - **JS contract:** one-way (kernel → JS); the bridge does NOT take
//!   structured arguments back from JS.
//!
//! Open questions parked for Round 3:
//! - Whether to expose span enter/exit as separate JS events or to
//!   flatten to (event + parent_span_id) pairs.
//! - How to surface kernel-side `field=value` structured data without
//!   `serde_json::Value`-allocating on every event.
