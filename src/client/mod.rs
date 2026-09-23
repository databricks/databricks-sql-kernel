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

//! Wire client and transport plumbing for talking to Databricks.
//!
//! This module is structured into three layers:
//!
//! - [`http`] — low-level HTTP client (connection pooling, proxy/TLS,
//!   retry orchestration). Used by every transport implementation.
//! - [`transport`] — the [`Transport`](transport::Transport) trait
//!   and its supporting value types. The trait is `pub(crate)` —
//!   wrappers never see it; they pick a transport via
//!   `SessionConfig.transport: Option<TransportMode>`.
//! - [`sea`] — [`SeaRestTransport`](sea::SeaRestTransport): the
//!   Statement Execution API over REST. v0's only `Transport` impl;
//!   future variants (`SeaGrpcTransport`, `ThriftTransport`) plug
//!   into the same trait.
//!
//! [`http`]: self::http
//! [`transport`]: self::transport
//! [`sea`]: self::sea

pub mod http;
pub mod retry;
pub mod sea;
pub(crate) mod transport;

pub(crate) const DBSQL_SESSION_ID_HEADER: &str = "x-databricks-dbsql-session-id";
pub(crate) const REALTIME_DIRECT_ROUTING_HEADER: &str = "x-databricks-realtime-direct-routing";
pub(crate) const REALTIME_SESSION_HEADER: &str = "x-databricks-realtime-session";

// Public surface — kept stable for advanced wrappers that tune HTTP.
pub use http::{DatabricksHttpClient, HttpClientConfig, ProxyConfig};
pub use retry::{
    build_retry_configs, RequestCategory, RequestType, RetryConfig, RetryConfigOverrides,
};

// Crate-internal surface — `Transport` trait + its value types.
pub(crate) use sea::SeaRestTransport;
pub(crate) use transport::{
    ChunkLinkFetchResult, DirectOutcome, ExecuteResponse, ExecuteResult, ExecuteResultData,
    ExecutionTimeout, InlineChunkFetchResult, SessionAttribution, SessionInfo, Transport,
    TransportConfig,
};
