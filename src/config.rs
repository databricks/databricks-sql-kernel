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

//! Typed configuration for the v0 Kernel API.
//!
//! Per `docs/spec-v0.md` "Public Rust API → Session → Config", config is
//! grouped by concern (one sub-struct per concern), not flattened into a
//! string bag. Wrappers translate connection-string / DSN attributes into
//! these types and call `SessionBuilder` setters one per group.
//!
//! `SessionConfig` itself is the assembled object the C ABI accepts
//! directly and that `Session::open()` consumes.
//!
//! ## Scope of v0
//!
//! The v0 spec ships **lean**. Only configs we have a concrete shape for
//! land with fields:
//!
//! - [`ConnectionConfig`] (host + warehouse_id; required)
//! - [`AuthConfig`] (PAT + OAuth variants; required)
//! - [`DefaultOpts`] (catalog/schema)
//! - [`HttpConfig`] (timeouts, pool size)
//! - [`ResultConfig`] (CloudFetch tuning today; future result-management
//!   knobs slot in here too)
//! - [`LoggingConfig`] (filter, file)
//! - [`TransportMode`]
//!
//! Configs whose shape we are still settling are reserved as empty
//! `#[non_exhaustive]` structs so the public type names exist (and
//! `SessionBuilder` setters compile) without committing to fields:
//!
//! - [`ProxyConfig`]
//! - [`TlsConfig`]
//! - [`TelemetryConfig`]
//!
//! All structs are `#[non_exhaustive]` so v1+ can grow new fields
//! without breaking the public API.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

// ─── Connection identity ────────────────────────────────────────────────────

/// Required: identifies the workspace and warehouse to connect to.
///
/// `#[non_exhaustive]` — construct via [`ConnectionConfig::new`].
/// External callers can read fields directly but cannot use a struct
/// literal.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Databricks workspace URL, e.g. `https://my-workspace.databricks.com`.
    pub host: String,

    /// Warehouse ID, e.g. `abc123`. Wrappers that accept an
    /// `http_path` like `/sql/1.0/warehouses/abc123` parse the ID on
    /// their side and pass it here — the kernel does not parse
    /// http_path strings.
    pub warehouse_id: String,
}

impl ConnectionConfig {
    /// Construct a connection config from the workspace host and the
    /// warehouse id. The required entry point — `#[non_exhaustive]`
    /// blocks struct-literal construction from outside the crate.
    pub fn new(host: impl Into<String>, warehouse_id: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            warehouse_id: warehouse_id.into(),
        }
    }
}

/// Optional: per-session defaults applied to statements that don't
/// override.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct DefaultOpts {
    pub catalog: Option<String>,
    pub schema: Option<String>,
}

// ─── Authentication ─────────────────────────────────────────────────────────

/// Authentication strategy.
///
/// `External` accepts a caller-supplied token provider for federated and
/// custom auth flows.
#[non_exhaustive]
pub enum AuthConfig {
    /// Personal access token.
    Pat { token: String },

    /// OAuth 2.0 machine-to-machine (client credentials).
    OAuthM2M {
        client_id: String,
        client_secret: String,
        // Future: scopes, token_url override, etc.
    },

    /// OAuth 2.0 user-to-machine (browser flow). Future: client_id,
    /// redirect_uri, scopes, etc.
    OAuthU2M {},

    /// OAuth 2.0 token exchange (RFC 8693), for federated identity.
    /// Future: subject_token, subject_token_type, audience, etc.
    OAuthFederation {},

    /// Caller-supplied token provider. The kernel calls
    /// `TokenProvider::token()` whenever it needs an Authorization
    /// header.
    External(Box<dyn TokenProvider>),
}

/// Caller-supplied token provider trait. Stub here; full async signature
/// lands alongside the auth implementation.
pub trait TokenProvider: Send + Sync {
    // Future: async fn token(&self) -> Result<String, Error>;
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pat { .. } => f.write_str("Pat { token: <redacted> }"),
            Self::OAuthM2M { client_id, .. } => f
                .debug_struct("OAuthM2M")
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .finish(),
            Self::OAuthU2M { .. } => f.write_str("OAuthU2M { .. }"),
            Self::OAuthFederation { .. } => f.write_str("OAuthFederation { .. }"),
            Self::External(_) => f.write_str("External(<token provider>)"),
        }
    }
}

// ─── HTTP client ────────────────────────────────────────────────────────────

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// Per-request timeout. Includes connect + body.
    pub request_timeout: Duration,

    /// Maximum total time across retries for a single logical RPC.
    pub overall_timeout: Duration,

    /// Connection pool size.
    pub pool_max_idle_per_host: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(120),
            overall_timeout: Duration::from_secs(900),
            pool_max_idle_per_host: 10,
        }
    }
}

// ─── HTTP proxy (reserved) ──────────────────────────────────────────────────

/// Reserved for v0.1. Fields land once the proxy story (auth, bypass,
/// system-proxy interplay) is settled.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct ProxyConfig {}

// ─── TLS (reserved) ─────────────────────────────────────────────────────────

/// Reserved for v0.1. Fields land once the TLS story (custom CA bundles,
/// cert pinning, FIPS mode) is settled.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {}

// ─── Result management (CloudFetch + future result knobs) ───────────────────

/// Result-stream tuning. Today this is exclusively CloudFetch knobs;
/// future non-CloudFetch result-management options (chunk-size hints,
/// decompression strategy, max-row buffer, etc.) slot in here too — the
/// name is intentionally general.
///
/// Renamed from `CloudFetchConfig` so the type doesn't fossilise around
/// one transport feature.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ResultConfig {
    /// Whether to opt into CloudFetch for large results. Default: true.
    pub cloudfetch_enabled: bool,

    /// Number of presigned URLs to prefetch ahead of the consumer.
    pub cloudfetch_link_prefetch_window: usize,

    /// Maximum number of decompressed chunks held in memory at once.
    /// Bounds parallelism and peak RSS.
    pub cloudfetch_max_chunks_in_memory: usize,
}

impl Default for ResultConfig {
    fn default() -> Self {
        Self {
            cloudfetch_enabled: true,
            cloudfetch_link_prefetch_window: 10,
            cloudfetch_max_chunks_in_memory: 4,
        }
    }
}

// ─── Logging ────────────────────────────────────────────────────────────────

#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct LoggingConfig {
    /// Log filter, e.g. `databricks_sql_kernel=debug`. If `None`, the
    /// kernel honors `RUST_LOG` from the environment.
    pub filter: Option<String>,

    /// File to write logs to. If `None`, logs go to stderr.
    pub file: Option<PathBuf>,
}

// ─── Telemetry (reserved) ───────────────────────────────────────────────────

/// Reserved for v0.1. Fields land once the telemetry exporter contract
/// is finalised.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct TelemetryConfig {}

// ─── Transport mode ─────────────────────────────────────────────────────────

/// Wire-protocol selection.
///
/// v0 supports `SeaRest` only. The enum is `#[non_exhaustive]` so v1+
/// can add `SeaGrpc` / `Thrift` without breaking wrappers. The actual
/// `Transport` trait that implements each variant is `pub(crate)` —
/// internal to the kernel.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransportMode {
    /// Statement Execution API over REST. Default.
    #[default]
    SeaRest,
}

// ─── Assembled SessionConfig ────────────────────────────────────────────────

/// The fully-assembled session configuration consumed by `Session::open`
/// and by the C ABI's `kernel_session_open`.
#[non_exhaustive]
#[derive(Debug)]
pub struct SessionConfig {
    /// Required: which workspace and warehouse.
    pub connection: ConnectionConfig,

    /// Optional: default catalog / schema.
    pub defaults: Option<DefaultOpts>,

    /// Required: how to authenticate.
    pub auth: AuthConfig,

    /// Server-bound session conf. Maps to SEA `session_confs` /
    /// Thrift `configuration`. The kernel passes this through
    /// unmodified — typed sub-structs above translate the *client-side*
    /// knobs.
    pub session_conf: HashMap<String, String>,

    pub http: HttpConfig,
    pub proxy: Option<ProxyConfig>,
    pub tls: TlsConfig,
    pub result: ResultConfig,
    pub logging: LoggingConfig,
    pub telemetry: TelemetryConfig,

    /// `None` = kernel picks the default (`SeaRest` today).
    pub transport: Option<TransportMode>,
}
