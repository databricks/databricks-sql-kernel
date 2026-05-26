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

//! `openSession()` — the binding's session-construction entry point.
//!
//! The kernel collapses ADBC's `Database` + `Connection` into a single
//! `Session`. The TS adapter layer reconstructs a `DBSQLClient` /
//! `Database` wrapper on top of this binding, so the napi surface itself
//! stays flat: one free function, one opaque `Connection` class.
//!
//! A free function (not a static method on a wrapper class) because
//! there is no kernel-side `Database` state to hold — everything
//! meaningful lives on `Session`. A `Database` wrapper class with no
//! fields would add a JS object allocation per session for no benefit.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use databricks_sql_kernel::{AuthConfig, DefaultOpts, HttpConfig, Session};

/// NodeJS-driver-appropriate default for `pool_max_idle_per_host`.
///
/// Applied by [`open_session`] when the JS caller does not pass
/// `maxConnections`. Matches the JDBC driver's `HttpConnectionPoolSize`
/// default (100) and closes the throughput gap vs the NodeJS Thrift
/// driver's `maxSockets: Infinity` pool for bursty workloads
/// (concurrent `executeStatement`, parallel CloudFetch chunk downloads).
///
/// The kernel core's [`HttpConfig::pool_max_idle_per_host`] default
/// remains at the conservative kernel value (10) — each binding
/// chooses its own user-facing default. The pyo3 binding inherits
/// the kernel default; this constant is napi-specific.
pub const NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 100;

use crate::connection::Connection;
use crate::error::napi_err_from_kernel;
use crate::runtime;
use crate::util::guarded;

/// JS-visible options for opening a Databricks SQL session over PAT.
/// `token` is required.
///
/// Catalog / schema / sessionConf are applied once at session creation
/// and remain in effect for every statement run on the resulting
/// `Connection`. The SEA wire protocol carries them on
/// `CreateSession`, not on `ExecuteStatement` — so there is no
/// per-statement override path on this binding.
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi(object)]
pub struct ConnectionOptions {
    /// Workspace host, e.g. `adb-…azuredatabricks.net`. The kernel
    /// normalises this — bare hostnames get `https://` prepended.
    pub host_name: String,
    /// JDBC-style HTTP path, e.g. `/sql/1.0/warehouses/abc123`. The
    /// kernel parses out the warehouse id.
    pub http_path: String,
    /// Personal access token. Must be non-empty (the kernel rejects
    /// empty PATs at session construction).
    pub token: String,
    /// Default catalog for statements executed on this session.
    /// Routed through the kernel's `DefaultOpts` and onto the SEA
    /// `CreateSession.catalog` wire field.
    pub catalog: Option<String>,
    /// Default schema for statements executed on this session.
    /// Routed through the kernel's `DefaultOpts` and onto the SEA
    /// `CreateSession.schema` wire field.
    pub schema: Option<String>,
    /// Server-bound session conf (Spark conf, `ANSI_MODE`, `TIMEZONE`,
    /// query-tag presets, …). Forwarded verbatim to SEA
    /// `session_confs`. Unknown keys are rejected server-side.
    pub session_conf: Option<HashMap<String, String>>,
    /// Maximum number of pooled HTTP connections per host. Routes
    /// through the kernel's [`HttpConfig::pool_max_idle_per_host`].
    /// Tunes the underlying `reqwest` connection pool — higher values
    /// reduce reconnect overhead when many statements run
    /// concurrently against the same warehouse.
    ///
    /// When the JS caller does NOT provide `maxConnections`, the napi
    /// binding applies a NodeJS-driver-appropriate default of
    /// [`NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST`] (100) — chosen to match
    /// the JDBC driver's `HttpConnectionPoolSize` default and to close
    /// the throughput gap vs the NodeJS Thrift driver's
    /// `maxSockets: Infinity` pool for bursty workloads. The kernel
    /// core's [`HttpConfig::pool_max_idle_per_host`] default remains
    /// at the conservative kernel value (10); each binding chooses
    /// its own user-facing default. Mirrors the Python connector's
    /// `max_connections` kwarg on the SEA backend, which exposes the
    /// knob but keeps its own urllib3-aligned default of 10.
    ///
    /// Napi-rs serialises `u32` as JS `number`; values up to
    /// `2^32 - 1` round-trip safely (any reasonable pool size fits).
    pub max_connections: Option<u32>,
}

/// Open a Databricks SQL session over PAT auth and return an opaque
/// `Connection` wrapping the kernel `Session`.
///
/// The JS-visible name is `openSession` (napi-rs converts snake_case
/// to camelCase for free functions).
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi]
pub async fn open_session(options: ConnectionOptions) -> napi::Result<Connection> {
    guarded(async move {
        // Cache the napi-rs tokio Handle on the very first async call
        // so Drop impls (which run on the V8 GC thread, outside any
        // tokio context) can still `spawn` cleanup tasks onto the
        // runtime that's driving this future.
        let _ = runtime::get_handle();

        // SessionConfig is `#[non_exhaustive]` — go through the
        // builder, which is the only public path that constructs it.
        // `http_path()` is the convenience setter that maps a bare
        // hostname + `/sql/1.0/warehouses/{id}` path into the kernel's
        // `ConnectionConfig`.
        let mut builder = Session::builder()
            .http_path(options.host_name, options.http_path)
            .auth(AuthConfig::Pat {
                token: options.token,
            });
        // Only wire `DefaultOpts` when at least one of catalog / schema
        // was provided; otherwise leave the config's `defaults` field
        // `None` so the kernel can distinguish "no defaults" from
        // "empty defaults" if that distinction ever becomes meaningful.
        // `DefaultOpts` is `#[non_exhaustive]` — must construct via
        // `default()` and mutate fields.
        if options.catalog.is_some() || options.schema.is_some() {
            let mut defaults = DefaultOpts::default();
            defaults.catalog = options.catalog;
            defaults.schema = options.schema;
            builder = builder.defaults(defaults);
        }
        if let Some(conf) = options.session_conf {
            builder = builder.session_conf(conf);
        }
        // `HttpConfig` is `#[non_exhaustive]` — must construct via
        // `default()` then mutate. The napi binding ALWAYS sets
        // `pool_max_idle_per_host` so the NodeJS-driver-appropriate
        // default ([`NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST`] = 100)
        // applies even when the JS caller doesn't pass `maxConnections`.
        // The kernel core's `HttpConfig::default()` (10) is intentionally
        // conservative; each binding chooses its own user-facing default.
        let mut http = HttpConfig::default();
        http.pool_max_idle_per_host = options
            .max_connections
            .map(|max| max as usize)
            .unwrap_or(NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST);
        builder = builder.http(http);
        let session = builder.open().await.map_err(napi_err_from_kernel)?;
        let session_id = session.session_id().to_owned();
        Ok(Connection {
            inner: Arc::new(Mutex::new(Some(session))),
            session_id,
        })
    })
    .await
}
