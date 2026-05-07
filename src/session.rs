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

//! `Session` — the v0 Kernel entry point.
//!
//! Per `docs/spec-v0.md` "Public Rust API → Session", `Session` collapses
//! ADBC's `Database` + `Connection` into a single stateful authenticated
//! handle. It is the surface every wrapper (ODBC, PySQL, NodeJS, Go)
//! targets, and the C ABI is a thin shim over its async methods.
//!
//! `'static` (no Rust lifetime parameter) for the same reason as every
//! other v0 handle — see `docs/spec-v0.md` addendum A1, "Why no Rust
//! lifetimes on the handles". Shared state lives behind
//! `Arc<SessionInner>`.
//!
//! ## Transactions
//!
//! Multi-statement transaction methods (`set_autocommit` / `commit` /
//! `rollback`) are intentionally **omitted** in v0. The Databricks
//! server-side semantics for autocommit are still being settled, and we
//! prefer to leave the surface unspecified rather than ship a signature
//! we may need to reshape. The methods will be added once the server
//! contract stabilises.

#![allow(dead_code, unused_variables)]

use crate::config::{
    AuthConfig, ConnectionConfig, DefaultOpts, HttpConfig, LoggingConfig, ProxyConfig,
    ResultConfig, SessionConfig, TelemetryConfig, TlsConfig, TransportMode,
};
use crate::kernel_error::Result;
use crate::metadata_v0::Metadata;
use crate::statement::mutable::Statement;
use std::collections::HashMap;
use std::sync::Arc;

/// Crate-internal session state. Wrapped in `Arc` and shared with every
/// child handle (`Statement`, `ExecutedStatement`, …) so the kernel
/// surface stays `'static`-friendly for foreign-language wrappers.
///
/// Body lands in PR 4a alongside the transport plumbing; for now this is
/// an empty stub.
pub(crate) struct SessionInner {
    // PR 4a: transport: Arc<dyn Transport>, session_info, config, ...
}

/// Stateful authenticated session against a Databricks workspace.
///
/// `'static` (no lifetime parameter) so wrappers can hold it inside
/// `pyclass` / `napi` / opaque C handles. Cheaply cloneable — clones
/// share the same underlying `SessionInner` via `Arc`.
pub struct Session {
    inner: Arc<SessionInner>,
}

impl Session {
    /// Primary entry point. Returns a builder; chain config-group setters
    /// and call `.open()` to construct the session.
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }

    /// Lower-level entry point. Used by callers that already have a
    /// fully-formed `SessionConfig` (e.g. deserialized from JSON, cached
    /// from a prior session, or built by the C ABI from a struct).
    pub async fn open(config: SessionConfig) -> Result<Session> {
        unimplemented!("PR 4a: implement open()")
    }

    /// Build a mutable statement bound to this session. Mutate its spec
    /// (sql, params, conf) via `Statement::spec()`, then call `execute()`
    /// or `submit()` to produce an immutable executed handle.
    pub fn statement(&self) -> Statement {
        Statement::new(Arc::clone(&self.inner))
    }

    /// Metadata operations — flat, per-method (ODBC-shaped).
    pub fn metadata(&self) -> Metadata {
        Metadata::new(Arc::clone(&self.inner))
    }

    // ─── Transactions (omitted in v0) ─────────────────────────────────────
    //
    // The autocommit / commit / rollback surface is intentionally not
    // shipped in v0. Add it once the server contract is settled:
    //
    //   pub async fn set_autocommit(&self, enabled: bool) -> Result<()>;
    //   pub async fn autocommit(&self) -> Result<bool>;
    //   pub async fn commit(&self) -> Result<()>;
    //   pub async fn rollback(&self) -> Result<()>;

    /// Explicit close. `Drop` also fires-and-forgets cleanup.
    pub async fn close(self) -> Result<()> {
        unimplemented!("PR 4a: implement close()")
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // PR 4a: if this is the last `Arc` (refcount drops to 0 inside
        // `SessionInner::Drop`), fire-and-forget DeleteSession against
        // the server so the workspace doesn't accumulate orphaned
        // sessions when callers skip explicit `close().await`.
    }
}

/// Fluent builder for `Session`.
///
/// Each setter takes a typed config group, not individual fields — keeps
/// the builder surface compact and matches how wrappers translate
/// connection-string / DSN attributes into config (one DSN attribute
/// group per setter call).
#[non_exhaustive]
#[derive(Default)]
pub struct SessionBuilder {
    connection: Option<ConnectionConfig>,
    auth: Option<AuthConfig>,
    defaults: Option<DefaultOpts>,
    http: Option<HttpConfig>,
    proxy: Option<ProxyConfig>,
    tls: Option<TlsConfig>,
    result: Option<ResultConfig>,
    logging: Option<LoggingConfig>,
    telemetry: Option<TelemetryConfig>,
    session_conf: HashMap<String, String>,
    transport: Option<TransportMode>,
}

impl SessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Required: workspace host + warehouse identity.
    pub fn connection(mut self, conn: ConnectionConfig) -> Self {
        self.connection = Some(conn);
        self
    }

    /// Required: how to authenticate.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Default catalog / schema applied to statements that don't
    /// override.
    pub fn defaults(mut self, defaults: DefaultOpts) -> Self {
        self.defaults = Some(defaults);
        self
    }

    /// HTTP client tuning — request / overall timeouts, connection-pool
    /// size. Defaults are sane for typical workloads.
    pub fn http(mut self, http: HttpConfig) -> Self {
        self.http = Some(http);
        self
    }

    /// HTTP proxy. Reserved in v0 (`#[non_exhaustive]` empty struct);
    /// fields land in v0.1.
    pub fn proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// TLS settings (custom CA, hostname-mismatch tolerance, …).
    /// Reserved in v0 (`#[non_exhaustive]` empty struct); fields land
    /// in v0.1.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Result-management config (CloudFetch tuning today; future result
    /// knobs slot in here too).
    pub fn result(mut self, result: ResultConfig) -> Self {
        self.result = Some(result);
        self
    }

    /// Logging — filter expression, output destination.
    pub fn logging(mut self, logging: LoggingConfig) -> Self {
        self.logging = Some(logging);
        self
    }

    /// Telemetry exporter. Reserved in v0 (`#[non_exhaustive]` empty
    /// struct); fields land in v0.1.
    pub fn telemetry(mut self, telemetry: TelemetryConfig) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Server-bound session conf passed through to SEA `session_confs`.
    pub fn session_conf(mut self, conf: HashMap<String, String>) -> Self {
        self.session_conf = conf;
        self
    }

    /// Transport override. Default = `SeaRest`. Wrappers translate
    /// user-facing flags (`UseThriftClient=0/1`, `transport=...`) into
    /// this field.
    pub fn transport(mut self, mode: TransportMode) -> Self {
        self.transport = Some(mode);
        self
    }

    /// Validate, assemble a `SessionConfig`, and open the session.
    /// Returns `Err(InvalidArgument)` if required fields (`connection`,
    /// `auth`) are missing.
    pub async fn open(self) -> Result<Session> {
        unimplemented!("PR 4a: validate, assemble SessionConfig, call Session::open")
    }
}
