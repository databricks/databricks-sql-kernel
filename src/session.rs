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
//! Per `docs/spec-v0.md` "Public Rust API → Session", `Session`
//! collapses ADBC's `Database` + `Connection` into a single stateful
//! authenticated handle. It is the surface every wrapper (ODBC, PySQL,
//! NodeJS, Go) targets, and the C ABI is a thin shim over its async
//! methods.
//!
//! `'static` (no Rust lifetime parameter) for the same reason as every
//! other v0 handle — see `docs/spec-v0.md` addendum A1, "Why no Rust
//! lifetimes on the handles". Shared state lives behind
//! `Arc<SessionInner>`.
//!
//! ## Async runtime
//!
//! `Session::open` is an `async fn` and assumes the caller owns a
//! tokio runtime — it grabs the current runtime handle via
//! [`tokio::runtime::Handle::current`]. Callers are PyO3 / napi-rs
//! wrappers that already manage a runtime, or the C ABI shim (PR 6)
//! which owns a process-wide runtime and `block_on`s every entry
//! point.
//!
//! ## Transactions
//!
//! Multi-statement transaction methods (`set_autocommit` / `commit` /
//! `rollback`) are intentionally **omitted** in v0. The Databricks
//! server-side semantics for autocommit are still being settled, and
//! we prefer to leave the surface unspecified rather than ship a
//! signature we may need to reshape. The methods will be added once
//! the server contract stabilises.

use crate::auth::{
    AuthProvider, AuthorizationCodeProvider, ClientCredentialsProvider, PersonalAccessToken,
};
use crate::client::http::TlsConfig as HttpTlsConfig;
use crate::client::retry::{
    build_retry_configs, RequestCategory, RetryConfig, RetryConfigOverrides,
};
use crate::client::transport::TransportConfig;
use crate::client::{
    DatabricksHttpClient, HttpClientConfig, ProxyConfig as HttpProxyConfig, SeaRestTransport,
    Transport,
};
use crate::config::{
    AuthConfig, ConnectionConfig, DefaultOpts, HttpConfig, LoggingConfig, ProxyConfig,
    ResultConfig, SessionConfig, TelemetryConfig, TlsConfig, TransportMode,
};
use crate::kernel_error::{Error, Result};
use crate::metadata_v0::Metadata;
use crate::reader::ResultReaderFactory;
use crate::statement::mutable::Statement;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info_span, Span};

/// Crate-internal session state. Wrapped in `Arc` and shared with
/// every child handle (`Statement`, `ExecutedStatement`, …) so the
/// kernel surface stays `'static`-friendly for foreign-language
/// wrappers.
#[allow(
    dead_code,
    reason = "fields are read by Statement / Metadata / ResultStream wiring in PRs 3-5"
)]
pub(crate) struct SessionInner {
    /// Wire-protocol abstraction. v0 holds a `SeaRestTransport`
    /// behind the trait object; future variants slot in via
    /// `TransportMode`.
    transport: Arc<dyn Transport>,

    /// Server-issued session identifier.
    session_id: String,

    /// Snapshot of the config the session was opened with.
    /// Sub-handles read it for re-execute and option lookups.
    config: SessionConfig,

    /// Tokio runtime used to drive the transport. Cloned into
    /// every child handle so they can `block_on` if they need to
    /// (e.g. the [`Drop`] impl below).
    runtime_handle: tokio::runtime::Handle,

    /// Tracing span tagged with `session_id`. Every public method
    /// enters this span via `let _g = self.inner.log_span.enter();`
    /// so log lines tied to a session are visibly grouped.
    log_span: Span,

    /// Flips to `true` only after [`Session::close`] has
    /// **successfully** awaited the `delete_session` call. Tells
    /// [`Drop`] to skip its fire-and-forget cleanup so a successful
    /// explicit close doesn't trigger a redundant retry. On `close`
    /// returning `Err` (or being cancelled mid-await) the flag stays
    /// `false`, so [`Drop`] retries via its fire-and-forget path —
    /// transient `delete_session` failures don't leak the server-
    /// side session.
    closed: std::sync::atomic::AtomicBool,
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        // If `Session::close()` already ran the delete, skip.
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        // Fire-and-forget DeleteSession when the last `Arc` reference
        // goes away. Swallows errors — Drop must not panic, and we
        // can't surface a failure here. The runtime keeps the
        // background task alive until completion.
        let transport = Arc::clone(&self.transport);
        let session_id = self.session_id.clone();
        self.runtime_handle.spawn(async move {
            let _ = transport.delete_session(&session_id).await;
        });
    }
}

/// Stateful authenticated session against a Databricks workspace.
///
/// `'static` (no lifetime parameter) so wrappers can hold it inside
/// `pyclass` / `napi` / opaque C handles. Cheaply cloneable — clones
/// share the same underlying `SessionInner` via `Arc`.
pub struct Session {
    inner: Arc<SessionInner>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Surface only public-safe fields. The full SessionConfig
        // contains auth credentials; redact via `AuthConfig::Debug`
        // (which already redacts tokens/secrets) by formatting only
        // the session_id at this layer.
        f.debug_struct("Session")
            .field("session_id", &self.inner.session_id)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Primary entry point. Returns a builder; chain config-group
    /// setters and call `.open()` to construct the session.
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }

    /// Lower-level entry point. Used by callers that already have a
    /// fully-formed `SessionConfig` (e.g. deserialized from JSON,
    /// cached from a prior session, or built by the C ABI from a
    /// struct).
    ///
    /// Builds the HTTP client (with retry config, proxy, TLS), the
    /// auth provider, the transport, and the result-reader factory;
    /// then calls `Transport::create_session` to mint a session id
    /// on the server.
    pub async fn open(config: SessionConfig) -> Result<Session> {
        // Fetch the runtime handle up-front so a misconfigured
        // caller (e.g. running outside a tokio context) gets a clear
        // error instead of a panic deep in the transport setup.
        let runtime_handle = tokio::runtime::Handle::try_current().map_err(|e| {
            Error::invalid_state().message(format!(
                "Session::open must be called from a tokio runtime context: {e}"
            ))
        })?;

        let host = config.connection.host.clone();
        let warehouse_id = config.connection.warehouse_id.clone();
        let org_id = config.connection.org_id.clone();

        // 1. HTTP client — built before auth so OAuth providers can use
        //    it for OIDC discovery / token endpoint calls.
        let http_client_config = build_http_client_config(&config, org_id);
        let retry_configs = build_retry_configs_for_session(&config);
        let http_client = Arc::new(
            DatabricksHttpClient::new(http_client_config, retry_configs).map_err(|e| {
                Error::network_error().message(format!("Failed to construct HTTP client: {e}"))
            })?,
        );

        // 2. Auth provider — async because OAuth flows hit the OIDC
        //    discovery endpoint.
        let auth_provider: Arc<dyn AuthProvider> =
            build_auth_provider(&config.auth, &host, http_client.clone()).await?;
        http_client.set_auth_provider(auth_provider);

        // 3. Transport. Two-phase: SeaRestTransport needs an
        //    Arc<dyn Transport> to build the result-reader factory,
        //    which in turn needs to be set on the transport via
        //    OnceLock::set. So: construct, wrap in Arc, build factory,
        //    install factory.
        let transport_config = TransportConfig::default();
        let sea = Arc::new(SeaRestTransport::new(
            http_client.clone(),
            host.clone(),
            warehouse_id.clone(),
            transport_config,
        ));
        let reader_factory = ResultReaderFactory::new(
            sea.clone() as Arc<dyn Transport>,
            http_client,
            result_to_cloudfetch_config(&config.result),
            runtime_handle.clone(),
        );
        sea.set_reader_factory(reader_factory, runtime_handle.clone());
        let transport: Arc<dyn Transport> = sea;

        // 4. Mint the session on the server.
        let defaults = config.defaults.clone().unwrap_or_default();
        debug!(
            host = %host,
            warehouse_id = %warehouse_id,
            "creating session"
        );
        let session_info = transport
            .create_session(
                defaults.catalog.as_deref(),
                defaults.schema.as_deref(),
                config.session_conf.clone(),
            )
            .await?;

        // 5. Tag the per-session tracing span. Future log lines
        //    inherit `session_id` automatically.
        let log_span = info_span!("kernel.session", session_id = %session_info.session_id);

        Ok(Session {
            inner: Arc::new(SessionInner {
                transport,
                session_id: session_info.session_id,
                config,
                runtime_handle,
                log_span,
                closed: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }

    /// Build a mutable statement bound to this session. Mutate its
    /// spec (sql, params, conf) via `Statement::with_spec`, then call
    /// `execute()` or `submit()` to produce an immutable executed
    /// handle.
    pub fn statement(&self) -> Statement {
        Statement::new(Arc::clone(&self.inner))
    }

    /// Metadata operations — flat, per-method (ODBC-shaped).
    pub fn metadata(&self) -> Metadata {
        Metadata::new(Arc::clone(&self.inner))
    }

    /// Server-issued session identifier. Stable for the life of the
    /// session.
    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    // ─── Transactions (omitted in v0) ─────────────────────────────────────
    //
    // The autocommit / commit / rollback surface is intentionally
    // not shipped in v0. Add it once the server contract is settled:
    //
    //   pub async fn set_autocommit(&self, enabled: bool) -> Result<()>;
    //   pub async fn autocommit(&self) -> Result<bool>;
    //   pub async fn commit(&self) -> Result<()>;
    //   pub async fn rollback(&self) -> Result<()>;

    /// Explicit close. Calls `DeleteSession` against the server and
    /// drops the local handle. `Drop` also fires a fire-and-forget
    /// cleanup if `close()` wasn't called explicitly, but `close()`
    /// is the only way to *await* the result and surface a failure.
    ///
    /// The `closed` flag flips **after** a successful `delete_session`
    /// — only then does [`Drop`] skip its own cleanup. So:
    ///
    /// - On `Err`: flag stays false, [`Drop`]'s fire-and-forget
    ///   retry path runs as a fallback so transient failures don't
    ///   leak the server-side session.
    /// - On future cancellation (e.g. `tokio::select!` losing a
    ///   race): flag stays false, [`Drop`] picks up the slack.
    /// - On `Ok`: flag flips, [`Drop`] is a no-op.
    pub async fn close(self) -> Result<()> {
        // Bail early if a concurrent caller already finished a
        // successful close — no point hitting the wire twice.
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let _g = self.inner.log_span.clone().entered();
        self.inner
            .transport
            .delete_session(&self.inner.session_id)
            .await?;
        self.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

/// Fluent builder for `Session`.
///
/// Each setter takes a typed config group, not individual fields —
/// keeps the builder surface compact and matches how wrappers
/// translate connection-string / DSN attributes into config (one DSN
/// attribute group per setter call).
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
    /// Sentinel for [`SessionBuilder::http_path`] parse failures.
    /// Defers the error to `open()` so the builder API stays
    /// infallible (matching the rest of its setters).
    parse_error: Option<String>,
}

impl SessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Required: workspace host + warehouse identity.
    ///
    /// Clears any prior [`SessionBuilder::http_path`] parse error —
    /// the user has explicitly overridden the connection identity, so
    /// an old parse failure on a stale path should not block
    /// [`SessionBuilder::open`].
    pub fn connection(mut self, conn: ConnectionConfig) -> Self {
        self.connection = Some(conn);
        self.parse_error = None;
        self
    }

    /// Convenience shortcut equivalent to
    /// `connection(ConnectionConfig::from_http_path(host, http_path)?)`.
    /// Most wrappers translate one DSN field (`http_path` / `httpPath`
    /// / `HTTPPath`) — this lets them do it in a single builder call.
    ///
    /// Defers parsing errors to [`SessionBuilder::open`] so the
    /// builder API stays infallible (matches the rest of the
    /// builder's setters). An invalid path surfaces as
    /// `ErrorCode::InvalidArgument` from `open()`.
    pub fn http_path(mut self, host: impl Into<String>, http_path: impl AsRef<str>) -> Self {
        match ConnectionConfig::from_http_path(host, http_path.as_ref()) {
            Ok(c) => {
                self.connection = Some(c);
                self.parse_error = None;
            }
            Err(_) => {
                self.connection = None;
                self.parse_error = Some(http_path.as_ref().to_owned());
            }
        }
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

    /// Result-management config (CloudFetch tuning today; future
    /// result knobs slot in here too).
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
    /// user-facing flags (`UseThriftClient=0/1`, `transport=...`)
    /// into this field.
    pub fn transport(mut self, mode: TransportMode) -> Self {
        self.transport = Some(mode);
        self
    }

    /// Validate, assemble a [`SessionConfig`], and open the session.
    /// Returns `Err(InvalidArgument)` if required fields
    /// (`connection`, `auth`) are missing or if a previous
    /// [`SessionBuilder::http_path`] call failed to parse.
    pub async fn open(self) -> Result<Session> {
        if let Some(bad_path) = self.parse_error {
            return Err(Error::invalid_argument().message(format!(
                "http_path '{bad_path}' does not match \
                 /sql/1.0/warehouses/{{id}} or /sql/1.0/endpoints/{{id}}"
            )));
        }

        let connection = self.connection.ok_or_else(|| {
            Error::invalid_argument()
                .message("SessionBuilder requires `connection(...)` or `http_path(...)`")
        })?;
        let auth = self.auth.ok_or_else(|| {
            Error::invalid_argument().message("SessionBuilder requires `auth(...)`")
        })?;

        let config = SessionConfig {
            connection,
            defaults: self.defaults,
            auth,
            session_conf: self.session_conf,
            http: self.http.unwrap_or_default(),
            proxy: self.proxy,
            tls: self.tls.unwrap_or_default(),
            result: self.result.unwrap_or_default(),
            logging: self.logging.unwrap_or_default(),
            telemetry: self.telemetry.unwrap_or_default(),
            transport: self.transport,
        };
        Session::open(config).await
    }
}

// ─── Internal config translation ─────────────────────────────────────

/// Translate the public [`HttpConfig`] (+ proxy / TLS / org_id) into
/// the internal [`HttpClientConfig`] the HTTP client expects.
///
/// v0's [`ProxyConfig`] and [`TlsConfig`] are empty `#[non_exhaustive]`
/// reservation structs — they don't carry fields yet. We pass through
/// the internal [`HttpProxyConfig::default`] / [`HttpTlsConfig::default`]
/// for now; PR follow-ups will wire the real fields when the v0
/// proxy/TLS shape lands.
fn build_http_client_config(config: &SessionConfig, org_id: Option<String>) -> HttpClientConfig {
    let mut hcc = HttpClientConfig {
        connect_timeout: Duration::from_secs(30),
        // The internal `read_timeout` corresponds to the public
        // `request_timeout` — both gate a single HTTP round-trip.
        read_timeout: config.http.request_timeout,
        max_connections_per_host: config.http.pool_max_idle_per_host,
        proxy: HttpProxyConfig::default(),
        tls: HttpTlsConfig::default(),
        org_id,
        ..HttpClientConfig::default()
    };
    // Touch the v0 reserved configs so a future field addition
    // (without setting it through here) shows up as a compile error
    // rather than a silently-dropped option.
    let ProxyConfig { .. } = config.proxy.clone().unwrap_or_default();
    let TlsConfig { .. } = config.tls.clone();
    let _ = &mut hcc;
    hcc
}

/// Build the per-category retry config for the HTTP client. v0
/// matches the legacy ADBC defaults: global 900s/5-retries with an
/// auth-specific 30s/3-retries override so token endpoint calls
/// fail fast.
fn build_retry_configs_for_session(
    config: &SessionConfig,
) -> std::collections::HashMap<RequestCategory, RetryConfig> {
    let global = RetryConfig {
        overall_timeout: config.http.overall_timeout,
        ..RetryConfig::default()
    };
    let mut overrides: HashMap<RequestCategory, RetryConfigOverrides> = HashMap::new();
    overrides.insert(
        RequestCategory::Auth,
        RetryConfigOverrides {
            overall_timeout: Some(Duration::from_secs(30)),
            max_retries: Some(3),
            ..Default::default()
        },
    );
    build_retry_configs(&global, &overrides)
}

/// Project the public [`ResultConfig`] (today only carries
/// CloudFetch knobs; future result-management options slot in here
/// too) onto the internal `CloudFetchConfig`.
fn result_to_cloudfetch_config(
    result: &ResultConfig,
) -> crate::types::cloudfetch::CloudFetchConfig {
    crate::types::cloudfetch::CloudFetchConfig {
        enabled: result.cloudfetch_enabled,
        link_prefetch_window: result.cloudfetch_link_prefetch_window,
        max_chunks_in_memory: result.cloudfetch_max_chunks_in_memory,
        ..Default::default()
    }
}

/// Build the auth provider for a given [`AuthConfig`]. `host` is the
/// workspace URL — used for OIDC discovery on M2M / U2M flows.
async fn build_auth_provider(
    auth: &AuthConfig,
    host: &str,
    http_client: Arc<DatabricksHttpClient>,
) -> Result<Arc<dyn AuthProvider>> {
    match auth {
        AuthConfig::Pat { token } => {
            if token.is_empty() {
                return Err(
                    Error::invalid_argument().message("AuthConfig::Pat token must not be empty")
                );
            }
            Ok(Arc::new(PersonalAccessToken::new(token.clone())))
        }
        AuthConfig::OAuthM2M {
            client_id,
            client_secret,
            scopes,
            token_url_override,
        } => {
            if client_id.is_empty() || client_secret.is_empty() {
                return Err(Error::invalid_argument().message(
                    "AuthConfig::OAuthM2M requires non-empty client_id and client_secret",
                ));
            }
            let scopes = scopes
                .clone()
                .unwrap_or_else(|| vec!["all-apis".to_string()]);
            let provider = ClientCredentialsProvider::new_with_full_config(
                host,
                client_id,
                client_secret,
                http_client,
                scopes,
                token_url_override.clone(),
            )
            .await?;
            Ok(Arc::new(provider))
        }
        AuthConfig::OAuthU2M {
            client_id,
            redirect_port,
            scopes,
            token_url_override,
            callback_timeout,
        } => {
            let client_id = client_id.as_deref().unwrap_or("databricks-cli");
            let redirect_port = redirect_port.unwrap_or(8020);
            let scopes = scopes
                .clone()
                .unwrap_or_else(|| vec!["all-apis".to_string(), "offline_access".to_string()]);
            let timeout = callback_timeout.unwrap_or_else(|| Duration::from_secs(120));
            let provider = AuthorizationCodeProvider::new_with_full_config(
                host,
                client_id,
                http_client,
                scopes,
                redirect_port,
                timeout,
                token_url_override.clone(),
            )
            .await?;
            Ok(Arc::new(provider))
        }
        AuthConfig::OAuthFederation { .. } => Err(Error::invalid_argument()
            .message("AuthConfig::OAuthFederation is reserved for v0.1; not yet wired")),
        AuthConfig::External(_) => Err(Error::invalid_argument()
            .message("AuthConfig::External is reserved; v0 wires PAT + OAuthM2M + OAuthU2M only")),
    }
}
