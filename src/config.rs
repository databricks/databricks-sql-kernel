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
//! [`TlsConfig`] now carries custom-CA + self-signed / hostname
//! relaxation knobs; configs whose shape we are still settling stay
//! as empty `#[non_exhaustive]` structs so the public type names
//! exist (and `SessionBuilder` setters compile) without committing
//! to fields:
//!
//! - [`ProxyConfig`]
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
/// `#[non_exhaustive]` — construct via [`ConnectionConfig::new`] or
/// [`ConnectionConfig::from_http_path`]. External callers can read
/// fields directly but cannot use a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Databricks workspace URL, e.g. `https://my-workspace.databricks.com`.
    pub host: String,

    /// Warehouse ID, e.g. `abc123`. Wrappers that have an `http_path`
    /// string (`/sql/1.0/warehouses/abc123`) should use
    /// [`ConnectionConfig::from_http_path`] which parses the id out.
    pub warehouse_id: String,

    /// Workspace organization ID for multi-tenant routing. When set,
    /// the kernel sends it as the `x-databricks-org-id` header on
    /// every request. Populated automatically from the `?o=` query
    /// parameter when constructing via
    /// [`ConnectionConfig::from_http_path`].
    pub org_id: Option<String>,
}

impl ConnectionConfig {
    /// Construct a connection config from the workspace host and a
    /// bare warehouse id (no `/sql/1.0/...` prefix).
    ///
    /// `host` is normalised: `https://` is prepended if no scheme is
    /// present, and any trailing `/` is stripped. So `"my-workspace.databricks.com"`,
    /// `"my-workspace.databricks.com/"`, and
    /// `"https://my-workspace.databricks.com"` all produce the same
    /// canonical `host`.
    pub fn new(host: impl Into<String>, warehouse_id: impl Into<String>) -> Self {
        Self {
            host: normalise_host(host.into()),
            warehouse_id: warehouse_id.into(),
            org_id: None,
        }
    }

    /// Construct a connection config from a workspace host and a
    /// JDBC-style HTTP path. Accepts:
    ///
    /// - `/sql/1.0/warehouses/{id}` (canonical form)
    /// - `/sql/1.0/endpoints/{id}` (legacy form, equivalent on the server)
    /// - With or without leading `/`
    /// - With an optional `?o={org_id}` query parameter (multi-tenant
    ///   workspaces); the org id is captured into [`Self::org_id`].
    ///
    /// `host` is normalised the same way as in
    /// [`ConnectionConfig::new`]. Returns
    /// `Err(ErrorCode::InvalidArgument)` if the path doesn't match
    /// any of the recognised forms.
    pub fn from_http_path(
        host: impl Into<String>,
        http_path: &str,
    ) -> crate::kernel_error::Result<Self> {
        let warehouse_id = parse_warehouse_id(http_path).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument().message(format!(
                "http_path '{http_path}' does not match \
                 /sql/1.0/warehouses/{{id}} or /sql/1.0/endpoints/{{id}}"
            ))
        })?;
        Ok(Self {
            host: normalise_host(host.into()),
            warehouse_id,
            org_id: parse_org_id(http_path),
        })
    }
}

/// Normalise a workspace host: prepend `https://` if no scheme is
/// present, and strip any trailing `/`. Done eagerly at config
/// construction so the rest of the kernel never has to second-guess
/// the input shape.
fn normalise_host(host: String) -> String {
    let with_scheme = if host.starts_with("http://") || host.starts_with("https://") {
        host
    } else {
        format!("https://{host}")
    };
    with_scheme.trim_end_matches('/').to_string()
}

/// Parse a warehouse id out of a JDBC-style HTTP path. Accepts the
/// `/sql/1.0/warehouses/{id}` and `/sql/1.0/endpoints/{id}` forms,
/// with or without a leading slash, with an optional `?o={org_id}`
/// query suffix that is stripped from the returned id.
fn parse_warehouse_id(http_path: &str) -> Option<String> {
    http_path
        .strip_prefix("/sql/1.0/warehouses/")
        .or_else(|| http_path.strip_prefix("sql/1.0/warehouses/"))
        .or_else(|| http_path.strip_prefix("/sql/1.0/endpoints/"))
        .or_else(|| http_path.strip_prefix("sql/1.0/endpoints/"))
        .map(|s| {
            // Strip trailing `/` and any query string.
            let s = s.trim_end_matches('/');
            s.split('?').next().unwrap_or(s).to_string()
        })
        .filter(|s| !s.is_empty())
}

/// Parse the `?o={org_id}` query parameter if present. Empty values
/// (`?o=`) are treated as absent.
fn parse_org_id(http_path: &str) -> Option<String> {
    let query = http_path.split('?').nth(1)?;
    query
        .split('&')
        .find_map(|param| param.strip_prefix("o="))
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
}

/// Optional: per-session defaults applied to statements that don't
/// override.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DefaultOpts {
    pub catalog: Option<String>,
    pub schema: Option<String>,

    /// When `true` (the default), transaction-control operations are
    /// silently no-oped: `Session::set_autocommit(false)`,
    /// `Session::commit()`, and `Session::rollback()` return `Ok(())`
    /// without issuing any server statement. Mirrors the reference/JDBC
    /// `IgnoreTransactions` connection property, whose default is `1`
    /// (transactions disabled). Set to `false` to opt into real
    /// multi-statement transactions (a Private Preview feature; requires
    /// catalog-managed tables server-side).
    pub ignore_transactions: bool,
}

impl Default for DefaultOpts {
    fn default() -> Self {
        Self {
            catalog: None,
            schema: None,
            // Match the reference/JDBC default of IgnoreTransactions=1:
            // transaction control is no-oped unless a caller opts in.
            ignore_transactions: true,
        }
    }
}

// ─── Token cache ────────────────────────────────────────────────────────────

/// Policy for the on-disk U2M OAuth token cache.
///
/// The cache persists the refresh token so a user is not sent through the
/// browser flow on every connection. Because a refresh token is a
/// long-lived credential, the kernel only ever writes one **encrypted** —
/// there is no plaintext mode.
///
/// | `enabled` | `passphrase` | result |
/// |---|---|---|
/// | `true` (default) | set | encrypted under the caller's passphrase |
/// | `true` (default) | unset | encrypted under a machine-local derived key |
/// | `false` | either | no disk reads or writes |
///
/// **Why the middle row is not an error.** The reference ODBC driver ships
/// `EnableTokenCache=1` with `TokenCachePassPhrase` empty and still
/// connects, so failing the connection there would break drop-in
/// compatibility for any DSN that never set a passphrase. (The JDBC driver
/// *does* fail in that case — `ClientConfigurator` throws "No token cache
/// passphrase configured" — but for the ODBC surface the reference driver
/// is the compatibility bar, and its behaviour is also the friendlier of
/// the two.) A supplied passphrase is still strictly better: see
/// [`super::auth::oauth::cache_crypto::derived_passphrase`] for exactly what
/// the fallback key protects against and what it does not.
///
/// Disabling the cache stops **disk persistence only**. The in-memory token
/// store still reuses and refreshes a token for the lifetime of the
/// session, matching JDBC's `NoOpTokenCache`, so turning this off does not
/// re-trigger the browser flow on every refresh within one process.
#[derive(Debug, Clone)]
pub struct TokenCacheConfig {
    /// Whether to read and write the on-disk cache. Default `true`,
    /// matching both the reference ODBC driver's and JDBC's
    /// `EnableTokenCache=1`.
    pub enabled: bool,
    /// Passphrase for the AES-256 encryption key. Optional: when absent (or
    /// blank) the kernel derives a machine-local key instead, matching the
    /// reference driver, which caches with an empty `TokenCachePassPhrase`.
    /// Supplying one is stronger — the cache then cannot be read without a
    /// secret the machine does not hold.
    pub passphrase: Option<String>,
}

impl Default for TokenCacheConfig {
    fn default() -> Self {
        Self {
            // Match the reference driver's / JDBC's EnableTokenCache=1.
            enabled: true,
            passphrase: None,
        }
    }
}

impl TokenCacheConfig {
    /// Resolve to the passphrase to encrypt with, or `None` when disk
    /// caching is disabled.
    ///
    /// A blank passphrase is treated as absent rather than honoured: it is
    /// almost always an unexpanded config placeholder (`$TOKEN_PASS` that
    /// never got substituted), and deriving a key from `""` across every
    /// machine would be materially weaker than the machine-local fallback.
    ///
    /// Infallible by design. Enabled-without-passphrase is a supported
    /// configuration, not an error, so this cannot fail the connection —
    /// see the type docs for why.
    pub(crate) fn resolve(&self) -> Option<std::borrow::Cow<'_, str>> {
        if !self.enabled {
            return None;
        }
        match self.passphrase.as_deref() {
            Some(p) if !p.trim().is_empty() => Some(std::borrow::Cow::Borrowed(p)),
            _ => Some(std::borrow::Cow::Owned(
                crate::auth::oauth::cache_crypto::derived_passphrase(),
            )),
        }
    }
}

// ─── Authentication ─────────────────────────────────────────────────────────

/// Authentication strategy.
///
/// `#[non_exhaustive]` so v1+ can add variants (e.g. an Azure-AD
/// service-principal shortcut) without breaking wrappers.
///
/// Optional fields default to sensible values when set to `None`:
/// - OAuth scopes default to `["all-apis"]` for M2M and
///   `["all-apis", "offline_access"]` for U2M.
/// - U2M `redirect_port` defaults to `8020`.
/// - U2M `client_id` defaults to `"databricks-cli"`.
/// - `token_url_override` defaults to OIDC discovery against the
///   workspace host.
#[non_exhaustive]
pub enum AuthConfig {
    /// Personal access token (`dapi…`).
    Pat { token: String },

    /// OAuth 2.0 machine-to-machine (client credentials grant).
    /// Used for service-principal authentication.
    OAuthM2M {
        client_id: String,
        client_secret: String,
        /// OAuth scopes to request. `None` → `["all-apis"]`.
        scopes: Option<Vec<String>>,
        /// Token endpoint override. `None` → discover via OIDC at
        /// `<host>/.well-known/oauth-authorization-server`.
        token_url_override: Option<String>,
    },

    /// OAuth 2.0 user-to-machine (authorization code + PKCE, browser
    /// flow). The kernel spins up a localhost listener on
    /// `redirect_port`, opens the user's browser to the workspace's
    /// authorize endpoint, and exchanges the returned code for a
    /// token.
    OAuthU2M {
        /// OAuth client ID. `None` → `"databricks-cli"` (the public
        /// CLI client registered on every workspace).
        client_id: Option<String>,
        /// Localhost port for the OAuth callback listener. `None` →
        /// `8020`.
        redirect_port: Option<u16>,
        /// OAuth scopes. `None` → `["all-apis", "offline_access"]`
        /// (the `offline_access` scope is required to receive a
        /// refresh token, which the kernel caches and refreshes
        /// transparently).
        scopes: Option<Vec<String>>,
        /// Token endpoint override. `None` → OIDC discovery.
        token_url_override: Option<String>,
        /// How long to wait for the user to complete the browser
        /// flow before timing out. `None` → 120 seconds.
        callback_timeout: Option<std::time::Duration>,
        /// On-disk token-cache policy. `None` → [`TokenCacheConfig::default`]
        /// (enabled, which then requires a passphrase — see that type).
        token_cache: Option<TokenCacheConfig>,
    },

    /// OAuth 2.0 token exchange (RFC 8693), for federated identity
    /// (e.g. exchanging a Workload Identity token for a Databricks
    /// access token).
    ///
    /// **Not yet implemented** in v0 — reserved with the final field
    /// shape so wrappers can compile against it. Calling
    /// [`Session::open`](crate::Session::open) with this variant
    /// returns `ErrorCode::InvalidArgument` until the implementation
    /// lands.
    OAuthFederation {
        subject_token: String,
        subject_token_type: String,
        audience: String,
    },

    /// Caller-supplied token provider for custom auth flows. The
    /// kernel calls [`TokenProvider::token`] whenever it needs an
    /// Authorization header.
    External(Box<dyn TokenProvider>),
}

impl AuthConfig {
    /// Construct a [`AuthConfig::Pat`] from an optional token, validating
    /// that it is present and non-empty.
    ///
    /// Bindings forward the raw optional from their host language; the
    /// required-field check lives here so every wrapper rejects a
    /// missing/empty token identically (`ErrorCode::InvalidArgument`)
    /// instead of each re-implementing it at the binding boundary.
    pub fn pat(token: Option<String>) -> crate::Result<Self> {
        match token {
            Some(token) if !token.is_empty() => Ok(AuthConfig::Pat { token }),
            _ => Err(crate::kernel_error::Error::invalid_argument()
                .message("PAT auth requires a non-empty access token")),
        }
    }

    /// Construct a [`AuthConfig::OAuthM2M`] from optional fields,
    /// validating that the required `client_id` / `client_secret` are
    /// present and non-empty. `scopes` / `token_url_override` are
    /// optional and forwarded as-is (the kernel fills defaults).
    pub fn oauth_m2m(
        client_id: Option<String>,
        client_secret: Option<String>,
        scopes: Option<Vec<String>>,
        token_url_override: Option<String>,
    ) -> crate::Result<Self> {
        let client_id = client_id.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("OAuth M2M auth requires a non-empty client_id")
        })?;
        let client_secret = client_secret.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("OAuth M2M auth requires a non-empty client_secret")
        })?;
        Ok(AuthConfig::OAuthM2M {
            client_id,
            client_secret,
            scopes,
            token_url_override,
        })
    }

    /// Construct a [`AuthConfig::OAuthU2M`] from optional fields. All
    /// U2M fields are optional — the kernel fills `databricks-cli` /
    /// port 8020 / default scopes when omitted — so this performs no
    /// validation and exists for symmetry with [`AuthConfig::pat`] /
    /// [`AuthConfig::oauth_m2m`] so bindings can forward optionals
    /// uniformly.
    pub fn oauth_u2m(
        client_id: Option<String>,
        redirect_port: Option<u16>,
        scopes: Option<Vec<String>>,
        token_url_override: Option<String>,
        callback_timeout: Option<std::time::Duration>,
        token_cache: Option<TokenCacheConfig>,
    ) -> Self {
        AuthConfig::OAuthU2M {
            client_id,
            redirect_port,
            scopes,
            token_url_override,
            callback_timeout,
            token_cache,
        }
    }
}

/// Caller-supplied token provider trait.
///
/// Used by [`AuthConfig::External`] to plug a host-side auth
/// mechanism (Azure AD MSAL, GCP ADC, AWS STS exchange, …) into the
/// kernel without the kernel needing to know the protocol details.
///
/// The kernel calls `token` whenever it needs to populate an
/// `Authorization: Bearer …` header. Implementations should cache
/// the token themselves and respect their own expiry; the kernel
/// does not cache across calls.
#[async_trait::async_trait]
pub trait TokenProvider: Send + Sync {
    /// Return a fresh access token. Called once per HTTP request
    /// that needs authentication; implementations are expected to
    /// cache and refresh internally.
    async fn token(&self) -> crate::kernel_error::Result<String>;
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pat { .. } => f.write_str("Pat { token: <redacted> }"),
            Self::OAuthM2M {
                client_id, scopes, ..
            } => f
                .debug_struct("OAuthM2M")
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .field("scopes", scopes)
                .finish(),
            Self::OAuthU2M {
                client_id,
                redirect_port,
                scopes,
                ..
            } => f
                .debug_struct("OAuthU2M")
                .field("client_id", client_id)
                .field("redirect_port", redirect_port)
                .field("scopes", scopes)
                .finish(),
            Self::OAuthFederation {
                subject_token_type,
                audience,
                ..
            } => f
                .debug_struct("OAuthFederation")
                .field("subject_token", &"<redacted>")
                .field("subject_token_type", subject_token_type)
                .field("audience", audience)
                .finish(),
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
    /// Retrying stops once cumulative elapsed time exceeds this.
    pub overall_timeout: Duration,

    /// Connection pool size (max idle warm sockets per remote host).
    ///
    /// Defaults to `100`, matching the JDBC driver's `HttpConnectionPoolSize`
    /// default. Raise this for workloads that burst many concurrent
    /// `executeStatement` calls or CloudFetch chunk downloads against a
    /// single host; too low a value forces fresh TLS handshakes when the
    /// pool fills.
    pub pool_max_idle_per_host: usize,

    /// Minimum backoff wait between retry attempts. Also the floor for
    /// a server-supplied `Retry-After` and the base of the exponential
    /// backoff (`min_wait * 2^attempt`) used when no `Retry-After` is
    /// present. Default 1s.
    pub retry_min_wait: Duration,

    /// Maximum backoff wait between retry attempts. Caps both the
    /// exponential backoff and a server-supplied `Retry-After`.
    /// Default 60s.
    pub retry_max_wait: Duration,

    /// Maximum number of retries **after** the initial attempt — a
    /// request is attempted at most `retry_max_retries + 1` times.
    /// Default 5 (→ 6 total attempts).
    ///
    /// Wrappers exposing a "total attempts" knob (the connector's
    /// `_retry_stop_after_attempts_count`, the JDBC driver, …) should
    /// map it as `retry_max_retries = max(0, total_attempts - 1)`.
    pub retry_max_retries: u32,

    /// Extra HTTP headers to send on every request. Lets wrappers
    /// forward caller-supplied headers (the connector's `http_headers`,
    /// which also carries the composed `User-Agent`) onto the kernel's
    /// HTTP stack.
    ///
    /// **`Authorization` / `x-databricks-org-id` are reserved:** a
    /// caller entry for either name is **silently dropped (with a
    /// `warn!`)** — it never reaches the wire. The kernel always adds
    /// its own `Authorization` (from the auth provider) and, when set,
    /// `x-databricks-org-id`, so auth and multi-tenant routing can't be
    /// hijacked by a caller header. (This mirrors the Thrift connector,
    /// where `auth_provider.add_headers` runs after the custom-header
    /// dict and overwrites those names.)
    ///
    /// **`User-Agent` is special-cased:** a caller `User-Agent` here
    /// **replaces** the kernel's base UA entirely — it is folded into the
    /// effective UA at client construction (becoming the UA verbatim) and
    /// removed from this list. This gives the SEA server an accurate
    /// driver identity for query-source attribution (e.g. the connector's
    /// `PyDatabricksSqlConnector/x (entry)`), instead of the
    /// `DatabricksJDBCDriverOSS/...` base.
    ///
    /// PRECONDITION: the caller UA's leading prefix MUST be in the
    /// gateway's `databricks.sqlexecapi.known_driver_prefixes` SAFE flag.
    /// The server gates the `INLINE_OR_EXTERNAL_LINKS` result disposition
    /// (single-response chunk links → parallel downloads) on a
    /// `startsWith` prefix match of that flag; a non-allow-listed prefix
    /// is rejected (HTTP 400 / Thrift redirect). `PyDatabricksSqlConnector`
    /// is allow-listed for this reason. When NO caller `User-Agent` is
    /// supplied, the base UA stays `DatabricksJDBCDriverOSS/...` so the
    /// gate still passes.
    ///
    /// **All other headers are sent as supplied.** A `Vec` (not a map)
    /// so order is preserved. No de-duplication is performed: duplicate
    /// names are applied via `reqwest`'s `HeaderMap::append`, so each
    /// entry is emitted on the wire in insertion order (the receiver
    /// sees every value, not just the last) — pass a name once unless
    /// you specifically intend multiple header lines.
    pub custom_headers: Vec<(String, String)>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(120),
            overall_timeout: Duration::from_secs(900),
            pool_max_idle_per_host: 100,
            // Mirror `RetryConfig::default()` so a stock `HttpConfig`
            // produces the same retry behaviour the kernel shipped
            // before these knobs were configurable.
            retry_min_wait: Duration::from_secs(1),
            retry_max_wait: Duration::from_secs(60),
            retry_max_retries: 5,
            custom_headers: Vec::new(),
        }
    }
}

// ─── HTTP proxy ───────────────────────────────────────────────────────────

/// Programmatic HTTP/HTTPS proxy configuration.
///
/// When [`url`](Self::url) is `None` (the default), the kernel does **not**
/// configure a proxy explicitly and `reqwest`'s standard behaviour applies —
/// the `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` environment variables are
/// honoured automatically. Setting [`url`](Self::url) **overrides** those env
/// vars with the supplied proxy, routing all (`http://` and `https://`)
/// traffic through it.
///
/// Basic-auth credentials may be embedded directly in the URL
/// (`http://user:pass@host:port`), or supplied out-of-band via
/// [`username`](Self::username) / [`password`](Self::password); the explicit
/// fields take precedence over URL-embedded credentials when both are set.
///
/// `ProxyConfig` is `#[non_exhaustive]` — construct via
/// [`ProxyConfig::default`] and assign the public fields you need:
///
/// ```no_run
/// # use databricks_sql_kernel::ProxyConfig;
/// let mut proxy = ProxyConfig::default();
/// proxy.url = Some("http://proxy.corp.example.com:8080".to_string());
/// ```
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct ProxyConfig {
    /// Proxy URL, e.g. `http://proxy.corp.example.com:8080` or
    /// `http://user:pass@proxy:8080`. Must use the `http://` or
    /// `https://` scheme. `None` ⇒ env-var behaviour (no override).
    pub url: Option<String>,

    /// Username for proxy basic-auth. Overrides any credentials
    /// embedded in [`url`](Self::url) when set.
    pub username: Option<String>,

    /// Password for proxy basic-auth. Secret material — redacted in
    /// `Debug`. Ignored unless [`username`](Self::username) is also set.
    pub password: Option<String>,

    /// Comma-separated list of hosts/domains to bypass the proxy
    /// (e.g. `"localhost,*.internal.corp,.example.com"`). Only applied
    /// when [`url`](Self::url) is set.
    pub bypass_hosts: Option<String>,
}

impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the explicit `password` field. The `url` may itself
        // carry embedded `user:pass@` credentials, so render only its
        // presence rather than the raw string.
        f.debug_struct("ProxyConfig")
            .field("url", &self.url.as_ref().map(|_| "<set>"))
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("bypass_hosts", &self.bypass_hosts)
            .finish()
    }
}

// ─── TLS ────────────────────────────────────────────────────────────────────

/// TLS settings for the HTTP client.
///
/// All defaults match the most secure stance: validate the system trust
/// store, require a valid certificate chain, and verify hostnames. Any
/// non-default field is a *relaxation* — flip these for dev / on-prem
/// boxes with self-signed certs, never against production.
///
/// The kernel exposes only fields that the internal rustls plumbing
/// already supports — cert pinning and FIPS mode are *not* in scope.
///
/// `TlsConfig` is `#[non_exhaustive]`, so construct it from
/// [`TlsConfig::default`] and assign the public fields you need:
///
/// ```no_run
/// # use databricks_sql_kernel::TlsConfig;
/// // From a file path:
/// let mut from_file = TlsConfig::default();
/// from_file.custom_ca_cert = Some(std::fs::read("/etc/ssl/corp-ca.pem").unwrap());
///
/// // From an in-memory buffer (the napi-rs `Buffer` / pyo3 `bytes`
/// // path — no tempfile shim needed):
/// let pem_bytes: Vec<u8> = b"-----BEGIN CERTIFICATE-----\n...".to_vec();
/// let mut from_bytes = TlsConfig::default();
/// from_bytes.custom_ca_cert = Some(pem_bytes);
/// # let _ = (from_file, from_bytes);
/// ```
//
// TODO(pr23-followup): the public Thrift NodeJS driver does NOT
// expose programmatic TLS config — its `ConnectionOptions` has no
// `ca` / `cert` / `key` field, and its `HttpConnection.createHttpsAgent`
// builds the agent with `rejectUnauthorized: false` HARDCODED
// (verified at `~/databricks-sql-nodejs/lib/connection/connections/HttpConnection.ts:67-77`,
// 2026-05-27). The only "TLS knobs" Thrift NodeJS users have today
// are the Node process-level env vars `NODE_EXTRA_CA_CERTS` and
// `NODE_TLS_REJECT_UNAUTHORIZED`, and even those are inert against
// the Thrift driver's hardcoded-permissive agent.
//
// The kernel surface here is intentionally STRICTER than Thrift's
// (default = full validation, opt-in relaxations) and richer (custom
// CA bundles, programmatic mTLS room). Motivated by:
//   1. Python connector parity — `databricks-sql-connector` exposes
//      `_tls_no_verify` / `_tls_verify_hostname` /
//      `_tls_trusted_ca_file` / `_tls_client_cert_*`. The kernel must
//      back those when pyo3 wires them up.
//   2. Doing better than Thrift's hardcoded-insecure default on the
//      SEA NodeJS path (napi binding).
//
// Revisit when (a) the napi binding wires these onto `ConnectionOptions`
// for JS callers, and (b) we decide whether to honour Node's
// `NODE_EXTRA_CA_CERTS` env var on the SEA path the way Node's own
// https stack does — likely yes, for ergonomic parity with the rest
// of the Node ecosystem.
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct TlsConfig {
    /// PEM-encoded CA certificate bytes to add to the trust store on
    /// top of the system roots. Use this when connecting through a
    /// corporate proxy that re-signs TLS or to an on-prem Databricks
    /// deployment using an internal CA.
    ///
    /// Carrying bytes (rather than a path) keeps the surface friendly
    /// for foreign-language wrappers — napi-rs / pyo3 callers pass a
    /// `Buffer` / `bytes` directly without needing to materialise a
    /// tempfile.
    ///
    /// **Multi-cert bundles (root + intermediate):** the kernel splits
    /// the PEM into individual certificates and adds each as a root,
    /// so a concatenated `cat root.pem intermediate.pem` blob works
    /// transparently regardless of the active TLS backend.
    pub custom_ca_cert: Option<Vec<u8>>,

    /// Accept self-signed server certificates.
    ///
    /// Note: the underlying reqwest backend does not distinguish
    /// "self-signed only" from "fully unvalidated" — setting this to
    /// `true` disables ALL chain validation (CA trust, expiration,
    /// etc.) via `danger_accept_invalid_certs(true)`. Use only in
    /// development.
    pub accept_self_signed: bool,

    /// Skip the certificate-hostname-vs-SNI match check. Use only
    /// when connecting via an IP literal or a hostname the cert
    /// wasn't issued for in a development / lab environment.
    pub skip_hostname_verification: bool,

    /// PEM-encoded client certificate for mutual TLS (mTLS).
    ///
    /// Set together with [`Self::client_key_pem`] when the server
    /// requires the client to present a certificate (mTLS handshake).
    /// Mirrors the Python connector's `_tls_client_cert_file` /
    /// `_tls_client_cert_key_file` knobs — wrappers read those files
    /// and pass the bytes here, no tempfile shim needed.
    ///
    /// **Backend note:** under the `tls-native` backend the cert chain
    /// and key are loaded as a separate `(cert, key)` pair via
    /// `reqwest::Identity::from_pkcs8_pem`; under `tls-rustls` they are
    /// concatenated into a single PEM and loaded via
    /// `reqwest::Identity::from_pem`. Either backend accepts a PEM
    /// `client_cert_pem` carrying a leaf certificate optionally followed
    /// by its intermediate chain.
    ///
    /// Both `client_cert_pem` and `client_key_pem` must be set
    /// together; setting only one is rejected at
    /// [`Session::open`](crate::Session::open) with
    /// `ErrorCode::InvalidArgument`.
    ///
    /// **Validation timing differs by backend.** Under `tls-native`,
    /// `from_pkcs8_pem` validates the cert/key pair at `Session::open`,
    /// so a malformed or mismatched pair surfaces synchronously as
    /// `ErrorCode::InvalidArgument`. Under `tls-rustls`, the cert and
    /// key are stored and only exercised during the TLS handshake, so
    /// the same problem surfaces later as a connection error on the
    /// first request rather than at `Session::open`.
    pub client_cert_pem: Option<Vec<u8>>,

    /// PEM-encoded private key for the mTLS client certificate.
    ///
    /// Accepted key encodings depend on the active TLS backend:
    /// - **`tls-rustls`** accepts PKCS#8 (`BEGIN PRIVATE KEY`), PKCS#1
    ///   (`BEGIN RSA PRIVATE KEY`), and SEC1 (`BEGIN EC PRIVATE KEY`).
    /// - **`tls-native`** requires a **PKCS#8** key (`BEGIN PRIVATE
    ///   KEY`) — `reqwest::Identity::from_pkcs8_pem` is PKCS#8-only.
    ///
    /// For portability across both backends, supply a PKCS#8 key. See
    /// [`Self::client_cert_pem`] for pairing rules and the
    /// backend-specific validation timing.
    pub client_key_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field(
                "custom_ca_cert",
                &self
                    .custom_ca_cert
                    .as_ref()
                    .map(|b| format!("<{} bytes>", b.len())),
            )
            .field("accept_self_signed", &self.accept_self_signed)
            .field(
                "skip_hostname_verification",
                &self.skip_hostname_verification,
            )
            .field(
                "client_cert_pem",
                &self
                    .client_cert_pem
                    .as_ref()
                    .map(|b| format!("<{} bytes>", b.len())),
            )
            // The private key is secret material — never render its
            // length or contents, just whether one was supplied.
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

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
    /// Whether to opt into CloudFetch (cloud-storage-backed download of
    /// large result sets via presigned URLs). Default: `true`.
    ///
    /// Setting this to `false` mirrors JDBC's `EnableQueryResultDownload=0`
    /// behaviour. It is a **connection-level** setting: read once at
    /// [`Session::open`](crate::Session::open) and applied to every
    /// statement by selecting the SEA `disposition` on each
    /// `ExecuteStatement` (`false` → `INLINE`, so SEA serves **all**
    /// result sizes via the inline-Arrow path regardless of size). This
    /// matches how the JDBC and Thrift reference drivers control
    /// cloud-fetch (per request), and is deliberately **not** expressed
    /// as a session conf — SEA has no `CAN_CLOUD_DOWNLOAD` conf and
    /// rejects one at ExecuteStatement with `CONFIG_NOT_AVAILABLE`
    /// (issue #196). Users who need inline-only behaviour (e.g.
    /// workspaces without cloud-storage egress) should set this to
    /// `false`.
    ///
    /// Disabling CloudFetch routes results to the inline-Arrow
    /// path. The kernel today supports the **single-attachment**
    /// inline path; the multi-chunk inline path is on the SEA
    /// inline-Arrow rollout (DD1) and is tracked as a follow-up
    /// (`InlineArrowProvider::new` consumes one attachment, and
    /// `src/reader/mod.rs` returns `Error::not_implemented` for
    /// inline responses that carry a `next_chunk_internal_link`).
    /// Once multi-chunk lands, this path will not match
    /// CloudFetch's concurrent chunk-download pipeline
    /// (`cloudfetch_max_chunks_in_memory`).
    pub cloudfetch_enabled: bool,

    /// Number of presigned URLs to prefetch ahead of the consumer.
    pub cloudfetch_link_prefetch_window: usize,

    /// Maximum number of decompressed chunks held in memory at once.
    /// Bounds parallelism and peak RSS.
    pub cloudfetch_max_chunks_in_memory: usize,

    /// Maximum number of decompressed Arrow chunks the inline-Arrow
    /// streaming reader holds in memory at once. Analogue of
    /// [`Self::cloudfetch_max_chunks_in_memory`] for the inline path
    /// (used when [`Self::cloudfetch_enabled`] is `false`).
    ///
    /// The inline path is sequential per the SEA contract — one
    /// `GetResultData` call at a time — so this knob bounds the
    /// **prefetch window** (chunks the kernel pulls ahead of the
    /// consumer) rather than the download parallelism. Larger values
    /// give the consumer a deeper buffer at the cost of memory.
    ///
    /// Default `4`. Range 1..=N; `1` disables prefetching and gives
    /// strict lazy fetch-on-demand semantics.
    pub inline_max_chunks_in_memory: usize,

    /// When `true`, every column of complex type (`List` /
    /// `LargeList` / `FixedSizeList` / `Map` / `Struct`, plus the
    /// precise types `VARIANT` / `GEOMETRY` / `GEOGRAPHY` carried in
    /// field metadata) is replaced with a `Utf8` column holding
    /// compact JSON text per row.
    ///
    /// The JSON format matches what the Databricks server emits on
    /// the Thrift wire when `complexTypesAsArrow=false` (compact —
    /// no whitespace between separators, double-quoted strings,
    /// JSON `null` for SQL NULL).
    ///
    /// Default `false`. Bindings that surface a flat-text view
    /// (PySQL when `_use_arrow_native_complex_types=False`, JDBC
    /// when `complexDatatypeSupport=false`) should set this `true`.
    /// ADBC and any other Arrow-native consumer should leave it
    /// `false`.
    pub complex_types_as_json: bool,

    /// When `true`, `Interval(*)` and `Duration(*)` columns are
    /// replaced with `Utf8` columns containing the canonical
    /// Databricks text form (`"Y-M"` for YEAR-MONTH,
    /// `"D HH:MM:SS.nnnnnnnnn"` for DAY-TIME / durations).
    ///
    /// Motivation: pyarrow's Python bindings can't decode Arrow's
    /// `month_interval` (id 21) — `.as_py()` / `.to_pylist()` /
    /// `cast(string)` / `to_pandas()` all fail with KeyError or
    /// `ArrowNotImplementedError`, killing the result set. Bindings
    /// that hand Arrow batches to pyarrow Python (the PySQL kernel
    /// backend) should set this `true`. Rust consumers and bindings
    /// with their own interval decoder can leave it `false`.
    ///
    /// Default `false`.
    pub intervals_as_string: bool,
}

impl Default for ResultConfig {
    fn default() -> Self {
        Self {
            cloudfetch_enabled: true,
            cloudfetch_link_prefetch_window: 10,
            cloudfetch_max_chunks_in_memory: 16,
            inline_max_chunks_in_memory: 4,
            complex_types_as_json: false,
            intervals_as_string: false,
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
    /// Thrift `configuration`. Typed sub-structs above translate
    /// the *client-side* knobs; this map carries everything else.
    ///
    /// The kernel passes user-supplied entries through unchanged,
    /// after filtering them against the kernel allowlist (see
    /// [`filter_session_conf`] / [`SESSION_CONF_ALLOWLIST`]). The
    /// kernel does not synthesise any entries here from typed
    /// client-side knobs.
    ///
    /// In particular, [`ResultConfig::cloudfetch_enabled`] is **not**
    /// expressed as a session conf: SEA has no `CAN_CLOUD_DOWNLOAD`
    /// conf (it is accepted at CreateSession but rejected at the first
    /// ExecuteStatement with `CONFIG_NOT_AVAILABLE`, issue #196).
    /// CloudFetch-disable is instead applied per-statement via the SEA
    /// `disposition` field, matching the JDBC / Thrift reference
    /// drivers. A stray caller-supplied `can_cloud_download` is dropped
    /// by [`filter_session_conf`] (it is not allowlisted).
    ///
    /// **Boolean-valued conf keys** should use the exact strings
    /// `"true"` / `"false"`. SEA accepts these literally; other forms
    /// (`"True"`, `"yes"`, `"1"`) either fail at session-create or
    /// behave as undefined on the server. The kernel does not
    /// pre-validate — its posture is "trust the user's session_conf" —
    /// so non-canonical values flow to the server as-is.
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

// ─── session_conf allowlist ─────────────────────────────────────────────────

/// Known session-conf keys, sourced from
/// <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters>
/// plus the additional keys the Java driver accepts (`ENABLE_PHOTON`,
/// `QUERY_TAGS`). Keys are matched case-insensitively against this list
/// and uppercased before being sent.
pub(crate) const SESSION_CONF_ALLOWLIST: &[&str] = &[
    "ANSI_MODE",
    // NOTE: `CAN_CLOUD_DOWNLOAD` is deliberately NOT allowlisted. SEA has
    // no such session conf — it is accepted at CreateSession but rejected
    // at the first ExecuteStatement with `CONFIG_NOT_AVAILABLE` (issue
    // #196). CloudFetch-disable is expressed per-statement via the SEA
    // `disposition` (see `SeaRestTransport::call_execute_api` and
    // `TransportConfig::cloudfetch_enabled`), matching both the JDBC and
    // Thrift reference drivers. A stray caller-supplied `can_cloud_download`
    // is therefore dropped by `filter_session_conf` (with a warn) rather
    // than forwarded to the wire where it would break the session.
    "COLLATION",
    "ENABLE_PHOTON",
    "LEGACY_TIME_PARSER_POLICY",
    "MAX_FILE_PARTITION_BYTES",
    "QUERY_TAGS",
    "READ_ONLY_EXTERNAL_METASTORE",
    "STATEMENT_TIMEOUT",
    "TIMEZONE",
    "USE_CACHED_RESULT",
];

/// Case-sensitive session-conf keys that must be sent **verbatim**
/// rather than uppercased. These are dotted `spark.*` Spark confs (as
/// opposed to the short SET-style SQL parameters in
/// [`SESSION_CONF_ALLOWLIST`], which Databricks echoes uppercase).
/// Spark stores `SQLConf` entries under their exact registered
/// lowercase-dotted key, so uppercasing would address the wrong entry.
///
/// Each tuple is `(uppercased_lookup_key, canonical_verbatim_key)`:
/// callers are matched case-insensitively (via the uppercased lookup)
/// but the canonical form is what we send on the wire.
///
/// `spark.sql.thriftserver.metadata.metricview.enabled` enables UC
/// Metric View metadata — when set to `"true"`, `SHOW TABLES`
/// (`Metadata::tables`) returns `METRIC_VIEW` rows and `getColumns`
/// surfaces `measure` columns. Opt-in: the connector sets it from its
/// `enable_metric_view_metadata` kwarg (default off), matching the
/// JDBC driver's `EnableMetricViewMetadata` (default `0`). The kernel
/// already lists `METRIC_VIEW` among its table types; this lets the
/// enabling conf through so the server actually emits the rows.
pub(crate) const SESSION_CONF_CASE_SENSITIVE_ALLOWLIST: &[(&str, &str)] = &[(
    "SPARK.SQL.THRIFTSERVER.METADATA.METRICVIEW.ENABLED",
    "spark.sql.thriftserver.metadata.metricview.enabled",
)];

/// Filter a caller-supplied `session_conf` map against the allowlists.
/// Unknown keys are dropped with a `tracing::warn!`.
///
/// - Keys in [`SESSION_CONF_ALLOWLIST`] are normalized to **uppercase**
///   (the server echoes uppercase for these SET-style params, so
///   SET-readback matches).
/// - Keys in [`SESSION_CONF_CASE_SENSITIVE_ALLOWLIST`] (dotted
///   `spark.*` confs) are matched case-insensitively but emitted in
///   their **canonical verbatim** form — Spark conf keys are
///   case-sensitive and must not be uppercased.
pub(crate) fn filter_session_conf(
    raw: std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;
    let mut out: HashMap<String, String> = HashMap::with_capacity(raw.len());
    for (k, v) in raw {
        let upper = k.to_ascii_uppercase();
        if SESSION_CONF_ALLOWLIST.contains(&upper.as_str()) {
            out.insert(upper, v);
        } else if let Some((_, canonical)) = SESSION_CONF_CASE_SENSITIVE_ALLOWLIST
            .iter()
            .find(|(lookup, _)| *lookup == upper.as_str())
        {
            // Send the canonical lowercase-dotted form verbatim, not
            // the caller's casing and not uppercased.
            out.insert((*canonical).to_string(), v);
        } else {
            crate::klog::warn!(
                key = %k,
                "dropping unknown session_conf key (not in kernel allowlist); \
                 see https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters"
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── token cache policy ────────────────────────────────────────

    #[test]
    fn token_cache_defaults_to_enabled() {
        // Both the reference ODBC driver and JDBC default EnableTokenCache
        // to 1, with no passphrase.
        assert!(TokenCacheConfig::default().enabled);
        assert!(TokenCacheConfig::default().passphrase.is_none());
    }

    #[test]
    fn token_cache_enabled_with_passphrase_resolves_to_it() {
        let cfg = TokenCacheConfig {
            enabled: true,
            passphrase: Some("s3cr3t".to_string()),
        };
        assert_eq!(cfg.resolve().as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn token_cache_enabled_without_passphrase_falls_back_to_derived_key() {
        // The reference ODBC driver ships EnableTokenCache=1 with an empty
        // TokenCachePassPhrase and still caches, so this combination must
        // NOT fail the connection — a DSN that never set a passphrase has to
        // keep working. It must also not disable caching silently, which
        // would reintroduce a browser prompt on every connection.
        let cfg = TokenCacheConfig::default();
        let passphrase = cfg
            .resolve()
            .expect("enabled + no passphrase must still cache");
        assert!(
            !passphrase.trim().is_empty(),
            "fallback passphrase must be non-empty, else the cache would be \
             encrypted under nothing"
        );
    }

    #[test]
    fn token_cache_blank_passphrase_uses_the_derived_key_not_the_blank() {
        // An all-whitespace passphrase is almost always an unexpanded config
        // placeholder. Honouring it literally would key every machine's cache
        // identically, which is strictly weaker than the machine-local
        // fallback, so blank must be treated as absent.
        let derived = TokenCacheConfig::default()
            .resolve()
            .expect("default resolves")
            .into_owned();
        for blank in ["", "   ", "\t\n"] {
            let cfg = TokenCacheConfig {
                enabled: true,
                passphrase: Some(blank.to_string()),
            };
            let resolved = cfg.resolve().expect("blank still caches");
            assert_ne!(
                resolved.as_ref(),
                blank,
                "blank passphrase {blank:?} must not be used verbatim"
            );
            assert_eq!(
                resolved.as_ref(),
                derived,
                "blank passphrase {blank:?} should fall back to the derived key"
            );
        }
    }

    #[test]
    fn token_cache_derived_key_is_stable_across_calls() {
        // If the fallback were not deterministic, every process would write a
        // cache the next one cannot read — a silent browser prompt on every
        // connection, which is the failure this whole feature avoids.
        assert_eq!(
            TokenCacheConfig::default()
                .resolve()
                .map(|p| p.into_owned()),
            TokenCacheConfig::default()
                .resolve()
                .map(|p| p.into_owned())
        );
    }

    #[test]
    fn token_cache_disabled_resolves_to_none_without_passphrase() {
        // Disabling must always be valid — it is the escape hatch for
        // processes that must not write credentials to disk at all.
        let cfg = TokenCacheConfig {
            enabled: false,
            passphrase: None,
        };
        assert_eq!(cfg.resolve(), None);
    }

    #[test]
    fn token_cache_disabled_ignores_a_supplied_passphrase() {
        // `enabled: false` wins: a leftover passphrase must not silently
        // re-enable disk persistence.
        let cfg = TokenCacheConfig {
            enabled: false,
            passphrase: Some("ignored".to_string()),
        };
        assert_eq!(cfg.resolve(), None);
    }

    // ─── session_conf allowlist ────────────────────────────────────

    #[test]
    fn session_conf_filter_keeps_known_keys_uppercased() {
        let mut raw = std::collections::HashMap::new();
        raw.insert("use_cached_result".to_string(), "false".to_string());
        raw.insert("ANSI_MODE".to_string(), "true".to_string());
        let out = filter_session_conf(raw);
        assert_eq!(
            out.get("USE_CACHED_RESULT").map(String::as_str),
            Some("false")
        );
        assert_eq!(out.get("ANSI_MODE").map(String::as_str), Some("true"));
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn session_conf_filter_drops_unknown_keys() {
        let mut raw = std::collections::HashMap::new();
        raw.insert("ANSI_MOD".to_string(), "true".to_string()); // typo
        raw.insert("NOT_A_REAL_PARAM".to_string(), "x".to_string());
        raw.insert("TIMEZONE".to_string(), "UTC".to_string());
        let out = filter_session_conf(raw);
        assert_eq!(out.len(), 1);
        assert_eq!(out.get("TIMEZONE").map(String::as_str), Some("UTC"));
    }

    #[test]
    fn session_conf_filter_drops_can_cloud_download() {
        // Regression tripwire for issue #196: `can_cloud_download` is NOT a
        // valid SEA session conf (the server rejects it at ExecuteStatement
        // with CONFIG_NOT_AVAILABLE). It must be dropped by the allowlist
        // filter — never forwarded to the wire — regardless of casing.
        // CloudFetch-disable is applied per-statement via the SEA
        // disposition instead. If someone re-adds it to the allowlist, this
        // test flips RED.
        for spelling in [
            "can_cloud_download",
            "CAN_CLOUD_DOWNLOAD",
            "Can_Cloud_Download",
        ] {
            let mut raw = std::collections::HashMap::new();
            raw.insert(spelling.to_string(), "false".to_string());
            let out = filter_session_conf(raw);
            assert!(
                out.is_empty(),
                "can_cloud_download ({spelling:?}) must be dropped, got {out:?}"
            );
        }
    }

    #[test]
    fn session_conf_filter_passes_metricview_conf_verbatim() {
        // The metric-view conf is a dotted spark.* key: it must survive
        // the allowlist AND be emitted lowercase-verbatim (NOT
        // uppercased like the SET-style params), because Spark conf
        // keys are case-sensitive.
        let mut raw = std::collections::HashMap::new();
        raw.insert(
            "spark.sql.thriftserver.metadata.metricview.enabled".to_string(),
            "true".to_string(),
        );
        let out = filter_session_conf(raw);
        assert_eq!(
            out.get("spark.sql.thriftserver.metadata.metricview.enabled")
                .map(String::as_str),
            Some("true"),
        );
        // It must NOT have been uppercased.
        assert!(!out.contains_key("SPARK.SQL.THRIFTSERVER.METADATA.METRICVIEW.ENABLED"));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn session_conf_filter_metricview_match_is_case_insensitive() {
        // A caller passing the key in any casing is recognised, but the
        // canonical lowercase form is always what we send.
        let mut raw = std::collections::HashMap::new();
        raw.insert(
            "SPARK.SQL.THRIFTSERVER.METADATA.METRICVIEW.ENABLED".to_string(),
            "true".to_string(),
        );
        let out = filter_session_conf(raw);
        assert_eq!(
            out.get("spark.sql.thriftserver.metadata.metricview.enabled")
                .map(String::as_str),
            Some("true"),
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn session_conf_allowlist_covers_jdbc_set() {
        // Parity check: every key the Java driver accepts must be
        // accepted here too (we add COLLATION which JDBC currently
        // lacks but the docs list).
        for jdbc_key in [
            "ANSI_MODE",
            "ENABLE_PHOTON",
            "LEGACY_TIME_PARSER_POLICY",
            "MAX_FILE_PARTITION_BYTES",
            "READ_ONLY_EXTERNAL_METASTORE",
            "STATEMENT_TIMEOUT",
            "TIMEZONE",
            "USE_CACHED_RESULT",
            "QUERY_TAGS",
        ] {
            assert!(
                SESSION_CONF_ALLOWLIST.contains(&jdbc_key),
                "missing JDBC-compatible key: {jdbc_key}"
            );
        }
    }

    // ─── AuthConfig validating constructors ───────────────────────

    #[test]
    fn pat_rejects_missing_or_empty_token() {
        let err = AuthConfig::pat(None).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("non-empty access token"));

        let err = AuthConfig::pat(Some(String::new())).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn pat_accepts_non_empty_token() {
        match AuthConfig::pat(Some("dapi-x".to_string())).unwrap() {
            AuthConfig::Pat { token } => assert_eq!(token, "dapi-x"),
            other => panic!("expected Pat, got {other:?}"),
        }
    }

    #[test]
    fn oauth_m2m_rejects_missing_or_empty_client_id() {
        let err = AuthConfig::oauth_m2m(None, Some("sec".to_string()), None, None).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_id"));

        let err = AuthConfig::oauth_m2m(Some(String::new()), Some("sec".to_string()), None, None)
            .unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_id"));
    }

    #[test]
    fn oauth_m2m_rejects_missing_or_empty_client_secret() {
        let err = AuthConfig::oauth_m2m(Some("cid".to_string()), None, None, None).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_secret"));

        let err = AuthConfig::oauth_m2m(Some("cid".to_string()), Some(String::new()), None, None)
            .unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_secret"));
    }

    #[test]
    fn oauth_m2m_accepts_full_credentials() {
        match AuthConfig::oauth_m2m(
            Some("cid".to_string()),
            Some("sec".to_string()),
            Some(vec!["all-apis".to_string()]),
            None,
        )
        .unwrap()
        {
            AuthConfig::OAuthM2M {
                client_id,
                client_secret,
                scopes,
                token_url_override,
            } => {
                assert_eq!(client_id, "cid");
                assert_eq!(client_secret, "sec");
                assert_eq!(scopes, Some(vec!["all-apis".to_string()]));
                assert!(token_url_override.is_none());
            }
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    // ─── ConnectionConfig parsing ──────────────────────────────────

    #[test]
    fn from_http_path_warehouses_form() {
        let c = ConnectionConfig::from_http_path("https://h", "/sql/1.0/warehouses/abc123")
            .expect("parse");
        assert_eq!(c.host, "https://h");
        assert_eq!(c.warehouse_id, "abc123");
        assert_eq!(c.org_id, None);
    }

    #[test]
    fn from_http_path_endpoints_form_is_equivalent() {
        let c = ConnectionConfig::from_http_path("h", "/sql/1.0/endpoints/abc123").expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
    }

    #[test]
    fn from_http_path_accepts_no_leading_slash() {
        let c = ConnectionConfig::from_http_path("h", "sql/1.0/warehouses/abc123").expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
    }

    #[test]
    fn from_http_path_strips_trailing_slash() {
        let c =
            ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123/").expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
    }

    #[test]
    fn from_http_path_captures_org_id() {
        let c = ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123?o=42")
            .expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
        assert_eq!(c.org_id, Some("42".to_string()));
    }

    #[test]
    fn from_http_path_ignores_empty_org_id() {
        let c =
            ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123?o=").expect("parse");
        assert_eq!(c.org_id, None);
    }

    #[test]
    fn from_http_path_handles_multiple_query_params() {
        let c = ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123?x=1&o=42&y=2")
            .expect("parse");
        assert_eq!(c.org_id, Some("42".to_string()));
    }

    #[test]
    fn from_http_path_rejects_unrecognised_path() {
        let err = ConnectionConfig::from_http_path("h", "/other/path").unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("/other/path"));
    }

    #[test]
    fn from_http_path_rejects_missing_warehouse_id() {
        // The prefix matches but the id segment is empty.
        let err = ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/").unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn new_does_not_set_org_id() {
        let c = ConnectionConfig::new("https://h", "abc123");
        assert_eq!(c.org_id, None);
    }

    #[test]
    fn new_prepends_https_when_scheme_missing() {
        let c = ConnectionConfig::new("workspace.databricks.com", "abc");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    #[test]
    fn new_preserves_explicit_https() {
        let c = ConnectionConfig::new("https://workspace.databricks.com", "abc");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    #[test]
    fn new_preserves_explicit_http_for_test_servers() {
        // wiremock and other test servers use http://; don't force
        // https:// when the caller has explicitly opted into http.
        let c = ConnectionConfig::new("http://127.0.0.1:8080", "abc");
        assert_eq!(c.host, "http://127.0.0.1:8080");
    }

    #[test]
    fn new_strips_trailing_slash() {
        let c = ConnectionConfig::new("https://workspace.databricks.com/", "abc");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    #[test]
    fn from_http_path_normalises_host_too() {
        let c = ConnectionConfig::from_http_path(
            "workspace.databricks.com/",
            "/sql/1.0/warehouses/abc",
        )
        .expect("parse");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    // ─── AuthConfig Debug redaction ────────────────────────────────

    #[test]
    fn debug_pat_redacts_token() {
        let c = AuthConfig::Pat {
            token: "dapi_secret".to_string(),
        };
        let s = format!("{c:?}");
        assert!(!s.contains("dapi_secret"), "token leaked: {s}");
        assert!(s.contains("<redacted>"));
    }

    #[test]
    fn debug_oauth_m2m_redacts_secret() {
        let c = AuthConfig::OAuthM2M {
            client_id: "client123".to_string(),
            client_secret: "supersecret".to_string(),
            scopes: None,
            token_url_override: None,
        };
        let s = format!("{c:?}");
        assert!(!s.contains("supersecret"), "secret leaked: {s}");
        assert!(s.contains("client123"));
        assert!(s.contains("<redacted>"));
    }

    #[test]
    fn debug_oauth_federation_redacts_subject_token() {
        let c = AuthConfig::OAuthFederation {
            subject_token: "supersecret".to_string(),
            subject_token_type: "urn:ietf:params:oauth:token-type:jwt".to_string(),
            audience: "databricks".to_string(),
        };
        let s = format!("{c:?}");
        assert!(!s.contains("supersecret"), "subject_token leaked: {s}");
        assert!(s.contains("<redacted>"));
        assert!(s.contains("databricks"));
    }

    // ─── TlsConfig ─────────────────────────────────────────────────

    #[test]
    fn tls_config_default_is_strict() {
        let tls = TlsConfig::default();
        assert!(tls.custom_ca_cert.is_none());
        assert!(!tls.accept_self_signed);
        assert!(!tls.skip_hostname_verification);
    }

    #[test]
    fn tls_config_debug_redacts_ca_cert_bytes() {
        // The PEM bytes themselves aren't secret, but a multi-kB blob
        // in every log line is unhelpful — Debug should summarise.
        let tls = TlsConfig {
            custom_ca_cert: Some(b"-----BEGIN CERTIFICATE-----...".to_vec()),
            accept_self_signed: false,
            skip_hostname_verification: false,
            ..TlsConfig::default()
        };
        let s = format!("{tls:?}");
        assert!(s.contains("<"), "expected byte-count summary, got: {s}");
        assert!(
            s.contains("bytes>"),
            "expected byte-count summary, got: {s}"
        );
        assert!(!s.contains("BEGIN CERTIFICATE"), "raw bytes leaked: {s}");
    }

    #[test]
    fn tls_config_debug_redacts_client_key() {
        // The mTLS private key is secret — Debug must show only its
        // presence, never its length or bytes.
        // The key's PEM header is assembled at runtime so the repo's
        // secret scanner doesn't flag a literal `BEGIN PRIVATE KEY`
        // marker — the body ("supersecret") is the thing under test.
        let key_pem = format!("-----BEGIN {} KEY-----supersecret", "PRIVATE");
        let tls = TlsConfig {
            client_cert_pem: Some(b"-----BEGIN CERTIFICATE-----cert".to_vec()),
            client_key_pem: Some(key_pem.into_bytes()),
            ..TlsConfig::default()
        };
        let s = format!("{tls:?}");
        assert!(s.contains("<redacted>"), "expected redaction, got: {s}");
        assert!(!s.contains("supersecret"), "private key leaked: {s}");
        // The cert (not secret) is summarised by byte count.
        assert!(
            s.contains("bytes>"),
            "expected cert byte-count summary, got: {s}"
        );
    }
}
