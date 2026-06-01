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
#[derive(Debug, Clone, Default)]
pub struct DefaultOpts {
    pub catalog: Option<String>,
    pub schema: Option<String>,
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
    /// behaviour. At session-create time the kernel sets the
    /// `can_cloud_download` session conf to `"false"` so SEA serves
    /// **all** result sizes via the inline-Arrow path, regardless of
    /// result size — see the SEA inline-Arrow design (DD1) for the wire
    /// contract. Users who need inline-only behaviour (e.g. workspaces
    /// without cloud-storage egress) should set this to `false`.
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
    /// **except** for keys synthesised from typed client-side
    /// knobs that have a wire counterpart:
    ///
    /// - [`ResultConfig::cloudfetch_enabled`] `= false` synthesises
    ///   `can_cloud_download = "false"` (case-insensitive on the
    ///   key) so SEA serves results inline. If the user already
    ///   supplied a non-empty `can_cloud_download` value under
    ///   any casing, the user's value wins and no synthesis
    ///   happens. The synthesis path also purges empty-string
    ///   case-variants the user may have left in the map (the
    ///   server's behaviour for empty values is undefined) so the
    ///   wire body carries at most one `can_cloud_download`-shaped
    ///   key.
    ///
    /// Future typed-knob → session_conf synthesis follows the
    /// same pattern (the wrapping `inject_default(...)` helper can
    /// be lifted out once a second knob lands here).
    ///
    /// **Boolean-valued conf keys** (e.g. `can_cloud_download`)
    /// should use the exact strings `"true"` / `"false"`. SEA
    /// accepts these literally; other forms (`"True"`, `"yes"`,
    /// `"1"`) either fail at session-create or behave as
    /// undefined on the server. The kernel does not pre-validate
    /// — its posture is "trust the user's session_conf" — so
    /// non-canonical values flow to the server as-is.
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
    // `CAN_CLOUD_DOWNLOAD` is the kernel-synthesised flag (set by
    // `Session::open` when `ResultConfig::cloudfetch_enabled = false`)
    // telling SEA to serve every result via the inline-Arrow path.
    // Listed here so a user-supplied value (with any casing) survives
    // the allowlist filter and so the kernel's own synthesis passes
    // through normalisation without special-casing.
    "CAN_CLOUD_DOWNLOAD",
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

/// Filter a caller-supplied `session_conf` map against
/// [`SESSION_CONF_ALLOWLIST`]. Unknown keys are dropped with a
/// `tracing::warn!`; known keys are normalized to uppercase (the
/// server echoes uppercase, so SET-readback matches).
pub(crate) fn filter_session_conf(
    raw: std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    use std::collections::HashMap;
    let mut out: HashMap<String, String> = HashMap::with_capacity(raw.len());
    for (k, v) in raw {
        let upper = k.to_ascii_uppercase();
        if SESSION_CONF_ALLOWLIST.contains(&upper.as_str()) {
            out.insert(upper, v);
        } else {
            tracing::warn!(
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
        };
        let s = format!("{tls:?}");
        assert!(s.contains("<"), "expected byte-count summary, got: {s}");
        assert!(
            s.contains("bytes>"),
            "expected byte-count summary, got: {s}"
        );
        assert!(!s.contains("BEGIN CERTIFICATE"), "raw bytes leaked: {s}");
    }
}
