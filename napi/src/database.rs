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

#[cfg(not(test))]
use napi::bindgen_prelude::Buffer;
#[cfg(test)]
type Buffer = Vec<u8>;

use databricks_sql_kernel::{
    AuthConfig, DefaultOpts, DriverSystemConfiguration, Error as KernelError,
    GeospatialResultRepresentation, HttpConfig, ProxyConfig, ResultConfig, Session,
    TelemetryConfig, TlsConfig, TokenCacheConfig,
};

/// NodeJS-driver-appropriate default for `pool_max_idle_per_host`.
///
/// Applied by [`open_session`] when the JS caller does not pass
/// `maxConnections`. Matches the JDBC driver's `HttpConnectionPoolSize`
/// default (100) and closes the throughput gap vs the NodeJS Thrift
/// driver's `maxSockets: Infinity` pool for bursty workloads
/// (concurrent `executeStatement`, parallel CloudFetch chunk downloads).
///
/// The kernel core's [`HttpConfig::pool_max_idle_per_host`] default is
/// also 100 (matching the JDBC `HttpConnectionPoolSize` default), so this
/// constant currently equals it. It is declared here to *pin* napi's
/// user-facing default against future kernel drift — each binding owns its
/// own default rather than silently inheriting the kernel's. The pyo3
/// binding, by contrast, inherits whatever the kernel default is.
pub const NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 100;

use crate::connection::Connection;
use crate::error::napi_err_from_kernel;
use crate::runtime;
use crate::util::guarded;

/// Authentication mode selector crossing the napi boundary. The string
/// literals are what napi-rs emits from this `#[napi(string_enum)]` — the
/// NodeJS SEA adapter (`KernelAuth`) matches them verbatim (`'Pat'`,
/// `'OAuthM2m'`, `'OAuthU2m'`, `'AzureSpM2m'`).
///
/// Mirrors the kernel [`AuthConfig`] variants this binding supports.
/// `OAuthFederation` / `External` are intentionally not exposed yet — the
/// kernel marks federation as not-yet-implemented and `External` is a
/// Rust-trait escape hatch with no JS-callback bridge.
#[napi(string_enum)]
#[derive(Default, Clone, Copy)]
pub enum AuthMode {
    /// Personal access token (`token`).
    #[default]
    Pat,
    /// OAuth 2.0 machine-to-machine — `oauthClientId` + `oauthClientSecret`.
    OAuthM2m,
    /// OAuth 2.0 machine-to-machine with a JWT private-key client
    /// assertion — `oauthClientId` + `jwtKeyFile` + `jwtKid`.
    OAuthM2mJwt,
    /// OAuth 2.0 user-to-machine (browser flow) — optional `oauthClientId`
    /// + `oauthRedirectPort`.
    OAuthU2m,
    /// Azure Entra service-principal M2M — `azureClientId` +
    /// `azureClientSecret` (+ optional `azureTenantId` /
    /// `azureWorkspaceResourceId`). The kernel builds the Entra token
    /// endpoint + `.default` scope, auto-discovers the tenant from the
    /// workspace when `azureTenantId` is omitted, and — when
    /// `azureWorkspaceResourceId` is set — additionally sends the SP
    /// management token for an RBAC-only service principal.
    AzureSpM2m,
}

/// A single extra HTTP header as an explicit `{ name, value }` pair.
///
/// An ordered list of these (`ConnectionOptions.custom_headers`) mirrors
/// the kernel core's `Vec<(String, String)>` and the pyo3 binding's
/// `http_headers`: order is preserved and duplicate `name`s are allowed.
/// A struct (rather than a raw `[name, value]` tuple) because napi-rs
/// does not marshal Rust tuples through `#[napi(object)]` fields; the
/// struct is the idiomatic, self-documenting equivalent and maps to a JS
/// `{ name: string, value: string }`.
#[napi(object)]
pub struct HeaderEntry {
    pub name: String,
    pub value: String,
}

/// Programmatic HTTP/HTTPS proxy configuration, mirroring the kernel's
/// internal [`ProxyConfig`]. Supplied as a structured object rather than a
/// flattened URL so credentials never have to be percent-encoded into the URL
/// and the bypass-host list can be expressed.
///
/// - `url` — proxy endpoint, e.g. `"http://proxy.corp.example.com:8080"`. Must
///   use the `http://` or `https://` scheme.
/// - `username` / `password` — optional proxy basic-auth, applied via
///   `reqwest`'s `Proxy::basic_auth` (not embedded in the URL).
/// - `bypassHosts` — optional comma-separated host/domain list that should
///   bypass the proxy (e.g. `"localhost,*.internal.corp"`).
#[napi(object)]
pub struct ProxyInput {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub bypass_hosts: Option<String>,
}

/// JS-visible options for opening a Databricks SQL session.
///
/// Authentication is selected by `authMode` (default [`AuthMode::Pat`]):
/// - `Pat` — `token` required.
/// - `OAuthM2m` — `oauthClientId` + `oauthClientSecret` required.
/// - `OAuthU2m` — `oauthClientId` / `oauthRedirectPort` optional
///   (defaults to the `databricks-sql-connector` client on port 8030).
/// - `AzureSpM2m` — `azureClientId` + `azureClientSecret` required
///   (+ optional `azureTenantId` / `azureWorkspaceResourceId`).
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
    /// Authentication mode. Omitted ⇒ [`AuthMode::Pat`] (back-compat:
    /// existing PAT callers pass only `token`).
    pub auth_mode: Option<AuthMode>,
    /// Personal access token. Required (and non-empty) for
    /// [`AuthMode::Pat`]; ignored otherwise.
    pub token: Option<String>,
    /// OAuth client id. Required for [`AuthMode::OAuthM2m`]; optional for
    /// [`AuthMode::OAuthU2m`] (defaults to `databricks-sql-connector`).
    pub oauth_client_id: Option<String>,
    /// OAuth client secret. Required for [`AuthMode::OAuthM2m`].
    pub oauth_client_secret: Option<String>,
    /// Localhost callback port for the [`AuthMode::OAuthU2m`] browser
    /// flow. Omitted ⇒ kernel default (8030).
    pub oauth_redirect_port: Option<u16>,
    /// OAuth scopes override (M2M / U2M). Omitted ⇒ kernel defaults
    /// (`["all-apis"]` for M2M; `["sql", "offline_access"]` for U2M).
    pub oauth_scopes: Option<Vec<String>>,
    /// OAuth token endpoint override. When omitted the kernel discovers the
    /// endpoint via the workspace's OIDC configuration. Set it to point the
    /// client-credentials / JWT client-assertion grant at the workspace's
    /// configured IdP token endpoint (e.g. Entra ID for Azure Databricks),
    /// which is required when that IdP — not Databricks-native OIDC — is the
    /// authority for [`AuthMode::OAuthM2m`] / [`AuthMode::OAuthM2mJwt`].
    pub token_url: Option<String>,
    /// U2M on-disk token-cache control (kernel path). Omitted ⇒ the kernel
    /// default (cache enabled, encrypted under a machine-local key, survives
    /// restarts). `false` disables on-disk persistence (re-login each fresh
    /// process); `true` keeps it enabled. Applies to [`AuthMode::OAuthU2m`].
    pub token_cache_enabled: Option<bool>,
    /// Optional passphrase for the U2M on-disk token cache (AES-256 key).
    /// Omitted/blank ⇒ a machine-local derived key. Supplying one is stronger.
    /// A passphrase with no explicit `tokenCacheEnabled` implies enabled.
    /// Applies to [`AuthMode::OAuthU2m`].
    pub token_cache_passphrase: Option<String>,
    /// Path to the PEM private-key file. Required for
    /// [`AuthMode::OAuthM2mJwt`].
    pub jwt_key_file: Option<String>,
    /// JWT key id (`kid`). Required for [`AuthMode::OAuthM2mJwt`].
    pub jwt_kid: Option<String>,
    /// Passphrase for an encrypted PKCS#8 key. Optional for
    /// [`AuthMode::OAuthM2mJwt`] (omit for an unencrypted key).
    pub jwt_passphrase: Option<String>,
    /// JWT signing algorithm (`RS256`/`384`/`512`, `PS256`/`384`/`512`,
    /// `ES256`, `ES384`). Optional for [`AuthMode::OAuthM2mJwt`]
    /// (omitted ⇒ kernel default `RS256`).
    pub jwt_algorithm: Option<String>,
    /// SP-wide Workload Identity Federation client id used during mandatory
    /// token exchange. Omitted selects BYOT / account-wide WIF.
    pub identity_federation_client_id: Option<String>,
    /// Azure Entra app-registration client id. Required for
    /// [`AuthMode::AzureSpM2m`]; ignored otherwise.
    pub azure_client_id: Option<String>,
    /// Azure Entra app-registration client secret. Required for
    /// [`AuthMode::AzureSpM2m`].
    pub azure_client_secret: Option<String>,
    /// Azure Entra tenant id. Optional for [`AuthMode::AzureSpM2m`];
    /// omitted ⇒ the kernel auto-discovers it from the workspace's
    /// `/aad/auth` redirect (matching the Thrift connector).
    pub azure_tenant_id: Option<String>,
    /// Azure workspace ARM resource id. Optional for [`AuthMode::AzureSpM2m`];
    /// when set the kernel also sends the SP management token
    /// (`X-Databricks-Azure-SP-Management-Token`) + the
    /// `X-Databricks-Azure-Workspace-Resource-Id` header, so a service
    /// principal with an Azure RBAC role but no workspace membership can
    /// authenticate. Omitted ⇒ the data token authenticates alone.
    pub azure_workspace_resource_id: Option<String>,
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
    /// Select whether `GEOMETRY` / `GEOGRAPHY` results use their native Arrow
    /// representation. `true` (the default) requests Arrow
    /// `struct<srid: int32, wkb: binary>` values; `false` requests WKT / EWKT
    /// in Arrow UTF-8 values. Binary mode requires the native Reyden Arrow
    /// path. This choice is applied locally and never sent to SEA.
    pub enable_geo_spatial_support: Option<bool>,
    /// Driver name reported in telemetry system configuration. Omitted ⇒
    /// kernel default.
    pub driver_name: Option<String>,
    /// Driver version reported in telemetry system configuration. Omitted ⇒
    /// kernel default.
    pub driver_version: Option<String>,
    /// Runtime name reported in telemetry system configuration. Omitted ⇒
    /// kernel default.
    pub runtime_name: Option<String>,
    /// Runtime version reported in telemetry system configuration. Omitted ⇒
    /// kernel default.
    pub runtime_version: Option<String>,
    /// Runtime vendor reported in telemetry system configuration. Omitted ⇒
    /// kernel default.
    pub runtime_vendor: Option<String>,
    /// Operating system name reported in telemetry system configuration.
    /// Omitted ⇒ kernel default.
    pub os_name: Option<String>,
    /// Operating system version reported in telemetry system configuration.
    /// Omitted ⇒ kernel default.
    pub os_version: Option<String>,
    /// Operating system architecture reported in telemetry system
    /// configuration. Omitted ⇒ kernel default.
    pub os_arch: Option<String>,
    /// Client application name reported in telemetry system configuration.
    /// Omitted ⇒ absent unless the kernel can infer a default.
    pub client_app_name: Option<String>,
    /// Locale name reported in telemetry system configuration. Omitted ⇒
    /// kernel default.
    pub locale_name: Option<String>,
    /// Character-set encoding reported in telemetry system configuration.
    /// Omitted ⇒ kernel default.
    pub char_set_encoding: Option<String>,
    /// Process name reported in telemetry system configuration. Omitted ⇒
    /// kernel default.
    pub process_name: Option<String>,
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
    /// core's [`HttpConfig::pool_max_idle_per_host`] default is also 100
    /// (matching the same JDBC default), so napi pins its own copy rather
    /// than inheriting it. Mirrors the Python connector's
    /// `max_connections` kwarg on the SEA backend, which exposes the
    /// knob but keeps its own urllib3-aligned default of 10.
    ///
    /// Napi-rs serialises `u32` as JS `number`; values up to
    /// `2^32 - 1` round-trip safely (any reasonable pool size fits).
    pub max_connections: Option<u32>,
    /// Render `INTERVAL` / `DURATION` result columns as strings
    /// (`ResultConfig.intervals_as_string`). The kernel default is
    /// native Arrow `month_interval` / `duration[us]` types; the NodeJS
    /// Thrift driver surfaces intervals as strings, so the SEA driver
    /// sets this `true` for byte-compatible parity. Omitted ⇒ kernel
    /// default (native Arrow interval types).
    pub intervals_as_string: Option<bool>,
    /// Render complex (`ARRAY` / `MAP` / `STRUCT` / `VARIANT`) result
    /// columns as JSON strings (`ResultConfig.complex_types_as_json`)
    /// instead of native Arrow nested types. Omitted ⇒ kernel default
    /// (native Arrow nested types, which the NodeJS Arrow decoder
    /// already renders identically to the Thrift path).
    pub complex_types_as_json: Option<bool>,
    /// Whether to verify the server's TLS certificate.
    ///
    /// Omitted / `true` ⇒ strict validation against the system / Mozilla
    /// trust store (full chain + expiry + hostname), matching JDBC / ODBC
    /// and every modern HTTPS client. This is the **default** for the SEA
    /// backend — secure by default.
    ///
    /// `false` ⇒ permissive: accept self-signed / untrusted / expired
    /// certs AND skip the hostname-vs-SNI check. This is **insecure** (no
    /// protection against active MITM); it exists only as an opt-out for
    /// parity with the legacy NodeJS Thrift driver, which hard-codes
    /// `rejectUnauthorized: false`. Prefer pairing strict checking with
    /// `custom_ca_cert` over disabling verification entirely.
    ///
    /// This is the master verify toggle: `false` disables chain validation
    /// (`TlsConfig::accept_self_signed`) **and** subsumes the hostname
    /// check (`skip_hostname_verification`), regardless of
    /// `check_server_certificate_hostname`.
    pub check_server_certificate: Option<bool>,
    /// Whether to verify that the server certificate matches the host
    /// (hostname-vs-SNI check), **independently** of full chain validation.
    ///
    /// Omitted / `true` ⇒ the hostname check runs (the secure default).
    /// `false` ⇒ skip only the hostname check while still validating the
    /// chain + expiry against the trust store — for connecting via an IP
    /// literal or a host the cert wasn't issued for, without dropping all
    /// validation. Ignored (already implied) when
    /// `check_server_certificate` is `false`, which disables everything.
    ///
    /// Mirrors the Python connector's `_tls_verify_hostname` knob and the
    /// kernel's [`TlsConfig::skip_hostname_verification`] (= `!check`).
    pub check_server_certificate_hostname: Option<bool>,
    /// PEM-encoded CA certificate bytes to add to the trust store on
    /// top of the system roots. Use for corporate TLS-inspecting
    /// proxies that re-sign TLS, or on-prem deployments with an
    /// internal CA. Honoured regardless of `check_server_certificate`.
    /// Maps onto the kernel [`TlsConfig::custom_ca_cert`].
    pub custom_ca_cert: Option<Buffer>,
    /// PEM-encoded client certificate for mutual TLS (mTLS). Set this
    /// together with `client_key_pem` when the server requires the
    /// client to present a certificate. A PEM carrying a leaf cert
    /// optionally followed by its intermediate chain is accepted.
    /// Maps onto the kernel [`TlsConfig::client_cert_pem`].
    ///
    /// `client_cert_pem` and `client_key_pem` must be supplied together;
    /// the kernel rejects setting only one at `open_session` with
    /// `InvalidArgument`.
    pub client_cert_pem: Option<Buffer>,
    /// PEM-encoded private key for the mTLS client certificate. Set this
    /// together with `client_cert_pem`. For portability across the
    /// kernel's TLS backends supply a PKCS#8 key (`BEGIN PRIVATE KEY`).
    /// Maps onto the kernel [`TlsConfig::client_key_pem`].
    pub client_key_pem: Option<Buffer>,
    /// Extra HTTP headers to send on every request — the route for
    /// caller-supplied headers (the NodeJS driver's `customHeaders` and
    /// the composed `User-Agent`). Maps onto the kernel
    /// [`HttpConfig::custom_headers`].
    ///
    /// An **ordered list** of `(name, value)` pairs, mirroring the kernel
    /// core's `Vec<(String, String)>` and the pyo3 binding's
    /// `http_headers` — order is preserved and duplicate names are
    /// allowed (the kernel emits each entry, and uses the **last**
    /// `User-Agent` as its effective UA).
    ///
    /// Three names are handled specially by the kernel:
    /// - `Authorization` / `x-databricks-org-id` are **reserved** — a
    ///   caller entry for either is silently dropped (skip-and-warn) so
    ///   auth and multi-tenant routing can't be hijacked by a custom
    ///   header. (The NodeJS driver also drops these before they cross
    ///   the FFI, matching the Python connector's double-wall.)
    /// - `User-Agent` **replaces** the `DatabricksSQLKernel/...` base.
    ///   Its leading prefix must therefore be allow-listed by the SEA
    ///   server. The NodeJS driver folds its `userAgentEntry` into a
    ///   `User-Agent` entry here.
    pub custom_headers: Option<Vec<HeaderEntry>>,

    /// Whether SEA API responses may use HTTP compression. Defaults to false.
    pub sea_response_compression_enabled: Option<bool>,

    /// Retry/backoff tuning — all optional. An unset field keeps the kernel's
    /// built-in policy (1s/60s exponential backoff, 6 total attempts, 900s
    /// budget). Mirrors the pyo3 binding's `retry_*` kwargs so the Node.js
    /// driver can forward the same retry knobs the Python connector does.
    ///
    /// Lower bound of the exponential backoff (also clamps a server
    /// `Retry-After`). Maps onto [`HttpConfig::retry_min_wait`].
    pub retry_min_wait_secs: Option<u32>,
    /// Upper bound of the exponential backoff. Maps onto
    /// [`HttpConfig::retry_max_wait`].
    pub retry_max_wait_secs: Option<u32>,
    /// **Total** number of attempts (matching the connector's
    /// `_retry_stop_after_attempts_count` and JDBC count semantics). The
    /// kernel's [`HttpConfig::retry_max_retries`] counts retries *after* the
    /// first attempt, so this is converted with `max(0, attempts - 1)` in
    /// [`build_http_config`] — `0` / `1` both mean a single attempt, no retry.
    pub retry_max_attempts: Option<u32>,
    /// Overall retry budget in whole seconds. Maps onto
    /// [`HttpConfig::overall_timeout`].
    pub retry_overall_timeout_secs: Option<u32>,
    /// Enable kernel telemetry export. Omitted / `false` keeps telemetry
    /// disabled. When enabled, the kernel records operation telemetry below the
    /// binding layer and exports through the Databricks SQL driver telemetry
    /// endpoint.
    pub telemetry_enabled: Option<bool>,
    /// Number of telemetry events buffered before a flush. Omitted ⇒ kernel
    /// default. Must be greater than zero when supplied.
    pub telemetry_batch_size: Option<u32>,
    /// Periodic telemetry flush interval in milliseconds. Omitted ⇒ kernel
    /// default. Must be greater than zero when supplied.
    pub telemetry_flush_interval_ms: Option<u32>,
    /// Maximum telemetry export retries. `0` disables telemetry export retries.
    pub telemetry_max_retries: Option<u32>,
    /// Fixed telemetry retry delay in milliseconds. `0` means immediate retry.
    pub telemetry_retry_delay_ms: Option<u32>,
    /// Maximum time explicit close waits for telemetry final flush, in
    /// milliseconds. Omitted ⇒ kernel default. Must be greater than zero when
    /// supplied.
    pub telemetry_close_flush_timeout_ms: Option<u32>,
    /// Enable telemetry circuit-breaker protection. Omitted ⇒ kernel default.
    pub telemetry_circuit_breaker_enabled: Option<bool>,
    /// Consecutive telemetry export failures before opening the circuit.
    /// Omitted ⇒ kernel default. Must be greater than zero when the circuit
    /// breaker is enabled.
    pub telemetry_circuit_breaker_threshold: Option<u32>,
    /// Time the telemetry circuit stays open before a half-open probe, in
    /// milliseconds. Omitted ⇒ kernel default. Must be greater than zero when
    /// the circuit breaker is enabled.
    pub telemetry_circuit_breaker_timeout_ms: Option<u32>,
    /// Programmatic HTTP/HTTPS proxy ([`ProxyInput`]) to route all kernel
    /// traffic through. Carries the proxy `url`, optional basic-auth
    /// `username` / `password`, and an optional `bypassHosts` list — mapped
    /// field-for-field onto the kernel [`ProxyConfig`].
    ///
    /// Omitted ⇒ the kernel does NOT configure a proxy explicitly and
    /// `reqwest`'s standard behaviour applies — the `HTTPS_PROXY` /
    /// `HTTP_PROXY` / `NO_PROXY` environment variables are still honoured.
    /// Setting this **overrides** those env vars. This complements the env-var
    /// path: callers who cannot set process env vars (e.g. a long-lived Node
    /// server) can now route a single connection through a proxy
    /// programmatically.
    pub proxy: Option<ProxyInput>,
    /// Per-connection socket read timeout, in milliseconds. Caps how
    /// long a single HTTP round-trip may block waiting on the server
    /// before the request errors out. Maps onto the kernel
    /// [`HttpConfig::request_timeout`] (the internal reqwest
    /// `Client::timeout`).
    ///
    /// Omitted ⇒ kernel default (120 000 ms / 120 s). Napi-rs
    /// serialises `u32` as JS `number`; the largest representable value
    /// (~49.7 days) far exceeds any sensible socket timeout.
    pub socket_timeout_ms: Option<u32>,
}

/// Map the JS `ConnectionOptions` auth fields onto the kernel
/// [`AuthConfig`]. Per-mode required-field validation lives in the
/// kernel's validating constructors ([`AuthConfig::pat`] /
/// [`AuthConfig::oauth_m2m`]); this binding just forwards the JS
/// optionals and maps any kernel `InvalidArgument` onto a napi error.
/// Pure (borrows `options`) so it is unit-testable without opening a
/// session.
fn build_auth_config(options: &ConnectionOptions) -> napi::Result<AuthConfig> {
    match options.auth_mode.unwrap_or_default() {
        AuthMode::Pat => AuthConfig::pat(
            options.token.clone(),
            options.identity_federation_client_id.clone(),
        )
        .map_err(napi_err_from_kernel),
        AuthMode::OAuthM2m => AuthConfig::oauth_m2m(
            options.oauth_client_id.clone(),
            options.oauth_client_secret.clone(),
            options.oauth_scopes.clone(),
            options.token_url.clone(),
            options.identity_federation_client_id.clone(),
        )
        .map_err(napi_err_from_kernel),
        // JWT private-key client assertion. client_id + jwt_key_file +
        // jwt_kid are mandatory; passphrase/algorithm are optional (the
        // kernel defaults the algorithm to RS256 and validates the
        // key/algorithm/passphrase at session open).
        AuthMode::OAuthM2mJwt => AuthConfig::oauth_m2m_private_key(
            options.oauth_client_id.clone(),
            options.jwt_key_file.clone(),
            options.jwt_kid.clone(),
            options.jwt_passphrase.clone(),
            options.jwt_algorithm.clone(),
            options.oauth_scopes.clone(),
            options.token_url.clone(),
        )
        .map_err(napi_err_from_kernel),
        // napi injects no client_id/scope default: it rides the kernel's
        // single U2M default bundle (databricks-sql-connector / sql
        // offline_access / port 8030), which already IS the standalone Node
        // driver's OAuth app identity. See PECOBLR-4039.
        AuthMode::OAuthU2m => {
            // An explicit empty client_id is still rejected — it would bind
            // an empty OAuth client and surface an opaque server error. Omit
            // the field to use the kernel default.
            if matches!(options.oauth_client_id.as_deref(), Some("")) {
                return Err(napi_err_from_kernel(
                    KernelError::invalid_argument().message(
                        "oauthClientId must be non-empty when set; omit it to use the \
                         kernel default 'databricks-sql-connector'",
                    ),
                ));
            }
            Ok(AuthConfig::oauth_u2m(
                options.oauth_client_id.clone(),
                // Node exposes a single redirect port; forward it as a
                // one-element candidate list (kernel binds the first free one).
                // A `0` port would bind an OS-assigned ephemeral port that can
                // never match the IdP's static redirect_uri, so map `0 -> None`
                // to fall back to the kernel default (matches C ABI + pyo3).
                options
                    .oauth_redirect_port
                    .filter(|&p| p != 0)
                    .map(|p| vec![p]),
                options.oauth_scopes.clone(),
                options.token_url.clone(),
                None,
                // U2M on-disk token-cache control. Both omitted ⇒ kernel
                // default (enabled, machine-local key). Otherwise honor the
                // caller's enable flag / passphrase; a passphrase alone
                // implies enabled.
                match (options.token_cache_enabled, &options.token_cache_passphrase) {
                    (None, None) => None,
                    (enabled, passphrase) => Some(TokenCacheConfig {
                        enabled: enabled.unwrap_or(true),
                        passphrase: passphrase.clone(),
                    }),
                },
                options.identity_federation_client_id.clone(),
            ))
        }
        // Azure Entra service-principal M2M. The kernel owns Azure
        // resolution: it builds the Entra token endpoint + `.default` scope
        // and auto-discovers the tenant from the workspace when
        // `azureTenantId` is omitted. The binding forwards the Entra
        // credentials (+ optional tenant + optional workspace resource id for
        // the management-token resource-id header). Mirrors the pyo3
        // `azure-sp-m2m` arm.
        AuthMode::AzureSpM2m => AuthConfig::azure_service_principal(
            options.azure_client_id.clone(),
            options.azure_client_secret.clone(),
            options.azure_tenant_id.clone(),
            options.azure_workspace_resource_id.clone(),
            options.identity_federation_client_id.clone(),
        )
        .map_err(napi_err_from_kernel),
    }
}

/// Map the JS TLS fields onto the kernel [`TlsConfig`].
///
/// The default (no toggles) is **secure** — it keeps the kernel's strict
/// [`TlsConfig::default()`] (full chain + expiry + hostname validation).
///
/// Two independent toggles, mirroring the Python connector's `tls_verify`
/// / `tls_verify_hostname`:
/// - `check_server_certificate: false` relaxes chain validation
///   (`accept_self_signed`) **and** subsumes the hostname check — the
///   legacy NodeJS Thrift driver's permissive `rejectUnauthorized: false`
///   behaviour.
/// - `check_server_certificate_hostname: false` skips **only** the
///   hostname check while the chain is still validated. When the master
///   `check_server_certificate` is `false`, the hostname check is already
///   off, so this flag is a no-op in that case.
///
/// A `custom_ca_cert` is always added to the trust store regardless of the
/// strictness toggles.
///
/// `TlsConfig` is `#[non_exhaustive]` — construct via `default()` then
/// mutate (same pattern as `ResultConfig` / `HttpConfig` above). Pure
/// (borrows `options`) so it is unit-testable without opening a session.
fn build_tls_config(options: &ConnectionOptions) -> TlsConfig {
    // Secure-by-default: only an explicit `false` relaxes verification.
    let verify = options.check_server_certificate.unwrap_or(true);
    let verify_hostname = options.check_server_certificate_hostname.unwrap_or(true);
    let mut tls = TlsConfig::default();
    tls.accept_self_signed = !verify;
    // Disabling chain validation subsumes the hostname check; otherwise the
    // hostname check follows its own toggle. Matches the Python connector
    // (`tls_verify=False` ⇒ both skips; `tls_verify_hostname=False` ⇒
    // hostname skip only).
    tls.skip_hostname_verification = !verify || !verify_hostname;
    if let Some(ca) = &options.custom_ca_cert {
        tls.custom_ca_cert = Some(ca.to_vec());
    }
    // mTLS client identity. Threaded through verbatim; the kernel enforces
    // the both-or-neither rule (and validates the PEM/key pairing) at
    // `Session::open`, surfacing `InvalidArgument` if only one is set.
    if let Some(cert) = &options.client_cert_pem {
        tls.client_cert_pem = Some(cert.to_vec());
    }
    if let Some(key) = &options.client_key_pem {
        tls.client_key_pem = Some(key.to_vec());
    }
    tls
}

/// Map the JS HTTP fields onto the kernel [`HttpConfig`].
///
/// Always sets `pool_max_idle_per_host` — to the JS-supplied
/// `max_connections` when present, otherwise the NodeJS-driver-appropriate
/// [`NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST`] (100; pinned independently of
/// the kernel default, which is also 100 today). `custom_headers` is
/// forwarded when supplied; the kernel handles the reserved/`User-Agent`
/// special cases. `HttpConfig` is `#[non_exhaustive]` — construct via
/// `default()` then mutate. Consumes the `custom_headers` list out of
/// `options` (pure otherwise) so it does not have to clone the header
/// strings.
fn build_http_config(options: &mut ConnectionOptions) -> HttpConfig {
    let mut http = HttpConfig::default();
    http.sea_response_compression_enabled =
        options.sea_response_compression_enabled.unwrap_or(false);
    http.pool_max_idle_per_host = options
        .max_connections
        .map(|max| max as usize)
        .unwrap_or(NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST);
    if let Some(headers) = options.custom_headers.take() {
        // `Vec<HeaderEntry>` → the kernel core's `Vec<(String, String)>`,
        // preserving order and duplicates (a `HashMap` would have collapsed
        // duplicate names).
        http.custom_headers = headers.into_iter().map(|h| (h.name, h.value)).collect();
    }
    // Retry/backoff knobs — only override the kernel default when the caller
    // set one. `retry_max_attempts` is a TOTAL attempt count; the kernel's
    // `retry_max_retries` counts retries *after* the first, so convert with
    // `saturating_sub(1)` (0/1 ⇒ no retries). Centralises the off-by-one here,
    // mirroring the pyo3 binding's `Session::new`.
    if let Some(s) = options.retry_min_wait_secs {
        http.retry_min_wait = std::time::Duration::from_secs(u64::from(s));
    }
    if let Some(s) = options.retry_max_wait_secs {
        http.retry_max_wait = std::time::Duration::from_secs(u64::from(s));
    }
    if let Some(attempts) = options.retry_max_attempts {
        http.retry_max_retries = attempts.saturating_sub(1);
    }
    if let Some(s) = options.retry_overall_timeout_secs {
        http.overall_timeout = std::time::Duration::from_secs(u64::from(s));
    }
    // Per-connection socket read timeout. Maps onto the kernel's
    // `request_timeout` (the reqwest `Client::timeout`). Omitted ⇒ kernel
    // default (120 s).
    if let Some(ms) = options.socket_timeout_ms {
        http.request_timeout = std::time::Duration::from_millis(u64::from(ms));
    }
    http
}

/// Map JS telemetry fields onto the kernel [`TelemetryConfig`].
///
/// Returns `None` when the caller did not mention telemetry at all, preserving
/// the kernel default. A caller may set telemetry knobs while leaving
/// `telemetry_enabled` false; the config is still passed through, but telemetry
/// remains disabled until explicitly enabled.
fn build_telemetry_config(options: &ConnectionOptions) -> napi::Result<Option<TelemetryConfig>> {
    let mentioned = options.telemetry_enabled.is_some()
        || options.telemetry_batch_size.is_some()
        || options.telemetry_flush_interval_ms.is_some()
        || options.telemetry_max_retries.is_some()
        || options.telemetry_retry_delay_ms.is_some()
        || options.telemetry_close_flush_timeout_ms.is_some()
        || options.telemetry_circuit_breaker_enabled.is_some()
        || options.telemetry_circuit_breaker_threshold.is_some()
        || options.telemetry_circuit_breaker_timeout_ms.is_some();
    if !mentioned {
        return Ok(None);
    }

    if matches!(options.telemetry_batch_size, Some(0)) {
        return Err(napi_err_from_kernel(
            KernelError::invalid_argument().message("telemetryBatchSize must be > 0"),
        ));
    }
    if matches!(options.telemetry_flush_interval_ms, Some(0)) {
        return Err(napi_err_from_kernel(
            KernelError::invalid_argument().message("telemetryFlushIntervalMs must be > 0"),
        ));
    }
    if matches!(options.telemetry_close_flush_timeout_ms, Some(0)) {
        return Err(napi_err_from_kernel(
            KernelError::invalid_argument().message("telemetryCloseFlushTimeoutMs must be > 0"),
        ));
    }
    if matches!(options.telemetry_circuit_breaker_threshold, Some(0))
        && options.telemetry_circuit_breaker_enabled.unwrap_or(true)
    {
        return Err(napi_err_from_kernel(
            KernelError::invalid_argument()
                .message("telemetryCircuitBreakerThreshold must be > 0 when enabled"),
        ));
    }
    if matches!(options.telemetry_circuit_breaker_timeout_ms, Some(0))
        && options.telemetry_circuit_breaker_enabled.unwrap_or(true)
    {
        return Err(napi_err_from_kernel(
            KernelError::invalid_argument()
                .message("telemetryCircuitBreakerTimeoutMs must be > 0 when enabled"),
        ));
    }

    let mut telemetry = TelemetryConfig::default();
    telemetry.enabled = options.telemetry_enabled.unwrap_or(false);
    if let Some(v) = options.telemetry_batch_size {
        telemetry.batch_size = v as usize;
    }
    if let Some(v) = options.telemetry_flush_interval_ms {
        telemetry.flush_interval = std::time::Duration::from_millis(u64::from(v));
    }
    if let Some(v) = options.telemetry_max_retries {
        telemetry.max_retries = v;
    }
    if let Some(v) = options.telemetry_retry_delay_ms {
        telemetry.retry_delay = std::time::Duration::from_millis(u64::from(v));
    }
    if let Some(v) = options.telemetry_close_flush_timeout_ms {
        telemetry.close_flush_timeout = std::time::Duration::from_millis(u64::from(v));
    }
    if let Some(v) = options.telemetry_circuit_breaker_enabled {
        telemetry.circuit_breaker_enabled = v;
    }
    if let Some(v) = options.telemetry_circuit_breaker_threshold {
        telemetry.circuit_breaker_threshold = v;
    }
    if let Some(v) = options.telemetry_circuit_breaker_timeout_ms {
        telemetry.circuit_breaker_timeout = std::time::Duration::from_millis(u64::from(v));
    }
    Ok(Some(telemetry))
}

/// Map JS driver/system fields onto the kernel [`DriverSystemConfiguration`].
///
/// Returns `None` when the caller did not mention any field, preserving the
/// kernel defaults. When at least one field is supplied, the kernel fills any
/// missing values it knows how to derive.
fn build_driver_system_configuration(
    options: &ConnectionOptions,
) -> Option<DriverSystemConfiguration> {
    let mentioned = options.driver_name.is_some()
        || options.driver_version.is_some()
        || options.runtime_name.is_some()
        || options.runtime_version.is_some()
        || options.runtime_vendor.is_some()
        || options.os_name.is_some()
        || options.os_version.is_some()
        || options.os_arch.is_some()
        || options.client_app_name.is_some()
        || options.locale_name.is_some()
        || options.char_set_encoding.is_some()
        || options.process_name.is_some();
    mentioned.then(|| DriverSystemConfiguration {
        driver_version: options.driver_version.clone(),
        runtime_name: options.runtime_name.clone(),
        runtime_version: options.runtime_version.clone(),
        runtime_vendor: options.runtime_vendor.clone(),
        os_name: options.os_name.clone(),
        os_version: options.os_version.clone(),
        os_arch: options.os_arch.clone(),
        driver_name: options.driver_name.clone(),
        client_app_name: options.client_app_name.clone(),
        locale_name: options.locale_name.clone(),
        char_set_encoding: options.char_set_encoding.clone(),
        process_name: options.process_name.clone(),
    })
}

fn geospatial_result_representation(enabled: Option<bool>) -> GeospatialResultRepresentation {
    if enabled.unwrap_or(true) {
        GeospatialResultRepresentation::Binary
    } else {
        GeospatialResultRepresentation::String
    }
}

/// Open a Databricks SQL session and return an opaque `Connection`
/// wrapping the kernel `Session`. Authentication is selected by
/// `options.auth_mode` (PAT / OAuth M2M / OAuth U2M) — see
/// [`build_auth_config`].
///
/// The JS-visible name is `openSession` (napi-rs converts snake_case
/// to camelCase for free functions).
#[allow(
    dead_code,
    reason = "exported via napi macro codegen; not called from Rust"
)]
#[napi]
pub async fn open_session(mut options: ConnectionOptions) -> napi::Result<Connection> {
    guarded(async move {
        // Cache the napi-rs tokio Handle on the very first async call
        // so Drop impls (which run on the V8 GC thread, outside any
        // tokio context) can still `spawn` cleanup tasks onto the
        // runtime that's driving this future.
        let _ = runtime::get_handle();

        // Resolve auth before the builder moves `host_name` / `http_path`
        // out of `options` (validation can fail fast, before any network).
        let auth_config = build_auth_config(&options)?;
        // Build the TLS config before the builder moves fields out of
        // `options`. Always wired (even at defaults) because the
        // permissive-by-default stance needs the relaxation flags set —
        // omitting `.tls()` would fall back to the kernel's strict
        // defaults, which is the opposite of the Thrift-parity default.
        let tls_config = build_tls_config(&options);
        // Build the HTTP config (pool sizing + custom headers) before the
        // builder moves the transport fields out of `options`. Takes
        // `&mut` because it moves the `custom_headers` list out of
        // `options` to avoid cloning the header strings.
        let http_config = build_http_config(&mut options);
        let telemetry_config = build_telemetry_config(&options)?;
        let driver_system_configuration = build_driver_system_configuration(&options);

        // SessionConfig is `#[non_exhaustive]` — go through the
        // builder, which is the only public path that constructs it.
        // `http_path()` is the convenience setter that maps a bare
        // hostname + `/sql/1.0/warehouses/{id}` path into the kernel's
        // `ConnectionConfig`.
        let mut builder = Session::builder()
            .http_path(options.host_name, options.http_path)
            .auth(auth_config);
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
        builder = builder.geospatial_result_representation(geospatial_result_representation(
            options.enable_geo_spatial_support,
        ));
        // Result-config knobs (interval / complex-type rendering). Merge
        // into one `ResultConfig` so the two toggles don't clobber each
        // other when both are set. Only wire it when at least one is
        // requested, leaving the kernel default otherwise. `ResultConfig`
        // is `#[non_exhaustive]` — construct via `default()` then mutate.
        let intervals_as_string = options.intervals_as_string.unwrap_or(false);
        let complex_types_as_json = options.complex_types_as_json.unwrap_or(false);
        if intervals_as_string || complex_types_as_json {
            let mut result = ResultConfig::default();
            result.intervals_as_string = intervals_as_string;
            result.complex_types_as_json = complex_types_as_json;
            builder = builder.result(result);
        }
        // HTTP config (pool sizing + custom headers + retry knobs +
        // socket-read timeout) was built up-front by `build_http_config`
        // before the transport fields moved.
        builder = builder.http(http_config);
        builder = builder.tls(tls_config);
        if let Some(telemetry) = telemetry_config {
            builder = builder.telemetry(telemetry);
        }
        if let Some(system_configuration) = driver_system_configuration {
            builder = builder.driver_system_configuration(system_configuration);
        }
        // Programmatic HTTP/HTTPS proxy. Only wire a `ProxyConfig` when a
        // proxy is supplied; otherwise leave it unset so the kernel keeps
        // reqwest's env-var behaviour (HTTPS_PROXY / NO_PROXY).
        // `ProxyConfig` is `#[non_exhaustive]` — construct via `default()`
        // then mutate, mapping the structured `ProxyInput` field-for-field.
        if let Some(p) = options.proxy {
            let mut proxy = ProxyConfig::default();
            proxy.url = Some(p.url);
            proxy.username = p.username;
            proxy.password = p.password;
            proxy.bypass_hosts = p.bypass_hosts;
            builder = builder.proxy(proxy);
        }
        let session = builder.open().await.map_err(napi_err_from_kernel)?;
        let session_id = session.session_id().to_owned();
        Ok(Connection {
            inner: Arc::new(Mutex::new(Some(session))),
            session_id,
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal options with only the required transport fields set;
    /// auth fields default to "absent" so each test sets what it needs.
    fn base() -> ConnectionOptions {
        ConnectionOptions {
            host_name: "adb-1.azuredatabricks.net".to_owned(),
            http_path: "/sql/1.0/warehouses/abc".to_owned(),
            auth_mode: None,
            token: None,
            oauth_client_id: None,
            oauth_client_secret: None,
            oauth_redirect_port: None,
            oauth_scopes: None,
            token_url: None,
            token_cache_enabled: None,
            token_cache_passphrase: None,
            jwt_key_file: None,
            jwt_kid: None,
            jwt_passphrase: None,
            jwt_algorithm: None,
            identity_federation_client_id: None,
            azure_client_id: None,
            azure_client_secret: None,
            azure_tenant_id: None,
            azure_workspace_resource_id: None,
            catalog: None,
            schema: None,
            session_conf: None,
            enable_geo_spatial_support: None,
            driver_name: None,
            driver_version: None,
            runtime_name: None,
            runtime_version: None,
            runtime_vendor: None,
            os_name: None,
            os_version: None,
            os_arch: None,
            client_app_name: None,
            locale_name: None,
            char_set_encoding: None,
            process_name: None,
            max_connections: None,
            intervals_as_string: None,
            complex_types_as_json: None,
            check_server_certificate: None,
            check_server_certificate_hostname: None,
            custom_ca_cert: None,
            client_cert_pem: None,
            client_key_pem: None,
            custom_headers: None,
            sea_response_compression_enabled: None,
            retry_min_wait_secs: None,
            retry_max_wait_secs: None,
            retry_max_attempts: None,
            retry_overall_timeout_secs: None,
            telemetry_enabled: None,
            telemetry_batch_size: None,
            telemetry_flush_interval_ms: None,
            telemetry_max_retries: None,
            telemetry_retry_delay_ms: None,
            telemetry_close_flush_timeout_ms: None,
            telemetry_circuit_breaker_enabled: None,
            telemetry_circuit_breaker_threshold: None,
            telemetry_circuit_breaker_timeout_ms: None,
            proxy: None,
            socket_timeout_ms: None,
        }
    }

    #[test]
    fn enable_geospatial_support_maps_to_kernel_representation() {
        assert_eq!(
            geospatial_result_representation(None),
            GeospatialResultRepresentation::Binary
        );
        assert_eq!(
            geospatial_result_representation(Some(true)),
            GeospatialResultRepresentation::Binary
        );
        assert_eq!(
            geospatial_result_representation(Some(false)),
            GeospatialResultRepresentation::String
        );
    }

    /// Assert a `build_auth_config` failure surfaces as a kernel
    /// `InvalidArgument` (napi `Status::InvalidArg` + `"code":
    /// "InvalidArgument"` in the error envelope) and that the kernel
    /// constructor's message text contains `substr`. The required-field
    /// validation now lives in the kernel ctors, so the binding tests
    /// assert the kernel error flows through `napi_err_from_kernel`.
    fn assert_invalid_arg(err: napi::Error, substr: &str) {
        assert_eq!(err.status, napi::Status::InvalidArg);
        let reason = err.reason.to_string();
        assert!(
            reason.contains("\"code\":\"InvalidArgument\""),
            "expected InvalidArgument envelope, got: {reason}"
        );
        assert!(
            reason.contains(substr),
            "expected message to contain {substr:?}, got: {reason}"
        );
    }

    #[test]
    fn default_mode_is_pat_and_requires_token() {
        let mut o = base();
        // No authMode + no token → fail-fast InvalidArgument from the
        // kernel `AuthConfig::pat` ctor.
        assert_invalid_arg(
            build_auth_config(&o).unwrap_err(),
            "requires a non-empty access token",
        );
        o.token = Some("dapi-xxx".to_owned());
        assert!(matches!(
            build_auth_config(&o).unwrap(),
            AuthConfig::Pat { .. }
        ));
    }

    #[test]
    fn azure_sp_m2m_builds_azure_service_principal() {
        // azure-sp-m2m forwards the Entra SP credentials (+ optional tenant +
        // optional workspace resource id) to the kernel's Azure SP flow.
        let mut o = base();
        o.auth_mode = Some(AuthMode::AzureSpM2m);
        o.azure_client_id = Some("entra-app-id".to_owned());
        o.azure_client_secret = Some("entra-secret".to_owned());
        o.azure_tenant_id = Some("tenant-abc".to_owned());
        o.azure_workspace_resource_id =
            Some("/subscriptions/s/resourceGroups/rg/workspace/w".to_owned());
        match build_auth_config(&o).unwrap() {
            AuthConfig::AzureServicePrincipal {
                azure_client_id,
                azure_client_secret,
                azure_tenant_id,
                azure_workspace_resource_id,
                ..
            } => {
                assert_eq!(azure_client_id, "entra-app-id");
                assert_eq!(azure_client_secret, "entra-secret");
                assert_eq!(azure_tenant_id.as_deref(), Some("tenant-abc"));
                assert_eq!(
                    azure_workspace_resource_id.as_deref(),
                    Some("/subscriptions/s/resourceGroups/rg/workspace/w")
                );
            }
            other => panic!("expected AzureServicePrincipal, got {other:?}"),
        }
    }

    #[test]
    fn azure_sp_m2m_tenant_and_resource_id_optional() {
        // Omitting the tenant + resource id is valid: the kernel auto-discovers
        // the tenant and sends the data token alone (no management token).
        let mut o = base();
        o.auth_mode = Some(AuthMode::AzureSpM2m);
        o.azure_client_id = Some("entra-app-id".to_owned());
        o.azure_client_secret = Some("entra-secret".to_owned());
        match build_auth_config(&o).unwrap() {
            AuthConfig::AzureServicePrincipal {
                azure_tenant_id,
                azure_workspace_resource_id,
                ..
            } => {
                assert!(azure_tenant_id.is_none());
                assert!(azure_workspace_resource_id.is_none());
            }
            other => panic!("expected AzureServicePrincipal, got {other:?}"),
        }
    }

    #[test]
    fn azure_sp_m2m_requires_client_id_and_secret() {
        // The kernel ctor rejects a missing/empty client id or secret; the
        // binding surfaces that as an InvalidArgument envelope.
        let mut o = base();
        o.auth_mode = Some(AuthMode::AzureSpM2m);
        o.azure_client_secret = Some("entra-secret".to_owned());
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "azure_client_id");

        let mut o = base();
        o.auth_mode = Some(AuthMode::AzureSpM2m);
        o.azure_client_id = Some("entra-app-id".to_owned());
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "azure_client_secret");
    }

    #[test]
    fn pat_rejects_empty_token() {
        let mut o = base();
        o.auth_mode = Some(AuthMode::Pat);
        o.token = Some(String::new());
        assert_invalid_arg(
            build_auth_config(&o).unwrap_err(),
            "requires a non-empty access token",
        );
    }

    #[test]
    fn federation_client_id_reaches_auth_config() {
        let mut o = base();
        o.token = Some("dapi-xxx".to_owned());
        o.identity_federation_client_id = Some("federation-client".to_owned());

        match build_auth_config(&o).unwrap() {
            AuthConfig::Pat {
                identity_federation_client_id,
                ..
            } => assert_eq!(
                identity_federation_client_id.as_deref(),
                Some("federation-client")
            ),
            other => panic!("expected Pat, got {other:?}"),
        }
    }

    #[test]
    fn token_url_forwards_as_override() {
        // `tokenUrl` must reach the built AuthConfig as
        // `token_url_override` for M2M, M2M-JWT, and U2M, so a Node caller can point
        // the token exchange at an Azure/Entra endpoint. Omitting it leaves the
        // override `None` (kernel OIDC discovery).
        let endpoint = "https://login.microsoftonline.com/tenant/oauth2/v2.0/token";

        let mut m2m = base();
        m2m.auth_mode = Some(AuthMode::OAuthM2m);
        m2m.oauth_client_id = Some("cid".to_owned());
        m2m.oauth_client_secret = Some("sec".to_owned());
        m2m.token_url = Some(endpoint.to_owned());
        match build_auth_config(&m2m).unwrap() {
            AuthConfig::OAuthM2M {
                token_url_override, ..
            } => assert_eq!(token_url_override.as_deref(), Some(endpoint)),
            other => panic!("expected OAuthM2M, got {other:?}"),
        }

        let mut u2m = base();
        u2m.auth_mode = Some(AuthMode::OAuthU2m);
        u2m.token_url = Some(endpoint.to_owned());
        match build_auth_config(&u2m).unwrap() {
            AuthConfig::OAuthU2M {
                token_url_override, ..
            } => assert_eq!(token_url_override.as_deref(), Some(endpoint)),
            other => panic!("expected OAuthU2M, got {other:?}"),
        }

        let mut jwt = base();
        jwt.auth_mode = Some(AuthMode::OAuthM2mJwt);
        jwt.oauth_client_id = Some("cid".to_owned());
        jwt.jwt_key_file = Some("/k.pem".to_owned());
        jwt.jwt_kid = Some("kid".to_owned());
        jwt.token_url = Some(endpoint.to_owned());
        match build_auth_config(&jwt).unwrap() {
            AuthConfig::OAuthM2MPrivateKey {
                token_url_override, ..
            } => assert_eq!(token_url_override.as_deref(), Some(endpoint)),
            other => panic!("expected OAuthM2MPrivateKey, got {other:?}"),
        }

        // Omitted → None (default OIDC discovery).
        let mut none = base();
        none.auth_mode = Some(AuthMode::OAuthM2m);
        none.oauth_client_id = Some("cid".to_owned());
        none.oauth_client_secret = Some("sec".to_owned());
        match build_auth_config(&none).unwrap() {
            AuthConfig::OAuthM2M {
                token_url_override, ..
            } => assert!(token_url_override.is_none()),
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    #[test]
    fn m2m_requires_client_id_and_secret() {
        let mut o = base();
        o.auth_mode = Some(AuthMode::OAuthM2m);
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "client_id");
        o.oauth_client_id = Some("cid".to_owned());
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "client_secret");
        o.oauth_client_secret = Some("sec".to_owned());
        o.oauth_scopes = Some(vec!["all-apis".to_owned()]);
        o.identity_federation_client_id = Some("federation-client".to_owned());
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthM2M {
                client_id,
                client_secret,
                scopes,
                identity_federation_client_id,
                ..
            } => {
                assert_eq!(client_id, "cid");
                assert_eq!(client_secret, "sec");
                assert_eq!(scopes, Some(vec!["all-apis".to_owned()]));
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client")
                );
            }
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    #[test]
    fn m2m_jwt_requires_client_id_key_file_and_kid() {
        let mut o = base();
        o.auth_mode = Some(AuthMode::OAuthM2mJwt);
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "client_id");
        o.oauth_client_id = Some("cid".to_owned());
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "jwt_key_file");
        o.jwt_key_file = Some("/k.pem".to_owned());
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "jwt_kid");
        o.jwt_kid = Some("kid".to_owned());
        o.jwt_passphrase = Some("pass".to_owned());
        o.jwt_algorithm = Some("ES256".to_owned());
        o.oauth_scopes = Some(vec!["all-apis".to_owned()]);
        let token_url = "https://idp.example/token";
        o.token_url = Some(token_url.to_owned());
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthM2MPrivateKey {
                client_id,
                jwt_key_file,
                jwt_kid,
                jwt_passphrase,
                jwt_algorithm,
                scopes,
                token_url_override,
                ..
            } => {
                assert_eq!(client_id, "cid");
                assert_eq!(jwt_key_file, "/k.pem");
                assert_eq!(jwt_kid, "kid");
                assert_eq!(jwt_passphrase.as_deref(), Some("pass"));
                assert_eq!(jwt_algorithm.as_deref(), Some("ES256"));
                assert_eq!(scopes, Some(vec!["all-apis".to_owned()]));
                assert_eq!(token_url_override.as_deref(), Some(token_url));
            }
            other => panic!("expected OAuthM2MPrivateKey, got {other:?}"),
        }
    }

    #[test]
    fn u2m_fields_are_all_optional() {
        let mut o = base();
        o.auth_mode = Some(AuthMode::OAuthU2m);
        // Bare U2M: napi injects no override, so client_id / scopes /
        // redirect_port are all None and the kernel fills its default bundle
        // (databricks-sql-connector / sql offline_access / 8030). See
        // PECOBLR-4039.
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthU2M {
                client_id,
                redirect_ports,
                scopes,
                ..
            } => {
                assert!(client_id.is_none());
                assert!(redirect_ports.is_none());
                assert!(scopes.is_none());
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
        // Explicit overrides thread through. Node's single redirect port is
        // forwarded as a one-element candidate list.
        o.oauth_client_id = Some("custom-cli".to_owned());
        o.oauth_redirect_port = Some(9000);
        o.identity_federation_client_id = Some("federation-client".to_owned());
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthU2M {
                client_id,
                redirect_ports,
                identity_federation_client_id,
                ..
            } => {
                assert_eq!(client_id.as_deref(), Some("custom-cli"));
                assert_eq!(redirect_ports, Some(vec![9000]));
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client")
                );
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_token_cache_control_maps_to_config() {
        let mut o = base();
        o.auth_mode = Some(AuthMode::OAuthU2m);

        // Neither field set → None → the kernel default (enabled, machine key).
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthU2M { token_cache, .. } => assert!(token_cache.is_none()),
            other => panic!("expected OAuthU2M, got {other:?}"),
        }

        // Explicit disable → Some(TokenCacheConfig { enabled: false }).
        o.token_cache_enabled = Some(false);
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthU2M {
                token_cache: Some(tc),
                ..
            } => {
                assert!(!tc.enabled);
                assert!(tc.passphrase.is_none());
            }
            other => panic!("expected OAuthU2M with token_cache, got {other:?}"),
        }

        // Passphrase with no explicit enable flag implies enabled.
        o.token_cache_enabled = None;
        o.token_cache_passphrase = Some("s3cret".to_owned());
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthU2M {
                token_cache: Some(tc),
                ..
            } => {
                assert!(tc.enabled);
                assert_eq!(tc.passphrase.as_deref(), Some("s3cret"));
            }
            other => panic!("expected OAuthU2M with token_cache, got {other:?}"),
        }
    }

    #[test]
    fn u2m_rejects_explicit_empty_client_id() {
        // Parity with pyo3: an explicit empty oauthClientId is rejected
        // (it would bind an empty client and surface an opaque server
        // error), rather than silently falling back to the default.
        let mut o = base();
        o.auth_mode = Some(AuthMode::OAuthU2m);
        o.oauth_client_id = Some(String::new());
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "oauthClientId");
    }

    #[test]
    fn tls_defaults_to_secure() {
        // No `check_server_certificate` ⇒ secure by default: strict chain +
        // hostname validation (the kernel's TlsConfig::default()). A caller
        // must opt out explicitly with `check_server_certificate: false`.
        let tls = build_tls_config(&base());
        assert!(
            !tls.accept_self_signed,
            "self-signed must be rejected by default"
        );
        assert!(
            !tls.skip_hostname_verification,
            "hostname check must run by default"
        );
        assert!(tls.custom_ca_cert.is_none());
    }

    #[test]
    fn tls_strict_when_check_enabled() {
        let mut o = base();
        o.check_server_certificate = Some(true);
        let tls = build_tls_config(&o);
        assert!(!tls.accept_self_signed);
        assert!(!tls.skip_hostname_verification);
        assert!(tls.custom_ca_cert.is_none());
    }

    #[test]
    fn tls_explicit_false_is_permissive() {
        let mut o = base();
        o.check_server_certificate = Some(false);
        let tls = build_tls_config(&o);
        assert!(tls.accept_self_signed);
        assert!(tls.skip_hostname_verification);
    }

    #[test]
    fn tls_hostname_check_can_be_disabled_independently() {
        // Chain validation stays ON; only the hostname check is skipped.
        // Mirrors the Python connector's `tls_verify_hostname=False`.
        let mut o = base();
        o.check_server_certificate_hostname = Some(false);
        let tls = build_tls_config(&o);
        assert!(!tls.accept_self_signed, "chain validation must remain on");
        assert!(
            tls.skip_hostname_verification,
            "hostname check must be skipped"
        );
    }

    #[test]
    fn tls_strict_master_explicit_with_hostname_off_and_custom_ca() {
        // Headline security-matrix cell, both flags explicit: verify the
        // chain (master = Some(true)) but skip the hostname check
        // (hostname = Some(false)), pairing with a custom CA — the
        // realistic "connect to an IP behind a corporate CA" case.
        const PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
        let mut o = base();
        o.check_server_certificate = Some(true);
        o.check_server_certificate_hostname = Some(false);
        o.custom_ca_cert = Some(PEM.to_vec());
        let tls = build_tls_config(&o);
        assert!(
            !tls.accept_self_signed,
            "chain validation must remain on (master = true)"
        );
        assert!(
            tls.skip_hostname_verification,
            "hostname check must be skipped (hostname = false)"
        );
        assert_eq!(tls.custom_ca_cert.as_deref(), Some(PEM));
    }

    #[test]
    fn tls_hostname_check_on_by_default_and_when_explicit_true() {
        for hostname in [None, Some(true)] {
            let mut o = base();
            o.check_server_certificate_hostname = hostname;
            let tls = build_tls_config(&o);
            assert!(!tls.skip_hostname_verification);
            assert!(!tls.accept_self_signed);
        }
    }

    #[test]
    fn tls_master_off_subsumes_hostname_toggle() {
        // `check_server_certificate=false` disables everything regardless
        // of the hostname flag (matches Python `tls_verify=False`).
        for hostname in [None, Some(true), Some(false)] {
            let mut o = base();
            o.check_server_certificate = Some(false);
            o.check_server_certificate_hostname = hostname;
            let tls = build_tls_config(&o);
            assert!(tls.accept_self_signed);
            assert!(tls.skip_hostname_verification);
        }
    }

    #[test]
    fn tls_custom_ca_threads_through_regardless_of_strictness() {
        const PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
        // Custom CA is honoured in both strict and permissive modes.
        for strict in [None, Some(true), Some(false)] {
            let mut o = base();
            o.check_server_certificate = strict;
            o.custom_ca_cert = Some(PEM.to_vec());
            let tls = build_tls_config(&o);
            assert_eq!(tls.custom_ca_cert.as_deref(), Some(PEM));
        }
    }

    #[test]
    fn tls_no_mtls_by_default() {
        let tls = build_tls_config(&base());
        assert!(tls.client_cert_pem.is_none());
        assert!(tls.client_key_pem.is_none());
    }

    #[test]
    fn tls_mtls_cert_and_key_thread_through() {
        const CERT: &[u8] = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
        // Opaque placeholder bytes — `build_tls_config` only threads the key
        // through, never parses it (the kernel validates the real PEM at
        // `open`), so the fixture deliberately avoids a real key header.
        const KEY: &[u8] = b"<client-key-pem-placeholder>";
        // Honoured regardless of the cert-verification toggle: mTLS is the
        // *client* identity, orthogonal to *server*-cert strictness.
        for strict in [None, Some(true), Some(false)] {
            let mut o = base();
            o.check_server_certificate = strict;
            o.client_cert_pem = Some(CERT.to_vec());
            o.client_key_pem = Some(KEY.to_vec());
            let tls = build_tls_config(&o);
            assert_eq!(tls.client_cert_pem.as_deref(), Some(CERT));
            assert_eq!(tls.client_key_pem.as_deref(), Some(KEY));
        }
    }

    #[test]
    fn tls_partial_mtls_threads_through_for_kernel_to_reject() {
        // The binding does NOT enforce the both-or-neither rule — it
        // forwards whatever is set so the kernel can raise the canonical
        // `InvalidArgument` at `open`. Assert each half threads through
        // independently (so a caller setting only one isn't silently
        // dropped before the kernel can complain).
        const CERT: &[u8] = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
        let mut cert_only = base();
        cert_only.client_cert_pem = Some(CERT.to_vec());
        let tls = build_tls_config(&cert_only);
        assert_eq!(tls.client_cert_pem.as_deref(), Some(CERT));
        assert!(tls.client_key_pem.is_none());

        let mut key_only = base();
        key_only.client_key_pem = Some(CERT.to_vec());
        let tls = build_tls_config(&key_only);
        assert!(tls.client_cert_pem.is_none());
        assert_eq!(tls.client_key_pem.as_deref(), Some(CERT));
    }

    #[test]
    fn http_defaults_pool_and_no_headers() {
        // Pin the napi-owned default value itself, so a drift in the
        // constant (or someone "simplifying" it to inherit the kernel's)
        // fails here. NOTE: this assertion can't catch deletion of the
        // override line in `build_http_config` on its own, because the
        // kernel `HttpConfig::default()` is *also* 100 today — the
        // discriminating test for the override is
        // `http_max_connections_overrides_pool` (sets a value the kernel
        // default is not). This test guards the no-`maxConnections` path
        // and the constant's value.
        assert_eq!(
            NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST, 100,
            "napi pins its own user-facing pool default"
        );
        let mut o = base();
        let http = build_http_config(&mut o);
        assert_eq!(
            http.pool_max_idle_per_host,
            NAPI_DEFAULT_POOL_MAX_IDLE_PER_HOST
        );
        assert!(http.custom_headers.is_empty());
    }

    #[test]
    fn http_max_connections_overrides_pool() {
        let mut o = base();
        o.max_connections = Some(7);
        let http = build_http_config(&mut o);
        assert_eq!(http.pool_max_idle_per_host, 7);
    }

    fn header(name: &str, value: &str) -> HeaderEntry {
        HeaderEntry {
            name: name.to_owned(),
            value: value.to_owned(),
        }
    }

    #[test]
    fn http_custom_headers_thread_through_in_order() {
        let mut o = base();
        // Ordered list (mirrors pyo3 / kernel core) — order and duplicates
        // are preserved verbatim, so assert the exact sequence.
        o.custom_headers = Some(vec![
            header("X-Trace", "abc123"),
            header("X-Env", "prod"),
            header("User-Agent", "MyApp/1.0"),
        ]);
        let http = build_http_config(&mut o);
        assert_eq!(
            http.custom_headers,
            vec![
                ("X-Trace".to_owned(), "abc123".to_owned()),
                ("X-Env".to_owned(), "prod".to_owned()),
                ("User-Agent".to_owned(), "MyApp/1.0".to_owned()),
            ]
        );
        // The list was moved out of `options` (taken), not cloned.
        assert!(o.custom_headers.is_none());
    }

    #[test]
    fn http_custom_headers_preserve_duplicate_names() {
        // Duplicate header names survive (a `HashMap` would have collapsed
        // them) — the kernel emits each and folds the last User-Agent.
        let mut o = base();
        o.custom_headers = Some(vec![
            header("User-Agent", "Caller/1.0"),
            header("User-Agent", "Connector/2.0"),
        ]);
        let http = build_http_config(&mut o);
        assert_eq!(http.custom_headers.len(), 2);
        assert_eq!(http.custom_headers[0].1, "Caller/1.0");
        assert_eq!(http.custom_headers[1].1, "Connector/2.0");
    }

    #[test]
    fn sea_response_compression_option_maps_to_http_config() {
        let mut defaults = base();
        assert!(!build_http_config(&mut defaults).sea_response_compression_enabled);

        let mut enabled = base();
        enabled.sea_response_compression_enabled = Some(true);
        assert!(build_http_config(&mut enabled).sea_response_compression_enabled);
    }

    #[test]
    fn http_retry_knobs_map_onto_http_config() {
        let mut o = base();
        o.retry_min_wait_secs = Some(2);
        o.retry_max_wait_secs = Some(45);
        o.retry_max_attempts = Some(4); // total attempts → 3 retries
        o.retry_overall_timeout_secs = Some(120);
        let http = build_http_config(&mut o);
        assert_eq!(http.retry_min_wait, std::time::Duration::from_secs(2));
        assert_eq!(http.retry_max_wait, std::time::Duration::from_secs(45));
        // total attempts (4) → retries after the first (3): the off-by-one.
        assert_eq!(http.retry_max_retries, 3);
        assert_eq!(http.overall_timeout, std::time::Duration::from_secs(120));
    }

    #[test]
    fn http_retry_attempts_zero_or_one_means_no_retries() {
        for attempts in [0u32, 1] {
            let mut o = base();
            o.retry_max_attempts = Some(attempts);
            let http = build_http_config(&mut o);
            assert_eq!(http.retry_max_retries, 0, "attempts={attempts}");
        }
    }

    #[test]
    fn http_retry_unset_keeps_kernel_defaults() {
        let mut o = base();
        let http = build_http_config(&mut o);
        let def = HttpConfig::default();
        assert_eq!(http.retry_min_wait, def.retry_min_wait);
        assert_eq!(http.retry_max_wait, def.retry_max_wait);
        assert_eq!(http.retry_max_retries, def.retry_max_retries);
        assert_eq!(http.overall_timeout, def.overall_timeout);
    }

    #[test]
    fn telemetry_options_map_onto_kernel_config() {
        let mut o = base();
        o.telemetry_enabled = Some(true);
        o.telemetry_batch_size = Some(25);
        o.telemetry_flush_interval_ms = Some(1_000);
        o.telemetry_max_retries = Some(2);
        o.telemetry_retry_delay_ms = Some(50);
        o.telemetry_close_flush_timeout_ms = Some(2_500);
        o.telemetry_circuit_breaker_enabled = Some(false);
        o.telemetry_circuit_breaker_threshold = Some(9);
        o.telemetry_circuit_breaker_timeout_ms = Some(60_000);

        let telemetry = build_telemetry_config(&o)
            .expect("valid telemetry config")
            .expect("telemetry mentioned");

        assert!(telemetry.enabled);
        assert_eq!(telemetry.batch_size, 25);
        assert_eq!(
            telemetry.flush_interval,
            std::time::Duration::from_millis(1_000)
        );
        assert_eq!(telemetry.max_retries, 2);
        assert_eq!(telemetry.retry_delay, std::time::Duration::from_millis(50));
        assert_eq!(
            telemetry.close_flush_timeout,
            std::time::Duration::from_millis(2_500)
        );
        assert!(!telemetry.circuit_breaker_enabled);
        assert_eq!(telemetry.circuit_breaker_threshold, 9);
        assert_eq!(
            telemetry.circuit_breaker_timeout,
            std::time::Duration::from_millis(60_000)
        );
    }

    #[test]
    fn telemetry_options_unmentioned_returns_none() {
        assert!(build_telemetry_config(&base()).unwrap().is_none());
    }

    #[test]
    fn telemetry_options_reject_zero_required_fields() {
        let mut o = base();
        o.telemetry_batch_size = Some(0);
        assert_invalid_arg(
            build_telemetry_config(&o).unwrap_err(),
            "telemetryBatchSize must be > 0",
        );

        let mut o = base();
        o.telemetry_flush_interval_ms = Some(0);
        assert_invalid_arg(
            build_telemetry_config(&o).unwrap_err(),
            "telemetryFlushIntervalMs must be > 0",
        );

        let mut o = base();
        o.telemetry_close_flush_timeout_ms = Some(0);
        assert_invalid_arg(
            build_telemetry_config(&o).unwrap_err(),
            "telemetryCloseFlushTimeoutMs must be > 0",
        );

        let mut o = base();
        o.telemetry_circuit_breaker_threshold = Some(0);
        assert_invalid_arg(
            build_telemetry_config(&o).unwrap_err(),
            "telemetryCircuitBreakerThreshold must be > 0 when enabled",
        );

        let mut o = base();
        o.telemetry_circuit_breaker_timeout_ms = Some(0);
        assert_invalid_arg(
            build_telemetry_config(&o).unwrap_err(),
            "telemetryCircuitBreakerTimeoutMs must be > 0 when enabled",
        );

        let mut o = base();
        o.telemetry_circuit_breaker_enabled = Some(false);
        o.telemetry_circuit_breaker_threshold = Some(0);
        assert!(build_telemetry_config(&o).is_ok());

        // A disabled breaker ignores the timeout knob too, so a zero timeout
        // is accepted rather than rejected.
        let mut o = base();
        o.telemetry_circuit_breaker_enabled = Some(false);
        o.telemetry_circuit_breaker_timeout_ms = Some(0);
        assert!(build_telemetry_config(&o).is_ok());
    }

    #[test]
    fn driver_system_options_map_onto_kernel_config() {
        assert!(build_driver_system_configuration(&base()).is_none());

        let mut o = base();
        o.driver_name = Some("NodeSQL".to_owned());
        o.driver_version = Some("1.2.3".to_owned());
        o.runtime_name = Some("node".to_owned());
        o.runtime_version = Some("22.0.0".to_owned());
        o.runtime_vendor = Some("Node.js".to_owned());
        o.os_name = Some("Linux".to_owned());
        o.os_version = Some("6.1".to_owned());
        o.os_arch = Some("x86_64".to_owned());
        o.client_app_name = Some("app".to_owned());
        o.locale_name = Some("en_US".to_owned());
        o.char_set_encoding = Some("UTF-8".to_owned());
        o.process_name = Some("node".to_owned());

        let system_configuration =
            build_driver_system_configuration(&o).expect("driver system configuration mentioned");

        assert_eq!(system_configuration.driver_name.as_deref(), Some("NodeSQL"));
        assert_eq!(
            system_configuration.driver_version.as_deref(),
            Some("1.2.3")
        );
        assert_eq!(system_configuration.runtime_name.as_deref(), Some("node"));
        assert_eq!(
            system_configuration.runtime_version.as_deref(),
            Some("22.0.0")
        );
        assert_eq!(
            system_configuration.runtime_vendor.as_deref(),
            Some("Node.js")
        );
        assert_eq!(system_configuration.os_name.as_deref(), Some("Linux"));
        assert_eq!(system_configuration.os_version.as_deref(), Some("6.1"));
        assert_eq!(system_configuration.os_arch.as_deref(), Some("x86_64"));
        assert_eq!(system_configuration.client_app_name.as_deref(), Some("app"));
        assert_eq!(system_configuration.locale_name.as_deref(), Some("en_US"));
        assert_eq!(
            system_configuration.char_set_encoding.as_deref(),
            Some("UTF-8")
        );
        assert_eq!(system_configuration.process_name.as_deref(), Some("node"));
    }
}
