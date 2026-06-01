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
    AuthConfig, DefaultOpts, HttpConfig, ResultConfig, Session, TlsConfig,
};

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

/// Authentication mode selector crossing the napi boundary. The string
/// literals are what napi-rs emits from this `#[napi(string_enum)]` — the
/// NodeJS SEA adapter (`SeaAuth`) matches them verbatim (`'Pat'`,
/// `'OAuthM2m'`, `'OAuthU2m'`).
///
/// Mirrors the kernel [`AuthConfig`] variants this binding supports.
/// `OAuthFederation` / `External` are intentionally not exposed yet — the
/// kernel marks federation as not-yet-implemented and `External` is a
/// Rust-trait escape hatch with no JS-callback bridge.
#[napi(string_enum)]
#[derive(Default)]
pub enum AuthMode {
    /// Personal access token (`token`).
    #[default]
    Pat,
    /// OAuth 2.0 machine-to-machine — `oauthClientId` + `oauthClientSecret`.
    OAuthM2m,
    /// OAuth 2.0 user-to-machine (browser flow) — optional `oauthClientId`
    /// + `oauthRedirectPort`.
    OAuthU2m,
}

/// JS-visible options for opening a Databricks SQL session.
///
/// Authentication is selected by `authMode` (default [`AuthMode::Pat`]):
/// - `Pat` — `token` required.
/// - `OAuthM2m` — `oauthClientId` + `oauthClientSecret` required.
/// - `OAuthU2m` — `oauthClientId` / `oauthRedirectPort` optional (kernel
///   defaults to the `databricks-cli` client on port 8020).
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
    /// [`AuthMode::OAuthU2m`] (kernel defaults to `databricks-cli`).
    pub oauth_client_id: Option<String>,
    /// OAuth client secret. Required for [`AuthMode::OAuthM2m`].
    pub oauth_client_secret: Option<String>,
    /// Localhost callback port for the [`AuthMode::OAuthU2m`] browser
    /// flow. Omitted ⇒ kernel default (8020).
    pub oauth_redirect_port: Option<u16>,
    /// OAuth scopes override (M2M / U2M). Omitted ⇒ kernel defaults
    /// (`["all-apis"]` for M2M; `["all-apis", "offline_access"]` for U2M).
    pub oauth_scopes: Option<Vec<String>>,
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
    /// Omitted / `false` ⇒ the driver matches the legacy NodeJS Thrift
    /// driver's permissive behaviour (`rejectUnauthorized: false`): it
    /// accepts self-signed / untrusted / expired certs AND skips the
    /// hostname-vs-SNI check. This is **insecure** (no protection
    /// against active MITM) but is the historical NodeJS-driver default
    /// since 2020, so the SEA backend matches it for drop-in parity.
    ///
    /// `true` ⇒ strict validation against the system / Mozilla trust
    /// store (full chain + expiry + hostname), matching JDBC / ODBC and
    /// every modern HTTPS client. Recommended.
    ///
    /// Maps onto the kernel [`TlsConfig::accept_self_signed`] +
    /// [`TlsConfig::skip_hostname_verification`] (both = `!check`).
    pub check_server_certificate: Option<bool>,
    /// PEM-encoded CA certificate bytes to add to the trust store on
    /// top of the system roots. Use for corporate TLS-inspecting
    /// proxies that re-sign TLS, or on-prem deployments with an
    /// internal CA. Honoured regardless of `check_server_certificate`.
    /// Maps onto the kernel [`TlsConfig::custom_ca_cert`].
    pub custom_ca_cert: Option<Buffer>,
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
        AuthMode::Pat => AuthConfig::pat(options.token.clone()).map_err(napi_err_from_kernel),
        AuthMode::OAuthM2m => AuthConfig::oauth_m2m(
            options.oauth_client_id.clone(),
            options.oauth_client_secret.clone(),
            options.oauth_scopes.clone(),
            None,
        )
        .map_err(napi_err_from_kernel),
        // All U2M fields are optional — the kernel fills `databricks-cli`
        // / port 8020 / default scopes when omitted.
        AuthMode::OAuthU2m => Ok(AuthConfig::oauth_u2m(
            options.oauth_client_id.clone(),
            options.oauth_redirect_port,
            options.oauth_scopes.clone(),
            None,
            None,
        )),
    }
}

/// Map the JS TLS fields onto the kernel [`TlsConfig`].
///
/// The default (no `check_server_certificate`) is **permissive** to
/// match the legacy NodeJS Thrift driver (`rejectUnauthorized: false`):
/// `accept_self_signed` + `skip_hostname_verification` are both set,
/// disabling chain and hostname validation. Opting into
/// `check_server_certificate: true` restores the kernel's strict
/// defaults. A `custom_ca_cert` is always added to the trust store
/// regardless of the strictness toggle.
///
/// `TlsConfig` is `#[non_exhaustive]` — construct via `default()` then
/// mutate (same pattern as `ResultConfig` / `HttpConfig` above). Pure
/// (borrows `options`) so it is unit-testable without opening a session.
fn build_tls_config(options: &ConnectionOptions) -> TlsConfig {
    let strict = options.check_server_certificate.unwrap_or(false);
    let mut tls = TlsConfig::default();
    // Permissive-by-default: when the caller hasn't opted into strict
    // checking, relax both knobs so behaviour matches Thrift's
    // always-on `rejectUnauthorized: false`.
    tls.accept_self_signed = !strict;
    tls.skip_hostname_verification = !strict;
    if let Some(ca) = &options.custom_ca_cert {
        tls.custom_ca_cert = Some(ca.to_vec());
    }
    tls
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
pub async fn open_session(options: ConnectionOptions) -> napi::Result<Connection> {
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
        builder = builder.tls(tls_config);
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
            catalog: None,
            schema: None,
            session_conf: None,
            max_connections: None,
            intervals_as_string: None,
            complex_types_as_json: None,
            check_server_certificate: None,
            custom_ca_cert: None,
        }
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
    fn m2m_requires_client_id_and_secret() {
        let mut o = base();
        o.auth_mode = Some(AuthMode::OAuthM2m);
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "client_id");
        o.oauth_client_id = Some("cid".to_owned());
        assert_invalid_arg(build_auth_config(&o).unwrap_err(), "client_secret");
        o.oauth_client_secret = Some("sec".to_owned());
        o.oauth_scopes = Some(vec!["all-apis".to_owned()]);
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthM2M {
                client_id,
                client_secret,
                scopes,
                ..
            } => {
                assert_eq!(client_id, "cid");
                assert_eq!(client_secret, "sec");
                assert_eq!(scopes, Some(vec!["all-apis".to_owned()]));
            }
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_fields_are_all_optional() {
        let mut o = base();
        o.auth_mode = Some(AuthMode::OAuthU2m);
        // Bare U2M: kernel fills databricks-cli / port 8020 / default scopes.
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthU2M {
                client_id,
                redirect_port,
                ..
            } => {
                assert!(client_id.is_none());
                assert!(redirect_port.is_none());
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
        // Explicit overrides thread through.
        o.oauth_client_id = Some("custom-cli".to_owned());
        o.oauth_redirect_port = Some(9000);
        match build_auth_config(&o).unwrap() {
            AuthConfig::OAuthU2M {
                client_id,
                redirect_port,
                ..
            } => {
                assert_eq!(client_id.as_deref(), Some("custom-cli"));
                assert_eq!(redirect_port, Some(9000));
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn tls_defaults_to_permissive_matching_thrift() {
        // No `check_server_certificate` ⇒ relax both knobs so behaviour
        // matches the legacy Thrift driver's `rejectUnauthorized: false`.
        let tls = build_tls_config(&base());
        assert!(tls.accept_self_signed, "self-signed should be accepted");
        assert!(
            tls.skip_hostname_verification,
            "hostname check should be skipped"
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
}
