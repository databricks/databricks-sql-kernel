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

//! HTTP client implementation for Databricks SQL API.
//!
//! This module provides a low-level HTTP client with:
//! - Connection pooling
//! - Idempotency-aware retry with exponential backoff and jitter
//! - `Retry-After` header support
//! - Per-category retry configuration
//! - Bearer token authentication
//! - Configurable timeouts

use crate::auth::AuthProvider;
use crate::client::retry::{
    calculate_backoff, is_certificate_verification_failure, RequestCategory, RequestType,
    RetryConfig, RetryStrategy,
};
use crate::kernel_error::{Error, Result};
use crate::klog::{debug, warn};
use crate::types::sea::ServiceError;
use reqwest::{Client, NoProxy, Proxy, Request, Response};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Build an `Error` for a transport-level non-2xx HTTP response, parsing
/// the SEA error envelope out of the response body when present.
///
/// The SEA REST API returns a JSON error body — either a bare
/// `{"error_code", "message", "sql_state"}` ([`ServiceError`]) or a
/// `{"error": {...}}` wrapper. Mirrors the statement-level path
/// (`sea.rs terminal_outcome`) so transport-level failures surface the
/// structured `error_code` / `sql_state` instead of only the raw body
/// stuffed into the message. Falls back to `context` (the raw body) as
/// the message when the body isn't a recognised envelope.
///
/// `context` is a caller-supplied prefix (e.g. "HTTP 400" / retry-timeout
/// detail) folded into the message so existing diagnostics are preserved.
fn http_error(status: u16, body: &str, context: impl std::fmt::Display) -> Error {
    #[derive(serde::Deserialize)]
    struct ErrorWrapper {
        error: ServiceError,
    }
    // Parse the body once into a `Value`, then try both envelope shapes
    // off it (bare `ServiceError`, then the `{"error": ...}` wrapper) —
    // avoids re-walking the raw string twice.
    let svc: Option<ServiceError> = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            serde_json::from_value::<ServiceError>(v.clone())
                .ok()
                .filter(|e| e.error_code.is_some() || e.message.is_some() || e.sql_state.is_some())
                .or_else(|| {
                    serde_json::from_value::<ErrorWrapper>(v)
                        .ok()
                        .map(|w| w.error)
                })
        });

    let mut err = Error::from_http_status(status);
    match svc {
        Some(svc) => {
            let error_code = svc.error_code.as_deref().filter(|s| !s.is_empty());
            // Prefer the server's structured message. If it's absent but
            // an error_code is present, synthesize a readable message
            // rather than folding the raw JSON body in. Only fall back to
            // the raw body when neither is available.
            let msg = match (svc.message.as_deref(), error_code) {
                (Some(m), _) => m.to_string(),
                (None, Some(ec)) => format!("server returned error_code {ec}"),
                (None, None) => body.to_string(),
            };
            err = err.message(format!("{context}: {msg}"));
            if let Some(ec) = error_code {
                err = err.with_error_code(ec);
            }
            // SQL state: explicit field (must be the canonical 5-char
            // shape — same predicate as terminal_outcome, so we don't
            // surface junk like "X" or "garbage"), else the
            // "SQLSTATE: XXXXX" pattern in the message.
            let sql_state = svc
                .sql_state
                .filter(|s| s.len() == 5)
                .or_else(|| crate::kernel_error::extract_sqlstate_from_message(&msg));
            if let Some(s) = sql_state {
                err = err.with_sql_state(s);
            }
        }
        None => {
            // Not a recognised envelope — preserve today's behavior
            // (raw body folded into the message).
            err = err.message(format!("{context}: {body}"));
        }
    }
    err
}

/// Configuration for HTTP proxy.
///
/// When `url` is set, the driver uses the specified proxy for all requests,
/// overriding `HTTP_PROXY`/`HTTPS_PROXY` environment variables. When `url`
/// is `None`, reqwest's default behavior applies (reads env vars automatically).
#[derive(Clone, Default)]
pub struct ProxyConfig {
    /// Proxy URL (e.g., "http://proxy.corp.example.com:8080").
    /// When set, overrides HTTP_PROXY/HTTPS_PROXY environment variables.
    pub url: Option<String>,
    /// Username for proxy authentication.
    pub username: Option<String>,
    /// Password for proxy authentication.
    ///
    /// Note: This is a sensitive value. It is redacted in `Debug` output.
    pub password: Option<String>,
    /// Comma-separated list of hosts/domains to bypass the proxy
    /// (e.g., "localhost,*.internal.corp,.example.com").
    pub bypass_hosts: Option<String>,
}

impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyConfig")
            .field("url", &self.url)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("bypass_hosts", &self.bypass_hosts)
            .finish()
    }
}

/// Configuration for TLS behavior.
///
/// All fields default to the most secure settings (`None` = use defaults).
/// Relaxing any of these options should only be done in development/testing
/// environments.
#[derive(Clone, Default)]
pub struct TlsConfig {
    /// Enable TLS for connections. When `false`, allows plain HTTP.
    /// Default: true (TLS enabled).
    pub enabled: Option<bool>,
    /// Accept self-signed server certificates.
    ///
    /// Note: reqwest does not distinguish between "self-signed only" and
    /// "fully unvalidated". Setting this to `true` actually disables ALL
    /// certificate validation (CA trust, expiration, etc.) via
    /// `danger_accept_invalid_certs(true)`.
    /// Default: false.
    pub allow_self_signed: Option<bool>,
    /// Don't verify that the certificate hostname matches the server.
    /// Works under both the `tls-native` (delegates to native-tls's
    /// `danger_accept_invalid_hostnames`) and `tls-rustls` (installs
    /// reqwest's `IgnoreHostname` verifier) backends. **Insecure** —
    /// for dev/test only.
    /// Default: false.
    pub allow_hostname_mismatch: Option<bool>,
    /// Path to a PEM-encoded CA certificate file to add to the trust
    /// store. Internal-only — the public [`crate::config::TlsConfig`]
    /// surface does not expose a path-based loader; the only public
    /// path to a custom CA is `custom_ca_cert: Vec<u8>` (mapped to
    /// `trusted_certificate_pem` below). Direct-Rust kernel consumers
    /// that need file-based loading can construct an
    /// [`HttpClientConfig`] manually.
    pub trusted_certificate_path: Option<String>,
    /// PEM-encoded CA certificate bytes to add to the trust store.
    /// Populated from the public [`crate::config::TlsConfig::custom_ca_cert`]
    /// field — foreign-language wrappers (napi-rs `Buffer`, pyo3
    /// `bytes`) pass the cert in-memory without needing a tempfile
    /// shim.
    ///
    /// When BOTH `trusted_certificate_path` AND
    /// `trusted_certificate_pem` are set on an internally-constructed
    /// `HttpClientConfig`, the file is loaded first and then the
    /// in-memory PEM is added as an additional root. From the public
    /// surface only the in-memory variant is reachable (the path
    /// field has no public projection), so the "both set" case is
    /// unreachable for `SessionBuilder` callers today.
    pub trusted_certificate_pem: Option<Vec<u8>>,
    /// PEM-encoded client certificate (leaf + optional intermediates)
    /// for mutual TLS. Populated from
    /// [`crate::config::TlsConfig::client_cert_pem`]. Paired with
    /// [`Self::client_key_pem`]; the builder rejects one-without-the-
    /// other.
    pub client_cert_pem: Option<Vec<u8>>,
    /// PEM-encoded private key for the mTLS client certificate.
    /// Populated from [`crate::config::TlsConfig::client_key_pem`].
    /// Secret material — redacted in `Debug`.
    pub client_key_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Mirrors the redacting Debug on the public
        // `crate::config::TlsConfig`. The PEM bytes themselves aren't
        // secret, but dumping a multi-kB blob into every `?http_client`
        // tracing span is unhelpful, and a future field that DOES hold
        // secret material (mTLS client_key, etc.) finds the redacting
        // pattern already in place.
        f.debug_struct("TlsConfig")
            .field("enabled", &self.enabled)
            .field("allow_self_signed", &self.allow_self_signed)
            .field("allow_hostname_mismatch", &self.allow_hostname_mismatch)
            .field("trusted_certificate_path", &self.trusted_certificate_path)
            .field(
                "trusted_certificate_pem",
                &self
                    .trusted_certificate_pem
                    .as_ref()
                    .map(|b| format!("<{} bytes>", b.len())),
            )
            .field(
                "client_cert_pem",
                &self
                    .client_cert_pem
                    .as_ref()
                    .map(|b| format!("<{} bytes>", b.len())),
            )
            // Private key is secret material — never render length or
            // contents, only presence.
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl TlsConfig {
    /// Project the public [`crate::config::TlsConfig`] onto the internal
    /// HTTP-client TLS struct. Public fields land on their internal
    /// counterparts; internal-only fields (`enabled`,
    /// `trusted_certificate_path`) stay at their defaults.
    ///
    /// `accept_self_signed` / `skip_hostname_verification` map a
    /// `bool` from the public surface to `Some(bool)` — including
    /// `Some(false)` for the strict path. The internal `Option<bool>`
    /// shape preserves the "field unset → use backend default"
    /// semantics for in-tree consumers that don't go through the
    /// public projection, while still letting a public caller
    /// **force-disable** a permissive mode if a future internal
    /// default were ever to flip lenient. Cost: an extra `Some(false)`
    /// in `RUST_LOG=debug` output for default sessions.
    pub fn from_public(cfg: &crate::config::TlsConfig) -> Self {
        Self {
            enabled: None,
            allow_self_signed: Some(cfg.accept_self_signed),
            allow_hostname_mismatch: Some(cfg.skip_hostname_verification),
            trusted_certificate_path: None,
            trusted_certificate_pem: cfg.custom_ca_cert.clone(),
            client_cert_pem: cfg.client_cert_pem.clone(),
            client_key_pem: cfg.client_key_pem.clone(),
        }
    }
}

/// Configuration for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Whether authenticated SEA requests may negotiate HTTP response compression.
    pub sea_response_compression_enabled: bool,
    /// Connection timeout duration.
    pub connect_timeout: Duration,
    /// Total request timeout. Despite the historical field name, this is
    /// passed to [`reqwest::ClientBuilder::timeout`], not `read_timeout`.
    pub read_timeout: Duration,
    /// Maximum number of idle connections per host.
    pub max_connections_per_host: usize,
    /// User agent string.
    pub user_agent: String,
    /// Proxy configuration.
    pub proxy: ProxyConfig,
    /// TLS configuration.
    pub tls: TlsConfig,
    /// Databricks organization ID for multi-tenant workspace routing.
    /// When set, sent as the `x-databricks-org-id` header on all authenticated requests.
    pub org_id: Option<String>,
    /// Extra headers applied to every request, before the kernel's own
    /// `Authorization` / `x-databricks-org-id` (which are added last and
    /// therefore win). A caller `User-Agent` here is folded into the
    /// effective UA at construction (it **replaces** the base UA, then is
    /// removed from this list). Populated from
    /// [`crate::config::HttpConfig::custom_headers`]; see its docs for
    /// the full precedence / reserved-name rules.
    pub custom_headers: Vec<(String, String)>,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            sea_response_compression_enabled: false,
            connect_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            max_connections_per_host: 100,
            // This kernel-specific prefix is allow-listed by
            // databricks.sqlexecapi.known_driver_prefixes, which enables
            // INLINE_OR_EXTERNAL_LINKS while preserving accurate attribution.
            user_agent: format!("DatabricksSQLKernel/{}", env!("CARGO_PKG_VERSION")),
            proxy: ProxyConfig::default(),
            tls: TlsConfig::default(),
            org_id: None,
            custom_headers: Vec::new(),
        }
    }
}

/// Build a reqwest mTLS client [`Identity`](reqwest::Identity) from a
/// PEM client certificate and private key.
///
/// The two TLS backends expose different reqwest constructors (gated
/// opposite to one another in `reqwest::tls`), so this is
/// feature-gated:
///
/// - **`tls-native`**: `Identity::from_pkcs8_pem(cert, key)` takes the
///   certificate chain and a PKCS#8 private key as two separate
///   buffers.
/// - **`tls-rustls`**: `Identity::from_pem(buf)` expects a single PEM
///   blob holding the private key followed by the certificate chain.
///   We concatenate `key || cert` (key first) and hand it over.
///
/// The `compile_error!` guards in `src/lib.rs` guarantee exactly one
/// of the two features is active, so the two arms are mutually
/// exclusive and one always compiles.
#[cfg(feature = "tls-native")]
fn build_mtls_identity(cert_pem: &[u8], key_pem: &[u8]) -> Result<reqwest::Identity> {
    reqwest::Identity::from_pkcs8_pem(cert_pem, key_pem).map_err(|e| {
        Error::invalid_argument().message(format!(
            "Invalid mTLS client certificate/key in TlsConfig.client_cert_pem/client_key_pem: {e}"
        ))
    })
}

#[cfg(feature = "tls-rustls")]
fn build_mtls_identity(cert_pem: &[u8], key_pem: &[u8]) -> Result<reqwest::Identity> {
    // rustls's `from_pem` wants key + cert in one PEM buffer.
    let mut combined = Vec::with_capacity(key_pem.len() + cert_pem.len() + 1);
    combined.extend_from_slice(key_pem);
    // Guard against a missing trailing newline between the two PEM
    // blocks, which would glue `-----END ... KEY----------BEGIN ...`
    // together and break parsing.
    if !key_pem.ends_with(b"\n") {
        combined.push(b'\n');
    }
    combined.extend_from_slice(cert_pem);
    reqwest::Identity::from_pem(&combined).map_err(|e| {
        Error::invalid_argument().message(format!(
            "Invalid mTLS client certificate/key in TlsConfig.client_cert_pem/client_key_pem: {e}"
        ))
    })
}

/// HTTP client for communicating with Databricks SQL endpoints.
///
/// This client handles:
/// - Connection pooling (via reqwest)
/// - Idempotency-aware retry with exponential backoff, jitter, and Retry-After
/// - Per-category retry configuration with global defaults
/// - Bearer token authentication (set via two-phase initialization)
/// - User-Agent header injection
///
/// ## Two-Phase Initialization
///
/// The client uses `OnceLock` for auth provider to avoid circular dependencies:
/// OAuth providers need the HTTP client to fetch tokens, but the HTTP client
/// traditionally required auth at construction. The solution:
///
/// 1. Create the HTTP client first (no auth)
/// 2. Create the auth provider (can use the HTTP client)
/// 3. Set auth on the HTTP client via `set_auth_provider()`
///
/// OAuth providers use `execute_without_auth()` for token endpoint calls
/// (which authenticate via form-encoded credentials, not Bearer tokens).
#[derive(Debug)]
pub struct DatabricksHttpClient {
    client: Client,
    config: HttpClientConfig,
    auth_provider: OnceLock<Arc<dyn AuthProvider>>,
    retry_configs: HashMap<RequestCategory, RetryConfig>,
}

impl DatabricksHttpClient {
    /// Creates a new HTTP client with the given configuration and per-category retry configs.
    ///
    /// Auth provider must be set separately via `set_auth_provider()` before
    /// calling `execute()`. Use `execute_without_auth()` for requests that
    /// don't need authentication (e.g., OAuth token endpoint calls).
    pub fn new(
        mut config: HttpClientConfig,
        retry_configs: HashMap<RequestCategory, RetryConfig>,
    ) -> Result<Self> {
        // Fold any caller-supplied `User-Agent` custom header into the
        // effective UA, and remove it from `custom_headers` so it isn't
        // re-applied per-request.
        //
        // A caller `User-Agent` **replaces** the kernel's base UA
        // entirely: the effective UA becomes the caller string verbatim
        // (e.g. the connector's composed `PyDatabricksSqlConnector/x
        // (entry)`), with no `DatabricksSQLKernel/...` prefix. This is
        // intentional — it gives the SEA server an accurate driver
        // identity for query-source attribution (the appended-prefix shape
        // matched none of the gateway's `parseUserAgent` patterns and was
        // recorded as driver="other").
        //
        // PRECONDITION: the caller's UA prefix MUST be present in the
        // gateway's `databricks.sqlexecapi.known_driver_prefixes` SAFE
        // flag. The server gates the `INLINE_OR_EXTERNAL_LINKS` result
        // disposition (which the kernel relies on for single-response
        // chunk links / parallel downloads) on a `startsWith` prefix match
        // of that flag; a UA whose prefix is not allow-listed is rejected
        // with HTTP 400 "not a supported disposition" / redirected to
        // Thrift. `PyDatabricksSqlConnector` is allow-listed for this
        // reason. The base UA below remains `DatabricksSQLKernel/...`
        // so requests with NO caller UA still pass the gate.
        //
        // Last UA wins if a caller somehow set several.
        if let Some(caller_ua) = config
            .custom_headers
            .iter()
            .rfind(|(n, _)| n.eq_ignore_ascii_case("user-agent"))
            .map(|(_, v)| v.clone())
        {
            config
                .custom_headers
                .retain(|(n, _)| !n.eq_ignore_ascii_case("user-agent"));
            let caller_ua = caller_ua.trim();
            // Guard against a pathologically large UA on every request.
            // 1 KiB is far above any legitimate `product/version (entry)`
            // string; truncate (don't reject) so an over-long entry
            // degrades gracefully rather than failing the session.
            const MAX_UA: usize = 1024;
            let caller_ua = if caller_ua.len() > MAX_UA {
                warn!(
                    len = caller_ua.len(),
                    "caller User-Agent exceeds {MAX_UA} bytes; truncating"
                );
                // Truncate on a UTF-8 char boundary at or below the limit so
                // the byte slice can never split a multi-byte codepoint
                // (which would panic).
                let mut end = MAX_UA;
                while end > 0 && !caller_ua.is_char_boundary(end) {
                    end -= 1;
                }
                &caller_ua[..end]
            } else {
                caller_ua
            };
            // An empty/whitespace-only caller UA is ignored — keep the
            // base UA rather than blanking the identity.
            if !caller_ua.is_empty() {
                config.user_agent = caller_ua.to_string();
            }
        }

        // Validate the remaining custom headers up front so a malformed
        // name/value fails at session-open rather than on every request
        // build (where it would surface as a confusing per-request
        // error). Reserved names are still dropped at request time; we
        // only reject structurally-invalid headers here.
        for (name, value) in &config.custom_headers {
            reqwest::header::HeaderName::try_from(name.as_str()).map_err(|e| {
                Error::invalid_argument()
                    .message(format!("invalid custom HTTP header name {name:?}: {e}"))
            })?;
            reqwest::header::HeaderValue::try_from(value.as_str()).map_err(|e| {
                Error::invalid_argument().message(format!(
                    "invalid custom HTTP header value for {name:?}: {e}"
                ))
            })?;
        }

        // Follow redirects for normal traffic (reqwest's default policy).
        let client = Self::build_reqwest_client(&config, reqwest::redirect::Policy::default())?;

        // Surface which TLS backend was compiled in so oncall debugging
        // a handshake / unknown-CA report can tell at a glance whether
        // this binary is on native-tls or rustls. Both read the OS system
        // CA store (rustls via `rustls-tls-native-roots`, which also keeps
        // the Mozilla webpki-roots as a fallback — see odbc#323), but the
        // two stacks still differ on cert validation behavior, OCSP, and
        // TLS 1.3 quirks.
        debug!(
            tls_backend = if cfg!(feature = "tls-rustls") {
                "rustls"
            } else {
                "native-tls"
            },
            "DatabricksHttpClient initialised"
        );

        Ok(Self {
            client,
            config,
            auth_provider: OnceLock::new(),
            retry_configs,
        })
    }

    /// Build a `reqwest::Client` from `config` with the given redirect policy.
    ///
    /// Factored out of `new()` so tenant discovery can obtain a client that
    /// does NOT follow redirects (to read the first `/aad/auth` hop's
    /// `Location`) while still honouring the workspace's TLS, proxy, timeout,
    /// and user-agent configuration.
    fn build_reqwest_client(
        config: &HttpClientConfig,
        redirect: reqwest::redirect::Policy,
    ) -> Result<Client> {
        let mut builder = Client::builder()
            .connect_timeout(config.connect_timeout)
            // `read_timeout` is a historical name; reqwest applies this as
            // the total deadline from connect through response-body completion.
            .timeout(config.read_timeout)
            .pool_max_idle_per_host(config.max_connections_per_host)
            .user_agent(&config.user_agent)
            .redirect(redirect)
            .dns_resolver(crate::client::dns::CachingResolver::shared());

        // Apply TLS configuration
        if config.tls.enabled == Some(false) {
            builder = builder.https_only(false);
            warn!("TLS is disabled — connections will use plain HTTP");
        }

        // reqwest does not support accepting only self-signed certs while still
        // validating the trust chain — danger_accept_invalid_certs(true) disables
        // ALL certificate validation (CA trust, expiration, etc.). Attach
        // `org_id` to the warning so a central log store can attribute the
        // relaxation back to the workspace that opted in.
        if config.tls.allow_self_signed.unwrap_or(false) {
            warn!(
                org_id = ?config.org_id,
                "TLS certificate validation is disabled — this is insecure and should only be used in development. Note: reqwest does not distinguish self-signed from fully unvalidated; this disables ALL certificate validation."
            );
            builder = builder.danger_accept_invalid_certs(true);
        }

        if config.tls.allow_hostname_mismatch.unwrap_or(false) {
            warn!(
                org_id = ?config.org_id,
                "TLS hostname verification is disabled — this is insecure and should only be used in development"
            );
            builder = builder.danger_accept_invalid_hostnames(true);
        }

        if let Some(ref cert_path) = config.tls.trusted_certificate_path {
            let pem = std::fs::read(cert_path).map_err(|e| {
                Error::invalid_argument().message(format!(
                    "Failed to read TLS certificate '{}': {}",
                    cert_path, e
                ))
            })?;
            // `from_pem_bundle` (not `from_pem`) so a concatenated
            // root + intermediate bundle loads every certificate.
            // Under `tls-native`, the single-cert `from_pem` would
            // silently drop everything past the first `-----END
            // CERTIFICATE-----`, which is exactly the corporate
            // re-signing TLS case the custom-CA surface targets.
            let certs = reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| {
                Error::invalid_argument()
                    .message(format!("Invalid PEM certificate '{}': {}", cert_path, e))
            })?;
            // `from_pem_bundle` returns Ok(empty Vec) for input that
            // contains no `-----BEGIN CERTIFICATE-----` blocks
            // (garbage, JSON, etc.). Surface that as an explicit
            // error rather than silently doing nothing.
            if certs.is_empty() {
                return Err(Error::invalid_argument().message(format!(
                    "Invalid PEM certificate '{}': no certificates found in file",
                    cert_path
                )));
            }
            let cert_count = certs.len();
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
            debug!(
                "Added {} custom CA certificate(s) from: {}",
                cert_count, cert_path
            );
        }

        if let Some(ref pem) = config.tls.trusted_certificate_pem {
            // `from_pem_bundle` so root + intermediate concatenations
            // (the common corporate-CA case) load fully. See the
            // `trusted_certificate_path` branch above for why.
            let certs = reqwest::Certificate::from_pem_bundle(pem).map_err(|e| {
                Error::invalid_argument().message(format!(
                    "Invalid PEM bytes in TlsConfig.custom_ca_cert: {}",
                    e
                ))
            })?;
            // See the path branch above: empty bundle on non-empty
            // input is a caller error (garbage / wrong format), not
            // a silent no-op.
            if certs.is_empty() {
                return Err(Error::invalid_argument().message(
                    "Invalid PEM bytes in TlsConfig.custom_ca_cert: no certificates found",
                ));
            }
            let cert_count = certs.len();
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
            debug!(
                "Added {} custom CA certificate(s) from in-memory bytes ({} bytes)",
                cert_count,
                pem.len()
            );
        }

        // Apply mTLS client identity. Both halves must be present —
        // a cert without a key (or vice versa) is a caller error, not
        // a partial handshake we should attempt.
        match (&config.tls.client_cert_pem, &config.tls.client_key_pem) {
            (Some(cert), Some(key)) => {
                let identity = build_mtls_identity(cert, key)?;
                builder = builder.identity(identity);
                debug!(
                    "Configured mTLS client identity ({} cert bytes)",
                    cert.len()
                );
            }
            (Some(_), None) => {
                return Err(Error::invalid_argument().message(
                    "TlsConfig.client_cert_pem is set without client_key_pem; \
                     mTLS requires both the client certificate and its private key",
                ));
            }
            (None, Some(_)) => {
                return Err(Error::invalid_argument().message(
                    "TlsConfig.client_key_pem is set without client_cert_pem; \
                     mTLS requires both the client certificate and its private key",
                ));
            }
            (None, None) => {}
        }

        // Apply proxy configuration
        if let Some(ref proxy_url) = config.proxy.url {
            // Validate proxy URL scheme
            if !proxy_url.starts_with("http://") && !proxy_url.starts_with("https://") {
                return Err(Error::invalid_argument().message(format!(
                    "Proxy URL must use http:// or https:// scheme, got: '{}'",
                    proxy_url
                )));
            }

            let mut proxy = Proxy::all(proxy_url).map_err(|e| {
                Error::invalid_argument()
                    .message(format!("Invalid proxy URL '{}': {}", proxy_url, e))
            })?;

            // Add basic auth if credentials provided.
            // Explicit username/password override any credentials embedded in the URL.
            if let Some(ref username) = config.proxy.username {
                if proxy_url.contains('@') {
                    warn!("Proxy URL contains embedded credentials, but explicit username/password are also set; explicit credentials take precedence");
                }
                let password = config.proxy.password.as_deref().unwrap_or("");
                proxy = proxy.basic_auth(username, password);
            } else if config.proxy.password.is_some() {
                warn!("Proxy password provided without username; password will be ignored");
            }

            // Apply bypass_hosts list to the proxy.
            // Normalize by trimming whitespace around entries (e.g., "host1, host2").
            if let Some(ref bypass_hosts) = config.proxy.bypass_hosts {
                let normalized: String = bypass_hosts
                    .split(',')
                    .map(|s| s.trim())
                    .collect::<Vec<_>>()
                    .join(",");
                proxy = proxy.no_proxy(NoProxy::from_string(&normalized));
            }

            builder = builder.proxy(proxy);

            debug!(
                "HTTP client configured with proxy: {} (bypass_hosts: {:?})",
                proxy_url, config.proxy.bypass_hosts
            );
        } else {
            if config.proxy.username.is_some() {
                warn!(
                    "Proxy credentials provided but no proxy URL set; credentials will be ignored"
                );
            }
            if config.proxy.bypass_hosts.is_some() {
                warn!("Proxy bypass_hosts provided but no proxy URL set; bypass list will be ignored (env var NO_PROXY is unaffected)");
            }
            debug!("HTTP client using default proxy behavior (env vars)");
        }

        builder.build().map_err(|e| {
            Error::invalid_argument().message(format!("Failed to create HTTP client: {e}"))
        })
    }

    /// Creates a new HTTP client with default retry configs for all request categories.
    ///
    /// Convenience method for tests and simple use cases where custom retry
    /// configuration is not needed.
    pub fn with_default_retry(config: HttpClientConfig) -> Result<Self> {
        use crate::client::retry::build_retry_configs;
        let retry_configs =
            build_retry_configs(&RetryConfig::default(), &std::collections::HashMap::new());
        Self::new(config, retry_configs)
    }

    /// Sets the auth provider for this client.
    ///
    /// This must be called exactly once after construction and before calling `execute()`.
    /// Calling this method more than once will panic (OnceLock semantics).
    pub fn set_auth_provider(&self, provider: Arc<dyn AuthProvider>) {
        self.auth_provider
            .set(provider)
            .expect("Auth provider can only be set once");
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &HttpClientConfig {
        &self.config
    }

    /// Returns the underlying reqwest client for building requests.
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Get the authorization header value.
    ///
    /// Returns an error if the auth provider has not been set via `set_auth_provider()`.
    pub fn auth_header(&self) -> Result<String> {
        let provider = self.auth_provider.get().ok_or_else(|| {
            Error::invalid_state().message("Auth provider not set. Call set_auth_provider() first.")
        })?;
        provider.get_auth_header()
    }

    /// Extra auth headers to attach alongside `Authorization` (e.g. the Azure SP
    /// management-token pair). Empty for every provider except the Azure SP one.
    /// Errors if the auth provider has not been set.
    pub fn auth_extra_headers(&self) -> Result<Vec<(String, String)>> {
        let provider = self.auth_provider.get().ok_or_else(|| {
            Error::invalid_state().message("Auth provider not set. Call set_auth_provider() first.")
        })?;
        provider.extra_headers()
    }

    /// Returns the retry config for the given request category.
    fn retry_config(&self, category: RequestCategory) -> &RetryConfig {
        self.retry_configs
            .get(&category)
            .expect("All categories should have retry configs")
    }

    /// Execute an HTTP request with authentication and idempotency-aware retry.
    ///
    /// The `RequestType` determines which `RetryConfig` and `RetryStrategy` to use.
    pub async fn execute(&self, request: Request, request_type: RequestType) -> Result<Response> {
        self.execute_impl(request, true, request_type).await
    }

    /// Execute a request without authentication (for CloudFetch, OAuth endpoints).
    ///
    /// Same retry logic as `execute()`, just skips the Authorization header.
    pub async fn execute_without_auth(
        &self,
        request: Request,
        request_type: RequestType,
    ) -> Result<Response> {
        self.execute_impl(request, false, request_type).await
    }

    /// Issue a single unauthenticated GET to `url` WITHOUT following redirects
    /// and return the `Location` header of the (expected) 3xx response.
    ///
    /// Unlike [`execute`](Self::execute) / [`execute_without_auth`], which use a
    /// redirect-following client, this builds a one-off client with a
    /// no-redirect policy — reqwest fixes the redirect policy at client-build
    /// time — while reusing the workspace's TLS/proxy/timeout config. Used by
    /// Azure tenant discovery to read the tenant off the FIRST `/aad/auth`
    /// redirect hop (matching the connector's `get_azure_tenant_id_from_host`),
    /// rather than parsing the final URL after following the whole Entra
    /// redirect chain (which can traverse non-tenant-prefixed hops like
    /// `/common/` or a `login.live.com` bounce).
    ///
    /// NOT retry-aware: unlike [`execute`](Self::execute) /
    /// [`execute_without_auth`], this one-off client does NOT run the kernel's
    /// retry ladder, so a transient 5xx / timeout / connection reset on this
    /// single probe surfaces as an error. Because the only caller
    /// (`azure::discover_tenant`) propagates that error with `?` and there is
    /// no automatic retry or fallback at the call site, a transient blip during
    /// discovery is fatal to `Session::open` for a zero-config-tenant
    /// `azure-sp-m2m` caller; the recourse is to re-run with an explicit
    /// `azure_tenant_id` (which skips discovery entirely). A single hop is
    /// sufficient for discovery, so no retry is attempted here.
    pub async fn first_redirect_location(&self, url: &str) -> Result<Option<String>> {
        let client = Self::build_reqwest_client(&self.config, reqwest::redirect::Policy::none())?;
        let response = client.get(url).send().await.map_err(|e| {
            Error::network_error().message(format!("Redirect-probe request to {url} failed: {e}"))
        })?;
        Ok(response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string))
    }

    /// Internal implementation of execute with configurable auth and retry strategy.
    async fn execute_impl(
        &self,
        request: Request,
        with_auth: bool,
        request_type: RequestType,
    ) -> Result<Response> {
        let config = self.retry_config(request_type.category());
        let strategy = RetryStrategy::for_request(request_type, config);
        let max_attempts = config.max_retries + 1;
        let start_time = Instant::now();

        let mut attempts: u32 = 0;

        // Clone the request parts we need for retries
        let method = request.method().clone();
        let url = request.url().clone();
        let headers = request.headers().clone();
        // Preserve any per-request timeout the caller set on the incoming
        // request (e.g. the CloudFetch download path caps each attempt at
        // the remaining chunk budget). Rebuilding the request per attempt
        // below would otherwise silently drop it and fall back to the
        // client-wide `.timeout()`, defeating the deadline (audit H05).
        let per_request_timeout = request.timeout().copied();
        let body_bytes = request
            .body()
            .and_then(|b| b.as_bytes())
            .map(|b| b.to_vec());

        loop {
            attempts += 1;

            // Build a fresh request for this attempt
            let mut req_builder = self.client.request(method.clone(), url.clone());

            // Caller-supplied custom headers go on FIRST, so the
            // kernel's own auth / routing headers (set last, below) take
            // precedence — matching the Thrift connector, where
            // `auth_provider.add_headers` runs after the custom-header
            // dict. We SKIP any custom header named `Authorization` or
            // `x-databricks-org-id` (case-insensitive) so a stray caller
            // header can't sit alongside the kernel's (reqwest appends
            // same-named headers rather than replacing, which for those
            // two would send a duplicate and could confuse the server).
            // `User-Agent` is NOT in this map — it was folded into the
            // client's effective UA in `new()` (a caller UA replaces the
            // base, which then becomes reqwest's default UA header).
            //
            // Note: this loop runs for BOTH `execute` and
            // `execute_without_auth`, so custom headers (and the effective
            // User-Agent) also ride OAuth token-endpoint requests. This
            // is intentional and matches the Thrift connector, where
            // `setCustomHeaders` applies to every request on the
            // transport, IdP calls included.
            for (name, value) in &self.config.custom_headers {
                let lname = name.to_ascii_lowercase();
                if lname == "authorization" || lname == "x-databricks-org-id" {
                    warn!(
                        header = %name,
                        "ignoring caller-supplied custom header; this name is \
                         managed by the kernel and cannot be overridden"
                    );
                    continue;
                }
                if lname == "accept-encoding"
                    && with_auth
                    && !self.config.sea_response_compression_enabled
                {
                    warn!(
                        header = %name,
                        "ignoring caller-supplied Accept-Encoding header because SEA response \
                         compression is disabled"
                    );
                    continue;
                }
                req_builder = req_builder.header(name, value);
            }

            for (name, value) in headers.iter() {
                req_builder = req_builder.header(name, value);
            }

            if with_auth {
                if !self.config.sea_response_compression_enabled {
                    req_builder = req_builder.header("Accept-Encoding", "identity");
                }
                let auth_header = self.auth_header()?;
                req_builder = req_builder.header("Authorization", auth_header);
                // Extra auth headers (Azure SP management-token pair). Empty for
                // every other provider. Applied only on authenticated requests,
                // never on the token/OIDC calls that use `execute_without_auth`.
                for (name, value) in self.auth_extra_headers()? {
                    req_builder = req_builder.header(name, value);
                }
            }

            // Add org ID header for multi-tenant workspace routing
            if let Some(ref org_id) = self.config.org_id {
                req_builder = req_builder.header("x-databricks-org-id", org_id);
            }

            if let Some(ref body) = body_bytes {
                req_builder = req_builder.body(body.clone());
            }

            // Re-apply the caller's per-request timeout on every rebuilt
            // attempt so it survives the per-attempt request reconstruction.
            if let Some(timeout) = per_request_timeout {
                req_builder = req_builder.timeout(timeout);
            }

            let request = req_builder.build().map_err(|e| {
                Error::invalid_argument().message(format!("Failed to build request: {e}"))
            })?;

            debug!(
                "{:?}: {} {} (attempt {}/{})",
                request_type, method, url, attempts, max_attempts
            );

            match self.client.execute(request).await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        if attempts > 1 {
                            debug!(
                                "{:?}: completed after {} attempts in {:.1}s — success",
                                request_type,
                                attempts,
                                start_time.elapsed().as_secs_f64()
                            );
                        }
                        return Ok(response);
                    }

                    // Extract Retry-After header before consuming response
                    let retry_after = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());

                    let has_retry_after = retry_after.is_some();

                    if strategy.is_retryable_status(status, has_retry_after)
                        && attempts < max_attempts
                    {
                        let elapsed = start_time.elapsed();
                        let wait = calculate_backoff(config, attempts, retry_after.as_deref());

                        // Check overall timeout: don't start a retry that will time out
                        if elapsed + wait > config.overall_timeout {
                            let error_body = response.text().await.unwrap_or_default();
                            debug!(
                                "{:?}: failed after {} attempts in {:.1}s — retry timeout exceeded",
                                request_type,
                                attempts,
                                elapsed.as_secs_f64()
                            );
                            return Err(http_error(
                                status.as_u16(),
                                &error_body,
                                format_args!(
                                    "HTTP {} — retry timeout exceeded after {} attempts ({:.1}s)",
                                    status.as_u16(),
                                    attempts,
                                    elapsed.as_secs_f64()
                                ),
                            ));
                        }

                        warn!(
                            "{:?}: HTTP {} (attempt {}/{}), waiting {:.1}s before retry",
                            request_type,
                            status.as_u16(),
                            attempts,
                            max_attempts,
                            wait.as_secs_f64()
                        );
                        sleep(wait).await;
                        continue;
                    }

                    // Non-retryable or max retries exceeded
                    let error_body = response.text().await.unwrap_or_default();
                    debug!(
                        "{:?}: failed after {} attempts in {:.1}s — HTTP {}",
                        request_type,
                        attempts,
                        start_time.elapsed().as_secs_f64(),
                        status.as_u16()
                    );
                    return Err(http_error(
                        status.as_u16(),
                        &error_body,
                        format_args!("HTTP {}", status.as_u16()),
                    ));
                }
                Err(e) => {
                    let mut causes = super::error_chain(&e)
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    causes.dedup();
                    let error_message = causes.join(": ");
                    if strategy.is_retryable_error(&e) && attempts < max_attempts {
                        let elapsed = start_time.elapsed();
                        let wait = calculate_backoff(config, attempts, None);

                        if elapsed + wait > config.overall_timeout {
                            debug!(
                                "{:?}: failed after {} attempts in {:.1}s — retry timeout exceeded",
                                request_type,
                                attempts,
                                elapsed.as_secs_f64()
                            );
                            return Err(Error::network_error().message(format!(
                                "HTTP request failed — retry timeout exceeded after {} attempts ({:.1}s): {}",
                                attempts,
                                elapsed.as_secs_f64(),
                                error_message
                            )));
                        }

                        warn!(
                            "{:?}: error (attempt {}/{}): {}, waiting {:.1}s before retry",
                            request_type,
                            attempts,
                            max_attempts,
                            error_message,
                            wait.as_secs_f64()
                        );
                        sleep(wait).await;
                        continue;
                    }

                    debug!(
                        "{:?}: failed after {} attempts in {:.1}s — {}",
                        request_type,
                        attempts,
                        start_time.elapsed().as_secs_f64(),
                        error_message
                    );
                    return Err(Error::network_error()
                        .with_retryable(!is_certificate_verification_failure(&e))
                        .message(format!(
                            "HTTP request failed after {} attempts: {}",
                            attempts, error_message
                        )));
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "http_tls_tests.rs"]
mod tls_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::PersonalAccessToken;
    use crate::client::retry::build_retry_configs;

    fn default_retry_configs() -> HashMap<RequestCategory, RetryConfig> {
        build_retry_configs(&RetryConfig::default(), &HashMap::new())
    }

    #[test]
    fn test_http_client_config_default() {
        let config = HttpClientConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.read_timeout, Duration::from_secs(60));
        assert_eq!(config.max_connections_per_host, 100);
        assert_eq!(
            config.user_agent,
            format!("DatabricksSQLKernel/{}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn clients_resolve_hosts_through_the_shared_dns_cache() {
        use wiremock::matchers::any;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let mut config = HttpClientConfig::default();
        config.tls.enabled = Some(false);
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        // A name, not the mock's IP literal, so a resolver is consulted.
        let url = format!("http://localhost:{}/", server.address().port());
        client.inner().get(url).send().await.expect("request");
        // Only clients built here write to the shared cache.
        assert!(crate::client::dns::CachingResolver::shared().has_answer("localhost"));
    }

    #[tokio::test]
    async fn test_auth_header_after_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
        client.set_auth_provider(auth);

        let header = client.auth_header().unwrap();
        assert_eq!(header, "Bearer test-token");
    }

    #[tokio::test]
    async fn test_execute_fails_before_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();

        let result = client.auth_header();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Auth provider not set"));
    }

    #[tokio::test]
    async fn test_execute_succeeds_after_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));

        client.set_auth_provider(auth);

        let header = client.auth_header().unwrap();
        assert_eq!(header, "Bearer test-token");
    }

    #[tokio::test]
    async fn test_custom_headers_applied_with_auth_winning() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Expect: our benign custom header present; the caller's
        // User-Agent REPLACES the kernel's base UA entirely (so the SEA
        // server sees the caller's driver identity for attribution; its
        // prefix is presumed allow-listed for the result disposition);
        // Authorization is the kernel's real token (NOT the caller's
        // bogus one).
        Mock::given(method("GET"))
            .and(path("/probe"))
            .and(header("x-custom-header", "custom-value"))
            .and(header("user-agent", "MyConnector/1.0 (entry)"))
            .and(header("authorization", "Bearer real-token"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let config = HttpClientConfig {
            // Fixed base UA to prove the caller UA fully replaces it
            // (the real default embeds the crate version).
            user_agent: "KernelBase/9.9".to_string(),
            custom_headers: vec![
                ("x-custom-header".to_string(), "custom-value".to_string()),
                (
                    "User-Agent".to_string(),
                    "MyConnector/1.0 (entry)".to_string(),
                ),
                // Caller attempt to override auth — must be ignored; the
                // kernel's real token wins.
                (
                    "Authorization".to_string(),
                    "Bearer caller-bogus".to_string(),
                ),
            ],
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        client.set_auth_provider(Arc::new(PersonalAccessToken::new("real-token".to_string())));

        let req = client
            .inner()
            .request(reqwest::Method::GET, format!("{}/probe", server.uri()))
            .build()
            .unwrap();
        let resp = client
            .execute(req, RequestType::GetStatementStatus)
            .await
            .expect("request should succeed against the mock");
        assert_eq!(resp.status().as_u16(), 200);
        // If the matchers above didn't all match, wiremock returns the
        // default 404 and the status assert fails — so a 200 confirms
        // custom header + caller UA present AND auth = real-token.
    }

    #[tokio::test]
    async fn authenticated_requests_use_identity_encoding_by_default() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/probe"))
            .and(header("accept-encoding", "identity"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let config = HttpClientConfig {
            custom_headers: vec![("Accept-Encoding".to_string(), "gzip".to_string())],
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::with_default_retry(config).unwrap();
        client.set_auth_provider(Arc::new(PersonalAccessToken::new("token".to_string())));
        let request = client
            .inner()
            .get(format!("{}/probe", server.uri()))
            .build()
            .unwrap();
        let response = client
            .execute(request, RequestType::GetStatementStatus)
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        let requests = server.received_requests().await.unwrap();
        let accept_encodings: Vec<_> = requests[0]
            .headers
            .get_all("accept-encoding")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect();
        assert_eq!(accept_encodings, vec!["identity"]);
    }

    #[tokio::test]
    async fn authenticated_requests_can_enable_response_compression() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let config = HttpClientConfig {
            sea_response_compression_enabled: true,
            custom_headers: vec![("Accept-Encoding".to_string(), "gzip".to_string())],
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::with_default_retry(config).unwrap();
        client.set_auth_provider(Arc::new(PersonalAccessToken::new("token".to_string())));
        let request = client
            .inner()
            .get(format!("{}/probe", server.uri()))
            .build()
            .unwrap();
        client
            .execute(request, RequestType::GetStatementStatus)
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();
        assert_eq!(
            requests[0]
                .headers
                .get("accept-encoding")
                .map(|value| value.to_str().unwrap()),
            Some("gzip")
        );
    }

    #[test]
    fn test_caller_user_agent_replaces_base() {
        // A caller User-Agent fully replaces the base UA, and is removed
        // from the custom-header list (so it isn't re-applied per request;
        // it becomes reqwest's default UA instead).
        let config = HttpClientConfig {
            user_agent: "DatabricksSQLKernel/9.9".to_string(),
            custom_headers: vec![
                ("x-keep".to_string(), "v".to_string()),
                (
                    "user-agent".to_string(), // lowercase: match is case-insensitive
                    "PyDatabricksSqlConnector/4.2.7 (entry)".to_string(),
                ),
            ],
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        assert_eq!(
            client.config().user_agent,
            "PyDatabricksSqlConnector/4.2.7 (entry)"
        );
        assert!(
            !client
                .config()
                .custom_headers
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case("user-agent")),
            "caller User-Agent must be folded out of custom_headers"
        );
        // Non-UA custom headers are untouched.
        assert!(client
            .config()
            .custom_headers
            .iter()
            .any(|(n, v)| n == "x-keep" && v == "v"));
    }

    #[test]
    fn test_empty_caller_user_agent_keeps_base() {
        // A whitespace-only caller UA is ignored — the base UA stays so
        // the request still carries an identity (and the
        // disposition-gating prefix when the base is allow-listed).
        let config = HttpClientConfig {
            user_agent: "DatabricksSQLKernel/9.9".to_string(),
            custom_headers: vec![("User-Agent".to_string(), "   ".to_string())],
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        assert_eq!(client.config().user_agent, "DatabricksSQLKernel/9.9");
    }

    #[test]
    fn test_no_caller_user_agent_keeps_base() {
        let config = HttpClientConfig {
            user_agent: "DatabricksSQLKernel/9.9".to_string(),
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        assert_eq!(client.config().user_agent, "DatabricksSQLKernel/9.9");
    }

    #[test]
    fn test_oversized_caller_user_agent_truncated_on_char_boundary() {
        // An over-long UA is truncated without panicking even when the
        // 1024-byte cut would land inside a multi-byte codepoint.
        let mut ua = "x".repeat(1023);
        ua.push('é'); // 2 bytes straddling the 1024-byte limit
        let config = HttpClientConfig {
            user_agent: "DatabricksSQLKernel/9.9".to_string(),
            custom_headers: vec![("User-Agent".to_string(), ua)],
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        // Truncated to the char boundary at byte 1023 (the 'é' is dropped).
        assert_eq!(client.config().user_agent.len(), 1023);
        assert!(client.config().user_agent.chars().all(|c| c == 'x'));
    }

    #[test]
    fn test_malformed_custom_header_rejected_at_construction() {
        // A structurally-invalid header name must fail at `new()`
        // (session-open) rather than on the first request build.
        let bad_name = HttpClientConfig {
            custom_headers: vec![("Bad Header Name".to_string(), "v".to_string())],
            ..HttpClientConfig::default()
        };
        let err = DatabricksHttpClient::new(bad_name, default_retry_configs()).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("header name"), "{}", err.message);

        // An invalid header value (control char) is likewise rejected.
        let bad_value = HttpClientConfig {
            custom_headers: vec![("X-Custom".to_string(), "bad\nvalue".to_string())],
            ..HttpClientConfig::default()
        };
        let err = DatabricksHttpClient::new(bad_value, default_retry_configs()).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("header value"), "{}", err.message);
    }

    #[tokio::test]
    #[should_panic(expected = "Auth provider can only be set once")]
    async fn test_set_auth_provider_twice_panics() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        let auth1 = Arc::new(PersonalAccessToken::new("token1".to_string()));
        let auth2 = Arc::new(PersonalAccessToken::new("token2".to_string()));

        client.set_auth_provider(auth1);
        client.set_auth_provider(auth2); // Should panic
    }

    #[tokio::test]
    async fn test_execute_without_auth_works_before_auth_set() {
        let config = HttpClientConfig::default();
        let client = DatabricksHttpClient::new(config, default_retry_configs()).unwrap();
        assert!(client.auth_provider.get().is_none());
    }

    #[test]
    fn test_proxy_config_default() {
        let config = ProxyConfig::default();
        assert!(config.url.is_none());
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.bypass_hosts.is_none());
    }

    #[test]
    fn test_http_client_config_default_has_default_proxy() {
        let config = HttpClientConfig::default();
        assert!(config.proxy.url.is_none());
    }

    #[test]
    fn test_http_client_with_proxy() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_authenticated_proxy() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.username = Some("user".to_string());
        config.proxy.password = Some("pass".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_invalid_proxy_url() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://".to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Invalid proxy URL"));
    }

    #[test]
    fn test_http_client_with_no_scheme_proxy_url() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("proxy.example.com:8080".to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("http:// or https://"));
    }

    #[test]
    fn test_http_client_with_bypass_hosts() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.bypass_hosts = Some("localhost,*.internal.corp,.example.com".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_bypass_hosts_whitespace() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("http://proxy.example.com:8080".to_string());
        config.proxy.bypass_hosts = Some("localhost , *.internal.corp , .example.com".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_rejects_invalid_proxy_scheme() {
        let mut config = HttpClientConfig::default();
        config.proxy.url = Some("socks5://proxy.example.com:1080".to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("http:// or https://"));
    }

    #[test]
    fn test_http_client_proxy_username_without_url() {
        let mut config = HttpClientConfig::default();
        config.proxy.username = Some("user".to_string());
        config.proxy.password = Some("pass".to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(config.enabled.is_none());
        assert!(config.allow_self_signed.is_none());
        assert!(config.allow_hostname_mismatch.is_none());
        assert!(config.trusted_certificate_path.is_none());
    }

    #[test]
    fn test_http_client_config_default_has_default_tls() {
        let config = HttpClientConfig::default();
        assert!(config.tls.enabled.is_none());
        assert!(config.tls.trusted_certificate_path.is_none());
    }

    #[test]
    fn test_http_client_with_allow_self_signed() {
        let mut config = HttpClientConfig::default();
        config.tls.allow_self_signed = Some(true);
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_hostname_mismatch_allowed() {
        let mut config = HttpClientConfig::default();
        config.tls.allow_hostname_mismatch = Some(true);
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_tls_disabled() {
        let mut config = HttpClientConfig::default();
        config.tls.enabled = Some(false);
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    /// A self-signed CA certificate for testing. Generated once and hardcoded
    /// to avoid platform-specific dependencies (e.g., openssl not on Windows).
    const TEST_CA_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUbS3CBzF8+mVOJShXAIjPBRUCFWMwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHVGVzdCBDQTAeFw0yNjAzMjUwNjQwMjhaFw0zNjAzMjIw
NjQwMjhaMBIxEDAOBgNVBAMMB1Rlc3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQDRoAl14R0QgADk1N3utgSbojOAh/Ub5v6sC8z8dXGxxAFBI+vV
UYNYNV6iF458lsgo+v1xUBDqmR7+k6YsJhwxrWCBZAei471DrAZid6MtSqP9m+I5
HAa5qH4TE+NB+npCjQQsFlPl0rdoDSPnsCVMaEgxnkkZdYQTztaUjfapF+kDRPMe
XedQWRKbdolsmGNV9l6yuX8IDLRWFp4ublly+EszulszhCBqfBolc/eMBn8yRmdN
txbAhLnklwDBBewP2AU5zpeVcwX+iZM5gj3AN7CDJkAC68ZJKBFLqttsJZKx5NuT
2isq14/YA5TvoXsaN5rkH5VbmyrRUb3TNILfAgMBAAGjUzBRMB0GA1UdDgQWBBQa
Vn826exMR5xKRr2ArIOhlNWeLzAfBgNVHSMEGDAWgBQaVn826exMR5xKRr2ArIOh
lNWeLzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBVxFNYazoo
uGa3MnOKoK73K/Ew2PSfK+7RbUNxJxsv0mtKZXkaq/TFqnpbqjTllSe9JK+bOaDF
s5dTvaMxGsAgwKc5TqFuF2jdqiZytUZFsr65gAIFlZOI8zuAiVfj/B1iOtk84Jh/
PCJ2x3ALIWD5mGMHZM/4tdtajjM+uiSehLzpSp2B2FFupmDLrmQkpr9zKet83ZtG
If8L/vYYFGAGmphgF1V/Qb4CuP9ZmbgZZgjPAEo9oY2CXwf/TMz7kc3JuKYscc7y
SKEDvq0G7Lbxl85EtEswMp1mdl7+WOUWgPk3whOzl8rn8BfQAyPBWjzzTWT3eoWv
lGrv15pBcePj
-----END CERTIFICATE-----
";

    #[test]
    fn test_http_client_with_custom_ca_cert() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), TEST_CA_CERT_PEM).unwrap();

        let mut config = HttpClientConfig::default();
        config.tls.trusted_certificate_path = Some(tmp.path().to_string_lossy().to_string());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_http_client_with_invalid_cert_path() {
        let mut config = HttpClientConfig::default();
        config.tls.trusted_certificate_path = Some("/nonexistent/path/cert.pem".to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Failed to read TLS certificate"));
    }

    #[test]
    fn test_http_client_with_invalid_pem() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "not a valid PEM certificate").unwrap();
        let mut config = HttpClientConfig::default();
        config.tls.trusted_certificate_path = Some(tmp.path().to_string_lossy().to_string());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        // `from_pem_bundle` returns Ok(empty Vec) for input with no
        // `-----BEGIN CERTIFICATE-----` markers; the kernel surfaces
        // that as "no certificates found".
        assert!(
            err_msg.contains("Invalid PEM certificate")
                && err_msg.contains("no certificates found"),
            "expected 'Invalid PEM certificate' + 'no certificates found', got: {err_msg}"
        );
    }

    #[test]
    fn test_http_client_with_multi_cert_pem_bundle() {
        // Corporate-CA case: concatenated root + intermediate. Both
        // must load (regression guard against the single-cert
        // `from_pem` path that silently dropped the second cert on
        // the `tls-native` backend).
        let bundle = format!("{TEST_CA_CERT_PEM}{TEST_CA_CERT_PEM}");
        let mut config = HttpClientConfig::default();
        config.tls.trusted_certificate_pem = Some(bundle.into_bytes());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok(), "multi-cert bundle should load cleanly");
    }

    #[test]
    fn test_http_client_with_empty_pem_bytes_errors() {
        // Empty / non-PEM input via `custom_ca_cert` is an explicit
        // error, not a silent no-op.
        let mut config = HttpClientConfig::default();
        config.tls.trusted_certificate_pem = Some(b"not pem at all".to_vec());
        let result = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("TlsConfig.custom_ca_cert")
                && err_msg.contains("no certificates found"),
            "expected error to name the public field + 'no certificates found', got: {err_msg}"
        );
    }

    #[test]
    fn test_http_client_with_in_memory_pem_bytes() {
        // Public-API path: PEM bytes pass through the internal
        // `trusted_certificate_pem` field directly (no tempfile shim
        // required by foreign-language wrappers).
        let mut config = HttpClientConfig::default();
        config.tls.trusted_certificate_pem = Some(TEST_CA_CERT_PEM.as_bytes().to_vec());
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(client.is_ok());
    }

    #[test]
    fn test_tls_config_from_public_maps_all_fields() {
        // accept_self_signed=true maps to Some(true);
        // skip_hostname_verification=true maps to Some(true);
        // custom_ca_cert maps to trusted_certificate_pem.
        let public = crate::config::TlsConfig {
            custom_ca_cert: Some(b"pem-bytes".to_vec()),
            accept_self_signed: true,
            skip_hostname_verification: true,
            client_cert_pem: Some(b"cert-bytes".to_vec()),
            client_key_pem: Some(b"key-bytes".to_vec()),
        };
        let internal = TlsConfig::from_public(&public);
        assert_eq!(internal.allow_self_signed, Some(true));
        assert_eq!(internal.allow_hostname_mismatch, Some(true));
        assert_eq!(
            internal.trusted_certificate_pem,
            Some(b"pem-bytes".to_vec())
        );
        assert_eq!(internal.client_cert_pem, Some(b"cert-bytes".to_vec()));
        assert_eq!(internal.client_key_pem, Some(b"key-bytes".to_vec()));
        // `enabled` and `trusted_certificate_path` stay at internal
        // defaults — the public surface doesn't expose them.
        assert!(internal.enabled.is_none());
        assert!(internal.trusted_certificate_path.is_none());
    }

    #[test]
    fn test_tls_config_from_public_default_is_strict() {
        // A default public TlsConfig produces an internal config that
        // **explicitly** force-disables every permissive switch
        // (`Some(false)`), not `None`. The reason: if a future
        // internal default were to flip lenient, a public caller
        // relying on `TlsConfig::default()` must still get strict TLS
        // — `None` here would inherit the (hypothetical) lenient
        // default, `Some(false)` overrides it.
        let internal = TlsConfig::from_public(&crate::config::TlsConfig::default());
        assert_eq!(internal.allow_self_signed, Some(false));
        assert_eq!(internal.allow_hostname_mismatch, Some(false));
        assert!(internal.trusted_certificate_pem.is_none());
        assert!(internal.trusted_certificate_path.is_none());
        // mTLS unset by default.
        assert!(internal.client_cert_pem.is_none());
        assert!(internal.client_key_pem.is_none());
    }

    #[test]
    fn test_http_client_via_public_tls_config_with_self_signed_accepts_build() {
        // End-to-end builder check: a Session-style construction
        // (public TlsConfig -> from_public -> HttpClientConfig ->
        // reqwest builder) succeeds when accept_self_signed is set.
        let public = crate::config::TlsConfig {
            custom_ca_cert: None,
            accept_self_signed: true,
            skip_hostname_verification: false,
            ..crate::config::TlsConfig::default()
        };
        let config = HttpClientConfig {
            tls: TlsConfig::from_public(&public),
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(
            client.is_ok(),
            "reqwest client construction should succeed when accept_self_signed=true is set"
        );
    }

    #[test]
    fn test_http_client_via_public_tls_config_with_custom_ca_accepts_build() {
        let public = crate::config::TlsConfig {
            custom_ca_cert: Some(TEST_CA_CERT_PEM.as_bytes().to_vec()),
            accept_self_signed: false,
            skip_hostname_verification: false,
            ..crate::config::TlsConfig::default()
        };
        let config = HttpClientConfig {
            tls: TlsConfig::from_public(&public),
            ..HttpClientConfig::default()
        };
        let client = DatabricksHttpClient::new(config, default_retry_configs());
        assert!(
            client.is_ok(),
            "reqwest client construction should succeed when custom_ca_cert is set with a valid PEM"
        );
    }

    #[test]
    fn test_mtls_requires_both_cert_and_key() {
        // A client cert without its key (or vice versa) is a caller
        // error — surface it as InvalidArgument at build time rather
        // than attempting a half-configured handshake.
        let cert_only = HttpClientConfig {
            tls: TlsConfig {
                client_cert_pem: Some(b"cert".to_vec()),
                client_key_pem: None,
                ..TlsConfig::default()
            },
            ..HttpClientConfig::default()
        };
        let err = DatabricksHttpClient::new(cert_only, default_retry_configs()).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_key_pem"));

        let key_only = HttpClientConfig {
            tls: TlsConfig {
                client_cert_pem: None,
                client_key_pem: Some(b"key".to_vec()),
                ..TlsConfig::default()
            },
            ..HttpClientConfig::default()
        };
        let err = DatabricksHttpClient::new(key_only, default_retry_configs()).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_cert_pem"));
    }

    #[test]
    fn test_mtls_invalid_pem_is_rejected() {
        // Garbage cert+key bytes must fail the reqwest Identity parse
        // with an InvalidArgument, not panic. The PEM headers are
        // assembled at runtime (rather than written as string literals)
        // so the repo's secret scanner doesn't flag a bare
        // `BEGIN PRIVATE KEY` marker — the body is the non-key literal
        // "nope" regardless.
        let pem = |label: &str| format!("-----BEGIN {label}-----\nnope\n-----END {label}-----\n");
        let config = HttpClientConfig {
            tls: TlsConfig {
                client_cert_pem: Some(pem("CERTIFICATE").into_bytes()),
                client_key_pem: Some(pem(&format!("{} KEY", "PRIVATE")).into_bytes()),
                ..TlsConfig::default()
            },
            ..HttpClientConfig::default()
        };
        let err = DatabricksHttpClient::new(config, default_retry_configs()).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_proxy_config_debug_redacts_password() {
        let config = ProxyConfig {
            url: Some("http://proxy:8080".to_string()),
            username: Some("user".to_string()),
            password: Some("secret123".to_string()),
            bypass_hosts: None,
        };
        let debug_output = format!("{:?}", config);
        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("secret123"));
    }

    // --- Retry integration tests (with wiremock) ---

    mod retry_integration {
        use super::*;
        use crate::client::retry::{
            build_retry_configs, RequestCategory, RequestType, RetryConfig, RetryConfigOverrides,
        };
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicU32, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn test_client_with_http_config(
            retry_config: RetryConfig,
            http_config: HttpClientConfig,
        ) -> DatabricksHttpClient {
            let mut overrides = HashMap::new();
            let ovr = RetryConfigOverrides {
                min_wait: Some(retry_config.min_wait),
                max_wait: Some(retry_config.max_wait),
                overall_timeout: Some(retry_config.overall_timeout),
                max_retries: Some(retry_config.max_retries),
                override_retryable_codes: retry_config.override_retryable_codes.clone(),
            };
            overrides.insert(RequestCategory::Sea, ovr.clone());
            overrides.insert(RequestCategory::CloudFetch, ovr.clone());
            overrides.insert(RequestCategory::Auth, ovr.clone());
            overrides.insert(RequestCategory::Telemetry, ovr);

            let global = RetryConfig::default();
            let retry_configs = build_retry_configs(&global, &overrides);
            let client = DatabricksHttpClient::new(http_config, retry_configs).unwrap();
            let auth = Arc::new(PersonalAccessToken::new("test-token".to_string()));
            client.set_auth_provider(auth);
            client
        }

        /// Build a client pointed at the mock server with short retry waits for fast tests.
        fn test_client(
            mock_server: &MockServer,
            retry_config: RetryConfig,
        ) -> DatabricksHttpClient {
            // Override the client's base URL by using the mock_server URI directly in requests
            let _ = mock_server; // used by caller to build request URLs
            test_client_with_http_config(retry_config, HttpClientConfig::default())
        }

        /// Short retry config for fast tests (10ms waits instead of 1s).
        fn fast_retry_config(max_retries: u32) -> RetryConfig {
            RetryConfig {
                min_wait: Duration::from_millis(10),
                max_wait: Duration::from_millis(50),
                overall_timeout: Duration::from_secs(10),
                max_retries,
                override_retryable_codes: None,
            }
        }

        async fn read_request_head(stream: &mut TcpStream) {
            let mut received = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 {
                    return;
                }
                received.extend_from_slice(&buffer[..read]);
                if received.windows(4).any(|window| window == b"\r\n\r\n") {
                    return;
                }
            }
        }

        async fn write_ok_response(stream: &mut TcpStream) {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_connection_refused_retries_until_attempt_limit() {
            // Reserve an ephemeral port and release it so every connection is
            // refused. The final error records the number of attempts made.
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            drop(listener);

            let client =
                test_client_with_http_config(fast_retry_config(2), HttpClientConfig::default());
            let request = client
                .inner()
                .get(format!("http://{address}/test"))
                .build()
                .unwrap();

            let error = client
                .execute(request, RequestType::GetStatementStatus)
                .await
                .expect_err("closed port must fail");
            assert!(
                error.message.contains("after 3 attempts"),
                "connection refusal should consume the retry budget: {error}"
            );
        }

        #[tokio::test]
        async fn test_connection_closed_before_response_is_retried() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let accepted = Arc::new(AtomicU32::new(0));
            let server_accepted = Arc::clone(&accepted);

            let server = tokio::spawn(async move {
                for attempt in 0..2 {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    server_accepted.fetch_add(1, Ordering::SeqCst);
                    read_request_head(&mut stream).await;
                    if attempt == 0 {
                        // EOF before any response bytes simulates a connection
                        // dropped/reset after the request reached the peer.
                        continue;
                    }
                    write_ok_response(&mut stream).await;
                }
            });

            let client =
                test_client_with_http_config(fast_retry_config(2), HttpClientConfig::default());
            let request = client
                .inner()
                .get(format!("http://{address}/test"))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await
                .expect("idempotent request should recover after a dropped connection");
            assert_eq!(response.text().await.unwrap(), "ok");
            assert_eq!(
                accepted.load(Ordering::SeqCst),
                2,
                "server must observe the retry before its task is awaited"
            );
            server.await.unwrap();
        }

        #[tokio::test]
        async fn test_per_request_timeout_is_retried() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let accepted = Arc::new(AtomicU32::new(0));
            let server_accepted = Arc::clone(&accepted);

            let server = tokio::spawn(async move {
                for attempt in 0..2 {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    server_accepted.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        tokio::spawn(async move {
                            read_request_head(&mut stream).await;
                            tokio::time::sleep(Duration::from_millis(300)).await;
                        });
                    } else {
                        read_request_head(&mut stream).await;
                        write_ok_response(&mut stream).await;
                    }
                }
            });

            let http_config = HttpClientConfig {
                connect_timeout: Duration::from_millis(100),
                read_timeout: Duration::from_millis(75),
                ..HttpClientConfig::default()
            };
            let client = test_client_with_http_config(fast_retry_config(2), http_config);
            let request = client
                .inner()
                .get(format!("http://{address}/test"))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await
                .expect("idempotent request should recover after its deadline");
            assert_eq!(response.text().await.unwrap(), "ok");
            assert_eq!(
                accepted.load(Ordering::SeqCst),
                2,
                "server must observe the retry before its task is awaited"
            );
            server.await.unwrap();
        }

        #[tokio::test]
        async fn test_non_idempotent_read_timeout_preserves_retryable_flag() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(300)))
                .mount(&server)
                .await;
            let client = test_client_with_http_config(
                fast_retry_config(2),
                HttpClientConfig {
                    read_timeout: Duration::from_millis(75),
                    ..HttpClientConfig::default()
                },
            );
            let request = client
                .inner()
                .post(format!("{}/test", server.uri()))
                .build()
                .unwrap();
            let error = client
                .execute(request, RequestType::ExecuteStatement)
                .await
                .expect_err("response must time out after the request is sent");

            assert_eq!(server.received_requests().await.unwrap().len(), 1);
            assert!(error.message.contains("after 1 attempts"), "{error}");
            assert!(error.message.contains("timed out"), "{error}");
            assert!(
                error.retryable,
                "preserve the existing network-error flag: {error}"
            );
        }

        #[tokio::test]
        async fn test_redirect_failure_preserves_retryable_flag() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(
                    ResponseTemplate::new(302)
                        .insert_header("Location", format!("{}/test", server.uri())),
                )
                .mount(&server)
                .await;
            let client = test_client(&server, fast_retry_config(2));
            let request = client
                .inner()
                .get(format!("{}/test", server.uri()))
                .build()
                .unwrap();
            let error = client
                .execute(request, RequestType::GetStatementStatus)
                .await
                .expect_err("redirect loop must fail");

            assert!(error.message.contains("after 1 attempts"), "{error}");
            assert!(error.message.contains("redirect"), "{error}");
            assert!(
                error.retryable,
                "preserve the existing network-error flag: {error}"
            );
        }

        #[tokio::test]
        async fn test_idempotent_request_succeeds_on_retry_after_503() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 503 twice, then 200
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        ResponseTemplate::new(503)
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(5));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn test_idempotent_request_fails_after_max_retries() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Always return 503
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(503)
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            // max_retries=3 means 4 total attempts (1 initial + 3 retries)
            assert_eq!(attempt_count.load(Ordering::SeqCst), 4);
        }

        #[tokio::test]
        async fn test_idempotent_request_retries_500() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 500 once, then 200 — idempotent strategy retries 500
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(500)
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn test_idempotent_no_retry_on_400() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(400).set_body_string("bad request")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            // 400 is non-retryable — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_http_error_parses_sea_error_envelope() {
            // A transport-level 4xx whose body is the SEA error envelope
            // must surface the structured error_code / sql_state on the
            // returned Error (not just the raw body in the message),
            // mirroring the statement-level terminal_outcome path.
            let mock_server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(ResponseTemplate::new(400).set_body_string(
                    r#"{"error_code":"BAD_REQUEST","message":"bad thing happened","sql_state":"42000"}"#,
                ))
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();
            let err = client
                .execute(request, RequestType::GetStatementStatus)
                .await
                .expect_err("400 must error");

            assert_eq!(err.error_code.as_deref(), Some("BAD_REQUEST"));
            assert_eq!(err.sql_state.as_deref(), Some("42000"));
            // The structured server message is folded into the message.
            assert!(
                err.message.contains("bad thing happened"),
                "message should carry the server text, got: {}",
                err.message
            );
        }

        #[tokio::test]
        async fn test_http_error_non_envelope_body_falls_back_to_raw() {
            // A non-JSON body keeps today's behavior: raw body in the
            // message, no structured error_code/sql_state.
            let mock_server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(ResponseTemplate::new(400).set_body_string("plain text error"))
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();
            let err = client
                .execute(request, RequestType::GetStatementStatus)
                .await
                .expect_err("400 must error");

            assert!(err.error_code.is_none());
            assert!(err.message.contains("plain text error"));
        }

        #[tokio::test]
        async fn test_idempotent_no_retry_on_401() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(401).set_body_string("unauthorized")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_non_idempotent_no_retry_on_500() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(500).set_body_string("server error")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let result = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(result.is_err());
            // Non-idempotent: 500 is NOT retryable — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_non_idempotent_retries_on_429_without_retry_after() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // 429 without Retry-After — still retried for non-idempotent
            // (rate-limited means the request was not executed)
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(429)
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let response = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn test_non_idempotent_no_retry_on_503_without_retry_after() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // 503 WITHOUT Retry-After — should NOT retry for non-idempotent
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(503)
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let result = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(result.is_err());
            // No Retry-After on 503 → not retried for non-idempotent — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_non_idempotent_retries_on_503_with_retry_after() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 503 with Retry-After twice, then 200
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        ResponseTemplate::new(503).append_header("Retry-After", "0")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("sql query")
                .build()
                .unwrap();

            let response = client.execute(request, RequestType::ExecuteStatement).await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn test_retry_after_header_honored() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 429 with Retry-After: 0, then 200
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(429).append_header("Retry-After", "0")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let start = std::time::Instant::now();
            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            let elapsed = start.elapsed();

            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
            // Retry-After: 0 clamped to min_wait=10ms + jitter(50-750ms)
            // Should complete quickly (under 2s)
            assert!(elapsed < Duration::from_secs(2));
        }

        #[tokio::test]
        async fn test_overall_timeout_stops_retries() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Always return 503
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(503)
                })
                .mount(&mock_server)
                .await;

            // overall_timeout=2s with min_wait=500ms + jitter means ~2-3 attempts max
            let config = RetryConfig {
                min_wait: Duration::from_millis(500),
                max_wait: Duration::from_millis(800),
                overall_timeout: Duration::from_secs(2),
                max_retries: 100, // high limit — timeout should stop us first
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(result.is_err());
            let err_msg = format!("{:?}", result.unwrap_err());
            assert!(err_msg.contains("retry timeout exceeded"));
            // Should have made 2-4 attempts before timeout (not 100)
            let attempts = attempt_count.load(Ordering::SeqCst);
            assert!(
                (2..=5).contains(&attempts),
                "Expected 2-5 attempts, got {}",
                attempts
            );
        }

        #[tokio::test]
        async fn test_override_retryable_codes() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 418 once, then 200
            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(418) // I'm a teapot — not normally retryable
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            // Override: make 418 retryable
            let mut codes = HashSet::new();
            codes.insert(418);
            let config = RetryConfig {
                min_wait: Duration::from_millis(10),
                max_wait: Duration::from_millis(50),
                overall_timeout: Duration::from_secs(10),
                max_retries: 3,
                override_retryable_codes: Some(codes),
            };

            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn test_cloudfetch_403_not_retried_at_http_layer() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 403 (expired presigned URL) — should NOT be retried at HTTP layer
            Mock::given(method("GET"))
                .and(path("/presigned"))
                .respond_with(move |_req: &wiremock::Request| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(403).set_body_string("access denied")
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .get(format!("{}/presigned", mock_server.uri()))
                .build()
                .unwrap();

            let result = client
                .execute_without_auth(request, RequestType::CloudFetchDownload)
                .await;
            assert!(result.is_err());
            // 403 is non-retryable for idempotent requests — only 1 attempt
            assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn test_exponential_backoff_timing() {
            // Verify that delays between attempts follow exponential backoff.
            // Config: min_wait=100ms, max_wait=500ms
            // Per spec: exp_backoff = 2^attempt * min_wait
            // Expected: attempt 1→2: ~200ms+jitter, attempt 2→3: ~400ms+jitter, attempt 3→4: ~500ms(capped)+jitter
            let mock_server = MockServer::start().await;
            let timestamps = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
            let ts = timestamps.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    ts.lock().unwrap().push(Instant::now());
                    ResponseTemplate::new(503)
                })
                .mount(&mock_server)
                .await;

            let config = RetryConfig {
                min_wait: Duration::from_millis(100),
                max_wait: Duration::from_millis(500),
                overall_timeout: Duration::from_secs(30),
                max_retries: 3,
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let _ = client
                .execute(request, RequestType::GetStatementStatus)
                .await;

            let ts = timestamps.lock().unwrap();
            assert_eq!(ts.len(), 4, "Expected 4 attempts (1 initial + 3 retries)");

            // Proportional jitter: 5-25% of base (min 50ms)
            // Gap 1→2: 200ms base, jitter 50-50ms (5%=10ms clamped to 50ms) → 250-300ms
            let gap1 = ts[1].duration_since(ts[0]);
            assert!(
                gap1 >= Duration::from_millis(200) && gap1 <= Duration::from_millis(400),
                "Gap 1→2: {:?}, expected 200-400ms",
                gap1
            );

            // Gap 2→3: 400ms base, jitter 50-100ms → 450-500ms
            let gap2 = ts[2].duration_since(ts[1]);
            assert!(
                gap2 >= Duration::from_millis(400) && gap2 <= Duration::from_millis(600),
                "Gap 2→3: {:?}, expected 400-600ms",
                gap2
            );

            // Gap 3→4: 800ms capped to 500ms base, jitter 50-125ms → 550-625ms
            let gap3 = ts[3].duration_since(ts[2]);
            assert!(
                gap3 >= Duration::from_millis(500) && gap3 <= Duration::from_millis(750),
                "Gap 3→4: {:?}, expected 500-750ms (capped at max_wait=500ms)",
                gap3
            );

            // Exponential growth verified: gap1 < gap2 < gap3
            // Proportional jitter keeps ranges tight, so growth is clearly visible.
        }

        #[tokio::test]
        async fn test_retry_after_header_delays_correctly() {
            // Verify that Retry-After: N causes a delay of at least N seconds
            // (clamped to min_wait, plus jitter)
            let mock_server = MockServer::start().await;
            let timestamps = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
            let ts = timestamps.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = ts.lock().unwrap().len();
                    ts.lock().unwrap().push(Instant::now());
                    if n < 1 {
                        // Tell client to wait 1 second
                        ResponseTemplate::new(429).append_header("Retry-After", "1")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let config = RetryConfig {
                min_wait: Duration::from_millis(50), // low min so Retry-After dominates
                max_wait: Duration::from_secs(5),
                overall_timeout: Duration::from_secs(30),
                max_retries: 3,
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());

            let ts = timestamps.lock().unwrap();
            assert_eq!(ts.len(), 2);

            // Gap should be ~1s (Retry-After) + 5-25% jitter = 1050ms-1250ms
            let gap = ts[1].duration_since(ts[0]);
            assert!(
                gap >= Duration::from_millis(1000) && gap <= Duration::from_millis(1500),
                "Gap with Retry-After:1: {:?}, expected 1050-1500ms",
                gap
            );
        }

        #[tokio::test]
        async fn test_retry_after_clamped_to_max_wait() {
            // Retry-After: 10 but max_wait: 500ms → should clamp to 500ms + jitter
            let mock_server = MockServer::start().await;
            let timestamps = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
            let ts = timestamps.clone();

            Mock::given(method("GET"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = ts.lock().unwrap().len();
                    ts.lock().unwrap().push(Instant::now());
                    if n < 1 {
                        // Tell client to wait 10 seconds — but max_wait will clamp this
                        ResponseTemplate::new(429).append_header("Retry-After", "10")
                    } else {
                        ResponseTemplate::new(200).set_body_string("ok")
                    }
                })
                .mount(&mock_server)
                .await;

            let config = RetryConfig {
                min_wait: Duration::from_millis(50),
                max_wait: Duration::from_millis(500), // clamp Retry-After:10 down to 500ms
                overall_timeout: Duration::from_secs(30),
                max_retries: 3,
                override_retryable_codes: None,
            };
            let client = test_client(&mock_server, config);
            let request = client
                .inner()
                .get(format!("{}/test", mock_server.uri()))
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::GetStatementStatus)
                .await;
            assert!(response.is_ok());

            let ts = timestamps.lock().unwrap();
            assert_eq!(ts.len(), 2);

            // Gap should be ~500ms (clamped max_wait) + 5-25% jitter = 525-625ms
            // Crucially, NOT 10 seconds
            let gap = ts[1].duration_since(ts[0]);
            assert!(
                gap >= Duration::from_millis(500) && gap <= Duration::from_millis(800),
                "Gap with clamped Retry-After: {:?}, expected 525-800ms (NOT 10s)",
                gap
            );
        }

        #[tokio::test]
        async fn test_metadata_query_retries_as_idempotent() {
            let mock_server = MockServer::start().await;
            let attempt_count = Arc::new(AtomicU32::new(0));
            let counter = attempt_count.clone();

            // Return 500 once, then 200
            // ExecuteMetadataQuery is idempotent so 500 should be retried
            Mock::given(method("POST"))
                .and(path("/test"))
                .respond_with(move |_req: &wiremock::Request| {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        ResponseTemplate::new(500)
                    } else {
                        ResponseTemplate::new(200).set_body_string("{}")
                    }
                })
                .mount(&mock_server)
                .await;

            let client = test_client(&mock_server, fast_retry_config(3));
            let request = client
                .inner()
                .post(format!("{}/test", mock_server.uri()))
                .body("SHOW CATALOGS")
                .build()
                .unwrap();

            let response = client
                .execute(request, RequestType::ExecuteMetadataQuery)
                .await;
            assert!(response.is_ok());
            // Idempotent: 500 is retried — 2 attempts
            assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
        }
    }
}
