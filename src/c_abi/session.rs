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

//! Session FFI — config marshalling, open / close / new_statement.
//!
//! Auth over the C ABI: **PAT, OAuth M2M (shared-secret and JWT
//! private-key), OAuth U2M** (interactive browser flow), and **Azure Entra
//! service-principal M2M** are all exposed (`set_auth_pat` / `set_auth_m2m` /
//! `set_auth_m2m_jwt` / `set_auth_u2m` / `set_auth_azure_sp`). A host may also
//! drive its own token flow and inject it via PAT.
//!
//! Transaction control (autocommit / commit / rollback) is exposed via
//! `kernel_session_set_autocommit` / `_get_autocommit` / `_commit` /
//! `_rollback`, gated by `kernel_session_config_set_ignore_transactions`
//! (default = ignore). Backed by the shared `Session` transaction methods.

use super::{block_on_ffi, KernelStatusCode};
use crate::config::{
    AuthConfig, ConnectionConfig, HttpConfig, ProxyConfig, ResultConfig, TelemetryConfig,
    TlsConfig, TokenCacheConfig,
};
use crate::kernel_error::Error;
use crate::session::Session;
use crate::telemetry::DriverSystemConfiguration;
use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::time::Duration;

/// Opaque session handle. Boxed [`Session`] on the Rust side.
#[repr(C)]
pub struct kernel_session_t {
    _private: [u8; 0],
}

/// How the caller chose to identify the warehouse.
enum ConnectionSpec {
    /// `host` + bare `warehouse_id`.
    HostWarehouse { host: String, warehouse_id: String },
    /// `host` + JDBC-style `http_path` (parsed for warehouse + org id).
    HttpPath { host: String, http_path: String },
}

/// Auth choice captured from the C-side setters. The C ABI wires PAT,
/// OAuth M2M (shared-secret and JWT private-key), OAuth U2M, and Azure Entra
/// service-principal M2M; other [`AuthConfig`] variants stay Rust-crate-only.
///
/// NOTE (secret handling): `token` / `client_secret` / `jwt_passphrase`
/// are copied out of the C string into owned `String`s and are not
/// zeroized on drop, so plaintext lingers in freed heap. This matches the
/// rest of the crate (no `zeroize`/`secrecy` anywhere); when the crate
/// adopts a zeroize-on-drop policy, these copies must be included.
enum AuthSpec {
    Pat {
        token: String,
    },
    OAuthM2M {
        client_id: String,
        client_secret: String,
    },
    /// OAuth M2M via a JWT private-key client assertion. `client_id`,
    /// `jwt_key_file`, and `jwt_kid` are required; `jwt_passphrase` (for an
    /// encrypted PKCS#8 key), `jwt_algorithm` (`None` ⇒ `RS256`), `scopes`
    /// (comma-separated; `None` ⇒ kernel default `all-apis`), and
    /// `token_url` (`None` ⇒ OIDC discovery) are optional.
    OAuthM2MJwt {
        client_id: String,
        jwt_key_file: String,
        jwt_kid: String,
        jwt_passphrase: Option<String>,
        jwt_algorithm: Option<String>,
        scopes: Option<String>,
        token_url: Option<String>,
    },
    /// OAuth user-to-machine (authorization code + PKCE, browser flow).
    /// All fields optional — the kernel fills `databricks-sql-connector` /
    /// port 8030 / `sql offline_access` when omitted.
    OAuthU2M {
        client_id: Option<String>,
        redirect_port: Option<u16>,
        /// Comma-separated scopes (split at build time). `None` ⇒ kernel
        /// default (`sql`, `offline_access`).
        scopes: Option<String>,
    },
    /// Azure Entra service-principal M2M. `azure_client_id` +
    /// `azure_client_secret` are required; `azure_tenant_id` (`None` ⇒
    /// kernel auto-discovers from the workspace `/aad/auth` redirect) and
    /// `azure_workspace_resource_id` (`None` ⇒ data token authenticates
    /// alone; `Some` ⇒ the kernel also sends the SP management token +
    /// resource-id header) are optional.
    AzureServicePrincipal {
        azure_client_id: String,
        azure_client_secret: String,
        azure_tenant_id: Option<String>,
        azure_workspace_resource_id: Option<String>,
    },
}

/// C-side session config builder state.
///
/// Constructed by [`kernel_session_config_new`], populated by the
/// `kernel_session_config_set_*` setters, and consumed by
/// [`kernel_session_open`]. Heap-boxed; C only ever holds a
/// `*mut KernelSessionConfig` to an incomplete type and never reads the
/// fields — so this is a private Rust-side builder box (like the boxed
/// `Session` / `Statement` handles) and is intentionally **not**
/// `#[repr(C)]`: it holds non-FFI-safe fields (`Option`, `HashMap`) and
/// a `#[repr(C)]` annotation would imply a stable C layout that neither
/// exists nor is needed.
pub struct KernelSessionConfig {
    connection: Option<ConnectionSpec>,
    auth: Option<AuthSpec>,
    session_conf: HashMap<String, String>,
    /// Programmatic HTTP proxy (populated by `..._set_proxy`). `None` ⇒
    /// reqwest's env-var proxy behaviour.
    proxy: Option<ProxySpec>,
    /// TLS relaxations / custom trust (populated by the `..._set_tls_*`
    /// setters). Defaults to the kernel's strict validation.
    tls: TlsSpec,
    /// Extra HTTP headers sent on every request (accumulated by
    /// `..._set_custom_header`, one `(name, value)` pair per call).
    custom_headers: Vec<(String, String)>,
    /// HTTP retry / backoff knobs (populated by `..._set_retry_config`).
    /// `None` keeps the kernel default policy (exponential backoff with
    /// jitter; see [`HttpConfig::default`] for the concrete values); `Some`
    /// lets a host make its own backoff/attempt policy authoritative (a
    /// JDBC-style `RetryWaitMin` / `RetryWaitMax` / `RetryMax` surface).
    retry: Option<RetrySpec>,
    /// Whether transaction control is ignored (no-oped). Defaults to `true`
    /// (matching the reference/JDBC `IgnoreTransactions=1`); a host opts into
    /// real transactions via `kernel_session_config_set_ignore_transactions`
    /// with `false`. Maps to [`DefaultOpts::ignore_transactions`].
    ignore_transactions: bool,
    /// SP-wide WIF client id applied to the selected auth mode.
    identity_federation_client_id: Option<String>,
    /// OAuth token-endpoint override (populated by
    /// `kernel_session_config_set_oauth_token_endpoint`). Applied as
    /// `token_url_override` to the selected OAuth mode (M2M or U2M) at build
    /// time; lets a host point the token exchange at a non-workspace endpoint
    /// (e.g. an Azure/Entra token endpoint for an Azure service principal).
    /// `None` ⇒ OIDC discovery against the workspace host.
    oauth_token_endpoint: Option<String>,
    /// Comma-separated OAuth scopes override (populated by
    /// `kernel_session_config_set_oauth_scopes`, split at build time), applied
    /// to whichever OAuth mode (M2M or U2M) is selected — matching the single
    /// `oauth_scopes` the pyo3 / napi bindings expose across both flows. `None`
    /// ⇒ kernel per-mode default (`all-apis` for M2M; `sql offline_access` for
    /// U2M). For U2M it takes precedence over scopes passed positionally to
    /// `kernel_session_config_set_auth_u2m`.
    oauth_scopes: Option<String>,
    /// On-disk U2M token-cache policy (populated by
    /// `kernel_session_config_set_u2m_token_cache_config`). Defaults to enabled with
    /// no passphrase, in which case the cache is encrypted under a
    /// machine-local derived key. Only U2M auth reads it — PAT and M2M never
    /// touch the cache. See [`TokenCacheConfig`].
    token_cache: TokenCacheConfig,
    /// Client-side result-management knobs (CloudFetch / inline streaming
    /// tuning, flat-text rendering). Populated by
    /// `kernel_session_config_set_client_conf`; defaults to
    /// [`ResultConfig::default`]. These are **client-side** behaviours
    /// folded into the kernel's `ResultConfig` at `open()` — they never
    /// reach the SEA wire (unlike [`Self::session_conf`]). See
    /// [`ResultConfig`].
    result: ResultConfig,
    /// Telemetry collection/export knobs. Defaults to disabled; populated by
    /// `kernel_session_config_set_telemetry_config`.
    telemetry: TelemetryConfig,
    /// Driver/runtime/system identity supplied by the binding layer. `None`
    /// lets the kernel use only its own defaults; `Some` lets the kernel fill
    /// any omitted fields it knows how to derive.
    driver_system_configuration: Option<DriverSystemConfiguration>,
}

/// C-side proxy config captured from `kernel_session_config_set_proxy`.
struct ProxySpec {
    url: String,
    username: Option<String>,
    password: Option<String>,
    bypass_hosts: Option<String>,
}

/// C-side HTTP retry config captured from
/// `kernel_session_config_set_retry_config`. The JDBC-style backoff/attempt
/// knobs a host tunes, plus the cumulative retry budget (`overall_timeout`);
/// every other `HttpConfig` field (per-request timeout, pool size) keeps its
/// default when a retry spec is set. `overall_timeout` is `None` when the host
/// left it 0, so [`HttpConfig::default`]'s budget is preserved.
struct RetrySpec {
    min_wait: Duration,
    max_wait: Duration,
    max_retries: u32,
    overall_timeout: Option<Duration>,
}

/// C-side TLS config captured from the `..._set_tls_*` setters.
#[derive(Default)]
struct TlsSpec {
    /// PEM-encoded CA bundle bytes (from a file the ODBC layer read).
    custom_ca_cert: Option<Vec<u8>>,
    accept_self_signed: bool,
    skip_hostname_verification: bool,
}

/// Read a required C string argument into an owned `String`, mapping a
/// null or non-UTF-8 pointer to a typed `InvalidArgument` error.
///
/// # Safety
/// `ptr`, if non-null, must point to a NUL-terminated C string valid
/// for the duration of the call.
unsafe fn required_str(ptr: *const c_char, what: &str) -> Result<String, Error> {
    if ptr.is_null() {
        return Err(Error::invalid_argument().message(format!("{what} must not be null")));
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map(|s| s.to_owned())
        .map_err(|_| Error::invalid_argument().message(format!("{what} is not valid UTF-8")))
}

/// Allocate a fresh, empty session config. Returns
/// `KernelStatusCode::Success` and writes an owned `*mut
/// KernelSessionConfig` to `*out` that the caller frees via
/// [`kernel_session_config_free`] (or hands to [`kernel_session_open`],
/// which consumes and frees it).
///
/// # Safety
/// `out` must be a valid, writable pointer.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_new(
    out: *mut *mut KernelSessionConfig,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    if out.is_null() {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_session_config_new: out must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    let config = Box::new(KernelSessionConfig {
        connection: None,
        auth: None,
        session_conf: HashMap::new(),
        proxy: None,
        tls: TlsSpec::default(),
        custom_headers: Vec::new(),
        retry: None,
        // Default to ignoring transactions, matching IgnoreTransactions=1.
        ignore_transactions: true,
        identity_federation_client_id: None,
        oauth_token_endpoint: None,
        oauth_scopes: None,
        // Match the reference driver's / JDBC's EnableTokenCache=1.
        token_cache: TokenCacheConfig::default(),
        // Kernel result defaults (CloudFetch enabled, Arrow-native types).
        result: ResultConfig::default(),
        telemetry: TelemetryConfig::default(),
        driver_system_configuration: None,
    });
    std::ptr::write(out, Box::into_raw(config));
    KernelStatusCode::Success
}

/// Free a session config without opening a session. Only needed if the
/// caller allocated a config but never passed it to
/// [`kernel_session_open`] (which consumes the config).
///
/// # Safety
/// `config` must have been returned by [`kernel_session_config_new`] and
/// not previously freed or consumed by [`kernel_session_open`].
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_free(config: *mut KernelSessionConfig) {
    if !config.is_null() {
        drop(Box::from_raw(config));
    }
}

/// Set the workspace host and bare warehouse id. Mutually exclusive with
/// [`kernel_session_config_set_http_path`] — the last one called wins.
///
/// # Safety
/// `config` must be a live config handle; `host` / `warehouse_id` must
/// be NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_warehouse(
    config: *mut KernelSessionConfig,
    host: *const c_char,
    warehouse_id: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.connection = Some(ConnectionSpec::HostWarehouse {
            host: required_str(host, "host")?,
            warehouse_id: required_str(warehouse_id, "warehouse_id")?,
        });
        Ok(())
    })
}

/// Set the workspace host and a JDBC-style HTTP path
/// (`/sql/1.0/warehouses/{id}` with an optional `?o={org_id}`).
/// Mutually exclusive with [`kernel_session_config_set_warehouse`] — the
/// last one called wins. The path is parsed at [`kernel_session_open`]
/// so a malformed path surfaces there as `InvalidArgument`.
///
/// # Safety
/// `config` must be a live config handle; `host` / `http_path` must be
/// NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_http_path(
    config: *mut KernelSessionConfig,
    host: *const c_char,
    http_path: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.connection = Some(ConnectionSpec::HttpPath {
            host: required_str(host, "host")?,
            http_path: required_str(http_path, "http_path")?,
        });
        Ok(())
    })
}

/// Set PAT (personal access token) authentication. Mutually exclusive
/// with [`kernel_session_config_set_auth_m2m`] — the last one called
/// wins.
///
/// # Safety
/// `config` must be a live config handle; `token` must be a
/// NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_auth_pat(
    config: *mut KernelSessionConfig,
    token: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.auth = Some(AuthSpec::Pat {
            token: required_str(token, "token")?,
        });
        Ok(())
    })
}

/// Set OAuth M2M (client-credentials / service-principal)
/// authentication. Mutually exclusive with
/// [`kernel_session_config_set_auth_pat`] — the last one called wins.
///
/// # Safety
/// `config` must be a live config handle; `client_id` / `client_secret`
/// must be NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_auth_m2m(
    config: *mut KernelSessionConfig,
    client_id: *const c_char,
    client_secret: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.auth = Some(AuthSpec::OAuthM2M {
            client_id: required_str(client_id, "client_id")?,
            client_secret: required_str(client_secret, "client_secret")?,
        });
        Ok(())
    })
}

/// Set OAuth M2M authentication via a JWT private-key client assertion
/// (RFC 7523). Instead of a client secret, the kernel signs a short-lived
/// JWT with the configured private key and sends it as the
/// `client_assertion` in the client-credentials grant. Mutually exclusive
/// with the other `set_auth_*` setters — the last one called wins.
///
/// `client_id`, `jwt_key_file` (path to a PEM/PKCS#8 private key), and
/// `jwt_kid` (key id written into the JWT header) are required. The rest
/// are optional (pass NULL to omit): `jwt_passphrase` (for an encrypted
/// PKCS#8 key), `jwt_algorithm` (`RS256`/`384`/`512`, `PS256`/`384`/`512`,
/// `ES256`, `ES384`; NULL → `RS256`), `scopes` (a comma-separated list;
/// NULL → the kernel default `all-apis`), and `token_url` (an explicit
/// token endpoint; NULL → discover via OIDC).
///
/// # Safety
/// `config` must be a live config handle; the required args must be
/// NUL-terminated C strings, and the optional args, if non-null, must be
/// NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_auth_m2m_jwt(
    config: *mut KernelSessionConfig,
    client_id: *const c_char,
    jwt_key_file: *const c_char,
    jwt_kid: *const c_char,
    jwt_passphrase: *const c_char,
    jwt_algorithm: *const c_char,
    scopes: *const c_char,
    token_url: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.auth = Some(AuthSpec::OAuthM2MJwt {
            client_id: required_str(client_id, "client_id")?,
            jwt_key_file: required_str(jwt_key_file, "jwt_key_file")?,
            jwt_kid: required_str(jwt_kid, "jwt_kid")?,
            jwt_passphrase: optional_str(jwt_passphrase, "jwt_passphrase")?,
            jwt_algorithm: optional_str(jwt_algorithm, "jwt_algorithm")?,
            scopes: optional_str(scopes, "scopes")?,
            token_url: optional_str(token_url, "token_url")?,
        });
        Ok(())
    })
}

/// Set OAuth U2M (user-to-machine: authorization code + PKCE, browser
/// flow) authentication. Mutually exclusive with the other `set_auth_*`
/// setters — the last one called wins.
///
/// All args are optional (pass NULL to omit): `client_id` defaults to the
/// `databricks-sql-connector` client, `redirect_port` to 8030, and `scopes`
/// (a comma-separated list) to the kernel default (`sql`,
/// `offline_access`). `offline_access` yields a refresh token, which the
/// kernel caches and refreshes transparently.
///
/// **Behaviour:** opening a session with U2M is interactive — at
/// `kernel_session_open` the kernel starts a localhost listener on the
/// redirect port and opens the user's browser to the workspace authorize
/// endpoint. Only use from an interactive context.
///
/// # Safety
/// `config` must be a live config handle; the optional args, if non-null,
/// must be NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_auth_u2m(
    config: *mut KernelSessionConfig,
    client_id: *const c_char,
    redirect_port: u16,
    scopes: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.auth = Some(AuthSpec::OAuthU2M {
            client_id: optional_str(client_id, "client_id")?,
            // 0 is the "unset" sentinel → kernel default (8030).
            redirect_port: (redirect_port != 0).then_some(redirect_port),
            scopes: optional_str(scopes, "scopes")?,
        });
        Ok(())
    })
}

/// Set Azure Entra service-principal M2M authentication. The kernel owns
/// Azure resolution: it builds the Entra token endpoint + `{app}/.default`
/// scope and auto-discovers the tenant from the workspace's `/aad/auth`
/// redirect when `azure_tenant_id` is omitted. Mutually exclusive with the
/// other `set_auth_*` setters — the last one called wins.
///
/// `azure_client_id` and `azure_client_secret` (the Entra app-registration
/// credentials) are required. The rest are optional (pass NULL to omit):
/// `azure_tenant_id` (NULL → auto-discover), and `azure_workspace_resource_id`
/// (the workspace's ARM resource id; when set, the kernel additionally sends
/// the Azure SP management token + `X-Databricks-Azure-Workspace-Resource-Id`
/// header, so a service principal with an Azure RBAC role but no workspace
/// membership can authenticate; NULL → the data token authenticates alone).
///
/// Azure AD U2M is not a separate setter — use
/// [`kernel_session_config_set_auth_u2m`], which runs the kernel's
/// workspace-federated browser flow (Azure Databricks workspaces support it).
///
/// # Safety
/// `config` must be a live config handle; `azure_client_id` /
/// `azure_client_secret` must be NUL-terminated C strings, and the optional
/// args, if non-null, must be NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_auth_azure_sp(
    config: *mut KernelSessionConfig,
    azure_client_id: *const c_char,
    azure_client_secret: *const c_char,
    azure_tenant_id: *const c_char,
    azure_workspace_resource_id: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.auth = Some(AuthSpec::AzureServicePrincipal {
            azure_client_id: required_str(azure_client_id, "azure_client_id")?,
            azure_client_secret: required_str(azure_client_secret, "azure_client_secret")?,
            azure_tenant_id: optional_str(azure_tenant_id, "azure_tenant_id")?,
            azure_workspace_resource_id: optional_str(
                azure_workspace_resource_id,
                "azure_workspace_resource_id",
            )?,
        });
        Ok(())
    })
}

/// Set the optional SP-wide Workload Identity Federation client id used by
/// mandatory token exchange. Leaving it unset selects BYOT / account-wide WIF.
///
/// # Safety
/// `config` must be live and `client_id` must be a NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_identity_federation_client_id(
    config: *mut KernelSessionConfig,
    client_id: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.identity_federation_client_id = Some(required_str(client_id, "client_id")?);
        Ok(())
    })
}

/// Set an OAuth token-endpoint override, applied as `token_url_override` to
/// whichever OAuth mode (M2M or U2M) is selected. Lets a host point the token
/// exchange at a non-workspace endpoint — e.g. the Azure/Entra token endpoint
/// `https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token` for an Azure
/// service principal. Leaving it unset uses OIDC discovery against the
/// workspace host. No-op for PAT auth.
///
/// # Safety
/// `config` must be live and `token_endpoint` must be a NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_oauth_token_endpoint(
    config: *mut KernelSessionConfig,
    token_endpoint: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.oauth_token_endpoint = Some(required_str(token_endpoint, "token_endpoint")?);
        Ok(())
    })
}

/// Set the OAuth scopes (comma-separated; split at build time), applied to
/// whichever OAuth mode (M2M or U2M) is selected — e.g.
/// `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default` for an Azure service
/// principal (M2M) or `sql,offline_access` (U2M). Mirrors the single
/// `oauth_scopes` the pyo3 / napi bindings expose across both flows. Leaving it
/// unset uses the kernel's per-mode default (`all-apis` for M2M; `sql
/// offline_access` for U2M). For U2M it takes precedence over any scopes passed
/// positionally to [`kernel_session_config_set_auth_u2m`].
///
/// # Safety
/// `config` must be live and `scopes` must be a NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_oauth_scopes(
    config: *mut KernelSessionConfig,
    scopes: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.oauth_scopes = Some(required_str(scopes, "scopes")?);
        Ok(())
    })
}

/// Append one HTTP header sent on every request (e.g. a partner-supplied
/// `X-…` header). Call once per header; order is preserved. Both `name`
/// and `value` are required. Maps to the kernel's `custom_headers`.
///
/// # Safety
/// `config` must be a live config handle; `name` / `value` must be
/// NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_custom_header(
    config: *mut KernelSessionConfig,
    name: *const c_char,
    value: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        let name = required_str(name, "header name")?;
        let value = required_str(value, "header value")?;
        c.custom_headers.push((name, value));
        Ok(())
    })
}

/// Configure the HTTP retry / backoff policy: the minimum and maximum
/// backoff wait between attempts (milliseconds), the maximum number of
/// retries after the initial attempt (so a request is tried at most
/// `max_retries + 1` times), and the cumulative retry budget
/// (`overall_timeout_ms`) across all attempts. Lets a host make its own
/// `RetryWaitMin` / `RetryWaitMax` / `RetryMax` / retry-timeout surface
/// authoritative instead of the kernel's built-in default policy (see
/// [`HttpConfig::default`] for the values that apply when this setter is not
/// called). `max_retries == 0` disables retries (single attempt); the backoff
/// between attempts is still exponential with jitter, clamped to
/// `[min_wait_ms, max_wait_ms]`.
///
/// `overall_timeout_ms == 0` keeps the kernel's default budget — pass a
/// non-zero value to override it (matching the pyo3/napi `retry_overall_timeout`
/// knob). There is no sentinel for "no budget": the retry budget is *always*
/// bounded, so a large `max_retries` is still capped by the (default or
/// overridden) budget. If exponential backoff sums past the budget before
/// `max_retries` is reached, the remaining attempts do not run — raise
/// `overall_timeout_ms` alongside a large `max_retries` if you want them all to
/// fire. In particular a non-zero `overall_timeout_ms` smaller than
/// `min_wait_ms` guarantees zero retries (the first backoff already exceeds the
/// budget); this is accepted but logged at `warn`.
///
/// `min_wait_ms == 0` is rejected as `InvalidArgument` (a zero floor is
/// genuinely invalid, not a correctable typo). A `max_wait_ms < min_wait_ms`
/// pair is *corrected* (max raised to min) with a `warn`, rather than rejected —
/// a backoff bound is a tuning hint and must not fail a host's connect. Maps to
/// `HttpConfig::retry_min_wait` / `retry_max_wait` / `retry_max_retries` /
/// `overall_timeout`.
///
/// # Safety
/// `config` must be a live config handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_retry_config(
    config: *mut KernelSessionConfig,
    min_wait_ms: u64,
    max_wait_ms: u64,
    max_retries: u32,
    overall_timeout_ms: u64,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        if min_wait_ms == 0 {
            return Err(Error::invalid_argument().message("retry min_wait_ms must be > 0"));
        }
        // A `max_wait_ms < min_wait_ms` pair is a tuning typo, not a fatal
        // config error: correct it (raise max to min) and warn, rather than
        // rejecting the whole config and failing the host's connect. A backoff
        // bound is a tuning hint and the ceiling is the driver's own, so this
        // matches the kernel's clamp-and-warn discipline (see
        // `build_retry_configs_for_session` / `calculate_backoff`) and the
        // reference ODBC driver's handling.
        let max_wait_ms = if max_wait_ms < min_wait_ms {
            crate::klog::warn!(
                min_wait_ms,
                max_wait_ms,
                "retry max_wait_ms is below min_wait_ms; raising max_wait_ms to \
                 min_wait_ms (a backoff bound is a tuning hint, not a hard failure)",
            );
            min_wait_ms
        } else {
            max_wait_ms
        };
        // A non-zero budget below the first backoff wait guarantees zero
        // retries (the client stops before the first sleep — src/client/http.rs).
        // Accept it (the single attempt is still coherent, and a valid-looking
        // config must not hard-fail a host's connect) but warn: the host has
        // asked for retries that can never fire.
        if overall_timeout_ms != 0 && overall_timeout_ms < min_wait_ms {
            crate::klog::warn!(
                overall_timeout_ms,
                min_wait_ms,
                max_retries,
                "retry overall_timeout_ms is below min_wait_ms; the retry budget is \
                 exhausted before the first backoff, so no retries will run",
            );
        }
        c.retry = Some(RetrySpec {
            min_wait: Duration::from_millis(min_wait_ms),
            max_wait: Duration::from_millis(max_wait_ms),
            // Clamp to `u32::MAX - 1` so the HTTP client's `max_retries + 1`
            // total-attempts computation (src/client/http.rs) can never
            // overflow — a debug build would panic and a release build would
            // wrap to 0, silently disabling all retries. This matches the
            // effective ceiling the napi/pyo3 bindings already enforce (they
            // derive the field via `saturating_sub(1)`); the C ABI is the only
            // path that could otherwise hand the client an unclamped u32.
            max_retries: max_retries.min(u32::MAX - 1),
            // 0 => keep the kernel default budget; any non-zero value overrides it.
            overall_timeout: (overall_timeout_ms != 0)
                .then(|| Duration::from_millis(overall_timeout_ms)),
        });
        Ok(())
    })
}

/// Configure telemetry collection and export.
///
/// Telemetry is disabled by default. Passing `enabled = true` records kernel
/// operation telemetry and exports it through the Databricks SQL driver
/// telemetry endpoint. Export failures are fail-open and never change user
/// operation results.
///
/// `batch_size`, `flush_interval_ms`, and `close_flush_timeout_ms` must be
/// greater than zero. `max_retries == 0` disables telemetry export retries.
/// `retry_delay_ms == 0` is accepted and means immediate retry.
///
/// # Safety
/// `config` must be a live config handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_telemetry_config(
    config: *mut KernelSessionConfig,
    enabled: bool,
    batch_size: usize,
    flush_interval_ms: u64,
    max_retries: u32,
    retry_delay_ms: u64,
    close_flush_timeout_ms: u64,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        if batch_size == 0 {
            return Err(Error::invalid_argument().message("telemetry batch_size must be > 0"));
        }
        if flush_interval_ms == 0 {
            return Err(
                Error::invalid_argument().message("telemetry flush_interval_ms must be > 0")
            );
        }
        if close_flush_timeout_ms == 0 {
            return Err(
                Error::invalid_argument().message("telemetry close_flush_timeout_ms must be > 0")
            );
        }

        c.telemetry = TelemetryConfig {
            enabled,
            batch_size,
            flush_interval: Duration::from_millis(flush_interval_ms),
            max_retries,
            retry_delay: Duration::from_millis(retry_delay_ms),
            close_flush_timeout: Duration::from_millis(close_flush_timeout_ms),
            ..Default::default()
        };
        Ok(())
    })
}

/// Configure telemetry circuit breaker behavior.
///
/// By default the circuit breaker is enabled with a threshold of 5 consecutive
/// export failures and a 60 second timeout. Use this setter after
/// `kernel_session_config_set_telemetry_config` to override only the circuit
/// breaker knobs.
///
/// `threshold` must be greater than zero when `enabled` is true.
/// `timeout_ms` must be greater than zero.
///
/// # Safety
/// `config` must be a live config handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_telemetry_circuit_breaker_config(
    config: *mut KernelSessionConfig,
    enabled: bool,
    threshold: u32,
    timeout_ms: u64,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        if enabled && threshold == 0 {
            return Err(Error::invalid_argument()
                .message("telemetry circuit breaker threshold must be > 0 when enabled"));
        }
        if timeout_ms == 0 {
            return Err(Error::invalid_argument()
                .message("telemetry circuit breaker timeout_ms must be > 0"));
        }

        c.telemetry.circuit_breaker_enabled = enabled;
        c.telemetry.circuit_breaker_threshold = threshold;
        c.telemetry.circuit_breaker_timeout = Duration::from_millis(timeout_ms);
        Ok(())
    })
}

/// Configure driver/runtime/system identity supplied by the binding layer.
///
/// Every string argument is optional: pass null for values the binding does
/// not know. The kernel fills any missing fields it can derive before emitting
/// telemetry. Calling this function with all-null fields explicitly records an
/// empty override, which is equivalent to not calling it for v0.
///
/// # Safety
/// `config` must be a live config handle. Non-null string pointers must point
/// to NUL-terminated UTF-8 strings valid for the duration of the call.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_driver_system_configuration(
    config: *mut KernelSessionConfig,
    driver_name: *const c_char,
    driver_version: *const c_char,
    runtime_name: *const c_char,
    runtime_version: *const c_char,
    runtime_vendor: *const c_char,
    os_name: *const c_char,
    os_version: *const c_char,
    os_arch: *const c_char,
    client_app_name: *const c_char,
    locale_name: *const c_char,
    char_set_encoding: *const c_char,
    process_name: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.driver_system_configuration = Some(DriverSystemConfiguration {
            driver_version: optional_str(driver_version, "driver_version")?,
            runtime_name: optional_str(runtime_name, "runtime_name")?,
            runtime_version: optional_str(runtime_version, "runtime_version")?,
            runtime_vendor: optional_str(runtime_vendor, "runtime_vendor")?,
            os_name: optional_str(os_name, "os_name")?,
            os_version: optional_str(os_version, "os_version")?,
            os_arch: optional_str(os_arch, "os_arch")?,
            driver_name: optional_str(driver_name, "driver_name")?,
            client_app_name: optional_str(client_app_name, "client_app_name")?,
            locale_name: optional_str(locale_name, "locale_name")?,
            char_set_encoding: optional_str(char_set_encoding, "char_set_encoding")?,
            process_name: optional_str(process_name, "process_name")?,
        });
        Ok(())
    })
}

/// Initialize kernel logging, **process-wide and once**. `level` is one
/// of "OFF"/"ERROR"/"WARN"/"INFO"/"DEBUG"/"TRACE" (NULL ⇒ honour
/// `RUST_LOG`, default `warn`); `file_path` is a log file (NULL ⇒ stderr).
///
/// Idempotent: the underlying `tracing` subscriber can only be installed
/// once per process, so the **first** call wins and later calls are
/// no-ops (a driver that opens many connections calls this each time with
/// the same target — only the first takes effect). Not tied to a session;
/// call it before/around `kernel_session_open`. Lets an ODBC/Go host route
/// the kernel's logs into the same file it writes its own logs to.
///
/// A non-UTF-8 `file_path` returns `InvalidArgument` (the requested path
/// can't be honoured). Otherwise returns `Success` when the FIRST call
/// installed logging against the requested destination (or was skipped for
/// level `OFF`), or `Internal` (with a stored last-error) when that first
/// install did not fully honour the request — the log file couldn't be
/// opened (logging fell back to stderr) or a global subscriber was already
/// installed (no kernel subscriber installed). Because init is
/// once-per-process, the Success/Internal result reflects the **first**
/// call's outcome, NOT a later call's `level`/`file_path` (which have no
/// effect). A caller that can't see stderr can use `Internal` to surface
/// the degradation.
///
/// # Safety
/// `level` / `file_path`, if non-null, must be NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_init_logging(
    level: *const c_char,
    file_path: *const c_char,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    // `level` is best-effort: a non-UTF-8 or unrecognized value falls back
    // to RUST_LOG / the default, so treat a bad level as absent.
    let level = optional_str(level, "log level").ok().flatten();
    // Level "OFF" means "do not install a subscriber", but that is still an
    // intentional, successful outcome. `init_logging` no longer special-cases
    // it (it works on full EnvFilter directives), so honour OFF here before
    // touching the file path.
    if level
        .as_deref()
        .is_some_and(|l| l.eq_ignore_ascii_case("off"))
    {
        return KernelStatusCode::Success;
    }
    // The C ABI takes a BARE level; expand it to the full directive the
    // shared installer expects (targeting the kernel's own log target).
    let filter = level.as_deref().map(crate::logging::filter_from_level);
    // `file_path` is NOT best-effort: a non-UTF-8 path is a real error (the
    // caller asked us to write there and we can't), so surface it rather
    // than silently dropping the path and reporting Success.
    let file = match optional_str(file_path, "log file path") {
        Ok(f) => f.map(std::path::PathBuf::from),
        Err(e) => return crate::c_abi::error::store_and_code(e),
    };
    if crate::logging::init_logging(&crate::logging::LogConfig { filter, file }) {
        KernelStatusCode::Success
    } else {
        crate::c_abi::error::store_and_code(Error::new(
            crate::kernel_error::ErrorCode::Internal,
            "kernel_init_logging: logging was not fully initialized as requested (the log file \
             could not be opened, or a global subscriber was already installed); kernel logs may \
             go to stderr or not flow",
        ))
    }
}

/// Add (or overwrite) one server-bound session-conf entry. Unknown keys
/// are filtered against the kernel allowlist at `open()` time, matching
/// the Rust-crate path.
///
/// # Safety
/// `config` must be a live config handle; `key` / `value` must be
/// NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_session_conf(
    config: *mut KernelSessionConfig,
    key: *const c_char,
    value: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        let key = required_str(key, "session_conf key")?;
        let value = required_str(value, "session_conf value")?;
        c.session_conf.insert(key, value);
        Ok(())
    })
}

/// Read an optional C string: null → `None`, else an owned `String`
/// (errors on non-UTF-8).
///
/// # Safety
/// `ptr`, if non-null, must be a NUL-terminated C string.
unsafe fn optional_str(ptr: *const c_char, what: &str) -> Result<Option<String>, Error> {
    if ptr.is_null() {
        return Ok(None);
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map(|s| Some(s.to_owned()))
        .map_err(|_| Error::invalid_argument().message(format!("{what} is not valid UTF-8")))
}

/// Configure an HTTP/HTTPS proxy. `url` is required (e.g.
/// `http://proxy:8080`); `username` / `password` / `bypass_hosts` are
/// optional (pass NULL to omit). Maps to the kernel's `ProxyConfig`.
///
/// # Safety
/// `config` must be a live config handle; `url` must be a NUL-terminated
/// C string; the optional args, if non-null, must be NUL-terminated C
/// strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_proxy(
    config: *mut KernelSessionConfig,
    url: *const c_char,
    username: *const c_char,
    password: *const c_char,
    bypass_hosts: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.proxy = Some(ProxySpec {
            url: required_str(url, "proxy url")?,
            username: optional_str(username, "proxy username")?,
            password: optional_str(password, "proxy password")?,
            bypass_hosts: optional_str(bypass_hosts, "proxy bypass_hosts")?,
        });
        Ok(())
    })
}

/// Add a PEM-encoded CA-certificate bundle (bytes) to the TLS trust store
/// on top of the system roots — for a corporate re-signing proxy or an
/// on-prem CA. Maps to `TlsConfig::custom_ca_cert`.
///
/// # Safety
/// `config` must be a live config handle; `pem` must point to `len`
/// readable bytes.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_tls_trusted_certs(
    config: *mut KernelSessionConfig,
    pem: *const u8,
    len: usize,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        if pem.is_null() {
            return Err(Error::invalid_argument().message("trusted certs pem must not be null"));
        }
        c.tls.custom_ca_cert = Some(std::slice::from_raw_parts(pem, len).to_vec());
        Ok(())
    })
}

/// Accept self-signed / otherwise-invalid server certificates (disables
/// chain validation). Dev/on-prem only. Maps to
/// `TlsConfig::accept_self_signed`.
///
/// # Safety
/// `config` must be a live config handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_tls_allow_self_signed(
    config: *mut KernelSessionConfig,
    allow: bool,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.tls.accept_self_signed = allow;
        Ok(())
    })
}

/// Skip the certificate hostname-vs-SNI check. Dev/lab only. Maps to
/// `TlsConfig::skip_hostname_verification`.
///
/// # Safety
/// `config` must be a live config handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_tls_skip_hostname_verification(
    config: *mut KernelSessionConfig,
    skip: bool,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.tls.skip_hostname_verification = skip;
        Ok(())
    })
}

/// Set whether transaction control is ignored (no-oped). Pass `true`
/// (the default) for `IgnoreTransactions=1` semantics — `SQLEndTran` /
/// commit / rollback / autocommit-off become silent no-ops. Pass `false`
/// to enable real multi-statement transactions (Private Preview; requires
/// catalog-managed tables server-side). Maps to
/// [`DefaultOpts::ignore_transactions`].
///
/// # Safety
/// `config` must be a live config handle.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_ignore_transactions(
    config: *mut KernelSessionConfig,
    ignore: bool,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.ignore_transactions = ignore;
        Ok(())
    })
}

/// Configure the on-disk OAuth token cache for U2M (browser) auth.
///
/// Mirrors the `EnableTokenCache` / `TokenCachePassPhrase` pair exposed by
/// the reference ODBC driver and JDBC:
///
/// * `enabled = true` (the default) persists the refresh token to
///   `~/.config/databricks-sql-kernel/oauth/` so the user is not sent
///   through the browser on every connection. The file is **always
///   encrypted** (AES-256-CBC, PBKDF2-HMAC-SHA256); there is no plaintext
///   mode. A non-null `passphrase` keys it; a null or blank one falls back
///   to a machine-local derived key, matching the reference driver, which
///   caches with an empty `TokenCachePassPhrase`. Supplying a passphrase is
///   stronger — see `TokenCacheConfig` for what the fallback does and does
///   not protect against.
/// * `enabled = false` disables **disk persistence only**. Tokens are
///   still reused and refreshed in memory for the session's lifetime, so
///   this does not re-trigger the browser flow on every refresh. Use it
///   when the process must not write credentials to disk at all.
///
/// `passphrase` is ignored when `enabled` is `false`. Only U2M auth touches
/// the cache; PAT and M2M ignore this setting entirely.
///
/// # Safety
/// `config` must be a live config handle. `passphrase`, if non-null, must
/// point to a NUL-terminated UTF-8 string valid for the duration of the
/// call.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_u2m_token_cache_config(
    config: *mut KernelSessionConfig,
    enabled: bool,
    passphrase: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        c.token_cache = TokenCacheConfig {
            enabled,
            passphrase: optional_str(passphrase, "passphrase")?,
        };
        Ok(())
    })
}

/// Set one **client-side** result knob by name (`key` = `value`).
///
/// This is the client-side counterpart to
/// `kernel_session_config_set_session_conf`: that one carries **server**
/// session confs (`ANSI_MODE`, `TIMEZONE`, …) to the SEA wire; this one
/// sets **client-side** knobs that tune how the kernel fetches, buffers,
/// and post-processes results and are **never sent to the server**. The two
/// channels are deliberately separate (see #196 / #226 — conflating them is
/// what made a client flag masquerade as a rejected server conf).
///
/// A key/value shape (rather than one typed setter per knob) is chosen so
/// the signature never changes as `ResultConfig` grows new knobs, and so a
/// host that already has DSN / connection-string attributes can forward
/// them in a loop. Safety is recovered at runtime: an unknown key or an
/// unparseable value is **rejected** with `InvalidArgument` (and a stored
/// error naming the problem), not silently ignored.
///
/// Recognised keys (matched case-insensitively), mapping to
/// [`ResultConfig`]:
/// - `cloudfetch_enabled` = `"true"` / `"false"` — `false` serves every
///   result inline (SEA `disposition = INLINE`), mirroring JDBC's
///   `EnableQueryResultDownload=0`.
/// - `cloudfetch_link_prefetch_window` = positive integer — presigned URLs
///   prefetched ahead of the consumer.
/// - `cloudfetch_max_chunks_in_memory` = positive integer — decompressed
///   CloudFetch chunks held at once (bounds peak RSS). Values above the
///   kernel ceiling (256) are **clamped** with a warning, not rejected.
/// - `inline_max_chunks_in_memory` = positive integer — inline-Arrow
///   prefetch window (used when `cloudfetch_enabled = false`).
/// - `complex_types_as_json` = `"true"` / `"false"` — render `ARRAY` /
///   `MAP` / `STRUCT` / `VARIANT` / `GEOMETRY` / `GEOGRAPHY` as JSON text
///   (PySQL `_use_arrow_native_complex_types=False`, JDBC
///   `complexDatatypeSupport=false`).
/// - `intervals_as_string` = `"true"` / `"false"` — render `INTERVAL` /
///   `DURATION` as canonical Databricks text (for pyarrow-Python bindings).
///
/// A server session-conf key passed here (e.g. `ANSI_MODE`) is rejected —
/// use `kernel_session_config_set_session_conf` for those.
///
/// # Safety
/// `config` must be a live config handle; `key` / `value` must be
/// NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_config_set_client_conf(
    config: *mut KernelSessionConfig,
    key: *const c_char,
    value: *const c_char,
) -> KernelStatusCode {
    set_on_config(config, |c| {
        let key = required_str(key, "client_conf key")?;
        let value = required_str(value, "client_conf value")?;
        c.result.apply_client_conf(&key, &value)
    })
}

/// Shared body for the config setters: null-check the handle, clear the
/// thread-local error, run `f` against the borrowed config, and store
/// any error.
///
/// # Safety
/// `config` must be a live `*mut KernelSessionConfig`.
unsafe fn set_on_config(
    config: *mut KernelSessionConfig,
    f: impl FnOnce(&mut KernelSessionConfig) -> Result<(), Error>,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(config) = config.as_mut() else {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("config handle must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    match f(config) {
        Ok(()) => KernelStatusCode::Success,
        Err(e) => {
            let code = KernelStatusCode::from(e.code);
            crate::c_abi::error::store_error(e);
            code
        }
    }
}

/// Open a session from a config produced by the
/// `kernel_session_config_*` surface.
///
/// **Consumes `config`**: on both success and failure the config box is
/// freed before returning, so the caller must not use or free `config`
/// afterwards. On success, `*out` holds an owned session handle the
/// caller releases with [`kernel_session_close`].
///
/// # Safety
/// `config` must have been returned by [`kernel_session_config_new`] and
/// not previously freed or consumed. `out` must be a valid, writable
/// pointer.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_open(
    config: *mut KernelSessionConfig,
    out: *mut *mut kernel_session_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    // The contract is "config is consumed on every path". Reclaim the box
    // up front (when non-null) so it is dropped even on the argument-guard
    // error paths below — otherwise a valid config + null out would leak
    // the box and its owned Strings (including any auth secret).
    let config = if config.is_null() {
        None
    } else {
        Some(Box::from_raw(config))
    };
    if config.is_none() || out.is_null() {
        // `config` (if Some) drops here, honouring the consume contract.
        crate::c_abi::error::store_error(
            Error::invalid_argument()
                .message("kernel_session_open: config and out must be non-null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    let config = config.expect("config is Some (checked above)");

    let session = match build_session(*config) {
        Ok(s) => s,
        Err(code) => return code, // error already stored by build_session
    };

    std::ptr::write(
        out,
        Box::into_raw(Box::new(session)) as *mut kernel_session_t,
    );
    KernelStatusCode::Success
}

/// Split a comma-separated OAuth scope string into a scope list. Trims
/// each entry and drops empties; an all-empty / blank input yields `None`
/// so the kernel applies its default scopes. E.g. `"a, ,b,"` → `["a","b"]`.
fn parse_scopes(s: &str) -> Option<Vec<String>> {
    let v: Vec<String> = s
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    (!v.is_empty()).then_some(v)
}

/// Fold the C-side custom headers and optional [`RetrySpec`] onto a fresh
/// [`HttpConfig`]. Both live on `HttpConfig`, so we start from defaults and set
/// only the fields a setter populated — every other HTTP default (per-request
/// timeout, pool size, and the retry knobs when no `RetrySpec` was set) is
/// preserved.
///
/// This is the code that actually configures live retries (called from
/// `build_session` whenever headers or a retry spec were set), so it is a pure
/// helper kept unit-testable without a live session.
fn merge_http_config(
    custom_headers: Vec<(String, String)>,
    retry: Option<RetrySpec>,
) -> HttpConfig {
    let mut http = HttpConfig {
        custom_headers,
        ..HttpConfig::default()
    };
    if let Some(r) = retry {
        http.retry_min_wait = r.min_wait;
        http.retry_max_wait = r.max_wait;
        http.retry_max_retries = r.max_retries;
        // Only override the cumulative budget when the host set it; otherwise
        // HttpConfig::default()'s budget is preserved.
        if let Some(t) = r.overall_timeout {
            http.overall_timeout = t;
        }
    }
    http
}

/// Assemble the kernel [`AuthConfig`] from the C-side [`AuthSpec`] plus the
/// cross-cutting OAuth overrides captured by the additive setters
/// (`set_oauth_token_endpoint` / `set_oauth_scopes`). Pure (no I/O), so the
/// wiring is unit-testable without opening a session.
///
/// `oauth_token_endpoint` and `oauth_scopes` both apply to whichever OAuth mode
/// is selected (M2M or U2M) — a single config carries exactly one mode, so one
/// shared knob each is unambiguous (matching the pyo3 / napi bindings, which
/// expose one `oauth_scopes` across both flows). For U2M, `oauth_scopes` takes
/// precedence over any scopes passed positionally to `set_auth_u2m` (kept for
/// back-compat). Both are ignored for PAT.
fn auth_spec_to_config(
    auth: AuthSpec,
    oauth_token_endpoint: Option<String>,
    oauth_scopes: Option<String>,
    identity_federation_client_id: Option<String>,
    token_cache: TokenCacheConfig,
) -> crate::kernel_error::Result<AuthConfig> {
    // `required_str` guards only `null`, so `set_oauth_token_endpoint(config, "")`
    // yields `Some("")`. Normalize a present-but-empty endpoint to `None` here so
    // the `.or(token_url)` fallbacks below survive (mirroring the scopes path,
    // which parses `Some("")` to `None`) instead of shadowing the positional
    // `token_url` with an empty endpoint that fails opaquely at `TokenUrl::new("")`.
    let oauth_token_endpoint = oauth_token_endpoint.filter(|s| !s.is_empty());
    let auth = match auth {
        AuthSpec::Pat { token } => AuthConfig::pat(Some(token), identity_federation_client_id)?,
        AuthSpec::OAuthM2M {
            client_id,
            client_secret,
        } => AuthConfig::oauth_m2m(
            Some(client_id),
            Some(client_secret),
            oauth_scopes.and_then(|s| parse_scopes(&s)),
            oauth_token_endpoint,
            identity_federation_client_id,
        )?,
        AuthSpec::OAuthM2MJwt {
            client_id,
            jwt_key_file,
            jwt_kid,
            jwt_passphrase,
            jwt_algorithm,
            scopes,
            token_url,
        } => AuthConfig::oauth_m2m_private_key(
            Some(client_id),
            Some(jwt_key_file),
            Some(jwt_kid),
            jwt_passphrase,
            jwt_algorithm,
            // The shared `set_oauth_scopes` wins; fall back to scopes passed
            // positionally to `set_auth_m2m_jwt` (same precedence as U2M below).
            oauth_scopes
                .and_then(|s| parse_scopes(&s))
                .or_else(|| scopes.and_then(|s| parse_scopes(&s))),
            // The shared `set_oauth_token_endpoint` wins; fall back to the
            // positional `token_url`.
            oauth_token_endpoint.or(token_url),
        )?,
        AuthSpec::OAuthU2M {
            client_id,
            redirect_port,
            scopes,
        } => AuthConfig::oauth_u2m(
            client_id,
            // The C ABI exposes a single redirect port; forward it as a
            // one-element candidate list (the kernel binds the first free
            // port, trivially that one). Keeps the FFI unchanged.
            redirect_port.map(|p| vec![p]),
            // The shared `set_oauth_scopes` wins; fall back to the scopes
            // passed positionally to `set_auth_u2m` (pre-existing path).
            // Parse each source *before* choosing precedence: a present-but-
            // empty shared value (`Some("")`) parses to `None`, so the
            // positional fallback is preserved instead of being shadowed.
            oauth_scopes
                .and_then(|s| parse_scopes(&s))
                .or_else(|| scopes.and_then(|s| parse_scopes(&s))),
            oauth_token_endpoint,
            None,
            Some(token_cache),
            identity_federation_client_id,
        ),
        AuthSpec::AzureServicePrincipal {
            azure_client_id,
            azure_client_secret,
            azure_tenant_id,
            azure_workspace_resource_id,
        } => AuthConfig::azure_service_principal(
            Some(azure_client_id),
            Some(azure_client_secret),
            azure_tenant_id,
            azure_workspace_resource_id,
            identity_federation_client_id,
        )?,
    };
    Ok(auth)
}

/// Translate the C-side config into a [`Session`], driving the async
/// `Session::open` on the process-wide runtime. Errors are stored in the
/// thread-local buffer; the returned `KernelStatusCode` is the mapped
/// code.
fn build_session(config: KernelSessionConfig) -> Result<Session, KernelStatusCode> {
    let KernelSessionConfig {
        connection,
        auth,
        session_conf,
        proxy,
        tls,
        custom_headers,
        retry,
        ignore_transactions,
        identity_federation_client_id,
        oauth_token_endpoint,
        oauth_scopes,
        token_cache,
        result,
        telemetry,
        driver_system_configuration,
    } = config;

    block_on_ffi(move || {
        Box::pin(async move {
            let connection = connection.ok_or_else(|| {
                Error::invalid_argument().message(
                    "session config requires a connection \
                     (set_warehouse or set_http_path)",
                )
            })?;
            let auth = auth.ok_or_else(|| {
                Error::invalid_argument().message(
                    "session config requires auth (set_auth_pat, set_auth_m2m, \
                     set_auth_m2m_jwt, or set_auth_u2m)",
                )
            })?;

            let connection = match connection {
                ConnectionSpec::HostWarehouse { host, warehouse_id } => {
                    ConnectionConfig::new(host, warehouse_id)
                }
                ConnectionSpec::HttpPath { host, http_path } => {
                    ConnectionConfig::from_http_path(host, &http_path)?
                }
            };

            let auth = auth_spec_to_config(
                auth,
                oauth_token_endpoint,
                oauth_scopes,
                identity_federation_client_id,
                token_cache,
            )?;

            let mut builder = Session::builder()
                .connection(connection)
                .auth(auth)
                .session_conf(session_conf)
                .result(result)
                .telemetry(telemetry)
                .defaults(crate::config::DefaultOpts {
                    ignore_transactions,
                    ..Default::default()
                });
            if let Some(system_configuration) = driver_system_configuration {
                builder = builder.driver_system_configuration(system_configuration);
            }

            // Skip `.http()` when neither headers nor a retry spec was set, so
            // the kernel's full `HttpConfig::default()` applies untouched.
            if !custom_headers.is_empty() || retry.is_some() {
                builder = builder.http(merge_http_config(custom_headers, retry));
            }

            // Programmatic proxy (env-var behaviour preserved when unset).
            if let Some(p) = proxy {
                builder = builder.proxy(ProxyConfig {
                    url: Some(p.url),
                    username: p.username,
                    password: p.password,
                    bypass_hosts: p.bypass_hosts,
                    ..ProxyConfig::default()
                });
            }

            // TLS relaxations / custom trust. Only build a TlsConfig when a
            // non-default knob was set, so the strict default is preserved
            // otherwise.
            if tls.custom_ca_cert.is_some()
                || tls.accept_self_signed
                || tls.skip_hostname_verification
            {
                builder = builder.tls(TlsConfig {
                    custom_ca_cert: tls.custom_ca_cert,
                    accept_self_signed: tls.accept_self_signed,
                    skip_hostname_verification: tls.skip_hostname_verification,
                    ..TlsConfig::default()
                });
            }

            builder.open().await
        })
    })
}

/// Close a session. Initiates the fire-and-forget server-side
/// `DeleteSession` immediately and releases the underlying Rust
/// [`Session`]. Does not block on the delete completing.
///
/// Close the session **last**: because the delete is initiated eagerly,
/// any still-open handle against this session — a statement / executed
/// handle, or a metadata result stream (which holds its own
/// `Arc<SessionInner>` and stays drivable) — must be drained and closed
/// before this call. Driving such a handle afterwards issues RPCs against
/// an already-deleting session and surfaces server errors. See the
/// `databricks_kernel.h` teardown-ordering contract.
///
/// # Safety
///
/// `session` must have been returned by [`kernel_session_open`] and not
/// previously closed. After this call, `session` is invalid; passing it
/// to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_close(session: *mut kernel_session_t) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    if session.is_null() {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_session_close: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    // Reclaim the box and initiate the fire-and-forget `DeleteSession`
    // right now. We call `initiate_delete()` explicitly rather than
    // relying on `Drop`: a still-live statement / executed handle holds
    // its own `Arc<SessionInner>` clone, so `SessionInner::Drop` (which
    // only fires when the *last* Arc drops) would otherwise defer the
    // server-side delete until those handles are released — contradicting
    // the header's "close initiates the delete" contract. `initiate_delete`
    // is idempotent, so the eventual `Drop` becomes a no-op. We still don't
    // `block_on` an awaited close — that would make close block on the
    // network for no caller benefit.
    let session = Box::from_raw(session as *mut Session);
    session.initiate_delete();
    drop(session);
    KernelStatusCode::Success
}

/// Report whether a session is still **open** (its server-side delete has
/// not been initiated). Writes `true`/`false` to `*out`.
///
/// This is a **local** check — it does NOT round-trip to the server, so it
/// will NOT detect a server-side idle timeout or a dropped network link.
/// It answers only "has this handle been closed/superseded". A host using
/// it to back `SQLGetConnectAttr(SQL_ATTR_CONNECTION_DEAD)` should treat a
/// `true` result as "not known-dead" rather than a positive liveness
/// guarantee. Does not borrow ownership; the caller still owns `session`.
///
/// # Safety
/// `session` must be a live handle from [`kernel_session_open`] (not yet
/// closed via [`kernel_session_close`]); `out` must be a writable `bool*`.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_is_open(
    session: *const kernel_session_t,
    out: *mut bool,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(session) = (session as *const Session).as_ref() else {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_session_is_open: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    if out.is_null() {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_session_is_open: out must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    std::ptr::write(out, session.is_open());
    KernelStatusCode::Success
}

/// Actively validate the session against the server by round-tripping a
/// lightweight `SELECT 1` test query (result discarded). This is the
/// I/O-performing complement of [`kernel_session_is_open`]: where that is
/// a local handle-state check, this reaches the server, so it DOES detect
/// a server-side idle timeout, a killed warehouse, or a dropped network
/// link.
///
/// Returns `Success` when the round-trip completes (the session is alive).
/// On failure it returns the mapped status code — a transport/availability
/// failure surfaces as `Unavailable` / `NetworkError` / `Timeout`, which a
/// host can treat as "connection dead" when backing
/// `SQL_ATTR_CONNECTION_DEAD`; the full typed error is retrievable via
/// `kernel_get_last_error`. Mirrors the reference / JDBC drivers'
/// connection-test probe.
///
/// Runs SQL, so — like the transaction-control entry points — it MUST be
/// invoked from a native (non-async-runtime) thread. Does not borrow
/// ownership; the caller still owns `session`.
///
/// # Safety
/// `session` must be a live handle from [`kernel_session_open`] (not yet
/// closed via [`kernel_session_close`]).
#[no_mangle]
pub unsafe extern "C" fn kernel_session_test(session: *const kernel_session_t) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(session) = (session as *const Session).as_ref() else {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_session_test: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    match block_on_ffi(|| session.test_connection()) {
        Ok(()) => KernelStatusCode::Success,
        Err(code) => code,
    }
}

/// Construct a new mutable statement bound to this session. On success,
/// `*out` holds an owned `kernel_statement_t` the caller releases with
/// `kernel_statement_close`.
///
/// # Safety
/// `session` must be a live session handle; `out` must be writable.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_new_statement(
    session: *mut kernel_session_t,
    out: *mut *mut super::statement::kernel_statement_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    if session.is_null() || out.is_null() {
        crate::c_abi::error::store_error(
            Error::invalid_argument()
                .message("kernel_session_new_statement: session and out must be non-null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    let session = &*(session as *const Session);
    let statement = session.statement();
    std::ptr::write(out, super::statement::into_raw_statement(statement));
    KernelStatusCode::Success
}

/// Set autocommit mode on the session. `enabled = false` begins explicit
/// transaction mode (subsequent statements run in a transaction until
/// `kernel_session_commit` / `kernel_session_rollback`); `true` returns to
/// auto-commit. Issues `SET AUTOCOMMIT = TRUE|FALSE` on the server.
///
/// A no-op returning `Success` when the session was configured with
/// `ignore_transactions = true` (the default). Must be called from a
/// native (non-async-runtime) thread.
///
/// # Safety
/// `session` must be a live session handle from [`kernel_session_open`].
#[no_mangle]
pub unsafe extern "C" fn kernel_session_set_autocommit(
    session: *mut kernel_session_t,
    enabled: bool,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(session) = (session as *const Session).as_ref() else {
        crate::c_abi::error::store_error(
            Error::invalid_argument()
                .message("kernel_session_set_autocommit: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    match block_on_ffi(|| session.set_autocommit(enabled)) {
        Ok(()) => KernelStatusCode::Success,
        Err(code) => code,
    }
}

/// Read the session's current autocommit mode into `*out`. Local read — no
/// server round-trip.
///
/// # Safety
/// `session` must be a live session handle; `out` must be a writable `bool*`.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_get_autocommit(
    session: *const kernel_session_t,
    out: *mut bool,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(session) = (session as *const Session).as_ref() else {
        crate::c_abi::error::store_error(
            Error::invalid_argument()
                .message("kernel_session_get_autocommit: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    if out.is_null() {
        crate::c_abi::error::store_error(
            Error::invalid_argument()
                .message("kernel_session_get_autocommit: out must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    std::ptr::write(out, session.autocommit());
    KernelStatusCode::Success
}

/// Commit the current transaction (issues `COMMIT`). A no-op returning
/// `Success` when the session has `ignore_transactions = true`. Must be
/// called from a native (non-async-runtime) thread.
///
/// # Safety
/// `session` must be a live session handle from [`kernel_session_open`].
#[no_mangle]
pub unsafe extern "C" fn kernel_session_commit(session: *mut kernel_session_t) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(session) = (session as *const Session).as_ref() else {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_session_commit: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    match block_on_ffi(|| session.commit()) {
        Ok(()) => KernelStatusCode::Success,
        Err(code) => code,
    }
}

/// Roll back the current transaction (issues `ROLLBACK`). A no-op returning
/// `Success` when the session has `ignore_transactions = true`. Requires
/// catalog-managed tables server-side for the rollback to take effect. Must
/// be called from a native (non-async-runtime) thread.
///
/// # Safety
/// `session` must be a live session handle from [`kernel_session_open`].
#[no_mangle]
pub unsafe extern "C" fn kernel_session_rollback(
    session: *mut kernel_session_t,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    let Some(session) = (session as *const Session).as_ref() else {
        crate::c_abi::error::store_error(
            Error::invalid_argument().message("kernel_session_rollback: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    match block_on_ffi(|| session.rollback()) {
        Ok(()) => KernelStatusCode::Success,
        Err(code) => code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn m2m_carries_scopes_and_token_endpoint_override() {
        // The additive setters must reach the built AuthConfig so an Azure SP
        // M2M config can point the client-credentials exchange at the Entra
        // token endpoint with the Azure resource scope.
        let token_endpoint = "https://login.microsoftonline.com/tenant/oauth2/v2.0/token";
        let auth = auth_spec_to_config(
            AuthSpec::OAuthM2M {
                client_id: "azure-client-id".to_string(),
                client_secret: "azure-client-secret".to_string(),
            },
            Some(token_endpoint.to_string()),
            Some("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default".to_string()),
            None,
            TokenCacheConfig::default(),
        )
        .expect("m2m config builds");

        match auth {
            AuthConfig::OAuthM2M {
                scopes,
                token_url_override,
                ..
            } => {
                assert_eq!(
                    scopes,
                    Some(vec![
                        "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default".to_string()
                    ])
                );
                assert_eq!(token_url_override.as_deref(), Some(token_endpoint));
            }
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    #[test]
    fn azure_sp_builds_azure_service_principal() {
        // azure-sp forwards the Entra SP credentials (+ optional tenant +
        // optional workspace resource id) and the shared federation id to the
        // kernel's Azure SP flow.
        let auth = auth_spec_to_config(
            AuthSpec::AzureServicePrincipal {
                azure_client_id: "entra-app-id".to_string(),
                azure_client_secret: "entra-secret".to_string(),
                azure_tenant_id: Some("tenant-abc".to_string()),
                azure_workspace_resource_id: Some(
                    "/subscriptions/s/resourceGroups/rg/workspace/w".to_string(),
                ),
            },
            None,
            None,
            Some("fed-client-id".to_string()),
            TokenCacheConfig::default(),
        )
        .expect("azure sp config builds");

        match auth {
            AuthConfig::AzureServicePrincipal {
                azure_client_id,
                azure_client_secret,
                azure_tenant_id,
                azure_workspace_resource_id,
                identity_federation_client_id,
            } => {
                assert_eq!(azure_client_id, "entra-app-id");
                assert_eq!(azure_client_secret, "entra-secret");
                assert_eq!(azure_tenant_id.as_deref(), Some("tenant-abc"));
                assert_eq!(
                    azure_workspace_resource_id.as_deref(),
                    Some("/subscriptions/s/resourceGroups/rg/workspace/w")
                );
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("fed-client-id")
                );
            }
            other => panic!("expected AzureServicePrincipal, got {other:?}"),
        }
    }

    #[test]
    fn azure_sp_tenant_and_resource_id_optional() {
        // Omitting tenant + resource id is valid — the kernel auto-discovers
        // the tenant and sends the data token alone.
        let auth = auth_spec_to_config(
            AuthSpec::AzureServicePrincipal {
                azure_client_id: "entra-app-id".to_string(),
                azure_client_secret: "entra-secret".to_string(),
                azure_tenant_id: None,
                azure_workspace_resource_id: None,
            },
            None,
            None,
            None,
            TokenCacheConfig::default(),
        )
        .expect("azure sp config builds");

        match auth {
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
    fn u2m_carries_token_endpoint_override() {
        let token_endpoint = "https://login.microsoftonline.com/tenant/oauth2/v2.0/token";
        let auth = auth_spec_to_config(
            AuthSpec::OAuthU2M {
                client_id: None,
                redirect_port: None,
                scopes: None,
            },
            Some(token_endpoint.to_string()),
            None,
            None,
            TokenCacheConfig::default(),
        )
        .expect("u2m config builds");

        match auth {
            AuthConfig::OAuthU2M {
                token_url_override, ..
            } => assert_eq!(token_url_override.as_deref(), Some(token_endpoint)),
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn oauth_scopes_setter_is_shared_by_m2m_and_u2m() {
        // The single `set_oauth_scopes` setter applies to whichever OAuth mode
        // is selected — matching the shared `oauth_scopes` on pyo3 / napi.
        let scopes = "custom-a,custom-b";
        let want = Some(vec!["custom-a".to_string(), "custom-b".to_string()]);

        let m2m = auth_spec_to_config(
            AuthSpec::OAuthM2M {
                client_id: "id".to_string(),
                client_secret: "sec".to_string(),
            },
            None,
            Some(scopes.to_string()),
            None,
            TokenCacheConfig::default(),
        )
        .expect("m2m");
        match m2m {
            AuthConfig::OAuthM2M { scopes, .. } => assert_eq!(scopes, want),
            other => panic!("expected OAuthM2M, got {other:?}"),
        }

        let u2m = auth_spec_to_config(
            AuthSpec::OAuthU2M {
                client_id: None,
                redirect_port: None,
                scopes: None,
            },
            None,
            Some(scopes.to_string()),
            None,
            TokenCacheConfig::default(),
        )
        .expect("u2m");
        match u2m {
            AuthConfig::OAuthU2M { scopes, .. } => assert_eq!(scopes, want),
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn oauth_scopes_setter_overrides_u2m_positional_scopes() {
        // For U2M, the shared setter wins over scopes passed positionally to
        // `set_auth_u2m` (kept for back-compat).
        let auth = auth_spec_to_config(
            AuthSpec::OAuthU2M {
                client_id: None,
                redirect_port: None,
                scopes: Some("from-set-auth-u2m".to_string()),
            },
            None,
            Some("from-set-oauth-scopes".to_string()),
            None,
            TokenCacheConfig::default(),
        )
        .expect("u2m");
        match auth {
            AuthConfig::OAuthU2M { scopes, .. } => {
                assert_eq!(scopes, Some(vec!["from-set-oauth-scopes".to_string()]))
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn empty_shared_oauth_scopes_falls_back_to_u2m_positional_scopes() {
        // A present-but-empty shared value (`set_oauth_scopes("")`) parses to
        // no scopes, so it must NOT shadow the positional `set_auth_u2m`
        // scopes: precedence is chosen on the *parsed* result, not the raw
        // `Option<String>`. Guards the documented positional-fallback contract.
        let auth = auth_spec_to_config(
            AuthSpec::OAuthU2M {
                client_id: None,
                redirect_port: None,
                scopes: Some("from-set-auth-u2m".to_string()),
            },
            None,
            Some(String::new()),
            None,
            TokenCacheConfig::default(),
        )
        .expect("u2m");
        match auth {
            AuthConfig::OAuthU2M { scopes, .. } => {
                assert_eq!(scopes, Some(vec!["from-set-auth-u2m".to_string()]))
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn empty_shared_oauth_token_endpoint_falls_back_to_jwt_positional_token_url() {
        // A present-but-empty shared value (`set_oauth_token_endpoint("")`) must
        // NOT shadow the positional `token_url`: precedence is chosen on the
        // normalized result, not the raw `Option<String>`. Otherwise the empty
        // endpoint wins and fails opaquely at `TokenUrl::new("")` during the
        // first token exchange. Mirrors the scopes fallback contract above.
        let positional = "https://login.microsoftonline.com/tenant/oauth2/v2.0/token";
        let auth = auth_spec_to_config(
            AuthSpec::OAuthM2MJwt {
                client_id: "id".to_string(),
                jwt_key_file: "/tmp/key.pem".to_string(),
                jwt_kid: "kid".to_string(),
                jwt_passphrase: None,
                jwt_algorithm: None,
                scopes: None,
                token_url: Some(positional.to_string()),
            },
            Some(String::new()),
            None,
            None,
            TokenCacheConfig::default(),
        )
        .expect("m2m jwt");
        match auth {
            AuthConfig::OAuthM2MPrivateKey {
                token_url_override, ..
            } => assert_eq!(token_url_override.as_deref(), Some(positional)),
            other => panic!("expected OAuthM2MPrivateKey, got {other:?}"),
        }
    }

    #[test]
    fn parse_scopes_trims_and_drops_empties() {
        assert_eq!(
            parse_scopes("a, ,b,"),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            parse_scopes("all-apis,offline_access"),
            Some(vec!["all-apis".to_string(), "offline_access".to_string()])
        );
        // All-empty / blank → None so the kernel applies its defaults.
        assert_eq!(parse_scopes(""), None);
        assert_eq!(parse_scopes("  , , "), None);
    }

    /// Custom headers accumulate in call order and are NOT deduped (the
    /// contract is append/ordered, unlike session_conf's overwrite map).
    /// Guards against a refactor to a dedup'ing map silently dropping
    /// repeated header names.
    #[test]
    fn custom_headers_accumulate_in_order_with_duplicates() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );
            let pairs = [("X-A", "1"), ("X-B", "2"), ("X-A", "3")];
            for (n, v) in pairs {
                let name = CString::new(n).unwrap();
                let value = CString::new(v).unwrap();
                assert_eq!(
                    kernel_session_config_set_custom_header(config, name.as_ptr(), value.as_ptr()),
                    KernelStatusCode::Success
                );
            }
            let got = &config.as_ref().unwrap().custom_headers;
            let expected: Vec<(String, String)> = pairs
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect();
            assert_eq!(
                *got, expected,
                "headers must be appended in order, duplicates kept"
            );
            kernel_session_config_free(config);
        }
    }

    /// The JWT private-key M2M setter captures the required and optional
    /// fields verbatim; NULL optionals record as `None`. Guards the C-side
    /// wiring into `AuthSpec::OAuthM2MJwt` (which then maps to
    /// `AuthConfig::oauth_m2m_private_key` at open).
    #[test]
    fn set_auth_m2m_jwt_records_fields() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );
            let client_id = CString::new("sp-client-id").unwrap();
            let key_file = CString::new("/keys/jwt.pem").unwrap();
            let kid = CString::new("kid-1").unwrap();
            let passphrase = CString::new("secret").unwrap();
            let algorithm = CString::new("ES256").unwrap();
            let scopes = CString::new("all-apis,sql").unwrap();
            assert_eq!(
                kernel_session_config_set_auth_m2m_jwt(
                    config,
                    client_id.as_ptr(),
                    key_file.as_ptr(),
                    kid.as_ptr(),
                    passphrase.as_ptr(),
                    algorithm.as_ptr(),
                    scopes.as_ptr(),
                    std::ptr::null(), // token_url omitted → OIDC discovery
                ),
                KernelStatusCode::Success
            );

            match config.as_ref().unwrap().auth.as_ref().unwrap() {
                AuthSpec::OAuthM2MJwt {
                    client_id,
                    jwt_key_file,
                    jwt_kid,
                    jwt_passphrase,
                    jwt_algorithm,
                    scopes,
                    token_url,
                } => {
                    assert_eq!(client_id, "sp-client-id");
                    assert_eq!(jwt_key_file, "/keys/jwt.pem");
                    assert_eq!(jwt_kid, "kid-1");
                    assert_eq!(jwt_passphrase.as_deref(), Some("secret"));
                    assert_eq!(jwt_algorithm.as_deref(), Some("ES256"));
                    assert_eq!(scopes.as_deref(), Some("all-apis,sql"));
                    assert_eq!(*token_url, None);
                }
                _ => panic!("expected AuthSpec::OAuthM2MJwt"),
            }
            kernel_session_config_free(config);
        }
    }

    /// The retry setter records the backoff/attempt knobs as a `RetrySpec` (in
    /// milliseconds → `Duration`); an untouched config leaves `None` so the
    /// kernel's default policy applies. `max_retries = 0` is valid (disables
    /// retries) and must still be recorded — not conflated with "unset".
    /// `overall_timeout_ms = 0` records as `None` (keep the kernel default
    /// budget); a non-zero value records as `Some`.
    #[test]
    fn set_retry_config_records_knobs() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );
            assert!(config.as_ref().unwrap().retry.is_none());
            // overall_timeout_ms = 0 → keep the kernel default budget (None).
            assert_eq!(
                kernel_session_config_set_retry_config(config, 500, 30_000, 0, 0),
                KernelStatusCode::Success
            );
            let r = config.as_ref().unwrap().retry.as_ref().unwrap();
            assert_eq!(r.min_wait, Duration::from_millis(500));
            assert_eq!(r.max_wait, Duration::from_millis(30_000));
            assert_eq!(r.max_retries, 0, "max_retries=0 (disable) must be recorded");
            assert_eq!(
                r.overall_timeout, None,
                "overall_timeout_ms=0 must keep the kernel default (None)"
            );
            // A non-zero overall_timeout_ms is recorded as Some(Duration).
            assert_eq!(
                kernel_session_config_set_retry_config(config, 500, 30_000, 3, 120_000),
                KernelStatusCode::Success
            );
            let r = config.as_ref().unwrap().retry.as_ref().unwrap();
            assert_eq!(r.overall_timeout, Some(Duration::from_millis(120_000)));
            // max_retries is clamped to u32::MAX - 1 so the HTTP client's
            // `max_retries + 1` total-attempts computation can't overflow.
            assert_eq!(
                kernel_session_config_set_retry_config(config, 500, 30_000, u32::MAX, 0),
                KernelStatusCode::Success
            );
            let r = config.as_ref().unwrap().retry.as_ref().unwrap();
            assert_eq!(
                r.max_retries,
                u32::MAX - 1,
                "max_retries must clamp to u32::MAX - 1 to keep max_retries + 1 from overflowing"
            );
            kernel_session_config_free(config);
        }
    }

    /// The retry setter rejects a zero min_wait with `InvalidArgument` (a zero
    /// floor is genuinely invalid), but a `max_wait < min_wait` pair is a tuning
    /// typo that is CORRECTED (max raised to min) with a warning rather than
    /// rejected — a backoff bound must not fail a host's connect
    /// (ERRORRECOV-014).
    #[test]
    fn set_retry_config_rejects_zero_min_corrects_bad_range() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );

            // Zero min_wait is still rejected outright.
            assert_eq!(
                kernel_session_config_set_retry_config(config, 0, 1_000, 3, 0),
                KernelStatusCode::InvalidArgument
            );
            // A rejected call leaves the config's retry unset.
            assert!(config.as_ref().unwrap().retry.is_none());

            // max_wait (1s) < min_wait (5s): accepted, and max is raised to min.
            assert_eq!(
                kernel_session_config_set_retry_config(config, 5_000, 1_000, 3, 0),
                KernelStatusCode::Success
            );
            let retry = config
                .as_ref()
                .unwrap()
                .retry
                .as_ref()
                .expect("retry spec should be recorded after correction");
            assert_eq!(retry.min_wait, Duration::from_millis(5_000));
            assert_eq!(
                retry.max_wait,
                Duration::from_millis(5_000),
                "max_wait should be corrected up to min_wait"
            );
            assert_eq!(retry.max_retries, 3);

            kernel_session_config_free(config);
        }
    }

    /// End-to-end: a **retry-only** config (no custom headers) must actually
    /// reach the HTTP client through `build_session`'s
    /// `!custom_headers.is_empty() || retry.is_some()` gate + the
    /// `merge_http_config` call. The `merge_*` unit tests cover the helper in
    /// isolation; this exercises the gate/wiring the helper hangs off of.
    ///
    /// It is *behaviourally distinguishing*, not just a coverage smoke: the
    /// config disables retries (`max_retries = 0`) and the mock returns a
    /// retryable `503` before it would serve `200`. With the retry spec plumbed
    /// through, the lone attempt fails on the `503` and `kernel_session_open`
    /// returns non-`Success`. A regression that reverted the gate to its
    /// pre-PR headers-only form would silently drop the retry-only policy, fall
    /// back to `HttpConfig::default()`'s 5-retry policy, sail past the `503` to
    /// the `200`, and open successfully — which this test would catch.
    ///
    /// A plain `#[test]` (not `#[tokio::test]`): `kernel_session_open` →
    /// `block_on_ffi` drives its own runtime and rejects being called from
    /// within one, so the mock server is started on a throwaway runtime and the
    /// C-ABI entry points are invoked from this native thread.
    #[test]
    fn build_session_wires_retry_only_policy_end_to_end() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // Start the mock and mount its behaviour on a short-lived runtime; the
        // server itself runs on its own background thread, so it keeps serving
        // after this runtime is dropped.
        let setup_rt = tokio::runtime::Runtime::new().expect("runtime");
        let attempts = Arc::new(AtomicU32::new(0));
        let server = setup_rt.block_on(async {
            let server = MockServer::start().await;
            let counter = attempts.clone();
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/sessions"))
                .respond_with(move |_req: &wiremock::Request| {
                    // First attempt 503 (retryable); a retrying policy would
                    // then see the 200 below.
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        ResponseTemplate::new(503)
                    } else {
                        ResponseTemplate::new(200).set_body_string(
                            r#"{"session_id":"sess-retry","warehouse_id":"test_wh"}"#,
                        )
                    }
                })
                .mount(&server)
                .await;
            server
        });

        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );
            let host = CString::new(server.uri()).unwrap();
            let http_path = CString::new("/sql/1.0/warehouses/test_wh").unwrap();
            assert_eq!(
                kernel_session_config_set_http_path(config, host.as_ptr(), http_path.as_ptr()),
                KernelStatusCode::Success
            );
            let token = CString::new("dapi-test-token").unwrap();
            assert_eq!(
                kernel_session_config_set_auth_pat(config, token.as_ptr()),
                KernelStatusCode::Success
            );
            // Retry-only: no custom headers. max_retries = 0 ⇒ single attempt.
            assert_eq!(
                kernel_session_config_set_retry_config(config, 500, 30_000, 0, 0),
                KernelStatusCode::Success
            );

            let mut session: *mut kernel_session_t = std::ptr::null_mut();
            let rc = kernel_session_open(config, &mut session);
            assert_ne!(
                rc,
                KernelStatusCode::Success,
                "retries disabled: the lone attempt must fail on the 503, proving the \
                 retry spec reached the client (a dropped policy would retry and open)"
            );
            assert!(
                session.is_null(),
                "a failed open must not write a session handle"
            );
            // Exactly one attempt — the disable policy was honoured (a default
            // policy would have retried and bumped this past 1).
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                1,
                "max_retries=0 must yield a single create-session attempt"
            );
        }
    }

    /// A non-zero `overall_timeout_ms` below `min_wait_ms` guarantees zero
    /// retries (the budget is exhausted before the first backoff), but it is a
    /// coherent single-attempt config — so it is ACCEPTED (not rejected like a
    /// degenerate range), recorded verbatim, and merely warned about. A host's
    /// valid-looking connect must not hard-fail on it.
    #[test]
    fn set_retry_config_small_budget_is_accepted() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );
            // budget (5s) < min_wait (10s): accepted, not InvalidArgument.
            assert_eq!(
                kernel_session_config_set_retry_config(config, 10_000, 60_000, 5, 5_000),
                KernelStatusCode::Success
            );
            let r = config.as_ref().unwrap().retry.as_ref().unwrap();
            assert_eq!(r.min_wait, Duration::from_millis(10_000));
            assert_eq!(
                r.max_retries, 5,
                "the requested attempts are recorded as-is"
            );
            assert_eq!(r.overall_timeout, Some(Duration::from_millis(5_000)));
            kernel_session_config_free(config);
        }
    }

    #[test]
    fn set_identity_federation_client_id_records_value() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );
            let client_id = CString::new("federation-client").unwrap();
            assert_eq!(
                kernel_session_config_set_identity_federation_client_id(config, client_id.as_ptr()),
                KernelStatusCode::Success
            );
            assert_eq!(
                config
                    .as_ref()
                    .unwrap()
                    .identity_federation_client_id
                    .as_deref(),
                Some("federation-client")
            );

            kernel_session_config_free(config);
        }
    }

    #[test]
    fn set_telemetry_config_records_knobs() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );

            assert_eq!(
                kernel_session_config_set_telemetry_config(config, true, 25, 1_000, 2, 50, 2_500),
                KernelStatusCode::Success
            );

            let telemetry = &config.as_ref().unwrap().telemetry;
            assert!(telemetry.enabled);
            assert_eq!(telemetry.batch_size, 25);
            assert_eq!(telemetry.flush_interval, Duration::from_millis(1_000));
            assert_eq!(telemetry.max_retries, 2);
            assert_eq!(telemetry.retry_delay, Duration::from_millis(50));
            assert_eq!(telemetry.close_flush_timeout, Duration::from_millis(2_500));
            assert!(
                telemetry.circuit_breaker_enabled,
                "base telemetry setter should preserve circuit breaker default"
            );
            assert_eq!(telemetry.circuit_breaker_threshold, 5);
            assert_eq!(telemetry.circuit_breaker_timeout, Duration::from_secs(60));

            kernel_session_config_free(config);
        }
    }

    #[test]
    fn set_telemetry_circuit_breaker_config_records_knobs() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );

            assert_eq!(
                kernel_session_config_set_telemetry_circuit_breaker_config(
                    config, false, 9, 12_345
                ),
                KernelStatusCode::Success
            );

            let telemetry = &config.as_ref().unwrap().telemetry;
            assert!(!telemetry.circuit_breaker_enabled);
            assert_eq!(telemetry.circuit_breaker_threshold, 9);
            assert_eq!(
                telemetry.circuit_breaker_timeout,
                Duration::from_millis(12_345)
            );

            kernel_session_config_free(config);
        }
    }

    #[test]
    fn set_telemetry_circuit_breaker_config_rejects_invalid_knobs() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );

            assert_eq!(
                kernel_session_config_set_telemetry_circuit_breaker_config(config, true, 0, 1),
                KernelStatusCode::InvalidArgument
            );
            assert_eq!(
                kernel_session_config_set_telemetry_circuit_breaker_config(config, true, 1, 0),
                KernelStatusCode::InvalidArgument
            );

            kernel_session_config_free(config);
        }
    }

    #[test]
    fn set_telemetry_config_rejects_zero_required_fields() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );

            assert_eq!(
                kernel_session_config_set_telemetry_config(config, true, 0, 1, 0, 0, 1),
                KernelStatusCode::InvalidArgument
            );
            assert_eq!(
                kernel_session_config_set_telemetry_config(config, true, 1, 0, 0, 0, 1),
                KernelStatusCode::InvalidArgument
            );
            assert_eq!(
                kernel_session_config_set_telemetry_config(config, true, 1, 1, 0, 0, 0),
                KernelStatusCode::InvalidArgument
            );
            assert!(
                !config.as_ref().unwrap().telemetry.enabled,
                "rejected calls must leave default disabled telemetry"
            );

            kernel_session_config_free(config);
        }
    }

    #[test]
    fn set_driver_system_configuration_records_optional_fields() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );

            let driver_name = CString::new("ODBC").unwrap();
            let driver_version = CString::new("1.2.3").unwrap();
            let runtime_name = CString::new("native").unwrap();
            let runtime_version = CString::new("0").unwrap();
            let runtime_vendor = CString::new("Databricks").unwrap();
            let os_name = CString::new("Linux").unwrap();
            let os_version = CString::new("6.1").unwrap();
            let os_arch = CString::new("x86_64").unwrap();
            let client_app_name = CString::new("app").unwrap();
            let locale_name = CString::new("en_US").unwrap();
            let char_set_encoding = CString::new("UTF-8").unwrap();
            let process_name = CString::new("odbc-test").unwrap();

            assert_eq!(
                kernel_session_config_set_driver_system_configuration(
                    config,
                    driver_name.as_ptr(),
                    driver_version.as_ptr(),
                    runtime_name.as_ptr(),
                    runtime_version.as_ptr(),
                    runtime_vendor.as_ptr(),
                    os_name.as_ptr(),
                    os_version.as_ptr(),
                    os_arch.as_ptr(),
                    client_app_name.as_ptr(),
                    locale_name.as_ptr(),
                    char_set_encoding.as_ptr(),
                    process_name.as_ptr(),
                ),
                KernelStatusCode::Success
            );

            let system_configuration = config
                .as_ref()
                .unwrap()
                .driver_system_configuration
                .as_ref()
                .expect("driver system configuration");
            assert_eq!(system_configuration.driver_name.as_deref(), Some("ODBC"));
            assert_eq!(
                system_configuration.driver_version.as_deref(),
                Some("1.2.3")
            );
            assert_eq!(system_configuration.runtime_name.as_deref(), Some("native"));
            assert_eq!(system_configuration.runtime_version.as_deref(), Some("0"));
            assert_eq!(
                system_configuration.runtime_vendor.as_deref(),
                Some("Databricks")
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
            assert_eq!(
                system_configuration.process_name.as_deref(),
                Some("odbc-test")
            );

            kernel_session_config_free(config);
        }
    }

    #[test]
    fn set_driver_system_configuration_allows_null_optional_fields() {
        unsafe {
            let mut config: *mut KernelSessionConfig = std::ptr::null_mut();
            assert_eq!(
                kernel_session_config_new(&mut config),
                KernelStatusCode::Success
            );

            assert_eq!(
                kernel_session_config_set_driver_system_configuration(
                    config,
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                    std::ptr::null(),
                ),
                KernelStatusCode::Success
            );

            let system_configuration = config
                .as_ref()
                .unwrap()
                .driver_system_configuration
                .as_ref()
                .expect("driver system configuration");
            assert_eq!(system_configuration, &DriverSystemConfiguration::default());

            kernel_session_config_free(config);
        }
    }

    /// A `RetrySpec` maps field-for-field onto `HttpConfig`, and a `Some`
    /// `overall_timeout` overrides the default budget. This covers the merge
    /// that actually configures live retries — a min/max swap or a dropped
    /// assignment would be caught here.
    #[test]
    fn merge_http_config_maps_every_retry_field() {
        let http = merge_http_config(
            Vec::new(),
            Some(RetrySpec {
                min_wait: Duration::from_millis(500),
                max_wait: Duration::from_millis(30_000),
                max_retries: 9,
                overall_timeout: Some(Duration::from_millis(120_000)),
            }),
        );
        assert_eq!(http.retry_min_wait, Duration::from_millis(500));
        assert_eq!(http.retry_max_wait, Duration::from_millis(30_000));
        assert_eq!(http.retry_max_retries, 9);
        assert_eq!(http.overall_timeout, Duration::from_millis(120_000));
    }

    /// A `None` `overall_timeout` (host left it 0) preserves the kernel's
    /// default 900s budget while still applying the other retry knobs — the
    /// case a `&&`/`||` gate slip or an unconditional override would break.
    #[test]
    fn merge_http_config_none_timeout_keeps_default_budget() {
        let default_budget = HttpConfig::default().overall_timeout;
        let http = merge_http_config(
            Vec::new(),
            Some(RetrySpec {
                min_wait: Duration::from_millis(1),
                max_wait: Duration::from_millis(1_000),
                max_retries: 2,
                overall_timeout: None,
            }),
        );
        assert_eq!(
            http.overall_timeout, default_budget,
            "None overall_timeout must keep HttpConfig::default()'s budget"
        );
        assert_eq!(http.retry_max_retries, 2, "other knobs still apply");
    }

    /// A retry-only config (no custom headers) still produces a populated
    /// `HttpConfig` — guards against a merge that only fires when headers are
    /// present. Conversely a headers-only config leaves every retry knob at
    /// its default.
    #[test]
    fn merge_http_config_retry_only_and_headers_only() {
        // Retry-only: knobs applied, headers empty.
        let retry_only = merge_http_config(
            Vec::new(),
            Some(RetrySpec {
                min_wait: Duration::from_millis(250),
                max_wait: Duration::from_millis(5_000),
                max_retries: 4,
                overall_timeout: None,
            }),
        );
        assert!(retry_only.custom_headers.is_empty());
        assert_eq!(retry_only.retry_min_wait, Duration::from_millis(250));
        assert_eq!(retry_only.retry_max_retries, 4);

        // Headers-only: header carried through, retry knobs untouched from default.
        let default = HttpConfig::default();
        let headers_only = merge_http_config(vec![("X-Test".to_string(), "1".to_string())], None);
        assert_eq!(
            headers_only.custom_headers,
            vec![("X-Test".to_string(), "1".to_string())]
        );
        assert_eq!(headers_only.retry_min_wait, default.retry_min_wait);
        assert_eq!(headers_only.retry_max_wait, default.retry_max_wait);
        assert_eq!(headers_only.retry_max_retries, default.retry_max_retries);
        assert_eq!(headers_only.overall_timeout, default.overall_timeout);
    }
}
