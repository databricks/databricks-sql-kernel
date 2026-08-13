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
//! Auth over the C ABI: **PAT, OAuth M2M, and OAuth U2M** (interactive
//! browser flow) are all exposed (`set_auth_pat` / `set_auth_m2m` /
//! `set_auth_u2m`). A host may also drive its own token flow and inject
//! it via PAT.
//!
//! Transaction control (autocommit / commit / rollback) is exposed via
//! `kernel_session_set_autocommit` / `_get_autocommit` / `_commit` /
//! `_rollback`, gated by `kernel_session_config_set_ignore_transactions`
//! (default = ignore). Backed by the shared `Session` transaction methods.

use super::{block_on_ffi, KernelStatusCode};
use crate::config::{
    AuthConfig, ConnectionConfig, HttpConfig, ProxyConfig, ResultConfig, TlsConfig,
    TokenCacheConfig,
};
use crate::kernel_error::Error;
use crate::session::Session;
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

/// Auth choice captured from the C-side setters. v0 C ABI wires PAT and
/// OAuth M2M; other [`AuthConfig`] variants stay Rust-crate-only.
///
/// NOTE (secret handling): `token` / `client_secret` are copied out of
/// the C string into owned `String`s and are not zeroized on drop, so
/// plaintext lingers in freed heap. This matches the rest of the crate
/// (no `zeroize`/`secrecy` anywhere); when the crate adopts a
/// zeroize-on-drop policy, these copies must be included.
enum AuthSpec {
    Pat {
        token: String,
    },
    OAuthM2M {
        client_id: String,
        client_secret: String,
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
        // Match the reference driver's / JDBC's EnableTokenCache=1.
        token_cache: TokenCacheConfig::default(),
        // Kernel result defaults (CloudFetch enabled, Arrow-native types).
        result: ResultConfig::default(),
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
            // 0 is the "unset" sentinel → kernel default (8020).
            redirect_port: (redirect_port != 0).then_some(redirect_port),
            scopes: optional_str(scopes, "scopes")?,
        });
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
        token_cache,
        result,
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
                    "session config requires auth (set_auth_pat, set_auth_m2m, or set_auth_u2m)",
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

            let auth = match auth {
                AuthSpec::Pat { token } => AuthConfig::pat(Some(token))?,
                AuthSpec::OAuthM2M {
                    client_id,
                    client_secret,
                } => AuthConfig::oauth_m2m(Some(client_id), Some(client_secret), None, None)?,
                AuthSpec::OAuthU2M {
                    client_id,
                    redirect_port,
                    scopes,
                } => AuthConfig::oauth_u2m(
                    client_id,
                    redirect_port,
                    scopes.and_then(|s| parse_scopes(&s)),
                    None,
                    None,
                    Some(token_cache),
                ),
            };

            let mut builder = Session::builder()
                .connection(connection)
                .auth(auth)
                .session_conf(session_conf)
                .result(result)
                .defaults(crate::config::DefaultOpts {
                    ignore_transactions,
                    ..Default::default()
                });

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
