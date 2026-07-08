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
//! Transaction methods (autocommit / commit / rollback) are deferred to
//! v0.1 per `docs/spec-v0.md` addendum A5.

use super::{block_on_ffi, KernelStatusCode};
use crate::config::{AuthConfig, ConnectionConfig, HttpConfig, ProxyConfig, TlsConfig};
use crate::kernel_error::Error;
use crate::session::Session;
use std::collections::HashMap;
use std::ffi::{c_char, CStr};

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
    /// All fields optional — the kernel fills `databricks-cli` /
    /// port 8020 / default scopes when omitted.
    OAuthU2M {
        client_id: Option<String>,
        redirect_port: Option<u16>,
        /// Comma-separated scopes (split at build time). `None` ⇒ kernel
        /// default (`all-apis`, `offline_access`).
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
}

/// C-side proxy config captured from `kernel_session_config_set_proxy`.
struct ProxySpec {
    url: String,
    username: Option<String>,
    password: Option<String>,
    bypass_hosts: Option<String>,
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
/// public `databricks-cli` client, `redirect_port` to 8020, and `scopes`
/// (a comma-separated list) to the kernel default (`all-apis`,
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
/// Returns `KernelStatusCode::Success` when logging was installed (or
/// intentionally skipped for level `OFF`). Returns
/// `KernelStatusCode::Internal` (with a stored last-error) when the
/// subscriber could not be installed — currently only when the requested
/// `file_path` fails to open — so a caller that can't see stderr (GUI /
/// BI host) can surface the failure or fall back deliberately. Because
/// init is once-per-process, this reflects the FIRST call's outcome.
///
/// # Safety
/// `level` / `file_path`, if non-null, must be NUL-terminated C strings.
#[no_mangle]
pub unsafe extern "C" fn kernel_init_logging(
    level: *const c_char,
    file_path: *const c_char,
) -> KernelStatusCode {
    crate::c_abi::error::clear_error();
    // Best-effort on the args: a non-UTF-8 arg is treated as absent rather
    // than an error, matching the layer's own tolerance.
    let level = optional_str(level, "log level").ok().flatten();
    let file = optional_str(file_path, "log file path").ok().flatten();
    if crate::logging::init_logging(&crate::logging::LogConfig { level, file }) {
        KernelStatusCode::Success
    } else {
        crate::c_abi::error::store_and_code(Error::new(
            crate::kernel_error::ErrorCode::Internal,
            "kernel_init_logging: failed to install the logging subscriber (log file could not be opened)",
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
                ),
            };

            let mut builder = Session::builder()
                .connection(connection)
                .auth(auth)
                .session_conf(session_conf);

            // Extra HTTP headers, if any. Build a fresh HttpConfig so all
            // other HTTP defaults (timeouts, pool, retry) are preserved.
            if !custom_headers.is_empty() {
                builder = builder.http(HttpConfig {
                    custom_headers,
                    ..HttpConfig::default()
                });
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

// Deferred to v0.1 (see addendum A5):
//
//   pub unsafe extern "C" fn kernel_session_set_autocommit(...) -> KernelStatusCode;
//   pub unsafe extern "C" fn kernel_session_commit(...) -> KernelStatusCode;
//   pub unsafe extern "C" fn kernel_session_rollback(...) -> KernelStatusCode;

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
}
