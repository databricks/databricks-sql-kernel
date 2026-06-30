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
//! Auth in the v0 C ABI is **PAT + OAuth M2M** only; OAuth U2M
//! (interactive browser flow) is Rust-crate-only per the migration
//! plan §6 ("OAuth U2M and the C ABI"). ODBC / Go consumers use PAT or
//! M2M, or drive their own token flow and inject it (not yet exposed
//! over the C ABI in v0).
//!
//! Transaction methods (autocommit / commit / rollback) are deferred to
//! v0.1 per `docs/spec-v0.md` addendum A5.

use super::{block_on_ffi, KernelStatusCode};
use crate::config::{AuthConfig, ConnectionConfig};
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
enum AuthSpec {
    Pat {
        token: String,
    },
    OAuthM2M {
        client_id: String,
        client_secret: String,
    },
}

/// C-side session config builder state.
///
/// Constructed by [`kernel_session_config_new`], populated by the
/// `kernel_session_config_set_*` setters, and consumed by
/// [`kernel_session_open`]. Heap-boxed; the C side holds a
/// `*mut KernelSessionConfig` and never sees the field layout — the
/// struct stays effectively opaque to C even though it carries real
/// fields on the Rust side.
///
/// The `version` field leads the struct per the C-ABI versioning
/// convention (every public struct carries a `uint32_t version` so a
/// future reader can detect older callers); it is set by
/// `kernel_session_config_new` and not exposed to setters.
#[repr(C)]
pub struct KernelSessionConfig {
    /// ABI version tag. `1` for v0.
    version: u32,
    connection: Option<ConnectionSpec>,
    auth: Option<AuthSpec>,
    session_conf: HashMap<String, String>,
}

const KERNEL_SESSION_CONFIG_VERSION: u32 = 1;

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
    crate::ffi::error::clear_error();
    if out.is_null() {
        crate::ffi::error::store_error(
            Error::invalid_argument().message("kernel_session_config_new: out must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    let config = Box::new(KernelSessionConfig {
        version: KERNEL_SESSION_CONFIG_VERSION,
        connection: None,
        auth: None,
        session_conf: HashMap::new(),
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
    crate::ffi::error::clear_error();
    let Some(config) = config.as_mut() else {
        crate::ffi::error::store_error(
            Error::invalid_argument().message("config handle must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    };
    match f(config) {
        Ok(()) => KernelStatusCode::Success,
        Err(e) => {
            let code = KernelStatusCode::from(e.code);
            crate::ffi::error::store_error(e);
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
    crate::ffi::error::clear_error();
    if config.is_null() || out.is_null() {
        crate::ffi::error::store_error(
            Error::invalid_argument()
                .message("kernel_session_open: config and out must be non-null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    // Take ownership of the config box; it is dropped at end of scope on
    // every path, so the caller never frees it.
    let config = Box::from_raw(config);

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

/// Translate the C-side config into a [`Session`], driving the async
/// `Session::open` on the process-wide runtime. Errors are stored in the
/// thread-local buffer; the returned `KernelStatusCode` is the mapped
/// code.
fn build_session(config: KernelSessionConfig) -> Result<Session, KernelStatusCode> {
    let KernelSessionConfig {
        version: _,
        connection,
        auth,
        session_conf,
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
                Error::invalid_argument()
                    .message("session config requires auth (set_auth_pat or set_auth_m2m)")
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
            };

            Session::builder()
                .connection(connection)
                .auth(auth)
                .session_conf(session_conf)
                .open()
                .await
        })
    })
}

/// Close a session. Releases the underlying Rust [`Session`] (which
/// fires `DeleteSession` server-side on drop / explicit close).
///
/// # Safety
///
/// `session` must have been returned by [`kernel_session_open`] and not
/// previously closed. After this call, `session` is invalid; passing it
/// to any other `kernel_*` function is UB.
#[no_mangle]
pub unsafe extern "C" fn kernel_session_close(session: *mut kernel_session_t) -> KernelStatusCode {
    crate::ffi::error::clear_error();
    if session.is_null() {
        crate::ffi::error::store_error(
            Error::invalid_argument().message("kernel_session_close: session must not be null"),
        );
        return KernelStatusCode::InvalidArgument;
    }
    // Reclaim the box; its `Drop` fires the fire-and-forget
    // `DeleteSession`. We deliberately don't `block_on` an explicit
    // `Session::close()` here — `Drop` already handles server cleanup,
    // and a synchronous close would make `kernel_session_close` block on
    // the network for no caller benefit.
    drop(Box::from_raw(session as *mut Session));
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
    crate::ffi::error::clear_error();
    if session.is_null() || out.is_null() {
        crate::ffi::error::store_error(
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
