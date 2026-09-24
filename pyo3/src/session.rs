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

//! `Session` — Python class wrapping the kernel's `Session`.
//!
//! Owns a multi-thread tokio runtime per session (one tokio worker
//! pool drives all I/O for this session). The runtime handle is
//! cloned into child `Statement` / `ExecutedStatement` objects so
//! they `block_on` the same pool without keeping the `Session`
//! pyclass alive longer than the user keeps a reference.

use crate::auth::{build_auth_config, OAuthParams};
use crate::executed_async::ExecutedAsyncStatement;
use crate::metadata::Metadata;
use crate::statement::Statement;
use databricks_sql_kernel::{ClientIdentity, ClientType, Session as KSession};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

/// Python-facing session handle.
///
/// Both fields of `SessionInner` are `Send + Sync` (the kernel
/// `Session` asserts it in `lib.rs`; `tokio::runtime::Runtime` is
/// documented `Send + Sync`), so the pyclass is **not** `unsendable`.
/// This lets Python use the same `Session` from multiple threads —
/// the legitimate pattern for `databricks-sql-python` connections
/// shared across a worker pool. The `Send + Sync` invariant is
/// regression-asserted in the test module at the bottom of this file.
#[pyclass(name = "Session")]
pub(crate) struct Session {
    /// `None` after `close()`. Methods return a clean Python error
    /// rather than panicking on a closed session.
    inner: Option<SessionInner>,
}

pub(crate) struct SessionInner {
    pub(crate) session: KSession,
    pub(crate) runtime: Arc<Runtime>,
}

#[derive(Default)]
struct TelemetryKwargs {
    enabled: bool,
    batch_size: Option<usize>,
    flush_interval_ms: Option<u64>,
    max_retries: Option<u32>,
    retry_delay_ms: Option<u64>,
    close_flush_timeout_ms: Option<u64>,
    circuit_breaker_enabled: Option<bool>,
    circuit_breaker_threshold: Option<u32>,
    circuit_breaker_timeout_ms: Option<u64>,
}

impl TelemetryKwargs {
    fn is_mentioned(&self) -> bool {
        self.enabled
            || self.batch_size.is_some()
            || self.flush_interval_ms.is_some()
            || self.max_retries.is_some()
            || self.retry_delay_ms.is_some()
            || self.close_flush_timeout_ms.is_some()
            || self.circuit_breaker_enabled.is_some()
            || self.circuit_breaker_threshold.is_some()
            || self.circuit_breaker_timeout_ms.is_some()
    }
}

fn build_telemetry_config(
    options: TelemetryKwargs,
) -> PyResult<databricks_sql_kernel::TelemetryConfig> {
    if matches!(options.batch_size, Some(0)) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "telemetry_batch_size must be > 0",
        ));
    }
    if matches!(options.flush_interval_ms, Some(0)) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "telemetry_flush_interval_ms must be > 0",
        ));
    }
    if matches!(options.close_flush_timeout_ms, Some(0)) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "telemetry_close_flush_timeout_ms must be > 0",
        ));
    }
    if matches!(options.circuit_breaker_threshold, Some(0))
        && options.circuit_breaker_enabled.unwrap_or(true)
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "telemetry_circuit_breaker_threshold must be > 0 when enabled",
        ));
    }
    if matches!(options.circuit_breaker_timeout_ms, Some(0))
        && options.circuit_breaker_enabled.unwrap_or(true)
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "telemetry_circuit_breaker_timeout_ms must be > 0 when enabled",
        ));
    }

    let mut telemetry = databricks_sql_kernel::TelemetryConfig::default();
    telemetry.enabled = options.enabled;
    if let Some(v) = options.batch_size {
        telemetry.batch_size = v;
    }
    if let Some(v) = options.flush_interval_ms {
        telemetry.flush_interval = std::time::Duration::from_millis(v);
    }
    if let Some(v) = options.max_retries {
        telemetry.max_retries = v;
    }
    if let Some(v) = options.retry_delay_ms {
        telemetry.retry_delay = std::time::Duration::from_millis(v);
    }
    if let Some(v) = options.close_flush_timeout_ms {
        telemetry.close_flush_timeout = std::time::Duration::from_millis(v);
    }
    if let Some(v) = options.circuit_breaker_enabled {
        telemetry.circuit_breaker_enabled = v;
    }
    if let Some(v) = options.circuit_breaker_threshold {
        telemetry.circuit_breaker_threshold = v;
    }
    if let Some(v) = options.circuit_breaker_timeout_ms {
        telemetry.circuit_breaker_timeout = std::time::Duration::from_millis(v);
    }
    Ok(telemetry)
}

#[allow(clippy::too_many_arguments)]
fn build_http_config(
    request_timeout_secs: Option<f64>,
    max_connections: Option<usize>,
    retry_min_wait_secs: Option<u64>,
    retry_max_wait_secs: Option<u64>,
    retry_max_attempts: Option<u32>,
    retry_overall_timeout_secs: Option<u64>,
    http_headers: Option<Vec<(String, String)>>,
) -> PyResult<Option<databricks_sql_kernel::HttpConfig>> {
    // Zero means "keep the kernel default", matching the C ABI setter. Use a
    // float because databricks-sql-python's existing `_socket_timeout` accepts
    // fractional seconds.
    let request_timeout = match request_timeout_secs {
        None => None,
        Some(s) => {
            if s == 0.0 {
                None
            } else {
                let timeout = std::time::Duration::try_from_secs_f64(s).map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(
                        "request_timeout_secs must be finite and >= 0",
                    )
                })?;
                if timeout.is_zero() {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "positive request_timeout_secs must round to at least 1 nanosecond",
                    ));
                }
                Some(timeout)
            }
        }
    };
    if max_connections == Some(0) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "max_connections must be greater than 0",
        ));
    }
    let has_http_headers = http_headers.as_ref().is_some_and(|h| !h.is_empty());
    if request_timeout.is_none()
        && max_connections.is_none()
        && retry_min_wait_secs.is_none()
        && retry_max_wait_secs.is_none()
        && retry_max_attempts.is_none()
        && retry_overall_timeout_secs.is_none()
        && !has_http_headers
    {
        return Ok(None);
    }

    let mut http = databricks_sql_kernel::HttpConfig::default();
    if let Some(timeout) = request_timeout {
        http.request_timeout = timeout;
    }
    if let Some(max) = max_connections {
        http.pool_max_idle_per_host = max;
    }
    if let Some(s) = retry_min_wait_secs {
        http.retry_min_wait = std::time::Duration::from_secs(s);
    }
    if let Some(s) = retry_max_wait_secs {
        http.retry_max_wait = std::time::Duration::from_secs(s);
    }
    if let Some(attempts) = retry_max_attempts {
        http.retry_max_retries = attempts.saturating_sub(1);
    }
    if let Some(s) = retry_overall_timeout_secs {
        http.overall_timeout = std::time::Duration::from_secs(s);
    }
    if let Some(headers) = http_headers {
        http.custom_headers = headers;
    }
    Ok(Some(http))
}

#[allow(clippy::too_many_arguments)]
fn build_driver_system_configuration(
    driver_name: Option<String>,
    driver_version: Option<String>,
    runtime_name: Option<String>,
    runtime_version: Option<String>,
    runtime_vendor: Option<String>,
    os_name: Option<String>,
    os_version: Option<String>,
    os_arch: Option<String>,
    client_app_name: Option<String>,
    locale_name: Option<String>,
    char_set_encoding: Option<String>,
    process_name: Option<String>,
) -> Option<databricks_sql_kernel::DriverSystemConfiguration> {
    let mentioned = driver_name.is_some()
        || driver_version.is_some()
        || runtime_name.is_some()
        || runtime_version.is_some()
        || runtime_vendor.is_some()
        || os_name.is_some()
        || os_version.is_some()
        || os_arch.is_some()
        || client_app_name.is_some()
        || locale_name.is_some()
        || char_set_encoding.is_some()
        || process_name.is_some();
    mentioned.then_some(databricks_sql_kernel::DriverSystemConfiguration {
        driver_version,
        runtime_name,
        runtime_version,
        runtime_vendor,
        os_name,
        os_version,
        os_arch,
        driver_name,
        client_app_name,
        locale_name,
        char_set_encoding,
        process_name,
    })
}

fn build_client_identity(driver_version: Option<&str>) -> Option<ClientIdentity> {
    driver_version
        .filter(|version| !version.trim().is_empty())
        .map(|version| ClientIdentity::new(ClientType::Python, version))
}

#[pymethods]
impl Session {
    /// Open a new session against the given workspace.
    ///
    /// `host` may be bare (`my-workspace.databricks.com`) or
    /// fully-qualified (`https://...`). `http_path` is the SQL
    /// endpoint path.
    ///
    /// Auth: pass `auth_type` plus the matching credentials.
    ///
    /// - `"pat"` (or omit `auth_type` and pass `access_token`):
    ///   personal access token.
    /// - `"oauth-m2m"`: service-principal client credentials —
    ///   `client_id` + `client_secret` (optional `oauth_scopes`,
    ///   `token_url`).
    /// - `"oauth-u2m"`: browser authorization-code + PKCE flow
    ///   (optional `client_id`, `redirect_ports`, `oauth_scopes`,
    ///   `token_url`, `oauth_callback_timeout_secs`).
    ///
    /// OAuth is kernel-owned: the kernel acquires and transparently
    /// refreshes tokens itself.
    ///
    /// `identity_federation_client_id` selects SP-wide Workload Identity
    /// Federation during mandatory token exchange. Omit it for BYOT /
    /// account-wide WIF.
    ///
    /// TLS: all knobs are optional relaxations / additions on top of
    /// the secure default (validate system trust store, verify chain
    /// + hostname). `tls_ca_cert` adds a PEM CA bundle (bytes);
    /// `tls_skip_verify` disables all chain validation;
    /// `tls_skip_hostname_verify` disables the hostname check;
    /// `tls_client_cert` + `tls_client_key` (both PEM bytes) configure
    /// mutual TLS. The two mTLS fields must be set together.
    ///
    /// Retry/backoff: all optional, default to the kernel's built-in
    /// policy (1s/60s backoff, 6 total attempts, 900s budget) when
    /// unset. `retry_min_wait_secs` / `retry_max_wait_secs` bound the
    /// exponential backoff (and clamp a server `Retry-After`);
    /// `retry_max_attempts` is the **total** number of attempts
    /// (converted to the kernel's retries-after-first internally, so
    /// `0`/`1` both mean a single attempt); `retry_overall_timeout_secs`
    /// caps cumulative time across retries. These apply to SQL +
    /// CloudFetch requests; token-endpoint (auth) calls keep a fixed
    /// fast-fail policy.
    ///
    /// `request_timeout_secs` sets the total timeout for one HTTP request,
    /// from connection start through response-body completion. A separate
    /// 30-second connection timeout also applies, so connection establishment
    /// is bounded by the shorter limit. Unset or `0` keeps the kernel default
    /// (120 seconds).
    ///
    /// `max_connections` sets the maximum idle connections retained per host.
    /// Unset keeps the kernel default (100); zero is rejected.
    ///
    /// `http_headers` is a list of `(name, value)` pairs sent on every
    /// request — used to forward the connector's caller headers (which
    /// also carry the composed `User-Agent`). The kernel applies them
    /// before its own `Authorization` / `x-databricks-org-id` headers,
    /// which always take precedence; a caller header for either of those
    /// names is ignored.
    ///
    /// `session_conf` is the server-bound session configuration map
    /// — passed through to SEA `session_confs` verbatim. Use for
    /// `ANSI_MODE`, `TIMEZONE`, `STATEMENT_TIMEOUT`, query-tag
    /// presets, etc. The kernel does not validate keys; the server
    /// rejects unknown ones with a `KernelError(code='SqlError')`
    /// when it picks them up.
    ///
    /// A non-empty `driver_version` also identifies the Python driver for
    /// connector-service feature flags. Omitting it performs no flag request.
    #[new]
    #[pyo3(signature = (
        host,
        http_path,
        *,
        auth_type=None,
        access_token=None,
        client_id=None,
        client_secret=None,
        oauth_scopes=None,
        token_url=None,
        redirect_ports=None,
        oauth_callback_timeout_secs=None,
        jwt_key_file=None,
        jwt_kid=None,
        jwt_passphrase=None,
        jwt_algorithm=None,
        identity_federation_client_id=None,
        azure_client_id=None,
        azure_client_secret=None,
        azure_tenant_id=None,
        azure_workspace_resource_id=None,
        token_cache_enabled=None,
        token_cache_passphrase=None,
        tls_ca_cert=None,
        tls_skip_verify=false,
        tls_skip_hostname_verify=false,
        tls_client_cert=None,
        tls_client_key=None,
        request_timeout_secs=None,
        max_connections=None,
        retry_min_wait_secs=None,
        retry_max_wait_secs=None,
        retry_max_attempts=None,
        retry_overall_timeout_secs=None,
        http_headers=None,
        catalog=None,
        schema=None,
        session_conf=None,
        driver_name=None,
        driver_version=None,
        runtime_name=None,
        runtime_version=None,
        runtime_vendor=None,
        os_name=None,
        os_version=None,
        os_arch=None,
        client_app_name=None,
        locale_name=None,
        char_set_encoding=None,
        process_name=None,
        complex_types_as_json=false,
        intervals_as_string=false,
        telemetry_enabled=false,
        telemetry_batch_size=None,
        telemetry_flush_interval_ms=None,
        telemetry_max_retries=None,
        telemetry_retry_delay_ms=None,
        telemetry_close_flush_timeout_ms=None,
        telemetry_circuit_breaker_enabled=None,
        telemetry_circuit_breaker_threshold=None,
        telemetry_circuit_breaker_timeout_ms=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        host: String,
        http_path: String,
        auth_type: Option<String>,
        access_token: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        oauth_scopes: Option<Vec<String>>,
        token_url: Option<String>,
        redirect_ports: Option<Vec<u16>>,
        oauth_callback_timeout_secs: Option<u64>,
        jwt_key_file: Option<String>,
        jwt_kid: Option<String>,
        jwt_passphrase: Option<String>,
        jwt_algorithm: Option<String>,
        identity_federation_client_id: Option<String>,
        azure_client_id: Option<String>,
        azure_client_secret: Option<String>,
        azure_tenant_id: Option<String>,
        azure_workspace_resource_id: Option<String>,
        // U2M on-disk token-cache control (kernel path); overrides the kernel
        // default (enabled). `False` disables on-disk persistence.
        token_cache_enabled: Option<bool>,
        token_cache_passphrase: Option<String>,
        tls_ca_cert: Option<Vec<u8>>,
        tls_skip_verify: bool,
        tls_skip_hostname_verify: bool,
        tls_client_cert: Option<Vec<u8>>,
        tls_client_key: Option<Vec<u8>>,
        request_timeout_secs: Option<f64>,
        max_connections: Option<usize>,
        retry_min_wait_secs: Option<u64>,
        retry_max_wait_secs: Option<u64>,
        retry_max_attempts: Option<u32>,
        retry_overall_timeout_secs: Option<u64>,
        http_headers: Option<Vec<(String, String)>>,
        catalog: Option<String>,
        schema: Option<String>,
        session_conf: Option<HashMap<String, String>>,
        driver_name: Option<String>,
        driver_version: Option<String>,
        runtime_name: Option<String>,
        runtime_version: Option<String>,
        runtime_vendor: Option<String>,
        os_name: Option<String>,
        os_version: Option<String>,
        os_arch: Option<String>,
        client_app_name: Option<String>,
        locale_name: Option<String>,
        char_set_encoding: Option<String>,
        process_name: Option<String>,
        complex_types_as_json: bool,
        intervals_as_string: bool,
        telemetry_enabled: bool,
        telemetry_batch_size: Option<usize>,
        telemetry_flush_interval_ms: Option<u64>,
        telemetry_max_retries: Option<u32>,
        telemetry_retry_delay_ms: Option<u64>,
        telemetry_close_flush_timeout_ms: Option<u64>,
        telemetry_circuit_breaker_enabled: Option<bool>,
        telemetry_circuit_breaker_threshold: Option<u32>,
        telemetry_circuit_breaker_timeout_ms: Option<u64>,
    ) -> PyResult<Self> {
        // Resolve auth_type. Explicit `auth_type` always wins. When
        // omitted, default to "pat" if `access_token` was passed
        // (back-compat for existing PAT callers). A bare
        // `Session(host, http_path)` with no auth is still rejected.
        let resolved_auth_type = match auth_type.as_deref() {
            Some(t) => t.to_owned(),
            None if access_token.is_some() => "pat".to_owned(),
            None => {
                // Boundary-only breadcrumb: this rejection happens in
                // pyo3 *before* any kernel code runs, so the kernel emits
                // nothing. Without this, a customer who turned on DEBUG to
                // chase a connection failure would see an empty log.
                tracing::debug!(
                    target: crate::logging::PYO3_LOG_TARGET,
                    "Session.__new__ rejected: no auth credentials provided"
                );
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Session requires auth: pass access_token (PAT) or set \
                     auth_type to 'oauth-m2m' / 'oauth-m2m-jwt' / 'oauth-u2m' \
                     with the matching credentials",
                ));
            }
        };
        let oauth = OAuthParams {
            client_id,
            client_secret,
            scopes: oauth_scopes,
            token_url,
            redirect_ports,
            callback_timeout_secs: oauth_callback_timeout_secs,
            jwt_key_file,
            jwt_kid,
            jwt_passphrase,
            jwt_algorithm,
            identity_federation_client_id,
            azure_client_id,
            azure_client_secret,
            azure_tenant_id,
            azure_workspace_resource_id,
            token_cache_enabled,
            token_cache_passphrase,
        };
        let auth_config = build_auth_config(&resolved_auth_type, access_token, oauth)?;

        // Build a TLS config only when the caller relaxed or extended
        // the secure default — otherwise leave the builder on its
        // default (full validation) so we don't allocate a config that
        // changes nothing.
        let tls_config = if tls_ca_cert.is_some()
            || tls_skip_verify
            || tls_skip_hostname_verify
            || tls_client_cert.is_some()
            || tls_client_key.is_some()
        {
            let mut tls = databricks_sql_kernel::TlsConfig::default();
            tls.custom_ca_cert = tls_ca_cert;
            tls.accept_self_signed = tls_skip_verify;
            tls.skip_hostname_verification = tls_skip_hostname_verify;
            tls.client_cert_pem = tls_client_cert;
            tls.client_key_pem = tls_client_key;
            Some(tls)
        } else {
            None
        };

        // Build an HttpConfig only when the caller tuned the request timeout,
        // retry/backoff, or supplied custom headers; otherwise leave the
        // builder on its defaults (120s request timeout,
        // 1s/60s/5-retries, no extra headers).
        //
        // `retry_max_attempts` is the **total** number of attempts
        // (matching the connector's `_retry_stop_after_attempts_count`
        // and JDBC's count semantics). The kernel's `retry_max_retries`
        // counts retries *after* the first attempt, so we convert with
        // `max(0, attempts - 1)` here — centralising the off-by-one so
        // wrappers can pass an intuitive total. `retry_max_attempts = 0`
        // or `1` both mean "one attempt, no retries".
        //
        // `http_headers` are extra headers sent on every request (the
        // connector forwards its `http_headers`, which also carries the
        // composed `User-Agent`). The kernel applies them before its own
        // `Authorization` / `x-databricks-org-id`, which always win.
        let http_config = build_http_config(
            request_timeout_secs,
            max_connections,
            retry_min_wait_secs,
            retry_max_wait_secs,
            retry_max_attempts,
            retry_overall_timeout_secs,
            http_headers,
        )?;

        let client_identity = build_client_identity(driver_version.as_deref());
        let driver_system_configuration = build_driver_system_configuration(
            driver_name,
            driver_version,
            runtime_name,
            runtime_version,
            runtime_vendor,
            os_name,
            os_version,
            os_arch,
            client_app_name,
            locale_name,
            char_set_encoding,
            process_name,
        );

        let telemetry_kwargs = TelemetryKwargs {
            enabled: telemetry_enabled,
            batch_size: telemetry_batch_size,
            flush_interval_ms: telemetry_flush_interval_ms,
            max_retries: telemetry_max_retries,
            retry_delay_ms: telemetry_retry_delay_ms,
            close_flush_timeout_ms: telemetry_close_flush_timeout_ms,
            circuit_breaker_enabled: telemetry_circuit_breaker_enabled,
            circuit_breaker_threshold: telemetry_circuit_breaker_threshold,
            circuit_breaker_timeout_ms: telemetry_circuit_breaker_timeout_ms,
        };
        let telemetry_config = if telemetry_kwargs.is_mentioned() {
            Some(build_telemetry_config(telemetry_kwargs)?)
        } else {
            None
        };

        // One tokio runtime per session. Multi-thread so CloudFetch
        // chunk downloads can overlap with the main poll loop. Worker
        // count left to tokio's default (logical CPU count).
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .enable_all()
                .thread_name("dbx-sql-kernel")
                .build()
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "failed to build tokio runtime: {e}"
                    ))
                })?,
        );

        // `Session::open` is async and grabs the current tokio handle.
        // Drive it on the runtime; the macro detaches the GIL for the
        // I/O and maps kernel errors into `KernelError`.
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Session.__new__: opening kernel session (GIL released)"
        );
        let rt = runtime.clone();
        let session = run_kernel!(py, rt, async {
            let mut builder = KSession::builder()
                .http_path(host, http_path)
                .auth(auth_config);
            if let Some(tls) = tls_config {
                builder = builder.tls(tls);
            }
            if let Some(http) = http_config {
                builder = builder.http(http);
            }
            if let Some(telemetry) = telemetry_config {
                builder = builder.telemetry(telemetry);
            }
            if let Some(system_configuration) = driver_system_configuration {
                builder = builder.driver_system_configuration(system_configuration);
            }
            if let Some(identity) = client_identity {
                builder = builder.client_identity(identity);
            }
            if catalog.is_some() || schema.is_some() {
                let mut defaults = databricks_sql_kernel::DefaultOpts::default();
                defaults.catalog = catalog;
                defaults.schema = schema;
                builder = builder.defaults(defaults);
            }
            if let Some(conf) = session_conf {
                builder = builder.session_conf(conf);
            }
            // Merge result-config knobs into one `ResultConfig` so two
            // toggles don't clobber each other when both are set.
            if complex_types_as_json || intervals_as_string {
                let mut result = databricks_sql_kernel::ResultConfig::default();
                result.complex_types_as_json = complex_types_as_json;
                result.intervals_as_string = intervals_as_string;
                builder = builder.result(result);
            }
            builder.open().await
        })?;

        Ok(Self {
            inner: Some(SessionInner { session, runtime }),
        })
    }

    /// Server-issued session id.
    #[getter]
    fn session_id(&self) -> PyResult<String> {
        let inner = alive!(self, "Session");
        Ok(inner.session.session_id().to_owned())
    }

    /// Return one opaque JSON value from the immutable feature-flag snapshot
    /// captured when this session opened. A missing flag returns `None`.
    /// This reads local state and performs no connector-service request.
    fn feature_flag(&self, name: &str) -> PyResult<Option<String>> {
        let inner = alive!(self, "Session");
        Ok(inner.session.feature_flags().get(name).map(str::to_owned))
    }

    /// Build a new `Statement` bound to this session.
    fn statement(&self) -> PyResult<Statement> {
        let inner = alive!(self, "Session");
        let kstmt = inner.session.statement();
        Ok(Statement::new(kstmt, inner.runtime.clone()))
    }

    /// Metadata operations bound to this session (flat per-method,
    /// ODBC-shaped). Returns a fresh `Metadata` handle on every call;
    /// the handle is cheap to construct and throw away.
    fn metadata(&self) -> PyResult<Metadata> {
        let inner = alive!(self, "Session");
        let kmeta = inner.session.metadata();
        Ok(Metadata::new(kmeta, inner.runtime.clone()))
    }

    /// Re-attach to an already-submitted server statement by its id,
    /// returning an `ExecutedAsyncStatement` the caller can poll
    /// (`status()`), await (`await_result()`), or cancel/close — without
    /// having been the handle that submitted it.
    ///
    /// This is the resume path the connector uses for
    /// `get_query_state` / `get_async_execution_result`: server
    /// statement state is keyed solely by the id, so a caller holding
    /// only the id can recover a working handle bound to this session.
    ///
    /// Issues one `GetStatementStatus` to seed the handle. Raises
    /// `KernelError` with code `NotFound` if the server doesn't
    /// recognise the id (never submitted, or its result TTL elapsed). A
    /// *closed* statement is not an error — the server keeps reporting
    /// `CLOSED` (and terminal `SUCCEEDED`/`FAILED`) until GC, so the
    /// returned handle's `status()` surfaces the real server state.
    fn attach_async_statement(
        &self,
        py: Python<'_>,
        statement_id: String,
    ) -> PyResult<ExecutedAsyncStatement> {
        let inner = alive!(self, "Session");
        let runtime = inner.runtime.clone();
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Session.attach_async_statement: re-attaching by id (GIL released)"
        );
        let exec = run_kernel!(
            py,
            runtime,
            inner.session.attach_async_statement(statement_id)
        )?;
        Ok(ExecutedAsyncStatement::new(exec, runtime))
    }

    /// Explicitly close the session. Idempotent. Subsequent calls on
    /// this `Session` raise `KernelError`.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        let SessionInner { session, runtime } = inner;
        let rt = runtime;
        tracing::debug!(
            target: crate::logging::PYO3_LOG_TARGET,
            "Session.close: closing kernel session (GIL released)"
        );
        run_kernel!(py, rt, session.close())?;
        Ok(())
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            Some(i) => format!(
                "Session(session_id={:?}, closed=False)",
                i.session.session_id()
            ),
            None => "Session(closed=True)".to_owned(),
        }
    }
}

#[cfg(test)]
mod send_sync_tests {
    use super::*;
    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn runtime_arc_is_send_sync() {
        assert_send_sync::<Arc<Runtime>>();
        assert_send_sync::<SessionInner>();
    }

    #[test]
    fn telemetry_kwargs_map_to_kernel_config() {
        let telemetry = build_telemetry_config(TelemetryKwargs {
            enabled: true,
            batch_size: Some(25),
            flush_interval_ms: Some(1_000),
            max_retries: Some(2),
            retry_delay_ms: Some(50),
            close_flush_timeout_ms: Some(2_500),
            circuit_breaker_enabled: Some(false),
            circuit_breaker_threshold: Some(9),
            circuit_breaker_timeout_ms: Some(60_000),
        })
        .expect("valid telemetry config");

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
    fn driver_version_builds_python_client_identity() {
        assert!(build_client_identity(None).is_none());
        assert!(build_client_identity(Some("")).is_none());
        assert!(build_client_identity(Some(" \t")).is_none());

        let identity = build_client_identity(Some("1.2.3")).unwrap();
        assert_eq!(identity.client_type(), ClientType::Python);
        assert_eq!(identity.version(), "1.2.3");
    }

    #[test]
    fn telemetry_kwargs_reject_zero_required_fields() {
        assert!(build_telemetry_config(TelemetryKwargs {
            batch_size: Some(0),
            ..TelemetryKwargs::default()
        })
        .is_err());
        assert!(build_telemetry_config(TelemetryKwargs {
            flush_interval_ms: Some(0),
            ..TelemetryKwargs::default()
        })
        .is_err());
        assert!(build_telemetry_config(TelemetryKwargs {
            close_flush_timeout_ms: Some(0),
            ..TelemetryKwargs::default()
        })
        .is_err());
        assert!(build_telemetry_config(TelemetryKwargs {
            circuit_breaker_threshold: Some(0),
            ..TelemetryKwargs::default()
        })
        .is_err());
        assert!(build_telemetry_config(TelemetryKwargs {
            circuit_breaker_timeout_ms: Some(0),
            ..TelemetryKwargs::default()
        })
        .is_err());
        assert!(build_telemetry_config(TelemetryKwargs {
            circuit_breaker_enabled: Some(false),
            circuit_breaker_threshold: Some(0),
            ..TelemetryKwargs::default()
        })
        .is_ok());
        // A disabled breaker ignores the timeout knob too.
        assert!(build_telemetry_config(TelemetryKwargs {
            circuit_breaker_enabled: Some(false),
            circuit_breaker_timeout_ms: Some(0),
            ..TelemetryKwargs::default()
        })
        .is_ok());
    }

    #[test]
    fn request_timeout_kwarg_maps_to_http_config() {
        let default = databricks_sql_kernel::HttpConfig::default();
        assert!(build_http_config(None, None, None, None, None, None, None)
            .unwrap()
            .is_none());
        assert!(
            build_http_config(Some(0.0), None, None, None, None, None, None)
                .unwrap()
                .is_none()
        );

        let http = build_http_config(Some(0.5), None, None, None, None, None, None)
            .unwrap()
            .expect("positive request timeout should build an HTTP config");
        assert_eq!(http.request_timeout, std::time::Duration::from_millis(500));
        assert_eq!(http.retry_min_wait, default.retry_min_wait);
        assert_eq!(http.overall_timeout, default.overall_timeout);

        let one_ns = std::time::Duration::from_nanos(1).as_secs_f64();
        assert!(
            build_http_config(Some(one_ns / 1_000.0), None, None, None, None, None, None).is_err()
        );
        let http = build_http_config(Some(one_ns), None, None, None, None, None, None)
            .unwrap()
            .expect("one-nanosecond request timeout should be accepted");
        assert_eq!(http.request_timeout, std::time::Duration::from_nanos(1));

        assert!(build_http_config(Some(-1.0), None, None, None, None, None, None).is_err());
        assert!(build_http_config(Some(f64::NAN), None, None, None, None, None, None).is_err());
    }

    #[test]
    fn max_connections_kwarg_maps_to_http_config() {
        let default = databricks_sql_kernel::HttpConfig::default();
        let http = build_http_config(None, Some(17), None, None, None, None, None)
            .unwrap()
            .expect("max connections should build an HTTP config");
        assert_eq!(http.pool_max_idle_per_host, 17);
        assert_eq!(http.request_timeout, default.request_timeout);

        assert!(build_http_config(None, Some(0), None, None, None, None, None).is_err());
    }

    #[test]
    fn driver_system_kwargs_map_to_kernel_config() {
        assert!(build_driver_system_configuration(
            None, None, None, None, None, None, None, None, None, None, None, None,
        )
        .is_none());

        let system_configuration = build_driver_system_configuration(
            Some("PySQL".to_owned()),
            Some("1.2.3".to_owned()),
            Some("CPython".to_owned()),
            Some("3.12".to_owned()),
            Some("Python".to_owned()),
            Some("Linux".to_owned()),
            Some("6.1".to_owned()),
            Some("x86_64".to_owned()),
            Some("app".to_owned()),
            Some("en_US".to_owned()),
            Some("UTF-8".to_owned()),
            Some("pytest".to_owned()),
        )
        .expect("driver system configuration mentioned");

        assert_eq!(system_configuration.driver_name.as_deref(), Some("PySQL"));
        assert_eq!(
            system_configuration.driver_version.as_deref(),
            Some("1.2.3")
        );
        assert_eq!(
            system_configuration.runtime_name.as_deref(),
            Some("CPython")
        );
        assert_eq!(
            system_configuration.runtime_version.as_deref(),
            Some("3.12")
        );
        assert_eq!(
            system_configuration.runtime_vendor.as_deref(),
            Some("Python")
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
        assert_eq!(system_configuration.process_name.as_deref(), Some("pytest"));
    }
}
