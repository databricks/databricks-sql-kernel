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

use super::client::TelemetryClient;
use super::exporter::TelemetryExporter;
use super::models::{
    unix_time_millis, DriverAuthFlow, DriverAuthMech, DriverConnectionParameters, DriverErrorInfo,
    DriverMode, DriverSystemConfiguration, FrontendLogContext, FrontendLogEntry, HostDetails,
    OperationDetail, OperationType, OssSqlDriverTelemetryLog, SqlExecutionEvent,
    TelemetryClientContext, TelemetryFrontendLog,
};
use crate::config::{AuthConfig, ConnectionConfig, TelemetryConfig};
use crate::kernel_error::Error as KernelError;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct TelemetryRuntime {
    connection: Arc<dyn ConnectionTelemetry>,
}

impl TelemetryRuntime {
    pub(crate) fn disabled() -> Self {
        Self {
            connection: Arc::new(NoOpConnectionTelemetry),
        }
    }

    pub(crate) fn from_config(
        config: &TelemetryConfig,
        connection: &ConnectionConfig,
        auth: &AuthConfig,
        session_id: String,
    ) -> Self {
        if !config.enabled || config.batch_size == 0 {
            return Self::disabled();
        }

        let Some(exporter) = test_or_noop_exporter(config) else {
            tracing::debug!(
                "telemetry: enabled in config but no exporter is wired in this build; \
                 collection is a no-op and no data will be exported"
            );
            return Self::disabled();
        };

        let workspace_id = match connection.org_id.as_deref() {
            None => {
                tracing::debug!(
                    "telemetry: connection.org_id is unset; defaulting workspace_id to 0"
                );
                0
            }
            Some(org_id) => org_id.parse::<i64>().unwrap_or_else(|e| {
                tracing::debug!(
                    org_id,
                    error = %e,
                    "telemetry: failed to parse connection.org_id as i64; defaulting workspace_id to 0"
                );
                0
            }),
        };

        let session = Arc::new(TelemetrySessionContext {
            session_id,
            workspace_id,
            system_configuration: build_system_configuration(),
            connection_params: build_connection_params(connection, auth),
            auth_type: auth_type(auth),
        });
        let client = Arc::new(TelemetryClient::new(
            exporter,
            config.batch_size,
            config.close_flush_timeout,
        ));

        Self {
            connection: Arc::new(RealConnectionTelemetry {
                session,
                client,
                next_event_id: AtomicU64::new(1),
            }),
        }
    }

    pub(crate) fn connection(&self) -> Arc<dyn ConnectionTelemetry> {
        Arc::clone(&self.connection)
    }
}

pub(crate) trait ConnectionTelemetry: Send + Sync {
    fn session(&self) -> Option<Arc<TelemetrySessionContext>>;

    fn emit_operation(
        &self,
        operation_type: OperationType,
        elapsed: Duration,
        error: Option<&KernelError>,
    );

    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

#[derive(Debug)]
pub(crate) struct TelemetrySessionContext {
    pub session_id: String,
    pub workspace_id: i64,
    pub system_configuration: DriverSystemConfiguration,
    pub connection_params: DriverConnectionParameters,
    pub auth_type: String,
}

#[derive(Debug)]
struct NoOpConnectionTelemetry;

impl ConnectionTelemetry for NoOpConnectionTelemetry {
    fn session(&self) -> Option<Arc<TelemetrySessionContext>> {
        None
    }

    fn emit_operation(
        &self,
        _operation_type: OperationType,
        _elapsed: Duration,
        _error: Option<&KernelError>,
    ) {
    }

    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}

struct RealConnectionTelemetry {
    session: Arc<TelemetrySessionContext>,
    client: Arc<TelemetryClient>,
    next_event_id: AtomicU64,
}

impl RealConnectionTelemetry {
    fn build_operation_log(
        &self,
        operation_type: OperationType,
        elapsed: Duration,
        error: Option<&KernelError>,
    ) -> TelemetryFrontendLog {
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        TelemetryFrontendLog {
            workspace_id: self.session.workspace_id,
            frontend_log_event_id: format!("{}-{event_id}", self.session.session_id),
            context: Some(FrontendLogContext {
                client_context: Some(TelemetryClientContext {
                    user_agent: Some(format!(
                        "{}/{}",
                        env!("CARGO_PKG_NAME"),
                        env!("CARGO_PKG_VERSION")
                    )),
                }),
                timestamp_millis: unix_time_millis(),
            }),
            entry: Some(FrontendLogEntry {
                sql_driver_log: Some(OssSqlDriverTelemetryLog {
                    session_id: Some(self.session.session_id.clone()),
                    system_configuration: Some(self.session.system_configuration.clone()),
                    driver_connection_params: Some(self.session.connection_params.clone()),
                    auth_type: Some(self.session.auth_type.clone()),
                    sql_operation: Some(SqlExecutionEvent {
                        operation_detail: Some(OperationDetail {
                            operation_type: Some(operation_type),
                            is_internal_call: Some(false),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    error_info: error.map(sanitize_error),
                    operation_latency_ms: Some(duration_millis(elapsed)),
                    ..Default::default()
                }),
            }),
        }
    }
}

impl ConnectionTelemetry for RealConnectionTelemetry {
    fn session(&self) -> Option<Arc<TelemetrySessionContext>> {
        Some(Arc::clone(&self.session))
    }

    fn emit_operation(
        &self,
        operation_type: OperationType,
        elapsed: Duration,
        error: Option<&KernelError>,
    ) {
        self.client
            .enqueue(self.build_operation_log(operation_type, elapsed, error));
    }

    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.client.flush_bounded().await;
        })
    }
}

fn test_or_noop_exporter(config: &TelemetryConfig) -> Option<Arc<dyn TelemetryExporter>> {
    #[cfg(test)]
    {
        config.exporter.clone()
    }

    #[cfg(not(test))]
    {
        let _ = config;
        None
    }
}

fn build_system_configuration() -> DriverSystemConfiguration {
    DriverSystemConfiguration {
        driver_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        runtime_name: Some("rust".to_string()),
        runtime_version: option_env!("RUSTC_VERSION").map(str::to_string),
        runtime_vendor: Some("rust-lang".to_string()),
        os_name: Some(std::env::consts::OS.to_string()),
        os_version: None,
        os_arch: Some(std::env::consts::ARCH.to_string()),
        driver_name: Some("Databricks SQL Kernel".to_string()),
        client_app_name: None,
        locale_name: std::env::var("LANG").ok().filter(|s| !s.is_empty()),
        char_set_encoding: Some("UTF-8".to_string()),
        // Use only the basename of argv[0]: the full path frequently embeds a
        // username or other host-specific/PII-ish information that should not
        // leave the process in telemetry.
        process_name: process_name(),
    }
}

/// Returns the file-name (basename) component of `argv[0]`, dropping any
/// leading directory path so host-specific/PII-ish path segments are not
/// exported. Falls back to the raw `argv[0]` if it has no file-name component.
fn process_name() -> Option<String> {
    let arg0 = std::env::args().next()?;
    let basename = std::path::Path::new(&arg0)
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or(arg0);
    Some(basename)
}

fn build_connection_params(
    connection: &ConnectionConfig,
    auth: &AuthConfig,
) -> DriverConnectionParameters {
    // `http_path` is reconstructed from `warehouse_id` (the config parses/normalises
    // the incoming path down to the id, so the canonical `/sql/1.0/warehouses/{id}`
    // form is reproduced here). `mode`, `enable_arrow`, `enable_direct_results`, and
    // `auto_commit` are fixed invariants of this kernel rather than placeholders: it
    // only speaks SEA, always fetches Arrow results with direct results enabled, and
    // exposes no autocommit-off path. Revisit these if the kernel ever gains a
    // configurable transport or session mode.
    DriverConnectionParameters {
        http_path: Some(format!("/sql/1.0/warehouses/{}", connection.warehouse_id)),
        mode: Some(DriverMode::Sea),
        host_info: Some(HostDetails {
            host_url: Some(connection.host.clone()),
            port: None,
            proxy_auth_type: None,
        }),
        auth_mech: Some(auth_mech(auth)),
        auth_flow: auth_flow(auth),
        enable_arrow: Some(true),
        enable_direct_results: Some(true),
        auto_commit: Some(true),
        ..Default::default()
    }
}

fn auth_type(auth: &AuthConfig) -> String {
    match auth {
        AuthConfig::Pat { .. } => "pat",
        AuthConfig::OAuthM2M { .. } => "oauth-m2m",
        AuthConfig::OAuthU2M { .. } => "oauth-u2m",
        AuthConfig::OAuthFederation { .. } => "oauth-federation",
        AuthConfig::External(_) => "external",
    }
    .to_string()
}

fn auth_mech(auth: &AuthConfig) -> DriverAuthMech {
    match auth {
        AuthConfig::Pat { .. } => DriverAuthMech::Pat,
        AuthConfig::OAuthM2M { .. }
        | AuthConfig::OAuthU2M { .. }
        | AuthConfig::OAuthFederation { .. } => DriverAuthMech::Oauth,
        AuthConfig::External(_) => DriverAuthMech::Other,
    }
}

fn auth_flow(auth: &AuthConfig) -> Option<DriverAuthFlow> {
    match auth {
        AuthConfig::Pat { .. } => Some(DriverAuthFlow::TokenPassthrough),
        AuthConfig::OAuthM2M { .. } => Some(DriverAuthFlow::ClientCredentials),
        AuthConfig::OAuthU2M { .. } => Some(DriverAuthFlow::BrowserBasedAuthentication),
        AuthConfig::OAuthFederation { .. } | AuthConfig::External(_) => None,
    }
}

fn sanitize_error(error: &KernelError) -> DriverErrorInfo {
    DriverErrorInfo {
        error_name: Some(error.code.slug().to_string()),
        // We deliberately do NOT emit a stack trace here: sibling drivers
        // (e.g. ADBC) omit trace data for security reasons, and the kernel has
        // no trace to surface at this layer. A SQLSTATE code is not a stack
        // trace, so we leave this `None` rather than overloading the field with
        // semantically-wrong data that downstream telemetry consumers would
        // misinterpret.
        stack_trace: None,
    }
}

fn duration_millis(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}
