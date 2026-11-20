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

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use adbc_core::error::{Result, Status};
use adbc_core::options::OptionValue;
use databricks_sql_kernel as kernel;

use crate::error::{adbc_error, map_kernel_error, unsupported};

pub(crate) const HOST: &str = "adbc.databricks.host";
pub(crate) const HTTP_PATH: &str = "adbc.databricks.http_path";
pub(crate) const WAREHOUSE_ID: &str = "adbc.databricks.warehouse_id";
pub(crate) const ACCESS_TOKEN: &str = "adbc.databricks.auth.token";
pub(crate) const CATALOG: &str = "adbc.databricks.catalog";
pub(crate) const SCHEMA: &str = "adbc.databricks.schema";
pub(crate) const USER_AGENT_ENTRY: &str = "adbc.databricks.user_agent_entry";
const USER_AGENT_ENTRY_ALIAS: &str = "adbc.spark.user_agent_entry";
const AUTH_TYPE: &str = "adbc.databricks.auth.type";
const GRANT_TYPE: &str = "adbc.databricks.oauth.grant_type";
const IDENTITY_FEDERATION_CLIENT_ID: &str = "adbc.databricks.identity_federation_client_id";
const OAUTH_CLIENT_ID: &str = "adbc.databricks.oauth.client_id";
const OAUTH_CLIENT_SECRET: &str = "adbc.databricks.oauth.client_secret";
const OAUTH_SCOPES: &str = "adbc.databricks.oauth.scopes";
const OAUTH_SCOPE: &str = "adbc.databricks.oauth.scope";
const OAUTH_TOKEN_URL: &str = "adbc.databricks.oauth.token_url";
const OAUTH_REDIRECT_PORTS: &str = "adbc.databricks.oauth.redirect_ports";
const OAUTH_CALLBACK_TIMEOUT_MS: &str = "adbc.databricks.oauth.callback_timeout_ms";
const TOKEN_CACHE_ENABLED: &str = "adbc.databricks.oauth.token_cache.enabled";
const TOKEN_CACHE_PASSPHRASE: &str = "adbc.databricks.oauth.token_cache.passphrase";
const JWT_KEY_FILE: &str = "adbc.databricks.oauth.jwt.key_file";
const JWT_KID: &str = "adbc.databricks.oauth.jwt.kid";
const JWT_PASSPHRASE: &str = "adbc.databricks.oauth.jwt.passphrase";
const JWT_ALGORITHM: &str = "adbc.databricks.oauth.jwt.algorithm";
const AZURE_CLIENT_ID: &str = "adbc.databricks.azure.client_id";
const AZURE_CLIENT_SECRET: &str = "adbc.databricks.azure.client_secret";
const AZURE_TENANT_ID: &str = "adbc.databricks.azure.tenant_id";
const AZURE_WORKSPACE_RESOURCE_ID: &str = "adbc.databricks.azure.workspace_resource_id";
const HTTP_PREFIX: &str = "adbc.databricks.http.";
const PROXY_PREFIX: &str = "adbc.databricks.proxy.";
const TLS_PREFIX: &str = "adbc.databricks.tls.";
const TELEMETRY_PREFIX: &str = "adbc.databricks.telemetry.";
const LOG_PREFIX: &str = "adbc.databricks.log.";
const SESSION_CONF_PREFIX: &str = "adbc.databricks.session_conf.";
const SERVER_SIDE_PROPERTY_PREFIX: &str = "adbc.databricks.ssp_";
const QUERY_TAGS: &str = "adbc.databricks.query_tags";
const ENABLE_COMPLEX_DATATYPE_SUPPORT: &str = "adbc.databricks.enable_complex_datatype_support";
const GEOSPATIAL_REPRESENTATION: &str = "adbc.databricks.geospatial.result_representation";
const ENABLE_GEOSPATIAL: &str = "adbc.databricks.enable_geospatial_support";
const CLOUDFETCH_MAX_CHUNKS_IN_MEMORY: &str = "adbc.databricks.cloudfetch.max_chunks_in_memory";
const CLOUDFETCH_ENABLED: &str = "adbc.databricks.cloudfetch.enabled";
const PROTOCOL: &str = "adbc.databricks.protocol";
pub(crate) const ESCAPE_CATALOG_NAME: &str = "adbc.databricks.metadata.escape_catalog_name";

#[derive(Clone, Default)]
pub(crate) struct SecretString(String);

impl SecretString {
    fn new(value: String) -> Self {
        Self(value)
    }
    fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[derive(Clone, Default)]
struct SecretBytes(Vec<u8>);

impl SecretBytes {
    fn new(value: Vec<u8>) -> Self {
        Self(value)
    }
    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[derive(Clone)]
struct Preferred<T> {
    canonical: Option<T>,
    alias: Option<T>,
}

impl<T> Default for Preferred<T> {
    fn default() -> Self {
        Self {
            canonical: None,
            alias: None,
        }
    }
}

impl<T> Preferred<T> {
    fn set(&mut self, value: T, canonical: bool) {
        if canonical {
            self.canonical = Some(value);
        } else {
            self.alias = Some(value);
        }
    }
    fn get(&self) -> Option<&T> {
        self.canonical.as_ref().or(self.alias.as_ref())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthMode {
    Pat,
    OAuthM2M,
    OAuthU2M,
    OAuthM2MPrivateKey,
    AzureServicePrincipal,
    OAuthFederation,
}

#[derive(Clone, Default)]
pub(crate) struct DatabaseConfig {
    uri: Option<String>,
    host: Preferred<String>,
    http_path: Preferred<String>,
    warehouse_id: Option<String>,
    catalog: Preferred<String>,
    schema: Preferred<String>,
    user_agent_entry: Preferred<String>,
    auth_mode: Preferred<AuthMode>,
    username: Option<String>,
    password: Option<SecretString>,
    token: Preferred<SecretString>,
    oauth_client_id: Option<String>,
    oauth_client_secret: Option<SecretString>,
    oauth_scopes: Preferred<Vec<String>>,
    oauth_token_url: Option<String>,
    oauth_redirect_ports: Option<Vec<u16>>,
    oauth_callback_timeout: Option<Duration>,
    token_cache_enabled: Option<bool>,
    token_cache_passphrase: Option<SecretString>,
    identity_federation_client_id: Option<String>,
    jwt_key_file: Option<String>,
    jwt_kid: Option<String>,
    jwt_passphrase: Option<SecretString>,
    jwt_algorithm: Option<String>,
    azure_client_id: Option<String>,
    azure_client_secret: Option<SecretString>,
    azure_tenant_id: Option<String>,
    azure_workspace_resource_id: Option<String>,
    http: kernel::HttpConfig,
    proxy: kernel::ProxyConfig,
    tls_ca_file: Option<String>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_accept_self_signed: bool,
    tls_skip_hostname_verification: bool,
    tls_client_cert_file: Option<String>,
    tls_client_cert_pem: Option<Vec<u8>>,
    tls_client_key_file: Option<String>,
    tls_client_key_pem: Option<SecretBytes>,
    logging: kernel::LoggingConfig,
    telemetry: kernel::TelemetryConfig,
    result: kernel::ResultConfig,
    session_conf: HashMap<String, String>,
    query_tags: Option<String>,
    escape_catalog_name: Option<bool>,
    geospatial: Preferred<kernel::GeospatialResultRepresentation>,
}

impl std::fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("uri", &self.uri.as_ref().map(|_| "<set>"))
            .field("host", &self.host.get())
            .field("http_path", &self.http_path.get())
            .field("warehouse_id", &self.warehouse_id)
            .field("catalog", &self.catalog.get())
            .field("schema", &self.schema.get())
            .field("user_agent_entry", &self.user_agent_entry.get())
            .field("auth_mode", &self.auth_mode.get())
            .field("token", &self.token.get())
            .field("oauth_client_secret", &self.oauth_client_secret)
            .field("token_cache_passphrase", &self.token_cache_passphrase)
            .field("jwt_passphrase", &self.jwt_passphrase)
            .field("azure_client_secret", &self.azure_client_secret)
            .field(
                "sea_response_compression_enabled",
                &self.http.sea_response_compression_enabled,
            )
            .field("http_request_timeout", &self.http.request_timeout)
            .field("http_overall_timeout", &self.http.overall_timeout)
            .field(
                "http_pool_max_idle_per_host",
                &self.http.pool_max_idle_per_host,
            )
            .field("http_retry_min_wait", &self.http.retry_min_wait)
            .field("http_retry_max_wait", &self.http.retry_max_wait)
            .field("http_retry_max_retries", &self.http.retry_max_retries)
            .field("proxy", &self.proxy)
            .field("tls_client_key_pem", &self.tls_client_key_pem)
            .field("session_conf_count", &self.session_conf.len())
            .field("custom_header_count", &self.http.custom_headers.len())
            .finish_non_exhaustive()
    }
}

pub(crate) struct ResolvedConfig {
    pub(crate) host: String,
    pub(crate) http_path: Option<String>,
    pub(crate) warehouse_id: Option<String>,
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) user_agent_entry: Option<String>,
    pub(crate) auth: kernel::AuthConfig,
    pub(crate) http: kernel::HttpConfig,
    pub(crate) proxy: Option<kernel::ProxyConfig>,
    pub(crate) tls: kernel::TlsConfig,
    pub(crate) logging: kernel::LoggingConfig,
    pub(crate) telemetry: kernel::TelemetryConfig,
    pub(crate) result: kernel::ResultConfig,
    pub(crate) session_conf: HashMap<String, String>,
    pub(crate) escape_catalog_name: bool,
    pub(crate) geospatial: kernel::GeospatialResultRepresentation,
}

impl DatabaseConfig {
    pub(crate) fn set_option(&mut self, key: &str, value: OptionValue) -> Result<()> {
        if let Some(name) = key.strip_prefix(SESSION_CONF_PREFIX) {
            if name.is_empty() {
                return Err(adbc_error(
                    "session-conf name must not be empty",
                    Status::InvalidArguments,
                ));
            }
            self.session_conf
                .insert(name.to_string(), string_value(value, key)?);
            return Ok(());
        }
        if let Some(name) = strip_prefix_ignore_ascii_case(key, SERVER_SIDE_PROPERTY_PREFIX) {
            validate_server_side_property_name(name)?;
            self.session_conf
                .insert(name.to_string(), string_value(value, key)?);
            return Ok(());
        }
        match key {
            adbc_core::constants::ADBC_OPTION_URI => self.uri = Some(string_value(value, key)?),
            HOST => self.host.set(string_value(value, key)?, true),
            "databricks.host" => self.host.set(string_value(value, key)?, false),
            HTTP_PATH => self.http_path.set(string_value(value, key)?, true),
            "databricks.http_path" | "http_path" | "HTTPPath" => {
                self.http_path.set(string_value(value, key)?, false)
            }
            WAREHOUSE_ID | "databricks.warehouse_id" => {
                self.warehouse_id = Some(string_value(value, key)?)
            }
            adbc_core::constants::ADBC_OPTION_USERNAME => {
                self.username = Some(string_value(value, key)?)
            }
            adbc_core::constants::ADBC_OPTION_PASSWORD => {
                self.password = Some(SecretString::new(string_value(value, key)?))
            }
            ACCESS_TOKEN => self
                .token
                .set(SecretString::new(string_value(value, key)?), true),
            "databricks.access_token" | "access_token" | "token" | "Auth_AccessToken" => self
                .token
                .set(SecretString::new(string_value(value, key)?), false),
            CATALOG => self.catalog.set(string_value(value, key)?, false),
            SCHEMA => self.schema.set(string_value(value, key)?, false),
            USER_AGENT_ENTRY => self
                .user_agent_entry
                .set(user_agent_entry(value, key)?, true),
            USER_AGENT_ENTRY_ALIAS => self
                .user_agent_entry
                .set(user_agent_entry(value, key)?, false),
            "adbc.connection.catalog" => self.catalog.set(string_value(value, key)?, true),
            "adbc.connection.db_schema" => self.schema.set(string_value(value, key)?, true),
            AUTH_TYPE => self
                .auth_mode
                .set(parse_auth_mode(&string_value(value, key)?)?, true),
            GRANT_TYPE => self
                .auth_mode
                .set(parse_grant_type(&string_value(value, key)?)?, false),
            OAUTH_CLIENT_ID => self.oauth_client_id = Some(string_value(value, key)?),
            OAUTH_CLIENT_SECRET => {
                self.oauth_client_secret = Some(SecretString::new(string_value(value, key)?))
            }
            OAUTH_SCOPES => self
                .oauth_scopes
                .set(parse_scopes(&string_value(value, key)?), true),
            OAUTH_SCOPE => self
                .oauth_scopes
                .set(parse_scopes(&string_value(value, key)?), false),
            OAUTH_TOKEN_URL => self.oauth_token_url = Some(string_value(value, key)?),
            OAUTH_REDIRECT_PORTS => {
                self.oauth_redirect_ports = Some(parse_ports(&string_value(value, key)?, key)?)
            }
            OAUTH_CALLBACK_TIMEOUT_MS => {
                self.oauth_callback_timeout = Some(duration(value, key, false)?)
            }
            TOKEN_CACHE_ENABLED => self.token_cache_enabled = Some(boolean(value, key)?),
            TOKEN_CACHE_PASSPHRASE => {
                self.token_cache_passphrase = Some(SecretString::new(string_value(value, key)?))
            }
            IDENTITY_FEDERATION_CLIENT_ID => {
                self.identity_federation_client_id = Some(string_value(value, key)?)
            }
            JWT_KEY_FILE | "Auth_JWT_Key_File" => {
                self.jwt_key_file = Some(string_value(value, key)?)
            }
            JWT_KID | "Auth_KID" => self.jwt_kid = Some(string_value(value, key)?),
            JWT_PASSPHRASE | "Auth_JWT_Passphrase" => {
                self.jwt_passphrase = Some(SecretString::new(string_value(value, key)?))
            }
            JWT_ALGORITHM | "Auth_JWT_Algorithm" => {
                self.jwt_algorithm = Some(string_value(value, key)?)
            }
            "UseJWTAssertion" => {
                if boolean_compat(value, key)? {
                    self.auth_mode.set(AuthMode::OAuthM2MPrivateKey, false);
                }
            }
            AZURE_CLIENT_ID => self.azure_client_id = Some(string_value(value, key)?),
            AZURE_CLIENT_SECRET => {
                self.azure_client_secret = Some(SecretString::new(string_value(value, key)?))
            }
            AZURE_TENANT_ID => self.azure_tenant_id = Some(string_value(value, key)?),
            AZURE_WORKSPACE_RESOURCE_ID => {
                self.azure_workspace_resource_id = Some(string_value(value, key)?)
            }
            QUERY_TAGS => self.query_tags = Some(string_value(value, key)?),
            ESCAPE_CATALOG_NAME => self.escape_catalog_name = Some(boolean_compat(value, key)?),
            ENABLE_COMPLEX_DATATYPE_SUPPORT => {
                self.result.complex_types_as_json = !boolean(value, key)?
            }
            GEOSPATIAL_REPRESENTATION => self
                .geospatial
                .set(parse_geospatial(&string_value(value, key)?)?, true),
            ENABLE_GEOSPATIAL => self.geospatial.set(
                if boolean(value, key)? {
                    kernel::GeospatialResultRepresentation::Binary
                } else {
                    kernel::GeospatialResultRepresentation::String
                },
                false,
            ),
            CLOUDFETCH_MAX_CHUNKS_IN_MEMORY | "databricks.cloudfetch.max_chunks_in_memory" => {
                self.result.cloudfetch_max_chunks_in_memory = positive_usize_compat(value, key)?
            }
            CLOUDFETCH_ENABLED | "databricks.cloudfetch.enabled" => {
                self.result.cloudfetch_enabled = boolean_compat(value, key)?
            }
            PROTOCOL => {
                let protocol = string_value(value, key)?;
                if !protocol.eq_ignore_ascii_case("rest") {
                    return Err(unsupported(&format!(
                        "database option {key:?} value {protocol:?}; only REST/SEA is supported"
                    )));
                }
            }
            "adbc.databricks.sea_response_compression_enabled" => {
                self.http.sea_response_compression_enabled = boolean(value, key)?
            }
            "adbc.databricks.http_request_timeout" => {
                self.http.request_timeout = duration_seconds(value, key)?
            }
            "SocketTimeout" => {
                let timeout = duration(value, key, false)?;
                self.http.request_timeout = timeout;
                self.http.storage_read_timeout = Some(timeout);
            }
            _ if key.starts_with(HTTP_PREFIX) => self.set_http(key, value)?,
            _ if key.starts_with(PROXY_PREFIX) => self.set_proxy(key, value)?,
            _ if key.starts_with(TLS_PREFIX) => self.set_tls(key, value)?,
            _ if key.starts_with(TELEMETRY_PREFIX) => self.set_telemetry(key, value)?,
            _ if key.starts_with(LOG_PREFIX) => self.set_logging(key, value)?,
            _ => return Err(unsupported(&format!("database option {key:?}"))),
        }
        Ok(())
    }

    fn set_http(&mut self, key: &str, value: OptionValue) -> Result<()> {
        match key {
            "adbc.databricks.http.request_timeout_ms" => {
                self.http.request_timeout = duration(value, key, false)?
            }
            "adbc.databricks.http.overall_timeout_ms" => {
                self.http.overall_timeout = duration(value, key, false)?
            }
            "adbc.databricks.http.pool_max_idle_per_host" => {
                self.http.pool_max_idle_per_host = positive_usize(value, key)?
            }
            "adbc.databricks.http.retry_min_wait_ms" => {
                self.http.retry_min_wait = duration(value, key, true)?
            }
            "adbc.databricks.http.retry_max_wait_ms" => {
                self.http.retry_max_wait = duration(value, key, true)?
            }
            "adbc.databricks.http.retry_max_retries" => {
                self.http.retry_max_retries = nonnegative_u32(value, key)?
            }
            "adbc.databricks.http.headers" => {
                self.http.custom_headers = parse_headers(&string_value(value, key)?)?
            }
            _ => return Err(unsupported(&format!("database option {key:?}"))),
        }
        Ok(())
    }

    fn set_proxy(&mut self, key: &str, value: OptionValue) -> Result<()> {
        match key {
            "adbc.databricks.proxy.url" => self.proxy.url = Some(string_value(value, key)?),
            "adbc.databricks.proxy.username" => {
                self.proxy.username = Some(string_value(value, key)?)
            }
            "adbc.databricks.proxy.password" => {
                self.proxy.password = Some(string_value(value, key)?)
            }
            "adbc.databricks.proxy.bypass_hosts" => {
                self.proxy.bypass_hosts = Some(string_value(value, key)?)
            }
            _ => return Err(unsupported(&format!("database option {key:?}"))),
        }
        Ok(())
    }

    fn set_tls(&mut self, key: &str, value: OptionValue) -> Result<()> {
        match key {
            "adbc.databricks.tls.ca_file" => self.tls_ca_file = Some(string_value(value, key)?),
            "adbc.databricks.tls.ca_pem" => self.tls_ca_pem = Some(bytes_value(value, key)?),
            "adbc.databricks.tls.accept_self_signed" => {
                self.tls_accept_self_signed = boolean(value, key)?
            }
            "adbc.databricks.tls.skip_hostname_verification" => {
                self.tls_skip_hostname_verification = boolean(value, key)?
            }
            "adbc.databricks.tls.client_cert_file" => {
                self.tls_client_cert_file = Some(string_value(value, key)?)
            }
            "adbc.databricks.tls.client_cert_pem" => {
                self.tls_client_cert_pem = Some(bytes_value(value, key)?)
            }
            "adbc.databricks.tls.client_key_file" => {
                self.tls_client_key_file = Some(string_value(value, key)?)
            }
            "adbc.databricks.tls.client_key_pem" => {
                self.tls_client_key_pem = Some(SecretBytes::new(bytes_value(value, key)?))
            }
            _ => return Err(unsupported(&format!("database option {key:?}"))),
        }
        Ok(())
    }

    fn set_logging(&mut self, key: &str, value: OptionValue) -> Result<()> {
        match key {
            "adbc.databricks.log.filter" => self.logging.filter = Some(string_value(value, key)?),
            "adbc.databricks.log.file" => {
                self.logging.file = Some(PathBuf::from(string_value(value, key)?))
            }
            _ => return Err(unsupported(&format!("database option {key:?}"))),
        }
        Ok(())
    }

    fn set_telemetry(&mut self, key: &str, value: OptionValue) -> Result<()> {
        match key {
            "adbc.databricks.telemetry.enabled" => {
                self.telemetry.enabled = Some(boolean(value, key)?)
            }
            "adbc.databricks.telemetry.batch_size" => {
                self.telemetry.batch_size = positive_usize_compat(value, key)?
            }
            "adbc.databricks.telemetry.flush_interval_ms" => {
                self.telemetry.flush_interval = duration_compat(value, key, false)?
            }
            "adbc.databricks.telemetry.max_retries" => {
                self.telemetry.max_retries = nonnegative_u32_compat(value, key)?
            }
            "adbc.databricks.telemetry.retry_delay_ms" => {
                self.telemetry.retry_delay = duration_compat(value, key, true)?
            }
            "adbc.databricks.telemetry.close_flush_timeout_ms" => {
                self.telemetry.close_flush_timeout = duration_compat(value, key, false)?
            }
            "adbc.databricks.telemetry.circuit_breaker_enabled" => {
                self.telemetry.circuit_breaker_enabled = boolean(value, key)?
            }
            "adbc.databricks.telemetry.circuit_breaker_threshold" => {
                self.telemetry.circuit_breaker_threshold = nonnegative_u32_compat(value, key)?
            }
            "adbc.databricks.telemetry.circuit_breaker_timeout_ms" => {
                self.telemetry.circuit_breaker_timeout = duration_compat(value, key, false)?
            }
            _ => return Err(unsupported(&format!("database option {key:?}"))),
        }
        Ok(())
    }

    pub(crate) fn get_string(&self, key: &str) -> Result<String> {
        if is_secret_key(key) {
            return Err(unsupported(&format!("reading secret option {key:?}")));
        }
        let value = if let Some(name) = key.strip_prefix(SESSION_CONF_PREFIX) {
            self.session_conf.get(name).cloned()
        } else if let Some(name) = strip_prefix_ignore_ascii_case(key, SERVER_SIDE_PROPERTY_PREFIX)
        {
            validate_server_side_property_name(name)?;
            self.session_conf.get(name).cloned()
        } else {
            match key {
                adbc_core::constants::ADBC_OPTION_URI => self.uri.clone(),
                HOST => self.host.get().cloned(),
                HTTP_PATH | "databricks.http_path" => self.http_path.get().cloned(),
                WAREHOUSE_ID | "databricks.warehouse_id" => self.warehouse_id.clone(),
                CATALOG | "adbc.connection.catalog" => self.catalog.get().cloned(),
                SCHEMA | "adbc.connection.db_schema" => self.schema.get().cloned(),
                USER_AGENT_ENTRY | USER_AGENT_ENTRY_ALIAS => self.user_agent_entry.get().cloned(),
                AUTH_TYPE => self
                    .auth_mode
                    .get()
                    .map(|mode| auth_mode_name(*mode).to_string()),
                GEOSPATIAL_REPRESENTATION => self.geospatial.get().map(|value| match value {
                    kernel::GeospatialResultRepresentation::String => "string".to_string(),
                    kernel::GeospatialResultRepresentation::Binary => "binary".to_string(),
                    _ => "unknown".to_string(),
                }),
                ESCAPE_CATALOG_NAME => Some(self.escape_catalog_name.unwrap_or(true).to_string()),
                _ => None,
            }
        };
        value.ok_or_else(|| {
            adbc_error(
                format!("database option {key:?} is not set"),
                Status::NotFound,
            )
        })
    }

    pub(crate) fn resolve(&self) -> Result<ResolvedConfig> {
        self.validate()?;
        let (uri_host, uri_path, uri_warehouse) = parse_uri(self.uri.as_deref())?;
        let host = self.host.get().cloned().or(uri_host).ok_or_else(|| {
            adbc_error(
                "required database option uri or adbc.databricks.host is missing",
                Status::InvalidState,
            )
        })?;
        let http_path = self.http_path.get().cloned().or(uri_path);
        let warehouse_id = self.warehouse_id.clone().or(uri_warehouse);
        if http_path.is_some() == warehouse_id.is_some() {
            return Err(adbc_error(
                "exactly one of http_path or warehouse_id is required",
                Status::InvalidState,
            ));
        }
        if (self.tls_client_cert_file.is_some() || self.tls_client_cert_pem.is_some())
            && !host.to_ascii_lowercase().starts_with("https://")
        {
            return Err(adbc_error(
                "mTLS requires an HTTPS workspace URI",
                Status::InvalidArguments,
            ));
        }
        let auth = self.resolve_auth()?;
        let tls = self.resolve_tls()?;
        let mut session_conf = self.session_conf.clone();
        if let Some(tags) = &self.query_tags {
            if let Some(existing) = session_conf.get("QUERY_TAGS") {
                if existing != tags {
                    return Err(adbc_error(
                        "adbc.databricks.query_tags conflicts with session_conf.QUERY_TAGS",
                        Status::InvalidArguments,
                    ));
                }
            } else {
                session_conf.insert("QUERY_TAGS".to_string(), tags.clone());
            }
        }
        Ok(ResolvedConfig {
            host,
            http_path,
            warehouse_id,
            catalog: self.catalog.get().cloned(),
            schema: self.schema.get().cloned(),
            user_agent_entry: self.user_agent_entry.get().cloned(),
            auth,
            http: self.http.clone(),
            proxy: self.proxy.url.as_ref().map(|_| self.proxy.clone()),
            tls,
            logging: self.logging.clone(),
            telemetry: self.telemetry.clone(),
            result: self.result.clone(),
            session_conf,
            escape_catalog_name: self.escape_catalog_name.unwrap_or(true),
            geospatial: self.geospatial.get().copied().unwrap_or_default(),
        })
    }

    fn validate(&self) -> Result<()> {
        if self.http_path.get().is_some() && self.warehouse_id.is_some() {
            return Err(adbc_error(
                "http_path and warehouse_id are mutually exclusive",
                Status::InvalidArguments,
            ));
        }
        if self.http.retry_min_wait > self.http.retry_max_wait {
            return Err(adbc_error(
                "retry_min_wait_ms must not exceed retry_max_wait_ms",
                Status::InvalidArguments,
            ));
        }
        if self.proxy.username.is_some() && self.proxy.url.is_none() {
            return Err(adbc_error(
                "proxy username requires proxy URL",
                Status::InvalidArguments,
            ));
        }
        if self.proxy.password.is_some() && self.proxy.username.is_none() {
            return Err(adbc_error(
                "proxy password requires proxy username",
                Status::InvalidArguments,
            ));
        }
        if let Some(url) = &self.proxy.url {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(adbc_error(
                    "proxy URL must use http or https",
                    Status::InvalidArguments,
                ));
            }
        }
        if self
            .proxy
            .url
            .as_deref()
            .is_some_and(proxy_url_has_userinfo)
        {
            return Err(adbc_error(
                "proxy URL must not contain embedded credentials; use proxy username and password options",
                Status::InvalidArguments,
            ));
        }
        if self.tls_ca_file.is_some() && self.tls_ca_pem.is_some() {
            return Err(adbc_error(
                "tls.ca_file and tls.ca_pem are mutually exclusive",
                Status::InvalidArguments,
            ));
        }
        if self.tls_client_cert_file.is_some() && self.tls_client_cert_pem.is_some() {
            return Err(adbc_error(
                "client certificate file and bytes are mutually exclusive",
                Status::InvalidArguments,
            ));
        }
        if self.tls_client_key_file.is_some() && self.tls_client_key_pem.is_some() {
            return Err(adbc_error(
                "client key file and bytes are mutually exclusive",
                Status::InvalidArguments,
            ));
        }
        let has_cert = self.tls_client_cert_file.is_some() || self.tls_client_cert_pem.is_some();
        let has_key = self.tls_client_key_file.is_some() || self.tls_client_key_pem.is_some();
        if has_cert != has_key {
            return Err(adbc_error(
                "mTLS client certificate and key must be supplied together",
                Status::InvalidArguments,
            ));
        }
        if self.telemetry.circuit_breaker_enabled && self.telemetry.circuit_breaker_threshold == 0 {
            return Err(adbc_error(
                "telemetry circuit breaker threshold must be positive",
                Status::InvalidArguments,
            ));
        }
        Ok(())
    }

    fn resolve_auth(&self) -> Result<kernel::AuthConfig> {
        let mode = self
            .auth_mode
            .get()
            .copied()
            .or_else(|| self.infer_auth_mode())
            .ok_or_else(|| {
                adbc_error(
                    "authentication type cannot be inferred",
                    Status::InvalidState,
                )
            })?;
        let federation = self.identity_federation_client_id.clone();
        match mode {
            AuthMode::Pat => kernel::AuthConfig::pat(
                self.token
                    .get()
                    .or(self.password.as_ref())
                    .map(|v| v.expose().to_string()),
                federation,
            )
            .map_err(map_kernel_error),
            AuthMode::OAuthM2M => kernel::AuthConfig::oauth_m2m(
                self.oauth_client_id.clone().or(self.username.clone()),
                self.oauth_client_secret
                    .as_ref()
                    .or(self.password.as_ref())
                    .map(|v| v.expose().to_string()),
                self.oauth_scopes.get().cloned(),
                self.oauth_token_url.clone(),
                federation,
            )
            .map_err(map_kernel_error),
            AuthMode::OAuthU2M => Ok(kernel::AuthConfig::oauth_u2m(
                self.oauth_client_id.clone(),
                self.oauth_redirect_ports.clone(),
                self.oauth_scopes.get().cloned(),
                self.oauth_token_url.clone(),
                self.oauth_callback_timeout,
                Some(kernel::TokenCacheConfig {
                    enabled: self.token_cache_enabled.unwrap_or(true),
                    passphrase: self
                        .token_cache_passphrase
                        .as_ref()
                        .map(|v| v.expose().to_string()),
                }),
                federation,
            )),
            AuthMode::OAuthM2MPrivateKey => kernel::AuthConfig::oauth_m2m_private_key(
                self.oauth_client_id.clone().or(self.username.clone()),
                self.jwt_key_file.clone(),
                self.jwt_kid.clone(),
                self.jwt_passphrase.as_ref().map(|v| v.expose().to_string()),
                self.jwt_algorithm.clone(),
                self.oauth_scopes.get().cloned(),
                self.oauth_token_url.clone(),
            )
            .map_err(map_kernel_error),
            AuthMode::AzureServicePrincipal => kernel::AuthConfig::azure_service_principal(
                self.azure_client_id.clone(),
                self.azure_client_secret
                    .as_ref()
                    .map(|v| v.expose().to_string()),
                self.azure_tenant_id.clone(),
                self.azure_workspace_resource_id.clone(),
                federation,
            )
            .map_err(map_kernel_error),
            AuthMode::OAuthFederation => Err(unsupported("direct OAuth federation (kernel gap)")),
        }
    }

    fn infer_auth_mode(&self) -> Option<AuthMode> {
        if self.azure_client_id.is_some() || self.azure_client_secret.is_some() {
            Some(AuthMode::AzureServicePrincipal)
        } else if self.jwt_key_file.is_some() || self.jwt_kid.is_some() {
            Some(AuthMode::OAuthM2MPrivateKey)
        } else if self.oauth_client_secret.is_some() {
            Some(AuthMode::OAuthM2M)
        } else if self.token.get().is_some() || self.password.is_some() {
            Some(AuthMode::Pat)
        } else {
            None
        }
    }

    fn resolve_tls(&self) -> Result<kernel::TlsConfig> {
        let read = |path: &str, option: &str| {
            std::fs::read(path).map_err(|_| {
                adbc_error(
                    format!("could not read file configured by {option}"),
                    Status::InvalidArguments,
                )
            })
        };
        let ca = match (&self.tls_ca_pem, &self.tls_ca_file) {
            (Some(bytes), _) => Some(bytes.clone()),
            (_, Some(path)) => Some(read(path, "adbc.databricks.tls.ca_file")?),
            _ => None,
        };
        let cert = match (&self.tls_client_cert_pem, &self.tls_client_cert_file) {
            (Some(bytes), _) => Some(bytes.clone()),
            (_, Some(path)) => Some(read(path, "adbc.databricks.tls.client_cert_file")?),
            _ => None,
        };
        let key = match (&self.tls_client_key_pem, &self.tls_client_key_file) {
            (Some(bytes), _) => Some(bytes.expose().to_vec()),
            (_, Some(path)) => Some(read(path, "adbc.databricks.tls.client_key_file")?),
            _ => None,
        };
        let mut tls = kernel::TlsConfig::default();
        tls.custom_ca_cert = ca;
        tls.accept_self_signed = self.tls_accept_self_signed;
        tls.skip_hostname_verification = self.tls_skip_hostname_verification;
        tls.client_cert_pem = cert;
        tls.client_key_pem = key;
        Ok(tls)
    }
}

fn strip_prefix_ignore_ascii_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let candidate = value.get(..prefix.len())?;
    candidate
        .eq_ignore_ascii_case(prefix)
        .then(|| &value[prefix.len()..])
}

fn validate_server_side_property_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
    {
        return Err(adbc_error(
            "server-side property name must contain only ASCII letters, digits, dots, and underscores",
            Status::InvalidArguments,
        ));
    }
    Ok(())
}

fn proxy_url_has_userinfo(url: &str) -> bool {
    url.split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(rest))
        .is_some_and(|authority| authority.contains('@'))
}

fn parse_auth_mode(value: &str) -> Result<AuthMode> {
    match value {
        "pat" => Ok(AuthMode::Pat),
        "oauth_m2m" => Ok(AuthMode::OAuthM2M),
        "oauth_u2m" => Ok(AuthMode::OAuthU2M),
        "oauth_m2m_private_key" => Ok(AuthMode::OAuthM2MPrivateKey),
        "azure_service_principal" => Ok(AuthMode::AzureServicePrincipal),
        "oauth_federation" => Ok(AuthMode::OAuthFederation),
        _ => Err(adbc_error(
            format!("invalid authentication type {value:?}"),
            Status::InvalidArguments,
        )),
    }
}
fn parse_grant_type(value: &str) -> Result<AuthMode> {
    match value {
        "access_token" => Ok(AuthMode::Pat),
        "client_credentials" => Ok(AuthMode::OAuthM2M),
        _ => Err(adbc_error(
            "invalid OAuth grant_type",
            Status::InvalidArguments,
        )),
    }
}
fn auth_mode_name(mode: AuthMode) -> &'static str {
    match mode {
        AuthMode::Pat => "pat",
        AuthMode::OAuthM2M => "oauth_m2m",
        AuthMode::OAuthU2M => "oauth_u2m",
        AuthMode::OAuthM2MPrivateKey => "oauth_m2m_private_key",
        AuthMode::AzureServicePrincipal => "azure_service_principal",
        AuthMode::OAuthFederation => "oauth_federation",
    }
}
fn parse_scopes(value: &str) -> Vec<String> {
    value
        .split([',', ' '])
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect()
}
fn parse_ports(value: &str, key: &str) -> Result<Vec<u16>> {
    value
        .split(',')
        .map(|v| {
            v.trim().parse::<u16>().map_err(|_| {
                adbc_error(
                    format!("option {key:?} requires comma-separated ports"),
                    Status::InvalidArguments,
                )
            })
        })
        .collect()
}
fn parse_geospatial(value: &str) -> Result<kernel::GeospatialResultRepresentation> {
    match value {
        "string" => Ok(kernel::GeospatialResultRepresentation::String),
        "binary" => Ok(kernel::GeospatialResultRepresentation::Binary),
        _ => Err(adbc_error(
            "geospatial result representation must be string or binary",
            Status::InvalidArguments,
        )),
    }
}
fn string_value(value: OptionValue, key: &str) -> Result<String> {
    match value {
        OptionValue::String(value) => Ok(value),
        _ => Err(adbc_error(
            format!("option {key:?} requires a string"),
            Status::InvalidArguments,
        )),
    }
}
fn user_agent_entry(value: OptionValue, key: &str) -> Result<String> {
    let value = string_value(value, key)?;
    if value.chars().any(char::is_control) {
        return Err(adbc_error(
            format!("option {key:?} must not contain control characters"),
            Status::InvalidArguments,
        ));
    }
    Ok(value)
}
fn bytes_value(value: OptionValue, key: &str) -> Result<Vec<u8>> {
    match value {
        OptionValue::Bytes(value) => Ok(value),
        _ => Err(adbc_error(
            format!("option {key:?} requires bytes"),
            Status::InvalidArguments,
        )),
    }
}
fn boolean(value: OptionValue, key: &str) -> Result<bool> {
    bool::try_from(value).map_err(|_| {
        adbc_error(
            format!("option {key:?} requires true or false"),
            Status::InvalidArguments,
        )
    })
}
fn boolean_compat(value: OptionValue, key: &str) -> Result<bool> {
    match value {
        OptionValue::Int(0) => Ok(false),
        OptionValue::Int(1) => Ok(true),
        other => boolean(other, key),
    }
}
fn integer(value: OptionValue, key: &str) -> Result<i64> {
    match value {
        OptionValue::Int(value) => Ok(value),
        _ => Err(adbc_error(
            format!("option {key:?} requires an integer"),
            Status::InvalidArguments,
        )),
    }
}
fn integer_compat(value: OptionValue, key: &str) -> Result<i64> {
    match value {
        OptionValue::String(value) => value.parse().map_err(|_| {
            adbc_error(
                format!("option {key:?} requires an integer"),
                Status::InvalidArguments,
            )
        }),
        other => integer(other, key),
    }
}
fn positive_usize(value: OptionValue, key: &str) -> Result<usize> {
    let value = integer(value, key)?;
    usize::try_from(value)
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| {
            adbc_error(
                format!("option {key:?} requires a positive integer"),
                Status::InvalidArguments,
            )
        })
}
fn positive_usize_compat(value: OptionValue, key: &str) -> Result<usize> {
    let value = integer_compat(value, key)?;
    usize::try_from(value)
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| {
            adbc_error(
                format!("option {key:?} requires a positive integer"),
                Status::InvalidArguments,
            )
        })
}
fn nonnegative_u32(value: OptionValue, key: &str) -> Result<u32> {
    u32::try_from(integer(value, key)?).map_err(|_| {
        adbc_error(
            format!("option {key:?} requires a nonnegative integer"),
            Status::InvalidArguments,
        )
    })
}
fn nonnegative_u32_compat(value: OptionValue, key: &str) -> Result<u32> {
    u32::try_from(integer_compat(value, key)?).map_err(|_| {
        adbc_error(
            format!("option {key:?} requires a nonnegative integer"),
            Status::InvalidArguments,
        )
    })
}
fn duration(value: OptionValue, key: &str, allow_zero: bool) -> Result<Duration> {
    let value = integer(value, key)?;
    let value = u64::try_from(value).map_err(|_| {
        adbc_error(
            format!("option {key:?} requires nonnegative milliseconds"),
            Status::InvalidArguments,
        )
    })?;
    if !allow_zero && value == 0 {
        return Err(adbc_error(
            format!("option {key:?} must be positive"),
            Status::InvalidArguments,
        ));
    }
    Ok(Duration::from_millis(value))
}
fn duration_compat(value: OptionValue, key: &str, allow_zero: bool) -> Result<Duration> {
    let value = u64::try_from(integer_compat(value, key)?).map_err(|_| {
        adbc_error(
            format!("option {key:?} requires nonnegative milliseconds"),
            Status::InvalidArguments,
        )
    })?;
    if !allow_zero && value == 0 {
        return Err(adbc_error(
            format!("option {key:?} must be positive"),
            Status::InvalidArguments,
        ));
    }
    Ok(Duration::from_millis(value))
}
fn duration_seconds(value: OptionValue, key: &str) -> Result<Duration> {
    let value = u64::try_from(integer(value, key)?).map_err(|_| {
        adbc_error(
            format!("option {key:?} requires nonnegative seconds"),
            Status::InvalidArguments,
        )
    })?;
    if value == 0 {
        return Err(adbc_error(
            format!("option {key:?} must be positive"),
            Status::InvalidArguments,
        ));
    }
    Ok(Duration::from_secs(value))
}
fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passphrase",
        "private_key",
        "client_key",
        "credential",
    ]
    .iter()
    .any(|part| key.contains(part))
}

fn parse_headers(value: &str) -> Result<Vec<(String, String)>> {
    let entries: Vec<HashMap<String, String>> = serde_json::from_str(value).map_err(|_| {
        adbc_error(
            "adbc.databricks.http.headers must be a JSON array of string name/value objects",
            Status::InvalidArguments,
        )
    })?;
    entries
        .into_iter()
        .map(|mut entry| {
            let name = entry.remove("name").ok_or_else(|| {
                adbc_error(
                    "HTTP header entry is missing name",
                    Status::InvalidArguments,
                )
            })?;
            let value = entry.remove("value").ok_or_else(|| {
                adbc_error(
                    "HTTP header entry is missing value",
                    Status::InvalidArguments,
                )
            })?;
            if !entry.is_empty() {
                return Err(adbc_error(
                    "HTTP header entry contains unknown fields",
                    Status::InvalidArguments,
                ));
            }
            let lower = name.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "authorization"
                    | "host"
                    | "content-length"
                    | "user-agent"
                    | "x-databricks-org-id"
                    | "x-request-id"
                    | "traceparent"
                    | "tracestate"
            ) {
                return Err(adbc_error(
                    format!("HTTP header {name:?} is managed by the driver"),
                    Status::InvalidArguments,
                ));
            }
            Ok((name, value))
        })
        .collect()
}

fn parse_uri(uri: Option<&str>) -> Result<(Option<String>, Option<String>, Option<String>)> {
    let Some(uri) = uri else {
        return Ok((None, None, None));
    };
    if uri.contains('@') {
        return Err(adbc_error(
            "URI must not contain embedded credentials",
            Status::InvalidArguments,
        ));
    }
    if uri.starts_with("http://") || uri.starts_with("https://") {
        return Ok((Some(uri.to_string()), None, None));
    }
    let rest = uri.strip_prefix("databricks://").ok_or_else(|| {
        adbc_error(
            "URI must use https or databricks scheme",
            Status::InvalidArguments,
        )
    })?;
    let (host, query) = rest.split_once('?').unwrap_or((rest, ""));
    if host.is_empty() {
        return Err(adbc_error(
            "Databricks URI host is empty",
            Status::InvalidArguments,
        ));
    }
    let mut path = None;
    let mut warehouse = None;
    for item in query.split('&').filter(|item| !item.is_empty()) {
        let (key, value) = item.split_once('=').unwrap_or((item, ""));
        match key {
            "http_path" => path = Some(value.to_string()),
            "warehouse_id" => warehouse = Some(value.to_string()),
            _ => {}
        }
    }
    Ok((Some(format!("https://{host}")), path, warehouse))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(config: &mut DatabaseConfig, key: &str, value: impl Into<OptionValue>) {
        config.set_option(key, value.into()).unwrap();
    }

    #[test]
    fn debug_redacts_every_secret_and_value_bag() {
        let mut config = DatabaseConfig::default();
        set(&mut config, ACCESS_TOKEN, "sentinel-token");
        set(&mut config, OAUTH_CLIENT_SECRET, "sentinel-client-secret");
        set(
            &mut config,
            TOKEN_CACHE_PASSPHRASE,
            "sentinel-cache-passphrase",
        );
        set(&mut config, JWT_PASSPHRASE, "sentinel-jwt-passphrase");
        set(&mut config, AZURE_CLIENT_SECRET, "sentinel-azure-secret");
        set(
            &mut config,
            "adbc.databricks.proxy.password",
            "sentinel-proxy-password",
        );
        set(
            &mut config,
            "adbc.databricks.http.headers",
            r#"[{"name":"x-custom","value":"sentinel-header"}]"#,
        );
        set(
            &mut config,
            "adbc.databricks.session_conf.secretish",
            "sentinel-conf",
        );
        let debug = format!("{config:?}");
        for sentinel in [
            "sentinel-token",
            "sentinel-client-secret",
            "sentinel-cache-passphrase",
            "sentinel-jwt-passphrase",
            "sentinel-azure-secret",
            "sentinel-proxy-password",
            "sentinel-header",
            "sentinel-conf",
        ] {
            assert!(!debug.contains(sentinel));
        }
    }

    #[test]
    fn catalog_name_escaping_defaults_true_and_can_be_disabled() {
        let mut config = DatabaseConfig::default();
        assert_eq!(config.get_string(ESCAPE_CATALOG_NAME).unwrap(), "true");
        set(&mut config, ESCAPE_CATALOG_NAME, "false");
        assert_eq!(config.get_string(ESCAPE_CATALOG_NAME).unwrap(), "false");
    }

    #[test]
    fn cloudfetch_max_chunks_option_configures_kernel_result_buffering() {
        let mut config = DatabaseConfig::default();
        set(&mut config, CLOUDFETCH_MAX_CHUNKS_IN_MEMORY, "8");
        assert_eq!(config.result.cloudfetch_max_chunks_in_memory, 8);

        set(
            &mut config,
            "databricks.cloudfetch.max_chunks_in_memory",
            "1",
        );
        assert_eq!(config.result.cloudfetch_max_chunks_in_memory, 1);

        let error = config
            .set_option(CLOUDFETCH_MAX_CHUNKS_IN_MEMORY, "0".into())
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn cloudfetch_enabled_option_configures_kernel_result_delivery() {
        let mut config = DatabaseConfig::default();
        set(&mut config, CLOUDFETCH_ENABLED, "false");
        assert!(!config.result.cloudfetch_enabled);

        set(&mut config, "databricks.cloudfetch.enabled", "true");
        assert!(config.result.cloudfetch_enabled);
    }

    #[test]
    fn complex_datatype_support_configures_kernel_result_shape() {
        let mut config = DatabaseConfig::default();

        set(&mut config, ENABLE_COMPLEX_DATATYPE_SUPPORT, "false");
        assert!(config.result.complex_types_as_json);

        set(&mut config, ENABLE_COMPLEX_DATATYPE_SUPPORT, "true");
        assert!(!config.result.complex_types_as_json);

        let error = config
            .set_option(ENABLE_COMPLEX_DATATYPE_SUPPORT, "not-a-boolean".into())
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn generic_ssp_options_configure_kernel_session_confs() {
        let mut config = DatabaseConfig::default();
        set(&mut config, "adbc.databricks.ssp_ansi_mode", "true");
        set(&mut config, "adbc.databricks.ssp_timezone", "UTC");
        set(
            &mut config,
            "ADBC.DATABRICKS.SSP_spark.sql.ansi.enabled",
            "false",
        );

        assert_eq!(
            config.session_conf.get("ansi_mode"),
            Some(&"true".to_string())
        );
        assert_eq!(
            config.session_conf.get("timezone"),
            Some(&"UTC".to_string())
        );
        assert_eq!(
            config.session_conf.get("spark.sql.ansi.enabled"),
            Some(&"false".to_string())
        );
        assert_eq!(
            config.get_string("adbc.databricks.ssp_ansi_mode").unwrap(),
            "true"
        );
        assert_eq!(
            config
                .get_string("ADBC.DATABRICKS.SSP_spark.sql.ansi.enabled")
                .unwrap(),
            "false"
        );
    }

    #[test]
    fn generic_ssp_options_reject_empty_or_unsafe_names() {
        let mut config = DatabaseConfig::default();
        for key in [
            "adbc.databricks.ssp_",
            "adbc.databricks.ssp_bad name",
            "adbc.databricks.ssp_also;bad",
            "adbc.databricks.ssp_non-ascii-é",
        ] {
            let error = config.set_option(key, "value".into()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{key}");
        }
    }

    #[test]
    fn session_conf_and_ssp_collisions_use_last_set_value() {
        let mut config = DatabaseConfig::default();
        set(
            &mut config,
            "adbc.databricks.session_conf.use_cached_result",
            "true",
        );
        set(
            &mut config,
            "adbc.databricks.ssp_use_cached_result",
            "false",
        );
        assert_eq!(
            config.session_conf.get("use_cached_result"),
            Some(&"false".to_string())
        );

        set(
            &mut config,
            "adbc.databricks.session_conf.use_cached_result",
            "true",
        );
        assert_eq!(
            config
                .get_string("adbc.databricks.ssp_use_cached_result")
                .unwrap(),
            "true"
        );
    }

    #[test]
    fn protocol_option_accepts_rest_only() {
        let mut config = DatabaseConfig::default();
        set(&mut config, PROTOCOL, "rest");
        set(&mut config, PROTOCOL, "REST");

        let error = config.set_option(PROTOCOL, "thrift".into()).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
    }

    #[test]
    fn proxy_url_rejects_embedded_credentials() {
        let mut config = DatabaseConfig::default();
        set(
            &mut config,
            "adbc.databricks.proxy.url",
            "https://user:secret@proxy.example.com:8443",
        );
        let error = config.validate().unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(!error.message.contains("secret"));

        set(
            &mut config,
            "adbc.databricks.proxy.url",
            "https://proxy.example.com:8443/path@segment",
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn proxy_basic_auth_reaches_kernel_config() {
        let mut config = DatabaseConfig::default();
        set(&mut config, HOST, "https://example.databricks.com");
        set(&mut config, HTTP_PATH, "/sql/1.0/warehouses/warehouse-id");
        set(&mut config, ACCESS_TOKEN, "sentinel-token");
        set(
            &mut config,
            "adbc.databricks.proxy.url",
            "https://proxy.example.com:8443",
        );
        set(&mut config, "adbc.databricks.proxy.username", "proxy-user");
        set(
            &mut config,
            "adbc.databricks.proxy.password",
            "sentinel-proxy-password",
        );
        set(
            &mut config,
            "adbc.databricks.proxy.bypass_hosts",
            "localhost,.example.com",
        );

        let proxy = config.resolve().unwrap().proxy.unwrap();
        assert_eq!(proxy.url.as_deref(), Some("https://proxy.example.com:8443"));
        assert_eq!(proxy.username.as_deref(), Some("proxy-user"));
        assert_eq!(proxy.password.as_deref(), Some("sentinel-proxy-password"));
        assert_eq!(
            proxy.bypass_hosts.as_deref(),
            Some("localhost,.example.com")
        );
        assert!(!format!("{proxy:?}").contains("sentinel-proxy-password"));
    }

    #[test]
    fn logging_options_reach_kernel_config() {
        let mut config = DatabaseConfig::default();
        set(&mut config, HOST, "https://example.databricks.com");
        set(&mut config, HTTP_PATH, "/sql/1.0/warehouses/warehouse-id");
        set(&mut config, ACCESS_TOKEN, "sentinel-token");
        set(
            &mut config,
            "adbc.databricks.log.filter",
            "databricks::sql::kernel=debug",
        );
        set(
            &mut config,
            "adbc.databricks.log.file",
            "/tmp/databricks-rust-adbc-test.log",
        );

        let logging = config.resolve().unwrap().logging;
        assert_eq!(
            logging.filter.as_deref(),
            Some("databricks::sql::kernel=debug")
        );
        assert_eq!(
            logging.file.as_deref(),
            Some(std::path::Path::new("/tmp/databricks-rust-adbc-test.log"))
        );
    }

    #[test]
    fn resolved_auth_debug_redacts_every_secret() {
        let mut pat = DatabaseConfig::default();
        set(&mut pat, AUTH_TYPE, "pat");
        set(&mut pat, ACCESS_TOKEN, "sentinel-pat-token");

        let mut m2m = DatabaseConfig::default();
        set(&mut m2m, AUTH_TYPE, "oauth_m2m");
        set(&mut m2m, OAUTH_CLIENT_ID, "client-id");
        set(&mut m2m, OAUTH_CLIENT_SECRET, "sentinel-m2m-secret");

        let mut u2m = DatabaseConfig::default();
        set(&mut u2m, AUTH_TYPE, "oauth_u2m");
        set(
            &mut u2m,
            TOKEN_CACHE_PASSPHRASE,
            "sentinel-cache-passphrase",
        );

        let mut private_key = DatabaseConfig::default();
        set(&mut private_key, AUTH_TYPE, "oauth_m2m_private_key");
        set(&mut private_key, OAUTH_CLIENT_ID, "client-id");
        set(&mut private_key, JWT_KEY_FILE, "/path/to/key.pem");
        set(&mut private_key, JWT_KID, "key-id");
        set(&mut private_key, JWT_PASSPHRASE, "sentinel-jwt-passphrase");

        let mut azure = DatabaseConfig::default();
        set(&mut azure, AUTH_TYPE, "azure_service_principal");
        set(&mut azure, AZURE_CLIENT_ID, "client-id");
        set(&mut azure, AZURE_CLIENT_SECRET, "sentinel-azure-secret");

        for (config, sentinel) in [
            (pat, "sentinel-pat-token"),
            (m2m, "sentinel-m2m-secret"),
            (u2m, "sentinel-cache-passphrase"),
            (private_key, "sentinel-jwt-passphrase"),
            (azure, "sentinel-azure-secret"),
        ] {
            let debug = format!("{:?}", config.resolve_auth().unwrap());
            assert!(!debug.contains(sentinel));
        }
    }

    #[test]
    fn identity_federation_client_id_reaches_supported_auth_variants() {
        fn with_federation(mut config: DatabaseConfig) -> DatabaseConfig {
            set(
                &mut config,
                IDENTITY_FEDERATION_CLIENT_ID,
                "federation-client-id",
            );
            config
        }

        let mut pat = with_federation(DatabaseConfig::default());
        set(&mut pat, ACCESS_TOKEN, "token");

        let mut m2m = with_federation(DatabaseConfig::default());
        set(&mut m2m, OAUTH_CLIENT_ID, "client-id");
        set(&mut m2m, OAUTH_CLIENT_SECRET, "client-secret");

        let mut u2m = with_federation(DatabaseConfig::default());
        set(&mut u2m, AUTH_TYPE, "oauth_u2m");

        let mut azure = with_federation(DatabaseConfig::default());
        set(&mut azure, AZURE_CLIENT_ID, "azure-client-id");
        set(&mut azure, AZURE_CLIENT_SECRET, "azure-client-secret");

        for config in [pat, m2m, u2m, azure] {
            let debug = format!("{:?}", config.resolve_auth().unwrap());
            assert!(debug.contains("federation-client-id"));
        }
    }

    #[test]
    fn u2m_token_cache_options_reach_kernel_auth_config() {
        let mut config = DatabaseConfig::default();
        set(&mut config, AUTH_TYPE, "oauth_u2m");
        set(&mut config, OAUTH_CLIENT_ID, "u2m-client-id");
        set(&mut config, OAUTH_REDIRECT_PORTS, "8020,8025");
        set(&mut config, OAUTH_SCOPES, "sql,offline_access");
        set(&mut config, TOKEN_CACHE_ENABLED, false);
        set(
            &mut config,
            TOKEN_CACHE_PASSPHRASE,
            "sentinel-cache-passphrase",
        );
        set(
            &mut config,
            IDENTITY_FEDERATION_CLIENT_ID,
            "federation-client-id",
        );

        match config.resolve_auth().unwrap() {
            kernel::AuthConfig::OAuthU2M {
                client_id,
                redirect_ports,
                scopes,
                token_cache,
                identity_federation_client_id,
                ..
            } => {
                assert_eq!(client_id.as_deref(), Some("u2m-client-id"));
                assert_eq!(redirect_ports, Some(vec![8020, 8025]));
                assert_eq!(scopes, Some(vec!["sql".into(), "offline_access".into()]));
                let token_cache = token_cache.expect("driver must set token-cache policy");
                assert!(!token_cache.enabled);
                assert_eq!(
                    token_cache.passphrase.as_deref(),
                    Some("sentinel-cache-passphrase")
                );
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client-id")
                );
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn private_key_options_reach_kernel_auth_config() {
        let mut config = DatabaseConfig::default();
        set(&mut config, AUTH_TYPE, "oauth_m2m_private_key");
        set(&mut config, OAUTH_CLIENT_ID, "private-key-client-id");
        set(&mut config, JWT_KEY_FILE, "/path/to/private-key.pem");
        set(&mut config, JWT_KID, "private-key-id");
        set(&mut config, JWT_PASSPHRASE, "sentinel-jwt-passphrase");
        set(&mut config, JWT_ALGORITHM, "PS256");
        set(&mut config, OAUTH_SCOPES, "all-apis,sql");

        match config.resolve_auth().unwrap() {
            kernel::AuthConfig::OAuthM2MPrivateKey {
                client_id,
                jwt_key_file,
                jwt_kid,
                jwt_passphrase,
                jwt_algorithm,
                scopes,
                ..
            } => {
                assert_eq!(client_id, "private-key-client-id");
                assert_eq!(jwt_key_file, "/path/to/private-key.pem");
                assert_eq!(jwt_kid, "private-key-id");
                assert_eq!(jwt_passphrase.as_deref(), Some("sentinel-jwt-passphrase"));
                assert_eq!(jwt_algorithm.as_deref(), Some("PS256"));
                assert_eq!(scopes, Some(vec!["all-apis".into(), "sql".into()]));
            }
            other => panic!("expected OAuthM2MPrivateKey, got {other:?}"),
        }
    }

    #[test]
    fn azure_service_principal_options_reach_kernel_auth_config() {
        let mut config = DatabaseConfig::default();
        set(&mut config, AUTH_TYPE, "azure_service_principal");
        set(&mut config, AZURE_CLIENT_ID, "azure-client-id");
        set(&mut config, AZURE_CLIENT_SECRET, "sentinel-azure-secret");
        set(&mut config, AZURE_TENANT_ID, "azure-tenant-id");
        set(
            &mut config,
            AZURE_WORKSPACE_RESOURCE_ID,
            "/subscriptions/test/resourceGroups/test/providers/Microsoft.Databricks/workspaces/test",
        );
        set(
            &mut config,
            IDENTITY_FEDERATION_CLIENT_ID,
            "federation-client-id",
        );

        match config.resolve_auth().unwrap() {
            kernel::AuthConfig::AzureServicePrincipal {
                azure_client_id,
                azure_client_secret,
                azure_tenant_id,
                azure_workspace_resource_id,
                identity_federation_client_id,
            } => {
                assert_eq!(azure_client_id, "azure-client-id");
                assert_eq!(azure_client_secret, "sentinel-azure-secret");
                assert_eq!(azure_tenant_id.as_deref(), Some("azure-tenant-id"));
                assert_eq!(
                    azure_workspace_resource_id.as_deref(),
                    Some(
                        "/subscriptions/test/resourceGroups/test/providers/Microsoft.Databricks/workspaces/test"
                    )
                );
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client-id")
                );
            }
            other => panic!("expected AzureServicePrincipal, got {other:?}"),
        }
    }

    #[test]
    fn canonical_value_wins_regardless_of_order() {
        let mut config = DatabaseConfig::default();
        set(&mut config, HTTP_PATH, "/sql/1.0/warehouses/canonical");
        set(&mut config, "http_path", "/sql/1.0/warehouses/alias");
        assert_eq!(
            config.http_path.get().unwrap(),
            "/sql/1.0/warehouses/canonical"
        );
    }

    #[test]
    fn secret_options_are_write_only() {
        let mut config = DatabaseConfig::default();
        set(&mut config, ACCESS_TOKEN, "sentinel-token");
        assert_eq!(
            config.get_string(ACCESS_TOKEN).unwrap_err().status,
            Status::NotImplemented
        );
    }

    #[test]
    fn headers_reject_security_sensitive_names() {
        for name in ["Authorization", "User-Agent", "user-agent"] {
            let mut config = DatabaseConfig::default();
            let value = format!(r#"[{{"name":"{name}","value":"secret"}}]"#);
            let error = config
                .set_option("adbc.databricks.http.headers", value.into())
                .unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(!error.message.contains("secret"));
        }
    }

    #[test]
    fn user_agent_entry_round_trips() {
        let mut config = DatabaseConfig::default();
        set(&mut config, USER_AGENT_ENTRY_ALIAS, "legacy-entry");
        set(&mut config, USER_AGENT_ENTRY, "partner+product");
        for key in [USER_AGENT_ENTRY, USER_AGENT_ENTRY_ALIAS] {
            assert_eq!(config.get_string(key).unwrap(), "partner+product");
        }

        let mut reverse = DatabaseConfig::default();
        set(&mut reverse, USER_AGENT_ENTRY, "partner+product");
        set(&mut reverse, USER_AGENT_ENTRY_ALIAS, "legacy-entry");
        assert_eq!(
            reverse.get_string(USER_AGENT_ENTRY).unwrap(),
            "partner+product"
        );
    }

    #[test]
    fn user_agent_entry_rejects_control_characters() {
        for key in [USER_AGENT_ENTRY, USER_AGENT_ENTRY_ALIAS] {
            for value in ["partner\nproduct", "partner\rproduct", "partner\0product"] {
                let mut config = DatabaseConfig::default();
                let error = config.set_option(key, value.into()).unwrap_err();
                assert_eq!(error.status, Status::InvalidArguments, "{key}: {value:?}");
                assert!(error.message.contains(key), "{key}: {value:?}");
            }
        }
    }

    #[test]
    fn http_pool_and_custom_headers_reach_kernel_config() {
        let mut config = DatabaseConfig::default();
        set(&mut config, "SocketTimeout", 4500_i64);
        set(&mut config, HOST, "https://example.databricks.com");
        set(&mut config, HTTP_PATH, "/sql/1.0/warehouses/warehouse-id");
        set(&mut config, ACCESS_TOKEN, "sentinel-token");
        set(
            &mut config,
            "adbc.databricks.http.pool_max_idle_per_host",
            37_i64,
        );
        set(
            &mut config,
            "adbc.databricks.http.headers",
            r#"[{"name":"x-databricks-test","value":"sentinel"}]"#,
        );
        set(&mut config, USER_AGENT_ENTRY, "partner+product");

        let resolved = config.resolve().unwrap();
        assert_eq!(resolved.http.pool_max_idle_per_host, 37);
        assert_eq!(
            resolved.http.storage_read_timeout,
            Some(Duration::from_millis(4500))
        );
        assert_eq!(
            resolved.user_agent_entry.as_deref(),
            Some("partner+product")
        );
        assert_eq!(
            resolved.http.custom_headers,
            vec![("x-databricks-test".into(), "sentinel".into())]
        );
    }

    #[test]
    fn telemetry_options_reach_kernel_config() {
        let mut config = DatabaseConfig::default();
        set(&mut config, HOST, "https://example.databricks.com");
        set(&mut config, HTTP_PATH, "/sql/1.0/warehouses/warehouse-id");
        set(&mut config, ACCESS_TOKEN, "sentinel-token");
        set(&mut config, "adbc.databricks.telemetry.enabled", true);
        set(&mut config, "adbc.databricks.telemetry.batch_size", "17");
        set(
            &mut config,
            "adbc.databricks.telemetry.flush_interval_ms",
            "2300",
        );
        set(&mut config, "adbc.databricks.telemetry.max_retries", "4");
        set(
            &mut config,
            "adbc.databricks.telemetry.retry_delay_ms",
            "250",
        );
        set(
            &mut config,
            "adbc.databricks.telemetry.close_flush_timeout_ms",
            "3100",
        );
        set(
            &mut config,
            "adbc.databricks.telemetry.circuit_breaker_enabled",
            true,
        );
        set(
            &mut config,
            "adbc.databricks.telemetry.circuit_breaker_threshold",
            "7",
        );
        set(
            &mut config,
            "adbc.databricks.telemetry.circuit_breaker_timeout_ms",
            "4200",
        );

        let telemetry = config.resolve().unwrap().telemetry;
        assert_eq!(telemetry.enabled, Some(true));
        assert_eq!(telemetry.batch_size, 17);
        assert_eq!(telemetry.flush_interval, Duration::from_millis(2300));
        assert_eq!(telemetry.max_retries, 4);
        assert_eq!(telemetry.retry_delay, Duration::from_millis(250));
        assert_eq!(telemetry.close_flush_timeout, Duration::from_millis(3100));
        assert!(telemetry.circuit_breaker_enabled);
        assert_eq!(telemetry.circuit_breaker_threshold, 7);
        assert_eq!(
            telemetry.circuit_breaker_timeout,
            Duration::from_millis(4200)
        );
    }

    #[test]
    fn telemetry_numeric_options_reject_invalid_strings() {
        for (key, value) in [
            ("adbc.databricks.telemetry.batch_size", "0"),
            ("adbc.databricks.telemetry.flush_interval_ms", "invalid"),
            ("adbc.databricks.telemetry.max_retries", "-1"),
        ] {
            let mut config = DatabaseConfig::default();
            let error = config.set_option(key, value.into()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{key}");
        }
    }

    #[test]
    fn conflicts_are_rejected_before_network() {
        let mut config = DatabaseConfig::default();
        set(&mut config, HTTP_PATH, "/sql/1.0/warehouses/a");
        set(&mut config, WAREHOUSE_ID, "b");
        let Err(error) = config.resolve() else {
            panic!("conflicting endpoint options should fail");
        };
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn http_path_preserves_workspace_organization_query() {
        let mut config = DatabaseConfig::default();
        set(&mut config, HOST, "https://example.databricks.com");
        set(
            &mut config,
            HTTP_PATH,
            "/sql/1.0/warehouses/warehouse-id?o=123456789",
        );
        set(&mut config, ACCESS_TOKEN, "sentinel-token");

        assert_eq!(
            config.resolve().unwrap().http_path.as_deref(),
            Some("/sql/1.0/warehouses/warehouse-id?o=123456789")
        );
    }

    #[test]
    fn reserved_direct_custom_federation_is_not_implemented() {
        let mut config = DatabaseConfig::default();
        set(&mut config, AUTH_TYPE, "oauth_federation");
        assert_eq!(
            config.resolve_auth().unwrap_err().status,
            Status::NotImplemented
        );
    }
}
