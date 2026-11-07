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

use std::sync::{Arc, Mutex};

use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::Optionable as _;
use databricks_sql_kernel as kernel;

use crate::config::DatabaseConfig;
use crate::error::option_not_found;
use crate::runtime::{RuntimeExt as _, RuntimeHandle};
use crate::Connection;

const DRIVER_NAME: &str = "Databricks ADBC Driver for Rust";
const DRIVER_USER_AGENT_NAME: &str = "RustADBCDatabricksDriver";

fn driver_user_agent() -> String {
    format!(
        "{DRIVER_USER_AGENT_NAME}/{} REST",
        env!("CARGO_PKG_VERSION")
    )
}

fn set_driver_user_agent(http: &mut kernel::HttpConfig) {
    http.custom_headers
        .retain(|(name, _)| !name.eq_ignore_ascii_case("user-agent"));
    http.custom_headers
        .push(("User-Agent".into(), driver_user_agent()));
}

fn driver_system_configuration() -> kernel::DriverSystemConfiguration {
    kernel::DriverSystemConfiguration {
        driver_name: Some(DRIVER_NAME.into()),
        driver_version: Some(env!("CARGO_PKG_VERSION").into()),
        ..Default::default()
    }
}

#[derive(Clone, Debug)]
pub struct Database {
    config: DatabaseConfig,
    runtime: Arc<Mutex<Option<Arc<RuntimeHandle>>>>,
}

impl Database {
    /// Create a database that executes kernel work on a caller-owned runtime.
    ///
    /// All connections created from this database share the runtime. Dropping
    /// the database or its connections never shuts down an injected runtime.
    pub fn with_runtime(runtime: Arc<tokio::runtime::Runtime>) -> Result<Self> {
        if !matches!(
            runtime.handle().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return Err(adbc_core::error::Error::with_message_and_status(
                "an injected runtime must use the Tokio multi-thread scheduler",
                adbc_core::error::Status::InvalidArguments,
            ));
        }
        Ok(Self {
            config: DatabaseConfig::default(),
            runtime: Arc::new(Mutex::new(Some(RuntimeHandle::injected(runtime)))),
        })
    }

    fn runtime(&self) -> Result<Arc<RuntimeHandle>> {
        let mut runtime = self.runtime.lock().map_err(|_| {
            adbc_core::error::Error::with_message_and_status(
                "database runtime lock is poisoned",
                adbc_core::error::Status::Internal,
            )
        })?;
        if let Some(runtime) = runtime.as_ref() {
            return Ok(Arc::clone(runtime));
        }
        let created = RuntimeHandle::create().map_err(|source| {
            adbc_core::error::Error::with_message_and_status(
                source.to_string(),
                adbc_core::error::Status::Internal,
            )
        })?;
        *runtime = Some(Arc::clone(&created));
        Ok(created)
    }

    fn open(&self) -> Result<Connection> {
        let mut resolved = self.config.resolve()?;
        set_driver_user_agent(&mut resolved.http);
        let mut builder = kernel::Session::builder()
            .auth(resolved.auth)
            .http(resolved.http)
            .tls(resolved.tls)
            .logging(resolved.logging)
            .telemetry(resolved.telemetry)
            .result(resolved.result)
            .driver_system_configuration(driver_system_configuration())
            .session_conf(resolved.session_conf)
            .geospatial_result_representation(resolved.geospatial);
        if let Some(proxy) = resolved.proxy {
            builder = builder.proxy(proxy);
        }
        builder = if let Some(http_path) = &resolved.http_path {
            builder.http_path(&resolved.host, http_path)
        } else {
            builder.connection(kernel::ConnectionConfig::new(
                &resolved.host,
                resolved.warehouse_id.as_deref().unwrap_or_default(),
            ))
        };
        if resolved.catalog.is_some() || resolved.schema.is_some() {
            let mut defaults = kernel::DefaultOpts::default();
            defaults.catalog.clone_from(&resolved.catalog);
            defaults.schema.clone_from(&resolved.schema);
            builder = builder.defaults(defaults);
        }
        let runtime = self.runtime()?;
        let session = runtime
            .block_on_safe(builder.open())
            .map_err(crate::error::map_kernel_error)?;
        Ok(Connection::new(
            session,
            runtime,
            resolved.escape_catalog_name,
        ))
    }
}

impl Default for Database {
    fn default() -> Self {
        Self {
            config: DatabaseConfig::default(),
            runtime: Arc::new(Mutex::new(None)),
        }
    }
}

impl adbc_core::Optionable for Database {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: OptionDatabase, value: OptionValue) -> Result<()> {
        self.config.set_option(key.as_ref(), value)
    }

    fn get_option_string(&self, key: OptionDatabase) -> Result<String> {
        self.config.get_string(key.as_ref())
    }

    fn get_option_bytes(&self, key: OptionDatabase) -> Result<Vec<u8>> {
        Err(option_not_found("database bytes", key.as_ref()))
    }
    fn get_option_int(&self, key: OptionDatabase) -> Result<i64> {
        Err(option_not_found("database integer", key.as_ref()))
    }
    fn get_option_double(&self, key: OptionDatabase) -> Result<f64> {
        Err(option_not_found("database double", key.as_ref()))
    }
}

impl adbc_core::Database for Database {
    type ConnectionType = Connection;

    fn new_connection(&self) -> Result<Connection> {
        self.open()
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Connection> {
        let mut connection = self.open()?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use adbc_core::{Database as _, Driver as _};

    use crate::config::{ACCESS_TOKEN, ESCAPE_CATALOG_NAME, HTTP_PATH};
    use crate::Driver;

    use super::*;

    #[test]
    fn options_round_trip() {
        let database = Driver
            .new_database_with_opts([
                (OptionDatabase::Uri, "https://example.databricks.com".into()),
                (
                    OptionDatabase::Other(HTTP_PATH.into()),
                    "/sql/1.0/warehouses/abc".into(),
                ),
                (OptionDatabase::Other(ACCESS_TOKEN.into()), "secret".into()),
            ])
            .unwrap();
        assert_eq!(
            database.get_option_string(OptionDatabase::Uri).unwrap(),
            "https://example.databricks.com"
        );
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(ESCAPE_CATALOG_NAME.into()))
                .unwrap(),
            "true"
        );

        let mut database = database;
        database
            .set_option(
                OptionDatabase::Other(ESCAPE_CATALOG_NAME.into()),
                "false".into(),
            )
            .unwrap();
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other(ESCAPE_CATALOG_NAME.into()))
                .unwrap(),
            "false"
        );
    }

    #[test]
    fn debug_does_not_expose_access_token() {
        let database = Driver
            .new_database_with_opts([(
                OptionDatabase::Other(ACCESS_TOKEN.into()),
                "sentinel-access-token".into(),
            )])
            .unwrap();
        let debug = format!("{database:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("sentinel-access-token"));
    }

    #[test]
    fn required_options_are_validated_before_network_io() {
        let error = Database::default().new_connection().err().unwrap();
        assert_eq!(error.status, adbc_core::error::Status::InvalidState);
    }

    #[test]
    fn kernel_receives_rust_adbc_driver_identity() {
        let identity = driver_system_configuration();
        assert_eq!(identity.driver_name.as_deref(), Some(DRIVER_NAME));
        assert_eq!(
            identity.driver_version.as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn kernel_receives_rust_adbc_user_agent() {
        let mut http = kernel::HttpConfig::default();
        http.custom_headers = vec![
            ("x-test-header".into(), "test-value".into()),
            ("user-agent".into(), "caller-agent".into()),
        ];

        set_driver_user_agent(&mut http);

        assert_eq!(
            http.custom_headers,
            vec![
                ("x-test-header".into(), "test-value".into()),
                (
                    "User-Agent".into(),
                    format!(
                        "{DRIVER_USER_AGENT_NAME}/{} REST",
                        env!("CARGO_PKG_VERSION")
                    ),
                ),
            ]
        );
    }

    #[test]
    fn clones_share_driver_owned_runtime() {
        let database = Database::default();
        let clone = database.clone();
        assert!(Arc::ptr_eq(
            &database.runtime().unwrap(),
            &clone.runtime().unwrap()
        ));
    }

    #[test]
    fn injected_runtime_is_shared_by_database_clones() {
        let injected = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        );
        let database = Database::with_runtime(Arc::clone(&injected)).unwrap();
        let clone = database.clone();
        assert!(Arc::ptr_eq(
            &database.runtime().unwrap(),
            &clone.runtime().unwrap()
        ));
        drop(database);
        drop(clone);
        assert_eq!(injected.block_on(async { 42 }), 42);
    }

    #[test]
    fn current_thread_runtime_cannot_be_injected() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        assert_eq!(
            Database::with_runtime(runtime).unwrap_err().status,
            adbc_core::error::Status::InvalidArguments
        );
    }
}
