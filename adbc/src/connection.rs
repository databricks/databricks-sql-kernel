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

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use adbc_core::error::{Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use databricks_sql_kernel as kernel;

use crate::cancellation::CancellationRegistry;
use crate::error::{adbc_error, map_kernel_error, option_not_found, unsupported};
use crate::runtime::{RuntimeExt as _, RuntimeHandle};
use crate::Statement;

#[path = "metadata.rs"]
pub(crate) mod metadata;

pub struct Connection {
    session: Option<kernel::Session>,
    runtime: Arc<RuntimeHandle>,
    catalog: Option<String>,
    schema: Option<String>,
    vendor_version: Mutex<Option<String>>,
    escape_catalog_name: bool,
    cancellation: Arc<CancellationRegistry>,
}

impl Connection {
    pub(crate) fn new(
        session: kernel::Session,
        runtime: Arc<RuntimeHandle>,
        catalog: Option<String>,
        schema: Option<String>,
        escape_catalog_name: bool,
    ) -> Self {
        Self {
            session: Some(session),
            runtime,
            catalog,
            schema,
            vendor_version: Mutex::new(None),
            escape_catalog_name,
            cancellation: Arc::new(CancellationRegistry::default()),
        }
    }

    fn session(&self) -> Result<&kernel::Session> {
        self.session
            .as_ref()
            .ok_or_else(|| adbc_error("connection is closed", Status::InvalidState))
    }

    fn execute_session_command(&self, sql: String) -> Result<()> {
        let mut statement = self.session()?.statement();
        let canceller = statement.canceller();
        let runtime = Arc::clone(&self.runtime);
        let _operation = self.cancellation.register(Arc::new(move || {
            let dispatched = runtime
                .block_on_safe(canceller.cancel())
                .map_err(map_kernel_error)?;
            if dispatched {
                Ok(())
            } else {
                Err(adbc_error(
                    "connection cancellation raced before the server statement identifier was available",
                    Status::Unknown,
                ))
            }
        }))?;
        statement.spec().sql(sql);
        let mut executed = self
            .runtime
            .block_on_safe(statement.execute())
            .map_err(map_kernel_error)?;
        while self
            .runtime
            .block_on_safe(executed.next_batch())
            .map_err(map_kernel_error)?
            .is_some()
        {}
        Ok(())
    }

    fn set_current_catalog(&mut self, value: String) -> Result<()> {
        let quoted = quote_session_identifier(&value)?;
        let result = self.execute_session_command(format!("USE CATALOG {quoted}"));
        update_after_success(&mut self.catalog, value, result)?;
        // USE CATALOG changes the server's current schema. Do not report the stale old value.
        self.schema = None;
        Ok(())
    }

    fn set_current_schema(&mut self, value: String) -> Result<()> {
        let quoted = quote_session_identifier(&value)?;
        let result = self.execute_session_command(format!("USE SCHEMA {quoted}"));
        update_after_success(&mut self.schema, value, result)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            // Await DeleteSession before the connection-owned runtime is dropped.
            // Kernel Session::Drop is intentionally fire-and-forget, which is not
            // sufficient here because this runtime is the task executor.
            let _ = self.runtime.block_on_safe(session.close());
        }
    }
}

fn quote_session_identifier(value: &str) -> Result<String> {
    if value.is_empty() || value.trim().is_empty() {
        return Err(adbc_error(
            "identifier must not be empty or whitespace",
            Status::InvalidArguments,
        ));
    }
    if value.contains('\0') {
        return Err(adbc_error(
            "identifier must not contain a NUL byte",
            Status::InvalidArguments,
        ));
    }
    if value.len() > 255 {
        return Err(adbc_error(
            "identifier exceeds the 255-byte limit",
            Status::InvalidArguments,
        ));
    }
    Ok(format!("`{}`", value.replace('`', "``")))
}

fn update_after_success(
    slot: &mut Option<String>,
    value: String,
    operation: Result<()>,
) -> Result<()> {
    operation?;
    *slot = Some(value);
    Ok(())
}

fn string_option(value: OptionValue, key: &str) -> Result<String> {
    match value {
        OptionValue::String(value) => Ok(value),
        _ => Err(adbc_error(
            format!("connection option {key} requires a string"),
            Status::InvalidArguments,
        )),
    }
}

impl adbc_core::Optionable for Connection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: OptionConnection, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                let requested = bool::try_from(value)?;
                self.runtime
                    .block_on_safe(self.session()?.set_autocommit(requested))
                    .map_err(map_kernel_error)?;
                if self.session()?.autocommit() != requested {
                    return Err(unsupported(
                        "disabling autocommit while transactions are ignored",
                    ));
                }
                Ok(())
            }
            OptionConnection::CurrentCatalog => {
                let catalog = string_option(value, "CurrentCatalog")?;
                self.set_current_catalog(catalog)
            }
            OptionConnection::CurrentSchema => {
                let schema = string_option(value, "CurrentSchema")?;
                self.set_current_schema(schema)
            }
            _ => Err(unsupported(&format!("connection option {key:?}"))),
        }
    }

    fn get_option_string(&self, key: OptionConnection) -> Result<String> {
        match key {
            OptionConnection::AutoCommit => Ok(self.session()?.autocommit().to_string()),
            OptionConnection::CurrentCatalog => self
                .catalog
                .clone()
                .ok_or_else(|| adbc_error("current catalog is not configured", Status::NotFound)),
            OptionConnection::CurrentSchema => self
                .schema
                .clone()
                .ok_or_else(|| adbc_error("current schema is not configured", Status::NotFound)),
            _ => Err(option_not_found("connection string", key.as_ref())),
        }
    }
    fn get_option_bytes(&self, key: OptionConnection) -> Result<Vec<u8>> {
        Err(option_not_found("connection bytes", key.as_ref()))
    }
    fn get_option_int(&self, key: OptionConnection) -> Result<i64> {
        Err(option_not_found("connection integer", key.as_ref()))
    }
    fn get_option_double(&self, key: OptionConnection) -> Result<f64> {
        Err(option_not_found("connection double", key.as_ref()))
    }
}

impl adbc_core::Connection for Connection {
    type StatementType = Statement;

    fn new_statement(&mut self) -> Result<Statement> {
        Ok(Statement::new(
            self.session()?.statement(),
            self.session()?.metadata(),
            Arc::clone(&self.runtime),
            Arc::clone(&self.cancellation),
            self.escape_catalog_name,
        ))
    }
    fn cancel(&mut self) -> Result<()> {
        self.cancellation.cancel()
    }
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_info(self.session()?, &self.runtime, &self.vendor_version, codes)
    }
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_objects(
            self.session()?,
            &self.runtime,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
            self.escape_catalog_name,
        )
    }
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        metadata::get_table_schema(
            self.session()?,
            &self.runtime,
            catalog,
            db_schema,
            table_name,
        )
    }
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        metadata::get_table_types(self.session()?, &self.runtime)
    }
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("get_statistic_names"))
    }
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("get_statistics"))
    }
    fn commit(&mut self) -> Result<()> {
        self.runtime
            .block_on_safe(self.session()?.commit())
            .map_err(map_kernel_error)
    }
    fn rollback(&mut self) -> Result<()> {
        self.runtime
            .block_on_safe(self.session()?.rollback())
            .map_err(map_kernel_error)
    }
    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(unsupported("read_partition"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_identifiers_are_quoted_and_validated() {
        assert_eq!(quote_session_identifier("a`b").unwrap(), "`a``b`");
        for invalid in ["", "  ", "a\0b"] {
            assert_eq!(
                quote_session_identifier(invalid).unwrap_err().status,
                Status::InvalidArguments
            );
        }
        assert_eq!(
            quote_session_identifier(&"a".repeat(256))
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn option_state_changes_only_after_server_success() {
        let mut state = Some("old".to_owned());
        let failure = Err(adbc_error("server rejected command", Status::InvalidState));
        assert!(update_after_success(&mut state, "new".into(), failure).is_err());
        assert_eq!(state.as_deref(), Some("old"));

        update_after_success(&mut state, "new".into(), Ok(())).unwrap();
        assert_eq!(state.as_deref(), Some("new"));
    }
}
