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

use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema};
use databricks_sql_kernel as kernel;
use databricks_sql_kernel::ExecutedStatementHandle as _;
use tokio::sync::Notify;

use crate::cancellation::{CancellationRegistry, OperationGuard};
use crate::connection::metadata;
use crate::error::{adbc_error, map_kernel_error, option_not_found, unsupported};
use crate::reader::{AsyncKernelReader, BoundRows, KernelReader, SequenceReader, SlicingReader};
use crate::runtime::{RuntimeExt as _, RuntimeHandle};

const PARAMETER_STYLE_OPTION: &str = "adbc.databricks.statement.parameter_style";
const QUERY_TAGS_OPTION: &str = "adbc.databricks.statement.query_tags";
const CLIENT_TIMEOUT_OPTION: &str = "adbc.databricks.statement.client_timeout_ms";
const QUERY_TIMEOUT_OPTION: &str = "adbc.apache.statement.query_timeout_s";
const ENABLE_DIRECT_RESULTS_OPTION: &str = "adbc.databricks.enable_direct_results";
const ROW_LIMIT_OPTION: &str = "adbc.databricks.statement.row_limit";
const ROWS_FETCHED_PER_BLOCK_OPTION: &str = "adbc.databricks.statement.rows_fetched_per_block";
const SERVER_TIMEOUT_OPTION: &str = "adbc.databricks.statement.timeout_seconds";
const STATEMENT_CONF_PREFIX: &str = "adbc.databricks.statement.conf.";
const IS_METADATA_COMMAND_OPTION: &str = "adbc.apache.statement.is_metadata_command";
const METADATA_CATALOG_OPTION: &str = "adbc.get_metadata.target_catalog";
const METADATA_SCHEMA_OPTION: &str = "adbc.get_metadata.target_db_schema";
const METADATA_TABLE_OPTION: &str = "adbc.get_metadata.target_table";
const METADATA_TABLE_TYPES_OPTION: &str = "adbc.get_metadata.target_table_types";
const METADATA_COLUMN_OPTION: &str = "adbc.get_metadata.target_column";
const METADATA_FOREIGN_CATALOG_OPTION: &str = "adbc.get_metadata.foreign_target_catalog";
const METADATA_FOREIGN_SCHEMA_OPTION: &str = "adbc.get_metadata.foreign_target_db_schema";
const METADATA_FOREIGN_TABLE_OPTION: &str = "adbc.get_metadata.foreign_target_table";
const METADATA_ESCAPE_WILDCARDS_OPTION: &str = "adbc.get_metadata.escape_pattern_wildcards";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ParameterStyle {
    #[default]
    Positional,
    Named,
}

#[derive(Debug, Clone)]
struct BoundParameter {
    name: Option<String>,
    param_type: String,
    value: Option<String>,
    binary: bool,
}

pub struct Statement {
    inner: Arc<Mutex<kernel::Statement>>,
    metadata: kernel::Metadata,
    canceller: kernel::StatementCanceller,
    runtime: Arc<RuntimeHandle>,
    binding: Option<Binding>,
    parameter_style: ParameterStyle,
    prepared: bool,
    client_timeout: Option<Duration>,
    enable_direct_results: bool,
    rows_fetched_per_block: Option<usize>,
    generation: Arc<AtomicU64>,
    current_cancellation: Arc<AtomicBool>,
    blocking_execute: Arc<AtomicBool>,
    cancellation_notify: Arc<Notify>,
    connection_cancellation: Arc<CancellationRegistry>,
    metadata_options: MetadataCommandOptions,
}

#[derive(Debug, Default)]
struct MetadataCommandOptions {
    enabled: bool,
    catalog: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    table_types: Option<String>,
    column: Option<String>,
    foreign_catalog: Option<String>,
    foreign_schema: Option<String>,
    foreign_table: Option<String>,
    escape_wildcards: bool,
    escape_catalog_name: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// State observed for a non-blocking Databricks statement.
pub enum AsyncStatementStatus {
    /// Accepted but not yet executing.
    Pending,
    /// Currently executing.
    Running,
    /// Completed with a result available for fetching.
    Succeeded,
    /// Completed and already closed by the server.
    Closed,
    /// Completed with a server error.
    Failed,
    /// Cancelled before completion.
    Cancelled,
    /// A newer kernel state not known by this driver version.
    Unknown,
}

/// A caller-driven non-blocking statement operation.
///
/// The handle exclusively borrows its [`Statement`] while the operation is
/// pending, preventing mutation or resubmission until the handle is consumed
/// or dropped.
pub struct SubmittedStatement<'statement> {
    statement: &'statement mut Statement,
    executed: Option<kernel::ExecutedAsyncStatement>,
    connection_operation: Option<OperationGuard>,
    expected_generation: u64,
}

impl SubmittedStatement<'_> {
    /// Poll the current server-side operation state once.
    pub fn status(&self) -> Result<AsyncStatementStatus> {
        let executed = self
            .executed
            .as_ref()
            .ok_or_else(|| adbc_error("submitted statement was consumed", Status::InvalidState))?;
        let status = self
            .statement
            .runtime
            .block_on_safe(executed.status())
            .map_err(map_kernel_error)?;
        Ok(match status {
            kernel::StatementStatus::Pending => AsyncStatementStatus::Pending,
            kernel::StatementStatus::Running => AsyncStatementStatus::Running,
            kernel::StatementStatus::Succeeded => AsyncStatementStatus::Succeeded,
            kernel::StatementStatus::Closed => AsyncStatementStatus::Closed,
            kernel::StatementStatus::Failed(_) => AsyncStatementStatus::Failed,
            kernel::StatementStatus::Cancelled => AsyncStatementStatus::Cancelled,
            _ => AsyncStatementStatus::Unknown,
        })
    }

    /// Poll to terminal state and return its Arrow result reader.
    pub fn await_result(mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let executed = self
            .executed
            .take()
            .ok_or_else(|| adbc_error("submitted statement was consumed", Status::InvalidState))?;
        let stream = self
            .statement
            .runtime
            .block_on_safe(executed.await_result())
            .map_err(map_kernel_error)?;
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(AsyncKernelReader::new(
            Arc::clone(&self.statement.runtime),
            executed,
            stream,
            Arc::clone(&self.statement.generation),
            self.expected_generation,
            self.connection_operation
                .take()
                .expect("submitted statement owns its connection operation"),
        ));
        Ok(match self.statement.rows_fetched_per_block {
            Some(max_rows) => Box::new(SlicingReader::new(reader, max_rows)),
            None => reader,
        })
    }

    /// Fetch a result after [`Self::status`] reported terminal success.
    pub fn fetch_result(mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let executed = self
            .executed
            .take()
            .ok_or_else(|| adbc_error("submitted statement was consumed", Status::InvalidState))?;
        let stream = self
            .statement
            .runtime
            .block_on_safe(executed.fetch_result())
            .map_err(map_kernel_error)?;
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(AsyncKernelReader::new(
            Arc::clone(&self.statement.runtime),
            executed,
            stream,
            Arc::clone(&self.statement.generation),
            self.expected_generation,
            self.connection_operation
                .take()
                .expect("submitted statement owns its connection operation"),
        ));
        Ok(match self.statement.rows_fetched_per_block {
            Some(max_rows) => Box::new(SlicingReader::new(reader, max_rows)),
            None => reader,
        })
    }

    /// Cancel the pending server-side operation.
    pub fn cancel(&self) -> Result<()> {
        let executed = self
            .executed
            .as_ref()
            .ok_or_else(|| adbc_error("submitted statement was consumed", Status::InvalidState))?;
        self.statement
            .runtime
            .block_on_safe(executed.cancel())
            .map_err(map_kernel_error)
    }
}

/// A thread-safe handle for cancelling the statement's current server operation.
#[derive(Clone)]
pub struct StatementCancellationHandle {
    canceller: kernel::StatementCanceller,
    runtime: Arc<RuntimeHandle>,
    current_cancellation: Arc<AtomicBool>,
    blocking_execute: Arc<AtomicBool>,
    cancellation_notify: Arc<Notify>,
}

impl StatementCancellationHandle {
    pub fn cancel(&self) -> Result<()> {
        if self.current_cancellation.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.canceller.statement_id().is_none() {
            return Err(adbc_error(
                "statement has no cancellable server operation",
                Status::InvalidState,
            ));
        }
        if self.current_cancellation.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if self.blocking_execute.load(Ordering::Acquire) {
            self.cancellation_notify.notify_waiters();
            return Ok(());
        }
        let dispatched = self
            .runtime
            .block_on_safe(self.canceller.cancel())
            .map_err(map_kernel_error)?;
        if dispatched {
            Ok(())
        } else {
            Err(adbc_error(
                "statement has no cancellable server operation",
                Status::InvalidState,
            ))
        }
    }
}

enum Binding {
    Batch(RecordBatch),
    Stream(Box<dyn RecordBatchReader + Send>),
}

async fn cancel_blocking_execution<F>(
    execution: F,
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
) -> Option<F::Output>
where
    F: Future,
{
    let notified = notify.notified();
    tokio::pin!(notified);
    if cancelled.load(Ordering::Acquire) {
        return None;
    }
    tokio::select! {
        biased;
        _ = &mut notified => None,
        result = execution => Some(result),
    }
}

impl Statement {
    /// Submit the SQL text without waiting for terminal state.
    ///
    /// This uses the kernel's `wait_timeout=0s` path. Call [`SubmittedStatement::status`]
    /// to poll explicitly or [`SubmittedStatement::await_result`] to poll and fetch.
    pub fn submit(&mut self) -> Result<SubmittedStatement<'_>> {
        if self.binding.is_some() {
            return Err(unsupported(
                "non-blocking execution with parameter bindings",
            ));
        }
        self.invalidate_results();
        let connection_operation = self.register_connection_operation()?;
        let runtime = Arc::clone(&self.runtime);
        let executed = {
            let mut inner = self.inner()?;
            match self.client_timeout {
                Some(timeout) => runtime.block_on_safe(inner.submit_with_timeout(timeout)),
                None => runtime.block_on_safe(inner.submit()),
            }
            .map_err(map_kernel_error)?
        };
        let expected_generation = self.generation.load(Ordering::Acquire);
        Ok(SubmittedStatement {
            statement: self,
            executed: Some(executed),
            connection_operation: Some(connection_operation),
            expected_generation,
        })
    }

    pub(crate) fn new(
        inner: kernel::Statement,
        metadata: kernel::Metadata,
        runtime: Arc<RuntimeHandle>,
        connection_cancellation: Arc<CancellationRegistry>,
        escape_catalog_name: bool,
    ) -> Self {
        let canceller = inner.canceller();
        Self {
            inner: Arc::new(Mutex::new(inner)),
            metadata,
            canceller,
            runtime,
            binding: None,
            parameter_style: ParameterStyle::default(),
            prepared: false,
            client_timeout: None,
            enable_direct_results: true,
            rows_fetched_per_block: None,
            generation: Arc::new(AtomicU64::new(0)),
            current_cancellation: Arc::new(AtomicBool::new(false)),
            blocking_execute: Arc::new(AtomicBool::new(false)),
            cancellation_notify: Arc::new(Notify::new()),
            connection_cancellation,
            metadata_options: MetadataCommandOptions {
                escape_catalog_name,
                ..MetadataCommandOptions::default()
            },
        }
    }

    /// Return a cloneable handle that can cancel an execution from another thread.
    pub fn cancellation_handle(&self) -> StatementCancellationHandle {
        StatementCancellationHandle {
            canceller: self.canceller.clone(),
            runtime: Arc::clone(&self.runtime),
            current_cancellation: Arc::clone(&self.current_cancellation),
            blocking_execute: Arc::clone(&self.blocking_execute),
            cancellation_notify: Arc::clone(&self.cancellation_notify),
        }
    }

    fn inner(&self) -> Result<MutexGuard<'_, kernel::Statement>> {
        self.inner
            .lock()
            .map_err(|_| adbc_error("kernel statement lock is poisoned", Status::Internal))
    }

    fn execute_inner(&self) -> Result<kernel::ExecutedStatement> {
        let mut inner = self.inner()?;
        if let Some(sql) = inner.spec_ref().sql.clone() {
            // A previous bound execution leaves raw parameters in the kernel
            // spec. An unbound execution must not accidentally reuse them.
            inner.spec().sql(sql);
        }
        self.blocking_execute.store(true, Ordering::Release);
        let cancelled = Arc::clone(&self.current_cancellation);
        let notify = Arc::clone(&self.cancellation_notify);
        let result = match self.client_timeout {
            Some(timeout) => self.runtime.block_on_safe(cancel_blocking_execution(
                inner.execute_with_timeout(timeout),
                cancelled,
                notify,
            )),
            None => self.runtime.block_on_safe(cancel_blocking_execution(
                inner.execute(),
                cancelled,
                notify,
            )),
        };
        self.blocking_execute.store(false, Ordering::Release);
        match result {
            Some(result) => result.map_err(map_kernel_error),
            None => Err(adbc_error("operation was cancelled", Status::Cancelled)),
        }
    }

    fn execute_async_inner(
        &self,
    ) -> Result<(kernel::ExecutedAsyncStatement, kernel::ResultStream)> {
        let runtime = Arc::clone(&self.runtime);
        let mut inner = self.inner()?;
        if let Some(sql) = inner.spec_ref().sql.clone() {
            // A previous bound execution leaves raw parameters in the kernel
            // spec. An unbound execution must not accidentally reuse them.
            inner.spec().sql(sql);
        }
        self.blocking_execute.store(true, Ordering::Release);
        let cancelled = Arc::clone(&self.current_cancellation);
        let notify = Arc::clone(&self.cancellation_notify);
        let submitted = match self.client_timeout {
            Some(timeout) => runtime.block_on_safe(cancel_blocking_execution(
                inner.submit_with_timeout(timeout),
                Arc::clone(&cancelled),
                Arc::clone(&notify),
            )),
            None => runtime.block_on_safe(cancel_blocking_execution(
                inner.submit_with_timeout(Duration::MAX),
                Arc::clone(&cancelled),
                Arc::clone(&notify),
            )),
        };
        drop(inner);
        let result = match submitted {
            Some(result) => match result.map_err(map_kernel_error) {
                Ok(executed) => {
                    let stream = runtime.block_on_safe(cancel_blocking_execution(
                        executed.await_result(),
                        cancelled,
                        notify,
                    ));
                    match stream {
                        Some(stream) => stream
                            .map(|stream| (executed, stream))
                            .map_err(map_kernel_error),
                        None => Err(adbc_error("operation was cancelled", Status::Cancelled)),
                    }
                }
                Err(error) => Err(error),
            },
            None => Err(adbc_error("operation was cancelled", Status::Cancelled)),
        };
        self.blocking_execute.store(false, Ordering::Release);
        result
    }

    fn invalidate_results(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn register_connection_operation(&mut self) -> Result<OperationGuard> {
        self.current_cancellation.store(false, Ordering::Release);
        let cancelled = Arc::clone(&self.current_cancellation);
        let cancellation = self.cancellation_handle();
        let generation = Arc::clone(&self.generation);
        let expected_generation = generation.load(Ordering::Acquire);
        self.connection_cancellation.register_while_with_signal(
            Arc::new(move || cancellation.cancel()),
            Arc::new(move || generation.load(Ordering::Acquire) == expected_generation),
            cancelled,
        )
    }

    fn set_other_option(&mut self, key: &str, value: OptionValue) -> Result<()> {
        if let Some(timeout) = parse_client_timeout_option(key, value.clone()) {
            self.client_timeout = timeout?;
            return Ok(());
        }
        match key {
            ENABLE_DIRECT_RESULTS_OPTION => {
                self.enable_direct_results = expect_bool_string(value, key)?;
                Ok(())
            }
            IS_METADATA_COMMAND_OPTION => {
                self.metadata_options.enabled = expect_bool_string(value, key)?;
                Ok(())
            }
            METADATA_CATALOG_OPTION => {
                self.metadata_options.catalog = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_SCHEMA_OPTION => {
                self.metadata_options.schema = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_TABLE_OPTION => {
                self.metadata_options.table = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_TABLE_TYPES_OPTION => {
                self.metadata_options.table_types = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_COLUMN_OPTION => {
                self.metadata_options.column = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_FOREIGN_CATALOG_OPTION => {
                self.metadata_options.foreign_catalog = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_FOREIGN_SCHEMA_OPTION => {
                self.metadata_options.foreign_schema = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_FOREIGN_TABLE_OPTION => {
                self.metadata_options.foreign_table = Some(expect_string(value, key)?);
                Ok(())
            }
            METADATA_ESCAPE_WILDCARDS_OPTION => {
                self.metadata_options.escape_wildcards = expect_bool_string(value, key)?;
                Ok(())
            }
            PARAMETER_STYLE_OPTION => {
                self.parameter_style = match expect_string(value, key)?.as_str() {
                    "positional" => ParameterStyle::Positional,
                    "named" => ParameterStyle::Named,
                    other => {
                        return Err(adbc_error(
                            format!("{key} expects positional or named, got {other:?}"),
                            Status::InvalidArguments,
                        ));
                    }
                };
                Ok(())
            }
            QUERY_TAGS_OPTION => {
                self.inner()?
                    .spec()
                    .statement_conf
                    .insert("query_tags".into(), expect_string(value, key)?);
                Ok(())
            }
            ROW_LIMIT_OPTION => {
                self.inner()?
                    .spec()
                    .row_limit(Some(expect_nonnegative_int(value, key)?));
                Ok(())
            }
            ROWS_FETCHED_PER_BLOCK_OPTION | "adbc.databricks.rows_fetched_per_block" => {
                let rows = expect_nonnegative_int(value, key)?;
                if rows == 0 {
                    return Err(adbc_error(
                        format!("{key} must be greater than zero"),
                        Status::InvalidArguments,
                    ));
                }
                self.rows_fetched_per_block = Some(usize::try_from(rows).map_err(|_| {
                    adbc_error(
                        format!("{key} exceeds the platform row-count limit"),
                        Status::InvalidArguments,
                    )
                })?);
                Ok(())
            }
            SERVER_TIMEOUT_OPTION => {
                let seconds = expect_nonnegative_int(value, key)?;
                self.inner()?
                    .spec()
                    .statement_conf
                    .insert("STATEMENT_TIMEOUT".into(), seconds.to_string());
                Ok(())
            }
            _ if key.starts_with(STATEMENT_CONF_PREFIX) => {
                let name = &key[STATEMENT_CONF_PREFIX.len()..];
                if name.is_empty() {
                    return Err(adbc_error(
                        "statement conf option has an empty key",
                        Status::InvalidArguments,
                    ));
                }
                self.inner()?
                    .spec()
                    .statement_conf
                    .insert(name.into(), expect_string(value, key)?);
                Ok(())
            }
            _ => Err(unsupported(&format!("statement option {key}"))),
        }
    }

    fn get_other_string(&self, key: &str) -> Result<String> {
        match key {
            ENABLE_DIRECT_RESULTS_OPTION => Ok(self.enable_direct_results.to_string()),
            IS_METADATA_COMMAND_OPTION => Ok(self.metadata_options.enabled.to_string()),
            METADATA_CATALOG_OPTION => metadata_string_option(
                self.metadata_options.catalog.as_ref(),
                METADATA_CATALOG_OPTION,
            ),
            METADATA_SCHEMA_OPTION => metadata_string_option(
                self.metadata_options.schema.as_ref(),
                METADATA_SCHEMA_OPTION,
            ),
            METADATA_TABLE_OPTION => {
                metadata_string_option(self.metadata_options.table.as_ref(), METADATA_TABLE_OPTION)
            }
            METADATA_TABLE_TYPES_OPTION => metadata_string_option(
                self.metadata_options.table_types.as_ref(),
                METADATA_TABLE_TYPES_OPTION,
            ),
            METADATA_COLUMN_OPTION => metadata_string_option(
                self.metadata_options.column.as_ref(),
                METADATA_COLUMN_OPTION,
            ),
            METADATA_FOREIGN_CATALOG_OPTION => metadata_string_option(
                self.metadata_options.foreign_catalog.as_ref(),
                METADATA_FOREIGN_CATALOG_OPTION,
            ),
            METADATA_FOREIGN_SCHEMA_OPTION => metadata_string_option(
                self.metadata_options.foreign_schema.as_ref(),
                METADATA_FOREIGN_SCHEMA_OPTION,
            ),
            METADATA_FOREIGN_TABLE_OPTION => metadata_string_option(
                self.metadata_options.foreign_table.as_ref(),
                METADATA_FOREIGN_TABLE_OPTION,
            ),
            METADATA_ESCAPE_WILDCARDS_OPTION => {
                Ok(self.metadata_options.escape_wildcards.to_string())
            }
            PARAMETER_STYLE_OPTION => Ok(match self.parameter_style {
                ParameterStyle::Positional => "positional",
                ParameterStyle::Named => "named",
            }
            .into()),
            QUERY_TAGS_OPTION => self
                .inner()?
                .spec_ref()
                .statement_conf
                .get("query_tags")
                .cloned()
                .ok_or_else(|| adbc_error("query tags are not set", Status::NotFound)),
            _ if key.starts_with(STATEMENT_CONF_PREFIX) => self
                .inner()?
                .spec_ref()
                .statement_conf
                .get(&key[STATEMENT_CONF_PREFIX.len()..])
                .cloned()
                .ok_or_else(|| adbc_error("statement conf is not set", Status::NotFound)),
            _ => Err(option_not_found("statement string", key)),
        }
    }

    fn get_other_int(&self, key: &str) -> Result<i64> {
        match key {
            CLIENT_TIMEOUT_OPTION => Ok(self
                .client_timeout
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)),
            QUERY_TIMEOUT_OPTION => Ok(self
                .client_timeout
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or(0)),
            ROW_LIMIT_OPTION => self
                .inner()?
                .spec_ref()
                .row_limit
                .ok_or_else(|| adbc_error("row limit is not set", Status::NotFound)),
            ROWS_FETCHED_PER_BLOCK_OPTION | "adbc.databricks.rows_fetched_per_block" => self
                .rows_fetched_per_block
                .map(|rows| rows as i64)
                .ok_or_else(|| adbc_error("rows fetched per block is not set", Status::NotFound)),
            SERVER_TIMEOUT_OPTION => self
                .inner()?
                .spec_ref()
                .statement_conf
                .get("STATEMENT_TIMEOUT")
                .ok_or_else(|| adbc_error("statement timeout is not set", Status::NotFound))?
                .parse()
                .map_err(|_| adbc_error("invalid stored statement timeout", Status::Internal)),
            _ => Err(option_not_found("statement integer", key)),
        }
    }
}

impl adbc_core::Optionable for Statement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: OptionStatement, value: OptionValue) -> Result<()> {
        match key {
            OptionStatement::Incremental => {
                if bool::try_from(value)? {
                    Err(unsupported("incremental partitioned execution"))
                } else {
                    self.invalidate_results();
                    Ok(())
                }
            }
            OptionStatement::Progress | OptionStatement::MaxProgress => Err(adbc_error(
                "statement progress options are read-only",
                Status::InvalidArguments,
            )),
            OptionStatement::Other(key) => {
                let result = self.set_other_option(&key, value);
                if result.is_ok() {
                    self.invalidate_results();
                }
                result
            }
            key => Err(unsupported(&format!("statement option {key:?}"))),
        }
    }

    fn get_option_string(&self, key: OptionStatement) -> Result<String> {
        match key {
            OptionStatement::Other(key) => self.get_other_string(&key),
            key => Err(option_not_found("statement string", key.as_ref())),
        }
    }
    fn get_option_bytes(&self, key: OptionStatement) -> Result<Vec<u8>> {
        Err(option_not_found("statement bytes", key.as_ref()))
    }
    fn get_option_int(&self, key: OptionStatement) -> Result<i64> {
        match key {
            OptionStatement::Other(key) => self.get_other_int(&key),
            key => Err(option_not_found("statement integer", key.as_ref())),
        }
    }
    fn get_option_double(&self, key: OptionStatement) -> Result<f64> {
        match key {
            OptionStatement::Progress | OptionStatement::MaxProgress => {
                Err(unsupported("statement progress"))
            }
            key => Err(option_not_found("statement double", key.as_ref())),
        }
    }
}

impl adbc_core::Statement for Statement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        validate_binding_schema(&batch.schema(), self.parameter_style)?;
        self.invalidate_results();
        self.binding = Some(Binding::Batch(batch));
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        validate_binding_schema(&reader.schema(), self.parameter_style)?;
        self.invalidate_results();
        self.binding = Some(Binding::Stream(reader));
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.invalidate_results();
        if self.metadata_options.enabled {
            if self.binding.is_some() {
                return Err(unsupported("parameter binding for metadata commands"));
            }
            let command =
                self.inner()?.spec_ref().sql.clone().ok_or_else(|| {
                    adbc_error("metadata command is not set", Status::InvalidState)
                })?;
            let reader = metadata::execute_statement_command(
                &self.metadata,
                &self.runtime,
                &command,
                self.metadata_options.catalog.as_deref(),
                self.metadata_options.schema.as_deref(),
                self.metadata_options.table.as_deref(),
                self.metadata_options.column.as_deref(),
                self.metadata_options.table_types.as_deref(),
                self.metadata_options.foreign_catalog.as_deref(),
                self.metadata_options.foreign_schema.as_deref(),
                self.metadata_options.foreign_table.as_deref(),
                self.metadata_options.escape_wildcards,
                self.metadata_options.escape_catalog_name,
            )?;
            return Ok(match self.rows_fetched_per_block {
                Some(max_rows) => Box::new(SlicingReader::new(reader, max_rows)),
                None => reader,
            });
        }
        let connection_operation = self.register_connection_operation()?;
        let Some(binding) = self.binding.take() else {
            if !self.enable_direct_results {
                let (executed, stream) = self.execute_async_inner()?;
                let reader: Box<dyn RecordBatchReader + Send> = Box::new(AsyncKernelReader::new(
                    Arc::clone(&self.runtime),
                    executed,
                    stream,
                    Arc::clone(&self.generation),
                    self.generation.load(Ordering::Acquire),
                    connection_operation,
                ));
                return Ok(match self.rows_fetched_per_block {
                    Some(max_rows) => Box::new(SlicingReader::new(reader, max_rows)),
                    None => reader,
                });
            }
            let executed = self.execute_inner()?;
            let reader: Box<dyn RecordBatchReader + Send> = Box::new(KernelReader::new(
                Arc::clone(&self.runtime),
                executed,
                Arc::clone(&self.generation),
                self.generation.load(Ordering::Acquire),
                connection_operation,
            ));
            return Ok(match self.rows_fetched_per_block {
                Some(max_rows) => Box::new(SlicingReader::new(reader, max_rows)),
                None => reader,
            });
        };

        if !self.enable_direct_results {
            return Err(unsupported(
                "parameter binding when direct results are disabled",
            ));
        }

        let mut rows = match binding {
            Binding::Batch(batch) => BoundRows::from_batch(batch),
            Binding::Stream(reader) => BoundRows::from_stream(reader),
        };
        let row = rows
            .next_row()?
            .ok_or_else(|| unsupported("executing a query with an empty parameter binding"))?;
        let base_spec = self.inner()?.spec_ref().clone();
        let executed = execute_bound_row(
            &self.inner,
            &self.runtime,
            &base_spec,
            &row,
            self.parameter_style,
            self.client_timeout,
        )?;
        let expected_generation = self.generation.load(Ordering::Acquire);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(SequenceReader::new(
            Arc::clone(&self.inner),
            Arc::clone(&self.runtime),
            Arc::clone(&self.generation),
            expected_generation,
            base_spec,
            self.parameter_style,
            self.client_timeout,
            rows,
            executed,
            connection_operation,
        ));
        Ok(match self.rows_fetched_per_block {
            Some(max_rows) => Box::new(SlicingReader::new(reader, max_rows)),
            None => reader,
        })
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        self.invalidate_results();
        let _connection_operation = self.register_connection_operation()?;
        let Some(binding) = self.binding.take() else {
            if !self.enable_direct_results {
                let (executed, stream) = self.execute_async_inner()?;
                let count = executed.num_modified_rows();
                drop(stream);
                self.runtime
                    .block_on_safe(executed.close())
                    .map_err(map_kernel_error)?;
                return Ok(count);
            }
            let executed = self.execute_inner()?;
            let count = executed.num_modified_rows();
            self.runtime
                .block_on_safe(executed.close())
                .map_err(map_kernel_error)?;
            return Ok(count);
        };

        if !self.enable_direct_results {
            return Err(unsupported(
                "parameter binding when direct results are disabled",
            ));
        }

        let mut rows = match binding {
            Binding::Batch(batch) => BoundRows::from_batch(batch),
            Binding::Stream(reader) => BoundRows::from_stream(reader),
        };
        let base_spec = self.inner()?.spec_ref().clone();
        let mut total = Some(0_i64);
        let mut first_error = None;
        let mut row_index = 0_usize;
        while let Some(row) = rows.next_row()? {
            let executed = match execute_bound_row(
                &self.inner,
                &self.runtime,
                &base_spec,
                &row,
                self.parameter_style,
                self.client_timeout,
            ) {
                Ok(executed) => executed,
                Err(mut error) => {
                    row_index += 1;
                    error.message = format!("batch row {row_index} failed: {}", error.message);
                    first_error.get_or_insert(error);
                    total = None;
                    continue;
                }
            };
            total = match (total, executed.num_modified_rows()) {
                (Some(total), Some(count)) => {
                    Some(total.checked_add(count).ok_or_else(|| {
                        adbc_error("affected row count overflow", Status::Internal)
                    })?)
                }
                _ => None,
            };
            // The kernel closes dropped executed handles asynchronously. Waiting
            // for every close would serialize an extra network round trip per row.
            drop(executed);
            row_index += 1;
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(total),
        }
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        self.invalidate_results();
        let connection_operation = self.register_connection_operation()?;
        let original_spec = self.inner()?.spec_ref().clone();
        let sql = original_spec
            .sql
            .clone()
            .ok_or_else(|| adbc_error("SQL query is not set", Status::InvalidState))?;
        if !is_describable_query(&sql) {
            return Err(unsupported("execute_schema for this SQL statement type"));
        }
        let describe_sql = format!(
            "DESCRIBE QUERY {}",
            kernel::statement::params::replace_parameter_markers_with_literals(&sql)
        );
        let result = (|| {
            {
                let mut inner = self.inner()?;
                *inner.spec() = original_spec.clone();
                inner.spec().sql(describe_sql);
            }
            let executed = self.execute_inner()?;
            let reader = KernelReader::new(
                Arc::clone(&self.runtime),
                executed,
                Arc::clone(&self.generation),
                self.generation.load(Ordering::Acquire),
                connection_operation,
            );
            kernel::statement::params::describe_result_to_schema(reader).map_err(map_kernel_error)
        })();
        let restore = self.inner().map(|mut inner| {
            *inner.spec() = original_spec;
        });
        match (result, restore) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(schema), Ok(())) => Ok(schema),
        }
    }
    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        Err(unsupported("execute_partitions"))
    }
    fn get_parameter_schema(&self) -> Result<Schema> {
        if !self.prepared {
            return Err(adbc_error(
                "get_parameter_schema requires prepare",
                Status::InvalidState,
            ));
        }
        if self.parameter_style == ParameterStyle::Named {
            return Err(unsupported(
                "get_parameter_schema for named parameter markers",
            ));
        }
        let sql = self
            .inner()?
            .spec_ref()
            .sql
            .clone()
            .ok_or_else(|| adbc_error("SQL query is not set", Status::InvalidState))?;
        let count = kernel::statement::params::count_parameter_markers(&sql);
        Ok(parameter_schema_from_binding(
            self.binding.as_ref(),
            count,
            self.parameter_style,
        ))
    }

    fn prepare(&mut self) -> Result<()> {
        if self.inner()?.spec_ref().sql.is_none() {
            return Err(adbc_error(
                "cannot prepare before setting SQL",
                Status::InvalidState,
            ));
        }
        self.invalidate_results();
        // SEA has no server prepare. Retaining SQL and reapplying binds on every
        // execution is the spec-correct client-side preparation available today.
        self.prepared = true;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.invalidate_results();
        self.inner()?.spec().sql(query.as_ref());
        self.binding = None;
        self.prepared = false;
        Ok(())
    }
    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(unsupported("Substrait plans"))
    }
    fn cancel(&mut self) -> Result<()> {
        self.cancellation_handle().cancel()?;
        self.current_cancellation.store(true, Ordering::Release);
        Ok(())
    }
}

fn parameter_schema_from_binding(
    binding: Option<&Binding>,
    count: usize,
    style: ParameterStyle,
) -> Schema {
    if let Some(binding) = binding {
        let schema = match binding {
            Binding::Batch(batch) => batch.schema(),
            Binding::Stream(reader) => reader.schema(),
        };
        if schema.fields().len() == count {
            if style == ParameterStyle::Named {
                return schema.as_ref().clone();
            }
            let fields = schema
                .fields()
                .iter()
                .map(|field| {
                    Field::new("", field.data_type().clone(), field.is_nullable())
                        .with_metadata(field.metadata().clone())
                })
                .collect::<Vec<_>>();
            return Schema::new_with_metadata(fields, schema.metadata().clone());
        }
    }
    Schema::new(
        (0..count)
            .map(|_| Field::new("", DataType::Null, true))
            .collect::<Vec<_>>(),
    )
}

fn convert_binding(batch: &RecordBatch, style: ParameterStyle) -> Result<Vec<BoundParameter>> {
    if batch.num_rows() != 1 {
        return Err(adbc_error(
            format!("expected one parameter row, got {}", batch.num_rows()),
            Status::InvalidArguments,
        ));
    }
    let parameters = kernel::statement::params::record_batch_to_sea_parameters(batch)
        .map_err(map_kernel_error)?;
    parameters
        .into_iter()
        .enumerate()
        .map(|(index, parameter)| {
            let binary = matches!(
                batch.schema().field(index).data_type(),
                DataType::Binary | DataType::LargeBinary
            );
            Ok(BoundParameter {
                name: match style {
                    ParameterStyle::Positional => None,
                    ParameterStyle::Named => Some(batch.schema().field(index).name().clone()),
                },
                // SEA currently interprets BINARY parameter text as literal
                // UTF-8 bytes. Send the kernel-produced hex as STRING and
                // reconstruct bytes in SQL with unhex(?).
                param_type: if binary {
                    "STRING".into()
                } else {
                    parameter.param_type
                },
                value: parameter.value,
                binary,
            })
        })
        .collect()
}

fn validate_binding_schema(schema: &arrow_schema::SchemaRef, style: ParameterStyle) -> Result<()> {
    if style == ParameterStyle::Named {
        let mut names = HashSet::with_capacity(schema.fields().len());
        for field in schema.fields() {
            if !is_valid_named_parameter(field.name()) {
                return Err(adbc_error(
                    format!(
                        "named parameter field {:?} must match [A-Za-z_][A-Za-z0-9_]*",
                        field.name()
                    ),
                    Status::InvalidArguments,
                ));
            }
            if !names.insert(field.name()) {
                return Err(adbc_error(
                    format!("duplicate named parameter field {:?}", field.name()),
                    Status::InvalidArguments,
                ));
            }
        }
    }
    Ok(())
}

fn is_valid_named_parameter(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(crate) fn execute_bound_row(
    inner: &Arc<Mutex<kernel::Statement>>,
    runtime: &Arc<RuntimeHandle>,
    base_spec: &kernel::StatementSpec,
    row: &RecordBatch,
    style: ParameterStyle,
    client_timeout: Option<Duration>,
) -> Result<kernel::ExecutedStatement> {
    let parameters = convert_binding(row, style)?;
    let mut inner = inner
        .lock()
        .map_err(|_| adbc_error("kernel statement lock is poisoned", Status::Internal))?;
    *inner.spec() = base_spec.clone();
    let base_sql = inner
        .spec_ref()
        .sql
        .clone()
        .ok_or_else(|| adbc_error("SQL query is not set", Status::InvalidState))?;
    let sql = match style {
        ParameterStyle::Positional => {
            let binary_parameters = parameters
                .iter()
                .enumerate()
                .filter_map(|(index, parameter)| parameter.binary.then_some(index))
                .collect::<Vec<_>>();
            rewrite_binary_parameter_markers(&base_sql, &binary_parameters)?
        }
        ParameterStyle::Named => {
            let binary_names = parameters
                .iter()
                .filter(|parameter| parameter.binary)
                .map(|parameter| {
                    parameter
                        .name
                        .as_deref()
                        .expect("named style always supplies parameter names")
                })
                .collect::<HashSet<_>>();
            rewrite_named_binary_parameter_markers(&base_sql, &binary_names)?
        }
    };
    // sql() is the public reset point for every parameter representation.
    inner.spec().sql(sql);
    for parameter in parameters {
        inner
            .spec()
            .param_raw(parameter.name, parameter.param_type, parameter.value);
    }
    match client_timeout {
        Some(timeout) => runtime.block_on_safe(inner.execute_with_timeout(timeout)),
        None => runtime.block_on_safe(inner.execute()),
    }
    .map_err(map_kernel_error)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SqlMarkerState {
    Normal,
    SingleQuote,
    DoubleQuote,
    BacktickQuote,
    LineComment,
    BlockComment,
}

fn rewrite_binary_parameter_markers(sql: &str, binary_parameters: &[usize]) -> Result<String> {
    rewrite_parameter_markers(sql, binary_parameters, &HashSet::new())
}

fn rewrite_named_binary_parameter_markers(
    sql: &str,
    binary_names: &HashSet<&str>,
) -> Result<String> {
    rewrite_parameter_markers(sql, &[], binary_names)
}

fn rewrite_parameter_markers(
    sql: &str,
    binary_parameters: &[usize],
    binary_names: &HashSet<&str>,
) -> Result<String> {
    if binary_parameters.is_empty() && binary_names.is_empty() {
        return Ok(sql.to_owned());
    }
    let mut rewritten =
        Vec::with_capacity(sql.len() + (binary_parameters.len() + binary_names.len()) * 7);
    let bytes = sql.as_bytes();
    let mut state = SqlMarkerState::Normal;
    let mut index = 0;
    let mut marker = 0;
    let mut rewritten_count = 0;
    let mut rewritten_names = HashSet::with_capacity(binary_names.len());
    while index < bytes.len() {
        match state {
            SqlMarkerState::Normal => {
                if bytes[index] == b'?' {
                    if binary_parameters.contains(&marker) {
                        rewritten.extend_from_slice(b"unhex(?)");
                        rewritten_count += 1;
                    } else {
                        rewritten.push(b'?');
                    }
                    marker += 1;
                    index += 1;
                } else if bytes[index] == b':'
                    && (index == 0
                        || (bytes[index - 1] != b':'
                            && !is_named_parameter_continue(bytes[index - 1])))
                    && bytes
                        .get(index + 1)
                        .is_some_and(|byte| is_named_parameter_start(*byte))
                {
                    let mut end = index + 2;
                    while bytes
                        .get(end)
                        .is_some_and(|byte| is_named_parameter_continue(*byte))
                    {
                        end += 1;
                    }
                    let name = std::str::from_utf8(&bytes[index + 1..end])
                        .expect("named parameter identifiers are ASCII");
                    if binary_names.contains(name) {
                        rewritten.extend_from_slice(b"unhex(");
                        rewritten.extend_from_slice(&bytes[index..end]);
                        rewritten.push(b')');
                        rewritten_names.insert(name);
                    } else {
                        rewritten.extend_from_slice(&bytes[index..end]);
                    }
                    index = end;
                } else if bytes[index] == b'\'' {
                    state = SqlMarkerState::SingleQuote;
                    rewritten.push(b'\'');
                    index += 1;
                } else if bytes[index] == b'"' {
                    state = SqlMarkerState::DoubleQuote;
                    rewritten.push(b'"');
                    index += 1;
                } else if bytes[index] == b'`' {
                    state = SqlMarkerState::BacktickQuote;
                    rewritten.push(b'`');
                    index += 1;
                } else if bytes.get(index..index + 2) == Some(b"--") {
                    state = SqlMarkerState::LineComment;
                    rewritten.extend_from_slice(b"--");
                    index += 2;
                } else if bytes.get(index..index + 2) == Some(b"/*") {
                    state = SqlMarkerState::BlockComment;
                    rewritten.extend_from_slice(b"/*");
                    index += 2;
                } else {
                    rewritten.push(bytes[index]);
                    index += 1;
                }
            }
            SqlMarkerState::SingleQuote => {
                rewritten.push(bytes[index]);
                if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        rewritten.push(b'\'');
                        index += 2;
                    } else {
                        state = SqlMarkerState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            SqlMarkerState::DoubleQuote => {
                rewritten.push(bytes[index]);
                if bytes[index] == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        rewritten.push(b'"');
                        index += 2;
                    } else {
                        state = SqlMarkerState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            SqlMarkerState::BacktickQuote => {
                rewritten.push(bytes[index]);
                if bytes[index] == b'`' {
                    if bytes.get(index + 1) == Some(&b'`') {
                        rewritten.push(b'`');
                        index += 2;
                    } else {
                        state = SqlMarkerState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            SqlMarkerState::LineComment => {
                rewritten.push(bytes[index]);
                if bytes[index] == b'\n' {
                    state = SqlMarkerState::Normal;
                }
                index += 1;
            }
            SqlMarkerState::BlockComment => {
                if bytes.get(index..index + 2) == Some(b"*/") {
                    rewritten.extend_from_slice(b"*/");
                    state = SqlMarkerState::Normal;
                    index += 2;
                } else {
                    rewritten.push(bytes[index]);
                    index += 1;
                }
            }
        }
    }
    if rewritten_count != binary_parameters.len() {
        return Err(adbc_error(
            "binary parameter has no corresponding positional SQL marker",
            Status::InvalidArguments,
        ));
    }
    if rewritten_names.len() != binary_names.len() {
        return Err(adbc_error(
            "binary named parameter has no corresponding SQL marker",
            Status::InvalidArguments,
        ));
    }
    Ok(String::from_utf8(rewritten).expect("rewriting preserves UTF-8"))
}

fn is_named_parameter_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_named_parameter_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_describable_query(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0;
    loop {
        while index < bytes.len() && (bytes[index].is_ascii_whitespace() || bytes[index] == b'(') {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"--") {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"/*") {
            index += 2;
            while index + 1 < bytes.len() && &bytes[index..index + 2] != b"*/" {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        break;
    }
    let end = bytes[index..]
        .iter()
        .position(|byte| !byte.is_ascii_alphabetic())
        .map(|offset| index + offset)
        .unwrap_or(bytes.len());
    matches!(
        sql[index..end].to_ascii_uppercase().as_str(),
        "SELECT" | "WITH" | "VALUES" | "FROM"
    )
}

fn expect_string(value: OptionValue, key: &str) -> Result<String> {
    match value {
        OptionValue::String(value) => Ok(value),
        _ => Err(adbc_error(
            format!("{key} expects a string value"),
            Status::InvalidArguments,
        )),
    }
}

fn expect_bool_string(value: OptionValue, key: &str) -> Result<bool> {
    let value = expect_string(value, key)?;
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(adbc_error(
            format!("{key} expects true or false"),
            Status::InvalidArguments,
        )),
    }
}

fn metadata_string_option(value: Option<&String>, key: &str) -> Result<String> {
    value
        .cloned()
        .ok_or_else(|| adbc_error(format!("{key} is not set"), Status::NotFound))
}

fn expect_nonnegative_int(value: OptionValue, key: &str) -> Result<i64> {
    let value = match value {
        OptionValue::Int(value) => value,
        OptionValue::String(value) => value.parse().map_err(|_| {
            adbc_error(
                format!("{key} expects a nonnegative integer"),
                Status::InvalidArguments,
            )
        })?,
        _ => {
            return Err(adbc_error(
                format!("{key} expects a nonnegative integer"),
                Status::InvalidArguments,
            ));
        }
    };
    if value < 0 {
        return Err(adbc_error(
            format!("{key} expects a nonnegative integer"),
            Status::InvalidArguments,
        ));
    }
    Ok(value)
}

fn nonzero_duration(value: i64, convert: impl FnOnce(u64) -> Duration) -> Option<Duration> {
    (value != 0).then(|| convert(value as u64))
}

fn parse_client_timeout_option(key: &str, value: OptionValue) -> Option<Result<Option<Duration>>> {
    let convert: fn(u64) -> Duration = match key {
        CLIENT_TIMEOUT_OPTION => Duration::from_millis,
        QUERY_TIMEOUT_OPTION => Duration::from_secs,
        _ => return None,
    };
    Some(expect_nonnegative_int(value, key).map(|value| nonzero_duration(value, convert)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BinaryArray, Int64Array, LargeBinaryArray, StringArray};
    use arrow_schema::{DataType, Field};

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let ids = Int64Array::from_iter_values((0..rows).map(|value| value as i64));
        let labels = StringArray::from_iter((0..rows).map(|_| Some("value")));
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(labels)]).unwrap()
    }

    #[test]
    fn parameter_schema_uses_matching_bound_arrow_types() {
        let batch = batch(1);
        let expected = batch.schema().as_ref().clone();
        let binding = Binding::Batch(batch);
        let actual = parameter_schema_from_binding(Some(&binding), 2, ParameterStyle::Positional);
        assert_eq!(
            actual
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            vec!["", ""]
        );
        assert_eq!(actual.field(0).data_type(), expected.field(0).data_type());
        assert_eq!(actual.field(1).data_type(), expected.field(1).data_type());
        assert_eq!(
            actual.field(0).is_nullable(),
            expected.field(0).is_nullable()
        );
        assert_eq!(
            actual.field(1).is_nullable(),
            expected.field(1).is_nullable()
        );

        let unknown = parameter_schema_from_binding(Some(&binding), 1, ParameterStyle::Positional);
        assert_eq!(unknown.fields().len(), 1);
        assert_eq!(unknown.field(0).data_type(), &DataType::Null);
        assert!(unknown.field(0).is_nullable());
    }

    #[test]
    fn positional_binding_preserves_types_and_values() {
        let params = convert_binding(&batch(1), ParameterStyle::Positional).unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, None);
        assert_eq!(params[0].param_type, "BIGINT");
        assert_eq!(params[0].value.as_deref(), Some("0"));
        assert_eq!(params[1].param_type, "STRING");
    }

    #[test]
    fn named_binding_uses_arrow_field_names() {
        let params = convert_binding(&batch(1), ParameterStyle::Named).unwrap();
        assert_eq!(params[0].name.as_deref(), Some("id"));
        assert_eq!(params[1].name.as_deref(), Some("label"));
    }

    #[test]
    fn binary_binding_sends_hex_as_string() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            true,
        )]));
        let values = BinaryArray::from(vec![Some(&[0x01, 0x02, 0x03, 0xff][..])]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
        let params = convert_binding(&batch, ParameterStyle::Positional).unwrap();
        assert!(params[0].binary);
        assert_eq!(params[0].param_type, "STRING");
        assert_eq!(params[0].value.as_deref(), Some("010203ff"));
    }

    #[test]
    fn large_binary_and_null_binary_preserve_semantics() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("large", DataType::LargeBinary, true),
            Field::new("null", DataType::Binary, true),
        ]));
        let large = LargeBinaryArray::from(vec![Some(&[0x80, 0xff][..])]);
        let null = BinaryArray::from(vec![None::<&[u8]>]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(large), Arc::new(null)]).unwrap();
        let params = convert_binding(&batch, ParameterStyle::Positional).unwrap();
        assert_eq!(params[0].value.as_deref(), Some("80ff"));
        assert_eq!(params[1].value, None);
        assert!(params.iter().all(|parameter| parameter.binary));
        assert!(params
            .iter()
            .all(|parameter| parameter.param_type == "STRING"));
        assert_eq!(
            rewrite_binary_parameter_markers("SELECT ?, ?", &[0, 1]).unwrap(),
            "SELECT unhex(?), unhex(?)"
        );
    }

    #[test]
    fn binary_rewrite_targets_only_real_selected_markers() {
        let sql = "SELECT '?' AS literal, ? AS number, ? AS bytes -- ?\n/* ? */ WHERE `?` = \"?\"";
        assert_eq!(
            rewrite_binary_parameter_markers(sql, &[1]).unwrap(),
            "SELECT '?' AS literal, ? AS number, unhex(?) AS bytes -- ?\n/* ? */ WHERE `?` = \"?\""
        );
    }

    #[test]
    fn binary_rewrite_is_stable_across_repeated_rows() {
        let base_sql = "SELECT ? AS payload";
        let first = rewrite_binary_parameter_markers(base_sql, &[0]).unwrap();
        let second = rewrite_binary_parameter_markers(base_sql, &[0]).unwrap();
        assert_eq!(first, "SELECT unhex(?) AS payload");
        assert_eq!(second, first);
        assert!(!second.contains("unhex(unhex"));
    }

    #[test]
    fn named_binary_binding_uses_string_hex_payload() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            true,
        )]));
        let values = BinaryArray::from(vec![Some(&[0x01][..])]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
        let params = convert_binding(&batch, ParameterStyle::Named).unwrap();
        assert_eq!(params[0].name.as_deref(), Some("payload"));
        assert_eq!(params[0].param_type, "STRING");
        assert_eq!(params[0].value.as_deref(), Some("01"));
    }

    #[test]
    fn named_binary_rewrite_wraps_repeated_markers_only() {
        let names = HashSet::from(["payload"]);
        let sql = "SELECT :plain, :payload, :payload, ':payload', \" :payload \", `:payload` -- :payload\n/* :payload */";
        assert_eq!(
            rewrite_named_binary_parameter_markers(sql, &names).unwrap(),
            "SELECT :plain, unhex(:payload), unhex(:payload), ':payload', \" :payload \", `:payload` -- :payload\n/* :payload */"
        );
    }

    #[test]
    fn named_binary_rewrite_respects_identifier_boundaries() {
        let names = HashSet::from(["payload"]);
        assert!(rewrite_named_binary_parameter_markers("SELECT x:payload", &names).is_err());
        assert_eq!(
            rewrite_named_binary_parameter_markers(
                "SELECT :payload_suffix, value::payload, :payload",
                &names
            )
            .unwrap(),
            "SELECT :payload_suffix, value::payload, unhex(:payload)"
        );
    }

    #[test]
    fn named_binding_rejects_invalid_or_ambiguous_field_names() {
        for invalid in ["", "1payload", "payload-name", "payload.name"] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                invalid,
                DataType::Binary,
                true,
            )]));
            let error = validate_binding_schema(&schema, ParameterStyle::Named).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.contains("must match"));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let error = validate_binding_schema(&schema, ParameterStyle::Named).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("duplicate"));
    }

    #[test]
    fn single_row_conversion_rejects_unsliced_batches() {
        let error = convert_binding(&batch(2), ParameterStyle::Positional).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("expected one parameter row"));
    }

    #[test]
    fn option_parsers_reject_wrong_types_and_negative_values() {
        assert!(expect_string(OptionValue::Int(1), "x").is_err());
        assert!(expect_nonnegative_int(OptionValue::Int(-1), "x").is_err());
        assert_eq!(
            expect_nonnegative_int(OptionValue::String("42".into()), "x").unwrap(),
            42
        );
        assert_eq!(nonzero_duration(0, Duration::from_secs), None);
        assert_eq!(
            nonzero_duration(42, Duration::from_secs),
            Some(Duration::from_secs(42))
        );
        assert_eq!(
            parse_client_timeout_option(QUERY_TIMEOUT_OPTION, OptionValue::String("42".into()))
                .unwrap()
                .unwrap(),
            Some(Duration::from_secs(42))
        );
        assert_eq!(
            parse_client_timeout_option(CLIENT_TIMEOUT_OPTION, OptionValue::Int(42))
                .unwrap()
                .unwrap(),
            Some(Duration::from_millis(42))
        );
        assert!(parse_client_timeout_option("not-a-timeout", OptionValue::Int(42)).is_none());
        assert!(expect_bool_string(OptionValue::String("TRUE".into()), "x").unwrap());
        assert!(!expect_bool_string(OptionValue::String("false".into()), "x").unwrap());
        assert!(expect_bool_string(OptionValue::String("yes".into()), "x").is_err());
    }

    #[test]
    fn describe_query_classifier_accepts_queries_but_not_commands() {
        assert!(is_describable_query(
            "/* comment */ WITH t AS (SELECT 1) SELECT * FROM t"
        ));
        assert!(is_describable_query("(SELECT 1)"));
        assert!(!is_describable_query("INSERT INTO t VALUES (1)"));
        assert!(!is_describable_query("DESCRIBE TABLE t"));
    }
}
