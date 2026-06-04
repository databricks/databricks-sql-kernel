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

//! Databricks SQL Kernel
//!
//! Rust client core for Databricks SQL — the shared engine that powers
//! the Databricks ODBC, Python, Go, and Node.js drivers.
//!
//! ## Overview
//!
//! The v0 Kernel API surfaces a Databricks-native, async-first
//! interface (see `docs/spec-v0.md`):
//!
//! - [`Session`] — stateful authenticated handle (collapses ADBC's
//!   `Database` + `Connection`).
//! - [`Statement`] — mutable lifecycle owner; produces immutable
//!   executed handles via `execute()` / `submit()`.
//! - [`ExecutedStatement`] / [`ExecutedAsyncStatement`] — sync /
//!   async-consumption executed handles, sharing the
//!   [`ExecutedStatementHandle`] trait.
//! - [`Metadata`] — flat per-method catalog metadata API.
//! - [`ResultStream`] — paginated result with a borrowed `Row<'a>`
//!   cursor.
//!
//! Two consumer surfaces:
//!
//! - **Direct Rust crate** for PyO3 / napi-rs wrappers (Python and
//!   Node.js).
//! - **Custom C ABI** under `crate::ffi` (skeleton today; bodies land
//!   when an ODBC / Go consumer needs them).
//!
//! ## Example
//!
//! ```ignore
//! use databricks_sql_kernel::{
//!     AuthConfig, ConnectionConfig, Session, TypedValue,
//! };
//!
//! let session = Session::builder()
//!     .connection(ConnectionConfig::new(
//!         "https://my-workspace.databricks.com",
//!         "abc123",
//!     ))
//!     .auth(AuthConfig::Pat { token: "dapi...".into() })
//!     .open()
//!     .await?;
//!
//! let mut stmt = session.statement();
//! stmt.spec()
//!     .sql("SELECT * FROM my_table WHERE id = ?")
//!     .param(1, TypedValue::Int64(42))?;
//!
//! let mut exec = stmt.execute().await?;
//! while let Some(_batch) = exec.result_stream_mut().next_batch().await? {
//!     while let Some(_row) = exec.result_stream_mut().next_row_in_batch() {
//!         // process row
//!     }
//! }
//! exec.close().await?;
//! ```

// === TLS feature wiring guard ===
//
// Exactly one of `tls-native` / `tls-rustls` must be active. Zero
// would compile but every HTTPS call would fail at runtime (reqwest
// can't pick a backend); both would link two TLS stacks into the
// binary and defeat the napi crate's reason for choosing rustls.
// Catch both cases at the build that introduces them.
#[cfg(not(any(feature = "tls-native", feature = "tls-rustls")))]
compile_error!(
    "databricks-sql-kernel requires one of `tls-native` (default) or `tls-rustls`. \
     If you set `default-features = false`, opt back into one explicitly."
);

#[cfg(all(feature = "tls-native", feature = "tls-rustls"))]
compile_error!(
    "`tls-native` and `tls-rustls` are mutually exclusive — enabling both links two \
     TLS stacks and defeats the napi crate's rustls switch. The napi binding must \
     disable defaults (`default-features = false`) before enabling `tls-rustls`."
);

pub mod auth;
pub mod client;
pub(crate) mod klog;
pub(crate) mod logging;
pub mod metadata;
pub mod reader;
pub mod result;
pub mod statement;
pub mod telemetry;
pub mod types;

pub mod config;
pub mod kernel_error;
pub mod metadata_v0;
pub mod result_stream;
pub mod session;
pub(crate) mod util;

// Custom C ABI for ODBC / Go consumers (skeleton).
pub mod ffi;

// === Public re-exports ===
pub use config::{
    AuthConfig, ConnectionConfig, DefaultOpts, HttpConfig, LoggingConfig, ProxyConfig,
    ResultConfig, SessionConfig, TelemetryConfig, TlsConfig, TransportMode,
};
pub use kernel_error::{Error, ErrorCode, Result};
/// The `tracing` target string stamped on every kernel log event.
/// Binding crates (pyo3, napi) reference this constant directly so the
/// logger name they expose to users can never drift from what the
/// kernel actually emits. See [`klog::KERNEL_LOG_TARGET`].
pub use klog::KERNEL_LOG_TARGET;
pub use metadata_v0::{Identifier, LikePattern, Metadata};
pub use result_stream::{OwnedBatchIter, ResultBatch, ResultStream, Row, SchemaRef};
pub use session::{Session, SessionBuilder};
pub use statement::executed::ExecutedStatement;
pub use statement::executed_async::{
    AsyncStatementCanceller, ExecutedAsyncStatement, StatementStatus,
};
pub use statement::handle::ExecutedStatementHandle;
pub use statement::mutable::Statement;
pub use statement::params_codec::parse_typed_value;
pub use statement::spec::{ResultFormat, StatementSpec, TypedValue};

// === Compile-time guarantees on auto-traits ===
//
// Public handles MUST be `Send + Sync` so wrappers (PyO3 `pyclass`,
// napi-rs resource wrappers, the C ABI) can move them across threads.
// Today `Arc<SessionInner>` derives both trivially, but the moment PR
// 4a adds `transport: Arc<dyn Transport>` (or any non-`Send` field) the
// auto-traits silently regress. These assertions catch that at the
// build that introduces it. Same idiom Tokio / hyper / sqlx use.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}

    // Stateful handles
    assert_send_sync::<Session>();
    assert_send_sync::<SessionBuilder>();
    assert_send_sync::<Statement>();
    assert_send_sync::<ExecutedStatement>();
    assert_send_sync::<ExecutedAsyncStatement>();
    assert_send_sync::<Metadata>();
    assert_send_sync::<ResultStream>();

    // Plain-data / value types
    assert_send_sync::<StatementSpec>();
    assert_send_sync::<Error>();
    assert_send_sync::<ErrorCode>();
    assert_send_sync::<ResultBatch>();
    assert_send_sync::<ResultFormat>();
    assert_send_sync::<TypedValue>();

    // Newtypes carry a lifetime; assert against the `'static` instantiation
    // (the borrow lifetime is not what determines auto-trait status).
    assert_send_sync::<Identifier<'static>>();
    assert_send_sync::<LikePattern<'static>>();
};

// Futures returned by the async public surface MUST stay `Send` so
// napi-rs's `execute_tokio_future` and `tokio::spawn` accept them.
// `tracing::span::EnteredSpan` is `!Send` (it carries a
// `PhantomData<*mut ()>`), so any maintainer reverting an
// `async {}.instrument(span)` pattern back to a `let _g = span.entered();`
// guard inside an async fn would silently regress this invariant —
// all in-tree tests would still pass. These per-method asserts catch
// it at the build that introduces it.
const _: fn() = || {
    fn assert_send<T: Send>(_: &T) {}
    // The bindings on these are only used for type-checking; the
    // closure is never invoked at runtime.
    let _: fn(&mut Statement) = |stmt| {
        assert_send(&stmt.execute());
        assert_send(&stmt.submit());
    };
    let _: fn(Statement) = |stmt| {
        assert_send(&stmt.close());
    };
    let _: fn(&ExecutedAsyncStatement) = |exec| {
        assert_send(&exec.status());
        assert_send(&exec.await_result());
        assert_send(&exec.fetch_result());
    };
    let _: fn(Session) = |session| {
        assert_send(&session.close());
    };
    let _: fn(&Metadata) = |md| {
        assert_send(&md.list_catalogs());
        assert_send(&md.list_schemas(None, None));
        assert_send(&md.list_tables(None, None, None, None));
        assert_send(&md.list_columns(None, None, None, None));
        assert_send(&md.functions(None, None, None));
        assert_send(&md.procedures(None, None, None));
        assert_send(&md.type_info());
        assert_send(&md.table_types());
    };
};
