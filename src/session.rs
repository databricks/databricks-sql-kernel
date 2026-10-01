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

//! `Session` — the v0 Kernel entry point.
//!
//! Per `docs/spec-v0.md` "Public Rust API → Session", `Session`
//! collapses ADBC's `Database` + `Connection` into a single stateful
//! authenticated handle. It is the surface every wrapper (ODBC, PySQL,
//! NodeJS, Go) targets, and the C ABI is a thin shim over its async
//! methods.
//!
//! `'static` (no Rust lifetime parameter) for the same reason as every
//! other v0 handle — see `docs/spec-v0.md` addendum A1, "Why no Rust
//! lifetimes on the handles". Shared state lives behind
//! `Arc<SessionInner>`.
//!
//! ## Async runtime
//!
//! `Session::open` is an `async fn` and assumes the caller owns a
//! tokio runtime — it grabs the current runtime handle via
//! [`tokio::runtime::Handle::current`]. Callers are PyO3 / napi-rs
//! wrappers that already manage a runtime, or the C ABI shim (PR 6)
//! which owns a process-wide runtime and `block_on`s every entry
//! point.
//!
//! ## Transactions
//!
//! Multi-statement transaction control (`set_autocommit` / `commit` /
//! `rollback`) is expressed as session-scoped SQL (`SET AUTOCOMMIT`,
//! `COMMIT`, `ROLLBACK`), mirroring the reference and JDBC drivers, and
//! is shared by every wrapper via the C ABI. It is gated by
//! [`DefaultOpts::ignore_transactions`] (default = ignore, matching
//! `IgnoreTransactions=1`): when ignored, the methods are no-ops; when
//! enabled, they drive real transactions (a Private Preview feature that
//! requires catalog-managed tables server-side).

use crate::auth::{
    AuthProvider, AuthorizationCodeProvider, ClientCredentialsProvider, FederationProvider,
    JwtClientCredentialsProvider, PersonalAccessToken,
};
use crate::client::http::TlsConfig as HttpTlsConfig;
use crate::client::retry::{
    build_retry_configs, RequestCategory, RetryConfig, RetryConfigOverrides,
};
use crate::client::transport::TransportConfig;
use crate::client::{
    DatabricksHttpClient, HttpClientConfig, ProxyConfig as HttpProxyConfig, SeaRestTransport,
    SessionAttribution, Transport,
};
use crate::config::{
    AuthConfig, ConnectionConfig, DefaultOpts, GeospatialResultRepresentation, HttpConfig,
    LoggingConfig, ProxyConfig, ResultConfig, SessionConfig, TelemetryConfig, TlsConfig,
    TransportMode,
};
use crate::feature_flags::{ClientIdentity, FeatureFlagResolver};
use crate::kernel_error::{Error, Result};
use crate::klog::debug;
use crate::metadata_v0::Metadata;
use crate::reader::ResultReaderFactory;
use crate::statement::executed_async::ExecutedAsyncStatement;
use crate::statement::mutable::Statement;
use crate::statement::timeout::TimeoutCleanupManager;
use crate::telemetry::{DriverSystemConfiguration, OperationType, TelemetryRuntime};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info_span, Instrument, Span};

/// Crate-internal session state. Wrapped in `Arc` and shared with
/// every child handle (`Statement`, `ExecutedStatement`, …) so the
/// kernel surface stays `'static`-friendly for foreign-language
/// wrappers.
#[allow(
    dead_code,
    reason = "config / runtime_handle are read by Statement wiring in PR 4"
)]
pub(crate) struct SessionInner {
    /// Wire-protocol abstraction. v0 holds a `SeaRestTransport`
    /// behind the trait object; future variants slot in via
    /// `TransportMode`.
    transport: Arc<dyn Transport>,

    /// Server-issued session identifier.
    session_id: String,

    /// Snapshot of the config the session was opened with.
    /// Sub-handles read it for re-execute and option lookups.
    config: SessionConfig,

    /// Tokio runtime used to drive the transport. Cloned into
    /// every child handle so they can `block_on` if they need to
    /// (e.g. the [`Drop`] impl below).
    runtime_handle: tokio::runtime::Handle,

    /// Admission control for timeout cleanup.
    timeout_cleanup: TimeoutCleanupManager,

    /// Tracing span tagged with `session_id`. Every public method
    /// enters this span via `let _g = self.inner.log_span.enter();`
    /// so log lines tied to a session are visibly grouped.
    log_span: Span,

    /// Session-scoped telemetry runtime. It is a no-op runtime when
    /// telemetry is disabled or initialization fails, so callsites can
    /// emit lifecycle events without branching on config.
    telemetry: Arc<TelemetryRuntime>,

    /// Authenticated connector-service reader for this workspace and outer
    /// driver. Values live in the process cache; the session retains no flag
    /// snapshot.
    feature_flags: Option<FeatureFlagResolver>,

    /// Flips to `true` only after [`Session::close`] has
    /// **successfully** awaited the `delete_session` call. Tells
    /// [`Drop`] to skip its fire-and-forget cleanup so a successful
    /// explicit close doesn't trigger a redundant retry. On `close`
    /// returning `Err` (or being cancelled mid-await) the flag stays
    /// `false`, so [`Drop`] retries via its fire-and-forget path —
    /// transient `delete_session` failures don't leak the server-
    /// side session.
    closed: std::sync::atomic::AtomicBool,

    /// Client-tracked autocommit state. `true` (the default) means every
    /// statement auto-commits at the server; `false` means statements run
    /// inside an explicit transaction until [`Session::commit`] /
    /// [`Session::rollback`]. Toggled by [`Session::set_autocommit`], which
    /// also issues the corresponding `SET AUTOCOMMIT` statement to the
    /// server. Cached here (rather than round-tripped on every read),
    /// mirroring the JDBC driver's `DatabricksSession.autoCommit`.
    autocommit: std::sync::atomic::AtomicBool,

    /// When `true`, transaction-control operations ([`Session::set_autocommit`]
    /// with `false`, [`Session::commit`], [`Session::rollback`]) are silently
    /// no-oped — they return `Ok(())` without issuing any server statement.
    /// Mirrors the reference/JDBC `IgnoreTransactions` connection property
    /// (default = ignore); drivers pass it through via
    /// [`DefaultOpts::ignore_transactions`]. Multi-statement transactions are
    /// a Private Preview feature; setting this `false` opts in.
    ignore_transactions: bool,
}

impl SessionInner {
    /// Crate-internal accessor for the session's transport. Used by
    /// `Metadata` (and PR 4's `Statement`) to drive RPCs without
    /// reaching into private fields.
    pub(crate) fn transport(&self) -> &Arc<dyn Transport> {
        &self.transport
    }

    /// Crate-internal accessor for the server-issued session ID.
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Typed feature-flag reader for kernel-internal consumers such as
    /// statements and result handling. Reads use the process-wide cache.
    // This accessor intentionally precedes its first kernel-owned consumer.
    #[allow(dead_code)]
    pub(crate) fn feature_flags(&self) -> Option<&FeatureFlagResolver> {
        self.feature_flags.as_ref()
    }

    /// Catalog configured as the session default, if any.
    pub(crate) fn default_catalog(&self) -> Option<&str> {
        self.config
            .defaults
            .as_ref()
            .and_then(|defaults| defaults.catalog.as_deref())
    }

    /// Crate-internal accessor for the per-session tracing span.
    /// Callers clone the span and `.entered()` it at the top of each
    /// public method body.
    pub(crate) fn log_span(&self) -> &Span {
        &self.log_span
    }

    /// Crate-internal accessor for the session's tokio runtime
    /// handle. Used by `ExecutedStatement::Drop` to spawn the
    /// fire-and-forget `close_statement` clean-up.
    pub(crate) fn runtime_handle(&self) -> &tokio::runtime::Handle {
        &self.runtime_handle
    }

    pub(crate) fn dispatch_timeout_cleanup(&self, statement_id: Option<String>) {
        self.timeout_cleanup.dispatch(
            &self.runtime_handle,
            Arc::clone(&self.transport),
            statement_id,
        );
    }

    pub(crate) fn statement_telemetry_observer(
        &self,
        statement_type: crate::telemetry::StatementType,
        operation_type: OperationType,
    ) -> crate::telemetry::StatementTelemetry {
        self.telemetry
            .statement_observer(statement_type, operation_type)
    }

    /// Initiate the fire-and-forget server-side `DeleteSession` now,
    /// idempotently. Flips `closed` first (via a compare-and-swap) so at
    /// most one delete is ever spawned — a later `Drop` (or a second
    /// call) becomes a no-op. Unlike waiting for the last `Arc` to drop,
    /// this fires immediately even while child handles (statements /
    /// executed handles) still hold `Arc<SessionInner>` clones, so an
    /// explicit close genuinely *initiates* the delete rather than
    /// deferring it to an idle-timeout.
    pub(crate) fn initiate_delete(&self) {
        if self
            .closed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return; // already closed / closing
        }
        let transport = Arc::clone(&self.transport);
        let session_id = self.session_id.clone();
        self.runtime_handle.spawn(async move {
            let _ = transport.delete_session(&session_id).await;
        });
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        // Fire-and-forget DeleteSession when the last `Arc` reference
        // goes away, unless an explicit close / a prior initiation
        // already fired it. `initiate_delete` is idempotent (flips
        // `closed` via compare-and-swap) and swallows errors — Drop must
        // not panic. The runtime keeps the background task alive until
        // completion.
        self.initiate_delete();
    }
}

/// Stateful authenticated session against a Databricks workspace.
///
/// `'static` (no lifetime parameter) so wrappers can hold it inside
/// `pyclass` / `napi` / opaque C handles. Cheaply cloneable — clones
/// share the same underlying `SessionInner` via `Arc`.
pub struct Session {
    inner: Arc<SessionInner>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Surface only public-safe fields. The full SessionConfig
        // contains auth credentials; redact via `AuthConfig::Debug`
        // (which already redacts tokens/secrets) by formatting only
        // the session_id at this layer.
        f.debug_struct("Session")
            .field("session_id", &self.inner.session_id)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Primary entry point. Returns a builder; chain config-group
    /// setters and call `.open()` to construct the session.
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }

    /// Lower-level entry point. Used by callers that already have a
    /// fully-formed `SessionConfig` (e.g. deserialized from JSON,
    /// cached from a prior session, or built by the C ABI from a
    /// struct).
    ///
    /// Builds the HTTP client (with retry config, proxy, TLS), the
    /// auth provider, the transport, and the result-reader factory;
    /// then calls `Transport::create_session` to mint a session id
    /// on the server.
    pub async fn open(mut config: SessionConfig) -> Result<Session> {
        // Fetch the runtime handle up-front so a misconfigured
        // caller (e.g. running outside a tokio context) gets a clear
        // error instead of a panic deep in the transport setup.
        let runtime_handle = tokio::runtime::Handle::try_current().map_err(|e| {
            Error::invalid_state().message(format!(
                "Session::open must be called from a tokio runtime context: {e}"
            ))
        })?;

        // A client certificate has no meaning on a plaintext transport.
        // Reject the contradictory configuration before constructing an
        // auth provider or issuing the CreateSession request; otherwise
        // reqwest silently ignores the identity for `http://` and sends the
        // bearer credential in cleartext.
        validate_mtls_endpoint_scheme(&config)?;

        // Install the tracing subscriber from the caller's LoggingConfig —
        // but ONLY when they explicitly configured a filter or a
        // destination. This is the direct-Rust counterpart to the C ABI's
        // `kernel_init_logging`: without it, a caller's `logging(...)` is
        // accepted and stored but never consumed, so a configured log
        // destination silently produces nothing (issue #213).
        //
        // The `is_some()` gate is load-bearing, NOT an optimisation. The
        // pyo3 binding opens sessions through this same path and NEVER sets
        // a `LoggingConfig`; it relies on the `tracing`→`log` bridge (the
        // crate's `tracing/log` feature) so `pyo3_log` can forward records
        // into Python's `logging`. Installing ANY subscriber here — even the
        // `RUST_LOG`/warn default an empty config would fall through to —
        // suppresses that bridge and breaks all kernel logging into Python.
        // So an unconfigured `LoggingConfig` must remain a true no-op,
        // preserving the "direct-Rust vs pyo3 are mutually exclusive"
        // invariant documented in `src/logging.rs`. It also keeps a C ABI
        // `kernel_init_logging(OFF, …)` — which intentionally installs
        // nothing — from being overridden by a stray `warn` subscriber on
        // the next `Session::open`.
        //
        // Idempotent and process-wide (`OnceLock`): the first configured
        // install (here or via `kernel_init_logging`) wins; later calls are
        // no-ops. The return value only signals whether the requested
        // destination was fully honoured; a `false` (log file unopenable, or
        // a subscriber already installed by the host) is a logging
        // degradation, not a reason to fail `open()`, so it is not
        // propagated as an error.
        if config.logging.filter.is_some() || config.logging.file.is_some() {
            let _ = crate::logging::init_logging(&crate::logging::LogConfig {
                filter: config.logging.filter.clone(),
                file: config.logging.file.clone(),
            });
        }

        // Fold client-only result knobs (e.g. cloudfetch_max_chunks_in_memory)
        // carried in session_conf into the typed ResultConfig and strip them
        // from session_conf, before either the reader factory reads
        // config.result (below) or the remaining server-bound session_conf
        // is sent on the wire.
        apply_client_result_overrides(&mut config);

        // Resolve the typed client-side representation once. It is applied
        // locally by the reader factory and is never synthesized into
        // `session_conf` or sent to SEA.
        let geospatial_result_representation =
            config.geospatial_result_representation.unwrap_or_default();

        let host = config.connection.host.clone();
        let warehouse_id = config.connection.warehouse_id.clone();
        let org_id = config.connection.org_id.clone();

        // 1. HTTP client — built before auth so OAuth providers can use
        //    it for OIDC discovery / token endpoint calls.
        let http_client_config = build_http_client_config(&config, org_id);
        let retry_configs = build_retry_configs_for_session(&config);
        let http_client = Arc::new(
            DatabricksHttpClient::new(http_client_config, retry_configs).map_err(|e| {
                Error::network_error().message(format!("Failed to construct HTTP client: {e}"))
            })?,
        );

        // 2. Auth provider — async because OAuth flows hit the OIDC
        //    discovery endpoint.
        let auth_provider: Arc<dyn AuthProvider> =
            build_auth_provider(&config.auth, &host, http_client.clone()).await?;
        http_client.set_auth_provider(auth_provider);

        // 3. Transport. Two-phase: SeaRestTransport needs an
        //    Arc<dyn Transport> to build the result-reader factory,
        //    which in turn needs to be set on the transport via
        //    OnceLock::set. So: construct, wrap in Arc, build factory,
        //    install factory.
        let defaults = config.defaults.clone().unwrap_or_default();
        let transport_config = TransportConfig {
            // Thread the connection-level CloudFetch toggle onto the
            // transport so every ExecuteStatement picks the right SEA
            // disposition (INLINE_OR_EXTERNAL_LINKS vs INLINE). This is
            // how cloud-fetch is disabled on SEA — see TransportConfig
            // and issue #196; there is no CAN_CLOUD_DOWNLOAD session conf.
            cloudfetch_enabled: config.result.cloudfetch_enabled,
            ..TransportConfig::default()
        };
        let sea = Arc::new(SeaRestTransport::new(
            http_client.clone(),
            host.clone(),
            warehouse_id.clone(),
            transport_config,
        ));
        let reader_factory = ResultReaderFactory::new(
            sea.clone() as Arc<dyn Transport>,
            http_client.clone(),
            result_to_cloudfetch_config(&config.result),
            result_to_post_process_options(&config.result),
            geospatial_result_representation,
            runtime_handle.clone(),
        );
        sea.set_reader_factory(reader_factory, runtime_handle.clone());
        let transport: Arc<dyn Transport> = sea;

        // 4. Mint the session on the server.
        debug!(
            host = %host,
            warehouse_id = %warehouse_id,
            "creating session"
        );

        // CloudFetch-disable is expressed per-statement via the SEA
        // `disposition` (threaded onto `TransportConfig` above), not via a
        // session conf. The kernel synthesises no geospatial session-conf
        // entry; caller-supplied server-bound entries pass through unchanged.
        // Resolve connector-service flags after authentication and immediately
        // before CreateSession. The resolver populates the process-wide
        // workspace cache; the session retains no private snapshot.
        let feature_flags = config
            .client_identity
            .clone()
            .map(|identity| FeatureFlagResolver::new(&host, Arc::clone(&http_client), identity));
        if let Some(feature_flags) = &feature_flags {
            feature_flags.prepare_for_session_open().await;
        }
        let create_started = Instant::now();
        let session_info = transport
            .create_session(
                defaults.catalog.as_deref(),
                defaults.schema.as_deref(),
                config.session_conf.clone(),
            )
            .await?;
        let create_elapsed = create_started.elapsed();

        let telemetry = Arc::new(TelemetryRuntime::from_config(
            &config.telemetry,
            &config.connection,
            &config.auth,
            config.driver_system_configuration.clone(),
            http_client,
            session_info.session_id.clone(),
        ));
        telemetry
            .connection()
            .emit_operation(OperationType::CreateSession, create_elapsed, None);

        // 5. Tag the per-session tracing span. Span-aware subscribers such as
        //    the file/stderr formatter attach `session_id` automatically;
        //    host bridges that forward only event-local fields do not. Pin the
        //    target so span-aware filtering stays consistent with `klog` events.
        let log_span = info_span!(
            target: crate::klog::KERNEL_LOG_TARGET,
            "kernel.session",
            session_id = %session_info.session_id
        );

        Ok(Session {
            inner: Arc::new(SessionInner {
                transport,
                session_id: session_info.session_id,
                config,
                runtime_handle,
                timeout_cleanup: TimeoutCleanupManager::default(),
                log_span,
                telemetry,
                feature_flags,
                closed: std::sync::atomic::AtomicBool::new(false),
                // Autocommit defaults to on (every statement commits); a
                // caller opts into explicit transactions via set_autocommit.
                autocommit: std::sync::atomic::AtomicBool::new(true),
                ignore_transactions: defaults.ignore_transactions,
            }),
        })
    }

    /// Build a mutable statement bound to this session. Mutate its
    /// spec (sql, params, conf) via `Statement::with_spec`, then call
    /// `execute()` or `submit()` to produce an immutable executed
    /// handle.
    pub fn statement(&self) -> Statement {
        Statement::new(Arc::clone(&self.inner))
    }

    /// Metadata operations — flat, per-method (ODBC-shaped).
    pub fn metadata(&self) -> Metadata {
        Metadata::new(Arc::clone(&self.inner))
    }

    // ─── Transaction control ────────────────────────────────────────────
    //
    // Transaction control is expressed as session-scoped SQL, matching the
    // reference and JDBC drivers: set_autocommit → `SET AUTOCOMMIT`,
    // commit → `COMMIT`, rollback → `ROLLBACK`. These run on this session
    // so the server keeps the transaction context across statements.
    // Autocommit state is client-tracked (cached in SessionInner) rather
    // than round-tripped on every read. When the session was built with
    // `ignore_transactions = true` (the default, mirroring
    // `IgnoreTransactions=1`), these are silent no-ops. Centralising this
    // here means every C-ABI consumer (ODBC / Python / Go / Node) gets
    // identical transaction semantics from one implementation.

    /// Current autocommit mode. `true` (default) = each statement commits
    /// immediately; `false` = statements run in an explicit transaction
    /// until [`commit`](Self::commit) / [`rollback`](Self::rollback). This
    /// is a local read (no server round-trip).
    pub fn autocommit(&self) -> bool {
        self.inner
            .autocommit
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Set autocommit mode. Issues `SET AUTOCOMMIT = TRUE|FALSE` on the
    /// server and, on success, caches the new state. Setting `false`
    /// begins explicit-transaction mode; setting `true` returns to
    /// auto-commit (committing any open transaction, per server
    /// semantics).
    ///
    /// No-op returning `Ok(())` when the session has `ignore_transactions`
    /// set (transactions disabled) — the state is not changed and no
    /// statement is sent. This mirrors the reference/JDBC behavior under
    /// `IgnoreTransactions=1`.
    pub async fn set_autocommit(&self, enabled: bool) -> Result<()> {
        if self.inner.ignore_transactions {
            return Ok(());
        }
        self.run_txn_statement(if enabled {
            "SET AUTOCOMMIT = TRUE"
        } else {
            "SET AUTOCOMMIT = FALSE"
        })
        .await?;
        self.inner
            .autocommit
            .store(enabled, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Commit the current transaction by issuing `COMMIT`. No-op returning
    /// `Ok(())` when `ignore_transactions` is set. On a server with no open
    /// transaction (e.g. autocommit still on) the server decides the
    /// outcome; the kernel surfaces any error unchanged.
    pub async fn commit(&self) -> Result<()> {
        if self.inner.ignore_transactions {
            return Ok(());
        }
        self.run_txn_statement("COMMIT").await
    }

    /// Roll back the current transaction by issuing `ROLLBACK`. No-op
    /// returning `Ok(())` when `ignore_transactions` is set. Requires
    /// catalog-managed tables server-side for the rollback to take effect.
    pub async fn rollback(&self) -> Result<()> {
        if self.inner.ignore_transactions {
            return Ok(());
        }
        self.run_txn_statement("ROLLBACK").await
    }

    /// Run a transaction-control statement (`SET AUTOCOMMIT` / `COMMIT` /
    /// `ROLLBACK`) to completion on this session and discard its result.
    /// These statements return no result set, so we execute and drop the
    /// executed handle (its `Drop` closes the server statement). Errors
    /// propagate through the normal `KernelError` taxonomy.
    async fn run_txn_statement(&self, sql: &'static str) -> Result<()> {
        let span = self.inner.log_span().clone();
        async {
            let mut stmt = self.statement();
            stmt.spec().sql(sql);
            stmt.execute().await.map(|_executed| ())
        }
        .instrument(span)
        .await
    }

    /// Whether this session is still open — i.e. its server-side delete
    /// has not been initiated via [`Session::close`] or drop cleanup. This is
    /// a **local** check, not a server round-trip: it reports whether the
    /// handle has been closed/superseded, so a caller doesn't drive an
    /// already-deleted session. It does **not** detect a server-side idle
    /// timeout or a dropped network link. Backs the C ABI's
    /// `kernel_session_is_open`.
    pub(crate) fn is_open(&self) -> bool {
        !self.inner.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Actively validate the session against the server by round-tripping a
    /// lightweight `SELECT 1` test query, discarding its result. Unlike
    /// [`is_open`](Self::is_open) — a local handle-state check — this is a
    /// real network probe: it detects a server-side idle timeout, a killed
    /// warehouse, or a dropped link that the local flag cannot see.
    ///
    /// Returns `Ok(())` when the round-trip completes (the session is
    /// alive). On failure the error carries the usual `KernelError`
    /// category, so a caller can tell a transport/availability failure
    /// (session dead) from, say, an auth error. This mirrors the reference
    /// and JDBC drivers' validate-on-borrow / connection-test probe (a
    /// `SELECT 1`), and backs the C ABI's `kernel_session_test` — the
    /// I/O-performing complement of `kernel_session_is_open` that a host
    /// can use to answer `SQL_ATTR_CONNECTION_DEAD` positively.
    ///
    /// Runs regardless of `ignore_transactions`: it is a read-only probe,
    /// not transaction control. Must be called from a native
    /// (non-async-runtime) thread when driven through the C ABI.
    pub async fn test_connection(&self) -> Result<()> {
        let span = self.inner.log_span().clone();
        async {
            let mut stmt = self.statement();
            stmt.spec().sql("SELECT 1");
            stmt.execute().await.map(|_executed| ())
        }
        .instrument(span)
        .await
    }

    /// Like [`test_connection`](Self::test_connection), but bounds the whole
    /// probe — every retry and HTTP round-trip included — to `deadline`. This
    /// is the per-call deadline a host backs `ConnectionTestTimeout` with
    /// (odbc#347-C): without it the probe runs on the session's own
    /// retry/timeout budget, so `ConnectionTestTimeout=1` and `=30` behave
    /// identically and a `SQL_ATTR_CONNECTION_DEAD` read during an outage can
    /// block for the session's unrelated retry budget rather than the
    /// customer-selected duration.
    ///
    /// A `None` deadline is exactly [`test_connection`] (no per-call bound).
    /// When the probe does not complete within `deadline`, returns a
    /// `Timeout`-category error (distinct wording so a host can tell "probe
    /// deadline hit" from a transport failure) — the caller decides what
    /// `SQL_ATTR_CONNECTION_DEAD` reports for that case. The bound is scoped to
    /// this future only: it wraps the probe in `tokio::time::timeout`, so it
    /// cannot alter the timeout of any other request on the session.
    pub async fn test_connection_with_timeout(&self, deadline: Option<Duration>) -> Result<()> {
        match deadline {
            None => self.test_connection().await,
            Some(d) => match tokio::time::timeout(d, self.test_connection()).await {
                Ok(result) => result,
                // Build via the categorised `Error::timeout()` constructor (repo
                // convention), but mark it non-retryable: unlike a transport
                // timeout, this deadline was deliberately set by the caller
                // (e.g. ConnectionTestTimeout), so re-running the probe would
                // just hit the same bound again. Retrying is the caller's
                // decision, not something a retry wrapper should do implicitly.
                Err(_elapsed) => Err(Error::timeout()
                    .message(format!(
                        "connection test did not complete within {} second(s)",
                        d.as_secs()
                    ))
                    .with_retryable(false)),
            },
        }
    }

    /// Adopt an already-submitted server statement by its id, producing
    /// an [`ExecutedAsyncStatement`] that can poll [`status`] / await
    /// results / cancel — without having been the handle that submitted
    /// it.
    ///
    /// This is the resume path: a caller (or a different process) that
    /// holds only the statement id can re-attach and drive it to
    /// completion, fetch its result, or read its terminal state. Server
    /// statement state is keyed solely by the id — every
    /// `GetStatementStatus` / `GetStatementResult` / cancel / close RPC
    /// takes just the id — so the only thing the originating handle
    /// carried that a bare id does not is *this session's* transport
    /// (auth, retry, poll cadence). Binding the id to this session is
    /// exactly what `attach` does.
    ///
    /// Issues a single `GetStatementStatus` to seed the handle's initial
    /// status (mirrors how `submit` seeds it from the submit response),
    /// so the extended-status accessors return useful values before the
    /// caller drives a poll. **Errors with [`ErrorCode::NotFound`] if
    /// the server doesn't recognise the id** (never submitted, or its
    /// result TTL elapsed and the server garbage-collected it) — the SEA
    /// server returns `404 NOT_FOUND` for an unknown-but-well-formed id,
    /// which the transport maps to `NotFound`. A *closed* statement is
    /// **not** an error: the server keeps reporting `CLOSED` (and a
    /// terminal `SUCCEEDED`/`FAILED` statement keeps reporting that
    /// state, with its result) until GC, so `attach` succeeds and
    /// [`status`] surfaces the real server state.
    ///
    /// The returned handle is standalone and **non-owning**: its
    /// validity flag is fresh (not shared with any parent `Statement`,
    /// since there is none), and it does **not** close the server
    /// statement on `Drop` or via [`close`], does **not** cancel it on a
    /// dropped `await_result()` poll, and its [`cancel`] errors — attach
    /// is a read-only adopt. The statement's lifecycle stays with
    /// whatever
    /// submitted it (or an explicit `close_statement`); a transient
    /// handle a poller creates and drops must not tear it down. (This is
    /// what lets `get_query_state` re-attach on every poll without the
    /// poll loop closing the statement it's watching.)
    ///
    /// [`status`]: ExecutedAsyncStatement::status
    /// [`close`]: ExecutedAsyncStatement::close
    /// [`cancel`]: ExecutedAsyncStatement::cancel
    /// [`ErrorCode::NotFound`]: crate::kernel_error::ErrorCode::NotFound
    pub async fn attach_async_statement(
        &self,
        statement_id: impl Into<String>,
    ) -> Result<ExecutedAsyncStatement> {
        let statement_id = statement_id.into();
        let span = self.inner.log_span().clone();
        async {
            // Seed initial_status from the wire — the same RPC
            // `ExecutedAsyncStatement::status` makes. Surfaces NotFound
            // here (rather than on the caller's first poll) for an
            // unknown/expired id. Stamp the statement id onto the error
            // (mirrors `terminal_outcome`, which sets query_id on every
            // error it builds) so callers can correlate which id was not
            // found.
            let response = self
                .inner
                .transport()
                .statement_status(&statement_id, SessionAttribution::Detached)
                .await
                .map_err(|e| e.with_query_id(&statement_id))?;
            // NON-OWNING read-only adopt: `new_attached` gives the handle
            // a fresh standalone `ValidityFlag`, `closed=true` (so `Drop`
            // / `close()` never fire `close_statement`), and
            // `owning=false` (so `await_result` doesn't arm the
            // drop-cancel guard and `cancel()` errors). The statement's
            // lifecycle stays with whoever submitted it; a poller that
            // re-attaches each tick can't tear down the statement it's
            // watching.
            Ok(ExecutedAsyncStatement::new_attached(
                Arc::clone(&self.inner),
                statement_id,
                response.status,
            ))
        }
        .instrument(span)
        .await
    }

    /// Server-issued session identifier. Stable for the life of the
    /// session.
    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    // ─── Transactions (omitted in v0) ─────────────────────────────────────
    //
    // The autocommit / commit / rollback surface is intentionally
    // not shipped in v0. Add it once the server contract is settled:
    //
    //   pub async fn set_autocommit(&self, enabled: bool) -> Result<()>;
    //   pub async fn autocommit(&self) -> Result<bool>;
    //   pub async fn commit(&self) -> Result<()>;
    //   pub async fn rollback(&self) -> Result<()>;

    /// Explicit close. Calls `DeleteSession` against the server and
    /// drops the local handle. `Drop` also fires a fire-and-forget
    /// cleanup if `close()` wasn't called explicitly, but `close()`
    /// is the only way to *await* the result and surface a failure.
    ///
    /// The `closed` flag flips **after** a successful `delete_session`
    /// — only then does [`Drop`] skip its own cleanup. So:
    ///
    /// - On `Err`: flag stays false, [`Drop`]'s fire-and-forget
    ///   retry path runs as a fallback so transient failures don't
    ///   leak the server-side session.
    /// - On future cancellation (e.g. `tokio::select!` losing a
    ///   race): flag stays false, [`Drop`] picks up the slack.
    /// - On `Ok`: flag flips, [`Drop`] is a no-op.
    pub async fn close(self) -> Result<()> {
        // Bail early if a concurrent caller already finished a
        // successful close — no point hitting the wire twice.
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let span = self.inner.log_span.clone();
        async {
            let telemetry = self.inner.telemetry.connection();
            let delete_started = Instant::now();
            if let Err(error) = self
                .inner
                .transport
                .delete_session(&self.inner.session_id)
                .await
            {
                telemetry.emit_operation(
                    OperationType::DeleteSession,
                    delete_started.elapsed(),
                    Some(&error),
                );
                telemetry.finish().await;
                return Err(error);
            }
            telemetry.emit_operation(OperationType::DeleteSession, delete_started.elapsed(), None);
            telemetry.finish().await;
            self.inner
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        }
        .instrument(span)
        .await
    }
}

/// Fluent builder for `Session`.
///
/// Each setter takes a typed config group, not individual fields —
/// keeps the builder surface compact and matches how wrappers
/// translate connection-string / DSN attributes into config (one DSN
/// attribute group per setter call).
#[non_exhaustive]
#[derive(Default)]
pub struct SessionBuilder {
    connection: Option<ConnectionConfig>,
    auth: Option<AuthConfig>,
    defaults: Option<DefaultOpts>,
    http: Option<HttpConfig>,
    proxy: Option<ProxyConfig>,
    tls: Option<TlsConfig>,
    result: Option<ResultConfig>,
    logging: Option<LoggingConfig>,
    telemetry: Option<TelemetryConfig>,
    client_identity: Option<ClientIdentity>,
    driver_system_configuration: Option<DriverSystemConfiguration>,
    session_conf: HashMap<String, String>,
    geospatial_result_representation: Option<GeospatialResultRepresentation>,
    transport: Option<TransportMode>,
    /// Sentinel for [`SessionBuilder::http_path`] parse failures.
    /// Defers the error to `open()` so the builder API stays
    /// infallible (matching the rest of its setters).
    parse_error: Option<String>,
}

impl SessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Required: workspace host + warehouse identity.
    ///
    /// Clears any prior [`SessionBuilder::http_path`] parse error —
    /// the user has explicitly overridden the connection identity, so
    /// an old parse failure on a stale path should not block
    /// [`SessionBuilder::open`].
    pub fn connection(mut self, conn: ConnectionConfig) -> Self {
        self.connection = Some(conn);
        self.parse_error = None;
        self
    }

    /// Convenience shortcut equivalent to
    /// `connection(ConnectionConfig::from_http_path(host, http_path)?)`.
    /// Most wrappers translate one DSN field (`http_path` / `httpPath`
    /// / `HTTPPath`) — this lets them do it in a single builder call.
    ///
    /// Defers parsing errors to [`SessionBuilder::open`] so the
    /// builder API stays infallible (matches the rest of the
    /// builder's setters). An invalid path surfaces as
    /// `ErrorCode::InvalidArgument` from `open()`.
    pub fn http_path(mut self, host: impl Into<String>, http_path: impl AsRef<str>) -> Self {
        match ConnectionConfig::from_http_path(host, http_path.as_ref()) {
            Ok(c) => {
                self.connection = Some(c);
                self.parse_error = None;
            }
            Err(_) => {
                self.connection = None;
                self.parse_error = Some(http_path.as_ref().to_owned());
            }
        }
        self
    }

    /// Required: how to authenticate.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Default catalog / schema applied to statements that don't
    /// override.
    pub fn defaults(mut self, defaults: DefaultOpts) -> Self {
        self.defaults = Some(defaults);
        self
    }

    /// Whether transaction control is ignored (no-oped). `true` (the
    /// default) matches the reference/JDBC `IgnoreTransactions=1`; `false`
    /// enables real multi-statement transactions (Private Preview).
    ///
    /// A convenience over [`Self::defaults`] for the common case of
    /// toggling only this flag — and the supported way to set it from
    /// outside the crate, since [`DefaultOpts`] is `#[non_exhaustive]`.
    /// Merges into any `DefaultOpts` already supplied via `defaults`.
    pub fn ignore_transactions(mut self, ignore: bool) -> Self {
        let mut d = self.defaults.take().unwrap_or_default();
        d.ignore_transactions = ignore;
        self.defaults = Some(d);
        self
    }

    /// HTTP client tuning — request / overall timeouts, connection-pool
    /// size. Defaults are sane for typical workloads.
    pub fn http(mut self, http: HttpConfig) -> Self {
        self.http = Some(http);
        self
    }

    /// HTTP proxy. Reserved in v0 (`#[non_exhaustive]` empty struct);
    /// fields land in v0.1.
    pub fn proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// TLS settings — custom CA bundle, self-signed acceptance,
    /// hostname-verification skip. See [`TlsConfig`] for the field
    /// list. Default is the most secure stance: validate against the
    /// system trust store, require a valid certificate chain, and
    /// verify hostnames.
    ///
    /// ```no_run
    /// # use databricks_sql_kernel::{TlsConfig, Session};
    /// # fn build() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut tls = TlsConfig::default();
    /// tls.custom_ca_cert = Some(std::fs::read("/etc/ssl/corp-ca.pem")?);
    /// let _builder = Session::builder().tls(tls);
    /// # Ok(()) }
    /// ```
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Result-management config (CloudFetch tuning today; future
    /// result knobs slot in here too).
    pub fn result(mut self, result: ResultConfig) -> Self {
        self.result = Some(result);
        self
    }

    /// Logging — filter expression, output destination.
    pub fn logging(mut self, logging: LoggingConfig) -> Self {
        self.logging = Some(logging);
        self
    }

    /// Telemetry exporter. Reserved in v0 (`#[non_exhaustive]` empty
    /// struct); fields land in v0.1.
    pub fn telemetry(mut self, telemetry: TelemetryConfig) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Driver family and version used for connector-service feature flags.
    ///
    /// Bindings should pass the identity of the outer driver, not the kernel's
    /// own crate version. Omitting identity keeps feature-flag resolution off.
    pub fn client_identity(mut self, identity: ClientIdentity) -> Self {
        self.client_identity = Some(identity);
        self
    }

    /// Driver/runtime identity stamped onto telemetry logs.
    ///
    /// Binding layers should use this to pass their own driver name,
    /// version, runtime, and process details. Any fields left unset are
    /// filled from kernel defaults when the session opens.
    pub fn driver_system_configuration(
        mut self,
        system_configuration: DriverSystemConfiguration,
    ) -> Self {
        self.driver_system_configuration = Some(system_configuration);
        self
    }

    /// Server-bound session conf passed through to SEA `session_confs`.
    pub fn session_conf(mut self, conf: HashMap<String, String>) -> Self {
        self.session_conf = conf;
        self
    }

    /// Request the physical representation used for `GEOMETRY` and
    /// `GEOGRAPHY` values inside Arrow result batches.
    ///
    /// Both choices keep the outer result format as Arrow. String mode emits
    /// `Utf8` EWKT values; binary mode emits
    /// `struct<srid: int32, wkb: binary>` values. The kernel normalizes the
    /// server result locally; this setting is not forwarded to SEA.
    pub fn geospatial_result_representation(
        mut self,
        representation: GeospatialResultRepresentation,
    ) -> Self {
        self.geospatial_result_representation = Some(representation);
        self
    }

    /// Transport override. Default = `SeaRest`. Wrappers translate
    /// user-facing flags (`UseThriftClient=0/1`, `transport=...`)
    /// into this field.
    pub fn transport(mut self, mode: TransportMode) -> Self {
        self.transport = Some(mode);
        self
    }

    /// Validate, assemble a [`SessionConfig`], and open the session.
    /// Returns `Err(InvalidArgument)` if required fields
    /// (`connection`, `auth`) are missing or if a previous
    /// [`SessionBuilder::http_path`] call failed to parse.
    pub async fn open(self) -> Result<Session> {
        if let Some(bad_path) = self.parse_error {
            return Err(Error::invalid_argument().message(format!(
                "http_path '{bad_path}' does not match \
                 /sql/1.0/warehouses/{{id}} or /sql/1.0/endpoints/{{id}}"
            )));
        }

        let connection = self.connection.ok_or_else(|| {
            Error::invalid_argument()
                .message("SessionBuilder requires `connection(...)` or `http_path(...)`")
        })?;
        let auth = self.auth.ok_or_else(|| {
            Error::invalid_argument().message("SessionBuilder requires `auth(...)`")
        })?;

        let config = SessionConfig {
            connection,
            defaults: self.defaults,
            auth,
            driver_system_configuration: self.driver_system_configuration,
            client_identity: self.client_identity,
            session_conf: self.session_conf,
            geospatial_result_representation: self.geospatial_result_representation,
            http: self.http.unwrap_or_default(),
            proxy: self.proxy,
            tls: self.tls.unwrap_or_default(),
            result: self.result.unwrap_or_default(),
            logging: self.logging.unwrap_or_default(),
            telemetry: self.telemetry.unwrap_or_default(),
            transport: self.transport,
        };
        Session::open(config).await
    }
}

// ─── Internal config translation ─────────────────────────────────────

/// Reject a fully configured mTLS identity on anything except HTTPS.
///
/// Incomplete identities are intentionally left to the HTTP-client
/// validator, which reports which half is missing. Ordinary explicit HTTP
/// endpoints remain supported for non-mTLS test servers.
fn validate_mtls_endpoint_scheme(config: &SessionConfig) -> Result<()> {
    if config.tls.client_cert_pem.is_none() || config.tls.client_key_pem.is_none() {
        return Ok(());
    }

    let scheme = config
        .connection
        .host
        .split_once("://")
        .map(|(scheme, _)| scheme);
    if scheme.is_some_and(|scheme| scheme.eq_ignore_ascii_case("https")) {
        return Ok(());
    }

    let scheme = scheme
        .filter(|scheme| !scheme.is_empty())
        .unwrap_or("<missing>");
    Err(Error::invalid_argument().message(format!(
        "mTLS client identity requires an HTTPS endpoint; got scheme '{scheme}'"
    )))
}

/// Translate the public [`HttpConfig`] (+ proxy / TLS / org_id) into
/// the internal [`HttpClientConfig`] the HTTP client expects.
///
/// The public [`ProxyConfig`] is projected onto the internal
/// [`HttpProxyConfig`] (`url` set ⇒ override env vars and route all
/// traffic through it; `url` unset ⇒ reqwest's env-var behaviour). The
/// public [`TlsConfig`] carries `custom_ca_cert` / `accept_self_signed`
/// / `skip_hostname_verification` and is projected onto the internal
/// [`HttpTlsConfig`] via [`HttpTlsConfig::from_public`].
fn build_http_client_config(config: &SessionConfig, org_id: Option<String>) -> HttpClientConfig {
    HttpClientConfig {
        connect_timeout: Duration::from_secs(30),
        // The historical internal `read_timeout` field is passed to reqwest's
        // total request `.timeout()` and corresponds to public `request_timeout`.
        read_timeout: config.http.request_timeout,
        max_connections_per_host: config.http.pool_max_idle_per_host,
        proxy: build_proxy_config(config.proxy.as_ref()),
        tls: HttpTlsConfig::from_public(&config.tls),
        org_id,
        custom_headers: config.http.custom_headers.clone(),
        ..HttpClientConfig::default()
    }
}

/// Project the public [`ProxyConfig`] onto the internal
/// [`HttpProxyConfig`]. `None` (no proxy config supplied) yields the
/// default internal config (`url == None`), under which the HTTP client
/// keeps reqwest's standard env-var behaviour. A destructuring binding
/// (NOT `..`) is used so a future public-field addition surfaces as a
/// compile error here rather than being silently dropped.
fn build_proxy_config(proxy: Option<&ProxyConfig>) -> HttpProxyConfig {
    let Some(proxy) = proxy else {
        return HttpProxyConfig::default();
    };
    let ProxyConfig {
        url,
        username,
        password,
        bypass_hosts,
    } = proxy.clone();
    HttpProxyConfig {
        url,
        username,
        password,
        bypass_hosts,
    }
}

/// End-to-end wall-clock budget for downloading a single CloudFetch
/// chunk. This is the single source of truth shared by two layers so they
/// can't drift:
///
/// - the CloudFetch HTTP category's `overall_timeout` in
///   [`build_retry_configs_for_session`] (bounds the inner HTTP client), and
/// - the provider's [`CloudFetchConfig::download_timeout`] in
///   [`result_to_cloudfetch_config`] (bounds the provider retry loop and
///   caps each HTTP attempt at the remaining budget).
///
/// Before both layers shared this budget, the per-attempt HTTP timeout
/// (`request_timeout`, default 120s) times the provider's `max_retries`
/// let a permanently-stuck chunk block the consumer for ~10 minutes on a
/// nominal budget (audit H05).
const CLOUDFETCH_DOWNLOAD_BUDGET: Duration = Duration::from_secs(20);

/// Build the per-category retry config for the HTTP client.
///
/// The global config is driven by the public [`HttpConfig`] retry
/// knobs (`retry_min_wait` / `retry_max_wait` / `retry_max_retries` /
/// `overall_timeout`), so callers can tune backoff and attempt counts.
///
/// The `Auth` category keeps a **fixed** fast-fail override (30s /
/// 3 retries) regardless of the user's global settings: token-endpoint
/// calls must not inherit a generous `overall_timeout` (e.g. 900s) or
/// they'd hang for many minutes on an unreachable IdP. This matches the
/// legacy ADBC behaviour and is intentionally not user-overridable.
///
/// The `CloudFetch` category is capped to **no HTTP-level retries** for
/// the same reason: chunk downloads already sit inside the CloudFetch
/// provider's own retry loop (`CloudFetchConfig::max_retries`, default 5).
/// If the inner HTTP client also inherited the global budget (default 5
/// retries / 900s `overall_timeout`) the two budgets would **multiply** —
/// a permanently-failing chunk gets attempted ~30 times with escalating
/// backoff and blocks the consumer well past three minutes instead of
/// surfacing a bounded terminal error (databricks-odbc#144 / #226: the
/// `SQLFetch` drain hung 186s, over the 180s conformance bound). Making
/// the provider loop the single retry authority keeps a persistent
/// failure terminal in bounded time; one-shot transient recovery
/// (CLOUDFETCH-010/011) still works via the provider loop. Fixed, like
/// `Auth` — and not user-overridable.
fn build_retry_configs_for_session(
    config: &SessionConfig,
) -> std::collections::HashMap<RequestCategory, RetryConfig> {
    // A tuning typo where `retry_max_wait < retry_min_wait` is CORRECTED, not
    // fatal: raise max to min and warn once here (the single per-session build
    // point), rather than letting it reach `calculate_backoff` — where an
    // uncorrected pair would otherwise panic `Duration::clamp`. Mirrors the
    // clamp-and-warn discipline `apply_client_result_overrides` uses for
    // `cloudfetch_max_chunks_in_memory`.
    let min_wait = config.http.retry_min_wait;
    let mut max_wait = config.http.retry_max_wait;
    if max_wait < min_wait {
        crate::klog::warn!(
            retry_min_wait = ?min_wait,
            retry_max_wait = ?max_wait,
            "retry_max_wait is below retry_min_wait; raising retry_max_wait to \
             retry_min_wait (a backoff bound is a tuning hint, not a hard failure)",
        );
        max_wait = min_wait;
    }

    let global = RetryConfig {
        min_wait,
        max_wait,
        max_retries: config.http.retry_max_retries,
        overall_timeout: config.http.overall_timeout,
        ..RetryConfig::default()
    };
    let mut overrides: HashMap<RequestCategory, RetryConfigOverrides> = HashMap::new();
    overrides.insert(
        RequestCategory::Auth,
        RetryConfigOverrides {
            overall_timeout: Some(Duration::from_secs(30)),
            max_retries: Some(3),
            ..Default::default()
        },
    );
    // Let the CloudFetch provider loop be the single retry authority for a
    // chunk download (see the doc comment above): no HTTP-level retries, and
    // a tight wall-clock bound so a stuck download can't inherit the global
    // 900s `overall_timeout`.
    overrides.insert(
        RequestCategory::CloudFetch,
        RetryConfigOverrides {
            overall_timeout: Some(CLOUDFETCH_DOWNLOAD_BUDGET),
            max_retries: Some(0),
            ..Default::default()
        },
    );
    overrides.insert(
        RequestCategory::Telemetry,
        RetryConfigOverrides {
            min_wait: Some(config.telemetry.retry_delay),
            max_wait: Some(config.telemetry.retry_delay),
            overall_timeout: Some(config.telemetry.close_flush_timeout),
            max_retries: Some(config.telemetry.max_retries),
            ..Default::default()
        },
    );
    build_retry_configs(&global, &overrides)
}

/// Fold any **client-side** result knobs that a host left in `session_conf`
/// into the typed [`ResultConfig`], then strip them so they can't leak onto
/// the SEA wire.
///
/// This is the legacy / convenience channel: the preferred entry point is
/// the C ABI's `kernel_session_config_set_client_conf` (which sets
/// `ResultConfig` directly), but a host that only has a `session_conf`
/// key/value bag — e.g. one forwarding DSN attributes without sorting them
/// into client vs. server — can still drop a client knob like
/// `cloudfetch_max_chunks_in_memory` in there and have it applied. Any key
/// recognised by [`ResultConfig::apply_client_conf`] (see
/// [`ClientConfKey`](crate::config::ClientConfKey)) is consumed here;
/// unrecognised keys are left in `session_conf` as server-bound confs.
///
/// **Best-effort, never fatal.** A recognised key with an unparseable value
/// is logged at `warn` and skipped (the field keeps its current value)
/// rather than failing session creation — a stray tuning value in the bag
/// must not turn into a connect error. (The typed `set_client_conf` path
/// *does* reject bad values, because there the caller asked for that key
/// explicitly; here the key is ambient in a mixed bag.) Every recognised
/// key is removed regardless of parse outcome, so no client-only key
/// survives onto the wire.
///
/// Matched case-insensitively. If a host sets two spellings of the same key
/// differing only in case, the lexicographically-last spelling wins
/// deterministically and all variants are stripped.
fn apply_client_result_overrides(config: &mut SessionConfig) {
    use crate::config::ClientConfKey;

    // Recognised client keys currently present in session_conf. A key is
    // "recognised" iff it parses to a ClientConfKey (case-insensitive).
    let mut present: Vec<String> = config
        .session_conf
        .keys()
        .filter(|k| ClientConfKey::parse(k).is_some())
        .cloned()
        .collect();
    if present.is_empty() {
        return;
    }
    // Deterministic winner among case-variants: sort so the lexicographically
    // -last spelling of each key is applied last (and thus wins).
    present.sort_unstable();

    for spelling in present {
        let Some(value) = config.session_conf.remove(&spelling) else {
            continue;
        };
        // Best-effort: a bad value in the ambient bag is warned-and-skipped,
        // never fatal. The key is already removed above so it can't reach the
        // wire regardless.
        if let Err(e) = config.result.apply_client_conf(&spelling, &value) {
            crate::klog::warn!(
                key = %spelling,
                value = %value,
                error = %e.message,
                "ignoring invalid client-side result knob in session_conf; keeping current value",
            );
        }
    }
}

/// Project the public [`ResultConfig`] (today only carries
/// CloudFetch knobs; future result-management options slot in here
/// too) onto the internal `CloudFetchConfig`.
fn result_to_cloudfetch_config(
    result: &ResultConfig,
) -> crate::types::cloudfetch::CloudFetchConfig {
    crate::types::cloudfetch::CloudFetchConfig {
        enabled: result.cloudfetch_enabled,
        link_prefetch_window: result.cloudfetch_link_prefetch_window,
        max_chunks_in_memory: result.cloudfetch_max_chunks_in_memory,
        inline_max_chunks_in_memory: result.inline_max_chunks_in_memory,
        // Share one budget with the CloudFetch HTTP category
        // (see CLOUDFETCH_DOWNLOAD_BUDGET) so the provider retry loop and
        // the inner HTTP client can't drift (audit H05).
        download_timeout: CLOUDFETCH_DOWNLOAD_BUDGET,
        ..Default::default()
    }
}

/// Project the public [`ResultConfig`] onto the internal
/// [`PostProcessOptions`](crate::reader::post_processor::PostProcessOptions)
/// consumed by `ResultReaderFactory`.
fn result_to_post_process_options(
    result: &ResultConfig,
) -> crate::reader::post_processor::PostProcessOptions {
    crate::reader::post_processor::PostProcessOptions {
        complex_types_as_json: result.complex_types_as_json,
        intervals_as_string: result.intervals_as_string,
        ..Default::default()
    }
}

/// Kernel default U2M OAuth client id. The default `{client_id, scopes,
/// redirect_ports}` form one coupled bundle (each OAuth app is registered with
/// its own redirect URIs): the `databricks-sql-connector` app, shared by the
/// Node and Go drivers. See PECOBLR-4039.
const DEFAULT_U2M_CLIENT_ID: &str = "databricks-sql-connector";
/// Default candidate redirect ports for [`DEFAULT_U2M_CLIENT_ID`], tried in
/// order — the kernel binds the first free one, so a bare-default consumer
/// (Node, Go, ODBC, bare Rust) gets busy-port fallback when 8030 is occupied
/// (a stale prior listener, or a second concurrent U2M connection), instead of
/// failing outright.
///
/// Why a contiguous 8030–8034 window is safe even though only 8030 is the
/// "canonical" registered port: [`DEFAULT_U2M_CLIENT_ID`] authenticates via
/// Databricks in-house OAuth, which normalizes loopback redirect URIs by
/// **dropping the port before comparison** (RFC 8252 §7.3 — "an exact match is
/// required except for the port component"). So every `http://localhost:{port}`
/// is accepted for this client regardless of the specific port, and no
/// per-port app registration is needed. (Validated live: :8030, plus arbitrary
/// ports, all complete the flow.) The first port is the canonical 8030 so the
/// common case is unchanged.
const DEFAULT_U2M_REDIRECT_PORTS: [u16; 5] = [8030, 8031, 8032, 8033, 8034];

/// Scopes registered for [`DEFAULT_U2M_CLIENT_ID`] (`sql offline_access`).
fn default_u2m_scopes() -> Vec<String> {
    vec!["sql".to_string(), "offline_access".to_string()]
}

/// Fill the kernel default U2M bundle for any field the caller left unset.
///
/// Extracted from [`build_auth_provider`] so the defaulting is unit-testable
/// without OIDC discovery. Because the bundle is coupled, a caller overriding
/// `client_id` for a different OAuth app must also supply the matching
/// `redirect_ports` (and usually `scopes`).
fn resolve_u2m_bundle(
    client_id: Option<&str>,
    redirect_ports: Option<&[u16]>,
    scopes: Option<&[String]>,
) -> (String, Vec<u16>, Vec<String>) {
    // A non-empty `redirect_ports` list (the caller's registered redirect URIs,
    // one or many) is used verbatim; otherwise the kernel default. Every entry
    // must be a registered redirect URI of the chosen OAuth app — the
    // {client_id, ports, scopes} bundle stays coupled. Single-port bindings
    // forward a one-element list.
    let ports = match redirect_ports {
        Some(ports) if !ports.is_empty() => ports.to_vec(),
        _ => DEFAULT_U2M_REDIRECT_PORTS.to_vec(),
    };
    (
        client_id.unwrap_or(DEFAULT_U2M_CLIENT_ID).to_string(),
        ports,
        scopes
            .map(<[String]>::to_vec)
            .unwrap_or_else(default_u2m_scopes),
    )
}

/// Build the auth provider for a given [`AuthConfig`]. `host` is the
/// workspace URL — used for OIDC discovery on M2M / U2M flows.
async fn build_auth_provider(
    auth: &AuthConfig,
    host: &str,
    http_client: Arc<DatabricksHttpClient>,
) -> Result<Arc<dyn AuthProvider>> {
    // Build the base provider for the configured auth flow, then wrap it with
    // mandatory OIDC token exchange (BYOT federation). The wrapper is a no-op
    // for non-JWT and same-host-issuer tokens, so it is safe to apply
    // unconditionally across PAT / M2M / U2M. See `auth::oauth::federation`.
    let base = build_base_auth_provider(auth, host, http_client.clone()).await?;
    let provider = FederationProvider::new(
        base,
        host,
        auth.identity_federation_client_id().map(str::to_owned),
        &http_client,
    )
    .await?;
    Ok(Arc::new(provider))
}

/// Constructs the underlying credential provider for the configured auth flow,
/// before token-federation wrapping.
async fn build_base_auth_provider(
    auth: &AuthConfig,
    host: &str,
    http_client: Arc<DatabricksHttpClient>,
) -> Result<Arc<dyn AuthProvider>> {
    match auth {
        AuthConfig::Pat { token, .. } => {
            if token.is_empty() {
                return Err(
                    Error::invalid_argument().message("AuthConfig::Pat token must not be empty")
                );
            }
            Ok(Arc::new(PersonalAccessToken::new(token.clone())))
        }
        AuthConfig::OAuthM2M {
            client_id,
            client_secret,
            scopes,
            token_url_override,
            ..
        } => {
            if client_id.is_empty() || client_secret.is_empty() {
                return Err(Error::invalid_argument().message(
                    "AuthConfig::OAuthM2M requires non-empty client_id and client_secret",
                ));
            }
            let scopes = scopes
                .clone()
                .unwrap_or_else(|| vec!["all-apis".to_string()]);
            let provider = ClientCredentialsProvider::new_with_full_config(
                host,
                client_id,
                client_secret,
                http_client,
                scopes,
                token_url_override.clone(),
            )
            .await?;
            Ok(Arc::new(provider))
        }
        AuthConfig::OAuthU2M {
            client_id,
            redirect_ports,
            scopes,
            token_url_override,
            callback_timeout,
            token_cache,
            ..
        } => {
            // Fill the coupled kernel default bundle for any unset field
            // (databricks-sql-connector / sql offline_access / 8030). Consumers
            // using a different app override the whole bundle. See PECOBLR-4039.
            // `callback_ports` is the caller's registered redirect range (or a
            // single-element default); the flow binds the first free one.
            let (client_id, callback_ports, scopes) = resolve_u2m_bundle(
                client_id.as_deref(),
                redirect_ports.as_deref(),
                scopes.as_deref(),
            );
            let timeout = callback_timeout.unwrap_or_else(|| Duration::from_secs(120));
            // `Some(passphrase)` enables the on-disk cache, `None` disables
            // it. When the cache is on but the caller supplied no passphrase,
            // `resolve` substitutes a machine-local derived one rather than
            // failing or writing plaintext — see `TokenCacheConfig`.
            let cache_policy = token_cache
                .clone()
                .unwrap_or_default()
                .resolve()
                .map(|p| p.into_owned());
            let provider = AuthorizationCodeProvider::new_with_full_config(
                host,
                &client_id,
                http_client,
                scopes,
                callback_ports,
                timeout,
                token_url_override.clone(),
                cache_policy,
            )
            .await?;
            Ok(Arc::new(provider))
        }
        AuthConfig::AzureServicePrincipal {
            azure_client_id,
            azure_client_secret,
            azure_tenant_id,
            azure_workspace_resource_id,
            ..
        } => {
            // Kernel owns Azure resolution: build the Entra token endpoint
            // ({login}/{tenant}/oauth2/v2.0/token) and the {app_id}/.default
            // scope, discovering the tenant from the workspace's /aad/auth
            // redirect when the caller omits it (Thrift parity). The provider
            // emits the Azure SP management token + resource-id header pair only
            // when a workspace resource id is supplied (JDBC parity), so an
            // RBAC-only SP (Azure role, not a workspace member) can authenticate
            // while the common workspace-member SP is unaffected.
            use crate::auth::oauth::azure::{self, AzureServicePrincipalProvider};
            if azure_client_id.is_empty() || azure_client_secret.is_empty() {
                return Err(Error::invalid_argument().message(
                    "AuthConfig::AzureServicePrincipal requires non-empty \
                     azure_client_id and azure_client_secret",
                ));
            }
            let tenant = match azure_tenant_id {
                Some(t) if !t.is_empty() => t.clone(),
                _ => azure::discover_tenant(host, &http_client).await?,
            };
            let provider = AzureServicePrincipalProvider::new(
                host,
                azure_client_id,
                azure_client_secret,
                &tenant,
                azure_workspace_resource_id.clone(),
                http_client,
            )
            .await?;
            Ok(Arc::new(provider))
        }
        AuthConfig::OAuthM2MPrivateKey {
            client_id,
            jwt_key_file,
            jwt_kid,
            jwt_passphrase,
            jwt_algorithm,
            scopes,
            token_url_override,
        } => {
            if client_id.is_empty() || jwt_key_file.is_empty() || jwt_kid.is_empty() {
                return Err(Error::invalid_argument().message(
                    "AuthConfig::OAuthM2MPrivateKey requires non-empty client_id, \
                     jwt_key_file, and jwt_kid",
                ));
            }
            let scopes = scopes
                .clone()
                .unwrap_or_else(|| vec!["all-apis".to_string()]);
            let provider = JwtClientCredentialsProvider::new(
                host,
                client_id,
                jwt_key_file,
                jwt_kid,
                jwt_passphrase.as_deref(),
                jwt_algorithm.as_deref(),
                http_client,
                scopes,
                token_url_override.clone(),
            )
            .await?;
            Ok(Arc::new(provider))
        }
        AuthConfig::OAuthFederation { .. } => Err(Error::invalid_argument()
            .message("AuthConfig::OAuthFederation is reserved for v0.1; not yet wired")),
        AuthConfig::External(_) => Err(Error::invalid_argument()
            .message("AuthConfig::External is reserved; v0 wires PAT + OAuthM2M + OAuthU2M only")),
    }
}

#[cfg(test)]
mod mtls_endpoint_tests {
    use super::*;

    fn config(host: &str, cert: bool, key: bool) -> SessionConfig {
        let tls = TlsConfig {
            client_cert_pem: cert.then(|| b"test-cert".to_vec()),
            client_key_pem: key.then(|| b"test-key".to_vec()),
            ..TlsConfig::default()
        };

        SessionConfig {
            connection: ConnectionConfig::new(host, "test_wh"),
            defaults: None,
            auth: AuthConfig::Pat {
                token: "dapi-test".to_string(),
                identity_federation_client_id: None,
            },
            driver_system_configuration: None,
            client_identity: None,
            session_conf: Default::default(),
            geospatial_result_representation: None,
            http: HttpConfig::default(),
            proxy: None,
            tls,
            result: ResultConfig::default(),
            logging: LoggingConfig::default(),
            telemetry: TelemetryConfig::default(),
            transport: None,
        }
    }

    #[test]
    fn complete_mtls_identity_accepts_only_https() {
        for host in [
            "https://workspace.databricks.com",
            "HTTPS://workspace.databricks.com",
            "workspace.databricks.com",
        ] {
            validate_mtls_endpoint_scheme(&config(host, true, true))
                .unwrap_or_else(|error| panic!("{host} should be accepted: {error}"));
        }

        for host in [
            "http://127.0.0.1:8080",
            "HTTP://127.0.0.1:8080",
            "ftp://workspace.databricks.com",
        ] {
            let error = validate_mtls_endpoint_scheme(&config(host, true, true))
                .expect_err("non-HTTPS mTLS endpoint must be rejected");
            assert_eq!(error.code, crate::kernel_error::ErrorCode::InvalidArgument);
            assert!(error.message.contains("HTTPS"), "{}", error.message);
        }
    }

    #[test]
    fn explicit_http_remains_available_without_a_complete_mtls_identity() {
        for (cert, key) in [(false, false), (true, false), (false, true)] {
            validate_mtls_endpoint_scheme(&config("http://127.0.0.1:8080", cert, key))
                .expect("scheme validation must not replace ordinary HTTP or missing-half checks");
        }
    }

    #[tokio::test]
    async fn session_open_rejects_http_mtls_before_client_construction() {
        for host in ["http://127.0.0.1:1", "HTTP://127.0.0.1:1"] {
            let error = Session::open(config(host, true, true))
                .await
                .expect_err("HTTP mTLS session must fail before dialing");
            assert_eq!(error.code, crate::kernel_error::ErrorCode::InvalidArgument);
            assert!(error.message.contains("HTTPS"), "{}", error.message);
            assert!(
                !error.message.contains("Failed to construct HTTP client"),
                "scheme validation must run before HTTP client construction: {}",
                error.message
            );
        }
    }
}

#[cfg(test)]
mod u2m_default_tests {
    use super::*;

    #[test]
    fn u2m_default_bundle_is_connector_sql_8030() {
        // Caller omits everything → kernel default databricks-sql-connector
        // app bundle (PECOBLR-4039), with the full default candidate-port
        // window so bare-default consumers get busy-port fallback too.
        let (client_id, ports, scopes) = resolve_u2m_bundle(None, None, None);
        assert_eq!(client_id, "databricks-sql-connector");
        assert_eq!(ports, vec![8030, 8031, 8032, 8033, 8034]);
        assert_eq!(
            scopes,
            vec!["sql".to_string(), "offline_access".to_string()]
        );
    }

    #[test]
    fn u2m_bundle_honours_caller_overrides() {
        // A consumer overriding for a different OAuth app supplies the whole
        // coupled bundle; each field is threaded through verbatim. A single
        // fixed port arrives as a one-element list.
        let custom = vec!["all-apis".to_string()];
        let (client_id, ports, scopes) =
            resolve_u2m_bundle(Some("databricks-sql-python"), Some(&[8020]), Some(&custom));
        assert_eq!(client_id, "databricks-sql-python");
        assert_eq!(ports, vec![8020]);
        assert_eq!(scopes, custom);
    }

    #[test]
    fn u2m_bundle_uses_the_full_redirect_ports_list() {
        // A multi-port list (the connector's registered range) is used verbatim
        // for busy-port fallback.
        let (_, ports, _) = resolve_u2m_bundle(None, Some(&[9001, 9002, 9003]), None);
        assert_eq!(ports, vec![9001, 9002, 9003]);
    }

    #[test]
    fn u2m_bundle_empty_or_none_ports_use_default() {
        // For the default client, both an empty list and None fall back to the
        // kernel default port window (first is the canonical 8030; the rest are
        // fallback candidates).
        let default = vec![8030, 8031, 8032, 8033, 8034];
        let (_, ports_empty, _) = resolve_u2m_bundle(None, Some(&[]), None);
        assert_eq!(ports_empty, default);
        let (_, ports_none, _) = resolve_u2m_bundle(None, None, None);
        assert_eq!(ports_none, default);
    }
}

#[cfg(test)]
mod retry_config_tests {
    use super::*;
    use crate::client::retry::RequestCategory;
    use std::time::Duration;

    /// Build a minimal `SessionConfig` with the given `HttpConfig` for
    /// exercising `build_retry_configs_for_session` in isolation.
    fn config_with_http(http: HttpConfig) -> SessionConfig {
        SessionConfig {
            connection: ConnectionConfig::new("https://example.databricks.com", "wh123"),
            defaults: None,
            auth: AuthConfig::Pat {
                token: "dapi-test".to_string(),
                identity_federation_client_id: None,
            },
            driver_system_configuration: None,
            client_identity: None,
            session_conf: Default::default(),
            geospatial_result_representation: None,
            http,
            proxy: None,
            tls: Default::default(),
            result: Default::default(),
            logging: Default::default(),
            telemetry: Default::default(),
            transport: None,
        }
    }

    fn config_with_telemetry(telemetry: TelemetryConfig) -> SessionConfig {
        SessionConfig {
            telemetry,
            ..config_with_http(HttpConfig::default())
        }
    }

    #[test]
    fn http_retry_knobs_flow_into_global_config() {
        let http = HttpConfig {
            retry_min_wait: Duration::from_secs(3),
            retry_max_wait: Duration::from_secs(90),
            retry_max_retries: 7,
            overall_timeout: Duration::from_secs(600),
            ..HttpConfig::default()
        };
        let configs = build_retry_configs_for_session(&config_with_http(http));

        // SEA inherits the user-tuned global config. (CloudFetch does NOT —
        // it keeps a fixed no-HTTP-retry override; see
        // `cloudfetch_override_stays_fixed_regardless_of_user_knobs`.)
        let c = &configs[&RequestCategory::Sea];
        assert_eq!(c.min_wait, Duration::from_secs(3), "Sea min_wait");
        assert_eq!(c.max_wait, Duration::from_secs(90), "Sea max_wait");
        assert_eq!(c.max_retries, 7, "Sea max_retries");
        assert_eq!(
            c.overall_timeout,
            Duration::from_secs(600),
            "Sea overall_timeout"
        );
    }

    #[test]
    fn cloudfetch_override_stays_fixed_regardless_of_user_knobs() {
        // CloudFetch chunk downloads nest inside the provider's own retry loop
        // (`CloudFetchConfig::max_retries`). The inner HTTP client must NOT add
        // a second retry budget on top — otherwise the two multiply and a
        // permanently-failing chunk blocks the consumer well past the
        // conformance bound (databricks-odbc#144 / #226). So — like Auth — the
        // CloudFetch category keeps a fixed override no matter how generous the
        // user's global retry knobs are: zero HTTP-level retries, tight
        // wall-clock bound.
        let http = HttpConfig {
            retry_max_retries: 99,
            overall_timeout: Duration::from_secs(3600),
            ..HttpConfig::default()
        };
        let configs = build_retry_configs_for_session(&config_with_http(http));
        let cf = &configs[&RequestCategory::CloudFetch];
        assert_eq!(
            cf.max_retries, 0,
            "CloudFetch must not retry at the HTTP layer"
        );
        assert_eq!(
            cf.overall_timeout,
            Duration::from_secs(20),
            "CloudFetch keeps its tight wall-clock bound"
        );
        assert_eq!(
            cf.overall_timeout, CLOUDFETCH_DOWNLOAD_BUDGET,
            "HTTP CloudFetch budget must be the shared constant"
        );
    }

    #[test]
    fn cloudfetch_provider_and_http_budgets_are_unified() {
        // The provider retry loop (CloudFetchConfig::download_timeout) and
        // the inner HTTP client (CloudFetch overall_timeout) must both be
        // the single shared budget so a stuck chunk is bounded end-to-end
        // and the two layers can't drift (audit H05).
        let result = ResultConfig::default();
        let cf = result_to_cloudfetch_config(&result);
        assert_eq!(
            cf.download_timeout, CLOUDFETCH_DOWNLOAD_BUDGET,
            "provider download_timeout must be the shared budget"
        );

        let http_cf = build_retry_configs_for_session(&config_with_http(HttpConfig::default()))
            [&RequestCategory::CloudFetch]
            .overall_timeout;
        assert_eq!(
            cf.download_timeout, http_cf,
            "provider and HTTP CloudFetch budgets must match"
        );
    }

    #[test]
    fn auth_override_stays_fixed_regardless_of_user_knobs() {
        // Even with a very generous user overall_timeout, the Auth
        // category keeps its fast-fail 30s / 3-retry override so token
        // endpoint calls can't hang for the full global budget.
        let http = HttpConfig {
            retry_max_retries: 99,
            overall_timeout: Duration::from_secs(3600),
            ..HttpConfig::default()
        };
        let configs = build_retry_configs_for_session(&config_with_http(http));
        let auth = &configs[&RequestCategory::Auth];
        assert_eq!(auth.overall_timeout, Duration::from_secs(30));
        assert_eq!(auth.max_retries, 3);
    }

    #[test]
    fn telemetry_retry_knobs_have_dedicated_category() {
        let telemetry = TelemetryConfig {
            max_retries: 2,
            retry_delay: Duration::from_millis(50),
            close_flush_timeout: Duration::from_secs(7),
            ..TelemetryConfig::default()
        };
        let configs = build_retry_configs_for_session(&config_with_telemetry(telemetry));
        let telemetry = &configs[&RequestCategory::Telemetry];

        assert_eq!(telemetry.min_wait, Duration::from_millis(50));
        assert_eq!(telemetry.max_wait, Duration::from_millis(50));
        assert_eq!(telemetry.max_retries, 2);
        assert_eq!(telemetry.overall_timeout, Duration::from_secs(7));
    }

    #[test]
    fn defaults_preserve_legacy_behaviour() {
        let configs = build_retry_configs_for_session(&config_with_http(HttpConfig::default()));
        let sea = &configs[&RequestCategory::Sea];
        assert_eq!(sea.min_wait, Duration::from_secs(1));
        assert_eq!(sea.max_wait, Duration::from_secs(60));
        assert_eq!(sea.max_retries, 5);
        assert_eq!(sea.overall_timeout, Duration::from_secs(900));
    }

    // ─── Proxy / socket-timeout projection ─────────────────────────

    /// `None` (no public proxy config) ⇒ internal proxy `url` stays
    /// unset, preserving reqwest's env-var behaviour.
    #[test]
    fn proxy_absent_keeps_env_var_behaviour() {
        let proxy = build_proxy_config(None);
        assert!(proxy.url.is_none());
        let cfg = config_with_http(HttpConfig::default());
        assert!(build_http_client_config(&cfg, None).proxy.url.is_none());
    }

    /// A public `ProxyConfig` with a URL (and optional auth / bypass)
    /// projects field-for-field onto the internal proxy config.
    #[test]
    fn proxy_fields_project_onto_internal_config() {
        let mut cfg = config_with_http(HttpConfig::default());
        cfg.proxy = Some(crate::config::ProxyConfig {
            url: Some("http://proxy.corp.example.com:8080".to_string()),
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            bypass_hosts: Some("localhost,*.internal".to_string()),
            ..Default::default()
        });
        let proxy = build_http_client_config(&cfg, None).proxy;
        assert_eq!(
            proxy.url.as_deref(),
            Some("http://proxy.corp.example.com:8080")
        );
        assert_eq!(proxy.username.as_deref(), Some("alice"));
        assert_eq!(proxy.password.as_deref(), Some("secret"));
        assert_eq!(proxy.bypass_hosts.as_deref(), Some("localhost,*.internal"));
    }

    /// The public `request_timeout` becomes the internal HTTP client's
    /// historically named `read_timeout`, which is applied as reqwest's total
    /// request timeout.
    #[test]
    fn request_timeout_becomes_internal_total_request_timeout() {
        let http = HttpConfig {
            request_timeout: Duration::from_millis(4500),
            ..HttpConfig::default()
        };
        let hcc = build_http_client_config(&config_with_http(http), None);
        assert_eq!(hcc.read_timeout, Duration::from_millis(4500));
    }

    // ─── Client result-config override (cloudfetch_max_chunks_in_memory) ───

    /// Build a minimal config carrying the given session_conf, for
    /// exercising `apply_client_result_overrides` in isolation.
    fn config_with_session_conf(conf: HashMap<String, String>) -> SessionConfig {
        let mut cfg = config_with_http(HttpConfig::default());
        cfg.session_conf = conf;
        cfg
    }

    /// A valid positive value folds into ResultConfig and the key is
    /// stripped from session_conf so it can never reach the SEA wire.
    #[test]
    fn cloudfetch_max_chunks_override_applies_and_is_stripped() {
        let mut cfg = config_with_session_conf(HashMap::from([(
            crate::config::ClientConfKey::CloudfetchMaxChunksInMemory
                .as_str()
                .to_string(),
            "4".to_string(),
        )]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(cfg.result.cloudfetch_max_chunks_in_memory, 4);
        assert!(
            cfg.session_conf.is_empty(),
            "client knob must be removed from session_conf, got {:?}",
            cfg.session_conf
        );
    }

    /// The key is matched case-insensitively, like the rest of the
    /// session_conf handling, and still stripped.
    #[test]
    fn cloudfetch_max_chunks_override_is_case_insensitive() {
        let mut cfg = config_with_session_conf(HashMap::from([(
            "CloudFetch_Max_Chunks_In_Memory".to_string(),
            "8".to_string(),
        )]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(cfg.result.cloudfetch_max_chunks_in_memory, 8);
        assert!(cfg.session_conf.is_empty());
    }

    /// Two spellings differing only in case resolve deterministically to the
    /// lexicographically-last one (not whichever HashMap iteration yields
    /// first), every variant is stripped, and an unrelated server key survives
    /// untouched. Guards against both the nondeterministic-winner bug and an
    /// over-broad removal that would wipe legitimate server SET params.
    #[test]
    fn cloudfetch_max_chunks_duplicate_case_variants_are_deterministic() {
        // "cloudfetch_..." (lowercase 'c') sorts after "CloudFetch_..."
        // (uppercase 'C') in ASCII, so the lowercase spelling's value (200)
        // wins regardless of insertion order; 200 clamps to the ceiling.
        let mut cfg = config_with_session_conf(HashMap::from([
            (
                "CloudFetch_Max_Chunks_In_Memory".to_string(),
                "8".to_string(),
            ),
            (
                "cloudfetch_max_chunks_in_memory".to_string(),
                "200".to_string(),
            ),
            // A benign server param that must NOT be removed.
            ("ansi_mode".to_string(), "true".to_string()),
        ]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(
            cfg.result.cloudfetch_max_chunks_in_memory, 200,
            "the lexicographically-last case variant must win deterministically"
        );
        assert!(
            !cfg.session_conf.keys().any(|k| k.eq_ignore_ascii_case(
                crate::config::ClientConfKey::CloudfetchMaxChunksInMemory.as_str()
            )),
            "every case variant of the client knob must be stripped, got {:?}",
            cfg.session_conf
        );
        assert_eq!(
            cfg.session_conf.get("ansi_mode").map(String::as_str),
            Some("true"),
            "an unrelated server param must survive untouched, got {:?}",
            cfg.session_conf
        );
    }

    /// Absent key ⇒ the ResultConfig default (16) is preserved.
    #[test]
    fn cloudfetch_max_chunks_absent_keeps_default() {
        let mut cfg = config_with_session_conf(HashMap::new());
        apply_client_result_overrides(&mut cfg);
        assert_eq!(
            cfg.result.cloudfetch_max_chunks_in_memory,
            ResultConfig::default().cloudfetch_max_chunks_in_memory
        );
    }

    /// A non-numeric or zero value is ignored (default preserved, no
    /// connect error) but still stripped from session_conf.
    #[test]
    fn cloudfetch_max_chunks_invalid_is_ignored_but_stripped() {
        for bad in ["0", "-1", "abc", ""] {
            let mut cfg = config_with_session_conf(HashMap::from([(
                crate::config::ClientConfKey::CloudfetchMaxChunksInMemory
                    .as_str()
                    .to_string(),
                bad.to_string(),
            )]));
            apply_client_result_overrides(&mut cfg);
            assert_eq!(
                cfg.result.cloudfetch_max_chunks_in_memory, 16,
                "value {bad:?} should keep the default",
            );
            assert!(
                cfg.session_conf.is_empty(),
                "value {bad:?} should still be stripped",
            );
        }
    }

    /// A value above the ceiling is clamped (not rejected) so a fat-fingered
    /// value can't drive peak RSS toward OOM; the just-at-ceiling value passes
    /// through unclamped. Both are still stripped from session_conf.
    #[test]
    fn cloudfetch_max_chunks_above_ceiling_is_clamped() {
        // Well above the ceiling → clamped down.
        let mut cfg = config_with_session_conf(HashMap::from([(
            crate::config::ClientConfKey::CloudfetchMaxChunksInMemory
                .as_str()
                .to_string(),
            "1000000".to_string(),
        )]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(
            cfg.result.cloudfetch_max_chunks_in_memory,
            crate::config::CLOUDFETCH_MAX_CHUNKS_CEILING,
            "an absurd value must clamp to the ceiling"
        );
        assert!(cfg.session_conf.is_empty());

        // Exactly at the ceiling → unchanged (boundary is inclusive).
        let mut cfg = config_with_session_conf(HashMap::from([(
            crate::config::ClientConfKey::CloudfetchMaxChunksInMemory
                .as_str()
                .to_string(),
            crate::config::CLOUDFETCH_MAX_CHUNKS_CEILING.to_string(),
        )]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(
            cfg.result.cloudfetch_max_chunks_in_memory,
            crate::config::CLOUDFETCH_MAX_CHUNKS_CEILING,
        );
    }

    /// All six client-side result knobs are recognised in session_conf,
    /// applied to the typed ResultConfig, and stripped so none reach the
    /// wire. Guards the parity between `ClientConfKey::ALL` and the
    /// session_conf sweep.
    #[test]
    fn all_client_conf_keys_apply_from_session_conf_and_are_stripped() {
        use crate::config::ClientConfKey as K;
        let mut cfg = config_with_session_conf(HashMap::from([
            (
                K::CloudfetchEnabled.as_str().to_string(),
                "false".to_string(),
            ),
            (
                K::CloudfetchLinkPrefetchWindow.as_str().to_string(),
                "7".to_string(),
            ),
            (
                K::CloudfetchMaxChunksInMemory.as_str().to_string(),
                "9".to_string(),
            ),
            (
                K::InlineMaxChunksInMemory.as_str().to_string(),
                "3".to_string(),
            ),
            (
                K::ComplexTypesAsJson.as_str().to_string(),
                "true".to_string(),
            ),
            (
                K::IntervalsAsString.as_str().to_string(),
                "true".to_string(),
            ),
            // A benign server param must survive untouched.
            ("ansi_mode".to_string(), "true".to_string()),
        ]));
        apply_client_result_overrides(&mut cfg);
        assert!(!cfg.result.cloudfetch_enabled);
        assert_eq!(cfg.result.cloudfetch_link_prefetch_window, 7);
        assert_eq!(cfg.result.cloudfetch_max_chunks_in_memory, 9);
        assert_eq!(cfg.result.inline_max_chunks_in_memory, 3);
        assert!(cfg.result.complex_types_as_json);
        assert!(cfg.result.intervals_as_string);
        // Every client key stripped; only the server param remains.
        assert_eq!(
            cfg.session_conf.get("ansi_mode").map(String::as_str),
            Some("true"),
            "server param must survive; got {:?}",
            cfg.session_conf
        );
        assert_eq!(cfg.session_conf.len(), 1, "got {:?}", cfg.session_conf);
    }
}

#[cfg(test)]
mod is_open_tests {
    use super::*;
    use crate::feature_flags::ClientType;
    use crate::telemetry::test_support::CapturingTelemetryExporter;
    use crate::telemetry::{TelemetryExportResult, TelemetryExporter, TelemetryRequest};
    use crate::ResultBatch;
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde_json::json;
    use serde_json::Value;
    use std::future::Future;
    use std::pin::Pin;
    use wiremock::matchers::{body_json, body_string_contains, header, method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn sessions_enable_version_exchange_on_standard_path() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(json!({
                "warehouse_id": "test_wh",
                "execution_mode": "FAST",
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "session_id": "sess-fast",
                "session_version": { "version_id": 1 },
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-fast"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let session = Session::builder()
            .http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .open()
            .await
            .expect("FAST session must open on the standard path");

        session.close().await.expect("FAST session must close");
    }

    /// `Session::is_open` reports true for a freshly-opened session and
    /// false once its server-side delete has been initiated. This is the
    /// only place the false path is reachable — over the C ABI
    /// `kernel_session_close` frees the handle in the same call, so the C
    /// wrapper can't observe it; `is_open`/`initiate_delete` are
    /// `pub(crate)`, so this must live in-crate. Guards against an
    /// inverted / hardcoded-true regression (hosts back
    /// `SQL_ATTR_CONNECTION_DEAD` on this).
    #[tokio::test]
    async fn is_open_flips_true_to_false_on_delete() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-is-open","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-is-open"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid http_path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .open()
            .await
            .expect("session must open");

        assert!(
            session.is_open(),
            "a freshly-opened session must report open"
        );
        session.inner.initiate_delete();
        assert!(
            !session.is_open(),
            "after initiate_delete the session must report not-open"
        );
    }

    /// `Session::test_connection` round-trips a `SELECT 1` and returns
    /// `Ok(())` when the server responds. Pins that the probe actually
    /// issues a statement (POST /statements) rather than being a local
    /// no-op like `is_open`.
    #[tokio::test]
    async fn test_connection_succeeds_and_issues_select_1() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_ipc::writer::StreamWriter;
        use arrow_schema::{DataType, Field, Schema};
        use base64::{engine::general_purpose::STANDARD, Engine};
        use serde_json::json;

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-test-conn","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        // The SELECT 1 result: one BIGINT column, one row.
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut ipc, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let body = json!({
            "statement_id": "stmt-test-conn",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": 1,
                    "columns": [{ "name": "1", "type_name": "BIGINT", "type_text": "BIGINT", "position": 0 }],
                },
                "total_chunk_count": 1,
                "total_row_count": 1,
                "total_byte_count": ipc.len() as i64,
                "truncated": false,
            },
            "result": {
                "chunk_index": 0,
                "row_offset": 0,
                "row_count": 1,
                "byte_count": ipc.len() as i64,
                "attachment": STANDARD.encode(&ipc),
            },
        })
        .to_string();
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        // Swallow the statement + session close DELETEs.
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;

        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid http_path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .open()
            .await
            .expect("session must open");

        session
            .test_connection()
            .await
            .expect("test_connection must succeed against a live server");

        // The probe must have issued a POST /statements (SELECT 1), not
        // just a local flag read.
        let recorded = server.received_requests().await.unwrap_or_default();
        assert!(
            recorded.iter().any(|r| {
                r.method.as_str() == "POST" && r.url.path() == "/api/2.0/sql/statements"
            }),
            "test_connection must round-trip a statement to the server"
        );
    }

    /// When the server is unreachable, `test_connection` returns an error
    /// (not `Ok`) — the property a host relies on to flip
    /// `SQL_ATTR_CONNECTION_DEAD` to dead. Contrasts with `is_open`, which
    /// would still report the handle open here.
    #[tokio::test]
    async fn test_connection_fails_when_server_unreachable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-dead","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        let uri = server.uri();

        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(uri, "/sql/1.0/warehouses/test_wh")
                    .expect("valid http_path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .open()
            .await
            .expect("session must open");

        // Drop the mock server: subsequent statement round-trips have
        // nowhere to land, so the SELECT 1 probe must fail.
        drop(server);

        // The local handle is still "open" — this is exactly the gap
        // test_connection closes.
        assert!(session.is_open(), "handle is still locally open");
        let err = session
            .test_connection()
            .await
            .expect_err("test_connection must fail once the server is gone");
        eprintln!("[test_connection] probe error after server drop: {err:?}");
    }

    #[test]
    fn default_opts_ignore_transactions_defaults_to_true() {
        // Matches the reference/JDBC IgnoreTransactions=1 default.
        assert!(DefaultOpts::default().ignore_transactions);
    }

    /// With ignore_transactions = true (default), the transaction-control
    /// methods are no-ops: they return Ok WITHOUT issuing any statement.
    /// The mock server exposes ONLY session create/delete — no statement
    /// endpoint — so any attempt to run `SET AUTOCOMMIT`/`COMMIT`/`ROLLBACK`
    /// SQL would fail. Their success here proves the no-op short-circuit.
    #[tokio::test]
    async fn transaction_controls_are_noop_when_ignored() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"session_id":"sess-txn","warehouse_id":"test_wh"}"#),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/sess-txn"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid http_path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            // ignore_transactions defaults to true; be explicit for the test.
            .defaults(DefaultOpts {
                ignore_transactions: true,
                ..Default::default()
            })
            .open()
            .await
            .expect("session must open");

        assert!(session.autocommit(), "default autocommit is on");
        // All three no-op and succeed without hitting a (non-existent)
        // statement endpoint.
        session
            .set_autocommit(false)
            .await
            .expect("set_autocommit no-op");
        session.commit().await.expect("commit no-op");
        session.rollback().await.expect("rollback no-op");
        // State unchanged because the set was ignored.
        assert!(
            session.autocommit(),
            "autocommit must stay on when transactions are ignored"
        );
    }

    #[tokio::test]
    async fn telemetry_disabled_emits_no_connection_events() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-disabled").await;
        let exporter = CapturingTelemetryExporter::default();

        let session = telemetry_test_session(&server, "sess-telemetry-disabled", {
            let exporter = std::sync::Arc::new(exporter.clone());
            TelemetryConfig::default().with_exporter(exporter)
        })
        .await;

        session.close().await.expect("close succeeds");

        assert!(
            exporter.requests().is_empty(),
            "disabled telemetry must not export requests"
        );
    }

    #[tokio::test]
    async fn feature_flags_are_loaded_before_session_open_and_shared_across_sessions() {
        // Keep this cache identity unique among session/binding tests. Mock
        // servers use ephemeral ports that the OS may reuse while the
        // process-wide feature-flag cache is still alive.
        const DRIVER_VERSION: &str = "session-feature-flag-ordering";
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/2.0/connector-service/feature-flags/GOLANG/{DRIVER_VERSION}"
            )))
            .and(header("authorization", "Bearer dapi_test_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "flags": [{
                    "name": "example.session-flag",
                    "value": "true"
                }],
                "ttl_seconds": 300
            })))
            .expect(1)
            .mount(&server)
            .await;
        mount_session_create_delete(&server, "sess-feature-flags").await;

        for iteration in 0..2 {
            let session = Session::builder()
                .connection(
                    ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                        .expect("valid http_path"),
                )
                .auth(AuthConfig::Pat {
                    token: "dapi_test_token".into(),
                    identity_federation_client_id: None,
                })
                .client_identity(ClientIdentity::new(ClientType::Go, DRIVER_VERSION))
                .open()
                .await
                .expect("session must open");

            if iteration == 0 {
                let requests = server.received_requests().await.unwrap_or_default();
                let feature_flag_index = requests
                    .iter()
                    .position(|request| request.method.as_str() == "GET")
                    .expect("feature-flag GET must be sent");
                let create_session_index = requests
                    .iter()
                    .position(|request| {
                        request.method.as_str() == "POST"
                            && request.url.path() == "/api/2.0/sql/sessions"
                    })
                    .expect("CreateSession must be sent");
                assert!(
                    feature_flag_index < create_session_index,
                    "feature flags must be fetched before CreateSession"
                );
            }

            // Kernel-owned features reach typed, cache-only getters through
            // SessionInner.
            let internal_flags = session
                .inner
                .feature_flags()
                .expect("configured client identity installs a resolver");
            assert_eq!(internal_flags.get_bool("example.session-flag"), Some(true));
            session.close().await.expect("close succeeds");
        }

        let requests = server.received_requests().await.unwrap_or_default();
        let feature_flag_requests = requests
            .iter()
            .filter(|request| request.method.as_str() == "GET")
            .count();
        let create_session_requests = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path() == "/api/2.0/sql/sessions"
            })
            .count();
        assert_eq!(feature_flag_requests, 1, "sessions share the flag cache");
        assert_eq!(create_session_requests, 2, "each open creates a session");
    }

    #[tokio::test]
    async fn session_without_client_identity_skips_feature_flag_resolution() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        mount_session_create_delete(&server, "sess-no-feature-flags").await;

        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid http_path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .open()
            .await
            .expect("session must open");

        assert!(session.inner.feature_flags().is_none());
        session.close().await.expect("close succeeds");
    }

    #[tokio::test]
    async fn telemetry_enabled_exports_create_and_delete_on_close() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-enabled").await;
        let exporter = CapturingTelemetryExporter::default();

        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session = telemetry_test_session(&server, "sess-telemetry-enabled", telemetry).await;

        session.close().await.expect("close succeeds");

        let requests = exporter.requests();
        assert_eq!(requests.len(), 1, "close should flush one pending batch");
        assert_eq!(requests[0].proto_logs.len(), 2);
        let operations = requests[0]
            .proto_logs
            .iter()
            .map(|log| {
                let value: Value = serde_json::from_str(log).expect("valid frontend log json");
                assert_eq!(
                    value["entry"]["sql_driver_log"]["session_id"],
                    "sess-telemetry-enabled"
                );
                value["entry"]["sql_driver_log"]["sql_operation"]["operation_detail"]
                    ["operation_type"]
                    .as_str()
                    .expect("operation_type string")
                    .to_string()
            })
            .collect::<Vec<_>>();

        assert_eq!(operations, vec!["CREATE_SESSION", "DELETE_SESSION"]);
    }

    #[tokio::test]
    async fn telemetry_uses_client_driver_system_configuration_with_kernel_defaults() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-system-config").await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));

        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid http_path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .telemetry(telemetry)
            .driver_system_configuration(DriverSystemConfiguration {
                driver_name: Some("databricks-sql-python".to_string()),
                driver_version: Some("1.2.3".to_string()),
                runtime_name: Some("python".to_string()),
                ..Default::default()
            })
            .open()
            .await
            .expect("session must open");

        session.close().await.expect("close succeeds");

        let requests = exporter.requests();
        assert_eq!(requests.len(), 1, "close should flush one pending batch");
        let value: Value =
            serde_json::from_str(&requests[0].proto_logs[0]).expect("valid frontend log json");
        let system = &value["entry"]["sql_driver_log"]["system_configuration"];
        assert_eq!(system["driver_name"], "databricks-sql-python");
        assert_eq!(system["driver_version"], "1.2.3");
        assert_eq!(system["runtime_name"], "python");
        assert_eq!(system["runtime_vendor"], "rust-lang");
        assert_eq!(system["char_set_encoding"], "UTF-8");
    }

    #[tokio::test]
    async fn telemetry_export_failure_does_not_fail_session_close() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-failure").await;
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(FailingTelemetryExporter));

        let session = telemetry_test_session(&server, "sess-telemetry-failure", telemetry).await;

        session
            .close()
            .await
            .expect("telemetry export failure must be fail-open");
    }

    #[tokio::test]
    async fn statement_execute_success_exports_terminal_statement_event() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-stmt-success").await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            succeeded_inline_body("stmt-telemetry-success", &int64_batch("n", vec![1])),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-stmt-success", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let mut exec = stmt.execute().await.expect("execute succeeds");
        assert!(exec.next_batch().await.expect("first batch").is_some());
        assert!(exec.next_batch().await.expect("stream exhausted").is_none());
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT"))
            .expect("statement event exported");
        let sql_log = &statement_log["entry"]["sql_driver_log"];
        assert_eq!(sql_log["session_id"], "sess-telemetry-stmt-success");
        assert_eq!(sql_log["sql_statement_id"], "stmt-telemetry-success");
        assert_eq!(
            sql_log["sql_operation"]["statement_type"], "SQL",
            "statement telemetry should classify user SQL"
        );
        assert_eq!(sql_log["sql_operation"]["execution_result"], "INLINE_ARROW");
        assert_eq!(sql_log["sql_operation"]["is_compressed"], false);
        assert!(
            sql_log["sql_operation"].get("retry_count").is_none(),
            "retry_count should be omitted until the transport reports a real value"
        );
        assert!(
            sql_log["sql_operation"]["result_latency"].is_object(),
            "consuming the result stream should record result latency"
        );
        assert_eq!(
            sql_log["sql_operation"]["operation_detail"]["is_internal_call"],
            false
        );
        assert!(
            sql_log["operation_latency_ms"].as_i64().unwrap_or_default() >= 0,
            "statement event should include operation latency"
        );
    }

    #[tokio::test]
    async fn statement_close_after_partial_fetch_exports_terminal_statement_event() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-stmt-partial-close").await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            succeeded_inline_body("stmt-telemetry-partial-close", &int64_batch("n", vec![1])),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-stmt-partial-close", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let mut exec = stmt.execute().await.expect("execute succeeds");
        assert!(exec.next_batch().await.expect("first batch").is_some());
        exec.close().await.expect("statement close succeeds");
        session.close().await.expect("close succeeds");

        let statement_logs = exported_logs(&exporter)
            .into_iter()
            .filter(|log| operation_type(log) == Some("EXECUTE_STATEMENT"))
            .collect::<Vec<_>>();
        assert_eq!(
            statement_logs.len(),
            1,
            "partial close should emit exactly one statement telemetry event"
        );
        let sql_log = &statement_logs[0]["entry"]["sql_driver_log"];
        assert_eq!(sql_log["sql_statement_id"], "stmt-telemetry-partial-close");
        assert_eq!(sql_log["sql_operation"]["execution_result"], "INLINE_ARROW");
        assert!(
            sql_log["sql_operation"]["result_latency"].is_object(),
            "partial close should record result latency"
        );
    }

    #[tokio::test]
    async fn statement_execute_failure_exports_sanitized_error_without_sql_text() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-stmt-failure").await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            failed_body(
                "stmt-telemetry-failure",
                "syntax failure near secret_token",
                "42000",
            ),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-stmt-failure", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT secret_token FROM sensitive_table");
        let err = match stmt.execute().await {
            Ok(_) => panic!("execute should fail"),
            Err(err) => err,
        };
        assert_eq!(err.sql_state.as_deref(), Some("42000"));
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT"))
            .expect("statement event exported");
        let encoded = serde_json::to_string(&statement_log).expect("json stringify");
        assert!(
            !encoded.contains("SELECT secret_token FROM sensitive_table"),
            "statement telemetry must not include SQL text"
        );
        let error_info = &statement_log["entry"]["sql_driver_log"]["error_info"];
        assert_eq!(
            statement_log["entry"]["sql_driver_log"]["sql_statement_id"],
            "stmt-telemetry-failure"
        );
        assert_eq!(error_info["stack_trace"], "42000");
        assert!(
            error_info["error_name"].is_string(),
            "sanitized error category should be present"
        );
    }

    #[tokio::test]
    async fn async_statement_status_and_await_records_poll_count() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-async").await;
        mount_close_any_statement(&server).await;
        mount_execute(&server, running_body("stmt-telemetry-async")).await;
        mount_status(
            &server,
            "stmt-telemetry-async",
            succeeded_inline_body("stmt-telemetry-async", &int64_batch("n", vec![1])),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session = telemetry_test_session(&server, "sess-telemetry-async", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let async_exec = stmt.submit().await.expect("submit succeeds");
        let _ = async_exec.status().await.expect("status succeeds");
        let mut stream = async_exec.await_result().await.expect("await succeeds");
        assert!(stream.next_batch().await.expect("first batch").is_some());
        assert!(stream
            .next_batch()
            .await
            .expect("stream exhausted")
            .is_none());
        let _again = async_exec.fetch_result().await.expect("fetch succeeds");
        session.close().await.expect("close succeeds");

        let async_statement_logs = exported_logs(&exporter)
            .into_iter()
            .filter(|log| operation_type(log) == Some("EXECUTE_STATEMENT_ASYNC"))
            .collect::<Vec<_>>();
        assert_eq!(
            async_statement_logs.len(),
            1,
            "repeated terminal async calls should finalize telemetry once"
        );
        let statement_log = &async_statement_logs[0];
        let detail = &statement_log["entry"]["sql_driver_log"]["sql_operation"]["operation_detail"];
        assert_eq!(
            statement_log["entry"]["sql_driver_log"]["sql_statement_id"],
            "stmt-telemetry-async"
        );
        assert!(
            detail["n_operation_status_calls"]
                .as_i64()
                .unwrap_or_default()
                >= 2,
            "status() plus await_result() should record status polls"
        );
        assert!(
            detail["operation_status_latency_millis"]
                .as_i64()
                .unwrap_or_default()
                >= 0
        );
    }

    #[tokio::test]
    async fn async_statement_close_finalizes_partial_result_telemetry() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-async-partial-close").await;
        mount_close_any_statement(&server).await;
        mount_execute(&server, running_body("stmt-telemetry-async-partial-close")).await;
        mount_status(
            &server,
            "stmt-telemetry-async-partial-close",
            succeeded_inline_body(
                "stmt-telemetry-async-partial-close",
                &int64_batch("n", vec![1]),
            ),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-async-partial-close", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let mut async_exec = stmt.submit().await.expect("submit succeeds");
        let mut stream = async_exec.await_result().await.expect("await succeeds");
        assert!(stream.next_batch().await.expect("first batch").is_some());

        async_exec.close().await.expect("close succeeds");
        session.close().await.expect("close succeeds");

        let async_statement_logs = exported_logs(&exporter)
            .into_iter()
            .filter(|log| operation_type(log) == Some("EXECUTE_STATEMENT_ASYNC"))
            .collect::<Vec<_>>();
        assert_eq!(
            async_statement_logs.len(),
            1,
            "partial async result close should finalize telemetry once even while \
             the result stream is still live"
        );
        assert_eq!(
            async_statement_logs[0]["entry"]["sql_driver_log"]["sql_statement_id"],
            "stmt-telemetry-async-partial-close"
        );
    }

    #[tokio::test]
    async fn async_statement_status_failure_exports_error_telemetry() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-status-failure").await;
        mount_close_any_statement(&server).await;
        mount_execute(&server, running_body("stmt-telemetry-status-failure")).await;
        mount_status(
            &server,
            "stmt-telemetry-status-failure",
            failed_body(
                "stmt-telemetry-status-failure",
                "async status failure near secret_token",
                "42000",
            ),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-status-failure", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT secret_token FROM sensitive_table");
        let async_exec = stmt.submit().await.expect("submit succeeds");
        let status = async_exec.status().await.expect("status succeeds");
        assert!(
            matches!(status, crate::statement::StatementStatus::Failed(_)),
            "status should surface terminal failure"
        );
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT_ASYNC"))
            .expect("async statement event exported");
        let encoded = serde_json::to_string(&statement_log).expect("json stringify");
        assert!(
            !encoded.contains("SELECT secret_token FROM sensitive_table"),
            "statement telemetry must not include SQL text"
        );
        let sql_log = &statement_log["entry"]["sql_driver_log"];
        assert_eq!(sql_log["sql_statement_id"], "stmt-telemetry-status-failure");
        assert_eq!(sql_log["error_info"]["stack_trace"], "42000");
    }

    #[tokio::test]
    async fn async_statement_drop_exports_terminal_statement_event() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-async-drop").await;
        mount_close_any_statement(&server).await;
        mount_execute(&server, running_body("stmt-telemetry-async-drop")).await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session = telemetry_test_session(&server, "sess-telemetry-async-drop", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let async_exec = stmt.submit().await.expect("submit succeeds");
        drop(async_exec);
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT_ASYNC"))
            .expect("async statement event exported on drop");
        let sql_log = &statement_log["entry"]["sql_driver_log"];
        assert_eq!(sql_log["sql_statement_id"], "stmt-telemetry-async-drop");
        assert!(
            sql_log.get("error_info").is_none(),
            "submit-and-drop should finalize as abandoned success, not as a statement error"
        );
    }

    #[tokio::test]
    async fn async_statement_await_poll_error_exports_error_telemetry() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-await-poll-error").await;
        mount_close_any_statement(&server).await;
        mount_execute(&server, running_body("stmt-telemetry-await-poll-error")).await;
        Mock::given(method("GET"))
            .and(path(
                "/api/2.0/sql/statements/stmt-telemetry-await-poll-error",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-await-poll-error", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let async_exec = stmt.submit().await.expect("submit succeeds");
        let err = match async_exec.await_result().await {
            Ok(_) => panic!("await should fail while polling status"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("Failed to parse status response"),
            "unexpected polling error: {err}"
        );
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT_ASYNC"))
            .expect("async statement event exported");
        assert_eq!(
            statement_log["entry"]["sql_driver_log"]["sql_statement_id"],
            "stmt-telemetry-await-poll-error"
        );
        assert!(
            statement_log["entry"]["sql_driver_log"]["error_info"].is_object(),
            "polling error should be exported as statement telemetry"
        );
    }

    #[tokio::test]
    async fn result_stream_exhaustion_exports_result_latency() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-result").await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            succeeded_inline_body("stmt-telemetry-result", &int64_batch("n", vec![1, 2, 3])),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session = telemetry_test_session(&server, "sess-telemetry-result", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT n FROM values");
        let mut exec = stmt.execute().await.expect("execute succeeds");
        let first = exec
            .next_batch()
            .await
            .expect("first batch")
            .expect("batch");
        let ResultBatch::Arrow(batch) = first;
        assert_eq!(batch.num_rows(), 3);
        assert!(exec.next_batch().await.expect("stream exhausted").is_none());
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT"))
            .expect("statement event exported");
        let result_latency =
            &statement_log["entry"]["sql_driver_log"]["sql_operation"]["result_latency"];
        assert!(
            result_latency["result_set_ready_latency_millis"]
                .as_i64()
                .is_some(),
            "first-batch latency should be recorded"
        );
        assert!(
            result_latency["result_set_consumption_latency_millis"]
                .as_i64()
                .is_some(),
            "consumption latency should be recorded on exhaustion"
        );
    }

    #[tokio::test]
    async fn dropping_unexhausted_result_stream_finalizes_statement_telemetry() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-drop").await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            succeeded_inline_body("stmt-telemetry-drop", &int64_batch("n", vec![1])),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session = telemetry_test_session(&server, "sess-telemetry-drop", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT 1 AS n");
        let exec = stmt.execute().await.expect("execute succeeds");
        drop(exec);
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT"))
            .expect("statement event exported after drop");
        assert_eq!(
            statement_log["entry"]["sql_driver_log"]["sql_statement_id"],
            "stmt-telemetry-drop"
        );
    }

    #[tokio::test]
    async fn cloudfetch_consumption_exports_chunk_details() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-cloudfetch").await;
        mount_close_any_statement(&server).await;
        let batch = int64_batch("n", vec![1, 2]);
        let ipc = arrow_ipc_bytes(&batch);
        let chunk_path = "/cloud-fetch/stmt-telemetry-cloudfetch/chunk-0.arrow";
        Mock::given(method("GET"))
            .and(path(chunk_path))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc.clone()))
            .mount(&server)
            .await;
        mount_execute(
            &server,
            succeeded_cloudfetch_body(
                "stmt-telemetry-cloudfetch",
                &batch,
                &format!("{}{}", server.uri(), chunk_path),
                ipc.len(),
            ),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session = telemetry_test_session(&server, "sess-telemetry-cloudfetch", telemetry).await;

        let mut stmt = session.statement();
        stmt.spec().sql("SELECT n FROM cloudfetch");
        let mut exec = stmt.execute().await.expect("execute succeeds");
        assert!(exec.next_batch().await.expect("first batch").is_some());
        assert!(exec.next_batch().await.expect("stream exhausted").is_none());
        session.close().await.expect("close succeeds");

        let statement_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("EXECUTE_STATEMENT"))
            .expect("statement event exported");
        let sql_operation = &statement_log["entry"]["sql_driver_log"]["sql_operation"];
        assert_eq!(sql_operation["execution_result"], "EXTERNAL_LINKS");
        let chunk_details = &sql_operation["chunk_details"];
        assert_eq!(chunk_details["total_chunks_present"], 1);
        assert_eq!(chunk_details["total_chunks_iterated"], 1);
        assert!(
            chunk_details["initial_chunk_latency_millis"]
                .as_i64()
                .is_some(),
            "initial chunk latency should be recorded"
        );
    }

    #[tokio::test]
    async fn metadata_list_catalogs_exports_metadata_telemetry_on_exhaustion() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-metadata-catalogs").await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            succeeded_inline_body(
                "stmt-telemetry-metadata-catalogs",
                &string_batch("catalog", vec!["main", "samples"]),
            ),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-metadata-catalogs", telemetry).await;

        let mut stream = session
            .metadata()
            .list_catalogs()
            .await
            .expect("list_catalogs succeeds");
        assert!(stream.next_batch().await.expect("first batch").is_some());
        assert!(stream
            .next_batch()
            .await
            .expect("stream exhausted")
            .is_none());
        drop(stream);
        session.close().await.expect("close succeeds");

        let metadata_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("LIST_CATALOGS"))
            .expect("metadata event exported");
        let sql_log = &metadata_log["entry"]["sql_driver_log"];
        assert_eq!(
            sql_log["sql_statement_id"],
            "stmt-telemetry-metadata-catalogs"
        );
        assert_eq!(sql_log["sql_operation"]["statement_type"], "METADATA");
        assert_eq!(sql_log["sql_operation"]["execution_result"], "INLINE_ARROW");
        assert!(
            sql_log["sql_operation"]["result_latency"].is_object(),
            "metadata stream exhaustion should record result latency"
        );
        assert!(
            sql_log.get("error_info").is_none(),
            "successful metadata call should not export error telemetry"
        );
    }

    #[tokio::test]
    async fn metadata_list_tables_fanout_exports_one_synthetic_event() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-tables-fanout").await;
        mount_close_any_statement(&server).await;
        mount_execute_for_sql(
            &server,
            "SHOW CATALOGS",
            succeeded_typed_inline_body(
                "stmt-telemetry-tables-catalogs",
                &string_batch("catalog", vec!["cat_a", "cat_b", "other"]),
            ),
        )
        .await;
        mount_execute_for_sql(
            &server,
            "SHOW TABLES IN CATALOG `cat_a`",
            succeeded_typed_inline_body(
                "stmt-telemetry-tables-cat-a",
                &server_tables_batch(vec![("s1", "t1", "cat_a", "TABLE")]),
            ),
        )
        .await;
        mount_execute_for_sql(
            &server,
            "SHOW TABLES IN CATALOG `cat_b`",
            succeeded_typed_inline_body(
                "stmt-telemetry-tables-cat-b",
                &server_tables_batch(vec![("s2", "v1", "cat_b", "VIEW")]),
            ),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-tables-fanout", telemetry).await;

        let catalog_pattern =
            crate::metadata_v0::LikePattern::new("cat\\_%").expect("valid catalog pattern");
        let mut stream = session
            .metadata()
            .list_tables(Some(catalog_pattern), None, None, None)
            .await
            .expect("list_tables fan-out succeeds");
        assert!(stream.next_batch().await.expect("first batch").is_some());
        assert!(stream
            .next_batch()
            .await
            .expect("stream exhausted")
            .is_none());
        drop(stream);
        session.close().await.expect("close succeeds");

        let logs = exported_logs(&exporter);
        let list_tables_logs = logs
            .iter()
            .filter(|log| operation_type(log) == Some("LIST_TABLES"))
            .collect::<Vec<_>>();
        assert_eq!(
            list_tables_logs.len(),
            1,
            "fan-out should export exactly one synthetic LIST_TABLES event"
        );
        assert!(
            logs.iter()
                .all(|log| operation_type(log) != Some("LIST_CATALOGS")),
            "internal catalog enumeration should use noop telemetry"
        );
        let sql_log = &list_tables_logs[0]["entry"]["sql_driver_log"];
        assert_eq!(sql_log["sql_operation"]["statement_type"], "METADATA");
        assert_eq!(sql_log["sql_operation"]["execution_result"], "INLINE_ARROW");
        assert!(
            sql_log.get("sql_statement_id").is_none(),
            "merged fan-out result has no single server statement id"
        );
    }

    #[tokio::test]
    async fn metadata_list_tables_single_catalog_exports_server_result_metadata() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-tables-single").await;
        mount_close_any_statement(&server).await;
        mount_execute(
            &server,
            succeeded_typed_inline_body(
                "stmt-telemetry-tables-single",
                &server_tables_batch(vec![("default", "t1", "main", "TABLE")]),
            ),
        )
        .await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-tables-single", telemetry).await;

        let catalog = crate::metadata_v0::LikePattern::new("main").expect("valid catalog pattern");
        let mut stream = session
            .metadata()
            .list_tables(Some(catalog), None, None, None)
            .await
            .expect("list_tables single catalog succeeds");
        assert!(stream.next_batch().await.expect("first batch").is_some());
        assert!(stream
            .next_batch()
            .await
            .expect("stream exhausted")
            .is_none());
        drop(stream);
        session.close().await.expect("close succeeds");

        let list_tables_logs = exported_logs(&exporter)
            .into_iter()
            .filter(|log| operation_type(log) == Some("LIST_TABLES"))
            .collect::<Vec<_>>();
        assert_eq!(
            list_tables_logs.len(),
            1,
            "single-catalog path should export one LIST_TABLES event"
        );
        let sql_log = &list_tables_logs[0]["entry"]["sql_driver_log"];
        assert_eq!(sql_log["sql_statement_id"], "stmt-telemetry-tables-single");
        assert_eq!(sql_log["sql_operation"]["statement_type"], "METADATA");
        assert_eq!(sql_log["sql_operation"]["execution_result"], "INLINE_ARROW");
    }

    #[tokio::test]
    async fn metadata_argument_error_exports_sanitized_error_telemetry() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-metadata-error").await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-metadata-error", telemetry).await;

        let table = crate::metadata_v0::Identifier::new("tbl").expect("valid table id");
        let error = match session.metadata().primary_keys(None, None, table).await {
            Ok(_) => panic!("missing catalog/schema should fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, crate::kernel_error::ErrorCode::InvalidArgument);
        session.close().await.expect("close succeeds");

        let metadata_log = exported_logs(&exporter)
            .into_iter()
            .find(|log| operation_type(log) == Some("LIST_PRIMARY_KEYS"))
            .expect("metadata error event exported");
        let sql_log = &metadata_log["entry"]["sql_driver_log"];
        assert_eq!(sql_log["sql_operation"]["statement_type"], "METADATA");
        assert!(
            sql_log.get("sql_statement_id").is_none(),
            "argument validation failure happens before a server statement exists"
        );
        assert_eq!(sql_log["error_info"]["error_name"], "InvalidArgument");
        assert!(
            sql_log["error_info"].get("stack_trace").is_none(),
            "non-SQLSTATE metadata validation errors should not leak messages into stack_trace"
        );
        assert!(
            !serde_json::to_string(&metadata_log)
                .expect("metadata log json")
                .contains("primary_keys requires a catalog identifier"),
            "metadata telemetry should not include raw error messages"
        );
    }

    #[tokio::test]
    async fn metadata_static_table_types_finalizes_on_drop() {
        let server = MockServer::start().await;
        mount_session_create_delete(&server, "sess-telemetry-metadata-table-types").await;
        let exporter = CapturingTelemetryExporter::default();
        let telemetry = TelemetryConfig {
            enabled: true,
            ..Default::default()
        }
        .with_exporter(std::sync::Arc::new(exporter.clone()));
        let session =
            telemetry_test_session(&server, "sess-telemetry-metadata-table-types", telemetry).await;

        let stream = session
            .metadata()
            .table_types()
            .await
            .expect("table_types succeeds");
        drop(stream);
        session.close().await.expect("close succeeds");

        let metadata_logs = exported_logs(&exporter)
            .into_iter()
            .filter(|log| operation_type(log) == Some("LIST_TABLE_TYPES"))
            .collect::<Vec<_>>();
        assert_eq!(
            metadata_logs.len(),
            1,
            "metadata stream drop should finalize telemetry exactly once"
        );
        let sql_log = &metadata_logs[0]["entry"]["sql_driver_log"];
        assert_eq!(sql_log["sql_operation"]["statement_type"], "METADATA");
        assert_eq!(sql_log["sql_operation"]["execution_result"], "INLINE_ARROW");
        assert!(
            sql_log.get("sql_statement_id").is_none(),
            "static metadata has no server statement id"
        );
    }

    async fn telemetry_test_session(
        server: &MockServer,
        expected_session_id: &str,
        telemetry: TelemetryConfig,
    ) -> Session {
        let session = Session::builder()
            .connection(
                ConnectionConfig::from_http_path(server.uri(), "/sql/1.0/warehouses/test_wh")
                    .expect("valid http_path"),
            )
            .auth(AuthConfig::Pat {
                token: "dapi_test_token".into(),
                identity_federation_client_id: None,
            })
            .telemetry(telemetry)
            .open()
            .await
            .expect("session must open");

        assert_eq!(session.session_id(), expected_session_id);
        session
    }

    async fn mount_session_create_delete(server: &MockServer, session_id: &str) {
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                r#"{{"session_id":"{session_id}","warehouse_id":"test_wh"}}"#
            )))
            .mount(server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(format!("/api/2.0/sql/sessions/{session_id}")))
            .respond_with(ResponseTemplate::new(200))
            .mount(server)
            .await;
    }

    async fn mount_close_any_statement(server: &MockServer) {
        Mock::given(method("DELETE"))
            .and(path_regex(r"^/api/2\.0/sql/statements/.+"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(server)
            .await;
    }

    async fn mount_execute(server: &MockServer, body: String) {
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }

    async fn mount_execute_for_sql(server: &MockServer, sql_substr: &str, body: String) {
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(sql_substr))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }

    async fn mount_status(server: &MockServer, statement_id: &str, body: String) {
        Mock::given(method("GET"))
            .and(path(format!("/api/2.0/sql/statements/{statement_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }

    fn exported_logs(exporter: &CapturingTelemetryExporter) -> Vec<Value> {
        exporter
            .requests()
            .into_iter()
            .flat_map(|request| request.proto_logs)
            .map(|log| serde_json::from_str(&log).expect("valid frontend log json"))
            .collect()
    }

    fn operation_type(log: &Value) -> Option<&str> {
        log["entry"]["sql_driver_log"]["sql_operation"]["operation_detail"]["operation_type"]
            .as_str()
    }

    fn int64_batch(name: &str, values: Vec<i64>) -> RecordBatch {
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(Int64Array::from(values))])
            .expect("valid batch")
    }

    fn string_batch(name: &str, values: Vec<&str>) -> RecordBatch {
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(StringArray::from(values))])
            .expect("valid batch")
    }

    fn server_tables_batch(rows: Vec<(&str, &str, &str, &str)>) -> RecordBatch {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("tableName", DataType::Utf8, false),
            Field::new("isTemporary", DataType::Boolean, false),
            Field::new("information", DataType::Utf8, true),
            Field::new("catalogName", DataType::Utf8, false),
            Field::new("tableType", DataType::Utf8, false),
            Field::new("remarks", DataType::Utf8, true),
        ]));
        let namespaces = rows.iter().map(|row| row.0).collect::<Vec<_>>();
        let table_names = rows.iter().map(|row| row.1).collect::<Vec<_>>();
        let catalog_names = rows.iter().map(|row| row.2).collect::<Vec<_>>();
        let table_types = rows.iter().map(|row| row.3).collect::<Vec<_>>();
        let row_count = rows.len();
        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(namespaces)),
                std::sync::Arc::new(StringArray::from(table_names)),
                std::sync::Arc::new(BooleanArray::from(vec![false; row_count])),
                std::sync::Arc::new(StringArray::from(vec![Some(""); row_count])),
                std::sync::Arc::new(StringArray::from(catalog_names)),
                std::sync::Arc::new(StringArray::from(table_types)),
                std::sync::Arc::new(StringArray::from(vec![Some(""); row_count])),
            ],
        )
        .expect("valid server tables batch")
    }

    fn arrow_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    fn succeeded_inline_body(statement_id: &str, batch: &RecordBatch) -> String {
        succeeded_inline_body_with_column_types(statement_id, batch, |_| ("BIGINT", "BIGINT"))
    }

    fn succeeded_typed_inline_body(statement_id: &str, batch: &RecordBatch) -> String {
        succeeded_inline_body_with_column_types(statement_id, batch, |data_type| match data_type {
            DataType::Boolean => ("BOOLEAN", "BOOLEAN"),
            DataType::Int64 => ("BIGINT", "BIGINT"),
            DataType::Utf8 => ("STRING", "STRING"),
            other => panic!("test helper does not map Arrow type {other:?}"),
        })
    }

    fn succeeded_inline_body_with_column_types(
        statement_id: &str,
        batch: &RecordBatch,
        column_type: impl Fn(&DataType) -> (&'static str, &'static str),
    ) -> String {
        let ipc = arrow_ipc_bytes(batch);
        let attachment = STANDARD.encode(&ipc);
        let columns: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let (type_name, type_text) = column_type(f.data_type());
                json!({
                    "name": f.name(),
                    "type_name": type_name,
                    "type_text": type_text,
                    "position": i as i32,
                })
            })
            .collect();

        json!({
            "statement_id": statement_id,
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": batch.num_columns() as i32,
                    "columns": columns,
                },
                "total_chunk_count": 1,
                "total_row_count": batch.num_rows() as i64,
                "total_byte_count": ipc.len() as i64,
                "truncated": false,
            },
            "result": {
                "chunk_index": 0,
                "row_offset": 0,
                "row_count": batch.num_rows() as i64,
                "byte_count": ipc.len() as i64,
                "attachment": attachment,
            },
        })
        .to_string()
    }

    fn succeeded_cloudfetch_body(
        statement_id: &str,
        batch: &RecordBatch,
        chunk_url: &str,
        byte_count: usize,
    ) -> String {
        let columns: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                json!({
                    "name": f.name(),
                    "type_name": "BIGINT",
                    "type_text": "BIGINT",
                    "position": i as i32,
                })
            })
            .collect();
        let expiration = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();

        json!({
            "statement_id": statement_id,
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": batch.num_columns() as i32,
                    "columns": columns,
                },
                "total_chunk_count": 1,
                "total_row_count": batch.num_rows() as i64,
                "total_byte_count": byte_count as i64,
                "truncated": false,
                "result_compression": "",
            },
            "result": {
                "external_links": [{
                    "external_link": chunk_url,
                    "expiration": expiration,
                    "chunk_index": 0,
                    "row_offset": 0,
                    "row_count": batch.num_rows() as i64,
                    "byte_count": byte_count as i64,
                    "next_chunk_index": null,
                }],
            },
        })
        .to_string()
    }

    fn running_body(statement_id: &str) -> String {
        json!({
            "statement_id": statement_id,
            "status": { "state": "RUNNING" },
        })
        .to_string()
    }

    fn failed_body(statement_id: &str, message: &str, sql_state: &str) -> String {
        json!({
            "statement_id": statement_id,
            "status": {
                "state": "FAILED",
                "sql_state": sql_state,
                "error": {
                    "error_code": "INVALID_PARAMETER_VALUE",
                    "message": format!("{message}. SQLSTATE: {sql_state}"),
                }
            },
        })
        .to_string()
    }

    #[derive(Debug)]
    struct FailingTelemetryExporter;

    impl TelemetryExporter for FailingTelemetryExporter {
        fn export<'a>(
            &'a self,
            _request: TelemetryRequest,
        ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
            Box::pin(async {
                Err(crate::telemetry::TelemetryExportError::Export(
                    "expected test failure".to_string(),
                ))
            })
        }
    }
}
