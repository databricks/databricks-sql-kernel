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
    AuthProvider, AuthorizationCodeProvider, ClientCredentialsProvider, PersonalAccessToken,
};
use crate::client::http::TlsConfig as HttpTlsConfig;
use crate::client::retry::{
    build_retry_configs, RequestCategory, RetryConfig, RetryConfigOverrides,
};
use crate::client::transport::TransportConfig;
use crate::client::{
    DatabricksHttpClient, HttpClientConfig, ProxyConfig as HttpProxyConfig, SeaRestTransport,
    Transport,
};
use crate::config::{
    AuthConfig, ConnectionConfig, DefaultOpts, HttpConfig, LoggingConfig, ProxyConfig,
    ResultConfig, SessionConfig, TelemetryConfig, TlsConfig, TransportMode,
};
use crate::kernel_error::{Error, Result};
use crate::klog::debug;
use crate::metadata_v0::Metadata;
use crate::reader::ResultReaderFactory;
use crate::statement::executed_async::ExecutedAsyncStatement;
use crate::statement::mutable::Statement;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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

    /// Tracing span tagged with `session_id`. Every public method
    /// enters this span via `let _g = self.inner.log_span.enter();`
    /// so log lines tied to a session are visibly grouped.
    log_span: Span,

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

        // Fold client-only result knobs (e.g. cloudfetch_max_chunks_in_memory)
        // carried in session_conf into the typed ResultConfig and strip them
        // from session_conf, before either the reader factory reads
        // config.result (below) or filter_session_conf runs on the wire body.
        apply_client_result_overrides(&mut config);

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
        let transport_config = TransportConfig::default();
        let sea = Arc::new(SeaRestTransport::new(
            http_client.clone(),
            host.clone(),
            warehouse_id.clone(),
            transport_config,
        ));
        let reader_factory = ResultReaderFactory::new(
            sea.clone() as Arc<dyn Transport>,
            http_client,
            result_to_cloudfetch_config(&config.result),
            result_to_post_process_options(&config.result),
            runtime_handle.clone(),
        );
        sea.set_reader_factory(reader_factory, runtime_handle.clone());
        let transport: Arc<dyn Transport> = sea;

        // 4. Mint the session on the server.
        let defaults = config.defaults.clone().unwrap_or_default();
        debug!(
            host = %host,
            warehouse_id = %warehouse_id,
            "creating session"
        );

        // When CloudFetch is disabled at the kernel layer, advertise this
        // capability to SEA via the `can_cloud_download` session conf.
        // Per the SEA inline-Arrow design (go/inline-arrow-sea, DD1), this
        // tells the server to fall back to the inline-Arrow path for all
        // result sizes instead of attempting cloud-fetch and erroring at
        // fetch time.
        //
        // Don't clobber an explicit non-empty user value — if the caller
        // wired `can_cloud_download` directly via `session_conf` for any
        // reason (e.g., to force inline-Arrow even with CloudFetch enabled
        // at the kernel layer), their value wins.
        //
        // **Case-insensitive key match** because SEA session-conf keys
        // are historically case-insensitive on the wire; without this, a
        // stray `"Can_Cloud_Download"` from the user would coexist with
        // the kernel's canonical-cased `"can_cloud_download"` and we'd
        // ship both keys (potentially with different values) — server
        // would pick one non-deterministically.
        //
        // **Empty-string user values are purged**: an empty value is
        // ambiguous (server behaviour undefined) and the kernel prefers
        // to inject its canonical `"false"` instead of shipping the
        // ambiguous empty. Purge-then-insert guarantees the wire body
        // carries at most one `can_cloud_download` key regardless of
        // case-variants the caller supplied.
        let mut session_conf = config.session_conf.clone();
        if !config.result.cloudfetch_enabled {
            let has_nonempty_user_value = session_conf
                .iter()
                .any(|(k, v)| k.eq_ignore_ascii_case("can_cloud_download") && !v.is_empty());
            crate::klog::debug!(
                cloudfetch_enabled = false,
                user_value_present = has_nonempty_user_value,
                "evaluating can_cloud_download injection",
            );
            if !has_nonempty_user_value {
                // Drop every case-variant (including empty-value ones)
                // before inserting the canonical key so we can't ship
                // duplicates that differ only in case.
                session_conf.retain(|k, _| !k.eq_ignore_ascii_case("can_cloud_download"));
                session_conf.insert("can_cloud_download".to_string(), "false".to_string());
            }
        }
        let session_info = transport
            .create_session(
                defaults.catalog.as_deref(),
                defaults.schema.as_deref(),
                crate::config::filter_session_conf(session_conf),
            )
            .await?;

        // 5. Tag the per-session tracing span. Future log lines
        //    inherit `session_id` automatically. Target is pinned to the
        //    kernel log target so the span (and any `log` record derived
        //    from it) maps to the `databricks.sql.kernel` Python logger,
        //    consistent with `klog` events — see `crate::klog`.
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
                log_span,
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

    /// Initiate the fire-and-forget server-side `DeleteSession` now,
    /// without awaiting it or consuming `self`. Idempotent. Used by the C
    /// ABI's `kernel_session_close` so the delete is initiated on close
    /// even when child handles still hold `Arc<SessionInner>` clones
    /// (otherwise it would be deferred until the last handle drops). For
    /// an awaitable close that surfaces failure, use [`Session::close`].
    pub(crate) fn initiate_delete(&self) {
        self.inner.initiate_delete();
    }

    /// Whether this session is still open — i.e. its server-side delete
    /// has not been initiated (via [`Session::close`] or
    /// [`Session::initiate_delete`]). This is a **local** check, not a
    /// server round-trip: it reports whether the handle has been
    /// closed/superseded, so a caller doesn't drive an already-deleted
    /// session. It does **not** detect a server-side idle timeout or a
    /// dropped network link. Backs the C ABI's `kernel_session_is_open`.
    pub(crate) fn is_open(&self) -> bool {
        !self.inner.closed.load(std::sync::atomic::Ordering::Acquire)
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
                .statement_status(&statement_id)
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
            self.inner
                .transport
                .delete_session(&self.inner.session_id)
                .await?;
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
    session_conf: HashMap<String, String>,
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

    /// Server-bound session conf passed through to SEA `session_confs`.
    pub fn session_conf(mut self, conf: HashMap<String, String>) -> Self {
        self.session_conf = conf;
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
            session_conf: self.session_conf,
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
        // The internal `read_timeout` corresponds to the public
        // `request_timeout` — both gate a single HTTP round-trip.
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
fn build_retry_configs_for_session(
    config: &SessionConfig,
) -> std::collections::HashMap<RequestCategory, RetryConfig> {
    let global = RetryConfig {
        min_wait: config.http.retry_min_wait,
        max_wait: config.http.retry_max_wait,
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
    build_retry_configs(&global, &overrides)
}

/// Client-only `session_conf` key that tunes how many decompressed
/// CloudFetch chunks the kernel holds in memory at once (peak RSS on
/// large result sets). It is a **client** knob, not a server SET
/// parameter: it is read by [`apply_client_result_overrides`] before
/// session creation and is deliberately absent from
/// [`SESSION_CONF_ALLOWLIST`](crate::config), so `filter_session_conf`
/// drops it and it never reaches the SEA wire. Exposed this way (rather
/// than as a new C-ABI symbol) so the Go/ODBC consumers can set it
/// through the existing `set_session_conf` entry point.
pub(crate) const CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS: &str = "cloudfetch_max_chunks_in_memory";

/// Upper bound applied to a host-supplied
/// [`CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS`] value. Peak RSS grows roughly
/// linearly with this knob (each in-memory chunk is tens of MB), so an
/// unbounded value — a typo like `"1000000"` meant as `"100"`, or a host bug —
/// risks OOM on large result sets. This ceiling (16× the default of 16, and 8×
/// the most aggressive value we've measured) is comfortably above any real
/// tuning yet bounds the blast radius of a fat-fingered value; a request above
/// it is clamped with a `warn!`, mirroring the retry setter's clamp discipline.
pub(crate) const CLOUDFETCH_MAX_CHUNKS_CEILING: usize = 256;

/// Fold client-only result-management knobs carried in `session_conf`
/// into the typed [`ResultConfig`], then remove them from `session_conf`
/// so they cannot leak onto the SEA wire.
///
/// Today this handles only [`CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS`]. A key
/// that is absent leaves the field at its `ResultConfig` default (16); a
/// present-but-unparseable or zero value is ignored (default preserved)
/// with a warning rather than failing session creation — a
/// best-effort tuning knob should never turn a typo into a connect
/// error. A value above [`CLOUDFETCH_MAX_CHUNKS_CEILING`] is clamped to
/// that ceiling (with a warning) so a fat-fingered value can't drive peak
/// RSS toward OOM. Matched case-insensitively, consistent with the rest of
/// the `session_conf` handling; if a host sets two spellings that differ only
/// in case the lexicographically-last one wins deterministically (and all
/// variants are stripped).
fn apply_client_result_overrides(config: &mut SessionConfig) {
    // Collect EVERY case-insensitive spelling of the key, not just the first
    // one `HashMap` iteration happens to yield. Two variants that differ only
    // in case (e.g. `cloudfetch_max_chunks_in_memory` and
    // `CloudFetch_Max_Chunks_In_Memory`) would otherwise resolve
    // nondeterministically across runs. We sort the matches and let the
    // lexicographically-last spelling win — a stable, run-independent rule —
    // and remove all of them regardless of parse outcome, so no client-only
    // key survives into `filter_session_conf` / the wire.
    let mut keys: Vec<String> = config
        .session_conf
        .keys()
        .filter(|k| k.eq_ignore_ascii_case(CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS))
        .cloned()
        .collect();
    if keys.is_empty() {
        return;
    }
    keys.sort_unstable();
    if keys.len() > 1 {
        crate::klog::warn!(
            variants = keys.len(),
            "multiple case-variant cloudfetch_max_chunks_in_memory keys set; \
             applying the lexicographically-last spelling deterministically",
        );
    }
    // Remove every variant; the winner's value drives the override.
    let mut raw = String::new();
    for k in &keys {
        if let Some(v) = config.session_conf.remove(k) {
            raw = v;
        }
    }
    match raw.trim().parse::<usize>() {
        Ok(n) if n > 0 => {
            // Clamp to a sane ceiling: peak RSS scales with this knob, so an
            // absurd value (typo / host bug) would otherwise risk OOM.
            let clamped = n.min(CLOUDFETCH_MAX_CHUNKS_CEILING);
            if clamped != n {
                crate::klog::warn!(
                    requested = n,
                    ceiling = CLOUDFETCH_MAX_CHUNKS_CEILING,
                    "clamping cloudfetch_max_chunks_in_memory to the ceiling \
                     (peak RSS scales with this knob)",
                );
            }
            config.result.cloudfetch_max_chunks_in_memory = clamped;
            crate::klog::debug!(
                max_chunks_in_memory = clamped,
                "applied client cloudfetch_max_chunks_in_memory override",
            );
        }
        _ => {
            crate::klog::warn!(
                value = %raw,
                "ignoring invalid cloudfetch_max_chunks_in_memory (want a positive integer); \
                 keeping the default",
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
    }
}

/// Build the auth provider for a given [`AuthConfig`]. `host` is the
/// workspace URL — used for OIDC discovery on M2M / U2M flows.
async fn build_auth_provider(
    auth: &AuthConfig,
    host: &str,
    http_client: Arc<DatabricksHttpClient>,
) -> Result<Arc<dyn AuthProvider>> {
    match auth {
        AuthConfig::Pat { token } => {
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
            redirect_port,
            scopes,
            token_url_override,
            callback_timeout,
        } => {
            let client_id = client_id.as_deref().unwrap_or("databricks-cli");
            let redirect_port = redirect_port.unwrap_or(8020);
            let scopes = scopes
                .clone()
                .unwrap_or_else(|| vec!["all-apis".to_string(), "offline_access".to_string()]);
            let timeout = callback_timeout.unwrap_or_else(|| Duration::from_secs(120));
            let provider = AuthorizationCodeProvider::new_with_full_config(
                host,
                client_id,
                http_client,
                scopes,
                redirect_port,
                timeout,
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
            },
            session_conf: Default::default(),
            http,
            proxy: None,
            tls: Default::default(),
            result: Default::default(),
            logging: Default::default(),
            telemetry: Default::default(),
            transport: None,
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

        // SEA + CloudFetch inherit the user-tuned global config.
        for cat in [RequestCategory::Sea, RequestCategory::CloudFetch] {
            let c = &configs[&cat];
            assert_eq!(c.min_wait, Duration::from_secs(3), "{cat:?} min_wait");
            assert_eq!(c.max_wait, Duration::from_secs(90), "{cat:?} max_wait");
            assert_eq!(c.max_retries, 7, "{cat:?} max_retries");
            assert_eq!(
                c.overall_timeout,
                Duration::from_secs(600),
                "{cat:?} overall_timeout"
            );
        }
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
    /// `read_timeout` (the per-connection socket read timeout the napi
    /// `socketTimeoutMs` option drives).
    #[test]
    fn request_timeout_becomes_internal_read_timeout() {
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
            CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS.to_string(),
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
            // A benign, allowlisted server param that must NOT be removed.
            ("ansi_mode".to_string(), "true".to_string()),
        ]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(
            cfg.result.cloudfetch_max_chunks_in_memory, 200,
            "the lexicographically-last case variant must win deterministically"
        );
        assert!(
            !cfg.session_conf
                .keys()
                .any(|k| k.eq_ignore_ascii_case(CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS)),
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
                CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS.to_string(),
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
            CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS.to_string(),
            "1000000".to_string(),
        )]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(
            cfg.result.cloudfetch_max_chunks_in_memory, CLOUDFETCH_MAX_CHUNKS_CEILING,
            "an absurd value must clamp to the ceiling"
        );
        assert!(cfg.session_conf.is_empty());

        // Exactly at the ceiling → unchanged (boundary is inclusive).
        let mut cfg = config_with_session_conf(HashMap::from([(
            CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS.to_string(),
            CLOUDFETCH_MAX_CHUNKS_CEILING.to_string(),
        )]));
        apply_client_result_overrides(&mut cfg);
        assert_eq!(
            cfg.result.cloudfetch_max_chunks_in_memory,
            CLOUDFETCH_MAX_CHUNKS_CEILING,
        );
    }

    /// The client knob is NOT in the server allowlist, so even if it were
    /// left in session_conf it would be filtered before the wire. This
    /// pins that invariant so a future allowlist edit can't accidentally
    /// promote it to a server SET parameter.
    #[test]
    fn cloudfetch_max_chunks_key_is_not_server_allowlisted() {
        let filtered = crate::config::filter_session_conf(HashMap::from([(
            CLIENT_CONF_CLOUDFETCH_MAX_CHUNKS.to_string(),
            "4".to_string(),
        )]));
        assert!(
            filtered.is_empty(),
            "client knob must not survive the server allowlist, got {filtered:?}",
        );
    }
}

#[cfg(test)]
mod is_open_tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            })
            .open()
            .await
            .expect("session must open");

        assert!(
            session.is_open(),
            "a freshly-opened session must report open"
        );
        session.initiate_delete();
        assert!(
            !session.is_open(),
            "after initiate_delete the session must report not-open"
        );
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
}
