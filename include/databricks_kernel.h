/*
 * Copyright (c) 2026 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Databricks SQL Kernel — C ABI (v0).
 *
 * Hand-written header for the kernel's custom C ABI (cbindgen
 * integration is a v0.1 concern). The bodies live in `src/c_abi/` in the
 * `databricks-sql-kernel` crate, compiled as a `cdylib` / `staticlib`.
 *
 * This is the surface ODBC / Go consumers link against. It is NOT ADBC:
 * the kernel exposes a Databricks-native session / statement / result /
 * metadata model. Result data crosses the boundary via the Arrow C Data
 * Interface (`FFI_ArrowArray` / `FFI_ArrowSchema`), one array per
 * `kernel_result_stream_next_batch` call (a pull model).
 *
 * ## Error reporting
 *
 * Every fallible entry point returns a `KernelStatusCode`. On any
 * non-`Success` return, call `kernel_get_last_error` to fetch the typed
 * detail for the current thread. The thread-local error buffer is reset
 * at the start of every kernel call, and the `char*` fields it hands
 * back are valid only until the next kernel call on the same thread —
 * copy them out before calling again.
 *
 * ## ABI versioning
 *
 * There is no ABI promise through v0.x: struct layouts and the function
 * set may change freely; ABI stability is committed at v1.0. All handle
 * types below are *opaque* (incomplete) to C — you only ever hold
 * pointers to them, never read their fields — so their Rust-side layout
 * is irrelevant. The one caller-*readable* struct, `KernelError`, is laid
 * out exactly as declared here; copy its fields out before the next
 * `kernel_*` call on the thread.
 *
 * Because there is no promise through v0.x, a consumer that links a
 * prebuilt library can drift from the header it compiled against (a stale
 * header vs a newer `.a`, or vice versa). The `DATABRICKS_KERNEL_ABI_VERSION`
 * macro below is the version this header describes; `kernel_abi_version()`
 * returns the version compiled into the linked library. A consumer should
 * compare the two once at load time and refuse to run on a mismatch —
 * otherwise a layout change (e.g. a reordered `KernelStatusCode`) would be
 * misread as valid data rather than surfaced as a clean error. The macro is
 * bumped in lockstep with the Rust `KERNEL_ABI_VERSION` on any layout or
 * semantic change.
 *
 * ## Lifecycle & threading notes
 *
 * - **Re-execution invalidates prior handles.** `kernel_statement_execute`
 *   auto-cancels and closes any `kernel_executed_statement_t` previously
 *   returned by the same statement; calls on the stale handle then return
 *   `KernelStatusCode_InvalidStatementHandle`. Don't hold two live
 *   executed handles from one statement.
 * - **A result stream borrows its executed handle.** Closing the executed
 *   handle invalidates any stream obtained from it: subsequent stream
 *   calls return `InvalidStatementHandle` (a defined error, not UB).
 * - **Close is best-effort async.** `kernel_session_close` initiates the
 *   server-side delete before returning — a detached task runs it on the
 *   kernel's process-wide runtime — but does not wait for completion. A
 *   process that exits immediately after `kernel_session_close` may drop
 *   the detached task before it runs, leaving the server session to expire
 *   on its own idle timeout.
 * - **Close the session LAST.** Because close initiates the server-side
 *   delete, any handle still open against that session — a statement /
 *   executed handle, or a metadata result stream (which holds its own
 *   session reference and remains drivable) — must be drained and closed
 *   BEFORE `kernel_session_close`. Driving such a handle afterwards issues
 *   RPCs against a session whose delete has already been initiated and
 *   will surface server errors. Order teardown: result streams → executed
 *   handles → statements → session. (Statement cancellers are not in this
 *   chain — freeing one drives no RPC; see the next bullet.)
 * - **Free statement cancellers too.** A `kernel_statement_canceller_t`
 *   holds its own session reference, so a leaked canceller pins the
 *   client-side session (and its connection pool) past
 *   `kernel_session_close`. Freeing one drives no RPC, so its order is
 *   independent of the chain above: free it once its execute has returned,
 *   regardless of statement/session close.
 */

#ifndef DATABRICKS_KERNEL_H
#define DATABRICKS_KERNEL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ─── ABI version ─────────────────────────────────────────────────────
 *
 * The C-ABI version this header describes. `kernel_abi_version()` returns
 * the version compiled into the linked library; compare the two once at
 * load time and refuse to run on a mismatch (see "ABI versioning" above).
 * Bumped in lockstep with the Rust `KERNEL_ABI_VERSION`.
 */
#define DATABRICKS_KERNEL_ABI_VERSION 1

/* Version of the C ABI compiled into the linked library. Infallible and
 * side-effect-free. */
uint32_t kernel_abi_version(void);

/* ─── Arrow C Data Interface ──────────────────────────────────────────
 *
 * Mirrors the canonical `ArrowArray` / `ArrowSchema` from the Arrow C
 * Data Interface (https://arrow.apache.org/docs/format/CDataInterface.html).
 * The kernel populates these via the `arrow-array` crate's `ffi` module.
 * If the consuming project already declares these (e.g. via Arrow's
 * `abi.h`), define `DATABRICKS_KERNEL_NO_ARROW_CDATA` before including
 * this header to suppress the duplicate definitions.
 */
#ifndef DATABRICKS_KERNEL_NO_ARROW_CDATA

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

#endif /* DATABRICKS_KERNEL_NO_ARROW_CDATA */

/* ─── Status + error ──────────────────────────────────────────────────
 *
 * Discriminants match `crate::c_abi::KernelStatusCode`. Keep this enum in
 * lockstep with that Rust enum (the Rust side enforces a 1:1 mapping
 * against `ErrorCode` with an exhaustive match).
 */
typedef enum KernelStatusCode {
  KernelStatusCode_Success = 0,
  KernelStatusCode_InvalidArgument = 1,
  KernelStatusCode_Unauthenticated = 2,
  KernelStatusCode_PermissionDenied = 3,
  KernelStatusCode_NotFound = 4,
  KernelStatusCode_ResourceExhausted = 5,
  KernelStatusCode_Unavailable = 6,
  KernelStatusCode_Timeout = 7,
  KernelStatusCode_Cancelled = 8,
  KernelStatusCode_DataLoss = 9,
  KernelStatusCode_Internal = 10,
  KernelStatusCode_InvalidStatementHandle = 11,
  KernelStatusCode_NetworkError = 12,
  KernelStatusCode_SqlError = 13,
} KernelStatusCode;

/* Server-side async execution status (mirror of
 * `KernelStatementStatusKind`). Reserved for the async submit path,
 * which is not exposed over the v0 C ABI. */
typedef enum KernelStatementStatusKind {
  KernelStatementStatusKind_Pending = 0,
  KernelStatementStatusKind_Running = 1,
  KernelStatementStatusKind_Succeeded = 2,
  KernelStatementStatusKind_Failed = 3,
  KernelStatementStatusKind_Cancelled = 4,
  KernelStatementStatusKind_Closed = 5,
} KernelStatementStatusKind;

/*
 * Typed last-error detail. Populated by `kernel_get_last_error`. The
 * `char*` fields point into the calling thread's last-error buffer and
 * are valid only until the next kernel call on that thread; a NULL
 * pointer means the field was absent. `vendor_code` / `http_status` are
 * `0` when absent.
 */
typedef struct KernelError {
  /* Discriminant value of `KernelStatusCode`. */
  int32_t code;
  const char* message;
  /* 5 ASCII chars + NUL, or NULL if absent. */
  const char* sql_state;
  int32_t vendor_code;
  uint16_t http_status;
  bool retryable;
  const char* query_id;
} KernelError;

/*
 * Fetch the last error recorded on the calling thread. Returns `false`
 * (leaving `*out` untouched) if no error has been recorded since the
 * last kernel call cleared the buffer.
 */
bool kernel_get_last_error(KernelError* out);

/* ─── Opaque handles ──────────────────────────────────────────────────
 *
 * All handles are owned heap allocations on the Rust side; the C side
 * holds them by pointer and releases each via its matching `*_close` /
 * `*_free`.
 */
typedef struct kernel_session_config_t KernelSessionConfig;
typedef struct kernel_session_t kernel_session_t;
typedef struct kernel_statement_t kernel_statement_t;
typedef struct kernel_executed_statement_t kernel_executed_statement_t;
/* Async-consumption executed handle. Reserved: the async submit path is
 * deferred in v0 (see kernel_statement_submit), so no consumer receives a
 * live instance yet. */
typedef struct kernel_executed_async_statement_t kernel_executed_async_statement_t;
typedef struct kernel_result_stream_t kernel_result_stream_t;
/* Detached canceller for a sync-executing statement, returned by
 * kernel_statement_canceller_new. The returned handle is safe to use from
 * another thread concurrently with the blocking kernel_statement_execute on the
 * originating statement (the _new call itself is NOT — see its doc).
 * Statement-scoped, not execution-scoped: it tracks whichever query is currently
 * in flight on its statement, so a canceller retained across a re-execute
 * cancels the LATER query. Use one per execute (new before execute, free after
 * it returns). */
typedef struct kernel_statement_canceller_t kernel_statement_canceller_t;
/* Cooperative cancel token for the blocking `*_cancellable` entry points
 * (kernel_session_open_cancellable, kernel_result_stream_next_batch_cancellable).
 * Create one before a blocking call, pass it in, and fire it from ANOTHER
 * thread (kernel_cancel_token_cancel) when the caller's deadline elapses; the
 * blocked call then drops its in-flight request future and returns
 * KernelStatusCode_Cancelled. One-shot (a fired token stays fired) — create a
 * fresh token per logical operation. Firing is purely local (no RPC), so it
 * never blocks. See "Cooperative cancellation" below. */
typedef struct kernel_cancel_token_t kernel_cancel_token_t;

/* ─── Session config ──────────────────────────────────────────────────
 *
 * Build a config, set exactly one connection form and exactly one auth
 * form, then hand it to `kernel_session_open` (which consumes it). Only
 * PAT, OAuth M2M, and OAuth U2M (interactive browser flow) auth are
 * exposed over the C ABI.
 */
KernelStatusCode kernel_session_config_new(KernelSessionConfig** out);
void kernel_session_config_free(KernelSessionConfig* config);

/* Connection identity — call exactly one; the last one wins. */
KernelStatusCode kernel_session_config_set_warehouse(KernelSessionConfig* config,
                                                     const char* host,
                                                     const char* warehouse_id);
KernelStatusCode kernel_session_config_set_http_path(KernelSessionConfig* config,
                                                     const char* host,
                                                     const char* http_path);

/* Authentication — call exactly one; the last one wins. */
KernelStatusCode kernel_session_config_set_auth_pat(KernelSessionConfig* config,
                                                    const char* token);
KernelStatusCode kernel_session_config_set_auth_m2m(KernelSessionConfig* config,
                                                    const char* client_id,
                                                    const char* client_secret);
/* OAuth U2M (user-to-machine: authorization code + PKCE, browser flow).
 * All args optional: `client_id` NULL → public `databricks-cli` client;
 * `redirect_port` 0 → kernel default (8020); `scopes` (comma-separated)
 * NULL → kernel default (`all-apis`,`offline_access`; `offline_access`
 * yields a cached, auto-refreshed refresh token). INTERACTIVE: opening the
 * session starts a localhost listener and opens the user's browser. */
KernelStatusCode kernel_session_config_set_auth_u2m(KernelSessionConfig* config,
                                                    const char* client_id,
                                                    uint16_t redirect_port,
                                                    const char* scopes);

/* Add (or overwrite) one server-bound session-conf entry. */
KernelStatusCode kernel_session_config_set_session_conf(KernelSessionConfig* config,
                                                        const char* key,
                                                        const char* value);

/* Append one HTTP header sent on every request. Call once per header
 * (order preserved); `name` and `value` are both required. */
KernelStatusCode kernel_session_config_set_custom_header(KernelSessionConfig* config,
                                                         const char* name, const char* value);

/* Initialize kernel logging, process-wide and ONCE (first call wins;
 * later calls are no-ops). `level` is OFF/ERROR/WARN/INFO/DEBUG/TRACE
 * (NULL → RUST_LOG, default warn); `file_path` NULL → stderr. Not tied to
 * a session — lets a host route kernel logs into its own log file.
 *
 * A non-UTF-8 `file_path` returns InvalidArgument. Otherwise the result
 * reflects the FIRST call's outcome (a later call's level/file have no
 * effect and are not described by its return): Success when logging was
 * installed against the request (or skipped for level OFF); Internal (with
 * a stored last-error) when that install did not fully honour the request
 * — the file could not be opened (falls back to stderr) or a global
 * subscriber was already installed (no kernel subscriber installed). */
KernelStatusCode kernel_init_logging(const char* level, const char* file_path);

/* ─── Log callback (reverse-call) ─────────────────────────────────────
 *
 * A host that links the kernel crate directly (pyo3 / napi) bridges the
 * kernel's tracing output into its own logger in-process. A C-ABI consumer
 * can't install a Rust subscriber, so this is the reverse-call path: register
 * a callback the kernel invokes with each `databricks*`-targeted log event, so
 * kernel-internal diagnostics flow into the host's logger instead of only
 * reaching stderr via kernel_init_logging.
 *
 * The callback fires from the kernel's log-drain thread (NOT a query worker),
 * so it MUST be thread-safe. It SHOULD NOT unwind: the kernel guards each call
 * with catch_unwind (the callback pointer uses Rust's C-unwind ABI), so a
 * panicking callback poisons the sink and stops delivering rather than aborting
 * the process — but subsequent logs are then silently dropped, so a well-behaved
 * callback must still not throw across the boundary. It MUST NOT call back into
 * any kernel_* entry point: a reentrant blocking kernel_* call from this thread
 * is NOT rejected (that guard only covers calls from a kernel worker) — it would
 * stall the drain thread and drop queued logs. `ctx`
 * is passed back verbatim on every call and must stay valid for the process
 * lifetime. `level` (the callback arg) is 1=ERROR..5=TRACE; `target` and
 * `message` are valid only for the duration of the call — copy them out.
 * Records are enqueued on a bounded channel and DROPPED on overflow (logs are
 * advisory and never back-pressure a kernel hot path); the drop count is
 * observable via kernel_logging_stats and a synthetic "N dropped" line the
 * drain thread emits through the callback on recovery.
 *
 * The `level` PARAMETER (OFF/ERROR/WARN/INFO/DEBUG/TRACE, case-insensitive, or
 * NULL) sets the forward threshold with the SAME precedence as
 * kernel_init_logging: an explicit recognised level wins, else RUST_LOG. A host
 * that maps its own logger level (Go's DATABRICKS_LOG_LEVEL) passes it here so
 * the callback filters on that one knob rather than only RUST_LOG. When neither
 * resolves (NULL/unrecognised AND no matching RUST_LOG) the current threshold
 * is left UNCHANGED — the warn default on the first call, or the
 * previously-set level on a re-registration.
 *
 * Mutually exclusive with kernel_init_logging: both install the one
 * process-global tracing subscriber. kernel_set_log_callback returns Internal
 * (with a stored last error) if a global subscriber was already installed, or
 * if it won the subscriber but the log-drain thread could not be started (the
 * two cases carry distinct error messages); install exactly ONE of the two.
 * Passing a NULL `cb` returns InvalidArgument (there is no detach in v0). */
typedef void (*kernel_log_callback)(void* ctx, int level, const char* target,
                                    const char* message);
KernelStatusCode kernel_set_log_callback(kernel_log_callback cb, void* ctx,
                                         const char* level);

/* Report the log-callback bridge's health, so a host can distinguish "kernel
 * logs are quiet" from "kernel logs are being dropped / the sink died / never
 * installed". Writes to each non-NULL out-param (a NULL out-param is skipped):
 *   - `installed`: true iff kernel logs are CURRENTLY delivering — the callback
 *     won the process-global subscriber AND the sink is not poisoned. A callback
 *     that unwinds poisons the sink (delivery stops), which reads false here even
 *     though the subscriber is still installed; registering a new callback clears
 *     the poison and flips this back to true.
 *   - `dropped`: cumulative kernel log records dropped AFTER install — because
 *     the bounded delivery channel was full (slow callback) or the sink was
 *     poisoned by a callback that unwound. Does NOT count the drain-thread-never-
 *     started case (a spawn failure leaves no channel to overflow); there
 *     `installed` reads false and kernel_set_log_callback returned Internal.
 * Never fails; safe to poll at any time. */
void kernel_logging_stats(bool* installed, uint64_t* dropped);

/* ─── Proxy / TLS (optional) ──────────────────────────────────────────
 *
 * All optional; defaults are: no explicit proxy (reqwest honours
 * HTTP(S)_PROXY / NO_PROXY env vars) and strict TLS (system trust store,
 * valid chain, hostname verified). Any setter below is a relaxation or
 * override.
 */

/* Route traffic through an HTTP/HTTPS proxy. `url` is required (e.g.
 * "http://proxy.corp:8080"); `username` / `password` / `bypass_hosts`
 * (comma-separated) are optional — pass NULL to omit. */
KernelStatusCode kernel_session_config_set_proxy(KernelSessionConfig* config, const char* url,
                                                 const char* username, const char* password,
                                                 const char* bypass_hosts);

/* Add a PEM-encoded CA-certificate bundle (`pem`/`len` bytes) to the TLS
 * trust store on top of the system roots (corporate re-signing proxy /
 * on-prem CA). */
KernelStatusCode kernel_session_config_set_tls_trusted_certs(KernelSessionConfig* config,
                                                             const uint8_t* pem, size_t len);

/* Accept self-signed / invalid server certs (disables chain validation).
 * Development / on-prem only. */
KernelStatusCode kernel_session_config_set_tls_allow_self_signed(KernelSessionConfig* config,
                                                                 bool allow);

/* Skip the certificate hostname-vs-SNI check. Development / lab only. */
KernelStatusCode kernel_session_config_set_tls_skip_hostname_verification(KernelSessionConfig* config,
                                                                         bool skip);

/* Configure a client certificate + private key for mutual TLS (mTLS).
 * BOTH halves are required and passed together: `cert_pem`/`cert_len` is a
 * PEM leaf cert optionally followed by its intermediate chain; `key_pem`/
 * `key_len` is the matching PEM private key (use PKCS#8 for portability
 * across the tls-native and tls-rustls backends). Taking both at once makes
 * cert-without-key unrepresentable. A null buffer or zero length for either
 * half returns InvalidArgument. The private key is never rendered in logs. */
KernelStatusCode kernel_session_config_set_tls_client_certificate(KernelSessionConfig* config,
                                                                  const uint8_t* cert_pem,
                                                                  size_t cert_len,
                                                                  const uint8_t* key_pem,
                                                                  size_t key_len);

/* Enable or disable CloudFetch (cloud-storage-backed download of large result
 * sets). Default enabled; `false` forces the inline-Arrow path for all result
 * sizes (mirrors JDBC EnableQueryResultDownload=0). The kernel sets the
 * `can_cloud_download` session conf internally — do NOT set that conf directly
 * via set_session_conf (the server rejects it as not user-settable). */
KernelStatusCode kernel_session_config_set_cloudfetch_enabled(KernelSessionConfig* config,
                                                              bool enabled);

/* ─── Session lifecycle ───────────────────────────────────────────────*/

/*
 * Open a session. CONSUMES `config` on both success and failure — do not
 * use or free `config` afterwards. On success, `*out` holds a session
 * handle released with `kernel_session_close`.
 */
KernelStatusCode kernel_session_open(KernelSessionConfig* config, kernel_session_t** out);
KernelStatusCode kernel_session_close(kernel_session_t* session);

/*
 * Close a session and BLOCK until the server-side DeleteSession completes
 * (or errors). Unlike kernel_session_close (fire-and-forget), a program
 * that exits right after this returns has a confirmed server-side delete —
 * the parity path for a host that wants close to behave like the
 * synchronous DeleteSession RPC. CONSUMES `session` like
 * kernel_session_close (released on success and failure alike); the same
 * "close the session LAST" teardown ordering applies.
 */
KernelStatusCode kernel_session_close_blocking(kernel_session_t* session);

/*
 * Like kernel_session_open, but races the (blocking) connect against
 * `cancel_token` so a host can bound a slow warehouse cold-start or a
 * connect-time network partition by its own deadline. A NULL `cancel_token`
 * behaves EXACTLY like kernel_session_open. If the token fires (or is already
 * fired) before connect completes, the connect future is dropped — aborting
 * the in-flight request — and this returns KernelStatusCode_Cancelled; `config`
 * is CONSUMED on every path regardless. `cancel_token` is borrowed, not
 * consumed: the caller still owns and must free it after this returns, and may
 * fire it from another thread while this call is in flight.
 */
KernelStatusCode kernel_session_open_cancellable(KernelSessionConfig* config,
                                                 kernel_session_t** out,
                                                 const kernel_cancel_token_t* cancel_token);

/* Report whether a session is still OPEN (its delete has not been
 * initiated). Writes true/false to `*out`. LOCAL check only — no server
 * round-trip, so it does NOT detect a server idle-timeout or dropped
 * link; it reports only whether the handle has been closed/superseded.
 * A host backing SQL_ATTR_CONNECTION_DEAD should read `true` as
 * "not known-dead", not a positive liveness guarantee. No ownership. */
KernelStatusCode kernel_session_is_open(const kernel_session_t* session, bool* out);

/* Construct a new mutable statement bound to this session. */
KernelStatusCode kernel_session_new_statement(kernel_session_t* session,
                                              kernel_statement_t** out);

/* ─── Statement ───────────────────────────────────────────────────────*/

/* Set the statement's SQL text, replacing any previously set SQL. Also clears
 * every previously-bound parameter (raw and the typed positional binders below),
 * so a reused statement handle starts each query with a fresh parameter set. */
KernelStatusCode kernel_statement_set_sql(kernel_statement_t* stmt, const char* sql);

/* Typed positional parameter binding (1-based ordinal); the kernel infers the
 * SEA wire type from the C type. These are the "typed params" the
 * kernel_statement_bind_parameter contract refers to: within one statement they
 * cannot be combined with kernel_statement_bind_parameter (raw binds) — pick one
 * binding style. The conflict is reported at kernel_statement_execute, not here. */
KernelStatusCode kernel_statement_bind_null(kernel_statement_t* stmt, size_t ordinal);
KernelStatusCode kernel_statement_bind_int64(kernel_statement_t* stmt, size_t ordinal,
                                             int64_t value);
KernelStatusCode kernel_statement_bind_double(kernel_statement_t* stmt, size_t ordinal,
                                              double value);
KernelStatusCode kernel_statement_bind_bool(kernel_statement_t* stmt, size_t ordinal, bool value);
KernelStatusCode kernel_statement_bind_string(kernel_statement_t* stmt, size_t ordinal,
                                              const char* value);

/* Bind a pre-marshalled parameter: host-supplied Databricks type name +
 * stringified value, positional or named. For a host that already stringifies
 * values and knows the SQL type (e.g. the Go driver). name NULL/empty ->
 * positional (ordinal assigned at execute in push order); non-empty -> named.
 * value NULL -> SQL NULL (pass sql_type "VOID"); sql_type must be non-NULL and
 * non-empty (an empty type is rejected here with InvalidArgument).
 *
 * Lifecycle: raw binds ACCUMULATE (each call appends a marker in push order);
 * they carry no ordinal to overwrite by. kernel_statement_set_sql is the sole
 * reset point — it clears ALL prior binds (raw AND the typed positional binders
 * below) so each set_sql begins a fresh parameter set. kernel_statement_execute
 * does NOT clear them, so a host reusing one statement handle across queries must
 * call set_sql before re-binding, or the prior query's markers persist and the
 * server sees the wrong parameter count.
 *
 * Two mutual-exclusion rules hold within one statement, both enforced at
 * kernel_statement_execute (this call always returns Success on a valid
 * arg — the conflict surfaces later as InvalidArgument from execute):
 *   - positional and named raw params cannot be mixed;
 *   - raw params cannot be mixed with the typed positional binders
 *     (kernel_statement_bind_null/int64/double/bool/string above). */
KernelStatusCode kernel_statement_bind_parameter(kernel_statement_t* stmt,
                                                 const char* name,
                                                 const char* sql_type,
                                                 const char* value);

/*
 * Wait-for-result execution. On success, `*out` holds an executed handle
 * released with `kernel_executed_statement_close`.
 */
KernelStatusCode kernel_statement_execute(kernel_statement_t* stmt,
                                          kernel_executed_statement_t** out);

/*
 * Submit-and-return (async). DEFERRED in v0: this always returns
 * `KernelStatusCode_InvalidArgument` (with an explanatory last error) and
 * writes nothing to `*out`. Use the synchronous `kernel_statement_execute`
 * path. Declared so the symbol/contract is visible; wired when an ODBC /
 * Go consumer needs caller-driven polling.
 */
KernelStatusCode kernel_statement_submit(kernel_statement_t* stmt,
                                         kernel_executed_async_statement_t** out);

/*
 * Free the statement lifecycle owner. Does NOT free executed-handle
 * boxes — each executed handle is a separate allocation that you must
 * release with `kernel_executed_statement_close`.
 *
 * ORDERING: close the statement LAST. Executed handles produced by this
 * statement (and any result stream borrowed from them) share a validity
 * flag with the statement; closing the statement invalidates them, after
 * which `kernel_executed_statement_get_result_stream`,
 * `kernel_executed_statement_num_modified_rows`, and
 * `kernel_result_stream_next_batch` on those handles return
 * `KernelStatusCode_InvalidStatementHandle` (a defined error, never UB).
 * Drain / close the result stream, then the executed handle(s), then the
 * statement. The statement must outlive every handle it produced.
 */
KernelStatusCode kernel_statement_close(kernel_statement_t* stmt);

/* ─── Sync-execute cancellation (detached canceller) ──────────────────*/

/*
 * Create a detached canceller for `stmt`. Obtain it BEFORE
 * `kernel_statement_execute`, which blocks the calling thread for the
 * whole query — the statement cannot be cancelled through its own handle
 * mid-execute (that would alias the mutable borrow).
 *
 * UNDEFINED BEHAVIOR: this call itself borrows `stmt` mutably. Calling it
 * while a `kernel_statement_execute` on the same `stmt` is in flight on
 * another thread aliases that execute's own mutable borrow — undefined
 * behavior (memory corruption), not a returned error. Create the canceller on
 * the same thread that owns `stmt`, before dispatching execute. The natural
 * "spawn the blocking execute, then lazily create a canceller when the host
 * decides to cancel" pattern is exactly this misuse — do not do it; create the
 * canceller first, hand it to the cancelling thread, then dispatch execute.
 * Only the RETURNED handle is concurrency-safe: it holds only cloned Arcs, so
 * once you have it, cancelling from another thread while execute runs is fine.
 *
 * The canceller holds its own session reference, so it must be freed
 * regardless of whether the statement or session is closed. On success `*out`
 * holds a canceller released with `kernel_statement_canceller_free`.
 */
KernelStatusCode kernel_statement_canceller_new(
    kernel_statement_t* stmt, kernel_statement_canceller_t** out);

/*
 * Cancel the originating statement, server-side. Statement-scoped: targets
 * the server statement id currently observed for this statement. Idempotent
 * and safe to call concurrently with `kernel_statement_execute`.
 *
 * On Success, `*dispatched` reports whether a cancel RPC was POSTed:
 * true if a server statement id had been observed and the RPC was sent for it,
 * false if no id has been observed yet — nothing has executed, or the query is
 * still in its initial round-trip before the server issued an id. Both cases
 * return Success; `*dispatched` is how a host tells a dispatched cancel from a
 * no-op. `*dispatched` is written on every non-NULL path (false before the RPC,
 * then the RPC outcome on Success), so it holds a defined value once this
 * returns; a NULL `dispatched` is ignored.
 *
 * `dispatched == true` means only that a cancel was POSTed for the currently
 * observed statement id — NOT that a running query was interrupted. The id slot
 * retains the last observed id after a query finishes, so a cancel fired
 * post-terminal (or, in the narrow re-execute handoff, against a superseded id)
 * still POSTs and still reports true; the server treats a cancel of an
 * already-finished statement as its own no-op.
 *
 * This call blocks on the cancel RPC, which on a retryable server error retries
 * with backoff and can run far longer than one round-trip — bounded by the SEA
 * retry budget (the overall_timeout / max_retries defaults in the kernel's retry
 * config), which can reach minutes during a partial server outage. There is no
 * cancel-of-cancel or timeout parameter, so a host wiring this into a Ctrl-C /
 * query-timeout handler should call it from an abandonable/joinable thread rather
 * than the app's main thread.
 */
KernelStatusCode kernel_statement_canceller_cancel(
    kernel_statement_canceller_t* canceller, bool* dispatched);

/*
 * Free a canceller handle. Does not touch the originating statement. The
 * canceller is single-owner: freeing it while another thread is inside
 * kernel_statement_canceller_cancel on this handle is UNDEFINED BEHAVIOR
 * (use-after-free), not merely wasteful. That call blocks on the cancel RPC,
 * which on a retryable server error retries with backoff and can stay in flight
 * far longer than one round-trip (see _cancel), so join or otherwise quiesce any
 * cancelling thread BEFORE freeing.
 */
KernelStatusCode kernel_statement_canceller_free(
    kernel_statement_canceller_t* canceller);

/* ─── Cooperative cancellation (blocking-call cancel token) ────────────
 *
 * The statement canceller above cancels a server-side STATEMENT (right for a
 * query still executing). A cancel token cancels a blocking C-ABI CALL itself —
 * connect or a stalled result-stream fetch — neither of which the statement
 * canceller reaches (the server statement is already done; the stall is
 * client-side). It is the generic mechanism a host uses to honour a caller
 * deadline / context on the `*_cancellable` entry points.
 *
 * Usage: create a token, spawn a watcher on the caller's deadline/context, make
 * the blocking `*_cancellable` call on the owning thread, and from the watcher
 * fire kernel_cancel_token_cancel when the deadline elapses. The blocked call
 * drops its in-flight request future and returns KernelStatusCode_Cancelled.
 * Firing is purely local (flip an atomic, wake the waiter) — no RPC, never
 * blocks. The token is ONE-SHOT (a fired token stays fired), so create a fresh
 * one per logical operation. Safe to fire from a different thread than the
 * blocking call (that is the intended use); do NOT free the token until both
 * the blocking call and any in-flight cancel have returned.
 */
KernelStatusCode kernel_cancel_token_new(kernel_cancel_token_t** out);
KernelStatusCode kernel_cancel_token_cancel(kernel_cancel_token_t* token);
/* Report whether the token has been fired (writes true/false to *out). Local
 * atomic read — no RPC, never blocks. Symmetric with kernel_session_is_open. */
KernelStatusCode kernel_cancel_token_is_cancelled(const kernel_cancel_token_t* token, bool* out);
KernelStatusCode kernel_cancel_token_free(kernel_cancel_token_t* token);

/* ─── Executed statement (sync) ───────────────────────────────────────*/

/*
 * Obtain a result-stream handle that BORROWS `exec`. The stream is valid
 * only as long as `exec` is: close the stream
 * (`kernel_result_stream_close`) before closing `exec`, and keep the
 * parent statement open until the stream is drained/closed (see
 * `kernel_statement_close`). At most one stream per executed handle; a
 * second call returns `KernelStatusCode_InvalidArgument`. Returns
 * `KernelStatusCode_InvalidStatementHandle` if the parent statement was
 * re-executed or closed.
 */
KernelStatusCode kernel_executed_statement_get_result_stream(kernel_executed_statement_t* exec,
                                                             kernel_result_stream_t** out);

/*
 * Rows modified by a DML statement, or -1 if not applicable / unknown.
 * A -1 with NO stored last error means "not applicable"; a -1 after a
 * stored `InvalidStatementHandle` means the parent statement was
 * re-executed or closed (call `kernel_get_last_error` to distinguish).
 */
int64_t kernel_executed_statement_num_modified_rows(const kernel_executed_statement_t* exec);

/*
 * Server statement (query) id of a successfully executed statement, as a
 * NUL-terminated C string — the success-path counterpart to the query id on
 * KernelError. Used by a host driver for EXECUTE_STATEMENT telemetry and
 * query-history correlation. The pointer is BORROWED from the executed handle,
 * valid until kernel_executed_statement_close; do NOT free it, and copy it out
 * to outlive the handle. Returns NULL on a null / invalidated handle (with a
 * stored last error); a live handle from a successful execute never returns NULL.
 */
const char* kernel_executed_statement_query_id(const kernel_executed_statement_t* exec);

KernelStatusCode kernel_executed_statement_close(kernel_executed_statement_t* exec);

/* ─── Result stream ───────────────────────────────────────────────────
 *
 * The schema is exported once; each `next_batch` call exports one Arrow
 * array. End-of-stream is signalled by a RELEASED array (its `release`
 * callback is NULL) returned with `KernelStatusCode_Success`.
 */
KernelStatusCode kernel_result_stream_get_schema(kernel_result_stream_t* stream,
                                                 struct ArrowSchema* out);
KernelStatusCode kernel_result_stream_next_batch(kernel_result_stream_t* stream,
                                                 struct ArrowArray* out_array,
                                                 struct ArrowSchema* out_schema);
/*
 * Like kernel_result_stream_next_batch, but races the (blocking) fetch against
 * `cancel_token` so a host can interrupt a HUNG CloudFetch chunk (a wedged S3 /
 * pre-signed-URL GET) instead of blocking on it forever. A NULL `cancel_token`
 * behaves EXACTLY like kernel_result_stream_next_batch. If the token fires
 * before the batch is ready, the fetch future is dropped (aborting the in-flight
 * download) and this returns KernelStatusCode_Cancelled; the stream handle stays
 * valid and owned by the caller (close it with kernel_result_stream_close as
 * usual — a cancelled fetch neither consumes nor frees it). `cancel_token` is
 * borrowed and safe to fire from another thread while this call is in flight
 * (see kernel_session_open_cancellable).
 */
KernelStatusCode kernel_result_stream_next_batch_cancellable(
    kernel_result_stream_t* stream, struct ArrowArray* out_array,
    struct ArrowSchema* out_schema, const kernel_cancel_token_t* cancel_token);
KernelStatusCode kernel_result_stream_close(kernel_result_stream_t* stream);

/* ─── Metadata ────────────────────────────────────────────────────────
 *
 * Each returns a self-contained result stream (released via
 * `kernel_result_stream_close`) carrying the server-shaped
 * (JDBC-canonical) columns. Pattern args accept SQL LIKE wildcards;
 * identifier args are exact.
 *
 * NULL string args mean "unfiltered" for the list_* calls (catalogs /
 * schemas / tables / columns). The key/constraint calls are stricter:
 * `kernel_metadata_primary_keys` requires a non-NULL catalog, schema, AND
 * table (Databricks SHOW KEYS must target a fully-qualified table), and
 * `kernel_metadata_foreign_keys` requires the full foreign_* (referencing)
 * triple while the parent_* (referenced) triple is optional. Passing NULL
 * for a required identifier returns `KernelStatusCode_InvalidArgument`
 * with an explanatory last error (see each function's comment).
 */
KernelStatusCode kernel_metadata_list_catalogs(kernel_session_t* session,
                                               kernel_result_stream_t** out);
KernelStatusCode kernel_metadata_list_schemas(kernel_session_t* session, const char* catalog,
                                              const char* schema_pattern,
                                              kernel_result_stream_t** out);
KernelStatusCode kernel_metadata_list_tables(kernel_session_t* session, const char* catalog,
                                             const char* schema_pattern, const char* table_pattern,
                                             const char* table_types_csv,
                                             kernel_result_stream_t** out);
KernelStatusCode kernel_metadata_list_columns(kernel_session_t* session, const char* catalog,
                                              const char* schema_pattern, const char* table_pattern,
                                              const char* column_pattern,
                                              kernel_result_stream_t** out);
/* `catalog`, `schema`, and `table` are all REQUIRED (non-NULL): the
 * underlying SHOW KEYS targets a fully-qualified table. NULL for any of
 * them returns `KernelStatusCode_InvalidArgument`. */
KernelStatusCode kernel_metadata_primary_keys(kernel_session_t* session, const char* catalog,
                                              const char* schema, const char* table,
                                              kernel_result_stream_t** out);
/* The foreign_* (referencing) triple is REQUIRED (all non-NULL); the
 * parent_* (referenced) triple is optional and filters the referenced
 * side. NULL for any foreign_* arg returns
 * `KernelStatusCode_InvalidArgument`. */
KernelStatusCode kernel_metadata_foreign_keys(kernel_session_t* session, const char* parent_catalog,
                                              const char* parent_schema, const char* parent_table,
                                              const char* foreign_catalog,
                                              const char* foreign_schema, const char* foreign_table,
                                              kernel_result_stream_t** out);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* DATABRICKS_KERNEL_H */
