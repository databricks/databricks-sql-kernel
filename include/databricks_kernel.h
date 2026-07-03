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
 *   kernel's process-wide runtime — but does not wait for completion. The
 *   delete is initiated even if statement / executed handles from this
 *   session are still open (they may keep running until you close them;
 *   close them first for orderly teardown). A process that exits
 *   immediately after `kernel_session_close` may drop the detached task
 *   before it runs, leaving the server session to expire on its own idle
 *   timeout.
 */

#ifndef DATABRICKS_KERNEL_H
#define DATABRICKS_KERNEL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

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

/* ─── Session config ──────────────────────────────────────────────────
 *
 * Build a config, set exactly one connection form and exactly one auth
 * form, then hand it to `kernel_session_open` (which consumes it). Only
 * PAT and OAuth M2M auth are exposed over the C ABI in v0; OAuth U2M
 * (interactive browser flow) is Rust-crate-only.
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

/* Add (or overwrite) one server-bound session-conf entry. */
KernelStatusCode kernel_session_config_set_session_conf(KernelSessionConfig* config,
                                                        const char* key,
                                                        const char* value);

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

/* ─── Session lifecycle ───────────────────────────────────────────────*/

/*
 * Open a session. CONSUMES `config` on both success and failure — do not
 * use or free `config` afterwards. On success, `*out` holds a session
 * handle released with `kernel_session_close`.
 */
KernelStatusCode kernel_session_open(KernelSessionConfig* config, kernel_session_t** out);
KernelStatusCode kernel_session_close(kernel_session_t* session);

/* Construct a new mutable statement bound to this session. */
KernelStatusCode kernel_session_new_statement(kernel_session_t* session,
                                              kernel_statement_t** out);

/* ─── Statement ───────────────────────────────────────────────────────*/

KernelStatusCode kernel_statement_set_sql(kernel_statement_t* stmt, const char* sql);

/* Positional parameter binding (1-based ordinal). */
KernelStatusCode kernel_statement_bind_null(kernel_statement_t* stmt, size_t ordinal);
KernelStatusCode kernel_statement_bind_int64(kernel_statement_t* stmt, size_t ordinal,
                                             int64_t value);
KernelStatusCode kernel_statement_bind_double(kernel_statement_t* stmt, size_t ordinal,
                                              double value);
KernelStatusCode kernel_statement_bind_bool(kernel_statement_t* stmt, size_t ordinal, bool value);
KernelStatusCode kernel_statement_bind_string(kernel_statement_t* stmt, size_t ordinal,
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
