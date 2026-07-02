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
 * C ABI smoke test — links the kernel cdylib and exercises the parts of
 * the surface that need no live warehouse: config lifecycle, null-handle
 * guards, and the error-reporting path. A connection error against an
 * unreachable host proves the open path marshals config, drives the
 * runtime, and routes a typed error back through `kernel_get_last_error`.
 *
 * Build / run (see the CMake/CI wiring in A6):
 *   cc -I include tests/c/session_smoke.c -L target/debug \
 *      -ldatabricks_sql_kernel -o session_smoke && ./session_smoke
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "databricks_kernel.h"

static int failures = 0;

#define CHECK(cond, msg)                                       \
  do {                                                         \
    if (!(cond)) {                                             \
      fprintf(stderr, "FAIL: %s (%s:%d)\n", msg, __FILE__, __LINE__); \
      failures++;                                              \
    } else {                                                   \
      fprintf(stderr, "ok: %s\n", msg);                        \
    }                                                          \
  } while (0)

/* Config new → set warehouse + PAT → free, with no open. */
static void test_config_lifecycle(void) {
  KernelSessionConfig* config = NULL;
  KernelStatusCode rc = kernel_session_config_new(&config);
  CHECK(rc == KernelStatusCode_Success, "config_new succeeds");
  CHECK(config != NULL, "config_new writes a non-null handle");

  rc = kernel_session_config_set_warehouse(config, "https://example.databricks.com", "wh123");
  CHECK(rc == KernelStatusCode_Success, "set_warehouse succeeds");

  rc = kernel_session_config_set_auth_pat(config, "dapi-not-a-real-token");
  CHECK(rc == KernelStatusCode_Success, "set_auth_pat succeeds");

  rc = kernel_session_config_set_session_conf(config, "ANSI_MODE", "true");
  CHECK(rc == KernelStatusCode_Success, "set_session_conf succeeds");

  kernel_session_config_free(config);
  CHECK(1, "config_free does not crash");
}

/* Null handles must return InvalidArgument, not crash. */
static void test_null_guards(void) {
  KernelStatusCode rc = kernel_session_config_new(NULL);
  CHECK(rc == KernelStatusCode_InvalidArgument, "config_new(NULL) → InvalidArgument");

  rc = kernel_session_close(NULL);
  CHECK(rc == KernelStatusCode_InvalidArgument, "session_close(NULL) → InvalidArgument");

  rc = kernel_statement_close(NULL);
  CHECK(rc == KernelStatusCode_InvalidArgument, "statement_close(NULL) → InvalidArgument");

  rc = kernel_result_stream_close(NULL);
  CHECK(rc == KernelStatusCode_InvalidArgument, "result_stream_close(NULL) → InvalidArgument");

  KernelError err;
  memset(&err, 0, sizeof(err));
  bool present = kernel_get_last_error(&err);
  CHECK(present, "last error is populated after a null-guard failure");
  CHECK(err.code == KernelStatusCode_InvalidArgument, "last error code is InvalidArgument");
  CHECK(err.message != NULL && strlen(err.message) > 0, "last error carries a message");
}

/* Missing connection / auth → the open path reports InvalidArgument. */
static void test_open_requires_connection_and_auth(void) {
  KernelSessionConfig* config = NULL;
  (void)kernel_session_config_new(&config);
  /* No connection, no auth set. */
  kernel_session_t* session = NULL;
  KernelStatusCode rc = kernel_session_open(config, &session);
  CHECK(rc == KernelStatusCode_InvalidArgument, "open without connection → InvalidArgument");
  CHECK(session == NULL, "session stays null on failed open");

  KernelError err;
  memset(&err, 0, sizeof(err));
  bool present = kernel_get_last_error(&err);
  CHECK(present, "open failure populates last error");
}

/* Open against an unreachable host → a transport/connection error code
 * (NOT Success), with the error surfaced via kernel_get_last_error. The
 * exact code depends on DNS/connect behaviour; we assert only that it
 * failed and produced a populated error, so the test stays hermetic. */
static void test_open_unreachable_host_errors(void) {
  KernelSessionConfig* config = NULL;
  (void)kernel_session_config_new(&config);
  /* RFC 5737 TEST-NET-1: guaranteed non-routable. */
  (void)kernel_session_config_set_warehouse(config, "https://192.0.2.1", "wh123");
  (void)kernel_session_config_set_auth_pat(config, "dapi-not-a-real-token");

  kernel_session_t* session = NULL;
  KernelStatusCode rc = kernel_session_open(config, &session);
  CHECK(rc != KernelStatusCode_Success, "open against unreachable host fails");
  CHECK(session == NULL, "session stays null on failed open");

  KernelError err;
  memset(&err, 0, sizeof(err));
  bool present = kernel_get_last_error(&err);
  CHECK(present, "unreachable-host open populates last error");
  CHECK(err.message != NULL, "error has a message");
}

/* http_path + M2M setters accept valid input. */
static void test_http_path_and_m2m_setters(void) {
  KernelSessionConfig* config = NULL;
  (void)kernel_session_config_new(&config);
  KernelStatusCode rc =
      kernel_session_config_set_http_path(config, "https://example.databricks.com",
                                          "/sql/1.0/warehouses/abc123");
  CHECK(rc == KernelStatusCode_Success, "set_http_path succeeds");
  rc = kernel_session_config_set_auth_m2m(config, "client-id", "client-secret");
  CHECK(rc == KernelStatusCode_Success, "set_auth_m2m succeeds");
  kernel_session_config_free(config);
}

/* A null required string argument to a setter → InvalidArgument. */
static void test_setter_null_arg_rejected(void) {
  KernelSessionConfig* config = NULL;
  (void)kernel_session_config_new(&config);
  KernelStatusCode rc = kernel_session_config_set_auth_pat(config, NULL);
  CHECK(rc == KernelStatusCode_InvalidArgument, "set_auth_pat(NULL token) → InvalidArgument");
  kernel_session_config_free(config);
}

/* Proxy + TLS setters accept valid input. */
static void test_proxy_and_tls_setters(void) {
  KernelSessionConfig* config = NULL;
  (void)kernel_session_config_new(&config);
  KernelStatusCode rc =
      kernel_session_config_set_proxy(config, "http://localhost:3128", "u", "p", "localhost");
  CHECK(rc == KernelStatusCode_Success, "set_proxy succeeds");
  rc = kernel_session_config_set_proxy(config, NULL, NULL, NULL, NULL);
  CHECK(rc == KernelStatusCode_InvalidArgument, "set_proxy(NULL url) → InvalidArgument");

  const unsigned char pem[] = "-----BEGIN CERTIFICATE-----\n";
  rc = kernel_session_config_set_tls_trusted_certs(config, pem, sizeof(pem) - 1);
  CHECK(rc == KernelStatusCode_Success, "set_tls_trusted_certs succeeds");
  rc = kernel_session_config_set_tls_allow_self_signed(config, true);
  CHECK(rc == KernelStatusCode_Success, "set_tls_allow_self_signed succeeds");
  rc = kernel_session_config_set_tls_skip_hostname_verification(config, true);
  CHECK(rc == KernelStatusCode_Success, "set_tls_skip_hostname_verification succeeds");
  kernel_session_config_free(config);
}

/* The async submit path is a deferred stub → InvalidArgument, no network. */
static void test_submit_stub_rejects(void) {
  kernel_executed_async_statement_t* out = NULL;
  /* A null statement is fine here — the stub rejects before touching it. */
  KernelStatusCode rc = kernel_statement_submit(NULL, &out);
  CHECK(rc == KernelStatusCode_InvalidArgument, "submit stub → InvalidArgument");
}

/* bind_* on a null statement handle → InvalidArgument, no crash. */
static void test_bind_null_handle_rejected(void) {
  KernelStatusCode rc = kernel_statement_bind_int64(NULL, 1, 42);
  CHECK(rc == KernelStatusCode_InvalidArgument, "bind_int64(NULL stmt) → InvalidArgument");
  rc = kernel_statement_bind_null(NULL, 1);
  CHECK(rc == KernelStatusCode_InvalidArgument, "bind_null(NULL stmt) → InvalidArgument");
  rc = kernel_statement_set_sql(NULL, "SELECT 1");
  CHECK(rc == KernelStatusCode_InvalidArgument, "set_sql(NULL stmt) → InvalidArgument");
}

int main(void) {
  test_config_lifecycle();
  test_null_guards();
  test_open_requires_connection_and_auth();
  test_open_unreachable_host_errors();
  test_http_path_and_m2m_setters();
  test_setter_null_arg_rejected();
  test_proxy_and_tls_setters();
  test_submit_stub_rejects();
  test_bind_null_handle_rejected();

  if (failures == 0) {
    fprintf(stderr, "\nAll C ABI smoke checks passed.\n");
    return 0;
  }
  fprintf(stderr, "\n%d C ABI smoke check(s) FAILED.\n", failures);
  return 1;
}
