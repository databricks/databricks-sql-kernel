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

int main(void) {
  test_config_lifecycle();
  test_null_guards();
  test_open_requires_connection_and_auth();
  test_open_unreachable_host_errors();

  if (failures == 0) {
    fprintf(stderr, "\nAll C ABI smoke checks passed.\n");
    return 0;
  }
  fprintf(stderr, "\n%d C ABI smoke check(s) FAILED.\n", failures);
  return 1;
}
