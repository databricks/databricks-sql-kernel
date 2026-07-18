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
 * C ABI smoke test — log-callback SUCCESS path, in its own process.
 *
 * session_smoke.c installs kernel_init_logging first, so its callback install
 * only exercises the mutual-exclusion (Internal) path. There is exactly one
 * process-global tracing subscriber, so the success path can't also be reached
 * there. This separate binary calls kernel_set_log_callback WITHOUT any prior
 * kernel_init_logging, so the callback wins the subscriber: install must return
 * Success and kernel_logging_stats must report installed=true.
 *
 * It links the cdylib through the same C typedef (kernel_log_callback) and the
 * two logging symbols the header declares, so it catches header/cdylib drift in
 * the callback ABI — a signature or symbol mismatch fails to compile or link.
 * (Forcing a real event through the callback needs a kernel-side tracing emit,
 * which only the Rust integration test tests/v0_c_abi_log_callback.rs can do;
 * this binary proves the install/stats contract from a real C consumer.)
 *
 * Build / run (via `make c-abi-test`, which compiles every tests/c/*.c):
 *   cc -I include tests/c/log_callback_smoke.c -L target/debug \
 *      -ldatabricks_sql_kernel -o log_callback_smoke && ./log_callback_smoke
 */

#include <stdint.h>
#include <stdio.h>

#include "databricks_kernel.h"

static int failures = 0;

#define CHECK(cond, msg)                                            \
  do {                                                              \
    if (!(cond)) {                                                  \
      fprintf(stderr, "FAIL: %s (%s:%d)\n", msg, __FILE__, __LINE__); \
      failures++;                                                   \
    } else {                                                        \
      fprintf(stderr, "ok: %s\n", msg);                             \
    }                                                               \
  } while (0)

static int g_calls = 0;
static void count_cb(void* ctx, int level, const char* target, const char* message) {
  (void)level;
  (void)target;
  (void)message;
  int* counter = (int*)ctx;
  if (counter) {
    (*counter)++;
  }
}

int main(void) {
  /* ABI-version lockstep: the library compiled-in version must equal the
   * header this test built against (same gate as session_smoke.c). */
  CHECK(kernel_abi_version() == DATABRICKS_KERNEL_ABI_VERSION,
        "kernel_abi_version() matches DATABRICKS_KERNEL_ABI_VERSION");

  /* A NULL callback is still rejected up front. */
  CHECK(kernel_set_log_callback(NULL, NULL, NULL) == KernelStatusCode_InvalidArgument,
        "set_log_callback(NULL cb) → InvalidArgument");

  /* Fresh process, no kernel_init_logging: the callback wins the one
   * process-global subscriber, so this MUST succeed (not the Internal
   * mutual-exclusion outcome session_smoke.c sees). */
  KernelStatusCode rc = kernel_set_log_callback(count_cb, &g_calls, "TRACE");
  CHECK(rc == KernelStatusCode_Success,
        "set_log_callback installs in a fresh process (no init_logging) → Success");

  bool installed = false;
  uint64_t dropped = 1;
  kernel_logging_stats(&installed, &dropped);
  CHECK(installed, "logging_stats reports installed=true after a successful install");
  CHECK(dropped == 0, "a healthy fresh install has dropped no records");

  /* Re-registering is last-writer-wins and still succeeds (NULL level defers to
   * the current threshold). */
  rc = kernel_set_log_callback(count_cb, &g_calls, NULL);
  CHECK(rc == KernelStatusCode_Success, "re-registering the callback → Success");

  /* NULL out-params must be tolerated. */
  kernel_logging_stats(NULL, NULL);
  CHECK(true, "logging_stats(NULL, NULL) is safe");

  if (failures == 0) {
    fprintf(stderr, "\nAll C ABI log-callback smoke checks passed.\n");
    return 0;
  }
  fprintf(stderr, "\n%d C ABI log-callback smoke check(s) FAILED.\n", failures);
  return 1;
}
