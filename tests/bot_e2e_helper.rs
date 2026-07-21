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

//! Offline coverage for the shared E2E connection helper (`tests/common`).
//!
//! Two jobs: (1) force Cargo to COMPILE `tests/common/mod.rs` in ordinary CI
//! (`cargo test`), so an API mismatch in `common::live_session` / `env_var` is
//! caught here rather than only when the engineer-bot first uses it; (2) prove the
//! config-file fallback that lets the bot's scrubbed `cargo test` still
//! authenticate (`DATABRICKS_TEST_CONFIG_FILE` → `access_token`). No network.

mod common;

use std::io::Write;

// ⚠️ SINGLE-TEST INVARIANT — do NOT add a second `#[test]` to this binary.
// The test below mutates process-global env (`set_var`/`remove_var` on
// `DATABRICKS_TOKEN`/`HOST`/`HTTP_PATH`/`TEST_CONFIG_FILE`) and depends on owning
// the first read that `config_file()`'s `OnceLock` caches. Rust runs a binary's
// tests on parallel threads by default, so a second test here would race on both
// the shared env and the one-shot cache and flake. If you need more offline
// coverage of `tests/common`, add it in a SEPARATE integration-test file (its own
// binary → its own process → its own env + `OnceLock`), or serialize with
// `serial_test` before adding one here.

/// End-to-end fallback: with `DATABRICKS_TOKEN` absent from the env (as under the
/// bot's scrub) but a config file present, `common::env_var("DATABRICKS_TOKEN")`
/// resolves from the file. This is the only test in this binary, so it owns the
/// first read that `config_file()`'s `OnceLock` caches.
#[test]
fn token_falls_back_to_config_file_when_env_absent() {
    let mut f = tempfile::NamedTempFile::new().expect("create temp config");
    write!(
        f,
        r#"{{"host":"h.example","http_path":"/sql/1.0/x","access_token":"dapi-from-file"}}"#
    )
    .expect("write temp config");
    let path = f.path().to_str().expect("temp path is utf-8").to_owned();

    // Reproduce the scrubbed-subprocess condition: the connection env vars are
    // absent, and the details come from the file the bot wrote. Clear all three so
    // the test is hermetic regardless of the CI runner's environment. Edition 2021,
    // so set_var/remove_var are safe; this runs before any read of the OnceLock cache.
    std::env::remove_var("DATABRICKS_TOKEN");
    std::env::remove_var("DATABRICKS_HOST");
    std::env::remove_var("DATABRICKS_HTTP_PATH");
    std::env::set_var("DATABRICKS_TEST_CONFIG_FILE", &path);

    assert_eq!(
        common::env_var("DATABRICKS_TOKEN").as_deref(),
        Some("dapi-from-file"),
        "token must resolve from DATABRICKS_TEST_CONFIG_FILE when the env var is stripped"
    );
    // host/http_path are not credential-shaped; the file supplies them here too
    // since the env is unset in this test.
    assert_eq!(
        common::env_var("DATABRICKS_HOST").as_deref(),
        Some("h.example")
    );
    assert_eq!(
        common::env_var("DATABRICKS_HTTP_PATH").as_deref(),
        Some("/sql/1.0/x")
    );

    // Env wins over the file: set the host explicitly and a FRESH read prefers it.
    // (config_file() is cached, but env_var checks the environment first.)
    std::env::set_var("DATABRICKS_HOST", "env-host");
    assert_eq!(
        common::env_var("DATABRICKS_HOST").as_deref(),
        Some("env-host")
    );
}
