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

//! Shared E2E test helpers.
//!
//! A test crate includes this with `mod common;` (the file is `tests/common/mod.rs`
//! so Cargo does NOT treat it as its own integration-test target). It provides the
//! same `env_var` / `live_session` the per-file e2e tests use, plus a
//! **config-file fallback** for the connection secrets.
//!
//! Why the fallback exists: the engineer-bot (databricks-bot-engine) runs the e2e
//! suite inside an agent-driven subprocess whose environment has every
//! credential-shaped variable — anything matching `*TOKEN*` / `*SECRET*` etc. —
//! stripped for safety (the engine's `shared/env_scrub.py`). `DATABRICKS_TOKEN` is
//! therefore removed before `cargo test` starts, so the token can't be passed as a
//! plain env var. The bot instead writes the connection details (token included) to
//! a JSON file and points at it with `DATABRICKS_TEST_CONFIG_FILE` — a name the
//! scrub deliberately preserves. `env_var` falls back to that file's fields.
//!
//! Normal CI (`rust-e2e.yml`) and local runs leave `DATABRICKS_TEST_CONFIG_FILE`
//! unset, so `env_var` resolves purely from the environment, unchanged.

#![allow(dead_code)]

use databricks_sql_kernel::{AuthConfig, Session};
use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

/// Connection details loaded once from the JSON file named by
/// `DATABRICKS_TEST_CONFIG_FILE` (or an empty map when unset/unreadable/invalid).
/// Keys: `host`, `http_path`, `access_token`.
fn config_file() -> &'static HashMap<String, String> {
    static CONFIG: OnceLock<HashMap<String, String>> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let path = match env::var("DATABRICKS_TEST_CONFIG_FILE") {
            Ok(p) if !p.is_empty() => p,
            _ => return HashMap::new(),
        };
        let contents = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return HashMap::new(),
        };
        serde_json::from_str::<HashMap<String, String>>(&contents).unwrap_or_default()
    })
}

/// Map an env-var name to the matching key in the config-file JSON.
fn config_key_for(env_key: &str) -> Option<&'static str> {
    match env_key {
        "DATABRICKS_HOST" => Some("host"),
        "DATABRICKS_HTTP_PATH" => Some("http_path"),
        "DATABRICKS_TOKEN" => Some("access_token"),
        _ => None,
    }
}

/// Read a connection value: the environment wins; if absent (e.g. the token was
/// stripped by the engine's env scrub), fall back to the config file.
pub fn env_var(key: &str) -> Option<String> {
    if let Ok(v) = env::var(key) {
        if !v.is_empty() {
            return Some(v);
        }
    }
    config_key_for(key).and_then(|k| config_file().get(k).cloned())
}

/// Open a `Session` against the live workspace using PAT auth, resolving the
/// connection from the environment with a config-file fallback (see module docs).
pub async fn live_session() -> Session {
    let host = env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST required");
    let path = env_var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required");
    let token = env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN required");

    Session::builder()
        .http_path(host, path)
        .auth(AuthConfig::Pat { token })
        .open()
        .await
        .expect("Session::open against live workspace")
}
