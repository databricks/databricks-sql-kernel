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
/// `DATABRICKS_TEST_CONFIG_FILE`. Keys: `host`, `http_path`, `access_token`.
///
/// The var UNSET (the normal-CI / local case) ⇒ an empty map, so every fixture
/// falls through to its env var unchanged. But when the var IS set (the
/// engineer-bot path) and the file is unreadable or malformed, we PANIC with a
/// specific message rather than silently returning an empty map: otherwise
/// `live_session` would later panic with the misleading `"DATABRICKS_TOKEN
/// required"`, as if no config were provided, making a misprovisioned run hard to
/// diagnose. Var-set-and-file-OK is the success path.
fn config_file() -> &'static HashMap<String, String> {
    static CONFIG: OnceLock<HashMap<String, String>> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let path = match env::var("DATABRICKS_TEST_CONFIG_FILE") {
            Ok(p) if !p.is_empty() => p,
            _ => return HashMap::new(),
        };
        let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("DATABRICKS_TEST_CONFIG_FILE is set to {path:?} but the file could not be read: {e}")
        });
        serde_json::from_str::<HashMap<String, String>>(&contents).unwrap_or_else(|e| {
            panic!("DATABRICKS_TEST_CONFIG_FILE {path:?} is not valid JSON of {{host, http_path, access_token}}: {e}")
        })
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
///
/// Empty values are filtered on BOTH paths, symmetrically — an empty env var and
/// an empty JSON field both resolve to `None`. This keeps a blank/misprovisioned
/// secret (e.g. a `jq`-written `"access_token": ""` from an empty CI secret) from
/// silently becoming `AuthConfig::Pat { token: "" }` and a confusing server-side
/// auth failure; instead `live_session` panics with the clear `… required`
/// message, the same as the env-absent case.
pub fn env_var(key: &str) -> Option<String> {
    if let Ok(v) = env::var(key) {
        if !v.is_empty() {
            return Some(v);
        }
    }
    config_key_for(key)
        .and_then(|k| config_file().get(k))
        .filter(|v| !v.is_empty())
        .cloned()
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
