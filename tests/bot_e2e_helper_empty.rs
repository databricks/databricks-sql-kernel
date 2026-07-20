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

//! Empty-value symmetry for the config-file fallback (its own binary so
//! `config_file()`'s `OnceLock` loads THIS empty-field file, not the populated
//! one in `bot_e2e_helper.rs`). A blank/misprovisioned secret must resolve to
//! `None` (→ clear `… required` panic) rather than `Some("")` (→ confusing
//! server-side auth failure) — the review finding this covers.

mod common;

use std::io::Write;

#[test]
fn empty_config_field_resolves_to_none() {
    let mut f = tempfile::NamedTempFile::new().expect("create temp config");
    // access_token deliberately empty — models an empty CI secret that jq wrote as "".
    write!(
        f,
        r#"{{"host":"h.example","http_path":"/sql/1.0/x","access_token":""}}"#
    )
    .expect("write temp config");
    let path = f.path().to_str().expect("temp path is utf-8").to_owned();

    std::env::remove_var("DATABRICKS_TOKEN");
    std::env::set_var("DATABRICKS_TEST_CONFIG_FILE", &path);

    // Empty file field is filtered exactly like an empty env var ⇒ None.
    assert_eq!(
        common::env_var("DATABRICKS_TOKEN"),
        None,
        "an empty access_token in the config file must resolve to None, not Some(\"\")"
    );
    // A populated field in the same file still resolves normally.
    assert_eq!(
        common::env_var("DATABRICKS_HOST").as_deref(),
        Some("h.example")
    );
}
