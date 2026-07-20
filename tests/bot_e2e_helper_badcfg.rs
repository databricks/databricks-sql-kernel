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

//! A set-but-malformed `DATABRICKS_TEST_CONFIG_FILE` must panic with a SPECIFIC
//! message (not silently degrade to the misleading "DATABRICKS_TOKEN required").
//! Own binary for a fresh `OnceLock`. Complements `bot_e2e_helper_empty.rs`
//! (empty field) and `bot_e2e_helper.rs` (valid file).

mod common;

use std::io::Write;

#[test]
#[should_panic(expected = "not valid JSON")]
fn malformed_config_file_panics_with_clear_message() {
    let mut f = tempfile::NamedTempFile::new().expect("create temp config");
    write!(f, "this is not json").expect("write temp config");
    let path = f.path().to_str().expect("temp path is utf-8").to_owned();

    std::env::remove_var("DATABRICKS_TOKEN");
    std::env::remove_var("DATABRICKS_HOST");
    std::env::remove_var("DATABRICKS_HTTP_PATH");
    std::env::set_var("DATABRICKS_TEST_CONFIG_FILE", &path);

    // Reading any connection value triggers the config-file load, which must panic
    // with the distinct "not valid JSON" message rather than resolve to None.
    let _ = common::env_var("DATABRICKS_TOKEN");
}
