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

use std::process::Command;

/// Capture the compiler version at build time and expose it to the crate as
/// `RUSTC_VERSION`, so `option_env!("RUSTC_VERSION")` in the telemetry runtime
/// can report the rustc version that built the kernel. Cargo/rustc do not set
/// this variable themselves, so without this build script the field is always
/// absent.
fn main() {
    // Cargo sets `RUSTC` to the compiler it is driving; fall back to `rustc`
    // on PATH if it is somehow unset.
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());

    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if let Some(version) = version {
        // e.g. "rustc 1.79.0 (129f3b996 2024-06-10)"
        println!("cargo:rustc-env=RUSTC_VERSION={version}");
    }

    println!("cargo:rerun-if-changed=build.rs");
}
