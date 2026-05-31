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

//! `databricks-sea-native` — napi-rs binding crate for the Databricks
//! SQL Node.js driver's SEA (Statement Execution API) path.
//!
//! Surface: `openSession()` (free function) → `Connection` (opaque
//! class with `executeStatement` / `close` / `sessionId`) →
//! `Statement` (opaque class with `fetchNextBatch` / `schema` /
//! `cancel` / `close` / `statementId`). Results cross the FFI as
//! Arrow IPC bytes (`ArrowBatch` / `ArrowSchema` POJOs in
//! `result.rs`); the TS adapter decodes them via `apache-arrow`.

#![deny(unsafe_op_in_unsafe_fn)]
// Skeleton-PR state: every `pub(crate)` item in this crate (error
// envelope helpers, tokio Handle accessor, panic-guarded wrapper) is
// authored against the contract used by the follow-up napi-classes PR
// (#25). Until that PR lands, the lib-only build has zero production
// callers and every helper looks dead. Tests in `#[cfg(test)] mod
// tests` exercise them, which is why `cargo test` is green even
// while `cargo clippy --lib -- -D warnings` would otherwise reject
// the build.
//
// We narrow the suppression to non-test builds so the lint still
// fires on genuinely-unused test code.
#![cfg_attr(not(test), allow(dead_code))]

#[macro_use]
extern crate napi_derive;

pub(crate) mod connection;
pub(crate) mod database;
pub(crate) mod error;
pub(crate) mod logger;
pub(crate) mod metadata;
pub(crate) mod result;
pub(crate) mod runtime;
pub(crate) mod statement;
pub(crate) mod util;

/// Returns the native binding's crate version (`CARGO_PKG_VERSION`).
///
/// Originally the round-1b smoke test; kept as a cheap "is the binding
/// loaded?" probe for the JS-side loader's structured diagnostics.
#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_matches_cargo_pkg_version() {
        // Doubles as the "binding loaded" smoke contract — keep this
        // even when the function is otherwise inert.
        assert_eq!(version(), env!("CARGO_PKG_VERSION"));
        assert!(!version().is_empty());
    }
}
