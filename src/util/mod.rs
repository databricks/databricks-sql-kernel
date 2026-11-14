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

//! Internal kernel utilities shared across modules.

pub(crate) mod runtime;

/// PROTOTYPE ONLY (kernel-cpu A/B): env-gated levers, read once per process.
#[allow(dead_code)]
pub(crate) mod kcpu {
    use std::sync::OnceLock;

    fn flag(slot: &'static OnceLock<bool>, name: &str) -> bool {
        *slot.get_or_init(|| std::env::var(name).map(|v| v == "1").unwrap_or(false))
    }

    macro_rules! gate {
        ($fn:ident, $env:literal) => {
            pub(crate) fn $fn() -> bool {
                static S: OnceLock<bool> = OnceLock::new();
                flag(&S, $env)
            }
        };
    }

    gate!(skip_close, "KCPU_SKIP_CLOSE");
    gate!(early_close, "KCPU_EARLY_CLOSE");
    gate!(no_reqwest_retry, "KCPU_NO_REQWEST_RETRY");
    gate!(lean_url, "KCPU_LEAN_URL");
    gate!(lean_exec, "KCPU_LEAN_EXEC");
    gate!(discard_fast, "KCPU_DISCARD_FAST");
    gate!(auth_cache, "KCPU_AUTH_CACHE");
}
