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

//! Translate Python auth kwargs into a kernel `AuthConfig`.
//!
//! Phase 1 ships PAT only. The kernel surface reserves
//! `AuthConfig::OAuthM2M`, `OAuthU2M`, `OAuthFederation`, and
//! `External`; wiring any of them into the Python wrapper needs a
//! deliberate design decision about credential ownership (does
//! PySQL own OAuth and feed tokens to the kernel, or does the
//! kernel run its own browser flow?). Until that's settled, the
//! connector backend raises `NotSupportedError` for non-PAT auth
//! and the wrapper rejects unknown `auth_type` values here so the
//! mistake surfaces at construction time, not deep inside an HTTP
//! retry loop.

use databricks_sql_kernel::AuthConfig;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Build a kernel `AuthConfig` from the Python kwargs the `Session`
/// constructor accepts.
///
/// `auth_type` is currently restricted to `"pat"`. Additional
/// variants land when the OAuth ownership question is resolved.
pub(crate) fn build_auth_config(
    auth_type: &str,
    access_token: Option<String>,
) -> PyResult<AuthConfig> {
    match auth_type {
        // Token presence/empty validation lives in the kernel's
        // `AuthConfig::pat` ctor so every binding rejects it
        // identically; map the kernel `InvalidArgument` message onto a
        // `PyValueError` to preserve this function's existing PyResult
        // contract.
        "pat" => AuthConfig::pat(access_token).map_err(|e| PyValueError::new_err(e.message)),
        other => Err(PyValueError::new_err(format!(
            "unsupported auth_type {other:?}; only 'pat' is supported today"
        ))),
    }
}
