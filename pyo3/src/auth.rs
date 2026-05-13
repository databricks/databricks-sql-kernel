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

//! Translation from Python auth kwargs to a kernel `AuthConfig`.
//!
//! Phase-1 surface covers PAT (Step 1) and `External` (this step).
//! `External` lets the connector keep ownership of its own OAuth /
//! MSAL / federation flows: it passes a Python callable that returns
//! a fresh bearer token, and the kernel calls it whenever it needs
//! one.
//!
//! Future steps add the kernel-driven OAuth variants
//! (`AuthConfig::OAuthM2M`, `OAuthU2M`, `OAuthFederation`) — useful
//! for callers that want the kernel to own the flow rather than
//! delegating back to Python.

use async_trait::async_trait;
use databricks_sql_kernel::config::TokenProvider;
use databricks_sql_kernel::{AuthConfig, Error, Result as KResult};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// A `TokenProvider` that delegates to a Python callable. The
/// callable is invoked once per HTTP request that needs auth, with
/// the GIL re-acquired on the calling tokio worker thread.
///
/// `Py<PyAny>` is `Send + Sync` in pyo3 (it's a reference-counted
/// pointer into the Python heap, not a borrow), so this struct
/// satisfies the kernel's `TokenProvider: Send + Sync` bound.
struct PythonTokenProvider {
    callback: Py<PyAny>,
}

impl PythonTokenProvider {
    fn new(callback: Py<PyAny>) -> Self {
        Self { callback }
    }
}

#[async_trait]
impl TokenProvider for PythonTokenProvider {
    async fn token(&self) -> KResult<String> {
        // The kernel calls us from a tokio worker without the GIL.
        // Acquire it for the duration of one Python call, extract
        // the result as a String, then drop the GIL again before
        // returning the future to the kernel.
        //
        // Note: this is a *sync* GIL-held call to the user's Python
        // callable. For typical auth providers (in-process token
        // cache lookup, occasional refresh) this is fine and matches
        // the connector's own auth-provider contract. If a wrapper
        // ever needs to await Python coroutines here, the pattern
        // is to make the callback return a token directly and own
        // the async work itself.
        Python::attach(|py| {
            let result = self.callback.call0(py).map_err(|e| {
                Error::unauthenticated().message(format!("Python token_callback raised: {e}"))
            })?;
            let token: String = result.extract(py).map_err(|e| {
                Error::unauthenticated()
                    .message(format!("Python token_callback must return str: {e}"))
            })?;
            if token.is_empty() {
                return Err(Error::unauthenticated()
                    .message("Python token_callback returned an empty string"));
            }
            Ok(token)
        })
    }
}

/// Build a kernel `AuthConfig` from the Python kwargs the `Session`
/// constructor accepts. Centralised here so the auth-kwarg surface
/// stays in one place.
///
/// `auth_type` is one of:
/// - `"pat"` — requires `access_token: str`.
/// - `"external"` — requires `token_callback: Callable[[], str]`.
pub(crate) fn build_auth_config(
    auth_type: &str,
    access_token: Option<String>,
    token_callback: Option<Py<PyAny>>,
) -> PyResult<AuthConfig> {
    match auth_type {
        "pat" => {
            let token = access_token
                .ok_or_else(|| PyValueError::new_err("auth_type='pat' requires access_token"))?;
            if token.is_empty() {
                return Err(PyValueError::new_err("access_token must not be empty"));
            }
            Ok(AuthConfig::Pat { token })
        }
        "external" => {
            let callback = token_callback.ok_or_else(|| {
                PyValueError::new_err(
                    "auth_type='external' requires token_callback (a no-arg callable returning str)",
                )
            })?;
            // Sanity check at construction time so a misspelled or
            // non-callable arg surfaces immediately rather than on
            // the first HTTP request.
            Python::attach(|py| -> PyResult<()> {
                if !callback.bind(py).is_callable() {
                    return Err(PyValueError::new_err("token_callback must be callable"));
                }
                Ok(())
            })?;
            Ok(AuthConfig::External(Box::new(PythonTokenProvider::new(
                callback,
            ))))
        }
        other => Err(PyValueError::new_err(format!(
            "unknown auth_type {other:?}; expected one of: pat, external"
        ))),
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end coverage for the Python→Rust→kernel auth bridge.
    //!
    //! These tests exercise the `TokenProvider` trampoline without
    //! needing a live workspace — we construct a `PythonTokenProvider`
    //! directly, run its async `token()` method on a tokio runtime,
    //! and check the value flows back correctly. This is the layer
    //! the rest of Step 5 builds on; if it breaks we want to catch
    //! it without spinning a Session against dogfood.
    //!
    //! `Python::initialize()` initialises the Python interpreter on
    //! first call; subsequent calls are no-ops.

    use super::*;
    use databricks_sql_kernel::ErrorCode;
    use pyo3::ffi::c_str;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn python_token_provider_returns_callback_value() {
        Python::initialize();
        let callback: Py<PyAny> = Python::attach(|py| {
            py.eval(c_str!("lambda: 'token-xyz'"), None, None)
                .unwrap()
                .unbind()
        });
        let provider = PythonTokenProvider::new(callback);
        let got = rt().block_on(provider.token()).unwrap();
        assert_eq!(got, "token-xyz");
    }

    #[test]
    fn python_token_provider_maps_python_exception_to_unauthenticated() {
        Python::initialize();
        let callback: Py<PyAny> = Python::attach(|py| {
            // Lambda raising RuntimeError. Done as exec+lookup so we
            // can carry a statement (`raise ...`) instead of an
            // expression.
            let globals = pyo3::types::PyDict::new(py);
            py.run(
                c_str!("def cb():\n    raise RuntimeError('simulated MSAL failure')\n"),
                Some(&globals),
                None,
            )
            .unwrap();
            globals.get_item("cb").unwrap().unwrap().unbind()
        });
        let provider = PythonTokenProvider::new(callback);
        let err = rt().block_on(provider.token()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Unauthenticated);
        assert!(
            err.message.contains("simulated MSAL failure"),
            "message should preserve the Python exception text: {}",
            err.message
        );
    }

    #[test]
    fn python_token_provider_rejects_non_string_return() {
        Python::initialize();
        let callback: Py<PyAny> = Python::attach(|py| {
            py.eval(c_str!("lambda: 12345"), None, None)
                .unwrap()
                .unbind()
        });
        let provider = PythonTokenProvider::new(callback);
        let err = rt().block_on(provider.token()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Unauthenticated);
        assert!(err.message.contains("must return str"), "{}", err.message);
    }

    #[test]
    fn python_token_provider_rejects_empty_string() {
        Python::initialize();
        let callback: Py<PyAny> =
            Python::attach(|py| py.eval(c_str!("lambda: ''"), None, None).unwrap().unbind());
        let provider = PythonTokenProvider::new(callback);
        let err = rt().block_on(provider.token()).unwrap_err();
        assert_eq!(err.code, ErrorCode::Unauthenticated);
        assert!(err.message.contains("empty"), "{}", err.message);
    }
}
