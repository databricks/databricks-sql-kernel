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
//! Supports three `auth_type` values:
//!
//! - `"pat"` — personal access token (`access_token`).
//! - `"oauth-m2m"` — OAuth 2.0 client-credentials (service principal):
//!   `client_id` + `client_secret`, optional `scopes` / `token_url`.
//! - `"oauth-u2m"` — OAuth 2.0 authorization-code + PKCE browser flow:
//!   optional `client_id` / `redirect_port` / `scopes` / `token_url` /
//!   `oauth_callback_timeout_secs`.
//!
//! OAuth is **kernel-owned**: the kernel acquires and refreshes tokens
//! itself (OIDC discovery against the workspace host, transparent
//! refresh for long-running sessions). The Python wrapper only forwards
//! the credentials / flow parameters.
//!
//! `AuthConfig::OAuthFederation` and `External` remain unwired — the
//! wrapper rejects unknown `auth_type` values here so the mistake
//! surfaces at construction time, not deep inside an HTTP retry loop.

use std::time::Duration;

use databricks_sql_kernel::AuthConfig;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// OAuth parameters forwarded from the Python `Session` constructor.
///
/// All fields are optional at the Python boundary; per-`auth_type`
/// validation (e.g. M2M requires `client_id` + `client_secret`) happens
/// in [`build_auth_config`]. Grouping them in one struct keeps
/// [`build_auth_config`]'s signature stable as OAuth knobs grow.
#[derive(Default)]
pub(crate) struct OAuthParams {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub scopes: Option<Vec<String>>,
    pub token_url: Option<String>,
    pub redirect_port: Option<u16>,
    pub callback_timeout_secs: Option<u64>,
}

/// Build a kernel `AuthConfig` from the Python kwargs the `Session`
/// constructor accepts.
///
/// `auth_type` is one of `"pat"`, `"oauth-m2m"`, `"oauth-u2m"`. Thin
/// wrapper over [`build_auth_config_inner`] that maps its plain-`String`
/// validation error onto a `PyValueError`. Keeping the logic in the
/// inner fn lets the unit tests assert on the error text without an
/// initialized Python interpreter (formatting a `PyErr` needs the GIL).
pub(crate) fn build_auth_config(
    auth_type: &str,
    access_token: Option<String>,
    oauth: OAuthParams,
) -> PyResult<AuthConfig> {
    build_auth_config_inner(auth_type, access_token, oauth).map_err(PyValueError::new_err)
}

/// Pure (Python-free) core of [`build_auth_config`]. Returns the
/// validation error as a `String` so it's unit-testable without the
/// GIL.
fn build_auth_config_inner(
    auth_type: &str,
    access_token: Option<String>,
    oauth: OAuthParams,
) -> Result<AuthConfig, String> {
    match auth_type {
        "pat" => {
            let token =
                access_token.ok_or_else(|| "auth_type='pat' requires access_token".to_owned())?;
            if token.is_empty() {
                return Err("access_token must not be empty".to_owned());
            }
            Ok(AuthConfig::Pat { token })
        }
        "oauth-m2m" => {
            // Client credentials grant: both halves are mandatory.
            let client_id = non_empty(oauth.client_id, "auth_type='oauth-m2m' requires client_id")?;
            let client_secret = non_empty(
                oauth.client_secret,
                "auth_type='oauth-m2m' requires client_secret",
            )?;
            Ok(AuthConfig::OAuthM2M {
                client_id,
                client_secret,
                scopes: oauth.scopes,
                token_url_override: oauth.token_url,
            })
        }
        "oauth-u2m" => {
            // Browser flow: every field is optional and `None` lets the
            // kernel apply its defaults (client_id "databricks-cli",
            // redirect_port 8020, scopes ["all-apis", "offline_access"],
            // callback timeout 120s). But a *present-yet-degenerate*
            // value is a footgun the kernel can't distinguish from an
            // intentional setting, so reject those here:
            //   - client_id="" would bypass the default and bind an
            //     empty client id;
            //   - redirect_port=0 binds an OS-assigned ephemeral port,
            //     which can't match the IdP's static redirect_uri;
            //   - callback_timeout_secs=0 means "time out immediately",
            //     not "wait forever" (a common cross-system confusion).
            if matches!(oauth.client_id.as_deref(), Some("")) {
                return Err(
                    "auth_type='oauth-u2m' client_id must be non-empty when set; \
                     omit it to use the default 'databricks-cli'"
                        .to_owned(),
                );
            }
            if oauth.redirect_port == Some(0) {
                return Err(
                    "auth_type='oauth-u2m' redirect_port must be non-zero when set \
                     (a static port the IdP redirect_uri can match); omit it for the default"
                        .to_owned(),
                );
            }
            if oauth.callback_timeout_secs == Some(0) {
                return Err(
                    "auth_type='oauth-u2m' oauth_callback_timeout_secs must be > 0 when set \
                     (0 would time out the browser flow immediately); omit it for the default"
                        .to_owned(),
                );
            }
            Ok(AuthConfig::OAuthU2M {
                client_id: oauth.client_id,
                redirect_port: oauth.redirect_port,
                scopes: oauth.scopes,
                token_url_override: oauth.token_url,
                callback_timeout: oauth.callback_timeout_secs.map(Duration::from_secs),
            })
        }
        other => Err(format!(
            "unsupported auth_type {other:?}; expected one of 'pat', 'oauth-m2m', 'oauth-u2m'"
        )),
    }
}

/// Unwrap an `Option<String>` that must be present and non-empty,
/// mapping both the missing and the empty case to the same error
/// message.
fn non_empty(value: Option<String>, msg: &str) -> Result<String, String> {
    match value {
        Some(s) if !s.is_empty() => Ok(s),
        _ => Err(msg.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pat_requires_token() {
        let err = build_auth_config_inner("pat", None, OAuthParams::default()).unwrap_err();
        assert!(err.contains("requires access_token"));
        let err = build_auth_config_inner("pat", Some(String::new()), OAuthParams::default())
            .unwrap_err();
        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn pat_builds() {
        let cfg =
            build_auth_config_inner("pat", Some("dapi123".into()), OAuthParams::default()).unwrap();
        assert!(matches!(cfg, AuthConfig::Pat { token } if token == "dapi123"));
    }

    #[test]
    fn m2m_requires_client_id_and_secret() {
        // No client_id.
        let err = build_auth_config_inner(
            "oauth-m2m",
            None,
            OAuthParams {
                client_secret: Some("sec".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("client_id"));

        // client_id but no secret.
        let err = build_auth_config_inner(
            "oauth-m2m",
            None,
            OAuthParams {
                client_id: Some("id".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("client_secret"));

        // Empty client_id rejected like a missing one.
        let err = build_auth_config_inner(
            "oauth-m2m",
            None,
            OAuthParams {
                client_id: Some(String::new()),
                client_secret: Some("sec".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("client_id"));
    }

    #[test]
    fn m2m_builds_with_optional_fields() {
        let cfg = build_auth_config_inner(
            "oauth-m2m",
            None,
            OAuthParams {
                client_id: Some("id".into()),
                client_secret: Some("sec".into()),
                scopes: Some(vec!["all-apis".into()]),
                token_url: Some("https://example/token".into()),
                ..Default::default()
            },
        )
        .unwrap();
        match cfg {
            AuthConfig::OAuthM2M {
                client_id,
                client_secret,
                scopes,
                token_url_override,
            } => {
                assert_eq!(client_id, "id");
                assert_eq!(client_secret, "sec");
                assert_eq!(scopes, Some(vec!["all-apis".to_string()]));
                assert_eq!(token_url_override.as_deref(), Some("https://example/token"));
            }
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_builds_with_all_defaults_none() {
        // U2M takes no mandatory fields; the kernel fills defaults.
        let cfg = build_auth_config_inner("oauth-u2m", None, OAuthParams::default()).unwrap();
        match cfg {
            AuthConfig::OAuthU2M {
                client_id,
                redirect_port,
                scopes,
                token_url_override,
                callback_timeout,
            } => {
                assert!(client_id.is_none());
                assert!(redirect_port.is_none());
                assert!(scopes.is_none());
                assert!(token_url_override.is_none());
                assert!(callback_timeout.is_none());
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_maps_callback_timeout_secs_to_duration() {
        let cfg = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                redirect_port: Some(9999),
                callback_timeout_secs: Some(300),
                ..Default::default()
            },
        )
        .unwrap();
        match cfg {
            AuthConfig::OAuthU2M {
                redirect_port,
                callback_timeout,
                ..
            } => {
                assert_eq!(redirect_port, Some(9999));
                assert_eq!(callback_timeout, Some(Duration::from_secs(300)));
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_rejects_empty_client_id() {
        // Some("") would bypass the kernel's None-only default and bind
        // an empty client id.
        let err = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                client_id: Some(String::new()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("client_id must be non-empty"), "{err}");
    }

    #[test]
    fn u2m_rejects_zero_redirect_port() {
        let err = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                redirect_port: Some(0),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("redirect_port must be non-zero"), "{err}");
    }

    #[test]
    fn u2m_rejects_zero_callback_timeout() {
        let err = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                callback_timeout_secs: Some(0),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(
            err.contains("oauth_callback_timeout_secs must be > 0"),
            "{err}"
        );
    }

    #[test]
    fn unknown_auth_type_rejected() {
        let err = build_auth_config_inner("kerberos", None, OAuthParams::default()).unwrap_err();
        assert!(err.contains("unsupported auth_type"));
    }
}
