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
//! Supports five `auth_type` values:
//!
//! - `"pat"` — personal access token (`access_token`).
//! - `"oauth-m2m"` — OAuth 2.0 client-credentials (service principal):
//!   `client_id` + `client_secret`, optional `scopes` / `token_url`.
//! - `"oauth-m2m-jwt"` — OAuth 2.0 client-credentials with a JWT
//!   private-key client assertion (RFC 7523): `client_id` +
//!   `jwt_key_file` + `jwt_kid`, optional `jwt_passphrase` /
//!   `jwt_algorithm` / `scopes` / `token_url`.
//! - `"oauth-u2m"` — OAuth 2.0 authorization-code + PKCE browser flow:
//!   optional `client_id` / `redirect_ports` / `scopes` / `token_url` /
//!   `oauth_callback_timeout_secs`.
//! - `"azure-sp-m2m"` — Azure Entra service-principal M2M:
//!   `azure_client_id` + `azure_client_secret`, optional
//!   `azure_tenant_id` (auto-discovered from the workspace when omitted).
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
    /// Candidate U2M redirect ports, tried in order (first free binds). A
    /// single fixed port is just a one-element list (`vec![port]`); a
    /// multi-port list gives the busy-port fallback the Thrift connector has
    /// across its registered ports (`databricks-sql-python` → the discrete
    /// set `8020`/`8025`, not a contiguous range).
    /// `None` → the databricks-sql-python default (8020) when the
    /// default client is in use, else the kernel default. At the pyo3
    /// boundary an explicit *empty* list is rejected (see
    /// [`build_auth_config`]), not defaulted.
    pub redirect_ports: Option<Vec<u16>>,
    pub callback_timeout_secs: Option<u64>,
    // JWT private-key M2M (`auth_type="oauth-m2m-jwt"`).
    pub jwt_key_file: Option<String>,
    pub jwt_kid: Option<String>,
    pub jwt_passphrase: Option<String>,
    pub jwt_algorithm: Option<String>,
    pub identity_federation_client_id: Option<String>,
    /// Azure Entra service-principal credentials (`azure-sp-m2m`). The kernel
    /// resolves the Entra endpoint / scope and (when `azure_tenant_id` is
    /// omitted) the tenant itself.
    pub azure_client_id: Option<String>,
    pub azure_client_secret: Option<String>,
    pub azure_tenant_id: Option<String>,
    pub azure_workspace_resource_id: Option<String>,
}

/// Default U2M OAuth client id for the **Python** binding.
///
/// Matches the standalone Thrift connector's `PYSQL_OAUTH_CLIENT_ID`
/// (`databricks-sql-python`) rather than the kernel's bare-Rust
/// fallback (`databricks-sql-connector`). Two reasons:
///
/// 1. **OAuth app identity** — the `use_kernel=True` path registers as
///    the same OAuth app the standalone Python connector does, so
///    admin-side consent screens / SSO policies attribute it
///    consistently (not as the generic CLI client).
/// 2. **Per-binding cache-key isolation** — the kernel's U2M token
///    cache is keyed on `{host, client_id, scopes}`. Distinct client
///    ids keep the pyo3 (Python) and napi (Node,
///    `databricks-sql-connector`) caches from colliding.
///
/// Note: this does NOT share a token-cache *entry* with the standalone
/// Thrift connector — the kernel writes to its own
/// `~/.config/databricks-sql-kernel/oauth/` directory, and the Thrift
/// connector caches U2M tokens in-memory only. The alignment is of the
/// OAuth app identity, not the on-disk cache.
const PYSQL_OAUTH_CLIENT_ID: &str = "databricks-sql-python";

/// Default U2M scopes for the **Python** binding when the caller relies
/// on the default `PYSQL_OAUTH_CLIENT_ID` above.
///
/// The Python binding overrides the U2M client to `databricks-sql-python`
/// (see `PYSQL_OAUTH_CLIENT_ID`), which is registered for `sql` (plus
/// `offline_access`) on the Databricks IdP. These scopes currently match the
/// kernel's own default (`sql offline_access`), but the binding pins them
/// explicitly so the `databricks-sql-python` bundle
/// (`{client_id, scopes, redirect_port}`) stays self-consistent and would not
/// silently break if the kernel default ever changed — a `databricks-sql-python`
/// authorization requesting `all-apis`, say, would fail with
/// `access_denied — Scopes 'all-apis' are not assigned to the client
/// databricks-sql-python`.
///
/// Matches `PYSQL_OAUTH_SCOPES` in the Thrift connector
/// (`databricks-sql-python` `auth/auth.py`), so `use_kernel=True` is
/// drop-in compatible with the standalone Thrift path.
///
/// Only applied when the caller passed neither `client_id` nor `scopes`
/// — a caller supplying their own `client_id` may legitimately want
/// different scopes, and an explicit `scopes` is always honoured verbatim.
const PYSQL_OAUTH_SCOPES: &[&str] = &["sql", "offline_access"];

/// Default U2M redirect port for the **Python** binding.
///
/// The kernel's intrinsic default is `8030` — the port registered for its
/// default `databricks-sql-connector` app. But the Python binding overrides
/// the client to `databricks-sql-python`, whose registered redirect port is
/// `8020` (the standalone connector's `PYSQL_OAUTH_REDIRECT_PORT_RANGE` starts
/// at 8020). `client_id`, `scopes`, and `redirect_port` are a coupled bundle —
/// overriding the client without pinning its port would inherit the kernel's
/// `8030` and fail the browser redirect with `redirect_uri_mismatch`.
///
/// Only applied when the caller relies on the default `client_id`; a caller
/// supplying their own `client_id` owns the matching port too.
const PYSQL_OAUTH_REDIRECT_PORT: u16 = 8020;

/// Build a kernel `AuthConfig` from the Python kwargs the `Session`
/// constructor accepts.
///
/// `auth_type` is one of `"pat"`, `"oauth-m2m"`, `"oauth-m2m-jwt"`,
/// `"oauth-u2m"`, `"azure-sp-m2m"`. Thin
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
            AuthConfig::pat(Some(token), oauth.identity_federation_client_id)
                .map_err(|err| err.to_string())
        }
        "oauth-m2m" => {
            // Client credentials grant: both halves are mandatory.
            let client_id = non_empty(oauth.client_id, "auth_type='oauth-m2m' requires client_id")?;
            let client_secret = non_empty(
                oauth.client_secret,
                "auth_type='oauth-m2m' requires client_secret",
            )?;
            AuthConfig::oauth_m2m(
                Some(client_id),
                Some(client_secret),
                oauth.scopes,
                oauth.token_url,
                oauth.identity_federation_client_id,
            )
            .map_err(|err| err.to_string())
        }
        "oauth-m2m-jwt" => {
            // JWT private-key client assertion: client_id + key file +
            // kid are mandatory; passphrase/algorithm are optional (the
            // kernel defaults the algorithm to RS256 and validates the
            // key/algorithm/passphrase at session open).
            let client_id = non_empty(
                oauth.client_id,
                "auth_type='oauth-m2m-jwt' requires client_id",
            )?;
            let jwt_key_file = non_empty(
                oauth.jwt_key_file,
                "auth_type='oauth-m2m-jwt' requires jwt_key_file",
            )?;
            let jwt_kid = non_empty(oauth.jwt_kid, "auth_type='oauth-m2m-jwt' requires jwt_kid")?;
            AuthConfig::oauth_m2m_private_key(
                Some(client_id),
                Some(jwt_key_file),
                Some(jwt_kid),
                oauth.jwt_passphrase,
                oauth.jwt_algorithm,
                oauth.scopes,
                oauth.token_url,
            )
            .map_err(|e| e.message)
        }
        "oauth-u2m" => {
            // Browser flow: every field is optional and `None` lets the
            // kernel apply its defaults (client_id "databricks-sql-connector",
            // redirect port 8030, scopes ["sql", "offline_access"],
            // callback timeout 120s). But a *present-yet-degenerate*
            // value is a footgun the kernel can't distinguish from an
            // intentional setting, so reject those here:
            //   - client_id="" would bypass the default and bind an
            //     empty client id;
            //   - an empty redirect_ports list, or a 0 entry (which binds an
            //     OS-assigned ephemeral port that can't match the IdP's
            //     static redirect_uri);
            //   - callback_timeout_secs=0 means "time out immediately",
            //     not "wait forever" (a common cross-system confusion).
            if matches!(oauth.client_id.as_deref(), Some("")) {
                return Err(
                    "auth_type='oauth-u2m' client_id must be non-empty when set; \
                     omit it to use the default 'databricks-sql-python'"
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
            // `redirect_ports` is the ordered list of candidate callback ports
            // (a single fixed port is just `[port]`). Reject the degenerate
            // shapes: an empty list carries no port, and a 0 entry binds an
            // ephemeral port that can't match the IdP's static redirect_uri.
            if let Some(ports) = oauth.redirect_ports.as_deref() {
                if ports.is_empty() {
                    return Err(
                        "auth_type='oauth-u2m' redirect_ports must be non-empty when set; \
                         omit it for the default"
                            .to_owned(),
                    );
                }
                if ports.contains(&0) {
                    return Err(
                        "auth_type='oauth-u2m' redirect_ports entries must be non-zero \
                         (each must be a static port the IdP redirect_uri can match)"
                            .to_owned(),
                    );
                }
            }
            // Default the U2M OAuth client to the Python connector's
            // own registered client (`databricks-sql-python`), NOT the
            // kernel's bare-Rust fallback (`databricks-sql-connector`). This
            // matches the standalone Thrift connector
            // (`PYSQL_OAUTH_CLIENT_ID` in databricks-sql-python's
            // auth.py) so a user gets the same OAuth app identity whether
            // they use the Thrift backend or `use_kernel=True`. It also
            // keeps the kernel's per-{host,client_id,scopes} cache key
            // distinct from the napi (Node) binding's, which defaults to
            // its own client.
            //
            // Note this does NOT share a token cache *entry* with the
            // standalone Thrift connector: the kernel writes to its own
            // `~/.config/databricks-sql-kernel/oauth/` directory, and the
            // Thrift connector caches U2M tokens in memory only. What is
            // aligned is the OAuth app identity, not the on-disk cache.
            //
            // When BOTH defaults are in play (caller passed no
            // `client_id` and no `scopes`), also default the scopes to
            // the Python-binding set so the default client/scopes pair
            // is internally consistent. The kernel's intrinsic scope
            // default is for its `databricks-sql-connector` client, not for
            // `databricks-sql-python` — see `PYSQL_OAUTH_SCOPES` above.
            let caller_set_client_id = oauth.client_id.is_some();
            let client_id = oauth
                .client_id
                .or_else(|| Some(PYSQL_OAUTH_CLIENT_ID.to_owned()));
            let scopes = match oauth.scopes {
                Some(s) => Some(s),
                None if !caller_set_client_id => {
                    Some(PYSQL_OAUTH_SCOPES.iter().map(|s| (*s).to_owned()).collect())
                }
                None => None,
            };
            // Pin the databricks-sql-python app's own redirect port when
            // relying on the default client_id — the kernel default (8030) is
            // for the databricks-sql-connector app and would mismatch. An
            // explicit caller list (e.g. the connector's registered range for
            // busy-port fallback) is used as-is; otherwise, under the default
            // client, forward the pinned databricks-sql-python port as a
            // one-element list. A custom client_id with no ports defers to the
            // kernel (None) — that app owns its own redirect port.
            let redirect_ports = match oauth.redirect_ports {
                Some(ports) => Some(ports),
                None if !caller_set_client_id => Some(vec![PYSQL_OAUTH_REDIRECT_PORT]),
                None => None,
            };
            Ok(AuthConfig::oauth_u2m(
                client_id,
                redirect_ports,
                scopes,
                oauth.token_url,
                oauth.callback_timeout_secs.map(Duration::from_secs),
                // `None` takes the kernel default: on-disk cache ENABLED,
                // encrypted under a machine-local derived key since this
                // binding exposes no passphrase option. That preserves
                // cross-connection SSO reuse (the point of the cache) without
                // ever writing the refresh token in the clear. Surfacing a
                // passphrase through `OAuthParams` — the equivalent of JDBC's
                // TokenCachePassPhrase — would strengthen it further.
                None,
                oauth.identity_federation_client_id,
            ))
        }
        "azure-sp-m2m" => {
            // Azure Entra service-principal M2M. The kernel owns Azure
            // resolution: it builds the Entra token endpoint + `.default`
            // scope and auto-discovers the tenant from the workspace when
            // `azure_tenant_id` is omitted. The binding forwards the Entra
            // credentials (+ optional tenant + optional workspace resource id
            // for the management-token resource-id header).
            AuthConfig::azure_service_principal(
                oauth.azure_client_id,
                oauth.azure_client_secret,
                oauth.azure_tenant_id,
                oauth.azure_workspace_resource_id,
                oauth.identity_federation_client_id,
            )
            .map_err(|err| err.to_string())
        }
        other => Err(format!(
            "unsupported auth_type {other:?}; expected one of 'pat', 'oauth-m2m', \
             'oauth-m2m-jwt', 'oauth-u2m', 'azure-sp-m2m'"
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
        let cfg = build_auth_config_inner(
            "pat",
            Some("dapi123".into()),
            OAuthParams {
                identity_federation_client_id: Some("federation-client".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(matches!(
            cfg,
            AuthConfig::Pat {
                token,
                identity_federation_client_id: Some(client_id),
            } if token == "dapi123" && client_id == "federation-client"
        ));
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
                identity_federation_client_id: Some("federation-client".into()),
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
                identity_federation_client_id,
            } => {
                assert_eq!(client_id, "id");
                assert_eq!(client_secret, "sec");
                assert_eq!(scopes, Some(vec!["all-apis".to_string()]));
                assert_eq!(token_url_override.as_deref(), Some("https://example/token"));
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client")
                );
            }
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    #[test]
    fn m2m_jwt_requires_client_id_key_file_and_kid() {
        // Missing client_id.
        let err = build_auth_config_inner(
            "oauth-m2m-jwt",
            None,
            OAuthParams {
                jwt_key_file: Some("/k.pem".into()),
                jwt_kid: Some("kid".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("client_id"), "{err}");

        // Missing key file.
        let err = build_auth_config_inner(
            "oauth-m2m-jwt",
            None,
            OAuthParams {
                client_id: Some("id".into()),
                jwt_kid: Some("kid".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("jwt_key_file"), "{err}");

        // Missing kid.
        let err = build_auth_config_inner(
            "oauth-m2m-jwt",
            None,
            OAuthParams {
                client_id: Some("id".into()),
                jwt_key_file: Some("/k.pem".into()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("jwt_kid"), "{err}");
    }

    #[test]
    fn m2m_jwt_builds_with_all_fields() {
        let cfg = build_auth_config_inner(
            "oauth-m2m-jwt",
            None,
            OAuthParams {
                client_id: Some("id".into()),
                jwt_key_file: Some("/k.pem".into()),
                jwt_kid: Some("kid".into()),
                jwt_passphrase: Some("pass".into()),
                jwt_algorithm: Some("ES256".into()),
                scopes: Some(vec!["all-apis".into()]),
                token_url: Some("https://example/token".into()),
                ..Default::default()
            },
        )
        .unwrap();
        match cfg {
            AuthConfig::OAuthM2MPrivateKey {
                client_id,
                jwt_key_file,
                jwt_kid,
                jwt_passphrase,
                jwt_algorithm,
                scopes,
                token_url_override,
            } => {
                assert_eq!(client_id, "id");
                assert_eq!(jwt_key_file, "/k.pem");
                assert_eq!(jwt_kid, "kid");
                assert_eq!(jwt_passphrase.as_deref(), Some("pass"));
                assert_eq!(jwt_algorithm.as_deref(), Some("ES256"));
                assert_eq!(scopes, Some(vec!["all-apis".to_string()]));
                assert_eq!(token_url_override.as_deref(), Some("https://example/token"));
            }
            other => panic!("expected OAuthM2MPrivateKey, got {other:?}"),
        }
    }

    #[test]
    fn u2m_defaults_client_id_and_scopes_to_python_connector() {
        // U2M takes no mandatory fields. The Python binding defaults the
        // OAuth client_id to `databricks-sql-python` (NOT the kernel's
        // default `databricks-sql-connector`) so it matches the standalone
        // Thrift connector and gets a cache key distinct from the napi
        // binding. It pins scopes to `["sql", "offline_access"]` for the
        // databricks-sql-python app; these match the kernel's default scopes
        // but are pinned explicitly so the bundle stays self-consistent.
        let cfg = build_auth_config_inner("oauth-u2m", None, OAuthParams::default()).unwrap();
        match cfg {
            AuthConfig::OAuthU2M {
                client_id,
                redirect_ports,
                scopes,
                token_url_override,
                callback_timeout,
                token_cache,
                identity_federation_client_id,
            } => {
                assert_eq!(client_id.as_deref(), Some("databricks-sql-python"));
                // Python bundle pins its own app's registered port (8020),
                // NOT the kernel default 8030 (databricks-sql-connector). With
                // no explicit range, the single pinned port is forwarded as a
                // one-element candidate list.
                assert_eq!(redirect_ports, Some(vec![8020]));
                assert_eq!(
                    scopes,
                    Some(vec!["sql".to_string(), "offline_access".to_string()])
                );
                assert!(token_url_override.is_none());
                assert!(callback_timeout.is_none());
                // Left as `None` so the kernel default applies: on-disk cache
                // enabled, encrypted under a machine-local derived key. Pinned
                // because overriding it here would silently drop the
                // cross-connection SSO reuse the cache exists to provide.
                assert!(
                    token_cache.is_none(),
                    "Python U2M should defer to the kernel's token-cache default"
                );
                assert!(identity_federation_client_id.is_none());
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_redirect_ports_are_forwarded() {
        // A caller-supplied port range (the connector's registered redirect
        // URIs) is carried through onto the config for busy-port fallback.
        let cfg = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                client_id: Some("databricks-sql-python".to_string()),
                redirect_ports: Some(vec![8020, 8021, 8022, 8023, 8024]),
                ..Default::default()
            },
        )
        .unwrap();
        match cfg {
            AuthConfig::OAuthU2M { redirect_ports, .. } => {
                assert_eq!(redirect_ports, Some(vec![8020, 8021, 8022, 8023, 8024]));
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_empty_redirect_ports_rejected() {
        let err = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                redirect_ports: Some(vec![]),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(
            err.contains("redirect_ports must be non-empty"),
            "got: {err}"
        );
    }

    #[test]
    fn u2m_zero_in_redirect_ports_rejected() {
        let err = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                redirect_ports: Some(vec![8020, 0]),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.contains("non-zero"), "got: {err}");
    }

    #[test]
    fn u2m_explicit_client_id_overrides_default() {
        let cfg = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                client_id: Some("my-custom-app".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        match cfg {
            AuthConfig::OAuthU2M {
                client_id,
                scopes,
                redirect_ports,
                ..
            } => {
                assert_eq!(client_id.as_deref(), Some("my-custom-app"));
                // Custom client_id without scopes/port: don't pin the Python
                // bundle's defaults — a custom app owns its own scopes and
                // redirect port, so both defer to the kernel (None).
                assert!(scopes.is_none());
                assert!(redirect_ports.is_none());
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn u2m_explicit_scopes_are_honoured_verbatim() {
        // Caller-supplied scopes win over the Python-binding default,
        // both with the default client_id and with a custom one.
        let custom = vec!["all-apis".to_string(), "offline_access".to_string()];
        let cfg = build_auth_config_inner(
            "oauth-u2m",
            None,
            OAuthParams {
                scopes: Some(custom.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        match cfg {
            AuthConfig::OAuthU2M {
                client_id, scopes, ..
            } => {
                assert_eq!(client_id.as_deref(), Some("databricks-sql-python"));
                assert_eq!(scopes, Some(custom));
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
                redirect_ports: Some(vec![9999]),
                callback_timeout_secs: Some(300),
                identity_federation_client_id: Some("federation-client".into()),
                ..Default::default()
            },
        )
        .unwrap();
        match cfg {
            AuthConfig::OAuthU2M {
                redirect_ports,
                callback_timeout,
                identity_federation_client_id,
                ..
            } => {
                // The single caller port is forwarded as a one-element list.
                assert_eq!(redirect_ports, Some(vec![9999]));
                assert_eq!(callback_timeout, Some(Duration::from_secs(300)));
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client")
                );
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
