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

//! Typed configuration for the v0 Kernel API.
//!
//! Per `docs/spec-v0.md` "Public Rust API → Session → Config", config is
//! grouped by concern (one sub-struct per concern), not flattened into a
//! string bag. Wrappers translate connection-string / DSN attributes into
//! these types and call `SessionBuilder` setters one per group.
//!
//! `SessionConfig` itself is the assembled object the C ABI accepts
//! directly and that `Session::open()` consumes.
//!
//! ## Scope of v0
//!
//! The v0 spec ships **lean**. Only configs we have a concrete shape for
//! land with fields:
//!
//! - [`ConnectionConfig`] (host + warehouse_id; required)
//! - [`AuthConfig`] (PAT + OAuth variants; required)
//! - [`DefaultOpts`] (catalog/schema)
//! - [`HttpConfig`] (timeouts, pool size)
//! - [`ResultConfig`] (CloudFetch tuning today; future result-management
//!   knobs slot in here too)
//! - [`LoggingConfig`] (filter, file)
//! - [`TransportMode`]
//!
//! [`TlsConfig`] now carries custom-CA + self-signed / hostname
//! relaxation knobs; configs whose shape we are still settling stay
//! as empty `#[non_exhaustive]` structs so the public type names
//! exist (and `SessionBuilder` setters compile) without committing
//! to fields:
//!
//! - [`ProxyConfig`]
//! - [`TelemetryConfig`]
//!
//! All structs are `#[non_exhaustive]` so v1+ can grow new fields
//! without breaking the public API.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::telemetry::DriverSystemConfiguration;

// ─── Connection identity ────────────────────────────────────────────────────

/// Required: identifies the workspace and warehouse to connect to.
///
/// `#[non_exhaustive]` — construct via [`ConnectionConfig::new`] or
/// [`ConnectionConfig::from_http_path`]. External callers can read
/// fields directly but cannot use a struct literal.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Databricks workspace URL, e.g. `https://my-workspace.databricks.com`.
    pub host: String,

    /// Warehouse ID, e.g. `abc123`. Wrappers that have an `http_path`
    /// string (`/sql/1.0/warehouses/abc123`) should use
    /// [`ConnectionConfig::from_http_path`] which parses the id out.
    pub warehouse_id: String,

    /// Workspace organization ID for multi-tenant routing. When set,
    /// the kernel sends it as the `x-databricks-org-id` header on
    /// every request. Populated automatically from the `?o=` query
    /// parameter when constructing via
    /// [`ConnectionConfig::from_http_path`].
    pub org_id: Option<String>,
}

impl ConnectionConfig {
    /// Construct a connection config from the workspace host and a
    /// bare warehouse id (no `/sql/1.0/...` prefix).
    ///
    /// `host` is normalised: `https://` is prepended if no scheme is
    /// present, and any trailing `/` is stripped. So `"my-workspace.databricks.com"`,
    /// `"my-workspace.databricks.com/"`, and
    /// `"https://my-workspace.databricks.com"` all produce the same
    /// canonical `host`.
    pub fn new(host: impl Into<String>, warehouse_id: impl Into<String>) -> Self {
        Self {
            host: normalise_host(host.into()),
            warehouse_id: warehouse_id.into(),
            org_id: None,
        }
    }

    /// Construct a connection config from a workspace host and a
    /// JDBC-style HTTP path. Accepts:
    ///
    /// - `/sql/1.0/warehouses/{id}` (canonical form)
    /// - `/sql/1.0/endpoints/{id}` (legacy form, equivalent on the server)
    /// - With or without leading `/`
    /// - With an optional `?o={org_id}` query parameter (multi-tenant
    ///   workspaces); the org id is captured into [`Self::org_id`].
    ///
    /// `host` is normalised the same way as in
    /// [`ConnectionConfig::new`]. Returns
    /// `Err(ErrorCode::InvalidArgument)` if the path doesn't match
    /// any of the recognised forms.
    pub fn from_http_path(
        host: impl Into<String>,
        http_path: &str,
    ) -> crate::kernel_error::Result<Self> {
        let warehouse_id = parse_warehouse_id(http_path).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument().message(format!(
                "http_path '{http_path}' does not match \
                 /sql/1.0/warehouses/{{id}} or /sql/1.0/endpoints/{{id}}"
            ))
        })?;
        Ok(Self {
            host: normalise_host(host.into()),
            warehouse_id,
            org_id: parse_org_id(http_path),
        })
    }
}

/// Normalise a workspace host: prepend `https://` if no scheme is
/// present, and strip any trailing `/`. Done eagerly at config
/// construction so the rest of the kernel never has to second-guess
/// the input shape.
fn normalise_host(host: String) -> String {
    let with_scheme = if host.starts_with("http://") || host.starts_with("https://") {
        host
    } else {
        format!("https://{host}")
    };
    with_scheme.trim_end_matches('/').to_string()
}

/// Parse a warehouse id out of a JDBC-style HTTP path. Accepts the
/// `/sql/1.0/warehouses/{id}` and `/sql/1.0/endpoints/{id}` forms,
/// with or without a leading slash, with an optional `?o={org_id}`
/// query suffix that is stripped from the returned id.
fn parse_warehouse_id(http_path: &str) -> Option<String> {
    http_path
        .strip_prefix("/sql/1.0/warehouses/")
        .or_else(|| http_path.strip_prefix("sql/1.0/warehouses/"))
        .or_else(|| http_path.strip_prefix("/sql/1.0/endpoints/"))
        .or_else(|| http_path.strip_prefix("sql/1.0/endpoints/"))
        .map(|s| {
            // Strip trailing `/` and any query string.
            let s = s.trim_end_matches('/');
            s.split('?').next().unwrap_or(s).to_string()
        })
        .filter(|s| !s.is_empty())
}

/// Parse the `?o={org_id}` query parameter if present. Empty values
/// (`?o=`) are treated as absent.
fn parse_org_id(http_path: &str) -> Option<String> {
    let query = http_path.split('?').nth(1)?;
    query
        .split('&')
        .find_map(|param| param.strip_prefix("o="))
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
}

/// Optional: per-session defaults applied to statements that don't
/// override.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DefaultOpts {
    pub catalog: Option<String>,
    pub schema: Option<String>,

    /// When `true` (the default), transaction-control operations are
    /// silently no-oped: `Session::set_autocommit(false)`,
    /// `Session::commit()`, and `Session::rollback()` return `Ok(())`
    /// without issuing any server statement. Mirrors the reference/JDBC
    /// `IgnoreTransactions` connection property, whose default is `1`
    /// (transactions disabled). Set to `false` to opt into real
    /// multi-statement transactions (a Private Preview feature; requires
    /// catalog-managed tables server-side).
    pub ignore_transactions: bool,
}

impl Default for DefaultOpts {
    fn default() -> Self {
        Self {
            catalog: None,
            schema: None,
            // Match the reference/JDBC default of IgnoreTransactions=1:
            // transaction control is no-oped unless a caller opts in.
            ignore_transactions: true,
        }
    }
}

// ─── Token cache ────────────────────────────────────────────────────────────

/// Policy for the on-disk U2M OAuth token cache.
///
/// The cache persists the refresh token so a user is not sent through the
/// browser flow on every connection. Because a refresh token is a
/// long-lived credential, the kernel only ever writes one **encrypted** —
/// there is no plaintext mode.
///
/// | `enabled` | `passphrase` | result |
/// |---|---|---|
/// | `true` (default) | set | encrypted under the caller's passphrase |
/// | `true` (default) | unset | encrypted under a machine-local derived key |
/// | `false` | either | no disk reads or writes |
///
/// **Why the middle row is not an error.** The reference ODBC driver ships
/// `EnableTokenCache=1` with `TokenCachePassPhrase` empty and still
/// connects, so failing the connection there would break drop-in
/// compatibility for any DSN that never set a passphrase. (The JDBC driver
/// *does* fail in that case — `ClientConfigurator` throws "No token cache
/// passphrase configured" — but for the ODBC surface the reference driver
/// is the compatibility bar, and its behaviour is also the friendlier of
/// the two.) A supplied passphrase is still strictly better: see
/// [`super::auth::oauth::cache_crypto::derived_passphrase`] for exactly what
/// the fallback key protects against and what it does not.
///
/// Disabling the cache stops **disk persistence only**. The in-memory token
/// store still reuses and refreshes a token for the lifetime of the
/// session, matching JDBC's `NoOpTokenCache`, so turning this off does not
/// re-trigger the browser flow on every refresh within one process.
#[derive(Debug, Clone)]
pub struct TokenCacheConfig {
    /// Whether to read and write the on-disk cache. Default `true`,
    /// matching both the reference ODBC driver's and JDBC's
    /// `EnableTokenCache=1`.
    pub enabled: bool,
    /// Passphrase for the AES-256 encryption key. Optional: when absent (or
    /// blank) the kernel derives a machine-local key instead, matching the
    /// reference driver, which caches with an empty `TokenCachePassPhrase`.
    /// Supplying one is stronger — the cache then cannot be read without a
    /// secret the machine does not hold.
    pub passphrase: Option<String>,
}

impl Default for TokenCacheConfig {
    fn default() -> Self {
        Self {
            // Match the reference driver's / JDBC's EnableTokenCache=1.
            enabled: true,
            passphrase: None,
        }
    }
}

impl TokenCacheConfig {
    /// Resolve to the passphrase to encrypt with, or `None` when disk
    /// caching is disabled.
    ///
    /// A blank passphrase is treated as absent rather than honoured: it is
    /// almost always an unexpanded config placeholder (`$TOKEN_PASS` that
    /// never got substituted), and deriving a key from `""` across every
    /// machine would be materially weaker than the machine-local fallback.
    ///
    /// Infallible by design. Enabled-without-passphrase is a supported
    /// configuration, not an error, so this cannot fail the connection —
    /// see the type docs for why.
    pub(crate) fn resolve(&self) -> Option<std::borrow::Cow<'_, str>> {
        if !self.enabled {
            return None;
        }
        match self.passphrase.as_deref() {
            Some(p) if !p.trim().is_empty() => Some(std::borrow::Cow::Borrowed(p)),
            _ => Some(std::borrow::Cow::Owned(
                crate::auth::oauth::cache_crypto::derived_passphrase(),
            )),
        }
    }
}

// ─── Authentication ─────────────────────────────────────────────────────────

/// Authentication strategy.
///
/// `#[non_exhaustive]` so v1+ can add variants (e.g. an Azure-AD
/// service-principal shortcut) without breaking wrappers.
///
/// Optional fields default to sensible values when set to `None`:
/// - OAuth scopes default to `["all-apis"]` for M2M and
///   `["sql", "offline_access"]` for U2M.
/// - U2M `redirect_ports` defaults to the `[8030, 8031, 8032, 8033, 8034]` window.
/// - U2M `client_id` defaults to `"databricks-sql-connector"`.
/// - `token_url_override` defaults to OIDC discovery against the
///   workspace host.
#[non_exhaustive]
pub enum AuthConfig {
    /// Personal access token (`dapi…`).
    Pat {
        token: String,
        /// SP-wide Workload Identity Federation client id. `None` selects
        /// BYOT / account-wide WIF behavior when token exchange is needed.
        identity_federation_client_id: Option<String>,
    },

    /// OAuth 2.0 machine-to-machine (client credentials grant).
    /// Used for service-principal authentication.
    OAuthM2M {
        client_id: String,
        client_secret: String,
        /// OAuth scopes to request. `None` → `["all-apis"]`.
        scopes: Option<Vec<String>>,
        /// Token endpoint override. `None` → discover via OIDC at
        /// `<host>/.well-known/oauth-authorization-server`.
        token_url_override: Option<String>,
        /// SP-wide Workload Identity Federation client id. `None` selects
        /// BYOT / account-wide WIF behavior when token exchange is needed.
        identity_federation_client_id: Option<String>,
    },

    /// OAuth 2.0 user-to-machine (authorization code + PKCE, browser
    /// flow). The kernel spins up a localhost listener on the first
    /// free `redirect_ports` candidate, opens the user's browser to
    /// the workspace's authorize endpoint, and exchanges the returned
    /// code for a token.
    OAuthU2M {
        /// OAuth client ID. `None` → `"databricks-sql-connector"` (the
        /// default OAuth app, shared by the Node and Go drivers).
        client_id: Option<String>,
        /// Candidate localhost callback ports, tried in order; the first
        /// one free is bound (busy-port fallback). `None`/empty → the
        /// `[8030, 8031, 8032, 8033, 8034]` default window (the
        /// `databricks-sql-connector` app accepts any loopback port).
        /// A single fixed port is just a one-element list (`vec![p]`) —
        /// which is how the single-port bindings (C ABI, N-API) forward
        /// their one port. Every entry must be a registered redirect URI
        /// of the OAuth app; a multi-port list mirrors the Thrift
        /// connector's registered redirect ports (`databricks-sql-python`
        /// → the discrete set `8020`/`8025`, not a contiguous range) for
        /// busy-port fallback.
        redirect_ports: Option<Vec<u16>>,
        /// OAuth scopes. `None` → `["sql", "offline_access"]`
        /// (the `offline_access` scope is required to receive a
        /// refresh token, which the kernel caches and refreshes
        /// transparently).
        scopes: Option<Vec<String>>,
        /// Token endpoint override. `None` → OIDC discovery.
        token_url_override: Option<String>,
        /// How long to wait for the user to complete the browser
        /// flow before timing out. `None` → 120 seconds.
        callback_timeout: Option<std::time::Duration>,
        /// On-disk token-cache policy. `None` → [`TokenCacheConfig::default`]
        /// (enabled, which then requires a passphrase — see that type).
        token_cache: Option<TokenCacheConfig>,
        /// SP-wide Workload Identity Federation client id. `None` selects
        /// BYOT / account-wide WIF behavior when token exchange is needed.
        identity_federation_client_id: Option<String>,
    },

    /// Azure Entra ID **service-principal** machine-to-machine auth — the
    /// `azure-sp-m2m` flow. Client-credentials against Entra directly
    /// (`{login_host}/{tenant}/oauth2/v2.0/token`) with the Azure app
    /// credentials and the `{app_id}/.default` scope. The kernel resolves the
    /// endpoint, scope, and (when `azure_tenant_id` is omitted) the tenant
    /// itself — so bindings forward only the credentials. The `Authorization`
    /// bearer is the Databricks-audience data token, which authenticates the
    /// common workspace-member SP on its own. The Azure Resource Manager
    /// management-token add-on (`X-Databricks-Azure-SP-Management-Token` +
    /// `X-Databricks-Azure-Workspace-Resource-Id`) is emitted only when
    /// `azure_workspace_resource_id` is set — matching the JDBC connector
    /// (`if (resourceId != null)`) — so an SP with an Azure RBAC role but no
    /// workspace membership can authenticate without forcing the extra ARM
    /// token exchange on every other SP.
    AzureServicePrincipal {
        /// Entra app-registration client id.
        azure_client_id: String,
        /// Entra app-registration client secret.
        azure_client_secret: String,
        /// Entra tenant id. `None` → auto-discovered from the workspace's
        /// `/aad/auth` redirect (matching the Thrift connector).
        azure_tenant_id: Option<String>,
        /// Azure workspace ARM resource id. When set, the kernel fetches the
        /// ARM management token and emits it together with the
        /// `X-Databricks-Azure-Workspace-Resource-Id` header, so the control
        /// plane can resolve the SP's Azure RBAC role against this specific
        /// workspace resource. When unset (the default), neither header is
        /// sent and the data token authenticates alone.
        azure_workspace_resource_id: Option<String>,
        /// SP-wide Workload Identity Federation client id, forwarded to the
        /// federation wrapper.
        identity_federation_client_id: Option<String>,
    },

    /// OAuth 2.0 machine-to-machine using a JWT private-key client
    /// assertion (RFC 7523) instead of a client secret. The kernel signs
    /// a short-lived JWT with the configured private key and sends it as
    /// the `client_assertion` in the client-credentials grant; the
    /// authorization server verifies it against the service principal's
    /// registered public key.
    ///
    /// Supported `jwt_algorithm` values: `RS256`/`384`/`512`,
    /// `PS256`/`384`/`512`, `ES256`, `ES384` (`None` → `RS256`). `ES512`
    /// is not supported.
    OAuthM2MPrivateKey {
        client_id: String,
        /// Path to the PEM-encoded private key file.
        jwt_key_file: String,
        /// Key id written into the JWT header (`kid`) so the server can
        /// select the matching registered public key.
        jwt_kid: String,
        /// Passphrase for an encrypted PKCS#8 key. `None` for an
        /// unencrypted key.
        jwt_passphrase: Option<String>,
        /// JWT signing algorithm name. `None` → `RS256`.
        jwt_algorithm: Option<String>,
        /// OAuth scopes to request. `None` → `["all-apis"]`.
        scopes: Option<Vec<String>>,
        /// Token endpoint override. `None` → discover via OIDC.
        token_url_override: Option<String>,
    },

    /// OAuth 2.0 token exchange (RFC 8693), for federated identity
    /// (e.g. exchanging a Workload Identity token for a Databricks
    /// access token).
    ///
    /// **Not yet implemented** in v0 — reserved with the final field
    /// shape so wrappers can compile against it. Calling
    /// [`Session::open`](crate::Session::open) with this variant
    /// returns `ErrorCode::InvalidArgument` until the implementation
    /// lands.
    OAuthFederation {
        subject_token: String,
        subject_token_type: String,
        audience: String,
    },

    /// Caller-supplied token provider for custom auth flows. The
    /// kernel calls [`TokenProvider::token`] whenever it needs an
    /// Authorization header.
    External(Box<dyn TokenProvider>),
}

impl AuthConfig {
    /// Construct a [`AuthConfig::Pat`] from an optional token, validating
    /// that it is present and non-empty.
    ///
    /// Bindings forward the raw optional from their host language; the
    /// required-field check lives here so every wrapper rejects a
    /// missing/empty token identically (`ErrorCode::InvalidArgument`)
    /// instead of each re-implementing it at the binding boundary. The
    /// optional federation client id is forwarded unchanged.
    pub fn pat(
        token: Option<String>,
        identity_federation_client_id: Option<String>,
    ) -> crate::Result<Self> {
        match token {
            Some(token) if !token.is_empty() => Ok(AuthConfig::Pat {
                token,
                identity_federation_client_id,
            }),
            _ => Err(crate::kernel_error::Error::invalid_argument()
                .message("PAT auth requires a non-empty access token")),
        }
    }

    /// Construct a [`AuthConfig::OAuthM2M`] from optional fields,
    /// validating that the required `client_id` / `client_secret` are
    /// present and non-empty. `scopes` / `token_url_override` are
    /// optional and forwarded as-is (the kernel fills defaults), as is the
    /// federation client id.
    pub fn oauth_m2m(
        client_id: Option<String>,
        client_secret: Option<String>,
        scopes: Option<Vec<String>>,
        token_url_override: Option<String>,
        identity_federation_client_id: Option<String>,
    ) -> crate::Result<Self> {
        let client_id = client_id.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("OAuth M2M auth requires a non-empty client_id")
        })?;
        let client_secret = client_secret.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("OAuth M2M auth requires a non-empty client_secret")
        })?;
        Ok(AuthConfig::OAuthM2M {
            client_id,
            client_secret,
            scopes,
            token_url_override,
            identity_federation_client_id,
        })
    }

    /// Construct a [`AuthConfig::OAuthU2M`] from optional fields. All
    /// U2M fields are optional — the kernel fills `databricks-sql-connector` /
    /// the `[8030, 8031, 8032, 8033, 8034]` port window / `sql offline_access`
    /// when omitted — so this performs no
    /// validation and exists for symmetry with [`AuthConfig::pat`] /
    /// [`AuthConfig::oauth_m2m`] so bindings can forward optionals
    /// uniformly, including the federation client id.
    ///
    /// `redirect_ports` is the ordered list of candidate callback ports
    /// (first free binds). Single-port callers pass a one-element list
    /// (`Some(vec![port])`); `None`/empty falls back to the
    /// `[8030, 8031, 8032, 8033, 8034]` default window.
    pub fn oauth_u2m(
        client_id: Option<String>,
        redirect_ports: Option<Vec<u16>>,
        scopes: Option<Vec<String>>,
        token_url_override: Option<String>,
        callback_timeout: Option<std::time::Duration>,
        token_cache: Option<TokenCacheConfig>,
        identity_federation_client_id: Option<String>,
    ) -> Self {
        AuthConfig::OAuthU2M {
            client_id,
            redirect_ports,
            scopes,
            token_url_override,
            callback_timeout,
            token_cache,
            identity_federation_client_id,
        }
    }

    /// Construct a [`AuthConfig::AzureServicePrincipal`] (the `azure-sp-m2m`
    /// flow), validating that the Entra `azure_client_id` / `azure_client_secret`
    /// are present and non-empty. `azure_tenant_id` is optional (the kernel
    /// auto-discovers it from the workspace when omitted).
    pub fn azure_service_principal(
        azure_client_id: Option<String>,
        azure_client_secret: Option<String>,
        azure_tenant_id: Option<String>,
        azure_workspace_resource_id: Option<String>,
        identity_federation_client_id: Option<String>,
    ) -> crate::Result<Self> {
        let azure_client_id = azure_client_id.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("Azure SP auth requires a non-empty azure_client_id")
        })?;
        let azure_client_secret =
            azure_client_secret
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    crate::kernel_error::Error::invalid_argument()
                        .message("Azure SP auth requires a non-empty azure_client_secret")
                })?;
        Ok(AuthConfig::AzureServicePrincipal {
            azure_client_id,
            azure_client_secret,
            azure_tenant_id: azure_tenant_id.filter(|s| !s.is_empty()),
            azure_workspace_resource_id: azure_workspace_resource_id.filter(|s| !s.is_empty()),
            identity_federation_client_id,
        })
    }

    /// SP-wide federation client id attached to the selected base auth flow.
    pub(crate) fn identity_federation_client_id(&self) -> Option<&str> {
        match self {
            Self::Pat {
                identity_federation_client_id,
                ..
            }
            | Self::OAuthM2M {
                identity_federation_client_id,
                ..
            }
            | Self::OAuthU2M {
                identity_federation_client_id,
                ..
            }
            | Self::AzureServicePrincipal {
                identity_federation_client_id,
                ..
            } => identity_federation_client_id.as_deref(),
            // JWT private-key M2M does not thread an SP-wide federation
            // client id yet (the flow already proves identity via the
            // signed assertion); wire it here if/when that's needed.
            Self::OAuthM2MPrivateKey { .. } | Self::OAuthFederation { .. } | Self::External(_) => {
                None
            }
        }
    }

    /// Construct a [`AuthConfig::OAuthM2MPrivateKey`] from optional
    /// fields, validating that the required `client_id`, `jwt_key_file`,
    /// and `jwt_kid` are present and non-empty. `jwt_passphrase` /
    /// `jwt_algorithm` / `scopes` / `token_url_override` are optional and
    /// forwarded as-is (the kernel fills defaults and validates the key /
    /// algorithm at session open).
    pub fn oauth_m2m_private_key(
        client_id: Option<String>,
        jwt_key_file: Option<String>,
        jwt_kid: Option<String>,
        jwt_passphrase: Option<String>,
        jwt_algorithm: Option<String>,
        scopes: Option<Vec<String>>,
        token_url_override: Option<String>,
    ) -> crate::Result<Self> {
        let client_id = client_id.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("OAuth M2M private-key auth requires a non-empty client_id")
        })?;
        let jwt_key_file = jwt_key_file.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("OAuth M2M private-key auth requires a non-empty jwt_key_file")
        })?;
        let jwt_kid = jwt_kid.filter(|s| !s.is_empty()).ok_or_else(|| {
            crate::kernel_error::Error::invalid_argument()
                .message("OAuth M2M private-key auth requires a non-empty jwt_kid")
        })?;
        Ok(AuthConfig::OAuthM2MPrivateKey {
            client_id,
            jwt_key_file,
            jwt_kid,
            jwt_passphrase,
            jwt_algorithm,
            scopes,
            token_url_override,
        })
    }
}

/// Caller-supplied token provider trait.
///
/// Used by [`AuthConfig::External`] to plug a host-side auth
/// mechanism (Azure AD MSAL, GCP ADC, AWS STS exchange, …) into the
/// kernel without the kernel needing to know the protocol details.
///
/// The kernel calls `token` whenever it needs to populate an
/// `Authorization: Bearer …` header. Implementations should cache
/// the token themselves and respect their own expiry; the kernel
/// does not cache across calls.
#[async_trait::async_trait]
pub trait TokenProvider: Send + Sync {
    /// Return a fresh access token. Called once per HTTP request
    /// that needs authentication; implementations are expected to
    /// cache and refresh internally.
    async fn token(&self) -> crate::kernel_error::Result<String>;
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pat {
                identity_federation_client_id,
                ..
            } => f
                .debug_struct("Pat")
                .field("token", &"<redacted>")
                .field(
                    "identity_federation_client_id",
                    identity_federation_client_id,
                )
                .finish(),
            Self::OAuthM2M {
                client_id,
                scopes,
                identity_federation_client_id,
                ..
            } => f
                .debug_struct("OAuthM2M")
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .field("scopes", scopes)
                .field(
                    "identity_federation_client_id",
                    identity_federation_client_id,
                )
                .finish(),
            Self::OAuthU2M {
                client_id,
                redirect_ports,
                scopes,
                identity_federation_client_id,
                ..
            } => f
                .debug_struct("OAuthU2M")
                .field("client_id", client_id)
                .field("redirect_ports", redirect_ports)
                .field("scopes", scopes)
                .field(
                    "identity_federation_client_id",
                    identity_federation_client_id,
                )
                .finish(),
            Self::AzureServicePrincipal {
                azure_client_id,
                azure_tenant_id,
                azure_workspace_resource_id,
                identity_federation_client_id,
                ..
            } => f
                .debug_struct("AzureServicePrincipal")
                .field("azure_client_id", azure_client_id)
                .field("azure_client_secret", &"<redacted>")
                .field("azure_tenant_id", azure_tenant_id)
                .field("azure_workspace_resource_id", azure_workspace_resource_id)
                .field(
                    "identity_federation_client_id",
                    identity_federation_client_id,
                )
                .finish(),
            Self::OAuthM2MPrivateKey {
                client_id,
                jwt_key_file,
                jwt_kid,
                jwt_algorithm,
                scopes,
                ..
            } => f
                .debug_struct("OAuthM2MPrivateKey")
                .field("client_id", client_id)
                .field("jwt_key_file", jwt_key_file)
                .field("jwt_kid", jwt_kid)
                .field("jwt_passphrase", &"<redacted>")
                .field("jwt_algorithm", jwt_algorithm)
                .field("scopes", scopes)
                .finish(),
            Self::OAuthFederation {
                subject_token_type,
                audience,
                ..
            } => f
                .debug_struct("OAuthFederation")
                .field("subject_token", &"<redacted>")
                .field("subject_token_type", subject_token_type)
                .field("audience", audience)
                .finish(),
            Self::External(_) => f.write_str("External(<token provider>)"),
        }
    }
}

// ─── HTTP client ────────────────────────────────────────────────────────────

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// Total timeout for one HTTP request, from connection start through
    /// response-body completion. A separate 30-second connection timeout also
    /// applies, so connection establishment is bounded by the shorter limit.
    pub request_timeout: Duration,

    /// Maximum total time across retries for a single logical RPC.
    /// Retrying stops once cumulative elapsed time exceeds this.
    pub overall_timeout: Duration,

    /// Connection pool size (max idle warm sockets per remote host).
    ///
    /// Defaults to `100`, matching the JDBC driver's `HttpConnectionPoolSize`
    /// default. Raise this for workloads that burst many concurrent
    /// `executeStatement` calls or CloudFetch chunk downloads against a
    /// single host; too low a value forces fresh TLS handshakes when the
    /// pool fills.
    pub pool_max_idle_per_host: usize,

    /// Minimum backoff wait between retry attempts. Also the floor for
    /// a server-supplied `Retry-After` and the base of the exponential
    /// backoff (`min_wait * 2^attempt`) used when no `Retry-After` is
    /// present. Default 1s.
    pub retry_min_wait: Duration,

    /// Maximum backoff wait between retry attempts. Caps both the
    /// exponential backoff and a server-supplied `Retry-After`.
    /// Default 60s.
    pub retry_max_wait: Duration,

    /// Maximum number of retries **after** the initial attempt — a
    /// request is attempted at most `retry_max_retries + 1` times.
    /// Default 5 (→ 6 total attempts).
    ///
    /// Wrappers exposing a "total attempts" knob (the connector's
    /// `_retry_stop_after_attempts_count`, the JDBC driver, …) should
    /// map it as `retry_max_retries = max(0, total_attempts - 1)`.
    pub retry_max_retries: u32,

    /// Extra HTTP headers to send on every request. Lets wrappers
    /// forward caller-supplied headers (the connector's `http_headers`,
    /// which also carries the composed `User-Agent`) onto the kernel's
    /// HTTP stack.
    ///
    /// **`Authorization` / `x-databricks-org-id` are reserved:** a
    /// caller entry for either name is **silently dropped (with a
    /// `warn!`)** — it never reaches the wire. The kernel always adds
    /// its own `Authorization` (from the auth provider) and, when set,
    /// `x-databricks-org-id`, so auth and multi-tenant routing can't be
    /// hijacked by a caller header. (This mirrors the Thrift connector,
    /// where `auth_provider.add_headers` runs after the custom-header
    /// dict and overwrites those names.)
    ///
    /// **`User-Agent` is special-cased:** a caller `User-Agent` here
    /// **replaces** the kernel's base UA entirely — it is folded into the
    /// effective UA at client construction (becoming the UA verbatim) and
    /// removed from this list. This gives the SEA server an accurate
    /// driver identity for query-source attribution (e.g. the connector's
    /// `PyDatabricksSqlConnector/x (entry)`), instead of the
    /// `DatabricksJDBCDriverOSS/...` base.
    ///
    /// PRECONDITION: the caller UA's leading prefix MUST be in the
    /// gateway's `databricks.sqlexecapi.known_driver_prefixes` SAFE flag.
    /// The server gates the `INLINE_OR_EXTERNAL_LINKS` result disposition
    /// (single-response chunk links → parallel downloads) on a
    /// `startsWith` prefix match of that flag; a non-allow-listed prefix
    /// is rejected (HTTP 400 / Thrift redirect). `PyDatabricksSqlConnector`
    /// is allow-listed for this reason. When NO caller `User-Agent` is
    /// supplied, the base UA stays `DatabricksJDBCDriverOSS/...` so the
    /// gate still passes.
    ///
    /// **All other headers are sent as supplied.** A `Vec` (not a map)
    /// so order is preserved. No de-duplication is performed: duplicate
    /// names are applied via `reqwest`'s `HeaderMap::append`, so each
    /// entry is emitted on the wire in insertion order (the receiver
    /// sees every value, not just the last) — pass a name once unless
    /// you specifically intend multiple header lines.
    pub custom_headers: Vec<(String, String)>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(120),
            overall_timeout: Duration::from_secs(900),
            pool_max_idle_per_host: 100,
            // Mirror `RetryConfig::default()` so a stock `HttpConfig`
            // produces the same retry behaviour the kernel shipped
            // before these knobs were configurable.
            retry_min_wait: Duration::from_secs(1),
            retry_max_wait: Duration::from_secs(60),
            retry_max_retries: 5,
            custom_headers: Vec::new(),
        }
    }
}

// ─── HTTP proxy ───────────────────────────────────────────────────────────

/// Programmatic HTTP/HTTPS proxy configuration.
///
/// When [`url`](Self::url) is `None` (the default), the kernel does **not**
/// configure a proxy explicitly and `reqwest`'s standard behaviour applies —
/// the `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` environment variables are
/// honoured automatically. Setting [`url`](Self::url) **overrides** those env
/// vars with the supplied proxy, routing all (`http://` and `https://`)
/// traffic through it.
///
/// Basic-auth credentials may be embedded directly in the URL
/// (`http://user:pass@host:port`), or supplied out-of-band via
/// [`username`](Self::username) / [`password`](Self::password); the explicit
/// fields take precedence over URL-embedded credentials when both are set.
///
/// `ProxyConfig` is `#[non_exhaustive]` — construct via
/// [`ProxyConfig::default`] and assign the public fields you need:
///
/// ```no_run
/// # use databricks_sql_kernel::ProxyConfig;
/// let mut proxy = ProxyConfig::default();
/// proxy.url = Some("http://proxy.corp.example.com:8080".to_string());
/// ```
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct ProxyConfig {
    /// Proxy URL, e.g. `http://proxy.corp.example.com:8080` or
    /// `http://user:pass@proxy:8080`. Must use the `http://` or
    /// `https://` scheme. `None` ⇒ env-var behaviour (no override).
    pub url: Option<String>,

    /// Username for proxy basic-auth. Overrides any credentials
    /// embedded in [`url`](Self::url) when set.
    pub username: Option<String>,

    /// Password for proxy basic-auth. Secret material — redacted in
    /// `Debug`. Ignored unless [`username`](Self::username) is also set.
    pub password: Option<String>,

    /// Comma-separated list of hosts/domains to bypass the proxy
    /// (e.g. `"localhost,*.internal.corp,.example.com"`). Only applied
    /// when [`url`](Self::url) is set.
    pub bypass_hosts: Option<String>,
}

impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the explicit `password` field. The `url` may itself
        // carry embedded `user:pass@` credentials, so render only its
        // presence rather than the raw string.
        f.debug_struct("ProxyConfig")
            .field("url", &self.url.as_ref().map(|_| "<set>"))
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("bypass_hosts", &self.bypass_hosts)
            .finish()
    }
}

// ─── TLS ────────────────────────────────────────────────────────────────────

/// TLS settings for the HTTP client.
///
/// All defaults match the most secure stance: validate the system trust
/// store, require a valid certificate chain, and verify hostnames. Any
/// non-default field is a *relaxation* — flip these for dev / on-prem
/// boxes with self-signed certs, never against production.
///
/// The kernel exposes only fields that the internal rustls plumbing
/// already supports — cert pinning and FIPS mode are *not* in scope.
///
/// `TlsConfig` is `#[non_exhaustive]`, so construct it from
/// [`TlsConfig::default`] and assign the public fields you need:
///
/// ```no_run
/// # use databricks_sql_kernel::TlsConfig;
/// // From a file path:
/// let mut from_file = TlsConfig::default();
/// from_file.custom_ca_cert = Some(std::fs::read("/etc/ssl/corp-ca.pem").unwrap());
///
/// // From an in-memory buffer (the napi-rs `Buffer` / pyo3 `bytes`
/// // path — no tempfile shim needed):
/// let pem_bytes: Vec<u8> = b"-----BEGIN CERTIFICATE-----\n...".to_vec();
/// let mut from_bytes = TlsConfig::default();
/// from_bytes.custom_ca_cert = Some(pem_bytes);
/// # let _ = (from_file, from_bytes);
/// ```
//
// TODO(pr23-followup): the public Thrift NodeJS driver does NOT
// expose programmatic TLS config — its `ConnectionOptions` has no
// `ca` / `cert` / `key` field, and its `HttpConnection.createHttpsAgent`
// builds the agent with `rejectUnauthorized: false` HARDCODED
// (verified at `~/databricks-sql-nodejs/lib/connection/connections/HttpConnection.ts:67-77`,
// 2026-05-27). The only "TLS knobs" Thrift NodeJS users have today
// are the Node process-level env vars `NODE_EXTRA_CA_CERTS` and
// `NODE_TLS_REJECT_UNAUTHORIZED`, and even those are inert against
// the Thrift driver's hardcoded-permissive agent.
//
// The kernel surface here is intentionally STRICTER than Thrift's
// (default = full validation, opt-in relaxations) and richer (custom
// CA bundles, programmatic mTLS room). Motivated by:
//   1. Python connector parity — `databricks-sql-connector` exposes
//      `_tls_no_verify` / `_tls_verify_hostname` /
//      `_tls_trusted_ca_file` / `_tls_client_cert_*`. The kernel must
//      back those when pyo3 wires them up.
//   2. Doing better than Thrift's hardcoded-insecure default on the
//      SEA NodeJS path (napi binding).
//
// Revisit when (a) the napi binding wires these onto `ConnectionOptions`
// for JS callers, and (b) we decide whether to honour Node's
// `NODE_EXTRA_CA_CERTS` env var on the SEA path the way Node's own
// https stack does — likely yes, for ergonomic parity with the rest
// of the Node ecosystem.
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct TlsConfig {
    /// PEM-encoded CA certificate bytes to add to the trust store on
    /// top of the system roots. Use this when connecting through a
    /// corporate proxy that re-signs TLS or to an on-prem Databricks
    /// deployment using an internal CA.
    ///
    /// Carrying bytes (rather than a path) keeps the surface friendly
    /// for foreign-language wrappers — napi-rs / pyo3 callers pass a
    /// `Buffer` / `bytes` directly without needing to materialise a
    /// tempfile.
    ///
    /// **Multi-cert bundles (root + intermediate):** the kernel splits
    /// the PEM into individual certificates and adds each as a root,
    /// so a concatenated `cat root.pem intermediate.pem` blob works
    /// transparently regardless of the active TLS backend.
    pub custom_ca_cert: Option<Vec<u8>>,

    /// Accept self-signed server certificates.
    ///
    /// Note: the underlying reqwest backend does not distinguish
    /// "self-signed only" from "fully unvalidated" — setting this to
    /// `true` disables ALL chain validation (CA trust, expiration,
    /// etc.) via `danger_accept_invalid_certs(true)`. Use only in
    /// development.
    pub accept_self_signed: bool,

    /// Skip the certificate-hostname-vs-SNI match check. Use only
    /// when connecting via an IP literal or a hostname the cert
    /// wasn't issued for in a development / lab environment.
    pub skip_hostname_verification: bool,

    /// PEM-encoded client certificate for mutual TLS (mTLS).
    ///
    /// Set together with [`Self::client_key_pem`] when the server
    /// requires the client to present a certificate (mTLS handshake).
    /// Mirrors the Python connector's `_tls_client_cert_file` /
    /// `_tls_client_cert_key_file` knobs — wrappers read those files
    /// and pass the bytes here, no tempfile shim needed.
    ///
    /// **Backend note:** under the `tls-native` backend the cert chain
    /// and key are loaded as a separate `(cert, key)` pair via
    /// `reqwest::Identity::from_pkcs8_pem`; under `tls-rustls` they are
    /// concatenated into a single PEM and loaded via
    /// `reqwest::Identity::from_pem`. Either backend accepts a PEM
    /// `client_cert_pem` carrying a leaf certificate optionally followed
    /// by its intermediate chain.
    ///
    /// Both `client_cert_pem` and `client_key_pem` must be set
    /// together; setting only one is rejected at
    /// [`Session::open`](crate::Session::open) with
    /// `ErrorCode::InvalidArgument`.
    ///
    /// **Validation timing differs by backend.** Under `tls-native`,
    /// `from_pkcs8_pem` validates the cert/key pair at `Session::open`,
    /// so a malformed or mismatched pair surfaces synchronously as
    /// `ErrorCode::InvalidArgument`. Under `tls-rustls`, the cert and
    /// key are stored and only exercised during the TLS handshake, so
    /// the same problem surfaces later as a connection error on the
    /// first request rather than at `Session::open`.
    pub client_cert_pem: Option<Vec<u8>>,

    /// PEM-encoded private key for the mTLS client certificate.
    ///
    /// Accepted key encodings depend on the active TLS backend:
    /// - **`tls-rustls`** accepts PKCS#8 (`BEGIN PRIVATE KEY`), PKCS#1
    ///   (`BEGIN RSA PRIVATE KEY`), and SEC1 (`BEGIN EC PRIVATE KEY`).
    /// - **`tls-native`** requires a **PKCS#8** key (`BEGIN PRIVATE
    ///   KEY`) — `reqwest::Identity::from_pkcs8_pem` is PKCS#8-only.
    ///
    /// For portability across both backends, supply a PKCS#8 key. See
    /// [`Self::client_cert_pem`] for pairing rules and the
    /// backend-specific validation timing.
    pub client_key_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field(
                "custom_ca_cert",
                &self
                    .custom_ca_cert
                    .as_ref()
                    .map(|b| format!("<{} bytes>", b.len())),
            )
            .field("accept_self_signed", &self.accept_self_signed)
            .field(
                "skip_hostname_verification",
                &self.skip_hostname_verification,
            )
            .field(
                "client_cert_pem",
                &self
                    .client_cert_pem
                    .as_ref()
                    .map(|b| format!("<{} bytes>", b.len())),
            )
            // The private key is secret material — never render its
            // length or contents, just whether one was supplied.
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

// ─── Result management (CloudFetch + future result knobs) ───────────────────

/// Result-stream tuning. Today this is exclusively CloudFetch knobs;
/// future non-CloudFetch result-management options (chunk-size hints,
/// decompression strategy, max-row buffer, etc.) slot in here too — the
/// name is intentionally general.
///
/// Renamed from `CloudFetchConfig` so the type doesn't fossilise around
/// one transport feature.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ResultConfig {
    /// Whether to opt into CloudFetch (cloud-storage-backed download of
    /// large result sets via presigned URLs). Default: `true`.
    ///
    /// Setting this to `false` mirrors JDBC's `EnableQueryResultDownload=0`
    /// behaviour. It is a **connection-level** setting: read once at
    /// [`Session::open`](crate::Session::open) and applied to every
    /// statement by selecting the SEA `disposition` on each
    /// `ExecuteStatement` (`false` → `INLINE`, so SEA serves **all**
    /// result sizes via the inline-Arrow path regardless of size). This
    /// matches how the JDBC and Thrift reference drivers control
    /// cloud-fetch (per request), and is deliberately **not** expressed
    /// as a session conf — SEA has no `CAN_CLOUD_DOWNLOAD` conf and
    /// rejects one at ExecuteStatement with `CONFIG_NOT_AVAILABLE`
    /// (issue #196). Users who need inline-only behaviour (e.g.
    /// workspaces without cloud-storage egress) should set this to
    /// `false`.
    ///
    /// Disabling CloudFetch routes results to the inline-Arrow
    /// path. The kernel today supports the **single-attachment**
    /// inline path; the multi-chunk inline path is on the SEA
    /// inline-Arrow rollout (DD1) and is tracked as a follow-up
    /// (`InlineArrowProvider::new` consumes one attachment, and
    /// `src/reader/mod.rs` returns `Error::not_implemented` for
    /// inline responses that carry a `next_chunk_internal_link`).
    /// Once multi-chunk lands, this path will not match
    /// CloudFetch's concurrent chunk-download pipeline
    /// (`cloudfetch_max_chunks_in_memory`).
    pub cloudfetch_enabled: bool,

    /// Number of presigned URLs to prefetch ahead of the consumer.
    pub cloudfetch_link_prefetch_window: usize,

    /// Maximum number of decompressed chunks held in memory at once.
    /// Bounds parallelism and peak RSS.
    pub cloudfetch_max_chunks_in_memory: usize,

    /// Maximum number of decompressed Arrow chunks the inline-Arrow
    /// streaming reader holds in memory at once. Analogue of
    /// [`Self::cloudfetch_max_chunks_in_memory`] for the inline path
    /// (used when [`Self::cloudfetch_enabled`] is `false`).
    ///
    /// The inline path is sequential per the SEA contract — one
    /// `GetResultData` call at a time — so this knob bounds the
    /// **prefetch window** (chunks the kernel pulls ahead of the
    /// consumer) rather than the download parallelism. Larger values
    /// give the consumer a deeper buffer at the cost of memory.
    ///
    /// Default `4`. Range 1..=N; `1` disables prefetching and gives
    /// strict lazy fetch-on-demand semantics.
    pub inline_max_chunks_in_memory: usize,

    /// When `true`, every column of complex type (`List` /
    /// `LargeList` / `FixedSizeList` / `Map` / `Struct`, plus the
    /// precise types `VARIANT` / `GEOMETRY` / `GEOGRAPHY` carried in
    /// field metadata) is replaced with a `Utf8` column holding
    /// compact JSON text per row.
    ///
    /// The JSON format matches what the Databricks server emits on
    /// the Thrift wire when `complexTypesAsArrow=false` (compact —
    /// no whitespace between separators, double-quoted strings,
    /// JSON `null` for SQL NULL).
    ///
    /// Default `false`. Bindings that surface a flat-text view
    /// (PySQL when `_use_arrow_native_complex_types=False`, JDBC
    /// when `complexDatatypeSupport=false`) should set this `true`.
    /// ADBC and any other Arrow-native consumer should leave it
    /// `false`.
    pub complex_types_as_json: bool,

    /// When `true`, `Interval(*)` and `Duration(*)` columns are
    /// replaced with `Utf8` columns containing the canonical
    /// Databricks text form (`"Y-M"` for YEAR-MONTH,
    /// `"D HH:MM:SS.nnnnnnnnn"` for DAY-TIME / durations).
    ///
    /// Motivation: pyarrow's Python bindings can't decode Arrow's
    /// `month_interval` (id 21) — `.as_py()` / `.to_pylist()` /
    /// `cast(string)` / `to_pandas()` all fail with KeyError or
    /// `ArrowNotImplementedError`, killing the result set. Bindings
    /// that hand Arrow batches to pyarrow Python (the PySQL kernel
    /// backend) should set this `true`. Rust consumers and bindings
    /// with their own interval decoder can leave it `false`.
    ///
    /// Default `false`.
    pub intervals_as_string: bool,
}

impl Default for ResultConfig {
    fn default() -> Self {
        Self {
            cloudfetch_enabled: true,
            cloudfetch_link_prefetch_window: 10,
            cloudfetch_max_chunks_in_memory: 16,
            inline_max_chunks_in_memory: 4,
            complex_types_as_json: false,
            intervals_as_string: false,
        }
    }
}

/// Upper bound applied to `cloudfetch_max_chunks_in_memory`. Peak RSS grows
/// roughly linearly with this knob (each in-memory chunk is tens of MB), so
/// an unbounded value — a typo like `"1000000"` meant as `"100"`, or a host
/// bug — risks OOM on large result sets. This ceiling (16× the default of 16,
/// and 8× the most aggressive value we've measured) is comfortably above any
/// real tuning yet bounds the blast radius of a fat-fingered value.
pub(crate) const CLOUDFETCH_MAX_CHUNKS_CEILING: usize = 256;

/// A recognised client-side result knob — the typed identity of a
/// `set_client_conf` key.
///
/// The C-ABI boundary (`kernel_session_config_set_client_conf`) is
/// stringly-typed by design (so the ABI never churns and hosts can forward
/// DSN attributes), but the string is parsed **once** into this enum via
/// [`ClientConfKey::parse`] and everything downstream matches on the enum.
/// The `match` in [`ResultConfig::apply_client_conf`] is exhaustive, so
/// adding a variant here won't compile until the new knob is handled —
/// there's no way to silently forget a knob. Each variant maps 1:1 onto a
/// field of [`ResultConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientConfKey {
    CloudfetchEnabled,
    CloudfetchLinkPrefetchWindow,
    CloudfetchMaxChunksInMemory,
    InlineMaxChunksInMemory,
    ComplexTypesAsJson,
    IntervalsAsString,
}

impl ClientConfKey {
    /// Every recognised key, in declaration order. Used for error messages
    /// and to pin parity with [`ResultConfig`]'s fields in tests.
    pub(crate) const ALL: &'static [ClientConfKey] = &[
        Self::CloudfetchEnabled,
        Self::CloudfetchLinkPrefetchWindow,
        Self::CloudfetchMaxChunksInMemory,
        Self::InlineMaxChunksInMemory,
        Self::ComplexTypesAsJson,
        Self::IntervalsAsString,
    ];

    /// The canonical wire spelling of this key.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CloudfetchEnabled => "cloudfetch_enabled",
            Self::CloudfetchLinkPrefetchWindow => "cloudfetch_link_prefetch_window",
            Self::CloudfetchMaxChunksInMemory => "cloudfetch_max_chunks_in_memory",
            Self::InlineMaxChunksInMemory => "inline_max_chunks_in_memory",
            Self::ComplexTypesAsJson => "complex_types_as_json",
            Self::IntervalsAsString => "intervals_as_string",
        }
    }

    /// Parse a caller-supplied key string (case-insensitive) into a known
    /// key, or `None` if it is not a recognised client-conf key.
    pub(crate) fn parse(key: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|k| key.eq_ignore_ascii_case(k.as_str()))
    }
}

impl ResultConfig {
    /// Apply one client-side result knob by name, parsing `value` into the
    /// matching typed field.
    ///
    /// This is the single validated entry point behind the C ABI's
    /// `kernel_session_config_set_client_conf` (and any host that forwards
    /// DSN / connection-string attributes as key/value pairs). The key is
    /// resolved via [`ClientConfKey::parse`] (case-insensitive).
    ///
    /// Unlike the server `session_conf` map, these knobs are **client-side**
    /// and never reach the SEA wire — so this deliberately does NOT accept
    /// server conf keys (`ANSI_MODE`, `TIMEZONE`, …); those remain in
    /// `session_conf` and pass through unchanged. Keeping the two channels
    /// separate is the point (see #226).
    ///
    /// # Errors
    /// Returns `ErrorCode::InvalidArgument` when:
    /// - `key` is not a recognised client-conf key, or
    /// - `value` cannot be parsed for that key's type (booleans accept
    ///   exactly `"true"` / `"false"` case-insensitively; the `usize`
    ///   windows require a positive integer).
    ///
    /// A `cloudfetch_max_chunks_in_memory` above
    /// [`CLOUDFETCH_MAX_CHUNKS_CEILING`] is **clamped** (with a `warn!`),
    /// not rejected — a fat-fingered value must not fail the connection, but
    /// it must not drive peak RSS toward OOM either.
    pub(crate) fn apply_client_conf(&mut self, key: &str, value: &str) -> crate::Result<()> {
        let Some(known) = ClientConfKey::parse(key) else {
            return Err(crate::kernel_error::Error::invalid_argument().message(format!(
                "unknown client conf key {key:?}; recognised client-side result knobs are {:?}. \
                 (Server session confs like ANSI_MODE / TIMEZONE go through session_conf, not here.)",
                ClientConfKey::ALL
                    .iter()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>(),
            )));
        };

        // Booleans accept exactly "true"/"false" (case-insensitive). SEA and
        // the reference drivers use these literal forms; anything else is a
        // caller error we surface rather than silently coerce.
        let parse_bool = |v: &str| -> crate::Result<bool> {
            match v.trim().to_ascii_lowercase().as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(
                    crate::kernel_error::Error::invalid_argument().message(format!(
                        "client conf '{}' expects \"true\" or \"false\", got {v:?}",
                        known.as_str(),
                    )),
                ),
            }
        };
        let parse_positive_usize = |v: &str| -> crate::Result<usize> {
            match v.trim().parse::<usize>() {
                Ok(n) if n > 0 => Ok(n),
                _ => Err(
                    crate::kernel_error::Error::invalid_argument().message(format!(
                        "client conf '{}' expects a positive integer, got {v:?}",
                        known.as_str(),
                    )),
                ),
            }
        };

        // Exhaustive: adding a ClientConfKey variant forces a new arm here.
        match known {
            ClientConfKey::CloudfetchEnabled => {
                self.cloudfetch_enabled = parse_bool(value)?;
            }
            ClientConfKey::CloudfetchLinkPrefetchWindow => {
                self.cloudfetch_link_prefetch_window = parse_positive_usize(value)?;
            }
            ClientConfKey::CloudfetchMaxChunksInMemory => {
                let n = parse_positive_usize(value)?;
                let clamped = n.min(CLOUDFETCH_MAX_CHUNKS_CEILING);
                if clamped != n {
                    crate::klog::warn!(
                        requested = n,
                        ceiling = CLOUDFETCH_MAX_CHUNKS_CEILING,
                        "clamping cloudfetch_max_chunks_in_memory to the ceiling \
                         (peak RSS scales with this knob)",
                    );
                }
                self.cloudfetch_max_chunks_in_memory = clamped;
            }
            ClientConfKey::InlineMaxChunksInMemory => {
                self.inline_max_chunks_in_memory = parse_positive_usize(value)?;
            }
            ClientConfKey::ComplexTypesAsJson => {
                self.complex_types_as_json = parse_bool(value)?;
            }
            ClientConfKey::IntervalsAsString => {
                self.intervals_as_string = parse_bool(value)?;
            }
        }
        Ok(())
    }
}

// ─── Logging ────────────────────────────────────────────────────────────────

#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct LoggingConfig {
    /// A full [`tracing_subscriber::EnvFilter`] directive, applied
    /// **verbatim** on the direct-Rust `Session::open` path.
    ///
    /// The kernel emits every event on the target
    /// [`crate::KERNEL_LOG_TARGET`] = `databricks::sql::kernel`, and
    /// `EnvFilter` matches by `::`-delimited segment prefix, so the
    /// directive MUST name that target with `::` separators, e.g.
    /// `databricks::sql::kernel=debug`. The underscore crate-name form
    /// (`databricks_sql_kernel=debug`) matches **nothing** — the log file
    /// is created but stays empty and no error is surfaced. If `None`, the
    /// kernel honors `RUST_LOG` from the environment (falling back to
    /// `databricks::sql::kernel=warn`).
    pub filter: Option<String>,

    /// Destination for logs. If it names an existing directory (or ends
    /// with a path separator), a `databricks-sql-kernel.log` file is
    /// created *inside* it; otherwise it is taken as the log file path. If
    /// `None`, logs go to stderr.
    pub file: Option<PathBuf>,
}

// ─── Telemetry ──────────────────────────────────────────────────────────────

/// Telemetry collection and export settings.
///
/// Telemetry is disabled by default. When enabled, the kernel records
/// driver operation metrics and exports them through the Databricks SQL
/// driver telemetry envelope. Telemetry failures are fail-open: export
/// errors are logged at trace/debug level and never change user operation
/// results.
#[non_exhaustive]
#[derive(Clone)]
pub struct TelemetryConfig {
    /// Whether telemetry collection is enabled.
    pub enabled: bool,

    /// Number of telemetry events to buffer before triggering a flush.
    pub batch_size: usize,

    /// Periodic background flush interval. Defaults to five seconds, matching
    /// JDBC. A zero interval is invalid and disables telemetry.
    pub flush_interval: Duration,

    /// Maximum number of HTTP retries for telemetry export.
    pub max_retries: u32,

    /// Fixed retry delay between telemetry export attempts.
    pub retry_delay: Duration,

    /// Maximum time an explicit session close should wait for the final
    /// telemetry flush. `Drop` never waits.
    pub close_flush_timeout: Duration,

    #[cfg(test)]
    pub(crate) exporter: Option<std::sync::Arc<dyn crate::telemetry::TelemetryExporter>>,
}

impl std::fmt::Debug for TelemetryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("TelemetryConfig");
        debug
            .field("enabled", &self.enabled)
            .field("batch_size", &self.batch_size)
            .field("flush_interval", &self.flush_interval)
            .field("max_retries", &self.max_retries)
            .field("retry_delay", &self.retry_delay)
            .field("close_flush_timeout", &self.close_flush_timeout);
        #[cfg(test)]
        debug.field(
            "exporter",
            &self.exporter.as_ref().map(|_| "<telemetry exporter>"),
        );
        debug.finish()
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 100,
            flush_interval: Duration::from_secs(5),
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            close_flush_timeout: Duration::from_secs(5),
            #[cfg(test)]
            exporter: None,
        }
    }
}

impl TelemetryConfig {
    #[cfg(test)]
    pub(crate) fn with_exporter(
        mut self,
        exporter: std::sync::Arc<dyn crate::telemetry::TelemetryExporter>,
    ) -> Self {
        self.exporter = Some(exporter);
        self
    }
}

// ─── Transport mode ─────────────────────────────────────────────────────────

/// Wire-protocol selection.
///
/// v0 supports `SeaRest` only. The enum is `#[non_exhaustive]` so v1+
/// can add `SeaGrpc` / `Thrift` without breaking wrappers. The actual
/// `Transport` trait that implements each variant is `pub(crate)` —
/// internal to the kernel.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransportMode {
    /// Statement Execution API over REST. Default.
    #[default]
    SeaRest,
}

// ─── Assembled SessionConfig ────────────────────────────────────────────────

/// The fully-assembled session configuration consumed by `Session::open`
/// and by the C ABI's `kernel_session_open`.
#[non_exhaustive]
#[derive(Debug)]
pub struct SessionConfig {
    /// Required: which workspace and warehouse.
    pub connection: ConnectionConfig,

    /// Optional: default catalog / schema.
    pub defaults: Option<DefaultOpts>,

    /// Required: how to authenticate.
    pub auth: AuthConfig,

    /// Client-provided driver/runtime identity. Telemetry uses this for
    /// `DriverSystemConfiguration`, filling any missing fields with
    /// kernel defaults at session-open time.
    pub driver_system_configuration: Option<DriverSystemConfiguration>,

    /// Server-bound session conf. Maps to SEA `session_confs` /
    /// Thrift `configuration`. Typed sub-structs above translate
    /// the *client-side* knobs; this map carries everything else.
    ///
    /// The kernel passes user-supplied entries through unchanged. The
    /// kernel does not synthesise any entries here from typed
    /// client-side knobs.
    ///
    /// In particular, [`ResultConfig::cloudfetch_enabled`] is **not**
    /// expressed as a session conf: SEA has no `CAN_CLOUD_DOWNLOAD`
    /// conf (it is accepted at CreateSession but rejected at the first
    /// ExecuteStatement with `CONFIG_NOT_AVAILABLE`, issue #196).
    /// CloudFetch-disable is instead applied per-statement via the SEA
    /// `disposition` field, matching the JDBC / Thrift reference
    /// drivers. A caller-supplied `can_cloud_download` is still forwarded
    /// unchanged because the kernel does not validate server-bound
    /// `session_conf` keys.
    ///
    /// **Boolean-valued conf keys** should use the exact strings
    /// `"true"` / `"false"`. SEA accepts these literally; other forms
    /// (`"True"`, `"yes"`, `"1"`) either fail at session-create or
    /// behave as undefined on the server. The kernel does not
    /// pre-validate — its posture is "trust the user's session_conf" —
    /// so non-canonical values flow to the server as-is.
    pub session_conf: HashMap<String, String>,

    pub http: HttpConfig,
    pub proxy: Option<ProxyConfig>,
    pub tls: TlsConfig,
    pub result: ResultConfig,
    pub logging: LoggingConfig,
    pub telemetry: TelemetryConfig,

    /// `None` = kernel picks the default (`SeaRest` today).
    pub transport: Option<TransportMode>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── token cache policy ────────────────────────────────────────

    #[test]
    fn token_cache_defaults_to_enabled() {
        // Both the reference ODBC driver and JDBC default EnableTokenCache
        // to 1, with no passphrase.
        assert!(TokenCacheConfig::default().enabled);
        assert!(TokenCacheConfig::default().passphrase.is_none());
    }

    #[test]
    fn token_cache_enabled_with_passphrase_resolves_to_it() {
        let cfg = TokenCacheConfig {
            enabled: true,
            passphrase: Some("s3cr3t".to_string()),
        };
        assert_eq!(cfg.resolve().as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn token_cache_enabled_without_passphrase_falls_back_to_derived_key() {
        // The reference ODBC driver ships EnableTokenCache=1 with an empty
        // TokenCachePassPhrase and still caches, so this combination must
        // NOT fail the connection — a DSN that never set a passphrase has to
        // keep working. It must also not disable caching silently, which
        // would reintroduce a browser prompt on every connection.
        let cfg = TokenCacheConfig::default();
        let passphrase = cfg
            .resolve()
            .expect("enabled + no passphrase must still cache");
        assert!(
            !passphrase.trim().is_empty(),
            "fallback passphrase must be non-empty, else the cache would be \
             encrypted under nothing"
        );
    }

    #[test]
    fn token_cache_blank_passphrase_uses_the_derived_key_not_the_blank() {
        // An all-whitespace passphrase is almost always an unexpanded config
        // placeholder. Honouring it literally would key every machine's cache
        // identically, which is strictly weaker than the machine-local
        // fallback, so blank must be treated as absent.
        let derived = TokenCacheConfig::default()
            .resolve()
            .expect("default resolves")
            .into_owned();
        for blank in ["", "   ", "\t\n"] {
            let cfg = TokenCacheConfig {
                enabled: true,
                passphrase: Some(blank.to_string()),
            };
            let resolved = cfg.resolve().expect("blank still caches");
            assert_ne!(
                resolved.as_ref(),
                blank,
                "blank passphrase {blank:?} must not be used verbatim"
            );
            assert_eq!(
                resolved.as_ref(),
                derived,
                "blank passphrase {blank:?} should fall back to the derived key"
            );
        }
    }

    #[test]
    fn token_cache_derived_key_is_stable_across_calls() {
        // If the fallback were not deterministic, every process would write a
        // cache the next one cannot read — a silent browser prompt on every
        // connection, which is the failure this whole feature avoids.
        assert_eq!(
            TokenCacheConfig::default()
                .resolve()
                .map(|p| p.into_owned()),
            TokenCacheConfig::default()
                .resolve()
                .map(|p| p.into_owned())
        );
    }

    #[test]
    fn token_cache_disabled_resolves_to_none_without_passphrase() {
        // Disabling must always be valid — it is the escape hatch for
        // processes that must not write credentials to disk at all.
        let cfg = TokenCacheConfig {
            enabled: false,
            passphrase: None,
        };
        assert_eq!(cfg.resolve(), None);
    }

    #[test]
    fn token_cache_disabled_ignores_a_supplied_passphrase() {
        // `enabled: false` wins: a leftover passphrase must not silently
        // re-enable disk persistence.
        let cfg = TokenCacheConfig {
            enabled: false,
            passphrase: Some("ignored".to_string()),
        };
        assert_eq!(cfg.resolve(), None);
    }

    // ─── AuthConfig validating constructors ───────────────────────

    #[test]
    fn pat_rejects_missing_or_empty_token() {
        let err = AuthConfig::pat(None, None).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("non-empty access token"));

        let err = AuthConfig::pat(Some(String::new()), None).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn pat_accepts_non_empty_token() {
        match AuthConfig::pat(
            Some("dapi-x".to_string()),
            Some("federation-client".to_string()),
        )
        .unwrap()
        {
            AuthConfig::Pat {
                token,
                identity_federation_client_id,
            } => {
                assert_eq!(token, "dapi-x");
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client")
                );
            }
            other => panic!("expected Pat, got {other:?}"),
        }
    }

    #[test]
    fn oauth_m2m_rejects_missing_or_empty_client_id() {
        let err =
            AuthConfig::oauth_m2m(None, Some("sec".to_string()), None, None, None).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_id"));

        let err = AuthConfig::oauth_m2m(
            Some(String::new()),
            Some("sec".to_string()),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_id"));
    }

    #[test]
    fn oauth_m2m_rejects_missing_or_empty_client_secret() {
        let err =
            AuthConfig::oauth_m2m(Some("cid".to_string()), None, None, None, None).unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_secret"));

        let err = AuthConfig::oauth_m2m(
            Some("cid".to_string()),
            Some(String::new()),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("client_secret"));
    }

    #[test]
    fn oauth_m2m_accepts_full_credentials() {
        match AuthConfig::oauth_m2m(
            Some("cid".to_string()),
            Some("sec".to_string()),
            Some(vec!["all-apis".to_string()]),
            None,
            Some("federation-client".to_string()),
        )
        .unwrap()
        {
            AuthConfig::OAuthM2M {
                client_id,
                client_secret,
                scopes,
                token_url_override,
                identity_federation_client_id,
            } => {
                assert_eq!(client_id, "cid");
                assert_eq!(client_secret, "sec");
                assert_eq!(scopes, Some(vec!["all-apis".to_string()]));
                assert!(token_url_override.is_none());
                assert_eq!(
                    identity_federation_client_id.as_deref(),
                    Some("federation-client")
                );
            }
            other => panic!("expected OAuthM2M, got {other:?}"),
        }
    }

    // ─── OAuthM2MPrivateKey (JWT) constructor ──────────────────────
    // Mirrors JDBC's builder null-checks (should_ThrowException_When_*).

    #[test]
    fn oauth_m2m_private_key_rejects_missing_or_empty_client_id() {
        let err = AuthConfig::oauth_m2m_private_key(
            None,
            Some("/k.pem".to_string()),
            Some("kid".to_string()),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("client_id"));

        let err = AuthConfig::oauth_m2m_private_key(
            Some(String::new()),
            Some("/k.pem".to_string()),
            Some("kid".to_string()),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("client_id"));
    }

    #[test]
    fn oauth_m2m_private_key_rejects_missing_key_file() {
        let err = AuthConfig::oauth_m2m_private_key(
            Some("cid".to_string()),
            None,
            Some("kid".to_string()),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("jwt_key_file"));
    }

    #[test]
    fn oauth_m2m_private_key_rejects_missing_kid() {
        let err = AuthConfig::oauth_m2m_private_key(
            Some("cid".to_string()),
            Some("/k.pem".to_string()),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("jwt_kid"));
    }

    #[test]
    fn oauth_m2m_private_key_accepts_all_fields() {
        match AuthConfig::oauth_m2m_private_key(
            Some("cid".to_string()),
            Some("/k.pem".to_string()),
            Some("kid".to_string()),
            Some("pass".to_string()),
            Some("ES256".to_string()),
            Some(vec!["all-apis".to_string()]),
            Some("https://h/token".to_string()),
        )
        .unwrap()
        {
            AuthConfig::OAuthM2MPrivateKey {
                client_id,
                jwt_key_file,
                jwt_kid,
                jwt_passphrase,
                jwt_algorithm,
                scopes,
                token_url_override,
            } => {
                assert_eq!(client_id, "cid");
                assert_eq!(jwt_key_file, "/k.pem");
                assert_eq!(jwt_kid, "kid");
                assert_eq!(jwt_passphrase.as_deref(), Some("pass"));
                assert_eq!(jwt_algorithm.as_deref(), Some("ES256"));
                assert_eq!(scopes, Some(vec!["all-apis".to_string()]));
                assert_eq!(token_url_override.as_deref(), Some("https://h/token"));
            }
            other => panic!("expected OAuthM2MPrivateKey, got {other:?}"),
        }
    }

    #[test]
    fn debug_oauth_m2m_private_key_redacts_passphrase() {
        let cfg = AuthConfig::oauth_m2m_private_key(
            Some("cid".to_string()),
            Some("/k.pem".to_string()),
            Some("kid".to_string()),
            Some("super-secret-pass".to_string()),
            Some("RS256".to_string()),
            None,
            None,
        )
        .unwrap();
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("<redacted>"), "{dbg}");
        assert!(!dbg.contains("super-secret-pass"), "{dbg}");
    }

    #[test]
    fn oauth_u2m_carries_identity_federation_client_id() {
        let auth = AuthConfig::oauth_u2m(
            None,
            None,
            None,
            None,
            None,
            None,
            Some("federation-client".to_string()),
        );

        assert_eq!(
            auth.identity_federation_client_id(),
            Some("federation-client")
        );
    }

    #[test]
    fn oauth_u2m_none_ports_stays_none() {
        // No ports supplied → redirect_ports is None (session fills the default).
        let auth = AuthConfig::oauth_u2m(None, None, None, None, None, None, None);
        match auth {
            AuthConfig::OAuthU2M { redirect_ports, .. } => assert_eq!(redirect_ports, None),
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn oauth_u2m_carries_redirect_ports_list() {
        // A caller (e.g. the connector's registered range) forwards the whole
        // list; a single-port binding forwards a one-element list.
        let auth = AuthConfig::oauth_u2m(
            None,
            Some(vec![8020, 8021, 8022]),
            None,
            None,
            None,
            None,
            None,
        );
        match auth {
            AuthConfig::OAuthU2M { redirect_ports, .. } => {
                assert_eq!(redirect_ports, Some(vec![8020, 8021, 8022]));
            }
            other => panic!("expected OAuthU2M, got {other:?}"),
        }
    }

    #[test]
    fn auth_variants_carry_identity_federation_client_id() {
        let auths = [
            AuthConfig::Pat {
                token: "token".to_string(),
                identity_federation_client_id: Some("federation-client".to_string()),
            },
            AuthConfig::OAuthM2M {
                client_id: "cid".to_string(),
                client_secret: "secret".to_string(),
                scopes: None,
                token_url_override: None,
                identity_federation_client_id: Some("federation-client".to_string()),
            },
            AuthConfig::OAuthU2M {
                client_id: None,
                redirect_ports: None,
                scopes: None,
                token_url_override: None,
                callback_timeout: None,
                token_cache: None,
                identity_federation_client_id: Some("federation-client".to_string()),
            },
        ];

        for auth in auths {
            assert_eq!(
                auth.identity_federation_client_id(),
                Some("federation-client")
            );
        }
    }

    #[test]
    fn oauth_federation_has_no_mandatory_federation_client_id() {
        let auth = AuthConfig::OAuthFederation {
            subject_token: "subject-token".to_string(),
            subject_token_type: "urn:ietf:params:oauth:token-type:jwt".to_string(),
            audience: "audience".to_string(),
        };

        assert_eq!(auth.identity_federation_client_id(), None);
    }

    // ─── ConnectionConfig parsing ──────────────────────────────────

    #[test]
    fn from_http_path_warehouses_form() {
        let c = ConnectionConfig::from_http_path("https://h", "/sql/1.0/warehouses/abc123")
            .expect("parse");
        assert_eq!(c.host, "https://h");
        assert_eq!(c.warehouse_id, "abc123");
        assert_eq!(c.org_id, None);
    }

    #[test]
    fn from_http_path_endpoints_form_is_equivalent() {
        let c = ConnectionConfig::from_http_path("h", "/sql/1.0/endpoints/abc123").expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
    }

    #[test]
    fn from_http_path_accepts_no_leading_slash() {
        let c = ConnectionConfig::from_http_path("h", "sql/1.0/warehouses/abc123").expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
    }

    #[test]
    fn from_http_path_strips_trailing_slash() {
        let c =
            ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123/").expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
    }

    #[test]
    fn from_http_path_captures_org_id() {
        let c = ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123?o=42")
            .expect("parse");
        assert_eq!(c.warehouse_id, "abc123");
        assert_eq!(c.org_id, Some("42".to_string()));
    }

    #[test]
    fn from_http_path_ignores_empty_org_id() {
        let c =
            ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123?o=").expect("parse");
        assert_eq!(c.org_id, None);
    }

    #[test]
    fn from_http_path_handles_multiple_query_params() {
        let c = ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/abc123?x=1&o=42&y=2")
            .expect("parse");
        assert_eq!(c.org_id, Some("42".to_string()));
    }

    #[test]
    fn from_http_path_rejects_unrecognised_path() {
        let err = ConnectionConfig::from_http_path("h", "/other/path").unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
        assert!(err.message.contains("/other/path"));
    }

    #[test]
    fn from_http_path_rejects_missing_warehouse_id() {
        // The prefix matches but the id segment is empty.
        let err = ConnectionConfig::from_http_path("h", "/sql/1.0/warehouses/").unwrap_err();
        assert_eq!(err.code, crate::kernel_error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn new_does_not_set_org_id() {
        let c = ConnectionConfig::new("https://h", "abc123");
        assert_eq!(c.org_id, None);
    }

    #[test]
    fn new_prepends_https_when_scheme_missing() {
        let c = ConnectionConfig::new("workspace.databricks.com", "abc");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    #[test]
    fn new_preserves_explicit_https() {
        let c = ConnectionConfig::new("https://workspace.databricks.com", "abc");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    #[test]
    fn new_preserves_explicit_http_for_test_servers() {
        // wiremock and other test servers use http://; don't force
        // https:// when the caller has explicitly opted into http.
        let c = ConnectionConfig::new("http://127.0.0.1:8080", "abc");
        assert_eq!(c.host, "http://127.0.0.1:8080");
    }

    #[test]
    fn new_strips_trailing_slash() {
        let c = ConnectionConfig::new("https://workspace.databricks.com/", "abc");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    #[test]
    fn from_http_path_normalises_host_too() {
        let c = ConnectionConfig::from_http_path(
            "workspace.databricks.com/",
            "/sql/1.0/warehouses/abc",
        )
        .expect("parse");
        assert_eq!(c.host, "https://workspace.databricks.com");
    }

    // ─── AuthConfig Debug redaction ────────────────────────────────

    #[test]
    fn debug_pat_redacts_token() {
        let c = AuthConfig::Pat {
            token: "dapi_secret".to_string(),
            identity_federation_client_id: None,
        };
        let s = format!("{c:?}");
        assert!(!s.contains("dapi_secret"), "token leaked: {s}");
        assert!(s.contains("<redacted>"));
    }

    #[test]
    fn debug_oauth_m2m_redacts_secret() {
        let c = AuthConfig::OAuthM2M {
            client_id: "client123".to_string(),
            client_secret: "supersecret".to_string(),
            scopes: None,
            token_url_override: None,
            identity_federation_client_id: None,
        };
        let s = format!("{c:?}");
        assert!(!s.contains("supersecret"), "secret leaked: {s}");
        assert!(s.contains("client123"));
        assert!(s.contains("<redacted>"));
    }

    #[test]
    fn debug_oauth_federation_redacts_subject_token() {
        let c = AuthConfig::OAuthFederation {
            subject_token: "supersecret".to_string(),
            subject_token_type: "urn:ietf:params:oauth:token-type:jwt".to_string(),
            audience: "databricks".to_string(),
        };
        let s = format!("{c:?}");
        assert!(!s.contains("supersecret"), "subject_token leaked: {s}");
        assert!(s.contains("<redacted>"));
        assert!(s.contains("databricks"));
    }

    // ─── TlsConfig ─────────────────────────────────────────────────

    #[test]
    fn tls_config_default_is_strict() {
        let tls = TlsConfig::default();
        assert!(tls.custom_ca_cert.is_none());
        assert!(!tls.accept_self_signed);
        assert!(!tls.skip_hostname_verification);
    }

    #[test]
    fn tls_config_debug_redacts_ca_cert_bytes() {
        // The PEM bytes themselves aren't secret, but a multi-kB blob
        // in every log line is unhelpful — Debug should summarise.
        let tls = TlsConfig {
            custom_ca_cert: Some(b"-----BEGIN CERTIFICATE-----...".to_vec()),
            accept_self_signed: false,
            skip_hostname_verification: false,
            ..TlsConfig::default()
        };
        let s = format!("{tls:?}");
        assert!(s.contains("<"), "expected byte-count summary, got: {s}");
        assert!(
            s.contains("bytes>"),
            "expected byte-count summary, got: {s}"
        );
        assert!(!s.contains("BEGIN CERTIFICATE"), "raw bytes leaked: {s}");
    }

    #[test]
    fn tls_config_debug_redacts_client_key() {
        // The mTLS private key is secret — Debug must show only its
        // presence, never its length or bytes.
        // The key's PEM header is assembled at runtime so the repo's
        // secret scanner doesn't flag a literal `BEGIN PRIVATE KEY`
        // marker — the body ("supersecret") is the thing under test.
        let key_pem = format!("-----BEGIN {} KEY-----supersecret", "PRIVATE");
        let tls = TlsConfig {
            client_cert_pem: Some(b"-----BEGIN CERTIFICATE-----cert".to_vec()),
            client_key_pem: Some(key_pem.into_bytes()),
            ..TlsConfig::default()
        };
        let s = format!("{tls:?}");
        assert!(s.contains("<redacted>"), "expected redaction, got: {s}");
        assert!(!s.contains("supersecret"), "private key leaked: {s}");
        // The cert (not secret) is summarised by byte count.
        assert!(
            s.contains("bytes>"),
            "expected cert byte-count summary, got: {s}"
        );
    }
}
