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

//! Azure Entra ID cloud-awareness for the kernel's OAuth flows.
//!
//! The kernel is the auth core: rather than have each binding reconstruct the
//! Azure endpoints, scopes, and app ids, that knowledge lives here once and is
//! shared by every binding (mirroring how the Go driver's core infers the
//! cloud). This module holds the Azure constants and the endpoint/scope
//! builders used by the `azure-sp-m2m` (service-principal M2M) flow.
//!
//! (Azure U2M has no dedicated flow: the in-house `databricks-oauth` browser
//! flow works against Azure workspaces, so it is the default there.)

/// Databricks first-party "Azure Databricks" application id (production). Used
/// as the M2M `{app_id}/.default` scope resource fallback. Constant across
/// clouds; the connector's `DATABRICKS_AZURE_TENANT_ID` env override maps onto
/// the resource app id.
pub(crate) const AZURE_DATABRICKS_APP_ID: &str = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";

// ─── M2M (azure-sp-m2m) ──────────────────────────────────────────────────────

/// Entra (Azure AD) login host. The connector's SP M2M path hardcodes the
/// commercial host, so we match it; sovereign-cloud login hosts are a
/// documented follow-up (the tenant discovery below already reads the real
/// redirect host, so extending this is where sovereign support would land).
const AZURE_AAD_LOGIN_HOST: &str = "https://login.microsoftonline.com";

/// Per-environment Azure Databricks resource app id, keyed by host suffix —
/// used as the `{app_id}/.default` M2M scope resource. Mirrors the connectors'
/// `AzureAppId` / `get_effective_azure_login_app_id`: dev / staging have their
/// own app ids; everything else (prod commercial + sovereign) uses the prod id.
const AZURE_APP_IDS_BY_SUFFIX: &[(&str, &str)] = &[
    (
        ".dev.azuredatabricks.net",
        "62a912ac-b58e-4c1d-89ea-b2dbfc7358fc",
    ),
    (
        ".staging.azuredatabricks.net",
        "4a67d088-db5c-48f1-9ff2-0aace800ae68",
    ),
];

/// Azure sovereign-cloud workspace host suffixes. The M2M token exchange in
/// [`sp_m2m_token_url`] only targets the commercial [`AZURE_AAD_LOGIN_HOST`], so
/// these hosts cannot be served yet; [`reject_sovereign_host`] rejects them up
/// front with a clear message instead of letting the exchange fail opaquely
/// against the commercial directory. Wiring the matching sovereign login hosts
/// is the documented follow-up.
const AZURE_SOVEREIGN_HOST_SUFFIXES: &[&str] = &[
    ".databricks.azure.us", // Azure Government (USGov)
    ".databricks.azure.cn", // Azure China
];

/// Reduce a workspace URL/host to its lowercase host authority (scheme and any
/// path/query/fragment dropped, trailing dot trimmed), so a suffix test can't
/// be fooled by the pattern appearing in the scheme or path (e.g.
/// `adb-1.dev.azuredatabricks.net.example.com`).
fn host_authority(host: &str) -> String {
    let host = host.to_ascii_lowercase();
    let authority = host
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(&host);
    authority
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(authority)
        .trim_end_matches('.')
        .to_string()
}

/// The effective Azure Databricks resource app id for `host` (the resource in
/// the M2M `.default` scope). Falls back to [`AZURE_DATABRICKS_APP_ID`] (prod)
/// for commercial prod and sovereign hosts.
pub(crate) fn effective_app_id(host: &str) -> &'static str {
    let authority = host_authority(host);
    for (suffix, app_id) in AZURE_APP_IDS_BY_SUFFIX {
        // Match the host suffix, not a substring: a host that merely embeds the
        // pattern (e.g. `adb-1.dev.azuredatabricks.net.example.com`) is not dev/staging.
        if authority.ends_with(suffix) {
            return app_id;
        }
    }
    AZURE_DATABRICKS_APP_ID
}

/// Reject a known Azure sovereign-cloud workspace `host` on the `azure-sp-m2m`
/// path. Because [`sp_m2m_token_url`] hardcodes the commercial
/// [`AZURE_AAD_LOGIN_HOST`], a sovereign workspace — even with an explicit
/// `azure_tenant_id` — would exchange credentials against the *commercial*
/// Entra directory and fail with an opaque Entra error. This mirrors the
/// sovereign rejection [`tenant_from_login_url`] already performs on the
/// discovery redirect, so both the explicit-tenant and discovery halves fail
/// the same clear way until sovereign login hosts are wired (the documented
/// follow-up).
pub(crate) fn reject_sovereign_host(host: &str) -> crate::kernel_error::Result<()> {
    use crate::kernel_error::Error;
    let authority = host_authority(host);
    for suffix in AZURE_SOVEREIGN_HOST_SUFFIXES {
        if authority.ends_with(suffix) {
            return Err(Error::invalid_argument().message(format!(
                "azure-sp-m2m: sovereign cloud workspace {host:?} is not yet \
                 supported (the token exchange only targets the commercial \
                 Microsoft login host, so an explicit azure_tenant_id would hit \
                 the wrong directory) — this is a documented follow-up"
            )));
        }
    }
    Ok(())
}

/// The Entra **token** endpoint for the M2M client-credentials grant:
/// `{login_host}/{tenant}/oauth2/v2.0/token`. Direct to Entra (no workspace
/// redirect is available for a non-browser flow).
pub(crate) fn sp_m2m_token_url(tenant: &str) -> String {
    format!("{AZURE_AAD_LOGIN_HOST}/{tenant}/oauth2/v2.0/token")
}

/// The M2M scope: `{effective_app_id}/.default`.
pub(crate) fn sp_m2m_scope(host: &str) -> String {
    format!("{}/.default", effective_app_id(host))
}

/// The Azure Resource Manager scope for the **management token** (commercial
/// cloud). Matches the connector's `AZURE_MANAGED_RESOURCE`
/// (`https://management.core.windows.net/`), as a v2.0 `.default` scope.
const AZURE_MANAGEMENT_SCOPE: &str = "https://management.core.windows.net/.default";

/// Header carrying the SP's Azure Resource Manager token, so the Databricks
/// control plane can validate the SP's Azure RBAC role on the workspace.
const SP_MANAGEMENT_TOKEN_HEADER: &str = "X-Databricks-Azure-SP-Management-Token";
/// Header naming the workspace's ARM resource id (paired with the management
/// token for the RBAC lookup).
const WORKSPACE_RESOURCE_ID_HEADER: &str = "X-Databricks-Azure-Workspace-Resource-Id";

/// Azure service-principal M2M provider (parity with the JDBC/Thrift
/// connector's `AzureMSICredentialProvider` / SP credential provider).
///
/// The `Authorization` bearer is the Databricks-audience **data** token, which
/// alone authenticates the common workspace-member SP. The Azure Resource
/// Manager **management token** add-on — the header pair
/// `X-Databricks-Azure-SP-Management-Token` and
/// `X-Databricks-Azure-Workspace-Resource-Id`, which lets the control plane
/// authorize an SP that holds an Azure RBAC role but is not a workspace
/// member — is emitted **only when a workspace resource id is configured**,
/// matching the JDBC connector (`if (resourceId != null) { … }`).
/// Gating it this way keeps the ARM `.default` exchange (and its consent
/// requirement) off the request hot path for the common case: an app
/// registration without ARM access would otherwise fail *every* authenticated
/// request on that second exchange, even though the data token alone would
/// authenticate it. When configured, the management token is an ordinary
/// client-credentials exchange (reusing [`ClientCredentialsProvider`], which
/// refreshes it), so it stays fresh on long-lived sessions.
#[derive(Debug)]
pub(crate) struct AzureServicePrincipalProvider {
    data: super::m2m::ClientCredentialsProvider,
    /// `Some` only when `azure_workspace_resource_id` was supplied — the
    /// management token and its paired resource-id header are emitted together
    /// or not at all (JDBC parity).
    management: Option<super::m2m::ClientCredentialsProvider>,
    /// `Some` only when `azure_workspace_resource_id` was supplied.
    workspace_resource_id: Option<String>,
}

impl AzureServicePrincipalProvider {
    /// Build the provider. `tenant` is the (already-resolved) Entra tenant;
    /// `host` selects the resource app id for the data scope. Both the data and
    /// management token sources hit the same Entra token endpoint with
    /// different scopes.
    pub(crate) async fn new(
        host: &str,
        azure_client_id: &str,
        azure_client_secret: &str,
        tenant: &str,
        workspace_resource_id: Option<String>,
        http_client: std::sync::Arc<crate::client::http::DatabricksHttpClient>,
    ) -> crate::kernel_error::Result<Self> {
        Self::new_with_token_url(
            host,
            azure_client_id,
            azure_client_secret,
            sp_m2m_token_url(tenant),
            workspace_resource_id,
            http_client,
        )
        .await
    }

    /// Build the provider against an explicit Entra token endpoint. `new`
    /// derives that endpoint from the tenant (`sp_m2m_token_url`); this seam
    /// takes it directly so tests can point both token sources at a mock server
    /// (the real endpoint host, `login.microsoftonline.com`, is not
    /// interceptable).
    async fn new_with_token_url(
        host: &str,
        azure_client_id: &str,
        azure_client_secret: &str,
        token_url: String,
        workspace_resource_id: Option<String>,
        http_client: std::sync::Arc<crate::client::http::DatabricksHttpClient>,
    ) -> crate::kernel_error::Result<Self> {
        let data = super::m2m::ClientCredentialsProvider::new_with_full_config(
            host,
            azure_client_id,
            azure_client_secret,
            http_client.clone(),
            vec![sp_m2m_scope(host)],
            Some(token_url.clone()),
        )
        .await?;
        // Build the management-token source ONLY when a workspace resource id
        // is configured (JDBC parity: `if (resourceId != null)`). Without one,
        // the ARM `management.core.windows.net/.default` exchange — which an app
        // registration lacking ARM access cannot complete — would run on every
        // authenticated request via `extra_headers()` and break the common
        // workspace-member SP case that the data token alone authenticates.
        let management = match &workspace_resource_id {
            Some(_) => Some(
                super::m2m::ClientCredentialsProvider::new_with_full_config(
                    host,
                    azure_client_id,
                    azure_client_secret,
                    http_client,
                    vec![AZURE_MANAGEMENT_SCOPE.to_string()],
                    Some(token_url),
                )
                .await?,
            ),
            None => None,
        };
        Ok(Self {
            data,
            management,
            workspace_resource_id,
        })
    }
}

impl crate::auth::AuthProvider for AzureServicePrincipalProvider {
    fn get_auth_header(&self) -> crate::kernel_error::Result<String> {
        self.data.get_auth_header()
    }

    fn extra_headers(&self) -> crate::kernel_error::Result<Vec<(String, String)>> {
        // The management token + resource-id header pair is emitted only when a
        // workspace resource id was configured (JDBC parity: `if (resourceId !=
        // null)`). In the common workspace-member case both are absent and the
        // data token in the `Authorization` bearer authenticates alone — so the
        // ARM `.default` exchange never runs on the request hot path.
        let (Some(management), Some(rid)) = (&self.management, &self.workspace_resource_id) else {
            return Ok(Vec::new());
        };
        // The management header carries the RAW access token (no `Bearer `
        // prefix), matching the connector's `cloud_token.access_token`.
        let bearer = management.get_auth_header()?;
        let token = bearer
            .strip_prefix("Bearer ")
            .unwrap_or(&bearer)
            .to_string();
        Ok(vec![
            (SP_MANAGEMENT_TOKEN_HEADER.to_string(), token),
            (WORKSPACE_RESOURCE_ID_HEADER.to_string(), rid.clone()),
        ])
    }
}

/// Discover the workspace's Azure tenant id by reading the `{host}/aad/auth`
/// redirect to Entra (`https://login.microsoftonline.com/{tenant}/oauth2/…`),
/// exactly as the connector's `get_azure_tenant_id_from_host` does. The tenant
/// is the first path segment of the redirect Location. Used when the caller
/// does not pass an explicit `azure_tenant_id`, so `azure-sp-m2m` is zero-config
/// on the tenant (Thrift parity).
pub(crate) async fn discover_tenant(
    host: &str,
    http_client: &std::sync::Arc<crate::client::http::DatabricksHttpClient>,
) -> crate::kernel_error::Result<String> {
    use crate::kernel_error::Error;

    let url = format!("{}/aad/auth", host.trim_end_matches('/'));
    // Read the tenant off the FIRST `/aad/auth` redirect hop's `Location`
    // (matching the connector's `get_azure_tenant_id_from_host`), NOT the final
    // URL after following the whole chain. The workspace's first 3xx points at
    // `login.microsoftonline.com/{tenant}/oauth2/authorize?…`; parsing that hop
    // avoids assuming the tenant stays the first path segment across every
    // downstream Entra redirect (a `/common/` step or a `login.live.com` bounce
    // would otherwise yield a bogus "tenant" or trip the host guard).
    let location = http_client.first_redirect_location(&url).await?;
    let location = location.ok_or_else(|| {
        Error::unauthenticated().message(format!(
            "Azure tenant discovery: {url} did not return a redirect Location; \
             pass azure_tenant_id explicitly"
        ))
    })?;
    let redirect_url = reqwest::Url::parse(&location).map_err(|e| {
        Error::unauthenticated().message(format!(
            "Azure tenant discovery: {url} redirect Location {location:?} is not a \
             valid URL ({e}); pass azure_tenant_id explicitly"
        ))
    })?;
    tenant_from_login_url(&redirect_url)
}

/// Parse the Azure tenant id from the Entra login URL the workspace's
/// `/aad/auth` redirects to (`https://login.microsoftonline.com/{tenant}/…`).
/// The tenant is the first path segment; the host must be the **commercial**
/// Microsoft login host (guards against a non-AAD redirect on a misconfigured
/// workspace). Sovereign clouds (e.g. `chinacloudapi.cn`) are intentionally
/// rejected here: [`sp_m2m_token_url`] only builds against the commercial
/// [`AZURE_AAD_LOGIN_HOST`], so accepting a sovereign redirect would let
/// discovery succeed while the token exchange silently hits the wrong
/// directory. Rejecting keeps the two halves consistent until sovereign login
/// hosts are wired (the documented follow-up), and steers the caller to pass
/// `azure_tenant_id` explicitly. Extracted so the parse is unit-testable
/// without a live workspace (the HTTP round-trip itself is verified against a
/// real Azure workspace).
fn tenant_from_login_url(final_url: &reqwest::Url) -> crate::kernel_error::Result<String> {
    use crate::kernel_error::Error;
    let host = final_url
        .host_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !host.contains("microsoftonline") {
        return Err(Error::unauthenticated().message(format!(
            "Azure tenant discovery: /aad/auth did not redirect to the commercial \
             Microsoft login host (got {host:?}); sovereign clouds are not yet \
             supported for azure-sp-m2m — pass azure_tenant_id explicitly"
        )));
    }
    final_url
        .path_segments()
        .and_then(|mut segments| segments.next())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            Error::unauthenticated().message(format!(
                "Azure tenant discovery: could not parse a tenant id from {final_url}; \
                 pass azure_tenant_id explicitly"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m2m_token_url_is_entra_v2_per_tenant() {
        assert_eq!(
            sp_m2m_token_url("tenant-123"),
            "https://login.microsoftonline.com/tenant-123/oauth2/v2.0/token"
        );
    }

    #[test]
    fn m2m_scope_is_effective_app_default() {
        // Prod / commercial → the prod Databricks Azure app id.
        assert_eq!(
            sp_m2m_scope("https://adb-1.4.azuredatabricks.net"),
            format!("{AZURE_DATABRICKS_APP_ID}/.default")
        );
    }

    #[test]
    fn effective_app_id_is_host_specific() {
        assert_eq!(
            effective_app_id("https://adb-1.azuredatabricks.net"),
            AZURE_DATABRICKS_APP_ID
        );
        assert_eq!(
            effective_app_id("https://adb-1.dev.azuredatabricks.net"),
            "62a912ac-b58e-4c1d-89ea-b2dbfc7358fc"
        );
        assert_eq!(
            effective_app_id("https://adb-1.staging.azuredatabricks.net"),
            "4a67d088-db5c-48f1-9ff2-0aace800ae68"
        );
        // Sovereign / unknown → prod app id fallback.
        assert_eq!(
            effective_app_id("https://adb-1.databricks.azure.us"),
            AZURE_DATABRICKS_APP_ID
        );
        // A host that merely embeds the dev/staging pattern mid-string must not
        // be misclassified — suffix match, not substring.
        assert_eq!(
            effective_app_id("https://adb-1.dev.azuredatabricks.net.example.com"),
            AZURE_DATABRICKS_APP_ID
        );
        assert_eq!(
            effective_app_id("https://adb-1.staging.azuredatabricks.net.evil.com"),
            AZURE_DATABRICKS_APP_ID
        );
    }

    #[test]
    fn tenant_parsed_from_entra_login_url() {
        // Matches the real /aad/auth redirect target verified against a live
        // workspace: login.microsoftonline.com/{tenant}/oauth2/authorize?…
        let url = reqwest::Url::parse(
            "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/authorize?client_id=x",
        )
        .unwrap();
        assert_eq!(
            tenant_from_login_url(&url).unwrap(),
            "9f37a392-f0ae-4280-9796-f1864a10effc"
        );
    }

    #[test]
    fn tenant_parse_rejects_non_microsoft_host() {
        // A workspace that redirects somewhere unexpected must not yield a bogus
        // "tenant" — fail so the caller passes azure_tenant_id explicitly.
        let url = reqwest::Url::parse("https://evil.example.com/not-a-tenant/x").unwrap();
        assert!(tenant_from_login_url(&url).is_err());
    }

    #[test]
    fn tenant_parse_rejects_sovereign_login_host() {
        // A China sovereign workspace redirects to login.chinacloudapi.cn, but
        // sp_m2m_token_url only targets the commercial AZURE_AAD_LOGIN_HOST — so
        // discovery must reject sovereign hosts rather than let the token
        // exchange silently hit the wrong directory. Consistency guard for the
        // documented sovereign follow-up.
        let url = reqwest::Url::parse(
            "https://login.chinacloudapi.cn/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/authorize?client_id=x",
        )
        .unwrap();
        assert!(tenant_from_login_url(&url).is_err());
    }

    #[test]
    fn reject_sovereign_host_matches_known_sovereign_suffixes() {
        // USGov and China workspace hosts are rejected: sp_m2m_token_url only
        // targets the commercial login host, so an explicit azure_tenant_id
        // would otherwise hit the wrong directory and fail opaquely.
        assert!(reject_sovereign_host("https://adb-1.11.databricks.azure.us").is_err());
        assert!(reject_sovereign_host("https://adb-1.11.databricks.azure.cn").is_err());
    }

    #[test]
    fn reject_sovereign_host_allows_commercial_and_dev_staging() {
        // Commercial prod, dev, and staging are all served by the commercial
        // login host, so they pass the guard.
        assert!(reject_sovereign_host("https://adb-1.4.azuredatabricks.net").is_ok());
        assert!(reject_sovereign_host("https://adb-1.dev.azuredatabricks.net").is_ok());
        assert!(reject_sovereign_host("https://adb-1.staging.azuredatabricks.net").is_ok());
        // A host that merely embeds a sovereign suffix mid-string is not
        // sovereign — suffix match on the authority, not a substring.
        assert!(reject_sovereign_host("https://adb-1.databricks.azure.us.example.com").is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn discover_tenant_reads_first_aad_auth_redirect_hop() {
        // `/aad/auth` returns a 302 whose Location is the Entra authorize URL
        // carrying the tenant as its first path segment. We must read that FIRST
        // hop's Location and NOT follow the redirect chain onward to
        // login.microsoftonline.com (which is unreachable/uninterceptable and,
        // more importantly, might traverse a non-tenant-prefixed hop). Mounting
        // ONLY `/aad/auth` proves discovery never issues a second request.
        use crate::client::http::{DatabricksHttpClient, HttpClientConfig};
        use std::sync::Arc;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/aad/auth"))
            .respond_with(ResponseTemplate::new(302).insert_header(
                "location",
                "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/authorize?client_id=x",
            ))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("http client"),
        );
        let tenant = discover_tenant(&mock_server.uri(), &http_client)
            .await
            .expect("tenant discovered");
        assert_eq!(tenant, "9f37a392-f0ae-4280-9796-f1864a10effc");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn discover_tenant_errors_when_no_redirect() {
        // A 200 (no Location) means the workspace isn't Azure / isn't
        // redirecting — surface an explicit error steering to azure_tenant_id.
        use crate::client::http::{DatabricksHttpClient, HttpClientConfig};
        use std::sync::Arc;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/aad/auth"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("http client"),
        );
        assert!(discover_tenant(&mock_server.uri(), &http_client)
            .await
            .is_err());
    }

    // ─── AzureServicePrincipalProvider header composition ────────────────────
    //
    // The Entra token endpoint, the `{app}/.default` scope, and the
    // client-credentials exchange are unit-tested elsewhere. These cover the
    // composition unique to this provider: `Authorization` is always the data
    // token; the `X-Databricks-Azure-SP-Management-Token` +
    // `X-Databricks-Azure-Workspace-Resource-Id` pair is emitted ONLY when a
    // workspace resource id is configured (JDBC parity), and neither header is
    // present otherwise. `new_with_token_url` points both token sources at a
    // mock server.
    use crate::auth::AuthProvider as _;
    use wiremock::matchers::body_string_contains;

    /// Mount two token responses on the same Entra endpoint, differentiated by
    /// the scope in the request body: the data scope (`{app}/.default`, contains
    /// `2ff814a6`) returns `data-token`; the management scope
    /// (`management.core.windows.net`) returns `mgmt-token`.
    async fn mount_dual_token_endpoint(mock_server: &wiremock::MockServer) {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, ResponseTemplate};
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/v2.0/token"))
            .and(body_string_contains("management.core.windows.net"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "mgmt-token", "token_type": "Bearer", "expires_in": 3600,
            })))
            .mount(mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/v2.0/token"))
            .and(body_string_contains("2ff814a6"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "data-token", "token_type": "Bearer", "expires_in": 3600,
            })))
            .mount(mock_server)
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sp_m2m_emits_management_token_and_resource_id_when_set() {
        use crate::client::http::{DatabricksHttpClient, HttpClientConfig};
        use std::sync::Arc;
        let mock_server = wiremock::MockServer::start().await;
        mount_dual_token_endpoint(&mock_server).await;
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).expect("client"),
        );
        let token_url = format!("{}/tenant-abc/oauth2/v2.0/token", mock_server.uri());
        let provider = AzureServicePrincipalProvider::new_with_token_url(
            "https://adb-1.4.azuredatabricks.net",
            "sp-client-id",
            "sp-client-secret",
            token_url,
            Some("/subscriptions/s/resourceGroups/rg/workspace/w".to_string()),
            http_client,
        )
        .await
        .expect("provider builds");

        assert_eq!(provider.get_auth_header().unwrap(), "Bearer data-token");
        let extra = provider.extra_headers().unwrap();
        assert!(extra.contains(&(
            SP_MANAGEMENT_TOKEN_HEADER.to_string(),
            "mgmt-token".to_string()
        )));
        assert!(extra.contains(&(
            WORKSPACE_RESOURCE_ID_HEADER.to_string(),
            "/subscriptions/s/resourceGroups/rg/workspace/w".to_string()
        )));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sp_m2m_omits_management_token_when_resource_id_unset() {
        use crate::client::http::{DatabricksHttpClient, HttpClientConfig};
        use std::sync::Arc;
        let mock_server = wiremock::MockServer::start().await;
        mount_dual_token_endpoint(&mock_server).await;
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default()).expect("client"),
        );
        let token_url = format!("{}/tenant-abc/oauth2/v2.0/token", mock_server.uri());
        let provider = AzureServicePrincipalProvider::new_with_token_url(
            "https://adb-1.4.azuredatabricks.net",
            "sp-client-id",
            "sp-client-secret",
            token_url,
            None,
            http_client,
        )
        .await
        .expect("provider builds");

        // The data token alone authenticates the common workspace-member SP.
        assert_eq!(provider.get_auth_header().unwrap(), "Bearer data-token");
        // Without a workspace resource id, neither the management token nor the
        // resource-id header is emitted (JDBC parity) — so the ARM `.default`
        // exchange never runs on the request hot path.
        let extra = provider.extra_headers().unwrap();
        assert!(extra.is_empty());
    }
}
