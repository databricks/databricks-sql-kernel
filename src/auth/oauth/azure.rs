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
//! builders used by the `azure-oauth` (U2M) and `azure-sp-m2m` (M2M) flows.
//!
//! **U2M (`azure-oauth`)** uses the **workspace-hosted v2.0** endpoints
//! (`{host}/oidc/oauth2/v2.0/{authorize,token}`) — the same pattern the Go
//! driver uses. The workspace redirects the browser to Entra, so no tenant is
//! needed. This is deliberately NOT the OIDC-discovered `{host}/oidc/v1/authorize`
//! path: a live test showed the workspace redirects that to a malformed
//! `login.microsoftonline.com/{tenant}/v1/authorize` (404), whereas the
//! `/oidc/oauth2/v2.0/authorize` path redirects to a valid Entra URL.

/// Databricks first-party "Azure Databricks" application id (production). Used
/// as the default resource in the U2M delegated scope and as the M2M `.default`
/// resource. Constant across clouds for the scope; the connector's
/// `DATABRICKS_AZURE_TENANT_ID` env override maps onto the `app_id` argument.
pub(crate) const AZURE_DATABRICKS_APP_ID: &str = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";

/// OAuth client id for the Azure U2M browser flow. Matches the connectors'
/// `PYSQL_OAUTH_AZURE_CLIENT_ID` / Go's `azureClientId`.
pub(crate) const AZURE_U2M_CLIENT_ID: &str = "96eecda7-19ea-49cc-abb5-240097d554f5";

/// Default localhost redirect port registered for [`AZURE_U2M_CLIENT_ID`].
pub(crate) const AZURE_U2M_REDIRECT_PORT: u16 = 8030;

/// The workspace-hosted v2.0 **authorize** endpoint for the Azure U2M flow.
/// The workspace redirects this to the tenant's Entra authorize URL.
pub(crate) fn u2m_authorize_url(host: &str) -> String {
    format!("{}/oidc/oauth2/v2.0/authorize", host.trim_end_matches('/'))
}

/// The workspace-hosted v2.0 **token** endpoint for the Azure U2M flow.
pub(crate) fn u2m_token_url(host: &str) -> String {
    format!("{}/oidc/oauth2/v2.0/token", host.trim_end_matches('/'))
}

/// The AAD delegated scope set for U2M: `{app_id}/user_impersonation` plus
/// `offline_access` (so a refresh token is issued). `app_id` defaults to
/// [`AZURE_DATABRICKS_APP_ID`] when the caller doesn't override it.
pub(crate) fn u2m_scopes(app_id: Option<&str>) -> Vec<String> {
    let app = app_id.unwrap_or(AZURE_DATABRICKS_APP_ID);
    vec![
        format!("{app}/user_impersonation"),
        "offline_access".to_string(),
    ]
}

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

/// The effective Azure Databricks resource app id for `host` (the resource in
/// the M2M `.default` scope). Falls back to [`AZURE_DATABRICKS_APP_ID`] (prod)
/// for commercial prod and sovereign hosts.
pub(crate) fn effective_app_id(host: &str) -> &'static str {
    let host = host.trim_end_matches('/').to_ascii_lowercase();
    for (suffix, app_id) in AZURE_APP_IDS_BY_SUFFIX {
        // Match the host component, ignoring scheme/path.
        if host.contains(suffix) {
            return app_id;
        }
    }
    AZURE_DATABRICKS_APP_ID
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
pub(crate) const AZURE_MANAGEMENT_SCOPE: &str = "https://management.core.windows.net/.default";

/// Header carrying the SP's Azure Resource Manager token, so the Databricks
/// control plane can validate the SP's Azure RBAC role on the workspace.
const SP_MANAGEMENT_TOKEN_HEADER: &str = "X-Databricks-Azure-SP-Management-Token";
/// Header naming the workspace's ARM resource id (paired with the management
/// token for the RBAC lookup).
const WORKSPACE_RESOURCE_ID_HEADER: &str = "X-Databricks-Azure-Workspace-Resource-Id";

/// Azure service-principal M2M provider. The `Authorization` bearer is the
/// Databricks-audience data token; when a workspace resource id is configured
/// it additionally emits the management-token pair via [`AuthProvider::extra_headers`]
/// so the control plane can authorize an SP that has an Azure RBAC role but is
/// not (yet) a workspace member. Both tokens are ordinary client-credentials
/// exchanges (reusing [`ClientCredentialsProvider`], which refreshes them), so
/// the management token stays fresh on long-lived sessions.
#[derive(Debug)]
pub(crate) struct AzureServicePrincipalProvider {
    data: super::m2m::ClientCredentialsProvider,
    /// `Some` only when `azure_workspace_resource_id` was supplied.
    management: Option<ManagementHeaders>,
}

#[derive(Debug)]
struct ManagementHeaders {
    token_source: super::m2m::ClientCredentialsProvider,
    workspace_resource_id: String,
}

impl AzureServicePrincipalProvider {
    /// Build the provider. `tenant` is the (already-resolved) Entra tenant;
    /// `host` selects the resource app id for the data scope. The management
    /// token source is created only when `workspace_resource_id` is `Some`.
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
    /// interceptable). Both the data-scope and management-scope client-
    /// credentials grants hit the same `token_url` with different scopes.
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

        let management = match workspace_resource_id {
            Some(rid) => {
                let token_source = super::m2m::ClientCredentialsProvider::new_with_full_config(
                    host,
                    azure_client_id,
                    azure_client_secret,
                    http_client,
                    vec![AZURE_MANAGEMENT_SCOPE.to_string()],
                    Some(token_url),
                )
                .await?;
                Some(ManagementHeaders {
                    token_source,
                    workspace_resource_id: rid,
                })
            }
            None => None,
        };

        Ok(Self { data, management })
    }
}

impl crate::auth::AuthProvider for AzureServicePrincipalProvider {
    fn get_auth_header(&self) -> crate::kernel_error::Result<String> {
        self.data.get_auth_header()
    }

    fn extra_headers(&self) -> crate::kernel_error::Result<Vec<(String, String)>> {
        let Some(mgmt) = &self.management else {
            return Ok(Vec::new());
        };
        // The management header carries the RAW access token (no `Bearer `
        // prefix), matching the connector's `cloud_token.access_token`.
        let bearer = mgmt.token_source.get_auth_header()?;
        let token = bearer
            .strip_prefix("Bearer ")
            .unwrap_or(&bearer)
            .to_string();
        Ok(vec![
            (
                WORKSPACE_RESOURCE_ID_HEADER.to_string(),
                mgmt.workspace_resource_id.clone(),
            ),
            (SP_MANAGEMENT_TOKEN_HEADER.to_string(), token),
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
    use crate::client::retry::RequestType;
    use crate::kernel_error::Error;

    let url = format!("{}/aad/auth", host.trim_end_matches('/'));
    let request = http_client.inner().get(&url).build().map_err(|e| {
        Error::io().message(format!(
            "Failed to build Azure tenant-discovery request: {e}"
        ))
    })?;
    // The workspace `/aad/auth` 302-redirects to
    // `login.microsoftonline.com/{tenant}/oauth2/authorize?…`. The kernel HTTP
    // client follows redirects, so the final URL still carries the tenant as
    // its first path segment (the tenant stays in the path throughout the
    // Microsoft login flow). Parse it from there.
    let response = http_client
        .execute_without_auth(request, RequestType::AuthDiscovery)
        .await?;
    tenant_from_login_url(response.url())
}

/// Parse the Azure tenant id from the Entra login URL the workspace's
/// `/aad/auth` redirects to (`https://login.microsoftonline.com/{tenant}/…`).
/// The tenant is the first path segment; the host must be a Microsoft login
/// host (guards against a non-AAD redirect on a misconfigured workspace).
/// Extracted so the parse is unit-testable without a live workspace (the HTTP
/// round-trip itself is verified against a real Azure workspace).
fn tenant_from_login_url(final_url: &reqwest::Url) -> crate::kernel_error::Result<String> {
    use crate::kernel_error::Error;
    let host = final_url
        .host_str()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !host.contains("microsoftonline") && !host.contains("chinacloudapi") {
        return Err(Error::unauthenticated().message(format!(
            "Azure tenant discovery: /aad/auth did not redirect to a Microsoft \
             login host (got {host:?}); pass azure_tenant_id explicitly"
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
    fn u2m_endpoints_are_workspace_v2_paths() {
        // The v2.0 path is the fix for the live 404: /oidc/v1/authorize
        // redirects to a malformed Entra URL, /oidc/oauth2/v2.0/authorize does not.
        let host = "https://adb-123.4.azuredatabricks.net";
        assert_eq!(
            u2m_authorize_url(host),
            "https://adb-123.4.azuredatabricks.net/oidc/oauth2/v2.0/authorize"
        );
        assert_eq!(
            u2m_token_url(host),
            "https://adb-123.4.azuredatabricks.net/oidc/oauth2/v2.0/token"
        );
    }

    #[test]
    fn u2m_endpoints_normalise_trailing_slash() {
        let host = "https://adb-123.4.azuredatabricks.net/";
        assert_eq!(
            u2m_authorize_url(host),
            "https://adb-123.4.azuredatabricks.net/oidc/oauth2/v2.0/authorize"
        );
    }

    #[test]
    fn u2m_scopes_default_to_databricks_app_impersonation() {
        assert_eq!(
            u2m_scopes(None),
            vec![
                format!("{AZURE_DATABRICKS_APP_ID}/user_impersonation"),
                "offline_access".to_string()
            ]
        );
    }

    #[test]
    fn u2m_scopes_honour_app_id_override() {
        let scopes = u2m_scopes(Some("custom-app-id"));
        assert_eq!(scopes[0], "custom-app-id/user_impersonation");
        assert_eq!(scopes[1], "offline_access");
    }

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

    // ─── AzureServicePrincipalProvider (two-token composition) ───────────────
    //
    // The Entra token endpoint (`sp_m2m_token_url`), the `{app}/.default` scope,
    // and the client-credentials exchange+refresh are each unit-tested
    // elsewhere. These tests cover the composition unique to this provider: the
    // `Authorization` bearer is always the *data* token, and the management
    // header pair is emitted (with the raw, un-`Bearer`-prefixed token) only
    // when a workspace resource id is configured. A `new_with_token_url` seam
    // points both token sources at a mock server (the real `new` derives the
    // same URL from the tenant, which is `login.microsoftonline.com` and not
    // interceptable).
    use crate::auth::AuthProvider;
    use crate::client::http::{DatabricksHttpClient, HttpClientConfig};
    use std::sync::Arc;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Mount two token responses on the same Entra endpoint, differentiated by
    /// the scope carried in the request body: the Databricks-audience data
    /// scope (`{app}/.default`, contains `2ff814a6`) returns `data-token`; the
    /// management scope (`management.core.windows.net`) returns `mgmt-token`.
    async fn mount_dual_token_endpoint(mock_server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/v2.0/token"))
            .and(body_string_contains("management.core.windows.net"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "mgmt-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            })))
            .mount(mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/v2.0/token"))
            .and(body_string_contains("2ff814a6"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "data-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            })))
            .mount(mock_server)
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sp_m2m_with_resource_id_emits_management_header_pair() {
        let mock_server = MockServer::start().await;
        mount_dual_token_endpoint(&mock_server).await;
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("http client"),
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

        // Authorization is always the data (Databricks-audience) token.
        assert_eq!(provider.get_auth_header().unwrap(), "Bearer data-token");

        // Both management headers present; the token is raw (no `Bearer ` prefix).
        let extra = provider.extra_headers().unwrap();
        assert!(extra.contains(&(
            WORKSPACE_RESOURCE_ID_HEADER.to_string(),
            "/subscriptions/s/resourceGroups/rg/workspace/w".to_string()
        )));
        assert!(extra.contains(&(
            SP_MANAGEMENT_TOKEN_HEADER.to_string(),
            "mgmt-token".to_string()
        )));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sp_m2m_without_resource_id_sends_data_token_only() {
        let mock_server = MockServer::start().await;
        // The management scope must never be requested when no resource id is set.
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/v2.0/token"))
            .and(body_string_contains("management.core.windows.net"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tenant-abc/oauth2/v2.0/token"))
            .and(body_string_contains("2ff814a6"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "data-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            })))
            .mount(&mock_server)
            .await;
        let http_client = Arc::new(
            DatabricksHttpClient::with_default_retry(HttpClientConfig::default())
                .expect("http client"),
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

        assert_eq!(provider.get_auth_header().unwrap(), "Bearer data-token");
        assert!(provider.extra_headers().unwrap().is_empty());
    }
}
