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
}
