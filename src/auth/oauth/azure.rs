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

/// The effective Azure Databricks resource app id for `host` (the resource in
/// the M2M `.default` scope). Falls back to [`AZURE_DATABRICKS_APP_ID`] (prod)
/// for commercial prod and sovereign hosts.
pub(crate) fn effective_app_id(host: &str) -> &'static str {
    // Reduce to the host authority (drop scheme and any path) before matching,
    // so the suffix test can't be fooled by the pattern appearing mid-string.
    let host = host.to_ascii_lowercase();
    let authority = host
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(&host);
    let authority = authority
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(authority)
        .trim_end_matches('.');
    for (suffix, app_id) in AZURE_APP_IDS_BY_SUFFIX {
        // Match the host suffix, not a substring: a host that merely embeds the
        // pattern (e.g. `adb-1.dev.azuredatabricks.net.example.com`) is not dev/staging.
        if authority.ends_with(suffix) {
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
}
