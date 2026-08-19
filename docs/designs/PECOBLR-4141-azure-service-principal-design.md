<!--
  Copyright (c) 2026 Databricks, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Azure Entra ID OAuth — kernel as the Azure-aware auth core (PECOBLR-4141 / 4120)

## Direction

**The kernel is the auth core; bindings are thin.** The kernel owns Azure
cloud-awareness — it detects the cloud from the workspace host, constructs the
correct Entra endpoints, auto-discovers the tenant, and applies the Azure client
id / scope / redirect-port defaults. Each binding forwards only its native auth
selector (+ the SP credentials for M2M); it does **not** build URLs, map scopes,
or discover tenants.

This supersedes the earlier "generic-override, kernel stays cloud-agnostic"
draft. The generic `token_url` / `scopes` overrides added in the first cut of
this PR **remain** as primitives (the Azure layer sets them internally, and they
stay usable for non-Azure custom endpoints), but Azure resolution now lives in
the kernel.

### Why (and the correction that motivated it)

A live run against a real Azure workspace showed the generic approach's fatal
gap: the kernel's OIDC discovery returns `{host}/oidc/v1/authorize`, and the
workspace redirects that to a **malformed** `login.microsoftonline.com/{tenant}/
v1/authorize` (404). Every connector instead uses `{host}/oidc/oauth2/v2.0/
authorize`, which redirects to a valid Entra URL. Getting that right — plus
tenant discovery, per-cloud hosts, and the Azure defaults — is knowledge that
would otherwise be duplicated into every binding adapter. Putting it in the
kernel (as the Go driver's core already does) keeps it in one place.

## Non-negotiable: invocation parity with Thrift

Switching a connection to `use_kernel=True` must **not** change how a user
invokes Azure auth. The kernel API must let each binding preserve its exact
Thrift-path `connect()` surface:

| Driver | Azure U2M — user passes | Azure M2M — user passes |
| --- | --- | --- |
| **Python** | `auth_type="azure-oauth"` | `auth_type="azure-sp-m2m"` + `azure_client_id` / `azure_client_secret`; **`azure_tenant_id` optional** (auto-discovered) |
| **Go** | `authType=OauthU2M` (Azure inferred from host; no azure param) | `authType=OauthM2M` + `clientID`/`clientSecret` — a *Databricks* SP (workspace OIDC), **not** Azure AD |
| **Node** | `authType='databricks-oauth'` + `azureTenantId` | `authType='databricks-oauth'` + `oauthClientId`/`Secret` + `azureTenantId` |

Consequences the kernel API must satisfy:
- `azure_tenant_id` is **optional** everywhere (kernel discovers it) — Thrift
  parity, and a correction to the first cut which wrongly required it.
- The kernel exposes **both** a Databricks-SP M2M (`oauth-m2m`, workspace OIDC)
  and an Azure-AD-SP M2M (`azure-sp-m2m`, Entra) — Go's "Azure M2M" maps to the
  former, Python's `azure-sp-m2m` to the latter.
- Each binding's thin adapter normalizes its native selector (Python's explicit
  `auth_type`, Go's host-inference, Node's `azureTenantId` flag) onto the kernel
  Azure API; the kernel accepts an explicit Azure signal and does the rest.

## What the kernel owns — `src/auth/oauth/azure.rs` (new)

1. **Cloud / environment detection** from the host suffix:
   `.azuredatabricks.net` → Public, `.databricks.azure.us` → USGov,
   `.databricks.azure.cn` → China. Per-cloud login host + Databricks app-id
   table (Public/dev/staging app ids from the connector's `AzureAppId`; Gov/China
   use the prod app id). Mirrors the Go driver's `azureTenants` map.
2. **Endpoint construction** (see per-flow sections).
3. **Tenant auto-discovery**: `GET {host}/aad/auth`, read the redirect to
   `login.microsoftonline.com/{tenant}/…`, parse the tenant from the path.
   Sovereign-aware (reads the real redirect host). Only needed for M2M.
4. **Azure defaults**: U2M client id `96eecda7-19ea-49cc-abb5-240097d554f5`,
   port `8030`; scope app-id `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` (overridable
   via `DATABRICKS_AZURE_TENANT_ID`, matching the connector).

## `azure-oauth` (U2M) — PECOBLR-4120

Browser authorization-code + PKCE, driven by the kernel's existing U2M provider
with Azure endpoints/values:

- **Authorize + token**: `{host}/oidc/oauth2/v2.0/authorize` and
  `{host}/oidc/oauth2/v2.0/token` — the **workspace-host v2.0** paths (the Go
  driver's proven pattern). No tenant needed: the workspace redirects the
  authorize to Entra. This is the exact fix for the live 404 (`/oidc/v1/*` →
  `/oidc/oauth2/v2.0/*`).
- **Client id** `96eecda7-…`, **scope** `{app_id}/user_impersonation
  offline_access`, **redirect port** `8030`.
- The kernel gains an **authorize-endpoint override** internally so the Azure
  path can pin `/oidc/oauth2/v2.0/authorize` instead of the discovered
  `/oidc/v1/authorize`. (Discovery is otherwise unchanged for non-Azure U2M.)

## `azure-sp-m2m` (M2M) — PECOBLR-4141

Client-credentials against **Entra directly** (no browser, so no workspace
redirect available):

- **Token endpoint**: `{login_host}/{tenant}/oauth2/v2.0/token`, tenant
  **auto-discovered** via `/aad/auth` when `azure_tenant_id` is omitted.
- **Scope**: `{effective_app_id}/.default` (app id from the host's cloud).
- **Credentials**: `azure_client_id` / `azure_client_secret` via HTTP Basic
  (Entra v2.0 advertises `client_secret_basic`; verified).
- **Management token / `azure_workspace_resource_id`: still not implemented.**
  No SQL connector (Go, Node) uses it; Python's Thrift path sends it but the SQL
  data path doesn't require it for a workspace-member SP. An SP that is not a
  workspace member (RBAC-only) remains unsupported on the kernel path,
  documented. Revisit only with a concrete SQL customer need; the seam would be a
  second-token + refreshing-extra-header mechanism.

## Federation

Unchanged. `FederationProvider` still wraps every flow; an Entra-issued token is
foreign-issuer and round-trips through `{host}/oidc/v1/token` (benign on Azure,
required for an Azure SP reaching a non-Azure workspace), with the non-breaking
fallback.

## AuthConfig

Two new variants (bindings map onto them):

```rust
AuthConfig::AzureOAuthU2M {
    client_id: Option<String>,        // None → 96eecda7-…
    redirect_ports: Option<Vec<u16>>, // None → [8030]
    app_id: Option<String>,           // scope resource; None → 2ff814a6-… / env
    token_cache: Option<TokenCacheConfig>,
    identity_federation_client_id: Option<String>,
}
AuthConfig::AzureServicePrincipal {   // azure-sp-m2m
    azure_client_id: String,
    azure_client_secret: String,
    azure_tenant_id: Option<String>,  // None → auto-discover via /aad/auth
    identity_federation_client_id: Option<String>,
}
```

The existing `OAuthU2M` / `OAuthM2M` variants are unchanged (Databricks-native
flows, incl. Go's Databricks-SP M2M and the workspace-OIDC databricks-oauth).

## Thin bindings

- **pyo3**: `auth_type="azure-oauth"` → `AzureOAuthU2M`; `auth_type="azure-sp-m2m"`
  + `azure_*` kwargs → `AzureServicePrincipal`. No endpoint/scope/tenant logic.
- **napi**: `AuthMode::AzureOAuth` / `AzureSpM2m` + `azure*` fields.
- **C ABI**: `set_auth_azure_oauth` / `set_auth_azure_sp_m2m` setters.
- **Connector #919**: `auth_bridge.py` simplifies to selector-mapping only —
  forwards `auth_type` + azure creds, stops constructing URLs / scopes and stops
  discovering the tenant (the kernel does it). Drops the kernel-path dependency
  on `AzureOAuthEndpointCollection` / `get_azure_tenant_id_from_host`.

## Testing

- **Unit (`azure.rs`)**: cloud/env detection per host suffix; endpoint
  construction (U2M workspace v2.0; M2M Entra token URL per cloud); scope
  app-id defaulting + `DATABRICKS_AZURE_TENANT_ID` override.
- **Wiremock**: tenant discovery (`/aad/auth` 302 → parse tenant); `azure-sp-m2m`
  hits the Entra token endpoint with Basic + `.default` scope; `azure-oauth`
  uses the `/oidc/oauth2/v2.0/authorize` path (regression for the live 404).
- **Binding boundary**: pyo3 kwargs accepted; napi/C-ABI selectors record values.
- **Live (required before GA)**: `azure-oauth` U2M against a real Azure workspace
  (browser) — confirm the v2.0 authorize path fixes the 404 and the query runs;
  `azure-sp-m2m` with a real workspace-member Entra SP; sovereign-cloud spot check.

## Status / evolution

- First cut: generic overrides, kernel cloud-agnostic (shipped in this PR's early
  commits). Retained as primitives.
- Live test found the `/oidc/v1/authorize` 404 → authorize must be v2.0.
- Decision (team): kernel is the auth core, bindings thin → Azure resolution
  moves into the kernel (this doc). Management token stays out.
