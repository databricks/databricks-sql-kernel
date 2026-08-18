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

# Azure Entra ID Service-Principal (M2M) Auth Design — PECOBLR-4141

## Overview

The kernel supports PAT, OAuth M2M (Databricks client credentials), and OAuth
U2M (browser). `databricks-sql-python` users who authenticate with an **Azure
service principal** (`azure_client_id` / `azure_client_secret` /
`azure_tenant_id` / `azure_workspace_resource_id`) cannot yet run on the kernel
path: those credentials are **Microsoft Entra ID (Azure AD) app-registration**
credentials, and the token must be minted by **Entra**
(`login.microsoftonline.com`), not by the workspace OIDC endpoint the existing
M2M provider discovers.

This design adds a kernel-native **Azure service-principal M2M** flow, forwards
the `azure_*` parameters across all four binding surfaces (Rust core, pyo3,
napi, C ABI), and reaches parity with the `databricks-sdk` `azure-client-secret`
credential strategy including its two Azure-specific request headers.

### What is *not* in scope

- **Azure U2M (`auth_type='azure-oauth'`) — tracked separately by PECOBLR-4120,
  not this ticket.** It is *not* plain workspace OIDC (an earlier draft of this
  doc claimed so — that was wrong). The Thrift-path reference
  (`AzureOAuthEndpointCollection`) uses the Azure app client id
  `96eecda7-19ea-49cc-abb5-240097d554f5` on redirect port `8030`, and an **AAD
  delegated scope** `{app_id}/user_impersonation offline_access` (app_id defaults
  to `2ff814a6-…`, overridable via `DATABRICKS_AZURE_TENANT_ID`) — not `sql
  offline_access`. Crucially, this needs **no new kernel auth flow**: the kernel
  already forwards an arbitrary client id / redirect port / scope verbatim
  through the existing `oauth-u2m` provider and discovers endpoints via
  `{host}/oidc/…` (which an Azure workspace redirects to Azure AD). The
  outstanding work there is **connector-side** — `auth_bridge.py` synthesising
  the Azure scope instead of forwarding the literal `sql offline_access` (see
  PECOBLR-4120, the Azure analogue of PECOBLR-4040 for `databricks-oauth`). This
  design leaves Azure U2M to PECOBLR-4120 and covers only Azure SP **M2M**, whose
  token *must* be minted by Entra client-credentials and so genuinely requires a
  new kernel flow.
- **Azure CLI / MSAL / managed-identity** credential sources. Only the
  client-secret service-principal flow is in scope (the `azure_*` kwargs the
  ticket names).

## Reference wire contract

Sourced from the Databricks docs
([aad-token-manual](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/aad-token-manual),
[azure-sp](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp))
and `databricks-sdk-py`'s `azure.py` credential strategy — the exact behavior
`databricks-sql-python` inherits today.

**Inner (Databricks-audience) token — always:**

```
POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
Content-Type: application/x-www-form-urlencoded

grant_type    = client_credentials
scope         = 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default   (URL-encoded .../%2F.default)
# client_id + client_secret via HTTP Basic auth
```

`2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` is the **Azure Databricks application
id** — a constant across all Azure clouds. The returned token's `aud` is that
id and its `iss` is `https://sts.windows.net/{tenant_id}/`.

**Management token — only when `azure_workspace_resource_id` is set:** a second
`client_credentials` request to the *same* endpoint with
`scope = https://management.core.windows.net/.default` (Public cloud).

**Headers attached to Databricks API requests:**

| Header | Sent when |
| --- | --- |
| `Authorization: Bearer <inner token>` | always |
| `X-Databricks-Azure-Workspace-Resource-Id: <azure_workspace_resource_id>` | `azure_workspace_resource_id` set |
| `X-Databricks-Azure-SP-Management-Token: <management token>` | `azure_workspace_resource_id` set (management token fetched) |

The management token + workspace-resource-id headers let the control plane
validate a service principal that has an Azure RBAC role on the workspace but is
not (yet) an explicit workspace member. When `azure_workspace_resource_id` is
unset, only the bearer is sent — the common SQL case where the SP is already a
workspace principal.

**Azure cloud environments.** Login host and management scope differ by cloud;
the app-id constant does not. Values are pinned from `databricks-sdk-py`'s
`azure.py` `ENVIRONMENTS` table during implementation:

| Environment | Login host | Management scope |
| --- | --- | --- |
| `PUBLIC` (default) | `https://login.microsoftonline.com` | `https://management.core.windows.net/.default` |
| `USGOVERNMENT` | `https://login.microsoftonline.us` | `https://management.usgovcloudapi.net/.default` |
| `CHINA` | `https://login.chinacloudapi.cn` | `https://management.chinacloudapi.cn/.default` |

## Key design decisions

### 1. A dedicated `AuthConfig` variant, not a reuse of `OAuthM2M`

`OAuthM2M`'s provider discovers the *workspace* token endpoint via OIDC and
requests Databricks scopes. Azure SP needs an Entra token endpoint built from
`{login_host}/{tenant}`, a fixed resource scope, a second (management) token,
and two extra headers. Bolting these onto `OAuthM2M` would tangle two flows and
muddy its config. A new variant keeps each flow's config self-describing —
consistent with the enum's existing `#[non_exhaustive]` note that anticipated
"an Azure-AD service-principal shortcut".

```rust
AuthConfig::AzureServicePrincipal {
    azure_client_id: String,
    azure_client_secret: String,
    azure_tenant_id: String,
    /// Enables the management-token path + both X-Databricks-Azure-* headers.
    azure_workspace_resource_id: Option<String>,
    /// None ⇒ derive from the workspace host (default PUBLIC).
    azure_environment: Option<AzureEnvironment>,
    /// Carried for uniformity with the other variants; forwarded to the
    /// federation wrapper.
    identity_federation_client_id: Option<String>,
}
```

A validating constructor `AuthConfig::azure_service_principal(...)` rejects
missing/empty `azure_client_id` / `azure_client_secret` / `azure_tenant_id`
(mirroring `oauth_m2m`), and `Debug` redacts the secret.

### 2. Extend the `AuthProvider` trait for extra headers

The trait returns only `Authorization` today. The two `X-Databricks-Azure-*`
headers are dynamic (the management token refreshes), so they can't be static
`custom_headers`. Add a default-empty method:

```rust
pub trait AuthProvider: Send + Sync + Debug {
    fn get_auth_header(&self) -> Result<String>;
    /// Extra headers to attach alongside `Authorization` on authenticated
    /// requests. Default: none. Only the Azure SP provider overrides it.
    fn extra_headers(&self) -> Result<Vec<(String, String)>> { Ok(Vec::new()) }
}
```

Every existing provider (PAT / M2M / U2M) inherits the empty default unchanged.
`FederationProvider` **delegates** `extra_headers()` to its inner provider so
the Azure headers survive the wrapper.

### 3. `FederationProvider` still wraps uniformly

The Azure inner token's issuer (`sts.windows.net`) is foreign to the workspace
host, so `FederationProvider` will attempt an exchange at `{host}/oidc/v1/token`.
This is **correct and non-breaking** in every case:

- **Azure workspace:** the exchange endpoint accepts the AAD assertion
  (`return_original_token_if_authenticated=true` path) and returns a usable
  token. If it ever fails, the existing non-breaking fallback uses the original
  AAD token directly — which the Azure workspace natively accepts, *with* the
  management headers still attached to the real request.
- **AWS/GCP workspace reached by an Azure SP:** exchange is *required*, and this
  is exactly the mechanism that provides it (the management headers are
  meaningless there and ignored as unknown `X-` headers).

Keeping federation uniform avoids special-casing and preserves the "mandatory,
issuer-gated" design. The exchange request itself goes out via
`execute_without_auth` and so does **not** carry the Azure extra headers (they
are attached only to authenticated API requests) — deliberate, since the
assertion authenticates that call.

### 4. `AzureEnvironment` auto-detected from host, overridable

`None` ⇒ derive from the workspace host suffix (`.azuredatabricks.net` →
`PUBLIC`, `.databricks.azure.us` → `USGOVERNMENT`, `.databricks.azure.cn` →
`CHINA`; default `PUBLIC`). An explicit value from the caller wins. This mirrors
the SDK, which auto-detects but honors `azure_environment` /
`ARM_ENVIRONMENT`.

## Module structure

### `src/auth/oauth/azure.rs` (new)

`AzureServicePrincipalProvider` implementing `AuthProvider`:

- Constructor `new(host, azure_client_id, azure_client_secret, azure_tenant_id,
  azure_workspace_resource_id, azure_environment, http_client)` (async, captures
  the runtime handle like the M2M/U2M providers).
- Builds the Entra token endpoint `{login_host}/{tenant}/oauth2/v2.0/token` from
  the resolved environment.
- **Inner token:** a `TokenStore` + `oauth2::BasicClient` client-credentials
  exchange with scope `{DATABRICKS_RESOURCE_ID}/.default`, reusing the exact
  lifecycle machinery `m2m.rs` uses (fresh/stale/expired, single-flight refresh).
- **Management token:** a *second* `TokenStore` populated only when
  `azure_workspace_resource_id` is `Some`, scope
  `{management_scope}` for the environment.
- `get_auth_header()` → `Bearer <inner>`.
- `extra_headers()` → `X-Databricks-Azure-Workspace-Resource-Id` +
  `X-Databricks-Azure-SP-Management-Token` when the resource id is set (each
  driven by a `get_or_refresh` on its store).

Constants (module-private): `DATABRICKS_RESOURCE_ID =
"2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"`, the `ENVIRONMENTS` table.

### `src/auth/oauth/mod.rs`, `src/auth/mod.rs`

`pub mod azure;`; re-export `AzureServicePrincipalProvider`. Add the
`extra_headers` default method to the `AuthProvider` trait.

### `src/auth/oauth/federation.rs`

Add `fn extra_headers(&self) -> Result<Vec<(String,String)>> {
self.inner.extra_headers() }` to the `AuthProvider` impl.

### `src/config.rs`

New `AuthConfig::AzureServicePrincipal` variant, `AzureEnvironment` enum,
`azure_service_principal(...)` validating constructor, `Debug` redaction of the
secret, and `identity_federation_client_id()` arm.

### `src/session.rs`

New arm in `build_base_auth_provider` constructing
`AzureServicePrincipalProvider`. The existing `build_auth_provider` wraps it with
`FederationProvider` unchanged.

### `src/client/http.rs`

In the request-build loop, on the `with_auth` branch, after setting
`Authorization`, apply `self.auth_extra_headers()?` (a new method reading the
provider from the `OnceLock` and calling `extra_headers()`). These ride only
authenticated `execute()` requests, never `execute_without_auth` (token/OIDC
calls). The `X-Databricks-Azure-*` names are not in the reserved set.

## Binding surfaces

Selector name `azure-sp-m2m` matches the connector's own `AuthType` enum value
(`databricks-sql-python` `auth/common.py`: `AZURE_SP_M2M = "azure-sp-m2m"`), so
the connector maps its detected Azure SP config onto the kernel verbatim with no
string translation.

### pyo3 (`pyo3/src/auth.rs`, `pyo3/src/session.rs`)

- New `Session(...)` kwargs: `azure_client_id`, `azure_client_secret`,
  `azure_tenant_id`, `azure_workspace_resource_id`, `azure_environment`.
- New `auth_type="azure-sp-m2m"` in `build_auth_config_inner`, requiring the
  three mandatory `azure_*` fields (validated with the existing `non_empty`
  helper) and forwarding the two optionals.
- Carried on the `OAuthParams`-adjacent path (a small `AzureParams` struct, or
  extend `OAuthParams`) to keep the ctor signature tidy.

### napi (`napi/src/database.rs`, `napi/index.d.ts` by hand)

- New `AuthMode::AzureSpM2m` variant + `azureClientId` / `azureClientSecret` /
  `azureTenantId` / `azureWorkspaceResourceId` / `azureEnvironment` fields on
  `ConnectionOptions`.
- New arm in `build_auth_config` calling `AuthConfig::azure_service_principal`.

### C ABI (`src/c_abi/session.rs`, `include/databricks_kernel.h` by hand)

- New `AuthSpec::AzureServicePrincipal { ... }` and a setter
  `kernel_session_config_set_auth_azure_sp_m2m(config, azure_client_id,
  azure_client_secret, azure_tenant_id, azure_workspace_resource_id /*nullable*/,
  azure_environment /*nullable*/)`, mutually exclusive with the other
  `set_auth_*` setters (last-wins, as today). Declared in the hand-written
  header; exercised in `tests/c/session_smoke.c`.

Each binding stages its own changelog entry (`pyo3/NEXT_CHANGELOG.md`,
`napi/NEXT_CHANGELOG.md`); the core entry lives in the root `NEXT_CHANGELOG.md`.
`CONNECTION_PARAMETERS.md` documents the new variant + fields.

## Error handling

| Scenario | Behavior |
| --- | --- |
| Missing/empty `azure_client_id` / `_secret` / `_tenant_id` | `InvalidArgument` at config construction (binding boundary) |
| Entra token endpoint error (bad creds, 4xx/5xx) | `Unauthenticated` from `get_auth_header()` (same as M2M) |
| Management token fetch fails | surfaced from `extra_headers()` → request fails with `Unauthenticated` (we do not silently drop a header the server may require) |
| Federation exchange fails | existing non-breaking fallback: original AAD token used |
| Unknown `azure_environment` string | `InvalidArgument` at the binding boundary |

## Testing

- **Unit (`config.rs`):** constructor rejects missing fields; `Debug` redacts
  the secret; `azure_environment` host auto-detection; `AuthConfig::azure_service_principal`
  round-trips.
- **Unit (`azure.rs`):** token endpoint / scope / management-scope construction
  per environment; `extra_headers()` present only when resource id set.
- **Wiremock integration (`tests/azure_sp_wiremock.rs`):** mock the Entra token
  endpoint (inner + management scopes distinguished by request body) and a
  workspace API endpoint; assert the outgoing request carries `Authorization:
  Bearer <inner>`, `X-Databricks-Azure-Workspace-Resource-Id`, and
  `X-Databricks-Azure-SP-Management-Token`; assert no extra headers when the
  resource id is unset; assert federation exchange interplay (issuer foreign →
  exchange attempted, fallback on failure).
- **`AuthProvider::extra_headers` default** covered by an existing provider (PAT)
  returning empty, and `FederationProvider` delegating to its inner.
- **Binding boundary:** pyo3 kwarg accepted (+ unknown-kwarg `TypeError` guard);
  napi field present on `ConnectionOptions`; C ABI setter records values / rejects
  NULL required args + smoke-test line.
- **E2E (`#[ignore]`):** real Azure workspace + SP creds via env vars
  (`DATABRICKS_HOST`, `DATABRICKS_AZURE_CLIENT_ID`, `_SECRET`, `_TENANT_ID`,
  optional `_WORKSPACE_RESOURCE_ID`).

## Deltas vs. `databricks-sdk` reference

- **Same** two-token model, resource id, scope strings, header names, v2.0
  endpoint, and environment table.
- **Kernel-specific:** the sync-trait-over-async bridge (`block_on_in_runtime`)
  and `TokenStore` lifecycle reused from the M2M provider rather than the SDK's
  refreshable-credentials wrapper; the two headers delivered via the new
  `AuthProvider::extra_headers()` seam rather than a per-request `HeaderFactory`.
- **Federation:** the kernel additionally runs its mandatory issuer-gated
  exchange on top (a no-op-or-benign round trip on Azure workspaces, and the
  required path on non-Azure workspaces) — the SDK sends the AAD token directly.
