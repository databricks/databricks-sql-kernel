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

# Azure Entra ID OAuth on the kernel via generic overrides — PECOBLR-4141 / PECOBLR-4120

## Overview

`databricks-sql-python` users authenticating to Azure Databricks with either an
**Azure service principal** (M2M — `azure-sp-m2m`) or the **Azure browser flow**
(U2M — `azure-oauth`) must keep working when the connector runs on the kernel
backend.

**The kernel does not need any Azure-specific auth code.** It already ships a
generic OAuth engine — `OAuthM2M` (client-credentials) and `OAuthU2M`
(authorization-code + PKCE) — whose endpoint, scope, and client-id are all
**overridable**. Azure is expressed by *driving those existing flows with Azure
values*, which the connectors already compute today. The kernel's job is only to
make every binding able to pass those overrides through.

This supersedes an earlier draft of this doc that proposed a dedicated
`AuthConfig::AzureServicePrincipal` variant, a new provider, an
`AuthProvider::extra_headers()` trait extension, and the Azure
management-token headers. All of that is **dropped** — see
[Why not a dedicated Azure flow](#why-not-a-dedicated-azure-flow).

## Why the kernel stays generic (evidence)

The two sibling SQL connectors already treat Azure as *generic OAuth with Azure
values*, not a distinct auth mode, and **neither uses the Azure management
token**:

- **Go (`databricks-sql-go`)** rolls its own OAuth, workspace-OIDC rooted, with
  the cloud inferred from the hostname; its kernel path has no Azure branch at
  all. Scope resource `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`. No
  `X-Databricks-Azure-SP-Management-Token`.
- **Node (`databricks-sql-nodejs`)** drives Azure via `openid-client` with scope
  `2ff814a6-…/.default` (M2M) / `…/user_impersonation` (U2M). Its **kernel path
  explicitly rejects** the Azure-direct options with: *"the workspace-OIDC
  discovery path handles Azure workspaces today without these options."* No
  management token.
- Only the general-purpose `databricks-sdk` carries the management-token
  machinery (`azure.py` `add_sp_management_token()`), for broad control-plane
  APIs — **not** the SQL data path (SEA / Thrift warehouse queries), which is all
  the kernel serves.

So the SQL-connector-consistent design is: keep the kernel a cloud-agnostic OAuth
engine; let each connector supply the Azure endpoint / scope / client-id it
already owns.

## What the kernel already has

`AuthConfig` (`src/config.rs`) exposes, on both OAuth variants:

- `OAuthM2M { client_id, client_secret, scopes, token_url_override, identity_federation_client_id }`
- `OAuthU2M { client_id, redirect_ports, scopes, token_url_override, callback_timeout, token_cache, identity_federation_client_id }`

`token_url_override`, `scopes`, and `client_id` are exactly the knobs Azure
needs. `FederationProvider` already wraps every flow and exchanges
foreign-issuer JWTs (Azure AD tokens included) at `{host}/oidc/v1/token`,
non-breaking. The HTTP client follows redirects by default, so workspace
`/oidc/.well-known/…` discovery transparently follows an Azure workspace's
redirect to Entra.

## How each Azure mode maps onto the existing flows

### Azure SP M2M (`azure-sp-m2m`, PECOBLR-4141)

Connector drives `OAuthM2M` with:

| Field | Azure value |
| --- | --- |
| `client_id` / `client_secret` | `azure_client_id` / `azure_client_secret` |
| `token_url_override` | `https://login.microsoftonline.com/{azure_tenant_id}/oauth2/v2.0/token` (per-cloud host) |
| `scopes` | `["2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"]` |

The `oauth2` crate sends `grant_type=client_credentials` with the client id/secret
via HTTP Basic to the override endpoint; the returned Entra token (audience
`2ff814a6-…`, issuer `sts.windows.net/{tenant}`) is used as the bearer.

> **Credential-transmission style (PR #263 review thread).** A reviewer noted the
> Azure docs *example* uses `client_secret_post` (form body), whereas the kernel's
> `exchange_client_credentials()` sends `client_secret_basic` (HTTP Basic).
> Verified non-blocking: Entra's published
> `token_endpoint_auth_methods_supported` is
> `["client_secret_post","private_key_jwt","client_secret_basic","self_signed_tls_client_auth"]`
> — `client_secret_basic` **is** supported, so no change to the M2M provider's
> credential transmission is required. Its foreign issuer means `FederationProvider` round-trips it through
`{host}/oidc/v1/token` (required when the workspace is *not* Azure; a benign
pass-through when it is).

> **Cloud/environment ownership (PR #263 review thread).** A reviewer correctly
> noted the Azure cloud (Public / USGov / China → login host) is a property of
> the *SP's Azure tenant*, not the workspace's host, so deriving it from the
> workspace host (as an earlier draft did) is wrong. In the generic-override
> design this is moot for the kernel: the connector builds the full
> `token_url_override` for the correct cloud and the kernel uses it verbatim —
> the kernel never infers an environment. The per-cloud host table lives in each
> connector (`databricks-sdk-py` `azure.py`, etc.).

### Azure U2M (`azure-oauth`, PECOBLR-4120)

Connector drives `OAuthU2M` with the Azure app client id
`96eecda7-19ea-49cc-abb5-240097d554f5`, `redirect_ports=[8030]`, and scope
`["{app_id}/user_impersonation", "offline_access"]` (`app_id` defaults to
`2ff814a6-…`, overridable via `DATABRICKS_AZURE_TENANT_ID`). Authorization/token
endpoints come from the kernel's existing OIDC discovery against `{host}/oidc/…`,
which the Azure workspace redirects to Entra.

Note the **kernel-blessed direction for Azure U2M is actually `databricks-oauth`**
(Databricks-native OAuth, which already works on Azure workspaces via
workspace-OIDC and needs *no* Azure values) — this is what the Go and Node kernel
paths use. `azure-oauth` (Entra-direct) is a legacy-compatibility path for callers
who pin it; it costs the kernel nothing beyond the override plumbing below.

### Static workspace-resource-id header, if ever needed

`X-Databricks-Azure-Workspace-Resource-Id` is a *static* string, so a connector
that wants it forwards it through the existing custom-headers channel
(pyo3 `http_headers`, napi `customHeaders`, C ABI
`kernel_session_config_set_custom_header`). No kernel change, no new seam.

## Kernel-repo work (the entire change)

pyo3 already exposes `token_url` + `oauth_scopes` + `client_id`, so **Python needs
no kernel change** — the work is in the other two bindings plus tests/docs.

### napi (`napi/src/database.rs`, `napi/index.d.ts` by hand)

Add an `oauthTokenUrl?: string` field to `ConnectionOptions` and forward it as
`token_url_override` in `build_auth_config` for **both** the `OAuthM2m` and
`OAuthU2m` arms (both currently hardcode `None`). `oauthScopes` /
`oauthClientId` are already forwarded.

### C ABI (`src/c_abi/session.rs`, `include/databricks_kernel.h` by hand)

- `AuthSpec::OAuthM2M` gains `scopes: Option<String>` and
  `token_endpoint: Option<String>`; `set_auth_m2m` takes two more (nullable)
  args and threads them into `AuthConfig::oauth_m2m` (currently passes
  `None, None`).
- `AuthSpec::OAuthU2M` gains `token_endpoint: Option<String>`; `set_auth_u2m`
  takes one more (nullable) arg and threads it into `AuthConfig::oauth_u2m`
  (currently `None`).
- Update the hand-written header and `tests/c/session_smoke.c`.

### Core refinement (optional but recommended)

In `m2m.rs`, when `token_endpoint_override` is `Some`, **skip OIDC discovery**:
the authorization endpoint is unused by the client-credentials grant, so
discovering it against the workspace is a needless round-trip and a needless
dependency on workspace OIDC for a pure-Entra token endpoint. Synthesize a
placeholder auth URL (unused) instead. U2M still discovers (it needs the
authorize endpoint for the browser step).

### Docs / changelog

`CONNECTION_PARAMETERS.md` documents the override knobs as the Azure mechanism
(with a worked `azure-sp-m2m` / `azure-oauth` example). Root + per-binding
`NEXT_CHANGELOG.md` entries.

## Connector responsibilities (out of this repo, for reference)

These land in `databricks-sql-python` / `-nodejs` (tracked by PECOBLR-4141 /
-4120 / the Node ticket), not here:

- **Python `auth_bridge.py`:** for `azure-sp-m2m`, forward `client_id` /
  `client_secret` / `token_url` / `scopes` as in the table above; for
  `azure-oauth`, synthesise the `{app_id}/user_impersonation offline_access`
  scope (the current bug is forwarding literal `sql offline_access`) and the
  Azure client id / port.
- **napi consumer (Node):** stop rejecting Azure in the kernel adapter; map its
  Azure options onto `oauthTokenUrl` / `oauthScopes` / `oauthClientId`.

## Why not a dedicated Azure flow

The earlier draft added `AuthConfig::AzureServicePrincipal`, a provider, an
`extra_headers()` trait method, and the management-token headers. Rejected
because:

- **No SQL connector uses the management token** (Go and Node confirm), so the
  `extra_headers()` seam and the second-token flow it existed for buy nothing on
  the SQL data path. Supporting the "Azure RBAC role but not a workspace member"
  scenario is a `databricks-sdk` control-plane concern, not a SQL one.
- A dedicated variant forks a **fourth** copy of the Azure constants (app id,
  per-cloud hosts, scope format) into the kernel, when the connectors already
  own and maintain them. The override approach keeps a single source of truth per
  connector and makes the kernel cloud-agnostic (matching Go and Node).
- Smaller, less security-sensitive change: no new provider, no trait change, no
  new auth variant.

If a future non-SQL consumer genuinely needs the management token, revisit then;
the `extra_headers()` design in this doc's git history is the starting point.

## Error handling

Unchanged from the existing OAuth flows: bad Entra credentials / endpoint errors
surface as `Unauthenticated` from `get_auth_header()` (M2M) or the browser-flow
error (U2M); a failed federation exchange falls back to the original token
(non-breaking). Missing required fields are rejected at the binding boundary by
the existing `AuthConfig::oauth_m2m` / `oauth_u2m` validating constructors.

## Testing

- **napi:** `ConnectionOptions.oauthTokenUrl` forwarded to `token_url_override`
  for M2M and U2M (unit test on `build_auth_config`); field present in
  `index.d.ts`.
- **C ABI:** `set_auth_m2m` scopes + token endpoint and `set_auth_u2m` token
  endpoint recorded and threaded (unit tests; NULL-optional handling); smoke-test
  line in `tests/c/session_smoke.c`.
- **Wiremock (`tests/azure_oauth_wiremock.rs`):** an Azure-shaped M2M run — mock
  an Entra-like token endpoint, drive `OAuthM2M` with `token_url_override` +
  `2ff814a6-…/.default` scope, assert the exchange hits the override endpoint
  with Basic auth and the right scope, and that the resulting foreign-issuer
  token flows through `FederationProvider`.
- **Discovery-follows-redirect:** a wiremock test that OIDC discovery follows a
  302 from `{host}/oidc/.well-known/…` to an Entra-like config host (guards the
  PECOBLR-4120 assumption).
- **E2E (`#[ignore]`):** real Azure workspace — `azure-sp-m2m` via env vars
  (`DATABRICKS_AZURE_CLIENT_ID` / `_SECRET` / `_TENANT_ID`) driving `oauth-m2m`
  with the override; and `databricks-oauth`/`azure-oauth` U2M against an Azure
  workspace.
- **Core:** if the skip-discovery-on-override refinement lands, a unit test that
  M2M with an override performs no discovery request.

## Deltas vs. reference

- **Same** Entra endpoint, `2ff814a6-…` app id, scope strings, and v2.0 endpoint
  as `databricks-sdk` / Go / Node — but supplied by the connector, not baked into
  the kernel.
- **Dropped** vs. `databricks-sdk`: the management token and its two
  `X-Databricks-Azure-*` headers (no SQL connector uses them).
- **Federation:** the kernel additionally runs its mandatory issuer-gated
  exchange on top, as it already does for every OAuth flow.
