# NEXT CHANGELOG — Node.js binding (`@databricks/databricks-sql-kernel`)

This file stages unreleased changes for the next Node.js binding
release. The Rust crate and Python binding stage their own unreleased
changes under [`../NEXT_CHANGELOG.md`](../NEXT_CHANGELOG.md) and
[`../pyo3/NEXT_CHANGELOG.md`](../pyo3/NEXT_CHANGELOG.md).

Only land entries here that affect the Node.js binding — napi glue, the
JavaScript / TypeScript API surface, the umbrella + per-platform package
layout, or npm publishing. Pure-kernel changes go in the root
`NEXT_CHANGELOG.md`.

## [Unreleased]

### Added

- `AuthMode.OAuthM2mJwt` — OAuth 2.0 machine-to-machine auth using a JWT private-key
  client assertion instead of a client secret. New `ConnectionOptions` fields
  `jwtKeyFile` (required), `jwtKid` (required), `jwtPassphrase` (optional, for
  encrypted PKCS#8 keys), and `jwtAlgorithm` (optional, default `RS256`), alongside
  the existing `oauthClientId` / `oauthScopes`. Forwards to the kernel's
  `AuthConfig::OAuthM2MPrivateKey`.
- `ConnectionOptions.tokenUrl` — OAuth token endpoint override. When omitted the
  kernel discovers the endpoint via the workspace's OIDC configuration; set it to
  point the client-credentials / JWT client-assertion grant at the workspace's
  configured IdP token endpoint (e.g. Entra ID for Azure Databricks). Required for
  `AuthMode.OAuthM2mJwt` against an external-IdP-backed workspace, since
  Databricks-native OIDC does not advertise the `private_key_jwt` method. Applies to
  `AuthMode.OAuthM2m` and `AuthMode.OAuthU2m` too. Forwards to the kernel's
  `token_url_override`.
- `ConnectionOptions.identityFederationClientId` selects SP-wide Workload
  Identity Federation during mandatory token exchange.
- `AuthMode.AzureSpM2m` — Azure Entra service-principal M2M. New
  `ConnectionOptions` fields `azureClientId` (required) + `azureClientSecret`
  (required), plus optional `azureTenantId` (kernel auto-discovers it from the
  workspace's `/aad/auth` redirect when omitted) and `azureWorkspaceResourceId`
  (when set, the kernel also sends the Azure SP management token +
  `X-Databricks-Azure-Workspace-Resource-Id` header, so a service principal with
  an Azure RBAC role but no workspace membership can authenticate; omitted ⇒ the
  data token authenticates alone). Forwards to the kernel's
  `AuthConfig::azure_service_principal`. The napi analog of the pyo3
  `auth_type="azure-sp-m2m"` surface. Azure AD **U2M** needs no new mode — it
  rides `AuthMode.OAuthU2m` (the kernel's workspace-federated browser flow,
  which Azure workspaces support).

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
