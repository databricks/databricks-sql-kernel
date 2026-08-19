# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Azure Entra U2M (`azure-oauth`) is now a kernel-owned flow
  (`AuthConfig::AzureOAuthU2M` / pyo3 `auth_type="azure-oauth"`). The kernel
  resolves the Azure browser flow itself — it pins the workspace v2.0
  endpoints (`{host}/oidc/oauth2/v2.0/{authorize,token}`, which the workspace
  redirects to Entra), the Azure app client id (`96eecda7-…`), port `8030`, and
  the `{app_id}/user_impersonation offline_access` scope — so bindings only
  forward the selector. The U2M provider gained an authorization-endpoint
  override so the Azure path pins `/oidc/oauth2/v2.0/authorize` instead of the
  discovered `/oidc/v1/authorize` (which an Azure workspace redirects to a
  malformed Entra URL). Live-verified end-to-end against an Azure workspace.
  (PECOBLR-4120.)

- Azure Entra service-principal M2M (`azure-sp-m2m`) is now a kernel-owned flow
  (`AuthConfig::AzureServicePrincipal` / pyo3 `auth_type="azure-sp-m2m"`). The
  kernel resolves the Entra token endpoint (`{login}/{tenant}/oauth2/v2.0/token`)
  and the `{effective_app_id}/.default` scope, and auto-discovers the tenant from
  the workspace's `/aad/auth` redirect when `azure_tenant_id` is omitted (Thrift
  parity) — so bindings forward only the SP credentials. The Azure
  management-token pair (`X-Databricks-Azure-SP-Management-Token` +
  `X-Databricks-Azure-Workspace-Resource-Id`) is an optional add-on gated on
  `azure_workspace_resource_id`: set it and the kernel fetches a second Entra
  token (ARM `.default` scope) so an SP holding only an Azure RBAC role (not a
  workspace member) can authenticate; omit it and the SP uses the data token
  alone. A new `AuthProvider::extra_headers()` carries the dynamic pair. Common
  case (tenant auto-discovered, data token only) live-verified end-to-end against
  an Azure workspace. (PECOBLR-4141.)

- Generic Azure/Entra OAuth overrides (the pre-dedicated-flow path, still valid
  for custom setups where a caller pins the endpoint/scope itself). New C ABI
  setters expose the pieces the C surface was missing:
  `kernel_session_config_set_oauth_token_endpoint()` and
  `kernel_session_config_set_oauth_scopes()` — both applied to whichever OAuth
  mode (M2M or U2M) is selected, matching the single `oauth_scopes` /
  `token_url` the pyo3 and napi bindings already share across both flows. Both
  are additive — existing `set_auth_m2m` / `set_auth_u2m` signatures are
  unchanged (for U2M, `set_oauth_scopes` takes precedence over scopes passed
  positionally to `set_auth_u2m`). (PECOBLR-4141 / PECOBLR-4120.)

### Changed

- OAuth M2M: when a `token_url_override` is set, OIDC discovery against the
  workspace is now **skipped** — the client-credentials grant never uses the
  authorization endpoint, so pinning the token endpoint (e.g. an Azure/Entra
  endpoint that is not the workspace's) no longer makes a needless round-trip
  to, or depends on, the workspace OIDC config.

- C ABI: `kernel_session_config_set_retry_config()` — expose the crate's existing
  HTTP retry knobs (`HttpConfig::retry_min_wait` / `retry_max_wait` /
  `retry_max_retries` / `overall_timeout`) over the C ABI so a host can make its own
  `RetryWaitMin` / `RetryWaitMax` / `RetryMax` / retry-budget policy authoritative
  instead of the kernel default; `max_retries=0` disables retries, and
  `overall_timeout_ms=0` keeps the kernel's default budget (non-zero overrides it).
  The retry budget is always bounded — a large `max_retries` is still capped by it.
  Benefits ODBC too. (Consumed by the Go driver via `WithRetries`,
  databricks/databricks-sql-go#412.)
- C ABI: client-only `session_conf` key `cloudfetch_max_chunks_in_memory` — carried
  by the existing `kernel_session_config_set_session_conf()` setter (no new symbol),
  it bounds how many decompressed CloudFetch chunks the kernel holds in memory at
  once (peak RSS on large result sets; default 16). Accepts a positive integer;
  a value above the kernel ceiling (256) is clamped, and a non-numeric / non-positive
  value is ignored (default preserved) with a warning rather than failing session
  creation. The key is client-only — it is consumed before session creation and never
  reaches the SEA wire. Benefits ODBC too.
- Auth: mandatory OIDC token exchange (BYOT federation) — when an authenticated
  token was issued by a party other than the target workspace (Azure AD, an external
  IdP, a Bring-Your-Own-Token, or a Workload Identity Federation token), the kernel
  now exchanges it for an in-house Databricks token at `<host>/oidc/v1/token` before
  using it, transparently across PAT / M2M / U2M. Gated by the token's `iss` claim
  (non-JWT and same-host tokens pass through with no HTTP call) and falls back to the
  original token if the exchange fails, so existing flows are unaffected. New optional
  `identity_federation_client_id` fields on the PAT, M2M, and U2M `AuthConfig`
  variants select SP-wide WIF; unset means BYOT / account-wide WIF. Follows the
  ADBC wire contract (`grant_type=jwt-bearer`, `assertion`, `scope=sql`).
- Auth constructors now accept the optional federation client id directly;
  the C ABI exposes it through
  `kernel_session_config_set_identity_federation_client_id()`.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
