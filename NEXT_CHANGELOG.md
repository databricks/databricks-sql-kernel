# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Azure Entra service-principal M2M (`azure-sp-m2m`) is now a kernel-owned flow
  (`AuthConfig::AzureServicePrincipal` / pyo3 `auth_type="azure-sp-m2m"`). The
  kernel resolves the Entra token endpoint (`{login}/{tenant}/oauth2/v2.0/token`)
  and the `{effective_app_id}/.default` scope, and auto-discovers the tenant from
  the workspace's `/aad/auth` redirect when `azure_tenant_id` is omitted (Thrift
  parity) — so bindings forward only the SP credentials. The `Authorization`
  bearer is the Databricks-audience data token, which alone authenticates the
  common workspace-member SP. When `azure_workspace_resource_id` is set, the
  kernel additionally sends the Azure SP management token
  (`X-Databricks-Azure-SP-Management-Token`, a second Entra token on the ARM
  `.default` scope) paired with the `X-Databricks-Azure-Workspace-Resource-Id`
  header, matching the JDBC connector (`if (resourceId != null)`), so a service
  principal that holds an Azure RBAC role but is not a workspace member can
  authenticate. When it is unset (the default) neither header is sent and no ARM
  management-scope token is fetched. A new `AuthProvider::extra_headers()`
  carries the pair. Covered by an `#[ignore]`d
  end-to-end test (`azure_sp_m2m_select_one_round_trip`) that exercises the flow
  against a live Azure workspace when Azure SP credentials are configured.
  (PECOBLR-4141.)

- Generic Azure/Entra OAuth overrides (the pre-dedicated-flow path, still valid
  for custom setups where a caller pins the endpoint/scope itself). New C ABI
  setters expose the pieces the C surface was missing:
  `kernel_session_config_set_oauth_token_endpoint()` and
  `kernel_session_config_set_oauth_scopes()` — both applied to whichever OAuth
  mode (M2M, M2M-JWT, or U2M) is selected, matching the single `oauth_scopes` /
  `token_url` the pyo3 and napi bindings already share across those flows. Both
  are additive — existing `set_auth_m2m` / `set_auth_m2m_jwt` / `set_auth_u2m`
  signatures are unchanged (for U2M and M2M-JWT, `set_oauth_scopes` /
  `set_oauth_token_endpoint` take precedence over scopes/token-endpoint passed
  positionally to those setters). (PECOBLR-4141 / PECOBLR-4120.)

- Auth: `AuthConfig::OAuthM2MPrivateKey` — OAuth 2.0 machine-to-machine using a JWT
  private-key client assertion (RFC 7523) instead of a client secret. The kernel
  signs a short-lived JWT with a PEM/PKCS#8 private key (RSA or EC; unencrypted or
  PBES2-passphrase-encrypted) and sends it as the `client_assertion` in the
  client-credentials grant. Supported algorithms: `RS256`/`384`/`512`,
  `PS256`/`384`/`512`, `ES256`, `ES384` (default `RS256`); `ES512` is not supported
  and is rejected with a clear error. Construct via `AuthConfig::oauth_m2m_private_key`.
- C ABI: `kernel_session_config_set_auth_m2m_jwt()` — expose the JWT private-key M2M
  flow over the C ABI (for ODBC / Go consumers), alongside the existing
  `set_auth_pat` / `set_auth_m2m` / `set_auth_u2m` setters. Required `client_id` /
  `jwt_key_file` / `jwt_kid`; optional `jwt_passphrase` (encrypted PKCS#8),
  `jwt_algorithm` (NULL → `RS256`), `scopes` (comma-separated; NULL → `all-apis`),
  and `token_url` (NULL → OIDC discovery). Maps to `AuthConfig::oauth_m2m_private_key`.

### Changed

- OAuth M2M: when a `token_url_override` is set, OIDC discovery against the
  workspace is now **skipped** — the client-credentials grant never uses the
  authorization endpoint, so pinning the token endpoint (e.g. an Azure/Entra
  endpoint that is not the workspace's) no longer makes a needless round-trip
  to, or depends on, the workspace OIDC config.

- **Breaking (OAuth U2M override semantics):** when a `token_url_override` is
  set for the U2M authorization-code flow, OIDC discovery is now **skipped** and
  the authorization endpoint is **derived** from the override by swapping a
  trailing `/token` → `/authorize` (the OAuth2 v2.0 sibling layout the override
  targets). Previously the kernel still ran discovery and used the *discovered*
  `authorization_endpoint`, overriding only the token endpoint. Consequently the
  override now **must end in `/token`** — a mismatch is rejected with
  `InvalidArgument` at session open instead of failing later. Callers who set a
  U2M `token_url` whose token path is not `/token`, or whose real authorize
  endpoint is not the `/authorize` sibling of that token path, must update the
  override (or drop it to fall back to discovery).

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
