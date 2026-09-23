# Version Changelog — Rust crate (`databricks-sql-kernel`)

This changelog tracks releases of the Rust crate published to crates.io.
The Python (pyo3) and Node.js (napi) bindings have their own changelogs
under [`pyo3/CHANGELOG.md`](pyo3/CHANGELOG.md) and
[`napi/CHANGELOG.md`](napi/CHANGELOG.md); they ship on independent
cadences and may not move in lockstep with the crate.

## [Unreleased]

### Added

### Changed

### Fixed

## [v1.1.0] - 2026-09-23

### Added
- Execution-scoped client query deadlines for Rust and C APIs.
- FAST-mode session version exchange.
- C ABI query tags, HTTP connection limits, connection-test deadlines, and
  result-stream cancellation.
- Release-with-debuginfo build profile and native symbol checks.

### Changed
- Client deadlines permit an in-flight status request five seconds to finish.
- Direct traffic identifies itself as Databricks SQL Kernel.
- rustls now includes OS trust roots.
- Updated rustls to remediate OSV findings.

### Fixed
- CloudFetch downloads have end-to-end deadlines and terminal link-fetch
  errors.
- C ABI workers preserve a two-MiB stack floor without shrinking larger
  configured stacks.
- mTLS rejects non-HTTPS endpoints.
- PAT telemetry omits OAuth auth-flow metadata.

## [v1.0.0] - 2026-08-31

### Fixed
- Explicit async statement close finalizes telemetry when a partially consumed
  result stream remains alive.

## [v0.3.0] - 2026-08-28

### Added
- C ABI mTLS client-certificate and private-key configuration via
  `kernel_session_config_set_tls_client_certificate()`.
- C ABI request-timeout configuration via
  `kernel_session_config_set_request_timeout()`.
- Result-stream and metadata lifecycle telemetry, including first-batch,
  consumption, and CloudFetch metrics.
- Per-host telemetry circuit breaker, configurable through Rust, C ABI, PyO3,
  and napi.

### Changed
- Caller-supplied server session configuration now passes through unchanged;
  client-only result options are still stripped before the SEA request.
- Temporary DNS resolution failures are retried; permanent unknown hosts fail
  immediately.

### Fixed
- Empty schema and column catalog filters return typed empty results without a
  server statement.
- Type-info metadata matches the Thrift `getTypeInfo` 20-row result shape.
- Telemetry preserves error query IDs, flushes on the kernel runtime, finalizes
  on explicit statement close, and emits `DELETE_SESSION` from C ABI close.

## [v0.2.1] - 2026-08-24

### Added
- **Transaction control on `Session`** — `set_autocommit(bool)` /
  `autocommit()` / `commit()` / `rollback()`, exposed over the C ABI as
  `kernel_session_set_autocommit` / `_get_autocommit` / `_commit` /
  `_rollback`. Implemented as session-scoped SQL (`SET AUTOCOMMIT`,
  `COMMIT`, `ROLLBACK`), mirroring the reference/JDBC drivers, so every
  wrapper (ODBC / Python / Go / Node) gets identical transaction
  semantics from one implementation. Gated by the new
  `DefaultOpts::ignore_transactions` (builder shortcut
  `SessionBuilder::ignore_transactions`; C ABI
  `kernel_session_config_set_ignore_transactions`), which defaults to
  `true` — matching the reference/JDBC `IgnoreTransactions=1`: when set,
  the control methods are silent no-ops. Set `false` to opt into real
  multi-statement transactions (Private Preview; requires catalog-managed
  tables server-side).
- Azure Entra service-principal M2M (`azure-sp-m2m`) is now a kernel-owned flow
  (`AuthConfig::AzureServicePrincipal` / pyo3 `auth_type="azure-sp-m2m"`). The
  kernel resolves the Entra token endpoint (`{login}/{tenant}/oauth2/v2.0/token`)
  and the `{effective_app_id}/.default` scope, and auto-discovers the tenant from
  the workspace's `/aad/auth` redirect when `azure_tenant_id` is omitted (Thrift
  parity) — so bindings forward only the SP credentials. The `Authorization`
  bearer is the Databricks-audience data token, which alone authenticates the
  common workspace-member SP. When `azure_workspace_resource_id` is set, the
  kernel additionally sends the Azure SP management token
  (`X-Databricks-Azure-SP-Management-Token`, a second Entra token for the Azure
  Resource Manager) paired with the `X-Databricks-Azure-Workspace-Resource-Id`
  header, matching the JDBC connector (`if (resourceId != null)`), so a service
  principal that holds an Azure RBAC role but is not a workspace member can
  authenticate. That token is minted via the Entra **v1.0** `resource=` grant
  (`{login}/{tenant}/oauth2/token`) so its `aud` is
  `https://management.core.windows.net/` verbatim — trailing slash included —
  which the control plane requires; the v2.0 `.default` form drops the slash and
  is rejected. When the resource id is unset (the default) neither header is sent
  and no management token is fetched. A new `AuthProvider::extra_headers()`
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
- C ABI: `kernel_session_config_set_auth_azure_sp()` — expose Azure Entra
  service-principal M2M over the C ABI (for ODBC / Go consumers). Required
  `azure_client_id` / `azure_client_secret`; optional `azure_tenant_id`
  (NULL → the kernel auto-discovers it from the workspace `/aad/auth` redirect) and
  `azure_workspace_resource_id` (NULL → the data token authenticates alone; set →
  the kernel also sends the Azure SP management token +
  `X-Databricks-Azure-Workspace-Resource-Id` header, so an SP with an Azure RBAC
  role but no workspace membership can authenticate). Maps to
  `AuthConfig::azure_service_principal`. Azure AD U2M has no separate setter — it
  rides `set_auth_u2m` (the kernel's workspace-federated browser flow, which Azure
  Databricks workspaces support).
- C ABI: `kernel_session_test()` — actively validate a session by round-tripping a
  lightweight `SELECT 1` test query (result discarded). The I/O-performing complement
  of `kernel_session_is_open()` (a local handle-state check): it reaches the server, so
  it detects a server-side idle timeout, a killed warehouse, or a dropped link. Returns
  `Success` when the session is alive; a transport/availability failure surfaces as
  `Unavailable` / `NetworkError` / `Timeout`, which a host can treat as "connection
  dead" when backing `SQL_ATTR_CONNECTION_DEAD`. Mirrors the reference / JDBC drivers'
  validate-on-borrow / connection-test probe. Runs SQL, so it must be called from a
  native (non-async-runtime) thread. Backed by the new `Session::test_connection()`.
  Benefits ODBC (databricks/databricks-odbc#298).

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

### Fixed
- **Metadata conformance (catalog match-all wildcard)** — a *literal*
  catalog `%`, `*`, or `""` reaching `Metadata::list_schemas` /
  `list_tables` is now scoped verbatim (`SHOW … IN [CATALOG] \`%\``) and
  matches nothing (0 rows), instead of being silently broadened to
  `SHOW … IN ALL CATALOGS`. Only a `None` catalog means "all catalogs";
  `list_columns` already behaved this way. This makes an escaped `\%`
  catalog (wildcard-escaping ON) and an exact `Identifier("%")` return
  the correct empty result, matching the reference driver. An *unescaped*
  `%` `LikePattern` on `list_tables` is still match-all (one
  `SHOW TABLES IN ALL CATALOGS`), and the C ABI's NULL/`""` → "all
  catalogs" convention is unchanged. Fixes #202, #221.
- **Metadata conformance (`foreign_keys` unspecified foreign table)** —
  `Metadata::foreign_keys` with a `None` `foreign_table` now returns an
  empty result (0 rows, no `SHOW FOREIGN KEYS` issued) instead of an
  `InvalidArgument` error, matching the JDBC SEA `listCrossReferences`
  contract and the ODBC primary-keys-only `SQLForeignKeys` form (covers
  both the parent-set and both-tables-null cases). `foreign_catalog` /
  `foreign_schema` remain required only when a `foreign_table` is
  supplied. Fixes #212 (METADATA-037/038).

## [v0.2.0] - 2026-06-08

**Beta release of the Databricks SQL Kernel.** Bumps the crate out
of its initial-public-release line into a beta-quality cut intended
for downstream-driver adoption (Reyden bring-up in
`databricks-sql-connector` and `@databricks/sql`). Public API
surface is stable; behaviour changes captured below.

### Added
- `Statement::execute_direct()` — directResults execution (the
  Thrift/JDBC model): one `ExecuteStatement` POST with the server
  inline wait, returning `DirectStatement::Completed` (terminal,
  result materialised inline) or `DirectStatement::Running` (a
  poll/cancel handle) **without** an internal poll loop. Added
  **alongside** an unchanged blocking `execute()`.

### Updated
- Removed `StatementSpec::query_timeout_secs` and the
  `resolve_wait_timeout` `CANCEL` branch: it mapped a per-statement
  timeout onto the SEA `wait_timeout` field, which is the server's
  inline-hold window (capped 5–50s), not a statement-execution
  timeout (use the `STATEMENT_TIMEOUT` SQL conf for that). The sync
  path now always omits `wait_timeout`.

### Fixed
- `terminal_outcome`: `CLOSED` is now an unconditional terminal
  **success** (parity with JDBC's `DatabricksSdkClient`); an empty
  `CLOSED` materialises an empty result set instead of erroring
  with `invalid_state("Statement was closed")`. Reaches all callers
  (`execute()`, `fetch_completed_result`, `execute_direct`).

## [v0.1.2] - 2026-06-06

Initial public release of the Rust kernel core.

### Added
- `databricks-sql-kernel` Rust crate published to crates.io.
- Async public API: `Session` → `Statement` → `ExecutedStatement` /
  `ExecutedAsyncStatement` → `ResultStream`, plus a flat `Metadata`
  surface (see `docs/spec-v0.md`).
- SEA backend (Statement Execution API) with CloudFetch and inline-Arrow
  result paths; PAT, OAuth M2M, and OAuth U2M authentication.
