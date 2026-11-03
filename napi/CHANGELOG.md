# Version Changelog — Node.js binding (`@databricks/databricks-sql-kernel`)

This changelog tracks releases of the Node.js binding published to npm
as `@databricks/databricks-sql-kernel` (built from this `napi/` directory).
The Rust crate and Python binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../pyo3/CHANGELOG.md`](../pyo3/CHANGELOG.md);
they ship on independent cadences.

## [Unreleased]

### Added

### Changed

### Fixed

## [v1.4.0] - 2026-11-03

### Changed
- Connections open through the warehouse sessions URL and keep Lakehouse
  Real-Time session affinity for statements they own.

### Updated
- Bundle the kernel's `ctutils` 0.4.4 security update.

### Fixed
- Telemetry from statements and result streams that outlive their
  `Connection` is flushed instead of dropped, and `Connection.close()` waits
  for a telemetry flush only when it closes the last open connection for its
  host.

## [v1.3.0] - 2026-10-23

### Updated
- Bundle the kernel's TLS and runtime dependency security updates.

### Fixed
- Preserve manifest column nullability in empty-result Arrow schemas when
  no Arrow schema is supplied.

## [v1.2.0] - 2026-10-19

### Added
- The default-enabled `ConnectionOptions.enableGeoSpatialSupport` option
  selects native SRID+WKB Arrow results when enabled and WKT / EWKT strings
  when disabled.
- `ConnectionOptions.seaResponseCompressionEnabled` opts back into HTTP
  compression for bandwidth-constrained SEA connections.

### Changed
- An omitted `telemetryEnabled` allows workspace feature-flag enablement;
  an explicit `true` or `false` still takes precedence.

### Fixed
- `getCrossReference` allows the foreign-key side to be omitted, returning an
  empty result without issuing a server statement.
- `getPrimaryKeys` accepts null or undefined catalog and schema values, resolving
  them from the current session, and preserves explicit empty names.
- `listFunctions` accepts an empty catalog and forwards it to native
  `GetFunctions` for Thrift-compatible behavior.

## [v1.1.0] - 2026-09-23

No Node.js-binding-specific changes.

## [v1.0.0] - 2026-08-31

### Fixed
- `AsyncStatement.close()` now finalizes async statement telemetry in
  partial-fetch flows.

## [v0.3.0] - 2026-08-28

### Added
- `tokenCacheEnabled` and `tokenCachePassphrase` on `openSession` control the
  encrypted U2M disk cache.
- `ExecuteOptions.rawParams` preserves pre-marshalled SQL type names such as
  `INTERVAL MONTH` and `INTERVAL DAY`.
- `telemetryCircuitBreakerEnabled`, `telemetryCircuitBreakerThreshold`, and
  `telemetryCircuitBreakerTimeoutMs` configure telemetry failure isolation.

### Fixed
- Empty catalog filters for `listSchemas()` and `listColumns()` return empty
  results instead of `InvalidArgument`.
- Explicit synchronous statement close finalizes telemetry after partial
  result consumption.

## [v0.2.1] - 2026-08-24

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

## [v0.2.0] - 2026-06-08

**Beta release of the Databricks SQL Kernel (Node.js binding).**
Ships on npm as `@databricks/databricks-sql-kernel@0.2.0` (umbrella
+ 8 platform packages). Intended for downstream-driver adoption in
`@databricks/sql`'s kernel-backed path.

### Added
- `Connection.executeStatementDirect(sql, options?)` — exposes the
  kernel's directResults `execute_direct`, returning
  `Either<Statement, AsyncStatement>` (terminal `Statement` for a
  fast query, or an `AsyncStatement` poll/cancel handle for a slow
  one). Consumed by the Node.js driver's default sync path.

### Updated
- Removed `ExecuteOptions.queryTimeoutSecs`: it threaded onto the
  now-removed kernel `query_timeout_secs`, which abused the SEA
  `wait_timeout` inline-hold window rather than being a real
  statement timeout.

## [v0.1.2] - 2026-06-06

Initial public release of the Node.js binding.

### Added
- Node.js (napi) bindings published to npm as
  `@databricks/databricks-sql-kernel` — an umbrella loader package plus
  per-platform native packages for Linux gnu/musl (x64/arm64), macOS
  (x64/arm64), and Windows (x64/arm64).
- napi-rs based bridge over the Rust kernel's async public API.
