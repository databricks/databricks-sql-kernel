# Version Changelog — Node.js binding (`@databricks/databricks-sql-kernel`)

This changelog tracks releases of the Node.js binding published to npm
as `@databricks/databricks-sql-kernel` (built from this `napi/` directory).
The Rust crate and Python binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../pyo3/CHANGELOG.md`](../pyo3/CHANGELOG.md);
they ship on independent cadences.

## [Unreleased]

### Added
- **U2M on-disk token-cache control on `openSession`** — `tokenCacheEnabled`
  and `tokenCachePassphrase` let a Node caller enable/disable (or set the
  AES-256 passphrase for) the kernel's built-in encrypted disk cache on the
  kernel path. Omitted keeps the kernel default (enabled, machine-local key,
  survives restarts); `tokenCacheEnabled: false` re-logins each fresh process; a
  passphrase with no explicit flag implies enabled. Applies to `OAuthU2m`.
  (PECOBLR-4146)

  The generated `index.d.ts` gains the two optional fields; `index.js` (the
  loader) is unchanged — new `#[napi(object)]` fields don't alter the exported
  symbol set.

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
