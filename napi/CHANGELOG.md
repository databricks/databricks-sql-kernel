# Version Changelog — Node.js binding (`@databricks/databricks-sql-kernel`)

This changelog tracks releases of the Node.js binding published to npm
as `@databricks/databricks-sql-kernel` (built from this `napi/` directory).
The Rust crate and Python binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../pyo3/CHANGELOG.md`](../pyo3/CHANGELOG.md);
they ship on independent cadences.

## [Unreleased]

### Added
- **Pluggable OAuth token store on `openSession`** — two optional callbacks
  (`oauthPersistenceRead` / `oauthPersistencePersist`) let a Node caller supply
  a custom OAuth U2M token store on the kernel path instead of the built-in disk
  cache. Both must be supplied together; they receive/return the kernel's
  `OAuthToken` as a JSON string (`KernelPersistArgs`). Wired via
  `ThreadsafeFunction`, awaiting the JS `Promise`. (PECOBLR-4146)

  The generated native contract (`index.js` / `index.d.ts`) was regenerated
  with the crate-correct `@napi-rs/cli@3.8.2`; the previously-committed contract
  was stale `@napi-rs/cli` v2 output, so that diff also migrates the loader to
  v3 (version-consistency checks, WASI fallback).

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
