# Version Changelog — Rust crate (`databricks-sql-kernel`)

This changelog tracks releases of the Rust crate published to crates.io.
The Python (pyo3) and Node.js (napi) bindings have their own changelogs
under [`pyo3/CHANGELOG.md`](pyo3/CHANGELOG.md) and
[`napi/CHANGELOG.md`](napi/CHANGELOG.md); they ship on independent
cadences and may not move in lockstep with the crate.

## [v0.1.2] - 2026-06-06

Initial public release of the Rust kernel core.

### Added
- `databricks-sql-kernel` Rust crate published to crates.io.
- Async public API: `Session` → `Statement` → `ExecutedStatement` /
  `ExecutedAsyncStatement` → `ResultStream`, plus a flat `Metadata`
  surface (see `docs/spec-v0.md`).
- SEA backend (Statement Execution API) with CloudFetch and inline-Arrow
  result paths; PAT, OAuth M2M, and OAuth U2M authentication.
