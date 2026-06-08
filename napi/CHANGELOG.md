# Version Changelog — Node.js binding (`@databricks/databricks-sql-kernel`)

This changelog tracks releases of the Node.js binding published to npm
as `@databricks/databricks-sql-kernel` (built from this `napi/` directory).
The Rust crate and Python binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../pyo3/CHANGELOG.md`](../pyo3/CHANGELOG.md);
they ship on independent cadences.

## [v0.1.2] - 2026-06-06

Initial public release of the Node.js binding.

### Added
- Node.js (napi) bindings published to npm as
  `@databricks/databricks-sql-kernel` — an umbrella loader package plus
  per-platform native packages for Linux gnu/musl (x64/arm64), macOS
  (x64/arm64), and Windows (x64/arm64).
- napi-rs based bridge over the Rust kernel's async public API.
