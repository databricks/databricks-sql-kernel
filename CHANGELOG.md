# Version Changelog

## [v0.1.2] - 2026-06-06

Initial public release of the Databricks SQL Kernel and its language
bindings.

### Added
- Rust core kernel (`databricks-sql-kernel`).
- Python bindings published to PyPI as `databricks-sql-kernel` —
  abi3 wheels for Linux (manylinux_2_28 x86_64/aarch64), macOS
  (x86_64/arm64), and Windows (x86_64), plus an sdist.
- Node.js (napi) bindings published to npm as
  `@databricks/databricks-sql-kernel` — an umbrella loader plus
  per-platform packages for Linux gnu/musl (x64/arm64), macOS
  (x64/arm64), and Windows (x64/arm64).
