# NEXT CHANGELOG — Python binding (`databricks-sql-kernel`)

This file stages unreleased changes for the next Python wheel release.
The Rust crate and Node.js binding stage their own unreleased changes
under [`../NEXT_CHANGELOG.md`](../NEXT_CHANGELOG.md) and
[`../napi/NEXT_CHANGELOG.md`](../napi/NEXT_CHANGELOG.md).

Only land entries here that affect the Python wheel — pyo3 glue, the
Python API surface, packaging / wheel-build behaviour. Pure-kernel
changes go in the root `NEXT_CHANGELOG.md`.

## [Unreleased]

### Added

- Added `Session(..., sea_response_compression_enabled=...)` to opt back into
  HTTP compression for bandwidth-constrained SEA connections.

### Changed

- An omitted `telemetry_enabled` now allows workspace feature-flag enablement;
  an explicit `True` or `False` still takes precedence.

### Updated

### Fixed

- Fixed an intermittent hang in `Session.close()` (or when Python frees a
  handle) when a runtime worker was forwarding a log record to Python
  `logging` at the moment the session's runtime shut down.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
