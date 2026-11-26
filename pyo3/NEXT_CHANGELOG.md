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

### Changed
- Allow pyarrow 24.x through 26.x on Python 3.10+ (`pyarrow>=23.0.1,<27`),
  so the wheel installs alongside pyarrow 26, the first release clear of
  CVE-2026-92287. Fresh `[kernel]` installs without their own pyarrow pin now
  resolve pyarrow 26 on Python 3.11+ (25.x on 3.10); pin `pyarrow<24` to stay
  on 23.x.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
