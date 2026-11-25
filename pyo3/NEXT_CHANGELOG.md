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

- `Session(host, http_path, port=...)`: an optional connection `port`
  (`int`, default `None`). A non-default port is folded into the host by the
  kernel. `0` and the scheme default (443 for `https`, 80 for `http`) change
  nothing. The host must not have credentials, a path, a query or a fragment.
  A port already in the host wins. A value outside `0..=65535`, or a non-int,
  raises `ValueError`. A bad host/port combination raises the usual
  `KernelError` (`InvalidArgument`).

### Changed

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
