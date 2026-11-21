# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Volume `PUT`, `GET` and `REMOVE`: `ResultStream::complete_volume_operation`
  runs the file transfer. Local paths, and the files they resolve to through
  symlinks, must be inside the caller's allowed directories. A `PUT` can also
  upload bytes the caller supplies.

### Changed

- Adds public fields to `Error` and `ResultManifest`. Code that builds these
  structs with struct literals must be updated. Supported consumers are the PyO3,
  NAPI, C ABI and ADBC bindings.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
