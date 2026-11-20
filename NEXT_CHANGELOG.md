# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Explicit, execution-scoped completion of server-marked Volume PUT/GET/REMOVE
  instructions, with allowed-root checks, resumable owned-byte PUT input, in-place GET
  publication and cached completion outcomes. Resolved symlink targets must remain
  within the configured roots.

### Changed

- This minor release adds public fields to `Error` and `ResultManifest`; no Rust consumer outside this repository's PyO3, NAPI and ADBC bindings builds these structs with literals.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
