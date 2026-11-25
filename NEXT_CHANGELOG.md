# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

### Changed

- Sessions reuse matching idle HTTP clients by default while keeping exclusive pools and independent idle-socket limits; the Rust-only `HttpConfig::reuse_client_across_sessions = false` opt-out preserves keep-alive within a session and currently has no corresponding Node.js, Python, C ABI or ADBC option. After the last HTTP wrapper drops, reuse retains idle sockets and TLS/proxy configuration for up to about 105 seconds while the runtime is driven (otherwise until the next cache access/shutdown), and a new session's system-trust snapshot can be up to 300 seconds old.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
