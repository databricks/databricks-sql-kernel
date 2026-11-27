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

### Updated

### Fixed

- The C ABI and shared telemetry runtime threads now block SIGPIPE. A TLS write after a peer reset no longer kills a host that doesn't ignore SIGPIPE, such as a Go program linking the C ABI; the write fails with `EPIPE` and the request reports the connection error instead.
- The Rust ADBC driver's own runtime threads now block SIGPIPE too, so a Go program loading the driver through a cgo driver manager isn't killed by a write after a peer reset. Caller-injected runtimes are unchanged.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
