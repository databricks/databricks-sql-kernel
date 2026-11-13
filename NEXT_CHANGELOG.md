# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- A native Rust ADBC 1.1 driver in `adbc/`, with an optional standard ADBC C
  ABI for driver-manager loading from other languages.
- `ExecutedStatement::try_next_batch_is_cheap()` and
  `ResultStream::try_next_batch_is_cheap()` report whether `try_next_batch` is
  supported and a ready batch needs no per-row work on the calling thread, so
  bindings can take it without giving up a host lock. Readers opt in with
  `ResultReader::try_next_batch_is_cheap` (default `false`).

### Changed

- Rust ADBC requests identify themselves as `RustADBCDatabricksDriver/<version>`
  with an optional partner entry from `adbc.databricks.user_agent_entry`
  (`adbc.spark.user_agent_entry` is a compatibility alias). `User-Agent` in
  `adbc.databricks.http.headers` is now rejected; use the dedicated entry option.

### Updated

### Fixed

- `Session::open` authenticates once before resolving feature flags. A failed OAuth token request now fails the open immediately instead of being sent a second time by CreateSession.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
