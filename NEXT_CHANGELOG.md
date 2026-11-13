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

### Changed

- Rust ADBC requests identify themselves as `RustADBCDatabricksDriver/<version>`
  with an optional partner entry from `adbc.databricks.user_agent_entry`
  (`adbc.spark.user_agent_entry` is a compatibility alias). `User-Agent` in
  `adbc.databricks.http.headers` is now rejected; use the dedicated entry option.
- All HTTP clients in a process share one DNS lookup per host at a time and
  reuse a successful answer for 30 s, so many sessions opening at once no
  longer send one identical lookup each.
- OAuth M2M and Azure service-principal sessions in one process that use the
  same host, client id, secret, scopes and HTTP transport settings share one
  OIDC discovery and one access token while any of them is open, so opening
  many sessions at once sends one token request instead of one per session.
  (The Azure management token is still fetched per session.)

### Updated

### Fixed

- `Session::open` authenticates once before resolving feature flags. A failed OAuth token request now fails the open immediately instead of being sent a second time by CreateSession.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
