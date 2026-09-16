# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Add an opt-in `release-with-debuginfo` Cargo profile and cross-platform
  symbol smoke checks for statically linked native consumers.
- Create SQL Exec API sessions in FAST mode and exchange the maximum observed session version.
- Add a C ABI setter for the maximum idle HTTP connections retained per host.
- C ABI: `kernel_statement_set_query_tags()` attaches per-statement query tags
  (serialized `key:value` wire form) to a statement's execution, letting C ABI
  consumers (ODBC, Go's kernel backend) reach parity with the Rust/PyO3/napi
  `query_tags` surface.

### Changed

- Client query deadlines now allow an already-running statement-status request
  up to five seconds to return a terminal result; no new poll starts after the
  configured deadline.

### Updated

### Fixed

- Reject non-HTTPS endpoints before connecting when an mTLS client
  certificate and private key are configured.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
