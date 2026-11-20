# NEXT CHANGELOG — Node.js binding (`@databricks/databricks-sql-kernel`)

This file stages unreleased changes for the next Node.js binding
release. The Rust crate and Python binding stage their own unreleased
changes under [`../NEXT_CHANGELOG.md`](../NEXT_CHANGELOG.md) and
[`../pyo3/NEXT_CHANGELOG.md`](../pyo3/NEXT_CHANGELOG.md).

Only land entries here that affect the Node.js binding — napi glue, the
JavaScript / TypeScript API surface, the umbrella + per-platform package
layout, or npm publishing. Pure-kernel changes go in the root
`NEXT_CHANGELOG.md`.

## [Unreleased]

### Added

### Changed

- Kernel sessions reuse idle HTTP clients by default, retaining sockets and TLS/proxy configuration for about 105 seconds after the last HTTP wrapper drops while the runtime is driven (otherwise until the next cache access/shutdown); a new session's system-trust snapshot can be up to 300 seconds old. The keep-alive-preserving opt-out, `HttpConfig::reuse_client_across_sessions = false`, is currently a kernel Rust setting with no Node.js option.

### Updated

### Fixed

- `AsyncStatement.close()` now releases its canceller and the result streams of its `AsyncResultHandle`s. These held the session until JS garbage collection, so a host's shared telemetry client, and its settings, outlived every closed session and were reused by the next one. Fetching from an `AsyncResultHandle` after its statement is closed now returns `InvalidStatementHandle`, unless a fetch was using the stream when `close()` ran.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
