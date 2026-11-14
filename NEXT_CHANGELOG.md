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
- All HTTP clients in a process share one DNS lookup per host at a time and
  reuse a successful answer for 30 s, so many sessions opening at once no
  longer send one identical lookup each. A lookup that hasn't succeeded after
  1 s, or that failed, is tried again alongside (up to 3 attempts), and every
  waiting session gets the first successful answer. A failure is reused for
  2 s.
- OAuth M2M and Azure service-principal sessions in one process that use the
  same host, client id, secret, scopes and HTTP transport settings share one
  OIDC discovery and one access token while any of them is open, so opening
  many sessions at once sends one token request instead of one per session.
  (The Azure management token is still fetched per session.)

### Updated

### Fixed

- `Session::open` authenticates once before resolving feature flags. A failed OAuth token request now fails the open immediately instead of being sent a second time by CreateSession.
- With CloudFetch disabled, sessions open again on warehouses that reject the `can_cloud_download` session setting (Lakehouse Real-Time). Those sessions request `INLINE` results, as 1.4.0 did, so results over the 25 MiB inline limit fail.
- CloseStatement and CancelStatement read their response body, so the HTTP connection is reused instead of a new TCP/TLS connection being opened for the next request.
- Telemetry batches rejected with HTTP 429/503 are kept and sent again after a backoff instead of being dropped, at most 1,000 events per request (or one batch, if larger), and telemetry retries log at debug instead of warn. Flushes on session close never wait out the backoff.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
