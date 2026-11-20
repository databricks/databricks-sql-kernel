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

- Table metadata overlaps catalog discovery with a scoped query for underscore patterns without percent or star characters in the literal scope, reusing the matching catalog's result even in a multi-catalog response. Speculation can delay remaining catalog queries; a miss may add one statement.
- Rust ADBC requests identify themselves as `RustADBCDatabricksDriver/<version>`
  with an optional partner entry from `adbc.databricks.user_agent_entry`
  (`adbc.spark.user_agent_entry` is a compatibility alias). `User-Agent` in
  `adbc.databricks.http.headers` is now rejected; use the dedicated entry option.
- **Breaking (Rust API):** `ClientCredentialsProvider`,
  `JwtClientCredentialsProvider` and `AuthorizationCodeProvider` constructors
  take `&Arc<DatabricksHttpClient>` instead of `Arc<DatabricksHttpClient>` and
  hold it weakly, matching `FederationProvider`. Callers must keep the client
  alive for as long as they use the provider; a token fetch after the client is
  dropped fails with `invalid_state`.

### Updated

### Fixed

- `Session::open` authenticates once before resolving feature flags. A failed OAuth token request now fails the open immediately instead of being sent a second time by CreateSession.
- A timed-out or reset CloudFetch download no longer writes the presigned URL's signature (`sig=`, `X-Amz-Signature`) to the log or the returned error. The request URL in those messages now ends at the path, and debug request lines stop at `?`.
- Closing an OAuth session (M2M, M2M private key, U2M or Azure service principal) now frees its HTTP client and auth provider. The provider held the HTTP client strongly while the client owned the provider, so the pair and the provider's tokio runtime handle leaked for every closed session.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
