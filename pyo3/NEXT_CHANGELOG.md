# NEXT CHANGELOG — Python binding (`databricks-sql-kernel`)

This file stages unreleased changes for the next Python wheel release.
The Rust crate and Node.js binding stage their own unreleased changes
under [`../NEXT_CHANGELOG.md`](../NEXT_CHANGELOG.md) and
[`../napi/NEXT_CHANGELOG.md`](../napi/NEXT_CHANGELOG.md).

Only land entries here that affect the Python wheel — pyo3 glue, the
Python API surface, packaging / wheel-build behaviour. Pure-kernel
changes go in the root `NEXT_CHANGELOG.md`.

## [Unreleased]

### Added

- `Session(auth_type="oauth-m2m-jwt", ...)` — OAuth 2.0 machine-to-machine auth using
  a JWT private-key client assertion instead of a client secret. New keyword args
  `jwt_key_file` (required), `jwt_kid` (required), `jwt_passphrase` (optional, for
  encrypted PKCS#8 keys), and `jwt_algorithm` (optional, default `RS256`), alongside
  the existing `client_id` / `oauth_scopes` / `token_url`. Forwards to the kernel's
  `AuthConfig::OAuthM2MPrivateKey`.
- `Session(..., identity_federation_client_id=...)` selects SP-wide Workload
  Identity Federation during mandatory token exchange.
- `Statement.set_row_limit(...)` sets the SEA server-side row cap; passing
  `None` clears it.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
