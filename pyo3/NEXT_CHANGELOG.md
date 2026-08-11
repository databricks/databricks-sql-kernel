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

- `Session(...)`: new optional `identity_federation_client_id` keyword — selects
  SP-wide Workload Identity Federation for mandatory OIDC token exchange. When
  the authenticated token is issued by a party other than the target workspace,
  the kernel exchanges it for an in-house Databricks token; this value is sent as
  `identity_federation_client_id`. Omit for BYOT / account-wide WIF. Not a feature
  toggle — the exchange is always attempted, gated only by the token's issuer.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
