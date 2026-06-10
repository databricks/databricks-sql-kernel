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

### Updated

### Fixed
- `auth_type="oauth-u2m"` now defaults scopes to `["sql", "offline_access"]`
  when the caller passes neither `client_id` nor `scopes`. The Python binding
  defaults the U2M client to `databricks-sql-python` (vs. the kernel's
  `databricks-cli`), but that client isn't registered for the `all-apis` scope
  the kernel intrinsically defaults to — so an out-of-the-box
  `Session(auth_type="oauth-u2m")` failed the authorization request with
  `access_denied — Scopes 'all-apis' are not assigned to the client
  databricks-sql-python`. Defaults now match `PYSQL_OAUTH_SCOPES` in the
  standalone Thrift connector, restoring `use_kernel=True` parity. A
  caller-supplied `client_id` or `scopes` is still honoured verbatim.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
