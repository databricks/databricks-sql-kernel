# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]


### Added

- C ABI: expose the kernel's existing mTLS client identity configuration
  through `kernel_session_config_set_tls_client_certificate()`. The client
  certificate and private key are supplied atomically and must both be
  non-empty.

- C ABI: `kernel_session_config_set_request_timeout()` configures the total
  timeout for one HTTP request in milliseconds; `0` keeps the kernel's
  120-second default. This enables Go and other C-ABI consumers to expose
  request-timeout configuration.

### Changed

- HTTP retries now treat temporary DNS resolver failures as transient while
  permanent unknown-host failures still fail immediately.

### Updated

### Fixed

- Metadata: empty schema/column patterns now return typed empty results without
  issuing a server statement.

- Metadata: `Metadata::type_info()` now matches the established Thrift
  `getTypeInfo` result shape and current 20-row type catalogue.

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
