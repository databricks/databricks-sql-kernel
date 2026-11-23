# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Opt-in TLS certificate revocation checking, off by default. It is supported
  only on Windows with `tls-rustls`, where Windows CryptoAPI decides the status
  after webpki has verified the chain. Requesting it anywhere else (including
  `tls-native`) fails session open with `InvalidArgument`. Set it with
  `TlsConfig::check_certificate_revocation` and
  `TlsConfig::accept_undetermined_certificate_revocation`, or with
  `kernel_session_config_set_tls_certificate_revocation`. CRL and OCSP fetches
  use Windows' networking and proxy settings, and Windows caches their results.
  The check is synchronous and runs inside the TLS handshake, so a connect or
  operation timeout can be exceeded by up to the revocation budget (about 10 s
  for CRL and OCSP retrieval in total). Session resumption is disabled while
  revocation is on.

### Changed

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
