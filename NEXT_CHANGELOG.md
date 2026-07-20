# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- C ABI: `kernel_abi_version()` + the `DATABRICKS_KERNEL_ABI_VERSION` header
  macro — a consumer that links a prebuilt library can compare the two once at
  load time and refuse to run on a mismatch, guarding against a stale
  header/`.a` layout drift. Bumped in lockstep on any layout/meaning change.
- C ABI: `kernel_session_config_set_tls_client_certificate()` — expose the
  crate's existing mTLS client cert/key (`TlsConfig::client_cert_pem` /
  `client_key_pem`) over the C ABI as a single paired setter. Both halves
  required together (cert-without-key is unrepresentable); the private key is
  never logged.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
