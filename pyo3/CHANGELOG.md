# Version Changelog — Python binding (`databricks-sql-kernel`)

This changelog tracks releases of the Python wheel published to PyPI as
`databricks-sql-kernel` (built from this `pyo3/` directory). The Rust
crate and Node.js binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../napi/CHANGELOG.md`](../napi/CHANGELOG.md);
they ship on independent cadences.

## [Unreleased]

### Added
- **U2M on-disk token-cache control** — `Session(…, token_cache_enabled=…,
  token_cache_passphrase=…)` lets a caller enable/disable (or set the AES-256
  passphrase for) the kernel's built-in encrypted disk cache on the kernel path.
  Omitted keeps the kernel default (enabled, machine-local key, survives
  restarts); `token_cache_enabled=False` re-logins each fresh process; a
  passphrase with no explicit flag implies enabled. Applies to `oauth-u2m`.
  The `databricks-sql-python` connector maps its `experimental_oauth_persistence`
  presence onto this enable flag (#913). (PECOBLR-4146)

## [v0.2.0] - 2026-06-08

**Beta release of the Databricks SQL Kernel (Python binding).**
Ships on PyPI as `databricks-sql-kernel==0.2.0`. Picks up the
kernel-side v0.2.0 behaviour changes (see the root
[`../CHANGELOG.md`](../CHANGELOG.md) — `execute_direct`,
`wait_timeout` removal, `CLOSED`-as-success fix) plus the pyo3-side
fix below. Intended for downstream-driver adoption in
`databricks-sql-connector[kernel]`.

### Fixed
- `auth_type="oauth-u2m"` now defaults scopes to `["sql", "offline_access"]`
  when the caller passes neither `client_id` nor `scopes` (#153). The
  Python binding defaults the U2M client to `databricks-sql-python` (vs.
  the kernel's `databricks-cli`), but that client isn't registered for
  the `all-apis` scope the kernel intrinsically defaults to — so an
  out-of-the-box `Session(auth_type="oauth-u2m")` failed the
  authorization request with `access_denied — Scopes 'all-apis' are not
  assigned to the client databricks-sql-python`. Defaults now match
  `PYSQL_OAUTH_SCOPES` in the standalone Thrift connector, restoring
  `use_kernel=True` parity. A caller-supplied `client_id` or `scopes` is
  still honoured verbatim.

## [v0.1.2] - 2026-06-06

Initial public release of the Python binding.

### Added
- Python bindings published to PyPI as `databricks-sql-kernel`.
- abi3 wheels for Linux (manylinux_2_28 x86_64/aarch64), macOS
  (x86_64/arm64), and Windows (x86_64), plus a source distribution.
- pyo3-based bridge over the Rust kernel's async public API with PyArrow
  interop for result batches.
