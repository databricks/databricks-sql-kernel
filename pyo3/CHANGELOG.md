# Version Changelog — Python binding (`databricks-sql-kernel`)

This changelog tracks releases of the Python wheel published to PyPI as
`databricks-sql-kernel` (built from this `pyo3/` directory). The Rust
crate and Node.js binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../napi/CHANGELOG.md`](../napi/CHANGELOG.md);
they ship on independent cadences.

## [Unreleased]

### Added
- **Pluggable OAuth token store via `oauth_persistence`** — `Session(…,
  oauth_persistence=obj)` lets a caller supply a custom OAuth U2M token store on
  the kernel path instead of the built-in disk cache. `obj` must expose
  `read(host) -> Optional[str]` and `persist(host, token_json) -> None`,
  exchanging the kernel's `OAuthToken` as a JSON string. Invoked under the GIL.
  The `databricks-sql-python` connector adapts its `experimental_oauth_persistence`
  onto this (#913). (PECOBLR-4146)

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
