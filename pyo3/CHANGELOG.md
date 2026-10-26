# Version Changelog — Python binding (`databricks-sql-kernel`)

This changelog tracks releases of the Python wheel published to PyPI as
`databricks-sql-kernel` (built from this `pyo3/` directory). The Rust
crate and Node.js binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../napi/CHANGELOG.md`](../napi/CHANGELOG.md);
they ship on independent cadences.

## [Unreleased]

### Added

### Changed

### Fixed

## [v1.3.0] - 2026-10-23

### Updated
- Bundle the kernel's TLS and runtime dependency security updates.

### Fixed
- Preserve manifest column nullability in empty-result PyArrow schemas when
  no Arrow schema is supplied.

## [v1.2.0] - 2026-10-19

### Added
- The default-enabled `Session(..., enable_geospatial_support=...)` argument
  selects native SRID+WKB Arrow results when enabled and WKT / EWKT strings
  when disabled.
- `Session(..., sea_response_compression_enabled=...)` opts back into HTTP
  compression for bandwidth-constrained SEA connections.

### Changed
- An omitted `telemetry_enabled` allows workspace feature-flag enablement;
  an explicit `True` or `False` still takes precedence.

### Fixed
- Fixed an intermittent hang in `Session.close()` or handle cleanup when a
  runtime worker was forwarding a log record to Python `logging` during
  runtime shutdown.

## [v1.1.0] - 2026-09-23

### Added
- `Session(..., max_connections=...)` configures idle HTTP connections per
  host.

## [v1.0.0] - 2026-08-31

### Fixed
- `ExecutedAsyncStatement.close()` now finalizes async statement telemetry in
  partial-fetch flows.

## [v0.3.0] - 2026-08-28

### Added
- `Session(..., token_cache_enabled=..., token_cache_passphrase=...)` controls
  the encrypted U2M disk cache.
- `Session(..., request_timeout_secs=...)` configures the per-request timeout;
  unset or `0` keeps the kernel default.
- `telemetry_circuit_breaker_enabled`, `telemetry_circuit_breaker_threshold`,
  and `telemetry_circuit_breaker_timeout_ms` configure telemetry failure
  isolation.

### Fixed
- Empty catalog filters for `list_schemas()` and `list_columns()` return empty
  results instead of `InvalidArgument`.
- Explicit synchronous statement close finalizes telemetry after partial
  result consumption.

## [v0.2.1] - 2026-08-24

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
