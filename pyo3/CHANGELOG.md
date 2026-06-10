# Version Changelog — Python binding (`databricks-sql-kernel`)

This changelog tracks releases of the Python wheel published to PyPI as
`databricks-sql-kernel` (built from this `pyo3/` directory). The Rust
crate and Node.js binding have their own changelogs under
[`../CHANGELOG.md`](../CHANGELOG.md) and [`../napi/CHANGELOG.md`](../napi/CHANGELOG.md);
they ship on independent cadences.

## [v0.2.0] - 2026-06-08

**Beta release of the Databricks SQL Kernel (Python binding).**
Ships on PyPI as `databricks-sql-kernel==0.2.0`. The pyo3 surface
is unchanged from v0.1.2; this release picks up the
kernel-side v0.2.0 behaviour changes (see the root
[`../CHANGELOG.md`](../CHANGELOG.md) — `execute_direct`,
`wait_timeout` removal, `CLOSED`-as-success fix). Intended for
downstream-driver adoption in `databricks-sql-connector[kernel]`.

## [v0.1.2] - 2026-06-06

Initial public release of the Python binding.

### Added
- Python bindings published to PyPI as `databricks-sql-kernel`.
- abi3 wheels for Linux (manylinux_2_28 x86_64/aarch64), macOS
  (x86_64/arm64), and Windows (x86_64), plus a source distribution.
- pyo3-based bridge over the Rust kernel's async public API with PyArrow
  interop for result batches.
