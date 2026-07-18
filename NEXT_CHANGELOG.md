# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- C ABI: `kernel_session_config_set_retry_config()` — expose the crate's existing
  HTTP retry knobs (`HttpConfig::retry_min_wait` / `retry_max_wait` /
  `retry_max_retries` / `overall_timeout`) over the C ABI so a host can make its own
  `RetryWaitMin` / `RetryWaitMax` / `RetryMax` / retry-budget policy authoritative
  instead of the kernel default; `max_retries=0` disables retries, and
  `overall_timeout_ms=0` keeps the kernel's default 900s budget (non-zero overrides
  it, matching the pyo3/napi `retry_overall_timeout` knob). Benefits ODBC too.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
