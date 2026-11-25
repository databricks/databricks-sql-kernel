# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- `ConnectionConfig::with_port(port)` takes a connection port and folds it
  into `host` (as `scheme://host:port`), so bindings no longer build the port
  into the host string themselves. Port `0` and the scheme default (443 for
  `https`, 80 for `http`) change nothing and are not validated. Rejected with
  `InvalidArgument`: userinfo in the host (shown as `<redacted>@host`), an
  empty host, a path/query/fragment, and an embedded port outside `1`–`65535`.
  An embedded port that is already in the host wins, with a warning if it
  differs. Bare IPv6 literals are bracketed.
- C ABI `kernel_session_config_set_port(config, port)`. It validates eagerly
  against the host set so far, and each later host setter re-checks it.
  `host_info.port` in connection telemetry reports the applied port.

### Changed

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
