# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Added Thrift-native metadata routing with compatible public-schema casts and one-based `getColumns` ordinals.
- Added native Arrow `GEOMETRY` / `GEOGRAPHY` results as tagged
  `struct<srid,wkb>` values, including nested values and empty-result schemas.
  The kernel treats Reyder's structured logical manifest as authoritative and
  reconciles attachments that omit the canonical tags without copying child
  buffers.
- Added typed client-side `GEOMETRY` / `GEOGRAPHY` result representation
  selection. The kernel normalizes both standard-warehouse WKT / EWKT and
  Reyden's native SRID+WKB struct to the requested representation. No
  representation setting is sent to SEA.
- Added the default-enabled
  `kernel_session_config_set_enable_geospatial_support` C ABI setting. Enabled
  returns native SRID+WKB Arrow results; disabled returns WKT / EWKT strings.

### Changed

- Native SRID+WKB is now the default geospatial result representation.

- Route function metadata through the Thrift-native metadata path when supported by the server.
- Authenticated SEA requests now use `Accept-Encoding: identity` by default to
  avoid gzipping inline LZ4-compressed Arrow results. Response compression can
  be re-enabled through `HttpConfig` or the C ABI.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
