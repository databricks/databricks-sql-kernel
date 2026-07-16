# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Cooperative cancel token for the blocking C-ABI entry points
  (`kernel_cancel_token_new` / `_cancel` / `_is_cancelled` / `_free`), plus
  cancellable variants `kernel_session_open_cancellable` and
  `kernel_result_stream_next_batch_cancellable`. A host creates a token before a
  blocking call, passes it in, and fires it from another thread when its
  deadline elapses; the call drops its in-flight request future (a real
  `reqwest` abort) and returns `Cancelled`. A null token is byte-for-byte the
  plain path, so existing symbols and the default path are unchanged. Additive
  only — ABI unchanged.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
