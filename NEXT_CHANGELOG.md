# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added
- `Statement::execute_direct()` — directResults execution (the Thrift/JDBC model):
  one `ExecuteStatement` POST with the server inline wait, returning
  `DirectStatement::Completed` (terminal, result materialised inline) or
  `DirectStatement::Running` (a poll/cancel handle) **without** an internal poll
  loop. Added **alongside** an unchanged blocking `execute()`.

### Updated
- Removed `StatementSpec::query_timeout_secs` and the `resolve_wait_timeout`
  `CANCEL` branch: it mapped a per-statement timeout onto the SEA `wait_timeout`
  field, which is the server's inline-hold window (capped 5–50s), not a
  statement-execution timeout (use the `STATEMENT_TIMEOUT` SQL conf for that).
  The sync path now always omits `wait_timeout`.

### Fixed
- `terminal_outcome`: `CLOSED` is now an unconditional terminal **success**
  (parity with JDBC's `DatabricksSdkClient`); an empty `CLOSED` materialises an
  empty result set instead of erroring with `invalid_state("Statement was
  closed")`. Reaches all callers (`execute()`, `fetch_completed_result`,
  `execute_direct`).

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
