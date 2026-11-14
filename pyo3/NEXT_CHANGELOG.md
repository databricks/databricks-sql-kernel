# NEXT CHANGELOG — Python binding (`databricks-sql-kernel`)

This file stages unreleased changes for the next Python wheel release.
The Rust crate and Node.js binding stage their own unreleased changes
under [`../NEXT_CHANGELOG.md`](../NEXT_CHANGELOG.md) and
[`../napi/NEXT_CHANGELOG.md`](../napi/NEXT_CHANGELOG.md).

Only land entries here that affect the Python wheel — pyo3 glue, the
Python API surface, packaging / wheel-build behaviour. Pure-kernel
changes go in the root `NEXT_CHANGELOG.md`.

## [Unreleased]

### Added
- `ExecutedStatement.fetch_all_rows(max_rows, nan_ok=False)` (and the same
  on `ResultStream`) returns all remaining rows as tuples of Python values,
  equal to what pyarrow's `to_pylist()` returns, when there are at most
  `max_rows` of them and every column type is supported; otherwise it
  returns `None` and the Arrow fetches return the batches it read.
  `schema_key()` returns a `SchemaKey` that compares equal for equal result
  schemas. Together they let callers handle small results without pyarrow.

### Changed
- `ExecutedStatement.fetch_next_batch()` returns batches that are already
  in memory without releasing the GIL, `close()` after the stream is
  drained no longer releases it either, and `Statement.execute()` on a
  thread other than the main thread no longer re-acquires it every
  100 ms to check for signals that only the main thread can receive.
  This cuts GIL hand-offs per query when many threads share the GIL.
- All sessions in a process share one tokio runtime instead of each
  starting its own worker per CPU, so many concurrent sessions use far
  fewer threads and less CPU per query.
- `Statement.execute()` remembers per thread whether it runs on the main
  thread (recomputed after `fork()`) instead of asking `threading` on every
  call.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
