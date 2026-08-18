# Version Changelog — Rust crate (`databricks-sql-kernel`)

This changelog tracks releases of the Rust crate published to crates.io.
The Python (pyo3) and Node.js (napi) bindings have their own changelogs
under [`pyo3/CHANGELOG.md`](pyo3/CHANGELOG.md) and
[`napi/CHANGELOG.md`](napi/CHANGELOG.md); they ship on independent
cadences and may not move in lockstep with the crate.

## [Unreleased]

### Added
- **Transaction control on `Session`** — `set_autocommit(bool)` /
  `autocommit()` / `commit()` / `rollback()`, exposed over the C ABI as
  `kernel_session_set_autocommit` / `_get_autocommit` / `_commit` /
  `_rollback`. Implemented as session-scoped SQL (`SET AUTOCOMMIT`,
  `COMMIT`, `ROLLBACK`), mirroring the reference/JDBC drivers, so every
  wrapper (ODBC / Python / Go / Node) gets identical transaction
  semantics from one implementation. Gated by the new
  `DefaultOpts::ignore_transactions` (builder shortcut
  `SessionBuilder::ignore_transactions`; C ABI
  `kernel_session_config_set_ignore_transactions`), which defaults to
  `true` — matching the reference/JDBC `IgnoreTransactions=1`: when set,
  the control methods are silent no-ops. Set `false` to opt into real
  multi-statement transactions (Private Preview; requires catalog-managed
  tables server-side).

### Fixed
- **CloudFetch download timeout** — a stalled cloud-storage response no
  longer hangs the result stream indefinitely. The chunk downloader now
  bounds each HTTP GET + body read with a per-chunk deadline
  (`CloudFetchConfig::download_timeout`, default 5 minutes via
  `DEFAULT_DOWNLOAD_TIMEOUT_SECS`), returning a retriable timeout on expiry
  so the provider retry loop can react. The deadline is per *attempt*: the
  retry loop re-applies it on each retry, so the worst-case bound for a
  repeatedly-stalling chunk is `max_retries * download_timeout`. reqwest's
  client-level timeout only covers the header phase, not the
  separately-awaited `response.bytes()`, which previously let a wedged read
  block for the full CI job window. 5 min matches the ADBC C# driver default;
  the Python (Thrift) driver uses a 60s between-bytes read timeout for the
  same purpose.
- **Metadata conformance (catalog match-all wildcard)** — a *literal*
  catalog `%`, `*`, or `""` reaching `Metadata::list_schemas` /
  `list_tables` is now scoped verbatim (`SHOW … IN [CATALOG] \`%\``) and
  matches nothing (0 rows), instead of being silently broadened to
  `SHOW … IN ALL CATALOGS`. Only a `None` catalog means "all catalogs";
  `list_columns` already behaved this way. This makes an escaped `\%`
  catalog (wildcard-escaping ON) and an exact `Identifier("%")` return
  the correct empty result, matching the reference driver. An *unescaped*
  `%` `LikePattern` on `list_tables` is still match-all (one
  `SHOW TABLES IN ALL CATALOGS`), and the C ABI's NULL/`""` → "all
  catalogs" convention is unchanged. Fixes #202, #221.
- **Metadata conformance (`foreign_keys` unspecified foreign table)** —
  `Metadata::foreign_keys` with a `None` `foreign_table` now returns an
  empty result (0 rows, no `SHOW FOREIGN KEYS` issued) instead of an
  `InvalidArgument` error, matching the JDBC SEA `listCrossReferences`
  contract and the ODBC primary-keys-only `SQLForeignKeys` form (covers
  both the parent-set and both-tables-null cases). `foreign_catalog` /
  `foreign_schema` remain required only when a `foreign_table` is
  supplied. Fixes #212 (METADATA-037/038).

## [v0.2.0] - 2026-06-08

**Beta release of the Databricks SQL Kernel.** Bumps the crate out
of its initial-public-release line into a beta-quality cut intended
for downstream-driver adoption (Reyden bring-up in
`databricks-sql-connector` and `@databricks/sql`). Public API
surface is stable; behaviour changes captured below.

### Added
- `Statement::execute_direct()` — directResults execution (the
  Thrift/JDBC model): one `ExecuteStatement` POST with the server
  inline wait, returning `DirectStatement::Completed` (terminal,
  result materialised inline) or `DirectStatement::Running` (a
  poll/cancel handle) **without** an internal poll loop. Added
  **alongside** an unchanged blocking `execute()`.

### Updated
- Removed `StatementSpec::query_timeout_secs` and the
  `resolve_wait_timeout` `CANCEL` branch: it mapped a per-statement
  timeout onto the SEA `wait_timeout` field, which is the server's
  inline-hold window (capped 5–50s), not a statement-execution
  timeout (use the `STATEMENT_TIMEOUT` SQL conf for that). The sync
  path now always omits `wait_timeout`.

### Fixed
- `terminal_outcome`: `CLOSED` is now an unconditional terminal
  **success** (parity with JDBC's `DatabricksSdkClient`); an empty
  `CLOSED` materialises an empty result set instead of erroring
  with `invalid_state("Statement was closed")`. Reaches all callers
  (`execute()`, `fetch_completed_result`, `execute_direct`).

## [v0.1.2] - 2026-06-06

Initial public release of the Rust kernel core.

### Added
- `databricks-sql-kernel` Rust crate published to crates.io.
- Async public API: `Session` → `Statement` → `ExecutedStatement` /
  `ExecutedAsyncStatement` → `ResultStream`, plus a flat `Metadata`
  surface (see `docs/spec-v0.md`).
- SEA backend (Statement Execution API) with CloudFetch and inline-Arrow
  result paths; PAT, OAuth M2M, and OAuth U2M authentication.
