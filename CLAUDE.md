<!--
  Copyright (c) 2026 Databricks, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# CLAUDE.md — Databricks SQL Kernel

Rust client core for Databricks SQL. Exposes a Databricks-native v0
async API (`Session` → `Statement` → `ExecutedStatement` /
`ExecutedAsyncStatement` → `ResultStream`, plus a flat `Metadata`
surface) intended for direct Rust use, PyO3 / napi-rs wrappers, and a
custom C ABI (skeleton today) for ODBC / Go consumers.

The standalone `adbc/` workspace implements ADBC 1.1 directly against that native
Rust API. Its optional standard ADBC C ABI is an outer compatibility surface;
the ADBC-to-kernel boundary remains native Rust.

The repo was seeded from `adbc-drivers/databricks` (Rust subtree); the
v0 surface lives at the crate root and the legacy ADBC layer has been
removed.

## Build & Test

```bash
cargo build
cargo test                                  # all tests, including doc tests
cargo +stable fmt --all
cargo clippy --all-targets -- -D warnings
make check                                  # fmt + clippy + tests
```

`cargo test` (no filters) is what CI runs. `--lib --tests` skips doc tests.

## Pre-commit checklist

```bash
cargo +stable fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

## PR conventions

- Title follows [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/): `type(scope): description`. No JIRA prefixes — CI rejects them.
- Commits must be DCO-signed (`git commit -s`). Enforced by `.github/workflows/dco-check.yml`.

## Architecture

v0 surface (see `docs/spec-v0.md`):

```
Session -> Statement -> ExecutedStatement       -> ResultStream
                     -> ExecutedAsyncStatement  -> ResultStream
Session -> Metadata (flat per-method catalog API)
```

| Module | Purpose |
|--------|---------|
| `session/` | `Session` + `SessionBuilder` (stateful authenticated handle) |
| `statement/` | `Statement` (mutable), `ExecutedStatement`, `ExecutedAsyncStatement`, `StatementSpec`, `TypedValue` |
| `result_stream/` | `ResultStream` + borrowed `Row<'a>` cursor over Arrow batches |
| `metadata_v0/` | Flat per-method `Metadata` surface (`catalogs`, `schemas`, `tables`, …) |
| `config/` | Typed `SessionConfig` / `AuthConfig` / `ConnectionConfig` / etc. |
| `kernel_error/` | `Error` + `ErrorCode` (kernel-native error type) |
| `auth/` | Authentication (PAT, OAuth M2M, OAuth U2M) |
| `client/` | HTTP transport to Databricks (SEA, retry, transport trait) |
| `reader/` | Result fetching (CloudFetch, inline Arrow) |
| `result/` | Result set abstraction over Arrow RecordBatches |
| `metadata/` | Shared SQL builders + schemas for `Metadata` |
| `ffi/` | Custom C ABI for ODBC / Go (skeleton) |
| `telemetry/` | Metrics collection |
| `adbc/` | Native Rust ADBC driver and optional driver-manager C ABI |

## Error handling

Internal code returns `crate::Result<T>`, built via the categorised
constructors on `Error` (`Error::invalid_argument()`, `Error::io()`,
`Error::not_implemented()`, `Error::invalid_state()`, …) chained with
`.message(...)` / `.with_source(...)` / `.sqlstate(...)`.

## Adding Cargo dependencies

CI runs `cargo-deny check` (`.github/workflows/license-check.yml`)
and fails on unapproved licenses or RustSec advisories. New
permissive licenses must be added to `[licenses].allow` in
`deny.toml` with PR-description justification. `about.toml` is
preserved separately for generating attribution pages on demand;
the CI gate lives on `deny.toml`.

## Supply-chain hardening

Per [go/hardened-gha](https://docs.google.com/document/d/1zoHgolfZO_IUBIU3w40NmYsoZ9eTRdAmjlfwYOxkj70/edit),
the protected runners block direct access to public package registries
(crates.io, PyPI, npm, etc.). Third-party dependency fetches route through
Databricks-filtered proxies.

How this shows up in the repo:

- **GitHub Actions steps** are SHA-pinned, with the version tag in
  a trailing comment (e.g. `uses: actions/checkout@34e1148... # v4`).
- **Fork-safe public dependencies** use
  `.github/actions/setup-public-package-proxies`. This credential-free
  composite configures Cargo and Python to use the filtered public proxies and
  does not require `id-token: write`; use it for jobs that run fork-controlled
  code and do not need private packages.
- **Authenticated or private dependencies** use
  `.github/actions/setup-jfrog`. This composite exchanges GitHub OIDC for
  access to `db-cargo-remote` and `db-pypi`, so callers must grant
  `id-token: write` and must not expose that job to fork-controlled code.
- `Cargo.lock` carries per-crate SHA256 checksums (`source = "registry+..."` +
  `checksum = "..."`), which is Cargo's native pinning mechanism. The pyo3
  sub-crate uses [uv](https://docs.astral.sh/uv/) for Python env management.
- **Python dep pinning follows the externally-published-library
  rule from [go/pinning-python-deps](https://docs.google.com/document/d/1thJsoOBW3NzvRV6pWpujcD8mn4KOXgAW8t10iw6Z6LA/edit)**:
  major-version upper bounds in `pyo3/pyproject.toml`
  (`pyarrow>=14,<19`), no exact pins, no transitive constraints,
  no committed lockfile. Exact pins would impede downstream
  `pip install databricks-sql-kernel` for end-users with their
  own pyarrow consumers. The `db-pypi` proxy's 7-day age filter
  is the baseline supply-chain protection; `uv sync` re-resolves
  each CI run within the major-version bounds.
- **Composite actions**: both proxy setup actions write Cargo and uv/pip
  configuration; only `setup-jfrog` performs an OIDC token exchange.

## Local development environment

Connection variables for E2E tests can be set via `.cargo/config.toml`
(gitignored — do **not** commit) under `[env]`:

```toml
[env]
DATABRICKS_HOST = "https://your-workspace.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/abc123"
DATABRICKS_TOKEN = "your-pat-token"
RUST_LOG = "databricks_sql_kernel=debug"
```

E2E tests requiring a live Databricks connection are marked `#[ignore]` and run only when those variables are set.

## References

- [Databricks SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
- [Apache Arrow Rust](https://docs.rs/arrow/latest/arrow/)
- `docs/spec-v0.md` — v0 Kernel API spec
- `docs/migration-plan.md` — staged migration plan from ADBC to v0
