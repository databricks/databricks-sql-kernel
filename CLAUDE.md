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

Rust client core for Databricks SQL. Currently exposed via the ADBC trait
surface from `adbc_core`; a native Rust API and a stable C ABI for ODBC/Go
consumers are planned.

The repo was seeded from `adbc-drivers/databricks` (Rust subtree) and is
being evolved into the kernel.

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
- Commits must be DCO-signed (`git commit -s`).

## Architecture

```
Driver -> Database -> Connection -> Statement -> ResultSet
```

| Module | Purpose |
|--------|---------|
| `auth/` | Authentication (PAT, OAuth M2M, OAuth U2M) |
| `client/` | HTTP communication with Databricks (SEA, retry) |
| `reader/` | Result fetching (CloudFetch, inline Arrow) |
| `result/` | Result set abstraction over Arrow RecordBatches |
| `metadata/` | Catalog/schema/table metadata |
| `ffi/` | C ABI for metadata propagation |
| `telemetry/` | Metrics collection |

## Error handling

- Internal code returns `crate::Result<T>`, built via `DatabricksErrorHelper` (`invalid_argument()`, `io()`, `not_implemented()`, `invalid_state()`, …).
- ADBC trait implementations (in `connection.rs`, `database.rs`, `statement/`) return `adbc_core::error::Result`. Convert with `.to_adbc()`.

## License headers

Apache 2.0 headers are required on all source files; CI runs Apache RAT.

Rust:
```rust
// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...
```

Markdown (HTML comment):
```html
<!--
  Copyright (c) 2026 Databricks, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  ...
-->
```

## Adding Cargo dependencies

CI runs `cargo-about` and fails on unapproved licenses. New permissive licenses must be added to the `accepted` list in `about.toml`.

## Local development environment

Connection variables for examples and E2E tests can be set via
`.cargo/config.toml` (gitignored — do **not** commit) under `[env]`:

```toml
[env]
DATABRICKS_HOST = "https://your-workspace.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/abc123"
DATABRICKS_TOKEN = "your-pat-token"
RUST_LOG = "databricks_sql_kernel=debug"
```

E2E tests requiring a live Databricks connection are marked `#[ignore]` and run only when those variables are set.

## References

- [ADBC specification](https://arrow.apache.org/adbc/)
- [Databricks SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
- [Apache Arrow Rust](https://docs.rs/arrow/latest/arrow/)
