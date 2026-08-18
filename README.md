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

# Databricks SQL Kernel

Rust client core for Databricks SQL — the shared engine intended to power the
Databricks ODBC, Python, Go, and Node.js drivers.

The kernel exposes a Databricks-native v0 async API (`Session` →
`Statement` → `ExecutedStatement` → `ResultStream`, plus a flat `Metadata`
surface) for direct Rust use, the PyO3 (Python) and napi-rs (Node.js)
wrappers, and a C ABI for ODBC / Go consumers. See
[`docs/spec-v0.md`](docs/spec-v0.md) for the API spec.

## Status

Early. Expect breaking changes.

## Building

```bash
cargo build
cargo test
make check    # fmt + clippy + tests
```

## Quick start

Open a session with the `SessionBuilder`, then run a statement:

```rust
use databricks_sql_kernel::{Session, AuthConfig, HttpConfig, ExecutedStatementHandle};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
// `HttpConfig` is `#[non_exhaustive]` — construct from the default and
// assign the public fields you want to override:
let mut http = HttpConfig::default();
http.pool_max_idle_per_host = 200;

let session = Session::builder()
    .http_path("my-workspace.databricks.com", "/sql/1.0/warehouses/abc123")
    .auth(AuthConfig::pat(Some(std::env::var("DATABRICKS_TOKEN")?))?)
    // optional config groups — omit to accept the defaults below:
    .http(http)
    .open()
    .await?;

let mut stmt = session.statement();
stmt.spec().sql("SELECT 1 AS n");
let mut exec = stmt.execute().await?;

let stream = exec.result_stream_mut();
while let Some(_batch) = stream.next_batch().await? { /* consume rows */ }

exec.close().await?;
session.close().await?;
    Ok(())
}
```

Each builder setter takes one config group. `.http_path(host, path)` is
shorthand for a `ConnectionConfig`; `.connection(...)`, `.auth(...)`,
`.defaults(...)`, `.http(...)`, `.proxy(...)`, `.tls(...)`,
`.result(...)`, `.logging(...)`, `.session_conf(...)`, and
`.transport(...)` set the rest.

## Configuration

A session is configured with a `SessionConfig` — typed sub-structs (one
per concern) passed to `Session::open`. `ConnectionConfig` and `AuthConfig`
are required; the rest are optional. See
[CONNECTION_PARAMETERS.md](CONNECTION_PARAMETERS.md) for every parameter —
its meaning, default value, and the struct it lives on.

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE.txt](NOTICE.txt).
