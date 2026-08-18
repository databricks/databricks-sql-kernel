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

The kernel is currently exposed via the [ADBC](https://arrow.apache.org/adbc/)
trait surface from `adbc_core`. A native (non-ADBC) Rust API and a stable C
ABI for ODBC/Go consumers are planned.

## Status

Early. Expect breaking changes.

## Building

```bash
cargo build
cargo test
make check    # fmt + clippy + tests
```

## Configuration

See [CONNECTION_PARAMETERS.md](CONNECTION_PARAMETERS.md) for every connection, auth, and session parameter the kernel accepts — its meaning, default value, and the config struct it lives on.

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE.txt](NOTICE.txt).
