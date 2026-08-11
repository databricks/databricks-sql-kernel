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

The kernel exposes a native Databricks v0 async API (`Session` → `Statement` →
`ExecutedStatement` → `ResultStream`, plus a flat `Metadata` surface) intended
for direct Rust use, PyO3 / napi-rs wrappers, and a custom C ABI (skeleton
today) for ODBC / Go consumers. The legacy ADBC layer has been removed.

## Status

Early. Expect breaking changes.

## Building

```bash
cargo build
cargo test
make check    # fmt + clippy + tests
```

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE.txt](NOTICE.txt).
