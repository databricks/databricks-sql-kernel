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

The separate [`adbc/`](https://github.com/databricks/databricks-sql-kernel/tree/main/adbc) Cargo workspace implements ADBC 1.1 directly over this
native Rust API. It provides both Rust traits and an optional standard ADBC C
entry point for driver managers and other languages; the kernel boundary does
not use FFI.

## Status

The native Rust API is production-ready. Its public APIs continue to evolve, so
expect breaking changes. The custom C ABI remains under development.

The Rust ADBC implementation under [`adbc/`](adbc/) is experimental,
unsupported, and not currently published or distributed.

## Building

```bash
cargo build
cargo test --workspace

# The following commands require a full repository checkout.
make check    # workspace fmt + clippy + tests and native ABI smoke tests
cargo test --manifest-path adbc/Cargo.toml --features ffi
```

Native consumers that statically link the kernel and publish separate crash
symbols can use the opt-in `release-with-debuginfo` profile. See
[`docs/release-debuginfo.md`](docs/release-debuginfo.md) for build, symbol
splitting, packaging, and binary-identity guidance.

## Configuration

See [CONNECTION_PARAMETERS.md](CONNECTION_PARAMETERS.md) for every connection, auth, and session parameter the kernel accepts — its meaning, default value, and the config struct it lives on.

## Community and support

See [CONTRIBUTING.md](CONTRIBUTING.md) to propose changes. Participation is
governed by the [Databricks Open Source Code of Conduct](CODE_OF_CONDUCT.md),
and security vulnerabilities should be reported according to
[SECURITY.md](SECURITY.md). For other project questions, contact
[eng-oss-sql-driver@databricks.com](mailto:eng-oss-sql-driver@databricks.com).

## Volume file operations

`ResultStream::complete_volume_operation` completes server-marked SQL PUT, GET,
and REMOVE instructions. Pass `VolumeOptions` with the local roots authorized for
PUT and GET. Completion preserves the instruction rows; callers that want the raw
result can omit completion. Complete the operation before reading its instruction
rows. The PyO3 `ExecutedStatement` and `ResultStream` classes expose
`is_volume_operation` and `complete_volume_operation`.

Local filesystem setup and download publication are synchronous. Cancellation
cannot interrupt a blocked filesystem call and can wait for the atomic rename
that publishes a completed GET.

A PUT instruction with `localFile == "__input_stream__"` returns
`NeedsInputStream` until owned input bytes are supplied. An empty byte buffer is a
valid upload. Other statements do not request or consume input bytes.

Local paths must satisfy both the lexical allowed-root check and a resolved-target
check. Symlinks within the configured roots are permitted, including links between
two authorized roots. Symlinks that resolve outside all authorized roots are
rejected. This is stricter than the lexical-only checks in existing Python and
Node.js Thrift transfer paths. An allowed root's symlink alias does not authorize
additional lexical paths.

GET replaces the destination only after a complete download. Transfer failures
and cancellation before publication preserve the destination's existing bytes.
Repeated completion on the same execution reuses its outcome, including errors.

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
