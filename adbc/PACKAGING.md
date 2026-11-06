# Packaging and release gates

The crate produces three library forms:

- `rlib` for native Rust consumers;
- `cdylib` for ADBC driver-manager loading;
- `staticlib` for consumers that link the ADBC entry point statically.

The optional `ffi` feature exports `AdbcDatabricksInit` and the conventional
`AdbcDriverInit` alias. This is an outer ADBC compatibility layer: the driver
continues to call the SQL kernel through its native Rust API.

Repository CI must validate all of the following before release:

1. `cargo test --manifest-path adbc/Cargo.toml --features ffi` passes on the declared MSRV.
2. A release `cdylib` dynamically loads through both exported entry points
   using `adbc_driver_manager` and allocates an ADBC database.
3. `cargo package --manifest-path adbc/Cargo.toml --list` contains only the manifest's
   source, tests, examples, documentation, and license files.
4. Repository license, advisory, and SBOM checks explicitly scan the standalone
   ADBC dependency graph.
5. Platform-specific release jobs validate Linux, macOS, and Windows artifacts.

Publication remains disabled while the repository and kernel crate are not
publicly released. A release must publish `databricks-sql-kernel` first, then
publish `databricks-adbc` with the matching version requirement. Both crates
are built from the same repository source during development; no kernel FFI or
C ABI is involved in that dependency.

The declared MSRV is Rust 1.89. Cargo dependencies used by internal CI must
resolve through the Databricks package proxy. Release artifacts must include
Apache 2.0 licensing and generated third-party attribution under the owning
repository's release process.
