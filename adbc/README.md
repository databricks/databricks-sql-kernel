# Databricks ADBC Driver for Rust

> [!WARNING]
> This implementation is experimental, unsupported, and not currently published
> or distributed. Its API and behavior may change without notice.

This crate implements ADBC 1.1 for Databricks on top of the Databricks SQL
Kernel. It provides the Rust `adbc_core` traits and an optional ADBC C driver
for driver managers and other languages.

## Build

Build the native Rust driver:

```text
cargo build --manifest-path adbc/Cargo.toml --release
```

Enable the optional ADBC C interface with the `ffi` feature:

```text
cargo build --manifest-path adbc/Cargo.toml --release --features ffi
```

The ADBC crate is a standalone Cargo workspace in this repository and depends
directly on the kernel's Rust crate.

## Connect from Rust

Create a driver and configure it with your Databricks workspace, SQL warehouse,
and credentials:

```rust,no_run
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database as _, Driver as _};
use databricks_adbc::Driver;

let mut driver = Driver;
let database = driver.new_database_with_opts([
    (
        OptionDatabase::Uri,
        OptionValue::String("https://example.cloud.databricks.com".into()),
    ),
    (
        OptionDatabase::Other("adbc.databricks.http_path".into()),
        OptionValue::String("/sql/1.0/warehouses/warehouse-id".into()),
    ),
    (
        OptionDatabase::Other("adbc.databricks.auth.token".into()),
        OptionValue::String(std::env::var("DATABRICKS_TOKEN")?),
    ),
])?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Authentication and configuration

The driver supports personal access tokens, OAuth machine-to-machine and
user-to-machine flows, private-key OAuth, and Azure service principals.

Configuration is available for catalogs and schemas, session settings, HTTP
and proxy behavior, TLS, logging, telemetry, result fetching, query execution,
and metadata. Supply credentials at runtime and do not place them in source
code, manifests, logs, or connection URLs.

## Supported functionality

The driver supports query and update execution, parameter binding,
cancellation, Arrow result streaming, and ADBC metadata operations such as
`GetInfo`, `GetObjects`, and table schema discovery. It also supports an
optional C interface and native Rust asynchronous submission and polling.

Transactions, partitioned execution, statistics, and progress reporting are
not currently implemented.

## Volume file operations

Set `adbc.databricks.statement.staging_allowed_local_paths` to a JSON array of
local directories to run the file transfer for Volume `PUT`, `GET` and `REMOVE`
before results are returned. The result rows stay readable. Without the option
no file moves and the result is only the server's instruction row.
The option remains enabled for the statement until set to JSON `null`; an empty
array still permits REMOVE while denying file PUT and GET. Local `..` components collapse
before symlink resolution. REMOVE of a missing object succeeds. Input-stream PUT is unsupported.
The client timeout ends at the terminal SQL status and does not bound Volume transfers.
`SocketTimeout` (milliseconds) also sets the idle limit for Volume transfers;
`adbc.databricks.http_request_timeout` sets only the SQL request deadline.
See the kernel [Volume file operations](../README.md#volume-file-operations).

## Compatibility and status

The current implementation targets Rust 1.89+, Rust edition 2021, ADBC 1.1,
and Arrow 59. Linux x86-64 is the currently validated platform.

The driver remains experimental. Publishing, platform qualification, and
support commitments require separate release-readiness review.
