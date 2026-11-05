# Databricks ADBC Driver for Rust

This crate implements ADBC 1.1 for Databricks. It calls the Databricks SQL
Kernel through its native Rust API and returns Arrow 59 record batches without
an IPC or C-ABI conversion between the driver and kernel.

The crate provides two consumer surfaces:

- the Rust `adbc_core` 0.24 traits; and
- an optional ADBC C driver for driver managers and other languages.

The C surface does not change the internal integration: it is an export layer
over the same Rust implementation.

## Build

```text
cargo build -p databricks-adbc --release
cargo build -p databricks-adbc --release --features ffi
```

The `ffi` build exports both `AdbcDatabricksInit` and the conventional
`AdbcDriverInit` alias. The release library is produced as an `rlib`, `cdylib`,
and `staticlib`; see `PACKAGING.md` for release gates.

This crate is a standalone Cargo workspace inside the kernel repository. Cargo
builds the kernel directly from `../` for repository development. The
accompanying version constraint becomes the registry dependency when the
crates are published; publish the matching kernel version first.

## Connect from Rust

Create a `Driver`, then create a database with a workspace URI, one
warehouse selector, and authentication options:

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

Exactly one of `adbc.databricks.http_path` and
`adbc.databricks.warehouse_id` is required. Preserve an organization query
parameter in the HTTP path when the workspace connection details include one.
Canonical `adbc.databricks.*` names take precedence over compatibility aliases.

## Authentication

Set `adbc.databricks.auth.type` to one of `pat`, `oauth_m2m`, `oauth_u2m`,
`oauth_m2m_private_key`, or `azure_service_principal`. When the type is omitted,
the driver infers it only when the supplied credentials are unambiguous.

| Mode | Required options |
| --- | --- |
| PAT | `adbc.databricks.auth.token` |
| OAuth M2M | `adbc.databricks.oauth.client_id`, `adbc.databricks.oauth.client_secret` |
| OAuth U2M | interactive login; optional client ID, scopes, redirect ports, callback timeout, and encrypted token-cache settings |
| Private-key OAuth M2M | client ID, `adbc.databricks.oauth.jwt.key_file`; optional key ID, algorithm, and passphrase |
| Azure service principal | `adbc.databricks.azure.client_id`, client secret, tenant ID, and workspace resource ID |

`adbc.databricks.identity_federation_client_id` can be added to supported base
authentication modes. Client secrets, tokens, passphrases, proxy passwords, and
private keys are write-only and redacted from debug output. Supply them at
runtime; do not place them in source code, manifests, logs, or connection URLs.
Do not include connection options, environment dumps, token-cache contents, or
credential files in bug reports. Rotate a credential immediately if it may
have appeared in output retained by an application or support system.

## Configuration

The main database option families are:

| Family | Options |
| --- | --- |
| Namespace | `adbc.databricks.catalog`, `adbc.databricks.schema` |
| Session | `adbc.databricks.session_conf.<name>`, `adbc.databricks.ssp_ansi_mode`, `adbc.databricks.ssp_timezone`, `adbc.databricks.query_tags` |
| HTTP | request/overall timeouts, idle pool size, retry limits/backoff, response compression, JSON custom headers |
| Proxy | URL, username, password, bypass hosts |
| TLS/mTLS | CA file or PEM, client certificate/key file or PEM, self-signed and hostname-verification controls |
| Logging | filter and file |
| Telemetry | enablement, batch/flush/retry settings, close timeout, circuit breaker settings |
| Results | complex datatype support, geospatial representation, CloudFetch enablement and buffering |
| Execution | `adbc.databricks.enable_direct_results` (default `true`); set `false` to use `wait_timeout=0s` and status polling |
| Metadata | `adbc.databricks.metadata.escape_catalog_name` |

Proxy credentials must use the dedicated options; URL userinfo is rejected.
Certificate and key options must be supplied as a pair. File-backed credential
and TLS values are read during database initialization.
The self-signed-certificate and hostname-verification bypasses weaken TLS and
must remain disabled outside isolated test environments.
Telemetry numeric options accept either ADBC integer values or their decimal
string forms, as used by connection-string and cross-driver adapters.

`adbc.databricks.metadata.escape_catalog_name` defaults to `true`. It escapes
only unescaped `_` characters in `GetObjects` and metadata-command catalog
patterns, matching the catalog-name behavior of the Python and Node drivers.
It preserves `%` wildcards and existing `\_` escapes and does not alter schema,
table, or column patterns. Set it to `false` for raw ADBC catalog-pattern
semantics.

Statement options include positional or named parameter style, query tags,
the portable `adbc.apache.statement.query_timeout_s` client deadline (with `0`
meaning unlimited), a millisecond Databricks client-timeout alias, server
timeout, row limit, and
`adbc.databricks.statement.rows_fetched_per_block` for client-observed Arrow
batch slicing, and `adbc.databricks.statement.conf.<name>`. `bind` and `bind_stream` accept Arrow
batches; multi-row bindings execute sequentially and expose one logical result
stream. `prepare` is client-side. Parameter schemas may contain nullable Arrow
`Null` fields when the server does not expose exact inferred types.

For compatibility with metadata-statement consumers, set
`adbc.apache.statement.is_metadata_command` to the string `true`, set the SQL
text to `GetCatalogs`, `GetSchemas`, `GetTables`, `GetColumns`,
`GetPrimaryKeys`, or `GetCrossReference`, and supply
filters through `adbc.get_metadata.target_catalog`, `target_db_schema`,
`target_table`, `target_column`, and `target_table_types`. `GetCrossReference`
also accepts the foreign-target catalog, schema, and table keys.
`adbc.get_metadata.escape_pattern_wildcards` defaults to `false`; when set to
`true`, backslash, `_`, and `%` are escaped so the supplied filters are treated
as literal identifiers. These
commands return the flat JDBC metadata schemas used by the other Databricks
drivers; `GetColumns` includes the Databricks `BASE_TYPE_NAME` extension. This
compatibility surface is separate from standard nested ADBC `GetObjects`.

Native Rust callers can use `Statement::submit` to obtain a borrowing
`SubmittedStatement`, poll its status, cancel it, or await/fetch its result.
The handle prevents mutation or resubmission of the statement while the
operation is pending. Client query deadlines and rows-per-block slicing apply
equally to synchronous and submitted execution. Dropping a submitted handle or
an incompletely consumed reader triggers best-effort server cleanup.

`adbc.databricks.cloudfetch.enabled` is wired to the kernel result
configuration. The current kernel/server combination does not yet support the
`false` path because SEA rejects inline Arrow results; disabling CloudFetch is
therefore not a production-supported configuration in this release.

## ADBC behavior

The driver supports query/update execution, cancellation, result draining,
`GetInfo`, `GetObjects`, table types, table schemas, catalogs/schemas, and
dedicated primary-key/cross-reference metadata commands. As in the C# driver,
`GetObjects` leaves `table_constraints` null; applications request key metadata
through the dedicated commands described above. Errors preserve ADBC status,
SQLSTATE, vendor code, and Databricks details when supplied by the kernel.

Transactions are ignored by the current kernel configuration: autocommit stays
enabled, and disabling it returns `NotImplemented`. Partitioned execution,
reading partitions, statistics, and progress reporting are optional ADBC
surfaces and currently return `NotImplemented`. Native Rust connection
cancellation is supported. The optional C driver intentionally reports
connection and statement cancellation as `NotImplemented` because `adbc_ffi`
0.24 cannot export concurrent cancel calls without violating Rust aliasing
rules. Native Rust cancellation handles remain available internally.

The standard ADBC methods are synchronous. The native Rust submit/poll extension
provides caller-driven non-blocking execution. Both surfaces are safe to call
from outside Tokio or from current-thread and multi-thread Tokio runtimes;
kernel work is bridged without nesting `Runtime::block_on` on the caller
runtime.

## Compatibility and status

The current implementation targets Rust 1.89+, Rust edition 2021, ADBC 1.1 (`adbc_core` 0.24),
Arrow 59, and `databricks-sql-kernel` 1.4. Linux x86-64 is the locally validated
platform. Windows and macOS artifacts require their platform release gates
before a production release.

Repository CI validates the native Rust surface, the optional ADBC C ABI, and
dynamic loading through both exported entry points. Cross-driver qualification
and release evidence remain separate from this public API documentation.
