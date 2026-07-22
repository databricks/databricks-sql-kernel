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

## Configuration

A session is configured with a `SessionConfig`, assembled from typed
sub-structs (one per concern) and passed to `Session::open`. Every
parameter, its meaning, and its default value is listed below, grouped
by the struct it lives on.

`ConnectionConfig` and `AuthConfig` are **required**; the rest are
optional and fall back to the defaults shown.

### `ConnectionConfig` — connection identity (required)

Construct via `ConnectionConfig::new(host, warehouse_id)` or
`ConnectionConfig::from_http_path(host, http_path)` (which parses the
warehouse id and org id out of a `/sql/1.0/warehouses/{id}` path).

| Field | Description | Default |
|-------|-------------|---------|
| `host` | Workspace URL, e.g. `https://my-workspace.databricks.com`. Bare hostnames get `https://` prepended and any trailing `/` stripped. | **(required)** |
| `warehouse_id` | Warehouse id, e.g. `abc123`. Accepts `/sql/1.0/warehouses/{id}` and legacy `/sql/1.0/endpoints/{id}` forms via `from_http_path`. | **(required)** |
| `org_id` | Workspace organization id for multi-tenant routing; sent as the `x-databricks-org-id` header. Auto-populated from the `?o=` query parameter of the HTTP path. | `None` |

### `AuthConfig` — authentication (required)

An enum; pick one variant. Use the validating constructors
(`AuthConfig::pat`, `::oauth_m2m`, `::oauth_u2m`).

**`Pat`** — personal access token:

| Field | Description | Default |
|-------|-------------|---------|
| `token` | Personal access token (`dapi…`). Must be non-empty. | **(required)** |

**`OAuthM2M`** — machine-to-machine (client credentials, service principal):

| Field | Description | Default |
|-------|-------------|---------|
| `client_id` | OAuth client id. Must be non-empty. | **(required)** |
| `client_secret` | OAuth client secret. Must be non-empty. | **(required)** |
| `scopes` | OAuth scopes to request. | `["all-apis"]` |
| `token_url_override` | Token endpoint override. | OIDC discovery at `<host>/.well-known/oauth-authorization-server` |

**`OAuthU2M`** — user-to-machine (authorization code + PKCE, browser flow):

| Field | Description | Default |
|-------|-------------|---------|
| `client_id` | OAuth client id. | `"databricks-cli"` |
| `redirect_port` | Localhost port for the OAuth callback listener. | `8020` |
| `scopes` | OAuth scopes. `offline_access` is required to receive a refresh token (cached and refreshed transparently). | `["all-apis", "offline_access"]` |
| `token_url_override` | Token endpoint override. | OIDC discovery |
| `callback_timeout` | How long to wait for the user to complete the browser flow. | `120s` |

Two further variants are reserved but not usable in v0:
`OAuthFederation` (RFC 8693 token exchange — returns `InvalidArgument`
until implemented) and `External` (a caller-supplied `TokenProvider`).

### `DefaultOpts` — session defaults (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `catalog` | Default catalog for statements on this session. | `None` (server default) |
| `schema` | Default schema for statements on this session. | `None` (server default) |
| `ignore_transactions` | When `true`, transaction-control calls (`set_autocommit(false)`, `commit()`, `rollback()`) are silently no-oped. Mirrors JDBC `IgnoreTransactions=1`. Set `false` to opt into real multi-statement transactions (Private Preview; requires catalog-managed tables). | `true` |

### `HttpConfig` — HTTP client (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `request_timeout` | Per-request timeout (connect + body). | `120s` |
| `overall_timeout` | Maximum total time across retries for one logical RPC. | `900s` |
| `pool_max_idle_per_host` | Connection-pool size (max idle warm sockets per host). Raise for bursty concurrent workloads. | `100` |
| `retry_min_wait` | Minimum backoff between retries; also the floor for a server `Retry-After` and the base of exponential backoff (`min_wait × 2^attempt`). | `1s` |
| `retry_max_wait` | Maximum backoff between retries; caps both the exponential backoff and a server `Retry-After`. | `60s` |
| `retry_max_retries` | Max retries **after** the first attempt — a request is tried at most `retry_max_retries + 1` times. | `5` (→ 6 total attempts) |
| `custom_headers` | Extra headers on every request; an ordered `Vec<(String, String)>` (duplicates allowed). `Authorization` / `x-databricks-org-id` are silently dropped (reserved); a `User-Agent` is folded into the effective UA. | `[]` |

### `ProxyConfig` — HTTP/HTTPS proxy (optional)

When `url` is `None`, the kernel does not configure a proxy and
`reqwest`'s standard `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` env-var
behaviour applies. Setting `url` **overrides** those env vars.

| Field | Description | Default |
|-------|-------------|---------|
| `url` | Proxy endpoint, e.g. `http://proxy.corp.example.com:8080`. Must be `http://` or `https://`. | `None` (env-var behaviour) |
| `username` | Proxy basic-auth username. Overrides credentials embedded in `url`. | `None` |
| `password` | Proxy basic-auth password. Ignored unless `username` is set. Redacted in `Debug`. | `None` |
| `bypass_hosts` | Comma-separated hosts/domains to bypass the proxy, e.g. `localhost,*.internal.corp,.example.com`. | `None` |

### `TlsConfig` — TLS / SSL (optional)

All defaults are the most secure stance (validate the system trust
store, require a valid chain, verify hostnames). Every non-default field
is a *relaxation* — use for dev / on-prem boxes, never against
production.

| Field | Description | Default |
|-------|-------------|---------|
| `custom_ca_cert` | PEM-encoded CA cert bytes added to the trust store on top of system roots (TLS-inspecting proxies, internal CAs). Multi-cert bundles supported. | `None` |
| `accept_self_signed` | Accept self-signed / untrusted / expired certs. **Disables all chain validation** — dev only. | `false` |
| `skip_hostname_verification` | Skip only the cert-hostname-vs-SNI check while still validating the chain (e.g. connecting via an IP literal). | `false` |
| `client_cert_pem` | PEM-encoded client certificate for mutual TLS (mTLS). Must be set together with the key. | `None` |
| `client_key_pem` | PEM-encoded private key for the mTLS client cert. Supply PKCS#8 for portability across TLS backends. Must be set together with the cert. | `None` |

Setting only one of `client_cert_pem` / `client_key_pem` is rejected at
`Session::open` with `InvalidArgument`.

### `ResultConfig` — result handling (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `cloudfetch_enabled` | Opt into CloudFetch (cloud-storage-backed download of large results via presigned URLs). Set `false` to force the inline-Arrow path (mirrors JDBC `EnableQueryResultDownload=0`). | `true` |
| `cloudfetch_link_prefetch_window` | Number of presigned URLs to prefetch ahead of the consumer. | `10` |
| `cloudfetch_max_chunks_in_memory` | Max decompressed CloudFetch chunks held in memory at once (bounds download parallelism and peak RSS). | `16` |
| `inline_max_chunks_in_memory` | Prefetch window for the inline-Arrow path (used when CloudFetch is disabled). `1` disables prefetching. | `4` |
| `complex_types_as_json` | Render complex columns (`ARRAY` / `MAP` / `STRUCT` / `VARIANT` / `GEOMETRY` / `GEOGRAPHY`) as compact JSON strings instead of native Arrow nested types. | `false` |
| `intervals_as_string` | Render `INTERVAL` / `DURATION` columns as canonical Databricks text (e.g. `"Y-M"`, `"D HH:MM:SS.nnnnnnnnn"`) instead of native Arrow interval types. | `false` |

### `LoggingConfig` — logging (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `filter` | Log filter, e.g. `databricks_sql_kernel=debug`. When `None`, the kernel honours the `RUST_LOG` env var. | `None` (uses `RUST_LOG`) |
| `file` | File to write logs to. When `None`, logs go to stderr. | `None` (stderr) |

### `transport` — wire protocol (optional)

`Option<TransportMode>`. v0 supports `SeaRest` (Statement Execution API
over REST) only; the enum is reserved to grow `SeaGrpc` / `Thrift` in
v1+. `None` lets the kernel pick the default.

Default: `SeaRest`.

### `session_conf` — server-side session configuration (optional)

`session_conf: HashMap<String, String>` carries **server-bound** SQL /
Spark configuration, forwarded to SEA `session_confs` and applied for
every statement on the session. Keys are matched **case-insensitively**
against an allowlist and **uppercased** before being sent; keys not in
the allowlist are **dropped with a warning**.

Allowlisted keys (see the [Databricks SQL configuration-parameters
reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters)
for per-parameter meaning and accepted values):

| Key | Notes |
|-----|-------|
| `ANSI_MODE` | Enable/disable ANSI SQL behaviour. |
| `CAN_CLOUD_DOWNLOAD` | Kernel-synthesised from `ResultConfig::cloudfetch_enabled = false`; a user-supplied value (any casing) wins. |
| `COLLATION` | Default collation. |
| `ENABLE_PHOTON` | Toggle the Photon engine. |
| `LEGACY_TIME_PARSER_POLICY` | Legacy datetime parsing behaviour. |
| `MAX_FILE_PARTITION_BYTES` | Max bytes per file partition. |
| `QUERY_TAGS` | Query tags (comma-separated `key:value`). |
| `READ_ONLY_EXTERNAL_METASTORE` | Treat the external metastore as read-only. |
| `STATEMENT_TIMEOUT` | Server-side per-statement timeout (seconds). This is the real query-timeout knob. |
| `TIMEZONE` | Session time zone, e.g. `UTC`. |
| `USE_CACHED_RESULT` | Toggle result caching. |

Dotted `spark.*` confs are matched case-insensitively but sent
**verbatim** (Spark conf keys are case-sensitive):

| Key | Notes |
|-----|-------|
| `spark.sql.thriftserver.metadata.metricview.enabled` | When `"true"`, `SHOW TABLES` returns `METRIC_VIEW` rows and `getColumns` surfaces `measure` columns. Mirrors JDBC `EnableMetricViewMetadata` (default off). |

Boolean-valued keys should use the exact strings `"true"` / `"false"`;
the kernel does not pre-validate values and forwards them as-is.

#### Client-only `session_conf` keys

Some keys carried in the same `session_conf` map are **client-side**
tuning knobs: the kernel reads them at session creation, applies them to
the typed config above, and then strips them so they never reach the SEA
wire (they are deliberately *not* in the allowlist). They exist so C ABI
(Go / ODBC) consumers can set them through the existing
`session_conf` entry point without a dedicated symbol.

| Key | Description | Default |
|-----|-------------|---------|
| `cloudfetch_max_chunks_in_memory` | Same knob as `ResultConfig::cloudfetch_max_chunks_in_memory` — max decompressed CloudFetch chunks held in memory at once. Matched case-insensitively; parse failures / `0` are ignored (default kept), and values above `256` are clamped (peak RSS scales with this, so the ceiling bounds OOM risk). | `16` |

Compression (LZ4) is **not** a parameter: the kernel detects it from the
server's result manifest (`result_compression`) and decompresses
`LZ4_FRAME` payloads automatically.

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE.txt](NOTICE.txt).
