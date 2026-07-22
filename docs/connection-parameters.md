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

# Connection Parameters

Every configuration knob the Databricks SQL Kernel accepts, grouped by
concern, with its meaning and default value.

The kernel is consumed through three surfaces, and each names the same
underlying knob slightly differently:

| Surface | How parameters are supplied |
|---------|-----------------------------|
| **Rust** (`databricks-sql-kernel`) | Typed config structs on `SessionConfig` — one sub-struct per concern (`ConnectionConfig`, `AuthConfig`, `HttpConfig`, `TlsConfig`, `ProxyConfig`, `ResultConfig`, `DefaultOpts`, `LoggingConfig`), assembled and passed to `Session::open`. |
| **Python** (`databricks-sql-kernel` wheel) | Keyword arguments to `Session(host, http_path, **kwargs)`. |
| **Node.js** (`@databricks/databricks-sql-kernel`) | Fields on the `ConnectionOptions` object passed to `openSession(options)`. |

Each table below lists the **Rust field**, the **Python kwarg**, and the
**Node.js option** for the same parameter, so wrapper authors and driver
users can cross-reference. A blank cell means that binding does not
expose the knob yet (the Rust core always does).

> **Note on defaults.** The "Default" column is the kernel-core default
> from `src/config.rs`. Some bindings apply a binding-specific default
> that differs — those are called out in the notes under each table.

---

## Table of contents

- [Connection identity](#connection-identity) — host, warehouse, org
- [Authentication](#authentication) — PAT, OAuth M2M, OAuth U2M
- [Session defaults](#session-defaults) — catalog, schema, transactions
- [HTTP client](#http-client) — timeouts, pool size, retries, headers
- [Proxy](#proxy)
- [TLS / SSL](#tls--ssl)
- [Result handling](#result-handling) — CloudFetch, type rendering
- [Logging](#logging)
- [Server-side session configuration](#server-side-session-configuration) — `session_conf`
- [Transport](#transport)

---

## Connection identity

Required. Identifies the workspace and warehouse to connect to.

| Rust (`ConnectionConfig`) | Python kwarg | Node.js (`ConnectionOptions`) | Description | Default |
|---|---|---|---|---|
| `host` | `host` | `hostName` | Workspace URL, e.g. `https://my-workspace.databricks.com`. Bare hostnames get `https://` prepended and any trailing `/` stripped. | **(required)** |
| `warehouse_id` | (parsed from `http_path`) | (parsed from `httpPath`) | Warehouse id, e.g. `abc123`. In Rust use `ConnectionConfig::from_http_path` to parse it out of a `/sql/1.0/warehouses/{id}` path; the bindings accept the full HTTP path directly. | **(required)** |
| `org_id` | (parsed from `http_path`) | (parsed from `httpPath`) | Workspace organization id for multi-tenant routing. Sent as the `x-databricks-org-id` header. Auto-populated from the `?o=` query parameter of the HTTP path. | `None` |

**Notes**

- Accepted HTTP-path forms: `/sql/1.0/warehouses/{id}` (canonical) and
  `/sql/1.0/endpoints/{id}` (legacy, equivalent server-side), with or
  without a leading `/`, and with an optional `?o={org_id}` suffix.
- Python / Node.js take `http_path` / `httpPath` and the kernel parses
  the warehouse id and org id out of it.

---

## Authentication

Required. The kernel supports a personal access token and two OAuth 2.0
flows. Select the flow with `auth_type` (Python) / `authMode` (Node.js),
or by choosing the `AuthConfig` variant (Rust). Omitting the selector
defaults to **PAT** when a token is supplied.

### Personal access token (PAT)

| Rust (`AuthConfig::Pat`) | Python kwarg | Node.js | Description | Default |
|---|---|---|---|---|
| `token` | `access_token` | `token` | Personal access token (`dapi…`). Must be non-empty. | **(required for PAT)** |

### OAuth 2.0 machine-to-machine (M2M, client credentials)

Service-principal auth. Select with `auth_type="oauth-m2m"` / `authMode: OAuthM2m`.

| Rust (`AuthConfig::OAuthM2M`) | Python kwarg | Node.js | Description | Default |
|---|---|---|---|---|
| `client_id` | `client_id` | `oauthClientId` | OAuth client id. Must be non-empty. | **(required for M2M)** |
| `client_secret` | `client_secret` | `oauthClientSecret` | OAuth client secret. Must be non-empty. | **(required for M2M)** |
| `scopes` | `oauth_scopes` | `oauthScopes` | OAuth scopes to request. | `["all-apis"]` |
| `token_url_override` | `token_url` | | Token endpoint override. | OIDC discovery at `<host>/.well-known/oauth-authorization-server` |

### OAuth 2.0 user-to-machine (U2M, browser flow)

Authorization code + PKCE. The kernel opens the user's browser and runs a
localhost listener for the callback. Select with `auth_type="oauth-u2m"` /
`authMode: OAuthU2m`.

| Rust (`AuthConfig::OAuthU2M`) | Python kwarg | Node.js | Description | Default |
|---|---|---|---|---|
| `client_id` | `client_id` | `oauthClientId` | OAuth client id. | `"databricks-cli"` |
| `redirect_port` | `redirect_port` | `oauthRedirectPort` | Localhost port for the OAuth callback listener. | `8020` |
| `scopes` | `oauth_scopes` | `oauthScopes` | OAuth scopes. `offline_access` is needed for a refresh token (cached and refreshed transparently). | `["all-apis", "offline_access"]` |
| `token_url_override` | `token_url` | | Token endpoint override. | OIDC discovery |
| `callback_timeout` | `oauth_callback_timeout_secs` | | How long to wait for the user to complete the browser flow. | `120s` |

**Notes**

- Two further Rust variants exist but are **not** exposed by the bindings:
  `AuthConfig::OAuthFederation` (RFC 8693 token exchange — reserved,
  returns `InvalidArgument` until implemented) and `AuthConfig::External`
  (a caller-supplied `TokenProvider` trait, a Rust-only escape hatch).
- The Node.js binding defaults the U2M client id to
  `databricks-sql-connector` (rather than the core's `databricks-cli`).

---

## Session defaults

Optional. Per-session defaults applied to every statement.

| Rust (`DefaultOpts`) | Python kwarg | Node.js | Description | Default |
|---|---|---|---|---|
| `catalog` | `catalog` | `catalog` | Default catalog for statements on this session (SEA `CreateSession.catalog`). | `None` (server default) |
| `schema` | `schema` | `schema` | Default schema for statements on this session (SEA `CreateSession.schema`). | `None` (server default) |
| `ignore_transactions` | | | When `true`, transaction-control calls (`set_autocommit(false)`, `commit()`, `rollback()`) are silently no-oped. Mirrors JDBC `IgnoreTransactions=1`. Set `false` to opt into real multi-statement transactions (Private Preview; requires catalog-managed tables). | `true` |

---

## HTTP client

Optional. Tunes timeouts, the connection pool, retry/backoff, and custom
headers.

| Rust (`HttpConfig`) | Python kwarg | Node.js | Description | Default |
|---|---|---|---|---|
| `request_timeout` | | `socketTimeoutMs` | Per-request timeout (connect + body) — caps a single HTTP round-trip. | `120s` |
| `overall_timeout` | `retry_overall_timeout_secs` | `retryOverallTimeoutSecs` | Maximum total time across retries for one logical RPC. | `900s` |
| `pool_max_idle_per_host` | | `maxConnections` | Connection-pool size (max idle warm sockets per host). Raise for bursty concurrent workloads. | `100` |
| `retry_min_wait` | `retry_min_wait_secs` | `retryMinWaitSecs` | Minimum backoff between retries; also the floor for a server `Retry-After` and the base of exponential backoff (`min_wait × 2^attempt`). | `1s` |
| `retry_max_wait` | `retry_max_wait_secs` | `retryMaxWaitSecs` | Maximum backoff between retries; caps both the exponential backoff and a server `Retry-After`. | `60s` |
| `retry_max_retries` | `retry_max_attempts` | `retryMaxAttempts` | See note below on retries-vs-attempts. | `5` retries (`6` total attempts) |
| `custom_headers` | `http_headers` | `customHeaders` | Extra headers on every request. Ordered list of `(name, value)`; duplicates allowed. | `[]` (none) |

**Notes**

- **Retries vs. attempts.** The Rust `retry_max_retries` counts retries
  *after* the first attempt (so `5` → 6 total attempts). The Python
  `retry_max_attempts` and Node.js `retryMaxAttempts` are **total
  attempts** (matching the connector's `_retry_stop_after_attempts_count`
  and JDBC), converted internally as `retry_max_retries = max(0, attempts − 1)`.
  Passing `0` or `1` both mean "one attempt, no retries".
- **Reserved headers.** `Authorization` and `x-databricks-org-id` in
  `custom_headers` are silently dropped (with a warning) so auth and
  multi-tenant routing can't be hijacked. A caller `User-Agent` is folded
  into the effective UA rather than sent as a duplicate.
- The Node.js binding applies a binding-specific `maxConnections` default
  of `100` when unset (matches the core).

---

## Proxy

Optional. Programmatic HTTP/HTTPS proxy. When unset, the kernel does not
configure a proxy and `reqwest`'s standard `HTTP_PROXY` / `HTTPS_PROXY` /
`NO_PROXY` environment-variable behaviour applies. Setting `url`
**overrides** those env vars.

| Rust (`ProxyConfig`) | Python kwarg | Node.js (`ProxyInput`) | Description | Default |
|---|---|---|---|---|
| `url` | | `proxy.url` | Proxy endpoint, e.g. `http://proxy.corp.example.com:8080`. Must be `http://` or `https://`. | `None` (env-var behaviour) |
| `username` | | `proxy.username` | Proxy basic-auth username. Overrides credentials embedded in `url`. | `None` |
| `password` | | `proxy.password` | Proxy basic-auth password. Ignored unless `username` is set. Redacted in logs. | `None` |
| `bypass_hosts` | | `proxy.bypassHosts` | Comma-separated hosts/domains to bypass the proxy, e.g. `localhost,*.internal.corp,.example.com`. | `None` |

**Notes**

- The Python binding does not currently expose a programmatic proxy;
  Python users rely on the `HTTP(S)_PROXY` environment variables.

---

## TLS / SSL

Optional. **All defaults are the most secure stance:** validate the
system trust store, require a valid chain, and verify hostnames. Every
non-default field is a *relaxation* — use for dev / on-prem boxes, never
against production.

| Rust (`TlsConfig`) | Python kwarg | Node.js | Description | Default |
|---|---|---|---|---|
| `custom_ca_cert` | `tls_ca_cert` | `customCaCert` | PEM-encoded CA cert bytes added to the trust store on top of system roots (for TLS-inspecting proxies or internal CAs). Multi-cert bundles (root + intermediate) are supported. | `None` |
| `accept_self_signed` | `tls_skip_verify` | `checkServerCertificate` (inverted) | Accept self-signed / untrusted / expired certs. **Disables all chain validation** — dev only. | `false` (validation on) |
| `skip_hostname_verification` | `tls_skip_hostname_verify` | `checkServerCertificateHostname` (inverted) | Skip only the cert-hostname-vs-SNI check while still validating the chain (e.g. connecting via an IP literal). | `false` (check on) |
| `client_cert_pem` | `tls_client_cert` | `clientCertPem` | PEM-encoded client certificate for mutual TLS (mTLS). Must be set together with the key. | `None` |
| `client_key_pem` | `tls_client_key` | `clientKeyPem` | PEM-encoded private key for the mTLS client cert. Supply PKCS#8 for portability across TLS backends. Must be set together with the cert. | `None` |

**Notes**

- The Node.js knobs are **positive** booleans: `checkServerCertificate`
  (default `true`) is the master verify toggle — setting it `false`
  disables chain validation *and* subsumes the hostname check;
  `checkServerCertificateHostname` (default `true`) toggles only the
  hostname check. These map to the inverse of the Rust
  `accept_self_signed` / `skip_hostname_verification` fields.
- Setting only one of `client_cert_pem` / `client_key_pem` is rejected at
  `Session::open` with `InvalidArgument`.

---

## Result handling

Optional. Tunes CloudFetch and how result columns are rendered.

| Rust (`ResultConfig`) | Python kwarg | Node.js | Description | Default |
|---|---|---|---|---|
| `cloudfetch_enabled` | | | Opt into CloudFetch (cloud-storage-backed download of large results via presigned URLs). Set `false` to force the inline-Arrow path (mirrors JDBC `EnableQueryResultDownload=0`). | `true` |
| `cloudfetch_link_prefetch_window` | | | Number of presigned URLs to prefetch ahead of the consumer. | `10` |
| `cloudfetch_max_chunks_in_memory` | | | Max decompressed CloudFetch chunks held in memory at once (bounds download parallelism and peak RSS). | `16` |
| `inline_max_chunks_in_memory` | | | Prefetch window for the inline-Arrow path (used when CloudFetch is disabled). `1` disables prefetching (strict lazy fetch). | `4` |
| `complex_types_as_json` | `complex_types_as_json` | `complexTypesAsJson` | Render complex columns (`ARRAY` / `MAP` / `STRUCT` / `VARIANT` / `GEOMETRY` / `GEOGRAPHY`) as compact JSON strings instead of native Arrow nested types. | `false` |
| `intervals_as_string` | `intervals_as_string` | `intervalsAsString` | Render `INTERVAL` / `DURATION` columns as canonical Databricks text (e.g. `"Y-M"`, `"D HH:MM:SS.nnnnnnnnn"`) instead of native Arrow interval types. Needed for pyarrow consumers, which can't decode Arrow `month_interval`. | `false` |

**Notes**

- The CloudFetch tuning knobs are Rust-core only today; the bindings
  expose the two result-rendering toggles (`complex_types_as_json`,
  `intervals_as_string`) but inherit the CloudFetch defaults.

---

## Logging

Optional. Rust-core only — bindings install their own log bridges (the
Node.js binding via `initKernelLogging`, the Python binding via its
`logging` module).

| Rust (`LoggingConfig`) | Description | Default |
|---|---|---|
| `filter` | Log filter, e.g. `databricks_sql_kernel=debug`. When `None`, the kernel honours the `RUST_LOG` environment variable. | `None` (uses `RUST_LOG`) |
| `file` | File to write logs to. When `None`, logs go to stderr. | `None` (stderr) |

---

## Server-side session configuration

The `session_conf` (Rust / Python) / `sessionConf` (Node.js) map carries
**server-bound** SQL / Spark configuration, forwarded to SEA
`session_confs` and applied for every statement on the session.

| Rust / Python | Node.js | Description | Default |
|---|---|---|---|
| `session_conf` | `sessionConf` | Map of server-side session-config key → value. | `{}` (empty) |

Keys are matched **case-insensitively** against a kernel allowlist and
**uppercased** before being sent (the server echoes them uppercase, so
`SET`-readback matches). Keys not in the allowlist are **dropped with a
warning**.

### Allowed keys

Sourced from the [Databricks SQL configuration-parameters
reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters)
plus the additional keys the Java driver accepts. See that page for the
per-parameter meaning and accepted values.

| Key | Notes |
|---|---|
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

### Case-sensitive Spark confs

Dotted `spark.*` confs are matched case-insensitively but sent
**verbatim** (Spark conf keys are case-sensitive and must not be
uppercased):

| Key | Notes |
|---|---|
| `spark.sql.thriftserver.metadata.metricview.enabled` | When `"true"`, `SHOW TABLES` returns `METRIC_VIEW` rows and `getColumns` surfaces `measure` columns. Mirrors JDBC `EnableMetricViewMetadata` (default off). |

**Notes**

- Boolean-valued keys should use the exact strings `"true"` / `"false"`;
  other forms (`"True"`, `"yes"`, `"1"`) are undefined server-side. The
  kernel does not pre-validate values — it trusts the supplied
  `session_conf` and forwards values as-is.

---

## Transport

Optional. Wire-protocol selection. Rust-core only.

| Rust (`TransportMode`) | Description | Default |
|---|---|---|
| `transport` | Wire protocol. v0 supports `SeaRest` (Statement Execution API over REST) only; the enum is reserved to grow `SeaGrpc` / `Thrift` in v1+. | `SeaRest` |

---

## Per-statement options

The parameters above are **session-level**. A few knobs are set
per-statement rather than per-session (via `StatementSpec` in Rust and
`ExecuteOptions` in the Node.js binding):

- `statement_conf` / `statementConf` — per-statement Spark-conf overlay,
  merged on top of the session `session_conf` (statement wins on key
  collisions).
- `query_tags` / `queryTags` — convenience over `statement_conf`'s
  `query_tags` key.
- `row_limit` / `rowLimit` — server-side cap on rows returned (SEA
  `row_limit`), independent of any SQL `LIMIT`.
- Bound parameters — positional (`?`) and named (`:name`) query
  parameters.

See `docs/spec-v0.md` and the `StatementSpec` docs for the full
per-statement surface.
