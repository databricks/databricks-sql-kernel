# Connection parameter reference

A session is configured with a `SessionConfig`, assembled from typed
sub-structs (one per concern) and passed to `Session::open`. Every
parameter, its meaning, and its default value is listed below, grouped
by the struct it lives on. `ConnectionConfig` and `AuthConfig` are
**required**; the rest are optional and fall back to the defaults shown.

| Group | Covers |
|-------|--------|
| [`ConnectionConfig`](#connectionconfig--connection-identity-required) | host, warehouse, org id *(required)* |
| [`AuthConfig`](#authconfig--authentication-required) | PAT / OAuth M2M / OAuth U2M *(required)* |
| [`DefaultOpts`](#defaultopts--session-defaults-optional) | default catalog / schema, transactions |
| [`HttpConfig`](#httpconfig--http-client-optional) | timeouts, connection pool, retries, headers |
| [`ProxyConfig`](#proxyconfig--httphttps-proxy-optional) | HTTP/HTTPS proxy |
| [`TlsConfig`](#tlsconfig--tls--ssl-optional) | cert validation, custom CA, mTLS |
| [`ResultConfig`](#resultconfig--result-handling-optional) | CloudFetch tuning, type rendering |
| [`LoggingConfig`](#loggingconfig--logging-optional) | log filter / file |
| [`transport`](#transport--wire-protocol-optional) | wire protocol |
| [`session_conf`](#session_conf--server-side-session-configuration-optional) | server-side SQL / Spark config |

## `ConnectionConfig` — connection identity (required)

Construct via `ConnectionConfig::new(host, warehouse_id)` or
`ConnectionConfig::from_http_path(host, http_path)` (which parses the
warehouse id and org id out of a `/sql/1.0/warehouses/{id}` path).

| Field | Description | Default |
|-------|-------------|---------|
| `host` | Workspace URL, e.g. `https://my-workspace.databricks.com`. Bare hostnames get `https://` prepended and any trailing `/` stripped. | **(required)** |
| `warehouse_id` | Warehouse id, e.g. `abc123`. Accepts `/sql/1.0/warehouses/{id}` and legacy `/sql/1.0/endpoints/{id}` forms via `from_http_path`. | **(required)** |
| `org_id` | Workspace organization id for multi-tenant routing; sent as the `x-databricks-org-id` header. Auto-populated from the `?o=` query parameter of the HTTP path. | `None` |

## `AuthConfig` — authentication (required)

An enum; pick one variant. Use the validating constructors
(`AuthConfig::pat`, `::oauth_m2m`, `::oauth_u2m`, `::azure_service_principal`).

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
| `client_id` | OAuth client id. | `"databricks-sql-connector"` |
| `redirect_ports` | Ordered candidate localhost ports for the OAuth callback listener; the first free port is bound (busy-port fallback). `None`/empty selects the default window. The default app (`databricks-sql-connector`) accepts any `http://localhost:{port}` because its in-house OAuth normalizes loopback redirect URIs by dropping the port (RFC 8252 §7.3); for other apps every port must be a registered redirect URI. | `[8030, 8031, 8032, 8033, 8034]` |
| `scopes` | OAuth scopes. `offline_access` is required to receive a refresh token (cached and refreshed transparently). | `["sql", "offline_access"]` |
| `token_url_override` | Token endpoint override. | OIDC discovery |
| `callback_timeout` | How long to wait for the user to complete the browser flow. | `120s` |

**Azure Entra ID (Microsoft Entra) OAuth.** The kernel is the Azure-aware auth
core — it resolves the Azure endpoints / scope / client id from the host, so
bindings just select the flow.

- **Azure U2M (`AzureOAuthU2M`, `azure-oauth`): planned — not yet in v0.**
  The `AzureOAuthU2M` variant / `AuthConfig::azure_oauth_u2m(...)` constructor and
  the pyo3 `auth_type="azure-oauth"` selector are **not** shipped in this release;
  the design (PECOBLR-4120) is tracked in
  `docs/designs/PECOBLR-4141-azure-service-principal-design.md`. When it lands the
  kernel will pin the workspace **v2.0** endpoints
  (`{host}/oidc/oauth2/v2.0/{authorize,token}`, which the workspace redirects to
  Entra), the Azure app client id (`96eecda7-…`), port `8030`, and the
  `{app_id}/user_impersonation offline_access` scope, with no tenant needed (the
  workspace redirect resolves it). Until then, use `azure-sp-m2m` for Azure
  authentication.
- **Azure service principal M2M (`AzureServicePrincipal`, `azure-sp-m2m`):
  supported.** The kernel owns Azure resolution: it builds the Entra token
  endpoint (`{login_host}/{tenant}/oauth2/v2.0/token`) and the
  `{app_id}/.default` data scope, and — when `azure_tenant_id` is omitted —
  auto-discovers the tenant from the workspace's `/aad/auth` redirect (matching
  the Thrift connector), so bindings forward only the credentials. Fields:
  `azure_client_id` / `azure_client_secret` (Entra app credentials, required),
  `azure_tenant_id` (optional — auto-discovered when omitted),
  `azure_workspace_resource_id` (optional — enables the management-token add-on,
  see below), `identity_federation_client_id` (optional WIF). Construct via the
  `AuthConfig::azure_service_principal(...)` validating constructor. See
  `docs/designs/PECOBLR-4141-azure-service-principal-design.md`.

  The generic `OAuthM2M` path (Entra `token_url_override` +
  `2ff814a6-…/.default` scope) still works and requires an explicit tenant;
  `azure-sp-m2m` is the preferred, zero-config-tenant flow.

The `azure-sp-m2m` flow authenticates a workspace-member SP with the Databricks
data token alone. The Azure **management-token** add-on — sending
`X-Databricks-Azure-SP-Management-Token` + `X-Databricks-Azure-Workspace-Resource-Id`
so the control plane can authorize an SP that holds an Azure RBAC role but is not
a workspace member — is emitted **only when `azure_workspace_resource_id` is set**,
matching the JDBC connector (`if (resourceId != null)`). When it is unset (the
default) neither header is sent and no ARM management-scope token is fetched, so
the common workspace-member SP is unaffected.

The bindings expose the generic OAuth overrides as: pyo3 `token_url` /
`oauth_scopes` / `client_id`; napi `oauthTokenUrl` / `oauthScopes` /
`oauthClientId`; C ABI `kernel_session_config_set_oauth_token_endpoint` /
`kernel_session_config_set_oauth_scopes` (+ the `set_auth_*` setters). The pyo3
binding additionally exposes `azure-sp-m2m` via `auth_type="azure-sp-m2m"` with
the `azure_client_id` / `azure_client_secret` / `azure_tenant_id` (optional)
kwargs.

Two further variants are reserved but not usable in v0:
`OAuthFederation` (RFC 8693 token exchange — returns `InvalidArgument`
until implemented) and `External` (a caller-supplied `TokenProvider`).

## `DefaultOpts` — session defaults (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `catalog` | Default catalog for statements on this session. | `None` (server default) |
| `schema` | Default schema for statements on this session. | `None` (server default) |
| `ignore_transactions` | When `true`, transaction-control calls (`set_autocommit(false)`, `commit()`, `rollback()`) are silently no-oped. Mirrors JDBC `IgnoreTransactions=1`. Set `false` to opt into real multi-statement transactions (Private Preview; requires catalog-managed tables). | `true` |

## `HttpConfig` — HTTP client (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `request_timeout` | Per-request timeout (connect + body). | `120s` |
| `overall_timeout` | Maximum total time across retries for one logical RPC. | `900s` |
| `pool_max_idle_per_host` | Connection-pool size (max idle warm sockets per host). Raise for bursty concurrent workloads. | `100` |
| `retry_min_wait` | Minimum backoff between retries; also the floor for a server `Retry-After` and the base of exponential backoff (`min_wait × 2^attempt`). | `1s` |
| `retry_max_wait` | Maximum backoff between retries; caps both the exponential backoff and a server `Retry-After`. | `60s` |
| `retry_max_retries` | Max retries **after** the first attempt — a request is tried at most `retry_max_retries + 1` times. | `5` (→ 6 total attempts) |
| `custom_headers` | Extra headers on every request; an ordered `Vec<(String, String)>` (duplicates allowed). `Authorization` / `x-databricks-org-id` are silently dropped (reserved); a `User-Agent` is folded into the effective UA. | `[]` |

## `ProxyConfig` — HTTP/HTTPS proxy (optional)

When `url` is `None`, the kernel does not configure a proxy and
`reqwest`'s standard `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` env-var
behaviour applies. Setting `url` **overrides** those env vars.

| Field | Description | Default |
|-------|-------------|---------|
| `url` | Proxy endpoint, e.g. `http://proxy.corp.example.com:8080`. Must be `http://` or `https://`. | `None` (env-var behaviour) |
| `username` | Proxy basic-auth username. Overrides credentials embedded in `url`. | `None` |
| `password` | Proxy basic-auth password. Ignored unless `username` is set. Redacted in `Debug`. | `None` |
| `bypass_hosts` | Comma-separated hosts/domains to bypass the proxy, e.g. `localhost,*.internal.corp,.example.com`. | `None` |

## `TlsConfig` — TLS / SSL (optional)

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

## `ResultConfig` — result handling (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `cloudfetch_enabled` | Opt into CloudFetch (cloud-storage-backed download of large results via presigned URLs). Set `false` to force the inline-Arrow path (mirrors JDBC `EnableQueryResultDownload=0`). | `true` |
| `cloudfetch_link_prefetch_window` | Number of presigned URLs to prefetch ahead of the consumer. | `10` |
| `cloudfetch_max_chunks_in_memory` | Max decompressed CloudFetch chunks held in memory at once (bounds download parallelism and peak RSS). | `16` |
| `inline_max_chunks_in_memory` | Prefetch window for the inline-Arrow path (used when CloudFetch is disabled). `1` disables prefetching. | `4` |
| `complex_types_as_json` | Render complex columns (`ARRAY` / `MAP` / `STRUCT` / `VARIANT` / `GEOMETRY` / `GEOGRAPHY`) as compact JSON strings instead of native Arrow nested types. | `false` |
| `intervals_as_string` | Render `INTERVAL` / `DURATION` columns as canonical Databricks text (e.g. `"Y-M"`, `"D HH:MM:SS.nnnnnnnnn"`) instead of native Arrow interval types. | `false` |

## `LoggingConfig` — logging (optional)

| Field | Description | Default |
|-------|-------------|---------|
| `filter` | Log filter, e.g. `databricks_sql_kernel=debug`. When `None`, the kernel honours the `RUST_LOG` env var. | `None` (uses `RUST_LOG`) |
| `file` | File to write logs to. When `None`, logs go to stderr. | `None` (stderr) |

## `transport` — wire protocol (optional)

`Option<TransportMode>`. v0 supports `SeaRest` (Statement Execution API
over REST) only; the enum is reserved to grow `SeaGrpc` / `Thrift` in
v1+. `None` lets the kernel pick the default.

Default: `SeaRest`.

## `session_conf` — server-side session configuration (optional)

`session_conf: HashMap<String, String>` carries **server-bound** SQL /
Spark configuration, forwarded to SEA `session_confs` and applied for
every statement on the session. The kernel does not validate,
normalize, uppercase, or drop server-bound keys; caller-supplied entries
are forwarded unchanged. Server-side validation errors, if any, come
from Databricks.

See the [Databricks SQL configuration-parameters
reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters)
for supported SQL parameter names and accepted values.

Boolean-valued keys should use the exact strings `"true"` / `"false"`;
the kernel does not pre-validate values and forwards them as-is.

### Client-only `session_conf` keys

Some keys carried in the same `session_conf` map are **client-side**
tuning knobs: the kernel reads them at session creation, applies them to
the typed config above, and then strips them so they never reach the SEA
wire. They exist so C ABI (Go / ODBC) consumers can set them through the
existing `session_conf` entry point without a dedicated symbol.

Each key mirrors a field of [`ResultConfig`](#resultconfig--result-handling-optional) and is matched **case-insensitively**. Boolean keys accept exactly `"true"` / `"false"` (case-insensitive); the integer window keys require a positive integer. When supplied through `session_conf`, an invalid value is ignored with a warning and the key is still stripped from the wire. The typed client-conf setters reject invalid values with `InvalidArgument`.

| Key | Description | Default |
|-----|-------------|---------|
| `cloudfetch_enabled` | Same knob as `ResultConfig::cloudfetch_enabled` — opt into CloudFetch (`false` forces the inline-Arrow path). | `true` |
| `cloudfetch_link_prefetch_window` | Same knob as `ResultConfig::cloudfetch_link_prefetch_window` — number of presigned URLs to prefetch ahead of the consumer. | `10` |
| `cloudfetch_max_chunks_in_memory` | Same knob as `ResultConfig::cloudfetch_max_chunks_in_memory` — max decompressed CloudFetch chunks held in memory at once. Values above `256` are clamped (with a warning) rather than rejected (peak RSS scales with this, so the ceiling bounds OOM risk). | `16` |
| `inline_max_chunks_in_memory` | Same knob as `ResultConfig::inline_max_chunks_in_memory` — prefetch window for the inline-Arrow path. `1` disables prefetching. | `4` |
| `complex_types_as_json` | Same knob as `ResultConfig::complex_types_as_json` — render complex columns as compact JSON strings instead of native Arrow nested types. | `false` |
| `intervals_as_string` | Same knob as `ResultConfig::intervals_as_string` — render `INTERVAL` / `DURATION` columns as canonical Databricks text instead of native Arrow interval types. | `false` |

Compression (LZ4) is **not** a parameter: the kernel detects it from the
server's result manifest (`result_compression`) and decompresses
`LZ4_FRAME` payloads automatically.
