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

# Kernel SEA feature matrix — kernel-side NOT-IMPLEMENTED features blocking Thrift↔SEA parity

**Generated:** 2026-05-28
**Audit source-of-truth:** `databricks-sql-nodejs/sea-workflow/audits/2026-05-28-cross-driver-audit.md`
**Scope:** kernel-side gaps that block the autonomous Thrift↔SEA parity drive (`useSEA: true`
in the NodeJS connector and `use_kernel=True` in the Python connector both depend on
these kernel surfaces). NodeJS-side and driver-test-side gaps are tracked separately —
this file only documents work the kernel must do.

This document covers rows that the cross-driver audit classified as **DOCUMENT-ONLY**
(per `databricks-sql-nodejs/sea-workflow/decisions/2026-05-28-autonomous-parity-decisions.md` D1):
kernel does not implement the feature today, so no NodeJS / driver-test wiring is possible
yet. Each row pins the contract that NodeJS Thrift and Python `use_sea=True` already
offer, so the kernel team has a precise target to match.

A second related file, [`jira-candidates-index.md`](./jira-candidates-index.md), tracks the
kernel-side bug candidates surfaced by the same audit (as opposed to missing-feature work).

---

## Legend

- **NodeJS Thrift behavior** — what the user-facing `databricks/databricks-sql-nodejs` API
  exposes today on the Thrift path. This is the parity target.
- **Current SEA behavior** — what the same API does (or refuses to do) on `useSEA: true`,
  i.e. via the kernel. "Not surfaced" = the option is silently ignored or rejected;
  "Throws" = the call fails with a clear error.
- **Kernel surface needed** — the smallest kernel-public-API change required before any
  driver work can wire the feature.
- **Recommended path forward** — sequencing across kernel → napi → NodeJS adapter →
  driver-test parity test.
- **JIRA-candidate** — filename under `databricks-sql-nodejs/sea-workflow/jira-candidates/`,
  if a candidate has been filed. Some rows do not yet have one because they are
  missing-feature work rather than a bug.

---

## Auth (Section 1)

### 1.6 — Azure tenant SP (multi-tenant OAuth)

| Field | Value |
|---|---|
| **Feature name** | OAuth with `azureTenantId` for Azure AD multi-tenant service principals |
| **Scope** | Connection-time auth. User passes `azureTenantId` alongside an OAuth client-id/secret so the kernel can mint tokens against an Azure AD tenant other than the workspace's home tenant. |
| **NodeJS Thrift behavior** | Y — `authType: 'databricks-oauth'` with `azureTenantId` set in `IDBSQLClient.connect` options. Tested by `databricks-sql-nodejs/tests/.../auth/AzureOAuthTests.test.ts`. |
| **Current SEA behavior** | NOT-IMPLEMENTED. The kernel's OAuth M2M / U2M code paths (generic OIDC) do not understand a tenant override. PR #383 on NodeJS SEA covers generic M2M / U2M only — not Azure tenant SP. |
| **Kernel surface needed** | Azure-AD-specific credential plumbing in `auth/` — at minimum an `azure_tenant_id` field on the relevant `AuthConfig` discriminant, with the matching URL templates wired into the OAuth flow. |
| **Recommended path forward** | 1) Kernel PR adds Azure-tenant credential plumbing in `auth/` and surfaces `azure_tenant_id` on `AuthConfig`. 2) napi binding exposes the field on `ConnectionOptions`. 3) NodeJS SEA adapter maps `azureTenantId` → napi field. 4) Driver-test re-enables the SEA leg of `AzureOAuthTests`. |
| **JIRA-candidate** | _none yet_ (missing-feature work; file once roadmap-accepted) |

### 1.7 — Custom auth provider

| Field | Value |
|---|---|
| **Feature name** | `authType: 'custom'` with user-supplied `IAuthentication` implementation |
| **Scope** | The Thrift path allows callers to provide their own `IAuthentication` implementation that signs each request. |
| **NodeJS Thrift behavior** | Y. |
| **Current SEA behavior** | NOT-IMPLEMENTED. |
| **Kernel surface needed** | None planned. |
| **Recommended path forward** | Document as **Thrift-only extensibility** — by-design SEA gap. The SEA backend authenticates via the kernel, which owns its own credential flow and HTTP transport, so a "callback per request" extension point does not fit the layered architecture. NodeJS Thrift continues to expose it for users who need the escape hatch. |
| **JIRA-candidate** | n/a (by-design gap) |

### 1.8 — Token-provider auth (refresh callback)

| Field | Value |
|---|---|
| **Feature name** | `authType: 'token-provider'` with an `ITokenProvider` exposing a refresh callback |
| **Scope** | Caller provides a function that returns a fresh bearer token on demand; the driver calls it whenever a request must be authorized or the previous token is near expiry. This is the modern federation pattern. |
| **NodeJS Thrift behavior** | Y. |
| **Current SEA behavior** | NOT-IMPLEMENTED. |
| **Kernel surface needed** | An external-token-provider hook on `Session` — a Rust trait (or function pointer) the kernel calls each time it needs a bearer token, in place of the kernel's own refresh logic. The hook must support async (token may itself come from an HTTP call). |
| **Recommended path forward** | 1) Kernel PR adds an `ExternalTokenProvider` trait or callback-shaped variant of `AuthConfig`. 2) napi exposes it as a `(this) => Promise<string>` JS callback. 3) NodeJS adapter maps `ITokenProvider` → napi callback. Bundle 1.8–1.11 into one kernel PR because they share the same hook. |
| **JIRA-candidate** | _none yet_ |

### 1.9 — External-token auth (federation)

| Field | Value |
|---|---|
| **Feature name** | `authType: 'external-token'` with a `getToken: TokenCallback` (federation flow) |
| **Scope** | Same shape as 1.8 — caller-supplied callback returns a bearer token. Used for token-federation scenarios where the token is minted by an external IdP. |
| **NodeJS Thrift behavior** | Y. |
| **Current SEA behavior** | NOT-IMPLEMENTED. |
| **Kernel surface needed** | Same hook as 1.8. |
| **Recommended path forward** | Bundle with 1.8. |
| **JIRA-candidate** | _none yet_ |

### 1.10 — Static-token auth (federation-compatible)

| Field | Value |
|---|---|
| **Feature name** | `authType: 'static-token'` with `staticToken` (federation-compatible) |
| **Scope** | Caller supplies a pre-minted token that is reused for the lifetime of the session. Federation-shaped variant of PAT, distinguished so the driver does not attempt refresh. |
| **NodeJS Thrift behavior** | Y. |
| **Current SEA behavior** | NOT-IMPLEMENTED. |
| **Kernel surface needed** | Same hook family as 1.8, with the refresh callback returning the same value each time (or a dedicated `StaticToken` variant on `AuthConfig`). |
| **Recommended path forward** | Bundle with 1.8 (single kernel PR for 1.8–1.11). |
| **JIRA-candidate** | _none yet_ |

### 1.11 — Federation client ID

| Field | Value |
|---|---|
| **Feature name** | `enableTokenFederation` + `federationClientId` on token-provider / external-token / static-token |
| **Scope** | When federation is in use, the driver must send a federation client identifier so the workspace can validate the inbound token. Bound to the auth variants in 1.8/1.9/1.10. |
| **NodeJS Thrift behavior** | Y. |
| **Current SEA behavior** | NOT-IMPLEMENTED. |
| **Kernel surface needed** | A `federation_client_id: Option<String>` field on the federation-shaped `AuthConfig` variants, threaded into the outgoing HTTP request headers / token-exchange payload as the workspace expects. |
| **Recommended path forward** | Bundle with 1.8 — the whole federation surface is one kernel PR. |
| **JIRA-candidate** | _none yet_ |

---

## Transport (Section 1, continued)

### 1.12 — HTTP / SOCKS proxy

| Field | Value |
|---|---|
| **Feature name** | HTTP-CONNECT and SOCKS5 proxy support with optional basic auth |
| **Scope** | `ProxyOptions { protocol, host, port, auth }` on the connection. The driver routes all requests through the proxy, including CloudFetch chunk fetches. |
| **NodeJS Thrift behavior** | Y — covered by `HttpProxyTests`, `ProxyAgentTests`, `SocksProxyTests` in the Thrift test suite. |
| **Current SEA behavior** | NOT-IMPLEMENTED. The kernel today defines a `ProxyConfig {}` struct that is empty — there is no proxy-routing logic in `client/`. |
| **Kernel surface needed** | A populated `ProxyConfig` (host, port, scheme, optional credentials) and the matching `reqwest::Proxy` wiring in `client/`. Must cover (a) the SEA control-plane requests and (b) CloudFetch chunk fetches (which may go to a different host). SOCKS5 is a stretch goal — HTTP-CONNECT is the priority. |
| **Recommended path forward** | 1) Kernel PR populates `ProxyConfig` and wires `reqwest::Client::builder().proxy(...)` into the SEA + CloudFetch transports. 2) napi exposes the fields on `ConnectionOptions`. 3) NodeJS SEA adapter maps the existing `proxy: { … }` option onto the napi fields. 4) Driver-test re-enables the SEA legs of `HttpProxyTests` / `ProxyAgentTests`. |
| **JIRA-candidate** | _none yet_ (kernel-feature gap; design lives at `docs/designs/proxy-support-design.md`) |

### 1.13 — TLS custom CA + mTLS

| Field | Value |
|---|---|
| **Feature name** | Custom CA bundle (`node_extra_ca_certs`-equivalent) and mutual TLS (client cert + key) |
| **Scope** | Drivers in regulated environments need to (a) trust a private CA when terminating TLS at an enterprise gateway, and (b) present a client cert (mTLS) on outbound calls. |
| **NodeJS Thrift behavior** | Y — `TlsTests`, `TlsEnforcementTests`, `MtlsTests` exercise the full surface against the Node TLS stack. |
| **Current SEA behavior** | NOT-IMPLEMENTED on `main`. **Kernel PR [#23](https://github.com/databricks/databricks-sql-kernel/pull/23) is open** and exposes the `TlsConfig` public-API fields needed to plumb custom-CA + mTLS into the kernel's `reqwest` client. |
| **Kernel surface needed** | The fields PR #23 adds to `TlsConfig` — custom CA bytes/path, client cert bytes/path, client key bytes/path. Once #23 merges, the kernel surface is complete for this row. |
| **Recommended path forward** | 1) Land kernel PR #23. 2) napi binding exposes the new `TlsConfig` fields. 3) NodeJS SEA adapter plumbs `node_extra_ca_certs` + mTLS options through napi. 4) Driver-test flips the SEA legs of `TlsTests` / `MtlsTests` from `it.skip` to active. Design notes are in `docs/designs/tls-config-design.md`. |
| **JIRA-candidate** | n/a (tracked by PR #23) |

### 1.14 — Custom HTTP headers

| Field | Value |
|---|---|
| **Feature name** | Caller-supplied arbitrary HTTP request headers (`userAgentEntry` + free-form headers) |
| **Scope** | Enterprise users need to inject correlation IDs, custom user-agent components, or workspace-specific routing headers on every outbound request. |
| **NodeJS Thrift behavior** | Y — Thrift supports both `userAgentEntry` and an arbitrary `headers` map. Covered by `CustomHeadersTests` on the Thrift side. |
| **Current SEA behavior** | NOT-IMPLEMENTED. The kernel has no per-session caller-headers map. |
| **Kernel surface needed** | A `custom_headers: HashMap<String, String>` (or equivalent typed surface) on the connection-level config, threaded into the `reqwest::Client` default-headers or per-request header builder for both SEA + CloudFetch transports. Reject reserved names (`Authorization`, `Content-Type`, …). |
| **Recommended path forward** | 1) Kernel PR adds a `custom_headers` field on the connection-time config, with reserved-name rejection. 2) napi exposes it. 3) NodeJS SEA adapter maps the existing user-facing options. 4) Driver-test enables the SEA leg of `CustomHeadersTests`. |
| **JIRA-candidate** | _none yet_ |

---

## Statement / Execution (Section 2)

### 2.15 — `row_limit` on `StatementSpec`

| Field | Value |
|---|---|
| **Feature name** | Public per-statement row cap (`row_limit=` in Python `use_sea`; equivalent JDBC `MaxRows`) |
| **Scope** | Server-side cap on the number of rows returned. Distinct from `LIMIT N` in SQL (which is a query rewrite) and from `max_rows` per fetch (which is client-driven pagination — see audit note 6 / row 2.16). |
| **NodeJS Thrift behavior** | NOT-IMPLEMENTED (matrix correction). Thrift's `ExecuteStatementOptions` has no `rowLimit` field; the only way to cap rows on the Thrift path is `LIMIT N` in the SQL. Python `use_sea=True` is the canonical surface that exposes `row_limit`. |
| **Current SEA behavior** | NOT-IMPLEMENTED in the public kernel API. The kernel **already carries `row_limit`** on the SEA wire request (`src/types/sea.rs::ExecuteStatementRequest.row_limit`) and on the internal `ExecuteParams`, but the public `StatementSpec` does not surface it — the field is populated only inside `prepare_for_dispatch` with no spec-fed input. |
| **Kernel surface needed** | Add `row_limit: Option<u64>` to the public `StatementSpec` and thread it through to the SEA execute payload that the kernel already constructs. This is a small surfacing change rather than new functionality. |
| **Recommended path forward** | 1) Kernel PR adds `StatementSpec.row_limit` and threads it. 2) napi exposes it on `ExecuteOptions`. 3) NodeJS adapters (both SEA and Thrift) expose it on the public `ExecuteStatementOptions`. 4) Driver-test adds a parity test that caps row count via the new option and verifies the cap is honored. |
| **JIRA-candidate** | _none yet_ |

### 2.17 — `queryTimeout` / `wait_timeout` enforcement

| Field | Value |
|---|---|
| **Feature name** | Server-enforced statement timeout in seconds |
| **Scope** | Caller-specified upper bound on how long the server will wait for a statement to complete before failing with a `QUERY_TIMEOUT` error. JDBC standard `Statement.setQueryTimeout`. |
| **NodeJS Thrift behavior** | Y in name only — `ExecuteStatementOptions.queryTimeout` exists, but the jira-candidate `2026-05-26-thrift-querytimeout-not-honoured.md` documents that the value is silently ignored on the wire. |
| **Current SEA behavior** | NOT-IMPLEMENTED. The kernel today exposes only `overall_timeout` on the HTTP client (transport-level) — not a per-statement server-side wait timeout. SEA explicitly rejects the option per the parity matrix. |
| **Kernel surface needed** | A new `wait_timeout` / `query_timeout` conf knob on `StatementSpec`, distinct from the transport `overall_timeout`. The kernel must serialize it onto the SEA execute payload (the SEA endpoint already accepts `wait_timeout` at the protocol level — see Python `use_sea=True`'s `wait_timeout` kwarg). |
| **Recommended path forward** | 1) Kernel PR adds `StatementSpec.wait_timeout`, threads to SEA payload. 2) napi exposes it. 3) NodeJS SEA adapter maps `queryTimeout` → napi field; NodeJS Thrift fixes the silent-ignore bug. 4) Driver-test pins the divergence: SEA enforces, Thrift surfaces a clear error or honours after the fix. |
| **JIRA-candidate** | NodeJS-side companion: `2026-05-26-thrift-querytimeout-not-honoured.md`. Kernel-side missing-feature row: _none yet_. |

---

## Error / Retry (Section 5)

### 5.5 / 5.6 — Retry policy options + exponential backoff

| Field | Value |
|---|---|
| **Feature name** | Configurable retry policy: status codes that retry (5xx / 429 / Retry-After), max attempts, backoff base, jitter, max-elapsed-time |
| **Scope** | Production drivers need configurable retry for transient 5xx + 429 responses with exponential backoff and `Retry-After` honoring. Thrift exposes this via `RetryStatusCodesTests` / `RetryAfterHeaderTests` / `RetryPolicyOptionsTests`. |
| **NodeJS Thrift behavior** | Y — full configurable retry policy. |
| **Current SEA behavior** | NOT-IMPLEMENTED. The kernel currently exposes only `overall_timeout` as a retry-related knob; there is no policy surface for status-code allowlists, base backoff, jitter, or max-attempts. |
| **Kernel surface needed** | A `retry_policy_options` struct in `config/`, threaded into the `client/` retry middleware (currently uses fixed defaults). Knobs to expose at minimum: retried status codes, max attempts, base backoff in ms, jitter factor, max elapsed time, honor `Retry-After`. |
| **Recommended path forward** | 1) Kernel PR adds `RetryPolicyOptions` and wires it through `client/`. Design notes already exist at `docs/designs/PECOBLR-2091-retry-logic-design.md`. 2) napi exposes the struct. 3) NodeJS SEA adapter maps the existing user-facing retry options. 4) Driver-test re-enables the SEA leg of `RetryStatusCodesTests` / `RetryAfterHeaderTests`. |
| **JIRA-candidate** | _none yet_ |

---

## Staging (Section 5, continued)

### 5.9 — Staging volume API (PUT / GET)

| Field | Value |
|---|---|
| **Feature name** | `stagingAllowedLocalPath` — local-file ↔ Databricks volume streaming through the driver |
| **Scope** | The Thrift driver exposes the staging-volume API where the server returns presigned URLs and the client uploads/downloads to a local file path; the driver enforces a configured allowlist of local paths. |
| **NodeJS Thrift behavior** | Y — `stagingAllowedLocalPath` option + staging test suite. |
| **Current SEA behavior** | NOT-IMPLEMENTED. The kernel has no Volume API. Python `use_kernel=True` has the same gap. |
| **Kernel surface needed** | A new staging/volume module that (a) recognises the server's `INTERNAL_STAGE_OP` response shape, (b) issues the presigned PUT/GET, (c) enforces the path allowlist, and (d) exposes a typed Rust surface. This is a substantial new module — roughly the size of the existing `reader/` (CloudFetch) module. |
| **Recommended path forward** | Treat as a sizeable kernel sub-project. 1) Kernel design doc + module under `staging/`. 2) Kernel PR implementing the surface + tests. 3) napi exposes the new types. 4) NodeJS SEA adapter + driver-test re-enable. Defer until the parity drive's main clusters are landed. |
| **JIRA-candidate** | _none yet_ |

---

## Result-fetching fidelity (Section 3)

### 3.10 — Empty-result manifest type fidelity (`INTERVAL_*` / `TIMESTAMP_NTZ` / `GEOMETRY` / `GEOGRAPHY`)

| Field | Value |
|---|---|
| **Feature name** | Preserving column type information for zero-row SEA results (manifest-driven schema decode) |
| **Scope** | When a SEA statement returns zero rows (e.g. `SELECT CAST(NULL AS INTERVAL DAY TO SECOND) WHERE 1=0`), the kernel must reconstruct the column types from the SEA `manifest.schema` rather than from the empty Arrow IPC stream. Today, for 5 types — `INTERVAL_YEAR_MONTH`, `INTERVAL_DAY_TIME`, `GEOMETRY`, `GEOGRAPHY`, `TIMESTAMP_NTZ` — the kernel degrades them or omits the type info. |
| **NodeJS Thrift behavior** | Y — full fidelity preserved (Thrift wire shape carries column-type info on every response). |
| **Current SEA behavior** | DEGRADED on the kernel manifest-decode path. Python `use_sea=True` (which bypasses the kernel manifest decoder) preserves all 5 types — confirming this is a **kernel-side bug**, not a SEA-protocol limitation. NodeJS SEA inherits the kernel's degradation. |
| **Kernel surface needed** | Manifest-decoder fix in `result_stream.rs` / `sea/manifest.rs`: explicit type-code branches for the 5 affected types, with field-level Arrow metadata populated from the manifest when the IPC stream is empty. |
| **Recommended path forward** | Treat as a **kernel bug**, not a feature gap. See `jira-candidates-index.md` for the candidate. The NodeJS adapter does not need to change once the kernel fix lands. |
| **JIRA-candidate** | [`2026-05-29-kernel-empty-result-manifest-fidelity.md`](../../databricks-sql-nodejs/sea-workflow/jira-candidates/2026-05-29-kernel-empty-result-manifest-fidelity.md) (filed 2026-05-29 by the C4 driver-test author; this PR adds it to the kernel-side index). |

---

## Connection-time `QUERY_TAGS` (Section 2, kernel-side bug)

### 2.13 / 3.9 — Connection-time `QUERY_TAGS` silently dropped on the kernel session-conf forwarding path

| Field | Value |
|---|---|
| **Feature name** | `query_tags={...}` (Python) / `sessionConfig: { QUERY_TAGS: '...' }` (NodeJS SEA) honoured at session open and visible via `SET QUERY_TAGS` / `current_query_tags()` |
| **Scope** | Customers attribute SQL workloads to teams/apps via `system.query.history.query_tags`. Connection-time tags are the natural way to set "this session is the data-platform daily job" once at open time rather than prepending `SET QUERY_TAGS` to every statement. |
| **NodeJS Thrift behavior** | Y on warehouses that honour `QUERY_TAGS` (separate Thrift `OpenSessionRequest.queryTags` field on the session-open RPC). |
| **Current SEA behavior** | DROPPED. The kernel's `session_conf` forwarding map either filters out `QUERY_TAGS` or the SEA endpoint receives a `CreateSession` payload that does not propagate the value into the underlying Spark session-conf state. Confirmed across Python `use_kernel=True` AND NodeJS SEA (both go through the same kernel session path). |
| **Kernel surface needed** | None — the surface (a `session_conf` map) already exists. This is a **bug fix** in the kernel's session-conf forwarding, specifically the `QUERY_TAGS` key handling in `session.rs` / SEA session-open payload construction. |
| **Recommended path forward** | Treat as a **kernel bug**, not a feature gap. See `jira-candidates-index.md` for the candidate. The NodeJS / Python adapters do not need to change once the kernel fix lands. |
| **JIRA-candidate** | [`2026-05-29-kernel-connection-time-querytags-drop.md`](../../databricks-sql-nodejs/sea-workflow/jira-candidates/2026-05-29-kernel-connection-time-querytags-drop.md) (filed 2026-05-29 by the C1 driver-test author; this PR adds it to the kernel-side index). |

---

## Summary

| # | Audit row | Feature | Class | Kernel status | Path forward |
|---|---|---|---|---|---|
| 1 | 1.6 | Azure tenant SP | Auth (missing-feature) | NOT-IMPLEMENTED | New kernel PR (Azure-tenant credential plumbing). |
| 2 | 1.7 | Custom auth provider | Auth (by-design SEA gap) | n/a | Document as Thrift-only extensibility — no kernel work. |
| 3 | 1.8 | Token-provider auth | Auth (missing-feature) | NOT-IMPLEMENTED | New kernel PR (external-token-provider hook on `Session`); bundle 1.8–1.11. |
| 4 | 1.9 | External-token auth | Auth (missing-feature) | NOT-IMPLEMENTED | Bundle with 1.8. |
| 5 | 1.10 | Static-token auth | Auth (missing-feature) | NOT-IMPLEMENTED | Bundle with 1.8. |
| 6 | 1.11 | Federation client ID | Auth (missing-feature) | NOT-IMPLEMENTED | Bundle with 1.8. |
| 7 | 1.12 | HTTP / SOCKS proxy | Transport (missing-feature) | `ProxyConfig {}` empty struct | New kernel PR (populate `ProxyConfig`, wire into `client/`). |
| 8 | 1.13 | TLS custom CA + mTLS | Transport (in-flight) | PR #23 open | Land PR #23, then napi + adapter work. |
| 9 | 1.14 | Custom HTTP headers | Transport (missing-feature) | NOT-IMPLEMENTED | New kernel PR (custom-headers map on connection config). |
| 10 | 2.15 | `row_limit` on `StatementSpec` | Statement (surfacing-only) | Internal-only | Surface `row_limit` on public `StatementSpec` (kernel already serializes it on the SEA wire). |
| 11 | 2.17 | `queryTimeout` / `wait_timeout` | Statement (missing-feature) | NOT-IMPLEMENTED | New kernel PR (`StatementSpec.wait_timeout` distinct from transport `overall_timeout`). |
| 12 | 5.5 / 5.6 | Retry policy options | Retry (missing-feature) | NOT-IMPLEMENTED | New kernel PR (`RetryPolicyOptions` per `docs/designs/PECOBLR-2091-retry-logic-design.md`). |
| 13 | 5.9 | Staging volume API | Staging (missing-feature) | No Volume API | New kernel staging module (sizeable; defer). |
| 14 | 3.10 | Empty-result manifest fidelity | Result-fetching (BUG) | Manifest decoder degrades 5 types | Kernel bug-fix in `result_stream.rs` / `sea/manifest.rs`. See `jira-candidates-index.md`. |
| 15 | 2.13 / 3.9 | Connection-time `QUERY_TAGS` drop | Session (BUG) | Session-conf forwarding drops key | Kernel bug-fix in `session.rs` session-conf forwarding. See `jira-candidates-index.md`. |

The 13 missing-feature rows unblock 11 distinct user-facing options across two driver
languages once landed. The two bugs (rows 14 + 15) are independent of any feature work
and can be fixed in isolation — the driver-test parity tests already pin the current
buggy behavior with `// TODO: flip after kernel bug fixed` markers, so the kernel-side
fix will surface as loud test failures in the driver-test suite (the trigger to flip
the assertions to strict equality).
