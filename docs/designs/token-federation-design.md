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

# Mandatory OIDC Token Exchange (BYOT Federation) Design

## Overview

This document describes the design for adding **mandatory OIDC token exchange**
to the Databricks Rust SQL kernel. When an auth flow yields a JWT that was issued
by a party other than the target Databricks workspace (Azure AD, an external IdP,
a Bring-Your-Own-Token, or a Workload Identity Federation token), the kernel
exchanges it for an in-house Databricks OAuth token at the workspace's
`/oidc/v1/token` endpoint before using it on any Databricks API.

The feature is transparent to callers: it wraps whatever auth provider is already
in use (PAT, M2M, U2M) and only fires when the token's issuer differs from the
workspace host.

### Reference material

- **Spec (authoritative):** *[External-Simba] Mandatory Token-exchange for BYOT*
  (Eric Wang, Oct 2024). North star: "we will only allow exchanging the BYOT for
  an in-house OAuth token; any direct access to Databricks resources should use
  the in-house OAuth token rather than the BYOT."
- **Reference implementations:**
  - **ADBC** (`adbc-drivers/databricks`, C#): `MandatoryTokenExchangeDelegatingHandler`,
    `TokenExchangeClient`, `JwtTokenDecoder`. Matches the spec wire contract.
  - **JDBC** (`databricks-jdbc`, Java): `DatabricksTokenFederationProvider`. Uses a
    **different** grant on the wire (see [Wire contract](#wire-contract) — JDBC is off-spec).

### Motivation

The kernel today supports PAT, OAuth M2M, and OAuth U2M. None of these exchange a
foreign token for an in-house one, so BYOT / federated-identity tokens (e.g. an
Azure AD token on an AWS workspace, or an SP-wide WIF token) cannot authenticate.
This feature closes that gap and brings the kernel to parity with the JDBC and
ADBC drivers.

---

## Key design decision: decorator, not a new auth variant

The spec models federation as a **cross-cutting wrapper** over any JWT-producing
auth flow — decode the token, compare its `iss` claim to the workspace host,
exchange if foreign, otherwise pass through. This is exactly how JDBC
(`wrapWithTokenFederationIfEnabled`) and ADBC (inner `DelegatingHandler`) do it.

We implement it the same way: a `FederationProvider` that **wraps** an inner
`Arc<dyn AuthProvider>`. This keeps the PAT / M2M / U2M providers untouched and
makes federation apply uniformly across all of them.

**The reserved `AuthConfig::OAuthFederation` variant is *not* used for this.**
That variant (`{ subject_token, subject_token_type, audience }`) currently only
exists as a type — `build_base_auth_provider` returns an error for it. Its field
shape is the RFC 8693 `token-exchange` grant, a distinct "caller brings their own
subject token" feature. It is left reserved and unchanged; this design is
orthogonal to it.

**Always-on, no `enable` flag.** "Mandatory" means the exchange is not gated by
config. It is gated *internally* by the issuer-vs-host check, which makes it free
for the common cases:

- PAT tokens (`dapi…`) are not JWTs → not parseable → pass through, no HTTP call.
- Same-host M2M / U2M tokens → `iss == host` → pass through, no HTTP call.
- Only genuinely foreign tokens trigger an exchange, and any exchange failure
  falls back to the original token, so the wrapper cannot break existing flows.

---

## Wire contract

The exchange request follows the spec (which matches **ADBC**):

```
POST {host}/oidc/v1/token
Accept: */*
Content-Type: application/x-www-form-urlencoded

grant_type = urn:ietf:params:oauth:grant-type:jwt-bearer
assertion  = <source JWT>
scope      = sql
# exactly one of the following:
identity_federation_client_id = <id>              # SP-wide WIF (client id configured)
return_original_token_if_authenticated = true     # BYOT / account-wide WIF (no client id)
```

Response: `{ "access_token", "token_type", "expires_in" }`.

### The four spec scenarios

| Scenario | Extra param sent | Server behavior |
| --- | --- | --- |
| SP-wide WIF | `identity_federation_client_id=<id>` | returns a new token |
| Account-wide WIF | `return_original_token_if_authenticated=true` | returns a new token |
| BYOT | `return_original_token_if_authenticated=true` | returns the original token |
| Azure Databricks + AAD | `return_original_token_if_authenticated=true` | returns the original token |

When `identity_federation_client_id` is set, the server ignores
`return_original_token_if_authenticated`; the two are therefore mutually
exclusive on the wire.

### Divergence from JDBC (intentional)

JDBC sends RFC 8693 `token-exchange` semantics
(`grant_type=…:token-exchange`, `subject_token` + `subject_token_type`,
`client_id`) and *always* sends `return_original_token_if_authenticated`. That is
**off-spec**. This design deliberately follows the spec / ADBC, not JDBC.

---

## Architecture

### Integration point

`Session::open` builds the auth provider once, then hands it to the HTTP client
(`set_auth_provider`). Federation slots in by wrapping the base provider: the
provider construction is split into `build_auth_provider` (which wraps) and
`build_base_auth_provider` (which matches the auth flow):

```
build_auth_provider(auth, host, identity_federation_client_id, http_client)
  ├── base = build_base_auth_provider(auth, host, http_client)
  │            └── match auth { Pat | OAuthM2M | OAuthU2M => existing construction }
  └── FederationProvider::new(base, host, identity_federation_client_id, http_client)
```

`OAuthFederation` and `External` arms keep their current error behavior in
`build_base_auth_provider` (so those variants short-circuit before the wrapper).

### Sync-trait-over-async bridge

`AuthProvider::get_auth_header(&self) -> Result<String>` is **synchronous**, but
the exchange is an async HTTP call. This is the same constraint the M2M provider
already solves: capture `tokio::runtime::Handle::current()` in an `async fn new`,
then drive the async work from the sync method via
`crate::util::runtime::block_on_in_runtime(&handle, fut)` (as the M2M provider does).
This makes the exchange **blocking**, which is what both JDBC and ADBC converged
to (ADBC PR #46 switched from background to blocking to stop the first request
going out with the un-exchanged token).

### Caching and concurrency

A provider tracks exactly **one active source token** at a time, so the cache is a
**single slot** — `RwLock<Option<CacheEntry>>` where `CacheEntry` is
`(source_token, exchanged: OAuthToken)`:

- Keying on the source token means that when the inner credential rotates (M2M /
  U2M refresh produces a new bearer), the slot is replaced and the new token is
  exchanged fresh.
- Storing an `OAuthToken` (which carries `is_expired()`) means an expired
  *exchanged* token is also re-exchanged.
- A single slot (vs. a map) can't leak: rotated / expired entries are overwritten,
  not accumulated. And holding the source token verbatim lets a cache hit compare
  it directly, with no per-request hashing on the hot path.

**No lock is ever held across the network call, and concurrent misses coalesce
single-flight.** A cache hit returns under a brief read lock. On a miss, callers
elect a leader via a lock-free `AtomicBool` (`compare_exchange`): the winner runs
the exchange with **no lock held** and publishes the outcome to the slot; the
losers sleep-poll the slot (1 ms cadence) until the leader clears the flag, then
read the published result. An RAII guard clears the flag on drop, so a leader that
panics can't leave followers spinning.

This is deliberately modeled on the kernel's own `TokenStore` refresh coordination
(`AtomicBool` leader election + sleep-poll followers), giving parity with that
in-repo convention and with the Go driver's `CachedTokenProvider`. Two reasons the
single-flight matters here specifically:

- `get_auth_header` is synchronous and runs on a tokio worker; holding a
  `std::sync::Mutex` across `block_on_in_runtime(exchange)` would park that worker
  for the whole retry budget. Coalescing keeps only **one** caller in the runtime
  per exchange (the leader), so N concurrent misses don't each enter
  `block_in_place`.
- On a rotated / expired token a busy shared session can miss on many threads at
  once. For **SP-wide WIF** each exchange mints a *distinct* server token, so
  letting them race would mint N tokens and orphan N−1. Coalescing keeps it to one.

We adapt `TokenStore`'s pattern in two ways it doesn't cover: the follower checks
the published entry is for **its** source token (if the upstream credential rotated
mid-wait, it falls back rather than returning another token's result), and the
leader **always publishes an outcome** — on failure it stores the original token
(short TTL) so followers read the same non-breaking fallback instead of spinning to
a timeout. On any exchange failure every caller uses the original token (spec:
"continue with the existing JWT token instead of throwing"). The leader's latency
is bounded by the **Auth-category retry budget** (30s / 3 retries — see
`build_retry_configs_for_session`), not the global ladder; followers additionally
cap their wait so a wedged leader can't hang them indefinitely.

---

## Module structure

All new code lives under `src/auth/oauth/`.

### `src/auth/oauth/jwt.rs` (new)

JWT claim decoder. **No new dependency** — uses the already-present `base64` 0.22
and `serde_json`. **No signature verification** (we only read `iss` to decide
routing; the workspace validates the token).

```rust
/// Extract the `iss` claim from a JWT, or None if the input is not a
/// parseable JWT (e.g. a PAT `dapi…` token) or has no issuer.
pub(crate) fn issuer(token: &str) -> Option<String>;
```

Mechanics: split on `.`, base64url-decode `parts[1]`, `serde_json` parse, read
`iss`. A non-JWT input returns `None`, which the caller treats as pass-through
(the spec's "if not JWT but auth type is OAuth, fall back to existing token").

### `src/auth/oauth/federation.rs` (new)

```rust
pub struct FederationProvider {
    inner: Arc<dyn AuthProvider>,
    token_endpoint: String,        // {host}/oidc/v1/token, built directly
    workspace_host: String,        // normalized host, for iss-host comparison
    identity_federation_client_id: Option<String>,
    http_client: Arc<DatabricksHttpClient>,
    runtime_handle: tokio::runtime::Handle,
    cache: RwLock<Option<CacheEntry>>,  // single slot: (source_token, exchanged)
    exchanging: AtomicBool,             // single-flight leader election
}

impl FederationProvider {
    pub async fn new(
        inner: Arc<dyn AuthProvider>,
        host: &str,
        identity_federation_client_id: Option<String>,
        http_client: Arc<DatabricksHttpClient>,
    ) -> Result<Self>;
}
```

`get_auth_header()` logic:

1. `let header = self.inner.get_auth_header()?;` → strip `"Bearer "` → `source`.
2. If `jwt::issuer(source)` is `None`, **or** the issuer host equals
   `self.workspace_host` → **return `header` unchanged** (pass-through). This is
   re-checked on every call so a fresher inner token is never shadowed by a stale
   cached exchange.
3. Foreign issuer → under a read lock, if the cache slot holds a non-expired entry
   for this exact source token, return it.
4. Miss (empty / rotated source / expired) → (current-thread runtime guard first —
   see below) then coalesce single-flight on `exchanging`:
   - **Leader** (won `compare_exchange(false → true)`): re-check the cache, then run
     `block_on_in_runtime(&self.runtime_handle, self.exchange(source))` **holding no
     lock**, take a brief write lock to publish the outcome, and drop the RAII guard
     that clears the flag.
   - **Follower** (lost the election): sleep-poll (1 ms) until the flag clears, then
     return the published entry if it is for *this* source token, else fall back to
     the original token.
5. **Leader success** → store `(source, OAuthToken)` (from `expires_in`), return
   `Bearer <new token>`.
   **Leader failure** → log at `warn!`, store the original token (short TTL) so
   followers and subsequent callers read the same fallback instead of re-hitting a
   failing endpoint, and **return the original `header`** (spec's non-breaking
   fallback). The error is never surfaced to the caller.

`block_on_in_runtime` uses `block_in_place`, which panics on a **current-thread**
runtime. Every shipped consumer (pyo3, napi, C ABI) drives the kernel on a
multi-thread runtime, but the resolver guards against it anyway: if
`runtime_handle.runtime_flavor()` is current-thread, it skips the exchange and
returns the original token (the same non-breaking posture as a failed exchange)
rather than panicking.

`async fn exchange(&self, source: &str) -> Result<OAuthToken>`:

- Build the form body via reqwest `.form(&[...])` on `http_client.inner()`, set
  `Accept: */*`, select `identity_federation_client_id` **xor**
  `return_original_token_if_authenticated=true`.
- Run through `http_client.execute_without_auth(req, RequestType::AuthTokenRequest)`
  — **without auth** because the request carries its own `assertion`, not the
  kernel bearer (the same pattern OIDC discovery uses). Using
  `RequestType::AuthTokenRequest` gives it the Auth-category retry ladder:
  exponential backoff on 408/502/503/504 and timeouts — exactly the spec's retry
  requirement.
- Parse `{access_token, token_type, expires_in}` → `OAuthToken::new(...)`.

### `src/auth/oauth/mod.rs`, `src/auth/mod.rs`

Add `pub mod jwt;` and `pub mod federation;`; re-export `FederationProvider`.

### `src/config.rs`

One caller-facing field (no wrapper struct, no `enabled`):

```rust
/// SP-wide Workload Identity Federation client id. `None` → BYOT /
/// account-wide WIF, where the exchange sends
/// `return_original_token_if_authenticated=true` instead.
pub identity_federation_client_id: Option<String>,
```

Placed on `SessionConfig` (top level, since it applies across PAT/M2M/U2M) and
threaded through the builder.

### `src/session.rs`

Split provider construction into `build_auth_provider` (wraps) and
`build_base_auth_provider` (matches the auth flow), as shown in
[Integration point](#integration-point). `SessionBuilder` gains an
`identity_federation_client_id(...)` setter, threaded onto `SessionConfig`.

### `src/kernel_error.rs`

No new variant. Exchange failures use existing `Error::unauthenticated()` /
`Error::network_error()` for the `warn!` log only; they are swallowed into the
fallback and never returned.

### Bindings — exposing `identity_federation_client_id` on every boundary

The exchange itself works through all consumers unchanged (it is gated by the
token issuer, not config), but the SP-wide WIF *selector* must be reachable from
each surface, or Python / Node / ODBC / Go / C callers can only ever take the
BYOT / account-wide WIF branch. It is threaded through:

- **C ABI** (`src/c_abi/session.rs`, consumed by ODBC and Go): new
  `kernel_session_config_set_identity_federation_client_id()` setter, stored on
  `KernelSessionConfig` and forwarded to the builder; declared in
  `include/databricks_kernel.h` (hand-written header).
- **pyo3** (`pyo3/src/session.rs`): new optional `identity_federation_client_id`
  keyword on `Session(...)`.
- **napi** (`napi/src/database.rs`): new optional `identityFederationClientId`
  field on `ConnectionOptions`; the generated `napi/index.d.ts` is updated by hand.

Each binding stages its own changelog entry (`pyo3/NEXT_CHANGELOG.md`,
`napi/NEXT_CHANGELOG.md`); the core entry lives in the root `NEXT_CHANGELOG.md`.

---

## Testing

New integration test file `tests/token_federation_wiremock.rs`, using `wiremock`
0.6 with `#[tokio::test(flavor = "multi_thread")]`. Most cases build the HTTP
client with `max_retries = 0` to avoid real backoff sleeps; the degraded-endpoint
concurrency case enables a small bounded retry budget.

| Case | Setup | Assertion |
| --- | --- | --- |
| Foreign issuer exchanged | inner JWT `iss=https://other/oidc`; mock `/oidc/v1/token` → new token | outgoing uses exchanged token; body has `grant_type=jwt-bearer`, `assertion`, `scope=sql` |
| Same-host pass-through | JWT with `iss` == workspace host | `/oidc/v1/token` `.expect(0)`; original token used |
| Non-JWT (PAT) pass-through | inner header `Bearer dapi…` | `.expect(0)`; original used |
| SP-wide WIF param | `identity_federation_client_id = Some("x")` | body has `identity_federation_client_id=x` and **no** `return_original…` |
| BYOT / account-wide param | client id `None` | body has `return_original_token_if_authenticated=true` and no client id |
| Exchange 401 → fallback | mock → 401 | no error; original token used |
| Exchange 500 → fallback | mock → 500 | no error; original token used |
| Cached across calls | same source token | exactly one exchange, then cache hits |
| Representative Databricks issuer | realistic `adb-….azuredatabricks.net/oidc` issuer vs. matching workspace host (not the mock URI) | `.expect(0)`; pass-through (non-circular same-host check) |
| Concurrent cold callers coalesce | 10 concurrent callers miss simultaneously; success endpoint | mock `.expect(1)` — exactly one exchange; all callers get the shared token (single-flight parity check) |
| Degraded endpoint fallback | always-503 endpoint, retries enabled, 8 concurrent callers | all fall back to original token, no error; leader-bounded wait |

Unit tests inline in `jwt.rs` (iss extraction, missing iss, malformed/2-part
input, base64url padding) and `federation.rs` (host comparison, param selection).

Binding boundary tests: a C ABI setter unit test (records the value, rejects NULL)
plus a check in the C smoke test (`tests/c/session_smoke.c`); a pyo3 test that the
kwarg is accepted (paired with an unknown-kwarg `TypeError` guard); and a napi test
that the field is present on the `ConnectionOptions` boundary struct.

---

## Deltas vs. reference drivers

- **vs ADBC:** identical wire contract and blocking + cached model. Kernel-specific
  differences are the sync-trait-over-async bridge (`block_on_in_runtime`) instead
  of an HTTP `DelegatingHandler`; a single expiry-aware cache slot rather than
  ADBC's insert-only `ConcurrentDictionary` (which never evicts); and single-flight
  coalescing (`AtomicBool` leader election + sleep-poll followers, mirroring the
  kernel's own `TokenStore` and Go's `CachedTokenProvider`) with no lock held across
  the network — where ADBC uses an `async SemaphoreSlim` held across the call.
- **vs JDBC:** deliberately does *not* copy JDBC's off-spec
  `token-exchange` / `subject_token` / `client_id` parameters; follows the spec's
  `jwt-bearer` / `assertion` / `identity_federation_client_id`.
