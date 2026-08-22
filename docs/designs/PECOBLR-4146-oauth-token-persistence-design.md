<!--
Copyright (c) 2026 Databricks, Inc.
Licensed under the Apache License, Version 2.0.
-->

# OAuth token persistence on the kernel path (PECOBLR-4146)

**Status:** Decided. **PuPr:** kernel-owned persistence (enable / disable /
locate the built-in encrypted disk cache). **Post-GA:** caller-supplied custom
token store, only if a named consumer needs a non-file sink.
**Parent:** PECOBLR-3727 (databricks-sql-kernel Reyden support).

## Problem

On the kernel path the kernel owns the OAuth **U2M** flow and, by default,
persists the refresh token to an encrypted on-disk cache
(`~/.config/databricks-sql-kernel/oauth/`, `0600`, `TokenCacheConfig.enabled =
true`, matching JDBC `EnableTokenCache=1`). Two questions follow:

1. Should a caller be able to **turn that off / relocate it** (e.g. a policy that
   forbids tokens at rest)?
2. Should a caller be able to substitute their **own token store** — the Thrift
   connectors' `OAuthPersistence` (Node) / `experimental_oauth_persistence`
   (Python) — backing a database, OS keychain, or in-memory object?

## Decision

For **PuPr**, answer (1): the kernel owns the token lifetime, and the caller can
enable/disable/locate the built-in encrypted disk cache. Answer (2) — a
caller-supplied custom store — is **post-GA**, built only when a named consumer
needs a non-file sink.

### Rationale

- **No demonstrated demand for custom stores.** `experimental_oauth_persistence`
  was introduced in the *foundational* OAuth PR (databricks-sql-python#15, 2022)
  as an extensibility hook — `experimental_`, documented "beta (private
  preview)", with **no named customer** driving it. The connector ships only
  `OAuthPersistenceCache` (in-memory) and `DevOnlyFilePersistence` ("only
  intended for development", writes **plaintext**). It ships **no production
  store**; the docstring tells prod users to "write your own".
- **The kernel already provides the missing prod store.** The kernel's built-in
  disk cache is exactly the hardened, restart-surviving store the connector told
  users to build — encrypted (`cache_crypto`), `0600`, cross-process — and it is
  **stronger** than anything the connector ships. So the common prod U2M need
  ("make the browser login survive restarts, securely") is met with **zero user
  code**.
- **The default divergence is really an on/off question.** Thrift Python defaults
  to *no* persistence; Thrift Node defaults to *in-memory*; the kernel defaults
  to *on-disk* (JDBC convention). Reconciling this is an **enable/disable**
  decision — squarely the PuPr scope.
- **No telemetry signal.** Connector telemetry records `auth_mech` + `auth_flow`
  only — nothing about whether a persistence object was supplied or its type — so
  custom-store adoption can't even be measured.
- **The custom-store path is the entire high-risk surface** (an async
  request/response `ThreadsafeFunction` on napi; a GIL callback on pyo3), and
  would ship without live-host E2E coverage. The PuPr scope is scalar config — no
  callbacks, no async-FFI, trivially testable.

Low-regret: the custom-store design is fully worked out and prototyped (see
*Post-GA*), so it can be added later with nothing lost by waiting.

## PuPr scope — kernel-owned persistence

The kernel already owns acquisition, refresh, in-memory reuse, and encrypted
on-disk persistence. PuPr adds **no new kernel auth machinery** — it exposes the
*existing* `TokenCacheConfig` (in `src/config.rs`, already `enabled` +
`passphrase`, already reachable over the C-ABI via
`kernel_session_config_set_u2m_token_cache_config`) through the two bindings as
scalar config:

- **napi** — `ConnectionOptions` gains optional `tokenCacheEnabled` /
  (optional) `tokenCachePassphrase` / (optional) cache-dir override.
- **pyo3** — `Session(…)` gains the matching optional kwargs.
- **Kernel** — optionally add a `dir`/`path` override field to `TokenCacheConfig`
  (the default location is fine for most; the override is a nice-to-have, not
  required for PuPr).

No token-store trait, no binding callbacks. A caller who supplies nothing gets
today's encrypted-disk default; a caller can disable persistence (no token at
rest) or point it at a chosen location.

### The default the connector adapters must choose

The kernel defaults **on-disk**; Thrift defaults **in-memory / none**. The
connector adapters (databricks-sql-nodejs#457, databricks-sql-python#913) must
consciously pick per binding:

- **Adopt the kernel/JDBC default** — persist to disk by default (fewer logins;
  but the refresh token is now at rest where it wasn't on Thrift), or
- **Match Thrift's conservative default** — disable the kernel disk cache unless
  the user opts in.

The enable/disable knob is what makes either choice expressible. This is a
product decision, flagged on those tickets — not something the kernel picks
silently.

### Out of scope — Go / C-ABI

The C-ABI has no callback surface and Go has no persistence concept to forward.
Its existing `kernel_session_config_set_u2m_token_cache_config` (enable/disable +
passphrase) already covers the PuPr scope for Go.

## Post-GA — caller-supplied custom store

Add this **only when a named consumer needs a non-file token sink** (OS keychain,
secrets manager / Vault, corporate policy mandating "no token on disk", or a
multi-tenant server sharing tokens in a DB). Until then it stays deferred.

**Design (prototyped — see below):**

- Kernel: a `TokenPersistence` trait (`async load/save` over `OAuthToken`), with
  today's disk cache refactored into a `DiskTokenCache` default impl, and
  `AuthConfig::OAuthU2M { token_store: Option<Arc<dyn TokenPersistence>> }` (set
  via the `oauth_u2m(…)` constructor, since `AuthConfig` is `#[non_exhaustive]`).
- Wire format: the kernel's `OAuthToken` serialized to JSON (lossless; the exact
  bytes the disk cache stores) — avoids reconstructing expiry from a partial
  token.
- Store key: the callback boundary MUST carry the full `{host, client_id,
  scopes}` triple — the same key `TokenPersistence` / `DiskTokenCache` use (the
  cache filename is a SHA-256 of all three; see `src/auth/oauth/cache.rs`).
  Keying a caller-supplied store on `host` alone would collide two U2M sessions
  to the same host with different `client_id`/`scopes` — the second `save`
  overwrites the first and a `load` can return a token minted for a different
  client/scope set — silently diverging from the built-in disk cache, which
  keeps those as distinct entries. So the napi/pyo3 persist/load args must
  include `client_id` and `scopes`, not just `host` and `token_json`.
- napi: `openSession` gains two callbacks wrapped as `ThreadsafeFunction`s;
  `load`/`save` `call_async` the JS function and `await` its `Promise`.
- pyo3: a `PyTokenStore` calling the Python object under the GIL
  (`Python::attach`), carried on `OAuthParams.token_store`.
- Error semantics: `load` error → cache miss (fresh login, never fails connect);
  `save` error → logged and swallowed.
- The same bridge (a callback into the binding) would also unblock the
  currently-rejected `token-provider` / `external-token` / `custom` authTypes and
  a Go/C-ABI store.

**Prototype status:** the full custom-store implementation (kernel trait +
`DiskTokenCache` + napi `ThreadsafeFunction` bridge + pyo3 GIL bridge) was built
and compiles / unit-tests clean; it lives in the branch history of the initial
PECOBLR-4146 PR. **It was not E2E-tested against live Node/Python U2M** — that,
plus a named requirement, is the gate before it ships.

## Testing

- **PuPr:** scalar-config plumbing — assert the napi/pyo3 flags map to
  `TokenCacheConfig`; kernel U2M E2E with the cache enabled (survives restart)
  and disabled (fresh login each run). The default disk path is already verified
  E2E (U2M open + a two-run disk-cache round-trip that skips the browser on the
  second run).
- **Post-GA (custom store):** the prototype's Rust mock-store unit tests, plus
  the missing piece — live-host E2E through the Node and Python connectors with a
  real custom in-memory store (round-trip + no deadlock on the napi TSFN / pyo3
  GIL paths).
