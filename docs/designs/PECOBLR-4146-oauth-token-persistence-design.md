<!--
Copyright (c) 2026 Databricks, Inc.
Licensed under the Apache License, Version 2.0.
-->

# Pluggable OAuth token persistence (PECOBLR-4146)

**Status:** implemented (kernel core + napi + pyo3). Go / C-ABI out of scope.
**Parent:** PECOBLR-3727 (databricks-sql-kernel Reyden support).

## Problem

On the kernel path the kernel owns the OAuth **U2M** flow and persists the
refresh token to a hardcoded encrypted on-disk cache
(`~/.config/databricks-sql-kernel/oauth/`). Callers could not supply their own
token store, so there was no equivalent to the Thrift connectors'
`OAuthPersistence` (Node) / `experimental_oauth_persistence` (Python) — a
caller-supplied `read`/`persist` object that can back onto a database, an OS
keychain, or an in-memory store.

## Design

A single kernel-side trait, injected through `AuthConfig`, with the two
callback-capable bindings wiring their host-language callables onto it.

### Kernel core — `src/auth/oauth/persistence.rs`

```rust
#[async_trait]
pub trait TokenPersistence: Send + Sync + std::fmt::Debug {
    async fn load(&self, host: &str, client_id: &str, scopes: &[String]) -> Result<Option<OAuthToken>>;
    async fn save(&self, host: &str, client_id: &str, scopes: &[String], token: &OAuthToken) -> Result<()>;
}
```

- **`DiskTokenCache`** (same module) is the default impl, wrapping the existing
  `TokenCache` + resolved passphrase — so behavior is **unchanged** when no
  custom store is supplied.
- `AuthConfig::OAuthU2M` gains `token_store: Option<Arc<dyn TokenPersistence>>`.
  When `Some`, it overrides the `token_cache` disk policy. Because `AuthConfig`
  is `#[non_exhaustive]`, cross-crate bindings set it through the `oauth_u2m(…)`
  constructor (a new trailing argument), not a struct literal.
- `session.rs::build_base_auth_provider` resolves the effective store: a
  caller store wins; otherwise it derives a `DiskTokenCache` from the
  `token_cache` policy (or `None` to disable persistence).
- `AuthorizationCodeProvider` calls `store.load(…)` before deciding to open a
  browser and `store.save(…)` after every acquisition/refresh.

**Error semantics (lenient by contract):** a `load` error is treated as a
cache miss (fresh browser login, never fails the connection); a `save` error is
logged and swallowed (a failed cache write must not fail otherwise-good auth).

### Wire format

The token crosses every binding boundary as the kernel's `OAuthToken`
**serialized to JSON** (serde) — the exact bytes the on-disk cache stores. This
is lossless (preserves `expires_at`, scopes, refresh token) and identical
across bindings, and it sidesteps reconstructing expiry from a partial token.
The `{host, client_id, scopes}` triple is the store key.

### napi (Node) — `napi/src/database.rs`

- `openSession(options, oauthPersistenceRead?, oauthPersistencePersist?)` — two
  optional JS callbacks. Both required together; a lone one is treated as "no
  store".
- `NapiTokenStore` wraps them as `ThreadsafeFunction`s (mirroring the existing
  `logger.rs` kernel→JS bridge). `load`/`save` call the JS function via
  `call_async` and `await` the returned `Promise`. `read(host) => Promise<string
  | undefined>`; `persist({host, tokenJson}) => Promise<void>`.

### pyo3 (Python) — `pyo3/src/auth.rs`, `pyo3/src/session.rs`

- `Session(…, oauth_persistence=…)` — a Python object exposing
  `read(host) -> Optional[str]` and `persist(host, token_json) -> None`.
- `PyTokenStore` calls the object under the GIL (`Python::attach`); no `await`
  is held across the GIL, so the async future stays `Send`. Carried on
  `OAuthParams.token_store` so `build_auth_config`'s signature and its
  GIL-less unit-testable inner stay unchanged.

### Out of scope — Go / C-ABI

The C-ABI has no callback surface (data-only config), and Go has no persistence
concept to forward, so a Go/C-ABI callback bridge is deferred. Go's only
existing knob (`kernel_session_config_set_u2m_token_cache_config`,
enable/disable + passphrase) already covers the one Go-relevant case. The same
`AuthConfig::External(Box<dyn TokenProvider>)`-style bridge would later unblock
both a Go store and the currently-rejected `token-provider` / `external-token` /
`custom` authTypes.

## Driver follow-ups

The connector repos adapt their existing persistence objects onto the new
binding surface: databricks-sql-python#913 (`experimental_oauth_persistence`)
and databricks-sql-nodejs#457 / #505 (`OAuthPersistence`, currently rejected on
the kernel path).

## Testing

- Rust: in-memory mock `TokenPersistence` (load-hit skips browser, save after
  acquire/refresh); existing wiremock U2M suite refactored onto `DiskTokenCache`.
- napi: `cargo test --features dyn-symbols`; end-to-end through the nodejs
  connector (needs a live Node host for the TSFN).
- pyo3: `cargo clippy` + maturin-linked build; full Python E2E with a fake
  store via the connector.
