# Engineer-bot learning log — databricks-sql-kernel

Durable, reusable engineering learnings distilled from merged PRs by the
engineer-bot retrospective flow (`engineer-bot-learning.yml` →
`databricks_bot_engine.engineer_bot.retrospective`). Each entry is a **Context** +
**Rule** pair; the Rule is written to be greppable so future engineer-bot runs can
surface it. Entries run oldest→newest.

Both bots also read this file as repo conventions (`.claude/knowledge/**` is always
injected into the system prompt), so a logged Rule feeds forward into future
author, follow-up, and review runs.

## Entries

### 2026-08-14: learnings since 2026-08-13T07:50:36Z
- **Context:** PR #241 (mandatory OIDC token exchange) added `FederationProvider`/`CacheEntry` with `#[derive(Debug)]`; both a review bot and a human reviewer flagged that the derived impl prints the raw source JWT and the minted in-house `access_token` in the clear, bypassing the redaction that `AuthConfig` in `src/config.rs` hand-writes.
  **Rule:** Never `#[derive(Debug)]` on a type that transitively holds auth tokens/secrets (access tokens, client secrets, JWTs) — hand-write a `Debug` that redacts the secret fields, matching the repo's existing `AuthConfig` redaction convention, and add a test asserting tokens never appear in Debug output.
- **Context:** PR #246 collapsed an ODBC empty `table_types` CSV to `None` because `Some(&[])` now means match-NONE (hiveserver2 #83 reversed the earlier empty==null reading); PR #248's reviewer flagged the mirror case where a C caller passing `""` records `Some("")` and silently selects SP-wide WIF instead of the documented "unset" BYOT branch.
  **Rule:** At C-ABI/ODBC boundaries an empty string or empty collection is NOT the same as absent — `required_str`/CSV splits yield `Some("")`/`Some(&[])`, distinct from `None`. Explicitly collapse empty→`None` (or reject it) so the documented "unset" semantics hold, and lock the collapse with a test since downstream match-arms treat `Some(empty)` differently from `None`.
- **Context:** PRs #247 and #241 threaded U2M OAuth defaults; reviewers repeatedly flagged that overriding the OAuth client without pinning its matching scopes and redirect port inherits the kernel's defaults for a *different* registered app — e.g. `databricks-sql-python` requesting `all-apis` → `access_denied`, or the `8030` default port → `redirect_uri_mismatch`.
  **Rule:** U2M OAuth `{client_id, scopes, redirect_port}` are a coupled bundle tied to one registered IdP app — when a binding overrides `client_id`, it must also pin the scopes and redirect port for that app (only when the caller relies on the default client); never half-override the bundle.
- **Context:** PR #247's reviewer caught that `napi/index.d.ts` is auto-generated from the Rust doc comments on `ConnectionOptions`, so a doc-only edit to `napi/src/database.rs` that skipped regenerating the `.d.ts` propagated stale values (port `8020`, wrong scopes) into the committed types; the fix hand-synced both sides.
  **Rule:** `napi/index.d.ts` is generated from the Rust doc comments in `napi/src/database.rs` — any change to those docs (or to defaults they describe) must regenerate or hand-sync `index.d.ts` in the same PR, or the two silently drift.

---
*Add new entries above this line (oldest→newest). The retrospective inserts each
new entry just above this footer.*
