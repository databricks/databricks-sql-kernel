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

# PR 5 — Async Statement: detailed plan

> Plan-and-implementation doc. Plan written first; the rest of this PR
> implements it. Companion to the PR 5 contract in
> `docs/migration-plan.md` §4 and the spec's `ExecutedAsyncStatement`
> in `docs/spec-v0.md` addendum A1.

## 1. Goal

After PR 5, callers can submit a statement without waiting for
completion and poll on their own cadence:

```rust
let mut stmt = session.statement();
stmt.spec().sql("SELECT count(*) FROM hot_table");
let async_exec = stmt.submit().await?;

// caller-driven polling
loop {
    match async_exec.status().await? {
        StatementStatus::Succeeded => break,
        StatementStatus::Pending | StatementStatus::Running => {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        terminal => return Err(...), // Failed / Cancelled / Closed
    }
}
let mut stream = async_exec.fetch_result().await?;

// or just await
let mut stream = async_exec.await_result().await?;
```

This is the surface long-running queries (>10s, beyond `execute()`'s
sync polling budget) and progress-tracking clients need.

## 2. Why submit needs a new Transport entry point

`Transport::execute_statement` (from PR 4) was carefully constructed
to **always return a ready-to-read `ExecuteResult`**: it polls
internally, waits for the terminal state, then builds the reader. The
sync `Statement::execute` rides that contract — when the future
resolves, results are ready.

`Statement::submit` has the opposite contract: **return as soon as the
server hands back a `statement_id`**, no polling, no reader. We need a
new transport method:

```rust
// New in PR 5.
async fn submit_statement(
    &self,
    session_id: &str,
    sql: &str,
    params: &ExecuteParams,
) -> Result<SubmitResult>;

pub(crate) struct SubmitResult {
    pub statement_id: String,
    pub status: StatementStatus,           // SEA wire status
    pub manifest: Option<ResultManifest>,  // may be present if server inlines
    pub result: Option<ExecuteResultData>, // ditto
}
```

`submit_statement` issues the POST with `wait_timeout=0s` and
**returns whatever the server replied with**, even if that's
`PENDING`/`RUNNING`. No internal polling, no reader construction.

When the caller eventually wants the reader (via `await_result()` or
`fetch_result()`), the kernel calls `Transport::get_statement_status`
to refresh and `ResultReaderFactory::create_reader` to materialise the
reader. The factory already handles every result-data shape (inline,
CloudFetch, empty); we just need to plumb it through.

To make the factory reachable from `ExecutedAsyncStatement`, we
expose a thin `Transport::fetch_completed_result(statement_id)` that
returns an `ExecuteResult` for an already-completed statement (or
errors if it isn't terminal). This keeps the factory ownership inside
the transport where it belongs.

## 3. `StatementStatus` shape

The skeleton's `StatementStatus` enum already exists:

```rust
#[non_exhaustive]
#[derive(Debug)]
pub enum StatementStatus {
    Pending,
    Running,
    Succeeded,
    Failed(Error),
    Cancelled,
    Closed,
}
```

PR 5 builds a `from_sea(sea_status: SeaStatementStatus, statement_id:
&str) -> StatementStatus` conversion — `Failed` carries the same
structured `Error` that `Transport::execute_statement` produces today
in its polling loop (sql_state extraction, error_code preservation).
This is factored from the existing `wait_for_completion` body in
`src/client/sea.rs` so both paths produce identical errors.

## 4. `ExecutedAsyncStatement` surface

```rust
pub struct ExecutedAsyncStatement {
    session: Arc<SessionInner>,
    statement_id: String,
    valid: ValidityFlag,
    closed: Arc<AtomicBool>,
}

impl ExecutedAsyncStatement {
    pub async fn status(&self) -> Result<StatementStatus>;
    pub async fn await_result(&self) -> Result<ResultStream>;
    pub async fn fetch_result(&self) -> Result<ResultStream>;
}

#[async_trait]
impl ExecutedStatementHandle for ExecutedAsyncStatement {
    fn statement_id(&self) -> &str;
    async fn cancel(&self) -> Result<()>;
    async fn close(&self) -> Result<()>;
}
```

### `status()` semantics

Single GetStatementStatus call. Validity-checked. Returns the public
`StatementStatus` enum (server-state → kernel-state mapping above).

### `await_result()` semantics

Polls `Transport::get_statement_status` with bounded backoff until
terminal:

- `Succeeded` → call `Transport::fetch_completed_result` to materialise
  the reader, wrap in `ResultStream`, return.
- `Failed(err)` → return the structured error verbatim.
- `Cancelled` → return `Error::cancelled()` (new helper if not
  present; otherwise `Error::invalid_state().message("...cancelled...")`).
- `Closed` without result → return `Error::invalid_state()`.

Polling cadence reuses `TransportConfig.poll_interval` /
`poll_timeout` so behaviour matches the sync path.

### `fetch_result()` semantics

No polling. Calls `Transport::fetch_completed_result` directly. If the
statement isn't terminal yet, returns `Error::invalid_state()` with a
message pointing the caller at `status()` first. (The spec's `A8`
addendum logged a future `FailedPrecondition` / `InvalidState`
taxonomy expansion; until then we use `InvalidState`.)

### `cancel` / `close` / `Drop`

Identical pattern to `ExecutedStatement` (PR 4): validity-checked
cancel; idempotent close via `AtomicBool` flag; fire-and-forget
close-on-Drop spawn through `session.runtime_handle()`.

## 5. `Statement::submit` body

Mirrors `Statement::execute` but uses `submit_statement` and
returns `ExecutedAsyncStatement`:

```rust
pub async fn submit(&mut self) -> Result<ExecutedAsyncStatement> {
    let _g = self.session.log_span().clone().entered();

    self.invalidate_prior().await;   // reused from PR 4

    let sql = /* validate spec.sql */;
    /* validate result_format, reject named params */;
    let exec_params = build_execute_params(spec, "0s");

    let cancel_state = MidExecuteCancelState::new(...);
    let guard = cancel_state.guard();

    let submitted = self.session.transport()
        .submit_statement(self.session.session_id(), &sql, &exec_params)
        .await?;

    cancel_state.observe(&submitted.statement_id);
    guard.defuse();

    let valid = ValidityFlag::new();
    self.last_statement_id = Some(submitted.statement_id.clone());
    self.last_valid = Some(valid.clone());

    Ok(ExecutedAsyncStatement::new(
        Arc::clone(&self.session),
        submitted.statement_id,
        valid,
    ))
}
```

`build_execute_params` is extracted from the existing `execute()`
body. The only difference vs `execute()` is the `wait_timeout`
parameter and the path that follows.

## 6. Folded-in §7 cleanups

PR 5 also closes three follow-ups logged in earlier PRs:

### 6.1 `ResultStream::into_owned_batches`

Owned-batch iterator for the C ABI / PyO3 / napi-rs:

```rust
impl ResultStream {
    pub fn into_owned_batches(self) -> OwnedBatchIter;
}

pub struct OwnedBatchIter {
    inner: Box<dyn ResultReader>,
    runtime: tokio::runtime::Handle,
    exhausted: bool,
}

impl Iterator for OwnedBatchIter {
    type Item = Result<RecordBatch>;
}
```

`runtime` is captured at construction; `next()` calls the inner reader
sync (the reader already `block_on`s internally for the network-driven
variants). Iterator stops on `None` or the first error. PR 6's C ABI
will consume this directly.

### 6.2 Per-method `tracing::Span` on Statement / ExecutedStatement

Every public Statement and ExecutedStatement method enters the
session's `log_span`. PR 4 added this on `Statement::execute` only.
PR 5 finishes the work for `Statement::submit`,
`ExecutedStatement::cancel`/`close`, and the new
`ExecutedAsyncStatement` methods.

### 6.3 Pre-statement_id mid-execute cancel — **explicitly deferred**

The PR 4 §7 follow-up about cancelling a future dropped before the
server returns `statement_id` requires a transport callback hook. Not
in PR 5 — the change is architecturally separate and the gap is
narrow in practice. Re-tracked for v0.1.

## 7. Tests

### Unit (in `src/`)

- `statement_status::tests` — `StatementStatus::from_sea` round-trips
  every SEA `StatementState` to the public kernel enum; `Failed`
  preserves `sql_state` and `error_code`.

### Wiremock (`tests/v0_async_execute_wiremock.rs`, new)

- `submit_returns_pending` — POST `/statements` with `wait_timeout=0s`
  returns `PENDING`; `ExecutedAsyncStatement::status()` returns
  `Pending`.
- `await_result_polls_until_succeeded` — submit → first status returns
  `RUNNING`, second returns `SUCCEEDED` with inline data;
  `await_result()` resolves to a `ResultStream` yielding the row.
- `await_result_propagates_failed_status` — submit → status returns
  `FAILED` with `error_code` + `sql_state` in the envelope;
  `await_result()` returns the structured `SqlError`.
- `fetch_result_rejects_non_terminal` — submit returns `RUNNING`;
  `fetch_result()` returns `InvalidState`.
- `re_submit_invalidates_prior_async_handle` — `submit()` twice on the
  same `Statement`; the first handle's `cancel()` returns
  `InvalidStatementHandle`.
- `executed_async_close_is_idempotent` — same shape as the
  `ExecutedStatement` test.

### E2E (`tests/v0_async_execute_e2e.rs`, new; `#[ignore]`)

- `submit_returns_quickly_for_short_query` — submit `SELECT 1`,
  measure end-to-end ≤ ~1s (no 10s sync-poll budget).
- `await_result_round_trips_select_one` — submit + await_result +
  read the single row.
- `status_reports_progress_for_long_query` — submit
  `SELECT * FROM range(1e7)`, observe `Running` at least once before
  `Succeeded`.
- `cancel_long_running_async_query` — submit a long-running query in
  one task, `cancel()` from another, observe the cancelled state.
- `fetch_result_after_succeeded_returns_rows` — submit + busy-poll
  via `status()` to `Succeeded`, then `fetch_result()` reads rows.

## 8. Estimated scope

| Module | LoC est | Risk |
|---|---:|---|
| `client/transport.rs` (+`submit_statement`, +`fetch_completed_result`, +`SubmitResult`) | ~40 | Low |
| `client/sea.rs` (impls, factor `submit_response_to_error`) | ~120 | Medium — reuse polling-loop error mapping |
| `statement/executed_async.rs` (bodies) | ~200 | Medium |
| `statement/mutable.rs` (`submit`, extract `build_execute_params`) | ~80 | Low |
| `statement_status` conversion | ~50 | Low |
| `result_stream.rs` (`into_owned_batches` + `OwnedBatchIter`) | ~60 | Low |
| Per-method tracing spans (small touches across statement/) | ~20 | Low |
| Unit tests | ~150 | Low |
| `tests/v0_async_execute_wiremock.rs` | ~350 | Medium — multi-stub sequencing |
| `tests/v0_async_execute_e2e.rs` | ~200 | Low |
| **Total** | **~1,300** | |

Smaller than PR 4 (~2,500 LOC) because the heavy lifting (`Transport`,
`ResultStream`, `Row`, params, validity, close semantics,
`MidExecuteCancelGuard`) is already in place. PR 5 is mostly wiring.

## 9. Decisions captured

- **New `Transport::submit_statement` + `fetch_completed_result`**
  instead of broadening `execute_statement`'s contract. The poll-or-
  not split lives at the transport boundary; the public
  `Statement::execute` / `Statement::submit` are thin layers above.
- **`SubmitResult.status` is the SEA wire enum, not the public one.**
  Public conversion happens at the `ExecutedAsyncStatement` boundary.
  Keeps internal types from leaking through.
- **`fetch_result()` returns `InvalidState`** for non-terminal
  statements until the `FailedPrecondition` taxonomy expansion lands
  (spec A8). Matches the existing PR 4 placeholder choice.
- **`Cancelled` maps to `Error::invalid_state` with "cancelled" in
  the message** until a dedicated `ErrorCode::Cancelled` lands
  (spec A8 follow-up). The existing `wait_for_completion` already
  uses this; we stay consistent.
- **`into_owned_batches` consumes `self`** (not `&mut self`). The
  spec mentions this in the result-stream addendum. Consistent with
  how `close()` already consumes `self`.
- **Mid-submit cancel** (drop guard) reuses
  `MidExecuteCancelState`/`Guard` from PR 4 with the same caveat: the
  guard only covers the post-success / pre-defuse window because the
  transport doesn't surface the statement_id until the wire call
  returns.

## 10. §7 follow-ups closed by this PR

From `docs/pr-4-plan.md` §7:
- **`ResultStream::into_owned_batches`** — closed in §6.1.
- **Per-method `tracing::Span` on Statement::* / ExecutedStatement::***
  — closed in §6.2 (this also closes the last bit of the PR 2
  follow-up about per-method spans).

From `docs/migration-plan.md` §7:
- **`Statement::submit` body** — primary deliverable.
- **`ExecutedAsyncStatement` bodies** — primary deliverable.

## 11. §7 follow-ups still open after PR 5

- **Pre-statement_id mid-execute cancel**: see §6.3. v0.1.
- **`ErrorCode` taxonomy expansion** (`FailedPrecondition`,
  `Cancelled`): spec A8 follow-up. Until it lands, PR 5 uses
  `InvalidState` for non-terminal-fetch and "cancelled" message
  string for Cancelled.

## 12. What's NOT in PR 5

- C ABI exports — PR 6.
- Legacy ADBC delete — PR 7.
- True multi-batch async-aware `ResultStream::next_batch` — the v0
  reader path uses `block_on` internally for CloudFetch, which is
  acceptable for the kernel's process-wide runtime model and good
  enough for v0. Revisit once we have a benchmark showing the
  `block_on` is a real bottleneck.
