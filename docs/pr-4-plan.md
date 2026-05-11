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

# PR 4 — Statement + Results (core): detailed plan

> Plan-and-implementation doc. Plan written first; the rest of this PR
> implements it. Companion to the PR 4 contract in
> `docs/migration-plan.md` §3.

## 1. Goal

After PR 4, the sync execute path works end-to-end:

```rust
let mut stmt = session.statement();
stmt.spec()
    .sql("SELECT id, name FROM t WHERE id = ?")
    .param(1, TypedValue::Int64(42))?;
let mut exec = stmt.execute().await?;

while let Some(_batch) = exec.result_stream_mut().next_batch().await? {
    while let Some(row) = exec.result_stream_mut().next_row_in_batch() {
        let id   = row.get_i64(0)?.unwrap_or_default();
        let name = row.get_str(1)?.unwrap_or("");
        // …
    }
}
exec.close().await?;
```

This is the bundle: `Statement` + `ExecutedStatement` + the
`ResultStream` batch/row API + `TypedValue` + executed-handle cancel +
mid-execute cancel via future drop. They all touch
`src/statement/*.rs` and `src/result_stream.rs` and form one cohesive
state machine; splitting them across PRs would force reviewers to
re-derive the same design context.

## 2. Cancellation model

This deviates from the language used in `migration-plan.md` §3, which
predates spec addendum A1. **Spec A1 is canonical.** The kernel
exposes two cancellation surfaces:

1. **Mid-execute (before `execute()` returns).** The caller cancels by
   dropping the future — `tokio::select!`, `timeout()`, or just
   letting the task die. The future installs a `Drop` guard that
   fires-and-forgets `Transport::cancel_statement` against the
   in-flight `statement_id`, so the server stops charging DBUs even
   though no handle was ever returned to the caller.

2. **Post-execute (after the handle is in hand).** The caller calls
   `ExecutedStatement::cancel()` (via the `ExecutedStatementHandle`
   trait).

There is **no `Statement::cancel`**. Adding one would force
`Statement::execute(&self)` + interior mutability, which buys nothing
because there is no other source of "the in-flight `statement_id`"
than the running future itself — and the future already owns the
drop-guard path. `Statement::execute` takes `&mut self` per spec A1
line 581-582; the borrow checker prevents a sibling caller from
reaching for `cancel()` anyway.

## 3. Module-by-module work

### 3.1 `src/statement/spec.rs` — `TypedValue` variants

Today `TypedValue` is `#[non_exhaustive]` with only `Null`. PR 4 adds
the full set the kernel supports:

```rust
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum TypedValue {
    Null,
    Bool(bool),
    Int8(i8),  Int16(i16),  Int32(i32),  Int64(i64),
    UInt8(u8), UInt16(u16), UInt32(u32), UInt64(u64),
    Float32(f32), Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Decimal { value: String, precision: u8, scale: i8 },
    Date(chrono::NaiveDate),
    Timestamp(chrono::NaiveDateTime),
    Interval(String),
}
```

`#[non_exhaustive]` is preserved so v1+ variants stay non-breaking.
`UInt*` are included even though Databricks SQL has no unsigned types
— the converter (§3.2) promotes them to the next wider signed type,
mirroring `arrow_type_to_databricks`.

`Decimal::value` is a string (canonical decimal form: `"-123.456"`) so
the variant doesn't pull in `rust_decimal` as a public dependency and
wrappers can pass arbitrarily-precise values without an
intermediate type. `Interval` is also a string (`"INTERVAL '1' DAY"`
form) — the SEA wire format expects it that way.

### 3.2 `src/statement/params.rs` — `TypedValue` → SEA

Add direct converters that produce `StatementParameter` without
round-tripping through Arrow:

```rust
pub(crate) fn typed_value_to_sea_parameter(
    ordinal: i32, value: &TypedValue,
) -> StatementParameter;

pub(crate) fn typed_value_to_named_sea_parameter(
    name: &str, value: &TypedValue,
) -> StatementParameter;
```

Each `TypedValue` variant maps to a specific Databricks SQL type
string and a wire value (`None` for `Null`, otherwise the canonical
string form). The same `format_decimal128`-style helpers used by
`record_batch_to_sea_parameters` aren't needed here — values are
already typed.

The legacy `record_batch_to_sea_parameters` stays put for the ADBC
`bind` / `bind_stream` paths; deletion lands in PR 7.

### 3.3 `src/statement/mutable.rs` — `Statement::execute`

```rust
pub async fn execute(&mut self) -> Result<ExecutedStatement> {
    let _g = self.session.log_span().clone().entered();

    // 1. Auto-cancel + close any prior handle.
    if let Some(prior) = self.last_statement_id.take() {
        if let Some(valid) = self.last_valid.take() {
            valid.invalidate();
        }
        let _ = self.session.transport().cancel_statement(&prior).await;
        let _ = self.session.transport().close_statement(&prior).await;
    }

    // 2. Validate the spec.
    let sql = self.spec.sql.as_deref().ok_or_else(|| {
        Error::invalid_argument().message("StatementSpec.sql must be set before execute()")
    })?;
    if self.spec.result_format != ResultFormat::Arrow {
        return Err(Error::invalid_argument()
            .message("v0 supports ResultFormat::Arrow only"));
    }

    // 3. Build ExecuteParams from spec.
    let exec_params = build_execute_params(&self.spec)?;

    // 4. Drop-guard: install fire-and-forget cancel against any
    //    statement_id observed before the future resolves.
    let observed_id: Arc<OnceLock<String>> = Arc::new(OnceLock::new());
    let guard = MidExecuteGuard::new(
        Arc::clone(&self.session),
        Arc::clone(&observed_id),
    );

    // 5. Drive the wire call. SeaRestTransport::execute_statement
    //    already polls to completion (wait_timeout 10s default,
    //    then GetStatementStatus loop).
    let result = self.session
        .transport()
        .execute_statement(self.session.session_id(), sql, &exec_params)
        .await?;

    // 6. Cancel guard armed it; defuse on success.
    let _ = observed_id.set(result.statement_id.clone());
    guard.defuse();

    // 7. Build ResultStream + ExecutedStatement.
    let valid = ValidityFlag::new();
    self.last_statement_id = Some(result.statement_id.clone());
    self.last_valid = Some(valid.clone());

    let result_stream = ResultStream::from_reader(result.reader)?;
    Ok(ExecutedStatement::new(
        Arc::clone(&self.session),
        result.statement_id,
        valid,
        result_stream,
    ))
}
```

`build_execute_params` converts `StatementSpec.positional_params /
named_params / statement_conf` into the wire `ExecuteParams`. Sets
`wait_timeout` to `"10s"` to match the migration plan.

**Drop guard.** `MidExecuteGuard` is a small RAII type that, on
`Drop`, reads `observed_id` and (if `Some`) spawns a fire-and-forget
`cancel_statement` on the runtime. `defuse()` flips an internal flag
so the success path doesn't double-cancel. The future records the
statement_id into the guard before the await on the response (so
even a future dropped *between* the wire call and the success path
fires cleanup).

But the transport's `execute_statement` doesn't expose the
statement_id until it returns successfully. So in v0 the guard only
arms post-success and pre-`defuse`. That's a narrow race; full
mid-execute cancel covering the *send-statement* phase needs
transport callback hooks and is tracked as a §7 follow-up.

### 3.4 `src/statement/executed.rs` — `cancel` / `close` / `Drop`

```rust
impl ExecutedStatementHandle for ExecutedStatement {
    fn statement_id(&self) -> &str { &self.statement_id }

    async fn cancel(&self) -> Result<()> {
        self.valid.check()?;
        self.session.transport().cancel_statement(&self.statement_id).await
    }

    async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // already closed, idempotent
        }
        self.session.transport().close_statement(&self.statement_id).await
    }
}

impl Drop for ExecutedStatement {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) { return; }
        let transport = Arc::clone(&self.session.transport());
        let runtime = self.session.runtime_handle().clone();
        let stmt_id = self.statement_id.clone();
        runtime.spawn(async move {
            let _ = transport.close_statement(&stmt_id).await;
        });
    }
}
```

`closed: AtomicBool` is a new field on `ExecutedStatement` for idempotent close.

Validity-flag check on `cancel` short-circuits zombie calls
locally — once `Statement::execute` re-ran or `Statement` dropped, the
flag is `false` and we return `InvalidStatementHandle` without hitting
the wire.

### 3.5 `src/result_stream.rs` — `Row<'a>` + accessors + row cursor

The current `Row<'a>` is `_phantom: PhantomData<&'a ()>` and every
accessor `unimplemented!("PR 5")`. PR 4 fills them.

```rust
pub struct Row<'a> {
    batch: &'a RecordBatch,
    row_idx: usize,
}
```

`ResultStream` gains `current_row_idx: usize` reset to `0` in
`next_batch`. `next_row_in_batch` returns `Some(Row { batch, row_idx
})` when `row_idx < batch.num_rows()` and `None` past the end. The
borrow against the stream's `current` batch is what keeps `Row<'a>`'s
lifetime tied to the parent.

Accessor surface (lifted from migration-plan §3 PR 4):

| Method | Returns | Arrow types accepted |
|---|---|---|
| `get_str` | `Option<&'a str>` | `Utf8`, `LargeUtf8` |
| `get_i64` | `Option<i64>` | `Int64`, widens `Int8`/`Int16`/`Int32` |
| `get_i32` | `Option<i32>` | `Int32`, widens `Int8`/`Int16` |
| `get_i16` | `Option<i16>` | `Int16`, widens `Int8` |
| `get_i8` | `Option<i8>` | `Int8` |
| `get_u64`/`u32`/`u16`/`u8` | `Option<uN>` | exact match (+ widening for the smaller types) |
| `get_f64` | `Option<f64>` | `Float64`, widens `Float32` |
| `get_f32` | `Option<f32>` | `Float32` |
| `get_bool` | `Option<bool>` | `Boolean` |
| `get_bytes` | `Option<&'a [u8]>` | `Binary`, `LargeBinary` |
| `get_decimal` | `Option<String>` | `Decimal128(p, s)` — formatted canonically |
| `get_date` | `Option<chrono::NaiveDate>` | `Date32`, `Date64` |
| `get_timestamp` | `Option<chrono::NaiveDateTime>` | `Timestamp(Second/Milli/Micro/Nano, _)` |
| `get_interval` | `Option<String>` | `Interval(*)` — canonical SQL form |

Plus `num_columns(&self) -> usize`. NULL handling: every accessor
returns `Ok(None)` for `array.is_null(row_idx)`. Type mismatch:
`Err(InvalidArgument)` naming the column index and observed
Arrow type.

Hand-rolling 14 accessors invites inconsistency. We use a small macro
(`primitive_getter!`) for the integer/float accessors keyed on Arrow
primitive type + Rust output type. String / binary / decimal / date /
timestamp / interval are bespoke.

### 3.6 `src/result_stream.rs` — wire `into_owned_batches` adapter (deferred)

The migration plan calls for an `into_owned_batches(self) -> impl
Iterator<Item = Result<RecordBatch>>`. PR 4 **defers** this to PR 5 —
the C ABI in PR 6 is the consumer, and PR 5's full-async work touches
the same surface. Tracked in §7.

### 3.7 Doc-comment cleanup

The skeleton's `unimplemented!("PR 5: …")` / `("PR 6: …")` strings
were authored against the original 12-PR numbering before the plan
consolidated to 7 PRs. Several breadcrumbs across `statement/*` and
`result_stream.rs` reference "PR 5" or "PR 6" for things that now ship
in PR 4. This PR drops them.

`#![allow(dead_code, unused_variables)]` at the file level in
`statement/executed.rs` and `statement/mutable.rs` comes off — bodies
now exist.

## 4. Tests

### Unit

- `statement::spec::tests` — already covers basic spec setters; add
  per-`TypedValue`-variant identity.
- `statement::params::tests` — add `typed_value_to_sea_parameter`
  cases per variant (NULL → `value: None`, primitives → expected
  type-name + string form, Decimal/Date/Timestamp/Interval canonical
  forms).
- `result_stream::tests` (new) — per-Arrow-type accessor against a
  synthetic `RecordBatch`. NULL handling. Type-mismatch errors.
  `next_row_in_batch` walks all rows, returns `None` at end. `Row`
  invalidation when `next_batch` is called.

### Wiremock (`tests/v0_execute_wiremock.rs`, new)

- SUCCEEDED inline path: `SELECT 1` → expect one batch with one
  Int64 cell == 1.
- RUNNING → SUCCEEDED poll path: first execute response is
  `RUNNING`; subsequent `GetStatementStatus` returns `SUCCEEDED`.
- FAILED → `Error.code == SqlError`, `sql_state` populated from the
  service error envelope.
- Parameterised positional: `SELECT ?, ?` with mixed `TypedValue`s.
- Parameterised named: `SELECT :a, :b`.
- Re-execute invalidates prior handle: call `stmt.execute()` twice,
  assert the first `ExecutedStatement::cancel()` returns
  `InvalidStatementHandle`.
- Drop-future fires cancel: kick off `execute()`, drop the future
  mid-flight, assert the mock observed a `POST /statements/{id}/cancel`.
  (Stretch; gated on whether wiremock can observe the cancel before
  the test process exits.)

### E2E (`tests/v0_execute_e2e.rs`, new; `#[ignore]`)

- `SELECT 1` round-trip.
- Multi-batch CloudFetch-driven `SELECT * FROM range(1_000_000)`.
- Parameterised `SELECT ?, ?, ?` of mixed `TypedValue` types.
- Error path: `SELECT * FROM does_not_exist` returns
  `ErrorCode::SqlError`.
- Re-execute on the same `Statement` → prior handle's `cancel()`
  returns `InvalidStatementHandle`.
- E2E cancellation: long-running `SELECT * FROM range(1e10)` in one
  task, `executed.cancel().await` from another; verify the kernel
  surfaces a `Cancelled` error.
- Type round-trip: for each `TypedValue` variant, `SELECT ?` with
  that variant bound, assert the returned row's accessor produces an
  equivalent value (where `equivalent` is type-specific: exact for
  integers / bool / string; approximate for floats; canonical-form
  match for decimal / date / timestamp / interval).

## 5. Estimated scope

| Module | LoC est | Risk |
|---|---:|---|
| `statement/spec.rs` (TypedValue) | ~80 | Low |
| `statement/params.rs` (TypedValue → SEA) | ~150 | Low |
| `statement/mutable.rs` (execute body + drop guard) | ~150 | Medium |
| `statement/executed.rs` (cancel / close / Drop) | ~80 | Low |
| `result_stream.rs` (Row + accessors + cursor) | ~400 | Medium |
| Unit tests | ~300 | Low |
| `tests/v0_execute_wiremock.rs` | ~400 | Medium |
| `tests/v0_execute_e2e.rs` | ~250 | Low |
| **Total** | **~1,800** | |

Smaller than PR 3 (~3,600 with the JDBC-reshape draft, ~2,400 after
the pivot). The complexity sits in the `Row<'a>` accessor table and
the drop-guard race window on `execute()`.

## 6. Decisions captured

- **`Statement::execute(&mut self)`** — per spec A1, not the
  `Arc<Mutex>` + `&self` form the migration plan §3 sketched.
  The migration plan predates A1. No `Statement::cancel`; mid-execute
  cancellation is "drop the future"; post-execute is on the handle.
- **Drop-guard scope** — guards against post-success / pre-defuse
  drops. Earlier-in-the-call drops are best-effort because the
  transport doesn't surface the assigned `statement_id` until it
  returns. Closing the rest of the gap needs a transport callback
  hook; tracked as §7 follow-up.
- **`TypedValue::Decimal::value: String`** keeps the kernel's
  public surface free of a `rust_decimal` dependency.
- **`UInt*` variants** are accepted at the public API but promoted
  to the next wider signed type at the SEA boundary, mirroring
  `arrow_type_to_databricks`.
- **`into_owned_batches`** deferred to PR 5 — C ABI in PR 6 is the
  only intended consumer and PR 5's async work touches the same
  surface.

## 7. §7 follow-ups added (logged in plan)

- **Mid-execute cancellation before server returns statement_id.**
  Requires a transport callback hook so the in-flight future learns
  the statement_id as soon as the server assigns it (rather than only
  on response). Tracked for v0.1.
- **`ResultStream::into_owned_batches`** — owned-batch adapter for
  PyO3 / napi-rs / C-ABI consumers. PR 5.
- **Per-method `tracing::Span::enter` on `Statement::*` /
  `ExecutedStatement::*`** — closes the last part of the PR 2 §7
  follow-up.

## 8. What's NOT in PR 4

- `Statement::submit` / `ExecutedAsyncStatement::*` — still
  `unimplemented!()`. PR 5.
- True multi-batch async-aware `ResultStream::next_batch` — v0 today
  drives via the sync `ResultReader::next_batch` (which `block_on`s
  internally for CloudFetch). PR 5 generalises.
- C ABI exports — PR 6.
- Owned-batch adapter (`into_owned_batches`) — PR 5.
