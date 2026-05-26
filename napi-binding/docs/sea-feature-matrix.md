

# SEA Feature Matrix — Kernel / pyo3 / Python Connector / NodeJS

Generated: 2026-05-25 (updated from live code at tips + 6 SEA-implementation PRs landing)

Sources verified at:


| Source                              | Branch / state                         | Tip commit                              |
| ----------------------------------- | -------------------------------------- | --------------------------------------- |
| Kernel main                         | `main`                                 | `5ff2bc2` (`TIMESTAMP_NTZ`/`LTZ` types) |
| Kernel napi binding (merged PR #25) | `msrathore/kernel-napi-classes`        | `e684c4c`                               |
| Kernel napi metadata (open PR)      | `msrathore/krn-napi-metadata-exposure` | `ba6bed1`                               |
| Kernel napi maxConnections (PR #74) | `msrathore/krn-max-connections`        | `0c5ca3f`                               |
| Kernel napi ExecuteOptions (PR #75) | `msrathore/krn-statement-options`      | `9fbc009`                               |
| Kernel napi async execute (PR #76)  | `msrathore/krn-async-execute`          | `48e4714`                               |
| Python connector                    | `databricks-sql-python-repro` main     | `fb55001c`                              |
| NodeJS SEA — merged stack tip       | `sea-operation` worktree               | `1f914fd`                               |
| NodeJS SEA metadata (open PR)       | `sea-execution-metadata` worktree      | `f8a86d5`                               |
| NodeJS SEA auth/OAuth (open PR)     | `sea-auth-u2m` worktree                | `bafd8b9`                               |
| NodeJS SEA maxConnections (PR #392) | `msrathore/sea-max-connections`        | `c213241`                               |
| NodeJS SEA statement-opts (PR #393) | `msrathore/sea-statement-options`      | `8c5618c`                               |
| NodeJS SEA async-execute (PR #394)  | `msrathore/sea-async-execute`          | `ca04a30`                               |
| NodeJS Thrift                       | `databricks-sql-nodejs` main           | —                                       |


**Python connector architecture note:** The connector has two SEA paths.  
`use_sea=True` (default) — native SEA HTTP stack, raw SQL for metadata.  
`use_kernel=True` — routes through the Rust kernel via pyo3 (`KernelDatabricksClient`).  
The two paths are mutually exclusive (`use_kernel and use_sea` raises at construction).  
The matrix distinguishes them where coverage differs.

**NodeJS SEA branch architecture note:** The PR stack is a linear chain merged up through `sea-operation`. Two follow-on open PRs are independent: `sea-execution-metadata` (9 metadata methods) and `sea-auth-u2m` (OAuth M2M + U2M). Neither is merged yet.

Legend: **Y** = implemented/merged, **PR** = implemented in open PR (not yet merged), **N** = not implemented

---

## Quick-reference — implementations landing in this PR round (2026-05-25)

Six feature PRs land the implementation work; PR #77 (this doc) records the status delta and 3 matrix corrections.

| Feature           | Status      | Kernel PR | NodeJS PR | Test coverage                                      |
| ----------------- | ----------- | --------- | --------- | -------------------------------------------------- |
| F1 maxConnections | Implemented | [#74](https://github.com/databricks/databricks-sql-kernel/pull/74) | [#392](https://github.com/databricks/databricks-sql-nodejs/pull/392) | 9 unit + 3 live e2e (pecotesting) |
| F2 Arrow complex types (not JSON) | Locked (kernel default) | (default, no kernel change) | [#392](https://github.com/databricks/databricks-sql-nodejs/pull/392) | 1 live e2e (pecotesting) |
| F3 statement conf map | Implemented (napi) | [#75](https://github.com/databricks/databricks-sql-kernel/pull/75) | [#393](https://github.com/databricks/databricks-sql-nodejs/pull/393) | 8 Rust unit + 8 NodeJS unit + 4 live e2e |
| F3 query tags | Implemented (byte-identical Thrift wire shape) | [#75](https://github.com/databricks/databricks-sql-kernel/pull/75) | [#393](https://github.com/databricks/databricks-sql-nodejs/pull/393) | 5 Rust parity tests + 4 live e2e |
| F3b async execute (submit + poll + await_result) | Implemented (napi surface only — facade integration deferred) | [#76](https://github.com/databricks/databricks-sql-kernel/pull/76) | [#394](https://github.com/databricks/databricks-sql-nodejs/pull/394) | 3 live e2e |
| F4 fetchAll (drain to rows) | Verified zero-code-change | (n/a) | [#393](https://github.com/databricks/databricks-sql-nodejs/pull/393) | 5 live e2e (pecotesting) |

**Total live e2e: 16/16 against pecotesting (independently reproduced by DA in own check-worktree).**

### Comparison to Python (reference implementation)

| Feature | Python `use_sea=True` | Python `use_kernel=True` (pyo3) | NodeJS SEA (this round) |
|---|---|---|---|
| `max_connections` | `max_connections=` kwarg → urllib3 `maxsize` | inherited via kernel | `maxConnections` (SEA-only; Thrift gap) |
| Complex types as Arrow | `_use_arrow_native_complex_types=True` (default) | inherited via kernel | kernel default `complex_types_as_json=false` |
| Statement conf + query tags | `statement_conf=` / `query_tags=` | `query_tags` raises `NotSupportedError` | napi `ExecuteOptions.statementConf` / `queryTags`; **public NodeJS `ExecuteStatementOptions` follow-up needed** (see Statement conf map row note in §3) |
| Async execute | `async_op=True` (submit + poll loop) | `submit()` + `await_result()` | napi `AsyncStatement` + `AsyncResultHandle`; **`SeaAsyncOperationBackend` facade-level integration is the next increment** |
| `fetchall_arrow()` | yes | yes | `DBSQLOperation.fetchAll` already routes through `SeaOperationBackend.fetchChunk` — no SEA-side adapter change needed; regression-locked in #393 |

### Deferred (need kernel changes — separate kernel PRs, not in this round)

- **Positional + named params** — needs JS↔napi `TypedValue` mapping. Python `use_sea=True` exposes via `TSparkParameter`; Thrift exposes too. Outside this PR scope.
- **Row limit** — needs kernel `StatementSpec.row_limit` field (currently only on internal `ExecuteParams`; see note 5).
- **`TIMESTAMP` / `TIMESTAMP_NTZ` / `TIMESTAMP_LTZ` / `VARIANT` params** — covered by positional-params follow-up. `TIMESTAMP_NTZ`/`LTZ`/`VARIANT` are also Thrift gaps per the parity rubric.
- **Max rows per fetch** — concept does not map cleanly between SEA Arrow-batch streaming and Thrift's direct-results first-batch hint (see note 6).
- **Wait timeout / `queryTimeout`** — Thrift exposes it (matrix correction below); SEA-side wait_timeout would be a separate config knob.

### Three matrix corrections in this PR

1. **Thrift `queryTimeout`** — matrix previously said N; Thrift code has `ExecuteStatementOptions.queryTimeout` at `lib/contracts/IDBSQLSession.ts:13`. Corrected to Y.
2. **Thrift `rowLimit`** — matrix previously said Y; Thrift `ExecuteStatementOptions` has no `rowLimit` field. Only way to cap rows is `LIMIT N` in SQL. Corrected to N.
3. **`maxRows per fetch` semantics** — Thrift's `maxRows` is a direct-results first-batch hint, NOT a per-fetch row cap (which is Python `use_sea`'s `max_rows=`). Same name, different semantics — clarified in note 6.

---

## 1. Metadata Methods

NodeJS column legend:

- **napi (merged)** = `msrathore/kernel-napi-classes` PR #25 — `Connection` has `executeStatement` + `close` only
- **napi (open PR)** = `msrathore/krn-napi-metadata-exposure` — adds 10 metadata methods to `Connection`
- **NodeJS SEA (merged)** = `sea-operation` worktree — all metadata stubs
- **NodeJS SEA (open PR)** = `sea-execution-metadata` worktree — 9 metadata methods wired


| Method                                       | Kernel Rust (main) | pyo3 (main) | Python `use_kernel=True` | Python `use_sea=True` | napi (merged) | napi (open PR) | NodeJS SEA (merged) | NodeJS SEA (open PR) | NodeJS Thrift |
| -------------------------------------------- | ------------------ | ----------- | ------------------------ | --------------------- | ------------- | -------------- | ------------------- | -------------------- | ------------- |
| `list_catalogs` / `getCatalogs`              | Y                  | Y           | Y (via pyo3)             | Y (raw SQL)           | N             | PR             | N (stub)            | PR                   | Y             |
| `list_schemas` / `getSchemas`                | Y                  | Y           | Y (via pyo3)             | Y (raw SQL)           | N             | PR             | N (stub)            | PR                   | Y             |
| `list_tables` / `getTables`                  | Y                  | Y           | Y (via pyo3, note 3)     | Y (raw SQL)           | N             | PR             | N (stub)            | PR                   | Y             |
| `list_columns` / `getColumns`                | Y                  | Y           | Y (via pyo3)             | Y (raw SQL)           | N             | PR             | N (stub)            | PR                   | Y             |
| `list_functions` / `getFunctions`            | Y                  | Y           | N (not on interface)     | N                     | N             | PR             | N (stub)            | PR                   | Y             |
| `list_procedures` / `getProcedures` (note 1) | Y                  | Y           | N                        | N                     | N             | N              | N (stub)            | N (note 1)           | N             |
| `list_table_types` / `getTableTypes`         | Y (static)         | Y           | N                        | N                     | N             | PR             | N (stub)            | PR                   | Y             |
| `list_type_info` / `getTypeInfo`             | Y (static)         | Y           | N                        | N                     | N             | PR             | N (stub)            | PR                   | Y             |
| `primary_keys` / `getPrimaryKeys`            | Y                  | Y           | N (not on interface)     | N                     | N             | PR             | N (stub)            | PR                   | Y             |
| `foreign_keys` / `getCrossReference`         | Y                  | Y           | N (not on interface)     | N                     | N             | PR             | N (stub)            | PR                   | Y             |


**Note 1 — procedures:** The kernel and pyo3 expose `procedures()`. The open napi PR (`msrathore/krn-napi-metadata-exposure`) does **not** include `listProcedures` — `SeaNativeConnection` in `SeaNativeLoader.ts` carries a NOTE comment that it is deferred because the napi binding doesn't surface it. `getInfo` is a permanent stub-throw in both merged and open NodeJS SEA paths: the kernel `Metadata` API has no equivalent. NodeJS Thrift also has no `getProcedures` — `TGetProceduresReq` is absent from the Databricks Thrift fork entirely.

**Note 2 — JDBC reshaping:** The kernel applies JDBC `DatabaseMetaData` column shapes for `catalogs`, `schemas`, `tables`, `columns`. `primary_keys`/`foreign_keys` pass server shape through unchanged. `table_types`/`type_info` are static in-memory (no wire call). `use_kernel=True` inherits this automatically; `use_sea=True` must normalize in Python (open normalization bugs in PRs #661/#662).

**Note 3 — `use_kernel` table_types filter:** `get_tables` with a non-empty `table_types` list drains the full kernel stream, applies a client-side `TABLE_TYPE` column filter (`ResultSetFilter._filter_arrow_table`), wraps in `_StaticArrowHandle`. Documented as a known phase-1 gap in `KernelDatabricksClient`'s module docstring.

---

## 2. Session / Connection Features


| Feature                                   | Kernel Rust        | pyo3                | Python `use_kernel=True`                            | Python `use_sea=True` | NodeJS SEA (merged) | NodeJS SEA (open PRs)                | NodeJS Thrift |
| ----------------------------------------- | ------------------ | ------------------- | --------------------------------------------------- | --------------------- | ------------------- | ------------------------------------ | ------------- |
| Open session                              | Y                  | Y                   | Y                                                   | Y                     | Y                   | Y                                    | Y             |
| Close session                             | Y                  | Y                   | Y                                                   | Y                     | Y                   | Y                                    | Y             |
| Initial catalog                           | Y                  | Y (`catalog=`)      | Y                                                   | Y                     | Y                   | Y                                    | Y             |
| Initial schema                            | Y                  | Y (`schema=`)       | Y                                                   | Y                     | Y                   | Y                                    | Y             |
| Session conf map                          | Y                  | Y (`session_conf=`) | Y (forwarded verbatim)                              | Y                     | Y (`sessionConf`)   | Y                                    | Y             |
| Auth — PAT                                | Y                  | Y                   | Y                                                   | Y                     | Y                   | Y                                    | Y             |
| Auth — OAuth M2M                          | Y (kernel)         | N (note 4)          | N (note 4)                                          | Y                     | N                   | PR (`sea-auth-u2m`, `OAuthM2m` mode) | Y             |
| Auth — OAuth U2M                          | Y (kernel)         | N (note 4)          | N (note 4)                                          | Y                     | N                   | PR (`sea-auth-u2m`, `OAuthU2m` mode) | Y             |
| Auth — Azure SP M2M                       | N                  | N                   | N                                                   | Y                     | N                   | N                                    | Y             |
| TLS config                                | Y                  | N (system roots)    | N (kernel manages TLS)                              | Y                     | N                   | N                                    | Y             |
| HTTP proxy                                | N                  | N                   | N (ignored)                                         | Y                     | N                   | N                                    | Y             |
| Custom HTTP headers                       | N                  | N                   | N (ignored)                                         | Y                     | N                   | N                                    | Y             |
| Connection pool (max_connections)         | Y (default 100)    | Y (inherited)       | Y (inherited)                                       | Y (`max_connections=`) | PR (#74 / #392)     | PR (#74 / #392)                      | N             |
| Complex types as Arrow (not JSON strings) | Y (post-processor) | N                   | Y (`_use_arrow_native_complex_types`, default True) | Y                     | Y (kernel default, e2e-locked PR #392) | Y (kernel default, e2e-locked PR #392) | Y             |


**Note 4 — pyo3 / use_kernel auth:** Both pyo3 and `KernelDatabricksClient.auth_bridge` accept only PAT today. Non-PAT raises `NotSupportedError` at session-open time. OAuth wiring is deferred pending the credential-ownership decision (kernel-owned vs connector-owned token refresh).

**Note 5 — row_limit (Section 3):** the kernel SEA wire request carries `row_limit` (see `src/types/sea.rs::ExecuteStatementRequest.row_limit`), but the public `StatementSpec` does not expose it — the field is populated only inside `prepare_for_dispatch` with no spec-fed input. Surfacing it through napi requires adding `StatementSpec.row_limit` first; tracked as a kernel-side follow-on. NodeJS Thrift's public `ExecuteStatementOptions` (`lib/contracts/IDBSQLSession.ts:7-30`) has NO `rowLimit` field — the only way to cap rows on the Thrift path today is `LIMIT N` in the SQL itself. This is a matrix correction (Thrift was previously marked `Y` for this row).

**Note 6 — max_rows_per_fetch (Section 3) — semantics divergence:** Thrift's `ExecuteStatementOptions.maxRows` is a **direct-results first-batch hint** (`getDirectResultsOptions(maxRows, ...)` populates the inline first-batch row count on `TExecuteStatementReq`). Python `use_sea`'s `max_rows=` is a **per-fetch row cap** governing how many rows each `fetch_many_arrow` call returns. The two are not the same feature despite sharing a name — Thrift's affects how much data the server inlines in the execute response (vs. CloudFetch external links); Python's affects pagination by the connector. The SEA path through the kernel does Arrow-batch streaming with server-driven batch sizing, so neither semantic maps cleanly to a single napi knob. Listed as "deferred — concept does not map cleanly" for NodeJS SEA. This is a matrix correction (the prior single-row treatment conflated the two semantics).

**Note 7 — Statement conf map + query tags napi-only scope (Section 3):** PR #75 (kernel napi `ExecuteOptions.statementConf`) + PR #393 (NodeJS adapter wiring) plumb the field through `SeaNativeLoader → Connection.executeStatement(sql, options?)` so the SEA execution path honors `statementConf` end-to-end. **However, the public NodeJS surface `ExecuteStatementOptions` (`lib/contracts/IDBSQLSession.ts:7-30`) does NOT yet expose `statementConf` to user code** — it remains internal-only for now. Wiring it to the public API is a follow-up increment after these PRs land. Query tags follow the same shape: the wire-level `serializeQueryTags`-byte-identical encoding is correct at the napi layer; surfacing as `executeStatement(sql, { queryTags: {...} })` on the public NodeJS API is the same follow-up. This is the scope-down DA flagged on PR #77 round-3 review.

---

## 3. Statement / Execution Features


| Feature                                    | Kernel Rust        | pyo3                              | Python `use_kernel=True`                    | Python `use_sea=True` | NodeJS SEA (merged) | NodeJS SEA (open PRs)   | NodeJS Thrift   |
| ------------------------------------------ | ------------------ | --------------------------------- | ------------------------------------------- | --------------------- | ------------------- | ----------------------- | --------------- |
| Sync execute                               | Y                  | Y                                 | Y                                           | Y                     | Y                   | Y                       | Y               |
| Async execute (submit + poll)              | Y                  | Y (`submit()` + `await_result()`) | Y (`async_op=True`)                         | Y (`async_op=True`)   | PR (#76 / #394 — napi `AsyncStatement` + `AsyncResultHandle`) | PR (#76 / #394) | Y               |
| Cancel                                     | Y                  | Y                                 | Y                                           | Y                     | Y                   | Y                       | Y               |
| Close statement                            | Y                  | Y                                 | Y                                           | Y                     | Y                   | Y                       | Y               |
| Positional params — basic types            | Y                  | Y (`bind_param`)                  | Y (TSparkParameter)                         | Y (TSparkParameter)   | N                   | N (deferred)            | Y               |
| Param type: TIMESTAMP                      | Y                  | Y                                 | Y                                           | Y                     | N                   | N                       | Y               |
| Param type: TIMESTAMP_NTZ                  | Y (`5ff2bc2`)      | Y (`5ff2bc2`)                     | Y                                           | Y                     | N                   | N                       | N               |
| Param type: TIMESTAMP_LTZ                  | Y (`5ff2bc2`)      | Y (`5ff2bc2`)                     | Y                                           | Y                     | N                   | N                       | N               |
| Param type: VARIANT                        | Y (`480011c`)      | N (not in pyo3 params.rs)         | N                                           | N                     | N                   | N                       | N               |
| Named params                               | N                  | N                                 | N                                           | N                     | N                   | N                       | N               |
| Compound types (ARRAY/MAP/STRUCT as Arrow) | Y (post-processor) | N                                 | Y (`_use_arrow_native_complex_types`)       | Y                     | Y (kernel default, e2e-locked PR #392) | Y                       | Y               |
| Row limit                                  | Y (kernel `ExecuteParams.row_limit`, not on `StatementSpec`) | N | N                                           | Y (`row_limit=`)      | N (deferred — needs `StatementSpec.row_limit` kernel addition, see note 5) | N (deferred) | N (no public surface beyond `LIMIT N` in SQL — matrix correction) |
| Max rows per fetch                         | Y (server-driven batch sizing, see note 6) | N                | N                                           | Y (`max_rows=`)       | N (concept does not map cleanly — see note 6) | N | Y (`maxRows` on `ExecuteStatementOptions` — note 6: this is "direct-results first-batch hint" not "per-fetch row cap", semantics differ from Python `use_sea`) |
| Wait timeout / queryTimeout                | Y                  | N                                 | N                                           | Y                     | N                   | N (explicitly rejected) | Y (`queryTimeout` on `ExecuteStatementOptions` — matrix correction; was previously N)               |
| Statement ID access                        | Y                  | Y                                 | Y                                           | Y                     | Y                   | Y                       | Y               |
| Statement conf map                         | Y                  | N                                 | N (`query_tags` raises `NotSupportedError`) | Y                     | PR (#75 / #393 — `ExecuteOptions.statementConf` at napi level only; **not exposed on public NodeJS `ExecuteStatementOptions` — follow-up needed to wire to public API**, see note 7) | PR (#75 / #393, napi-only)         | Y               |
| Query tags                                 | N/A (via `statement_conf["query_tags"]`) | N/A    | N (`NotSupportedError`)                     | Y                     | PR (#75 / #393 — byte-identical Thrift `serializeQueryTags` wire shape; napi-only surface, same caveat as Statement conf map row) | PR (#75 / #393, napi-only) | Y               |


---

## 4. Result / Fetch Features


| Feature                                             | Kernel Rust | pyo3                     | Python `use_kernel=True` | Python `use_sea=True`  | NodeJS SEA (merged)        | NodeJS Thrift |
| --------------------------------------------------- | ----------- | ------------------------ | ------------------------ | ---------------------- | -------------------------- | ------------- |
| Arrow result (inline)                               | Y           | Y                        | Y                        | Y                      | Y                          | Y             |
| CloudFetch (external links)                         | Y           | Y                        | Y                        | Y                      | Y                          | Y             |
| Schema access                                       | Y           | Y (`arrow_schema()`)     | Y                        | Y                      | Y (`schema()` → IPC bytes) | Y             |
| Fetch next batch                                    | Y           | Y (`fetch_next_batch()`) | Y                        | Y                      | Y (`fetchNextBatch()`)     | Y             |
| Fetch all (drain to table)                          | Y           | Y (`fetch_all_arrow()`)  | Y                        | Y (`fetchall_arrow()`) | Y (e2e-locked PR #393 — facade `DBSQLOperation.fetchAll` already routes through `SeaOperationBackend.fetchChunk`, no SEA-side code change needed) | Y             |
| Result format: Arrow IPC bytes (napi)               | N/A         | N/A                      | N/A                      | N/A                    | Y                          | N/A           |
| Result format: pyarrow zero-copy (C Data Interface) | N/A         | Y (via `ToPyArrow`)      | Y                        | Y                      | N/A                        | N/A           |
| Chunk-level retries (CloudFetch)                    | Y           | Y                        | Y                        | Y                      | Y                          | Y             |


---

## 5. Error Handling


| Feature                 | Kernel Rust          | pyo3                           | Python `use_kernel=True`                 | Python `use_sea=True`      | NodeJS SEA (merged)                    | NodeJS Thrift       |
| ----------------------- | -------------------- | ------------------------------ | ---------------------------------------- | -------------------------- | -------------------------------------- | ------------------- |
| Typed error codes       | Y (`ErrorCode` enum) | Y (`KernelError` Python class) | Y (→ PEP 249 via `reraise_kernel_error`) | Y (`ServerOperationError`) | Y (JSON sentinel + JS class hierarchy) | Y (HiveDriverError) |
| SQLSTATE on errors      | Y                    | Y                              | Y                                        | Y                          | Y (via kernel)                         | Y                   |
| Error message chaining  | Y                    | Y                              | Y                                        | Y                          | Y                                      | Y                   |
| Closed-handle detection | Y                    | Y                              | Y                                        | Y                          | Y                                      | Y                   |


---

## 6. Key Gaps Summary

### Python connector `use_kernel=True`

- Only 4 of 10 metadata methods wired — `DatabricksClient` interface defines only `get_catalogs`, `get_schemas`, `get_tables`, `get_columns`; the other 6 are not on the interface
- `get_tables` with `table_types` uses client-side filter (drains full stream to pyarrow Table)
- PAT-only auth — `auth_bridge.py` raises `NotSupportedError` for OAuth
- `query_tags`, row_limit, max_rows, wait_timeout, statement_conf not surfaced
- `VARIANT` TypedValue variant not supported in pyo3 `params.rs` (merged to kernel main but not pyo3)

### Python connector `use_sea=True` vs `use_kernel=True`

- Richer auth (M2M, U2M, Azure SP); `use_kernel=True` is PAT-only
- Raw SQL metadata requiring Python normalization; `use_kernel=True` gets JDBC shapes from Rust
- Exposes row_limit, max_rows, wait_timeout, statement_conf, query_tags, HTTP proxy

### NodeJS SEA — merged vs open PRs

- **Merged** (`sea-operation` tip): `executeStatement` + `close` only; all metadata throw stubs; PAT auth only
- **Open `sea-execution-metadata`**: 9 metadata methods wired; async execution + params still deferred; `getProcedures`/`getInfo` permanently absent
- **Open `sea-auth-u2m`**: adds `OAuthM2m` + `OAuthU2m` discriminated auth modes; independent of metadata PR
- **Open `sea-max-connections` (PR #392)**: maxConnections (SEA-only extension; Thrift gap), Arrow-native complex types lock-in (kernel default already; e2e regression-locked)
- **Open `sea-statement-options` (PR #393)**: `statementConf` + `queryTags` (byte-identical Thrift `serializeQueryTags` wire shape); `fetchAll` regression-lock (zero adapter change; facade already routes through SeaOperationBackend)
- **Open `sea-async-execute` (PR #394)**: napi `AsyncStatement` + `AsyncResultHandle` + `submitStatement`; live e2e for submit→awaitResult drain, status() enum, cancel-while-pending

### NodeJS SEA vs Thrift permanent gaps

- `getInfo` — kernel `Metadata` API has no equivalent; permanently absent from SEA path
- `getProcedures` — napi binding does not surface it; SEA path cannot wire it until napi PR adds it
- Azure SP M2M auth — kernel implements PAT + OAuth M2M (generic OIDC) + OAuth U2M; Azure SP is connector-level logic not implemented in kernel

### NodeJS SEA — still deferred after PRs #392/#393/#394 land

- Positional / named parameters: needs JS↔napi `TypedValue` mapping (separate kernel + napi PR)
- Row limit: needs kernel `StatementSpec.row_limit` field (see note 5; currently only on internal `ExecuteParams`)
- TIMESTAMP / TIMESTAMP_NTZ / TIMESTAMP_LTZ / VARIANT params: covered by positional-params follow-up (TIMESTAMP_NTZ/LTZ/VARIANT are also Thrift gaps per the rubric)
- Max rows per fetch: concept does not map cleanly between SEA Arrow-batch streaming and Thrift's direct-results first-batch hint (see note 6)

---

## Source Locations


| Component                                     | Path                                                                                                     |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Kernel metadata API                           | `/home/madhavendra.rathore/databricks-sql-kernel-sea-WT/napi-binding/src/metadata_v0.rs`                 |
| pyo3 metadata binding                         | `/home/madhavendra.rathore/databricks-sql-kernel-sea-WT/napi-binding/pyo3/src/metadata.rs`               |
| pyo3 session binding                          | `/home/madhavendra.rathore/databricks-sql-kernel-sea-WT/napi-binding/pyo3/src/session.rs`                |
| pyo3 params (TypedValue)                      | `/home/madhavendra.rathore/databricks-sql-kernel-sea-WT/napi-binding/pyo3/src/params.rs`                 |
| napi Connection (merged PR #25)               | `/home/madhavendra.rathore/databricks-sql-kernel-sea-WT/napi-binding/napi/src/connection.rs`             |
| napi TypeScript types (merged)                | `/home/madhavendra.rathore/databricks-sql-kernel-sea-WT/napi-binding/napi/index.d.ts`                    |
| napi metadata (open PR branch)                | `msrathore/krn-napi-metadata-exposure` in kernel repo                                                    |
| Python connector kernel client                | `/home/madhavendra.rathore/databricks-sql-python-repro/src/databricks/sql/backend/kernel/client.py`      |
| Python connector auth bridge                  | `/home/madhavendra.rathore/databricks-sql-python-repro/src/databricks/sql/backend/kernel/auth_bridge.py` |
| Python connector SEA backend                  | `/home/madhavendra.rathore/databricks-sql-python-repro/src/databricks/sql/backend/sea/backend.py`        |
| Python connector backend interface            | `/home/madhavendra.rathore/databricks-sql-python-repro/src/databricks/sql/backend/databricks_client.py`  |
| NodeJS SEA session backend (merged)           | `/home/madhavendra.rathore/databricks-sql-nodejs-sea-WT/operation/lib/sea/SeaSessionBackend.ts`          |
| NodeJS SEA session backend (open metadata PR) | `/home/madhavendra.rathore/databricks-sql-nodejs-sea-WT/execution-metadata/lib/sea/SeaSessionBackend.ts` |
| NodeJS SEA native loader (open metadata PR)   | `/home/madhavendra.rathore/databricks-sql-nodejs-sea-WT/execution-metadata/lib/sea/SeaNativeLoader.ts`   |
| NodeJS SEA auth (open OAuth PR)               | `/home/madhavendra.rathore/databricks-sql-nodejs-sea-WT/auth-u2m/lib/sea/SeaAuth.ts`                     |
| NodeJS Thrift driver                          | `/home/madhavendra.rathore/databricks-sql-nodejs/lib/hive/HiveDriver.ts`                                 |


