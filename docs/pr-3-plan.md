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

# PR 3 — Metadata: detailed plan

> Plan-only doc. No code changes. Companion to the migration-plan PR 3
> contract in `docs/migration-plan.md` — this doc fills in the design
> calls and module-by-module work that the high-level plan doesn't.

## 1. Goal

After PR 3, every method on the v0 spec's `Metadata` works against a
real workspace and returns a `ResultStream` whose schema matches the
**JDBC/ODBC contract**.

```rust
let session = Session::builder()…open().await?;
let md = session.metadata();

let mut stream = md.list_tables(
    Some(Identifier::new("main")?),
    Some(LikePattern::new("information%")?),
    None,                               // table pattern
    Some(&["TABLE", "VIEW"]),           // table types
).await?;

// stream's schema is JDBC-shaped:
//   TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, …
```

Statement-free path — exercises `Session::open` end-to-end without
needing PR 4's `Statement` work to land first. Drivers (PySQL,
Node.js, Go, future ODBC) drop their own metadata-reshaping code and
consume the kernel's output as-is.

## 2. Why JDBC/ODBC-conforming output (not Databricks-native)

The kernel exists to centralize code shared by all drivers. Metadata
shapes are the canonical example: every driver today re-implements
the same reshape from Databricks `SHOW CATALOGS` / `SHOW TABLES` /
etc. into the JDBC/ODBC schema their callers expect.

The Hive `TGetTablesResp` shape (used by PySQL and Node today via
Thrift) and ODBC `SQLTables` are **the same** shape — both
standardised on the JDBC `DatabaseMetaData.getTables()` columns
decades ago:

| Method        | JDBC / ODBC columns                                                                                   |
|---------------|-------------------------------------------------------------------------------------------------------|
| `list_catalogs`  | `TABLE_CAT`                                                                                        |
| `list_schemas`   | `TABLE_SCHEM`, `TABLE_CATALOG`                                                                     |
| `list_tables`    | `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`, `REMARKS`, `TYPE_CAT`, `TYPE_SCHEM`, `TYPE_NAME`, `SELF_REFERENCING_COL_NAME`, `REF_GENERATION` |
| `list_columns`   | `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `TYPE_NAME`, `COLUMN_SIZE`, `BUFFER_LENGTH`, `DECIMAL_DIGITS`, `NUM_PREC_RADIX`, `NULLABLE`, `REMARKS`, `COLUMN_DEF`, `SQL_DATA_TYPE`, `SQL_DATETIME_SUB`, `CHAR_OCTET_LENGTH`, `ORDINAL_POSITION`, `IS_NULLABLE`, `SCOPE_CATALOG`, `SCOPE_SCHEMA`, `SCOPE_TABLE`, `SOURCE_DATA_TYPE`, `IS_AUTOINCREMENT`, `IS_GENERATEDCOLUMN` |
| `primary_keys`   | `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `KEY_SEQ`, `PK_NAME`                      |
| `foreign_keys`   | `PKTABLE_CAT`, `PKTABLE_SCHEM`, `PKTABLE_NAME`, `PKCOLUMN_NAME`, `FKTABLE_CAT`, `FKTABLE_SCHEM`, `FKTABLE_NAME`, `FKCOLUMN_NAME`, `KEY_SEQ`, `UPDATE_RULE`, `DELETE_RULE`, `FK_NAME`, `PK_NAME`, `DEFERRABILITY` |
| `procedures`     | `PROCEDURE_CAT`, `PROCEDURE_SCHEM`, `PROCEDURE_NAME`, `RESERVED1`, `RESERVED2`, `RESERVED3`, `REMARKS`, `PROCEDURE_TYPE`, `SPECIFIC_NAME` |
| `functions`      | `FUNCTION_CAT`, `FUNCTION_SCHEM`, `FUNCTION_NAME`, `REMARKS`, `FUNCTION_TYPE`, `SPECIFIC_NAME`     |
| `type_info`      | `TYPE_NAME`, `DATA_TYPE`, `COLUMN_SIZE`, `LITERAL_PREFIX`, `LITERAL_SUFFIX`, `CREATE_PARAMS`, `NULLABLE`, `CASE_SENSITIVE`, `SEARCHABLE`, `UNSIGNED_ATTRIBUTE`, `FIXED_PREC_SCALE`, `AUTO_UNIQUE_VALUE`, `LOCAL_TYPE_NAME`, `MINIMUM_SCALE`, `MAXIMUM_SCALE`, `SQL_DATA_TYPE`, `SQL_DATETIME_SUB`, `NUM_PREC_RADIX`, `INTERVAL_PRECISION` |
| `table_types`    | `TABLE_TYPE`                                                                                       |

For columns where Databricks has no value (e.g. `BUFFER_LENGTH`,
`SQL_DATETIME_SUB`), the kernel emits `NULL`. The column count and
order match the spec exactly so drivers can index by position
without guessing.

## 3. Module-by-module work

### 3.1 `src/metadata_v0.rs` — `Identifier` / `LikePattern` / `Metadata`

**`Identifier::new` validation** (currently a no-op pass-through):

- Reject empty string.
- Reject embedded NUL bytes (`\0`) — would terminate the string at
  the first FFI/C ABI boundary and silently truncate.
- Reject backtick characters (`` ` ``) — the kernel backtick-escapes
  identifiers at the SQL boundary; a literal backtick would break
  out of the escaping. Databricks itself rejects identifiers with
  backticks.
- Reject strings exceeding 255 bytes (Databricks identifier-length cap).

Returns `Err(ErrorCode::InvalidArgument)` with a clear message naming
the offending input.

**`LikePattern::new` validation**:

- Reject empty string (caller should pass `None` for "match all").
- Reject embedded NUL bytes.
- Reject strings exceeding 255 bytes.

Backslash-escaping of literal `%` / `_` is the **caller's
responsibility** per ODBC convention; the kernel does not interpret
the pattern.

**`Metadata::*` methods**: each method's body fills in:

1. Validate inputs (already type-checked via `Identifier` /
   `LikePattern`; nothing additional here).
2. Build the SQL via the new SQL builders (§3.2) or fetch a static
   in-memory `RecordBatch` (§3.5).
3. Send the request via `Transport::execute_statement` (or skip
   straight to a synthesised response for the static methods).
4. Reshape the raw response Arrow batch to the JDBC/ODBC schema via
   the new shaping functions (§3.3).
5. Wrap the shaped reader in a `ResultStream` via the new
   `ResultStream::from_reader` adapter (§3.4) and return.

Each method wraps its body in `let _g = self.session.log_span.clone().entered();`
so log lines are tagged with the session's `session_id` (a §7
follow-up from PR 2).

### 3.2 `src/metadata/sql.rs` — SQL builders

Existing builders that already work (used by today's `Transport`):

- `build_show_catalogs`
- `build_show_schemas`
- `build_show_tables`
- `build_show_columns`
- `build_get_procedures` (currently `#[allow(dead_code, …)]`)
- `build_get_procedure_columns` (currently `#[allow(dead_code, …)]`)

**Drop the `#[allow(dead_code, …)]`** on the procedure builders +
their helpers (`escape_sql_string`, `resolve_catalog_prefix`) — PR 3
calls them from the v0 path. Closes a §7 follow-up from PR 1.

**New builders to add**:

- `build_get_primary_keys(catalog: Option<&str>, schema: Option<&str>, table: &str) -> String`
  → emits `SHOW KEYS IN CATALOG \`X\` IN SCHEMA \`Y\` IN TABLE \`Z\``
  (matches the legacy E2E test SQL). Backtick-escapes identifiers
  using `escape_sql_string`.
- `build_get_foreign_keys(parent_catalog/schema/table, foreign_catalog/schema/table) -> String`
  → emits `SHOW FOREIGN KEYS IN CATALOG \`X\` IN SCHEMA \`Y\` IN TABLE \`Z\``.
  ODBC's `SQLForeignKeys` accepts both parent and foreign sides; we
  query foreign-side and the response includes the parent linkage.
- `build_get_functions(catalog: Option<&str>, schema_pattern: Option<&str>, name_pattern: Option<&str>) -> String`
  → emits a SELECT against `information_schema.routines` with
  `routine_type = 'FUNCTION'`, mirroring the procedures builder's
  shape. Uses `resolve_catalog_prefix` for the cross-catalog `system`
  fallback.

`type_info` and `table_types` do **not** get SQL builders — they're
static (§3.5).

### 3.3 `src/metadata/shape.rs` (new file) — JDBC/ODBC reshape

This is the bulk of PR 3's new code. One `reshape_*` function per
metadata method takes an Arrow `RecordBatch` from the raw
`Transport::list_*` response and rebuilds it with the JDBC/ODBC
schema.

```rust
pub(crate) fn reshape_catalogs(raw: RecordBatch) -> Result<RecordBatch>;
pub(crate) fn reshape_schemas(raw: RecordBatch, default_catalog: Option<&str>) -> Result<RecordBatch>;
pub(crate) fn reshape_tables(raw: RecordBatch) -> Result<RecordBatch>;
pub(crate) fn reshape_columns(raw: RecordBatch) -> Result<RecordBatch>;
pub(crate) fn reshape_primary_keys(raw: RecordBatch) -> Result<RecordBatch>;
pub(crate) fn reshape_foreign_keys(raw: RecordBatch) -> Result<RecordBatch>;
pub(crate) fn reshape_procedures(raw: RecordBatch) -> Result<RecordBatch>;
pub(crate) fn reshape_functions(raw: RecordBatch) -> Result<RecordBatch>;
```

Each function:
- Validates the raw schema has the columns it expects (catches
  Databricks-side schema drift loudly rather than producing wrong
  data silently).
- Maps source columns to JDBC names; emits `NULL` for unmapped
  JDBC columns.
- For type-bearing methods (`list_columns`, `type_info`), uses
  `metadata::type_mapping` (existing) to translate Databricks type
  names to ODBC `DATA_TYPE` integers.
- Returns a `RecordBatch` matching `metadata::schemas::*` (§3.6).

The legacy `metadata::parse::*` family stays put — it parses raw
SHOW results into intermediate Rust structs that
`adbc/metadata_builder.rs` uses to assemble the ADBC `get_objects`
nested table. PR 3 introduces *new* parallel reshape functions that
target JDBC schemas instead of Rust structs. The two paths share the
type-mapping helpers but otherwise live side by side. PR 7 removes
the `parse_*` / `metadata_builder` family with the legacy ADBC delete.

**Empty-result handling**: `SHOW PRIMARY KEYS` / `SHOW FOREIGN KEYS`
return zero rows when the table has no PKs/FKs. The reshape
functions return a 0-row `RecordBatch` with the JDBC schema — not
an error.

### 3.4 `src/result_stream.rs` — `ResultStream::from_reader` adapter

The skeleton's `ResultStream` body lands in PR 4 (full multi-batch
+ async-aware). PR 3 needs the smallest possible adapter that lets
`Metadata::*` return a `ResultStream`:

```rust
impl ResultStream {
    /// Build a `ResultStream` over a kernel-internal `ResultReader`.
    /// Sync internally — `next_batch().await` just delegates to the
    /// reader's sync `next_batch` for v0. PR 4 generalises this to
    /// drive real async fetches.
    pub(crate) fn from_reader(reader: Box<dyn ResultReader + Send>) -> Result<Self>;
}
```

Adds `next_batch()` and `schema()` bodies wired against the reader
— enough that `Metadata`-driven streams work end-to-end.
`next_row_in_batch()` and `Row<'a>` accessors stay
`unimplemented!()` for PR 4.

`from_reader` is `pub(crate)` — wrappers never call it directly;
they go through `Metadata::*` or (in PR 4) `ExecutedStatement::result_stream`.

**Format envelope**. Per the spec, `next_batch` returns
`Option<&ResultBatch>`. `from_reader` always wraps the underlying
`RecordBatch` in `ResultBatch::Arrow(rb)` for v0.

### 3.5 `src/metadata/static_data.rs` (new file) — `type_info` / `table_types`

Both methods return a fixed in-memory `RecordBatch`. No wire call,
no SQL.

**`table_types`** returns 3 rows: `TABLE`, `VIEW`, `SYSTEM TABLE`.
This is the Databricks-supported set; matches what `SHOW TABLES`
emits in `tableType` and what the legacy `Connection::get_table_types`
returns today.

**`type_info`** returns the JDBC `DatabaseMetaData.getTypeInfo`
shape with one row per Databricks SQL type:
`BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`,
`DOUBLE`, `DECIMAL`, `STRING`, `BINARY`, `DATE`, `TIMESTAMP`,
`TIMESTAMP_NTZ`, `INTERVAL DAY TIME`, `INTERVAL YEAR MONTH`,
`ARRAY`, `MAP`, `STRUCT`, `VARIANT`. Constants table; lifted from
the JDBC driver's `DatabricksDatabaseMetaData.getTypeInfo()` so we
match what JDBC users already rely on.

The `RecordBatch` is built once at the call site and wrapped in an
in-memory `ResultReader` — small new helper:

```rust
struct InMemoryReader {
    schema: SchemaRef,
    batch: Option<RecordBatch>,  // None after first next_batch()
}
```

Lives in `src/reader/mod.rs` next to the existing `EmptyReader`.

### 3.6 `src/metadata/schemas.rs` (new file) — JDBC schema constants

A `Lazy<SchemaRef>` per metadata method:

```rust
pub(crate) static CATALOGS_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static SCHEMAS_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static TABLES_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static COLUMNS_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static PRIMARY_KEYS_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static FOREIGN_KEYS_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static PROCEDURES_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static FUNCTIONS_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static TYPE_INFO_SCHEMA: Lazy<SchemaRef> = …;
pub(crate) static TABLE_TYPES_SCHEMA: Lazy<SchemaRef> = …;
```

Field names, types, and nullability come from the JDBC spec.
Single source of truth — both the reshape functions (§3.3) and the
static-data builders (§3.5) clone from these.

### 3.7 `src/client/transport.rs` — new Transport methods

**Add four methods** to the `pub(crate) trait Transport`:

```rust
async fn list_primary_keys(
    &self,
    session_id: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
) -> Result<ExecuteResult>;

async fn list_foreign_keys(
    &self,
    session_id: &str,
    parent_catalog: Option<&str>,
    parent_schema: Option<&str>,
    parent_table: Option<&str>,
    foreign_catalog: Option<&str>,
    foreign_schema: Option<&str>,
    foreign_table: Option<&str>,
) -> Result<ExecuteResult>;

async fn list_functions(
    &self,
    session_id: &str,
    catalog: Option<&str>,
    schema_pattern: Option<&str>,
    name_pattern: Option<&str>,
) -> Result<ExecuteResult>;
```

`list_procedures` and `list_procedure_columns` already exist on the
trait; PR 3 just drops the `#[allow(dead_code, …)]` reasons since
the v0 path now reaches them. Closes the corresponding §7 follow-up.

**Implement the new methods on `SeaRestTransport`** in
`src/client/sea.rs` — each is a thin call into `execute_and_read`
with the SQL from §3.2.

### 3.8 `src/lib.rs` — re-exports

Already exposes `Identifier`, `LikePattern`, `Metadata`. No changes.

### 3.9 Legacy ADBC layer

`src/adbc/connection.rs::get_objects` keeps calling the lower-level
`Transport::list_*` methods directly (it doesn't go through
`Metadata::*`). No churn needed; the ADBC `get_objects` shape is
nested-tree, different from the JDBC per-method shape, so reshaping
through `Metadata::list_tables` would just throw away work to
rebuild it. Plan §5.1's "after PR 3 delegates to `Metadata::list_*`"
note was speculative — leave the legacy layer alone, delete it in
PR 7.

`src/adbc/connection.rs::get_table_types` continues to return its
hardcoded list. The legacy ADBC `metadata-ffi` path keeps using
`Transport::list_procedures` / `list_procedure_columns` directly.

## 4. Tests

**Unit (in `src/`)**:

- `metadata_v0::tests` — `Identifier::new` validation: empty,
  NUL byte, backtick, oversize. `LikePattern::new` validation:
  empty, NUL byte, oversize.
- `metadata::shape::tests` — for each `reshape_*`: feed a
  synthesised raw `RecordBatch` and assert the output column names,
  order, types, and a sample row's values. NULL-handling for
  JDBC-only columns.
- `metadata::static_data::tests` — `type_info` and `table_types`
  return non-empty batches with the expected JDBC schemas.
- `metadata::sql::tests` — SQL strings for the new builders match
  expected forms (snapshot strings; same pattern the existing
  tests use).

**Wiremock (`tests/v0_metadata_wiremock.rs`)**:

- For each `Metadata::list_*` method: stub `POST /api/2.0/sql/statements`
  + `GET /api/2.0/sql/statements/{id}` to return a synthesised raw
  Databricks Arrow result. Drive the call through `Session::open`
  + `Metadata::list_*`, assert the returned `ResultStream`'s schema
  matches the JDBC `*_SCHEMA` constant and the row data is reshaped
  correctly.
- Validation errors: `Identifier::new("")` propagates as
  `ErrorCode::InvalidArgument`.

**E2E (`tests/v0_metadata_e2e.rs`, `#[ignore]`)**:

- `list_catalogs` returns at least the `main` catalog.
- `list_schemas("main", None)` returns `default`, `information_schema`, …
- `list_tables("main", "information_schema", None, None)` returns
  the catalog's information_schema views.
- `list_columns("main", "information_schema", "tables", None)` returns
  the columns of the `tables` view.
- `primary_keys("main", "information_schema", "tables")` runs
  cleanly (zero rows is a valid result).
- `foreign_keys` runs cleanly.
- `procedures` and `functions` against `system.information_schema`
  return at least one row.
- `type_info` returns the static set.
- `table_types` returns 3 rows.

For each E2E test, also assert the result schema matches the
corresponding `*_SCHEMA` constant. Catches schema drift between the
kernel's reshape and the spec.

**Legacy regression**: `tests/metadata_e2e.rs` keeps passing
unchanged. `Connection::get_objects` and the legacy procedure path
still go through their own pipelines.

## 5. Estimated scope

| Module                              | LoC est | Risk    |
|-------------------------------------|--------:|---------|
| `metadata_v0.rs` validation + bodies| ~250    | Low     |
| `metadata/sql.rs` (new builders + drop allows) | ~80 | Low |
| `metadata/shape.rs` (new)           | ~700    | Medium  |
| `metadata/schemas.rs` (new)         | ~300    | Low     |
| `metadata/static_data.rs` (new)     | ~250    | Low     |
| `result_stream.rs` (`from_reader`)  | ~80     | Low     |
| `client/transport.rs` (new methods) | ~50     | Low     |
| `client/sea.rs` (new impls)         | ~150    | Low     |
| `reader/mod.rs` (`InMemoryReader`)  | ~40     | Low     |
| Tests (unit + wiremock + E2E)       | ~700    | Medium  |
| **Total**                           | ~2600   |         |

Larger than PR 2 (~1300 LOC). The bulk is `shape.rs` and
`schemas.rs` — both mechanical schema-translation code, low risk
once the JDBC contract is locked. The medium-risk surface is the
NULL-handling for unmapped columns and the type translations in
`reshape_columns` / `reshape_type_info`.

## 6. Decisions captured

- **JDBC/ODBC-conforming output schemas.** `TGetTablesResp` (Hive)
  ≡ `SQLTables` (ODBC) ≡ `DatabaseMetaData.getTables()` (JDBC).
  Drivers (PySQL, Node) already expect this shape from their
  Thrift backend; the kernel returning the same shape is a drop-in
  replacement.
- **`procedures` and `functions` ship in PR 3** (not deferred to a
  PR 3a). The information_schema SQL builders already exist in
  `metadata/sql.rs`; PR 3 wires them through `Transport` +
  `Metadata`. Closes the §7 metadata-ffi follow-up from PR 1.
- **`type_info` and `table_types` are static.** Hardcoded in-memory
  `RecordBatch`. No wire call. Databricks supports a fixed enum;
  the answer doesn't change between calls.
- **Legacy `parse_*` / `metadata_builder.rs` stay put.** PR 3
  introduces parallel reshape functions; the legacy ADBC
  `get_objects` keeps using its existing pipeline. Both go away in
  PR 7.
- **`ResultStream::from_reader` is the smallest sync-bridging shim
  that works.** PR 4 generalises to multi-batch + async-aware.

## 7. §7 follow-ups cleared

PR 3 closes these PR 1 follow-ups in `docs/migration-plan.md` §7:

- Drop `#[allow(dead_code, …)]` on
  `SqlCommandBuilder::escape_sql_string` /
  `resolve_catalog_prefix` / `build_get_procedures` /
  `build_get_procedure_columns` — all called from the v0
  `Metadata` path.
- Drop `#[allow(dead_code, …)]` on
  `Transport::list_procedures` / `list_procedure_columns` — same.

PR 3 follow-ups (added to §7 on merge):

- **Type-mapping completeness for `list_columns`.**
  `reshape_columns` calls `metadata::type_mapping::databricks_type_to_arrow`
  to fill `DATA_TYPE`. Coverage today is the legacy set — VARIANT,
  TIMESTAMP_NTZ, etc. need a sweep against the latest Databricks
  type list. Not blocking PR 3; tracked for a follow-up cleanup.
- **`type_info` accuracy.** The static type_info row content
  (`COLUMN_SIZE`, `LITERAL_PREFIX`, etc.) is lifted from JDBC.
  Manual review against the JDBC source on merge to make sure we
  didn't fork. Tracked for the JDBC team to sign off.
- **Per-method `tracing::Span::enter`.** PR 2 only enters the
  session span on `Session::close`. PR 3 adds it to every
  `Metadata::*` call. PR 4 finishes the job for
  `Statement::*` / `ExecutedStatement::*`.

## 8. What's NOT in PR 3

- `Statement::*` bodies — PR 4.
- `ResultStream::next_row_in_batch` / `Row<'a>` — PR 4.
- Multi-batch / true-async `ResultStream::next_batch` — PR 4.
- C ABI metadata accessors — PR 6.
- Refining the legacy `kernel_error::Error → adbc_core::error::Error`
  status mapping — moot at PR 7 (legacy delete).
