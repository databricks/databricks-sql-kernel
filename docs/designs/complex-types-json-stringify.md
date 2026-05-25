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

# Complex types as JSON strings — design

Optional kernel-level conversion of `ARRAY` / `MAP` / `STRUCT` /
`VARIANT` columns into compact JSON text. Off by default; opted into
per session by callers that want a stringified surface (JDBC's
`complexDatatypeSupport=false`, PySQL's
`_use_arrow_native_complex_types=False`).

Status: design.

## Why

### What users see today

The Databricks SQL server returns complex-typed columns differently
depending on the wire protocol:

- **Thrift**: when the connection's `complexTypesAsArrow=false`,
  the server stringifies complex columns to compact JSON
  server-side. The Python connector's Thrift backend receives
  `Utf8` columns with values like `'["a","b","c"]'` and hands them
  to the user.
- **SEA**: the server always returns native Arrow `List` / `Map` /
  `Struct` arrays. There is no `complexTypesAsArrow=false`
  equivalent — the wire is always native.

That asymmetry breaks parity for both new SEA-family backends:

- The Python connector's **native SEA backend** (`backend/sea/`)
  accepts `_use_arrow_native_complex_types=False` but ignores it —
  it always serves native Python objects, not JSON strings.
- The Python connector's **kernel backend** (`backend/kernel/`)
  inherits the same gap: the kernel hands it native Arrow batches,
  and the connector doesn't stringify on the way out.

The Thrift vs Kernel comparator (`databricks-driver-test#304`)
surfaces this as 5 diffs in the `COMPLEX_TYPES_DISABLED` suite.
Same gap surfaces against the native SEA backend.

### Why each driver doing it is the wrong shape

JDBC already does its own client-side conversion. Its
`DatabricksArray.toString()` / `DatabricksMap.toString()` /
`DatabricksStruct.toString()` produce compact JSON:

| Type | JDBC output | Thrift wire | Match? |
|---|---|---|---|
| `ARRAY<STRING>` | `["a","b","c"]` | `["a","b","c"]` | yes |
| `MAP<STRING,STRING>` | `{"k1":"v1","k2":"v2"}` | `{"k1":"v1","k2":"v2"}` | yes |
| `STRUCT<a:STRING,b:INT>` | `{"a":"foo","b":1}` | `{"a":"foo","b":1}` | yes |

So **JDBC and Thrift already converge on the same compact-JSON
output**. Two consumers, identical contract, implemented twice in
two languages.

The Python connector would need the same. Plus a future Go / Node
binding would need it again. That's the same 50–100 lines of JSON
serialization repeated per language, with subtle differences in
edge cases (string escaping, null handling, nested ordering,
geospatial formatting) that drift over time.

**ADBC** is the only outlier: it wants raw Arrow, period. Its
binding opts out by leaving the kernel option off.

### Proposal

Push the JSON stringification into the kernel as an opt-in feature.

- **One implementation** of the JSON serializer (in Rust), used by
  JDBC's eventual kernel migration, the Python connector, future
  Go / Node bindings.
- **One source of truth** for the byte-for-byte JSON format —
  same comparator that validates Thrift parity also validates
  kernel output, so drift gets caught immediately.
- **ADBC unaffected** — opt-in flag means it pays no cost and sees
  no change.

## API shape

### Configuration surface

Add a single bool on `SessionBuilder` (and `Session`'s config):

```rust
pub struct SessionBuilder {
    ...
    /// When `true`, the kernel post-processes each result batch:
    /// every column of complex type (`List` / `LargeList` /
    /// `FixedSizeList` / `Map` / `Struct`, plus the precise types
    /// `VARIANT` / `GEOMETRY` / `GEOGRAPHY` carried in field
    /// metadata) is replaced with a `Utf8` column holding compact
    /// JSON text per row.
    ///
    /// The JSON format matches what the Databricks server emits on
    /// the Thrift wire when `complexTypesAsArrow=false` (compact —
    /// no whitespace between separators, double-quoted strings,
    /// JSON `null` for SQL NULL). See `docs/designs/
    /// complex-types-json-stringify.md` for the exact rules.
    ///
    /// Default `false`. Bindings that surface a flat-text view
    /// (PySQL when `_use_arrow_native_complex_types=False`, JDBC
    /// when `complexDatatypeSupport=false`) should set this `true`.
    /// ADBC and any other Arrow-native consumer should leave it
    /// `false`.
    complex_types_as_json: bool,
}
```

Per-statement override is not in scope for v0; if it turns out
needed, add a `SessionConfig::complex_types_as_json` override on
`StatementSpec` later. Keeping it session-level matches both PySQL
and JDBC's actual usage — the flag is a connection-level kwarg in
both.

### Wire integration

No wire change. The flag is purely client-side post-processing.

### Result-stream integration

The existing `ManifestAugmentingReader` is renamed and generalised
into a single `ResultPostProcessor` that owns *all* per-batch
post-processing the kernel does today (and adds tomorrow). It runs
exactly **one** `next_batch` indirection regardless of how many
transforms are enabled, and **zero** per-row work for transforms
that are inactive.

```rust
pub(crate) struct ResultPostProcessor {
    inner: Box<dyn ResultReader + Send>,
    schema: SchemaRef,
    /// `None` when no per-batch reshape is needed (no `databricks.*`
    /// metadata to copy through *and* JSON stringification disabled).
    /// `Some` when at least one transform applies; the closure walks
    /// the batch once, runs every active transform inline, returns a
    /// fresh `RecordBatch`.
    transform: Option<BatchTransform>,
}

type BatchTransform =
    Box<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>;
```

`ResultPostProcessor::new` runs once at construction time and
decides what to do:

1. Take the inner schema; apply the manifest-augmentation rewrite
   to produce the output schema (no-op when manifest is `None`).
2. Inspect the *augmented* schema: are any columns complex
   (`List` / `Map` / `Struct` / `Utf8`-with-VARIANT-metadata /
   ...)?
3. **Decide the transform shape once**, based on what's active:
   - **No manifest, no JSON**: `transform = None`, batches passed
     through untouched.
   - **Manifest only**: the closure just re-projects each batch's
     columns onto the augmented schema (single `RecordBatch::try_new`).
   - **JSON only** (shouldn't happen — manifest is always present
     in execute responses — but kept consistent): walk only the
     complex columns.
   - **Both**: a single fused walk that re-projects *and*
     stringifies complex columns in the same pass, producing
     exactly one new `RecordBatch` per upstream batch.

The hot path on `next_batch` is then unconditional:

```rust
fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
    let Some(batch) = self.inner.next_batch()? else {
        return Ok(None);
    };
    match &self.transform {
        Some(transform) => Ok(Some(transform(&batch)?)),
        None => Ok(Some(batch)),
    }
}
```

Single struct, single hot-path branch (matched once per batch, not
per row), and the "pass-through" case really is pass-through (no
`Vec<ArrayRef>` allocation, no `try_new`, no schema rebuild).

### Why one struct instead of separate `ManifestAugmentingReader`
### and `JsonStringifyingReader`

The natural object-oriented factoring is one wrapper per
transform. We deliberately don't do that for three reasons:

1. **Per-batch overhead matters.** Result batches arrive
   thousands at a time for CloudFetch streams; every extra
   `Box<dyn ResultReader>` indirection is a virtual call per batch.
   Today's two transforms could grow (interval stringification,
   future others). Each is microseconds, but they compound and
   would force a measurement-driven cleanup later.
2. **Pass-through paths should actually be free.** With per-
   transform wrappers, the "no-op" path (no manifest, no JSON)
   still costs one `RecordBatch::try_new` per batch to re-project
   onto an identical schema. With one consolidated processor that
   decides `transform = None` at construction, the upstream batch
   flows through without any rebuild.
3. **The transforms are unidirectional and tightly coupled.**
   JSON stringification needs to read field metadata that the
   manifest augmenter writes. Layering them per the natural
   factoring still requires the JSON layer to look up the
   augmenter's output, so the coupling is real either way. A
   fused walk is no harder to reason about than two sequential
   walks that already have to agree on field-metadata semantics.

If the kernel later accumulates a transform that genuinely
benefits from being separately addressable (e.g. an Arrow-FFI
adapter that wants only some of the transforms), we revisit then.
Today, all transforms apply to all `ResultReader` consumers, so
the consolidated shape is correct.

### Migration of the existing `ManifestAugmentingReader`

`ManifestAugmentingReader` (introduced in PR #35) is renamed to
`ResultPostProcessor`. The existing constructor signature changes
from

```rust
ManifestAugmentingReader::new(inner, manifest: Option<&ResultManifest>)
```

to

```rust
ResultPostProcessor::new(
    inner,
    manifest: Option<&ResultManifest>,
    options: PostProcessOptions,
)
```

where `PostProcessOptions` is a small struct holding the JSON flag
(and any future transform flags). The two existing tests
(`test_manifest_augmenting_reader_*`) are updated to construct the
new struct with `options::default()` (JSON off) and assert the
same behaviour they do today.

The wrap site in `ResultReaderFactory::create_reader` becomes:

```rust
pub(crate) fn create_reader(
    &self,
    statement_id: &str,
    response: &ExecuteResponse,
) -> Result<Box<dyn ResultReader + Send>> {
    let inner = self.create_inner_reader(statement_id, response)?;
    Ok(Box::new(ResultPostProcessor::new(
        inner,
        response.manifest.as_ref(),
        self.post_process_options(),
    )?))
}
```

`post_process_options()` reads the session config (`complex_types_as_json`)
and builds the `PostProcessOptions` for this reader.

## JSON format

The format **must match Thrift's wire format byte-for-byte**, since
that's the contract every existing PySQL/JDBC user already sees.
Verified against Thrift output on Dogfood (`comparator_tests
.oss_jdbc_tests.test_result_set_types`).

### Compact serialization rules

- **No whitespace** between separators. `[1,2]` not `[1, 2]`.
  Equivalent to Python `json.dumps(obj, separators=(',', ':'))`.
- **JSON `null`** for SQL NULL — not `"null"`.
- **Double-quoted strings** with standard JSON escaping
  (`\\"`, `\\\\`, `\\b`, `\\f`, `\\n`, `\\r`, `\\t`,
  `\\uXXXX` for control chars).
- **Map keys** stringified the same way (Databricks `MAP` requires
  string keys; non-string keys are a server-side error).

### Per-type formatting

| Type | JSON form | Notes |
|---|---|---|
| `BOOLEAN` | `true` / `false` | |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | bare integer | `42` |
| `FLOAT`, `DOUBLE` | bare number | `3.14`. `NaN` / `±Inf` are server-side errors today; if they ever surface, emit `null` to stay valid JSON. |
| `DECIMAL(p,s)` | bare number, fixed precision | `99.99` |
| `STRING`, `CHAR`, `VARCHAR` | quoted, JSON-escaped | `"abc"` |
| `BINARY` | base64 quoted | `"aGVsbG8="` (matches what Thrift emits) |
| `DATE` | quoted ISO 8601 | `"2024-01-15"` |
| `TIMESTAMP` | quoted ISO 8601 with offset | `"2024-01-15T10:30:00.000Z"` |
| `TIMESTAMP_NTZ` | quoted ISO 8601 no offset | `"2024-01-15T10:30:00.000"` |
| `INTERVAL YEAR TO MONTH` | quoted Databricks text | `"2-6"` |
| `INTERVAL DAY TO SECOND` | quoted Databricks text | `"3 12:30:15.000000000"` |
| `ARRAY<T>` | `[v1,v2,...]` | Recurse on T |
| `MAP<K,V>` | `{"k1":v1,"k2":v2}` | Keys stringified per K's rules but always rendered quoted |
| `STRUCT<f1:T1,...>` | `{"f1":v1,...}` | Field-name keys quoted; field order follows declared schema |
| `VARIANT` | depends — the server emits the variant's own JSON value already | Pass through verbatim (already JSON-text on the wire) |
| `GEOMETRY` / `GEOGRAPHY` | quoted WKT | `"POINT (1.5 2.5)"` |

Edge cases worth calling out, with the rule we pick:

- **Null inside nested types**: rendered as bare `null`, not the
  string `"null"`. Matches both Thrift and JDBC.
- **Empty containers**: `[]` and `{}` respectively.
- **String quoting inside MAP keys**: keys are always quoted in
  the JSON output even when the underlying Arrow type is integer
  (Databricks `MAP` permits any key type at the SQL level; JSON
  requires string keys). Cast to canonical string form using the
  same per-type rules. Matches JDBC.
- **Float precision**: emit the shortest round-trippable form
  (Rust's `f64::to_string` does this). Matches Java's
  `Double.toString`, which is what JDBC produces.

## Implementation sketch

### Files touched

- **`src/reader/post_processor.rs`** (new) — `ResultPostProcessor`
  struct + `PostProcessOptions`. Rename target for the existing
  `ManifestAugmentingReader` (which is folded into this).
- **`src/reader/post_processor/json.rs`** (new) — pure functions
  for the per-type JSON serialization. No `ResultReader` impl;
  just `to_json_string(value: &dyn Array, row: usize, field: &Field) -> String`-shape
  helpers. Lets us unit-test the formatter without spinning up a
  reader.
- **`src/reader/mod.rs`** — drop `ManifestAugmentingReader`,
  rewire `ResultReaderFactory::create_reader` to call
  `ResultPostProcessor::new` instead. `augment_schema_with_manifest`
  helper moves to `post_processor.rs`.
- **`src/client/sea.rs`** — no change; the factory already runs
  here.
- **`src/session/builder.rs`** + **`src/session.rs`** — surface
  `complex_types_as_json` on `SessionBuilder`. `ResultReaderFactory`
  reads it via `self.config` (already plumbed for the wire-path
  config knobs).

The JSON serialization itself uses `serde_json` for primitive value
emission (already pulled in transitively via `reqwest`) so we don't
hand-roll escaping rules.

### `ResultPostProcessor::new` decision logic

Run once at construction. Cheap — proportional to column count, not
row count:

1. Compute the output schema:
   - If `manifest = Some`, apply `augment_schema_with_manifest`.
   - Else, use the inner schema as-is.
2. Decide whether the batch transform is needed:
   - Inspect the augmented schema. Are there any complex columns
     (`List` / `LargeList` / `FixedSizeList` / `Map` / `Struct`,
     or `Utf8`+VARIANT/GEOMETRY/GEOGRAPHY metadata)?
   - If `options.complex_types_as_json` is `true` *and* the schema
     has complex columns, **JSON path** is active.
   - If the output schema differs structurally from the inner
     schema (i.e. we wrote field metadata, so `Field` objects
     differ), **re-projection path** is active.
   - Else, neither is needed.
3. Build a single `BatchTransform` closure capturing only the
   work that's actually active:
   - Neither active → `transform = None`. Hot path is pass-through.
   - Re-projection only → closure does one `RecordBatch::try_new`
     onto the augmented schema, reusing the source `ArrayRef`s.
   - JSON only (shouldn't happen, but for completeness) → closure
     walks complex columns, leaves primitives untouched.
   - Both → closure does a single fused walk: for each column
     index, either reproject the source `ArrayRef` (primitive) or
     emit a fresh `Utf8` array of JSON strings (complex). Builds
     the final `RecordBatch` from the resulting columns.

### Per-batch hot path

```rust
fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
    let Some(batch) = self.inner.next_batch()? else {
        return Ok(None);
    };
    match &self.transform {
        Some(t) => Ok(Some(t(&batch)?)),
        None => Ok(Some(batch)),
    }
}
```

One branch per batch, picked once at construction. The
pass-through case is genuinely free — no allocation, no rebuild,
the upstream `RecordBatch` flows through.

### JSON serialization details

For each complex column, the closure produced at construction
knows:

- The column index in the source batch.
- A row-formatter function specialised to that column's schema
  (closed over the Arrow `DataType` and any per-field
  `databricks.type_name` metadata).

Per-row, the formatter recurses through the type:

- Primitives: `serde_json::Value::to_string()` on the canonical
  Rust value (matching the per-type formatting table above).
- `List` / `Map` / `Struct`: walk children, recurse.
- `Utf8` with VARIANT/GEOMETRY/GEOGRAPHY metadata: pass through
  the source value as-is (the server has already emitted it as
  JSON / WKT text).

The output array is built incrementally via `StringBuilder` for
each complex column.

VARIANT / GEOMETRY / GEOGRAPHY columns can be handled by a fast
path that just copies the source `ArrayRef` (no re-emit needed),
since the data is already correct.

### Schema rewriting

The output schema mirrors the input's order and field names but
every complex column becomes `Utf8` nullable. Field metadata
(`databricks.*` keys) is preserved on the rewritten field so type
info isn't lost — consumers reading metadata can still tell the
original column was `ARRAY<INT>`.

## Test plan

- Unit tests on `json_stringify_batch` per type, exercising:
  - Each primitive: scalar formatting, NULL handling.
  - `ARRAY<primitive>`, `ARRAY<ARRAY<primitive>>`, `ARRAY<STRUCT>`.
  - `MAP<STRING,STRING>` and `MAP<STRING,ARRAY<INT>>`.
  - `STRUCT` with mixed primitive + nested fields.
  - Empty containers, all-null containers.
  - String quoting / escape characters / unicode / control chars.
  - DECIMAL precision/scale boundaries (no scientific notation).
- Format comparison fixtures against captured Thrift output for
  the `comparator_tests.oss_jdbc_tests.test_result_set_types`
  rows the comparator uses. These get committed as golden files
  so we know the format hasn't drifted.
- End-to-end via the comparator's `THRIFT_VS_KERNEL_COMPLEX_DISABLED`
  suite — should drop from 5 diffs to 0 once the feature is on.

## Migration

- Default `false`: kernel behaviour unchanged for existing consumers.
- PySQL connector flips the flag when `_use_arrow_native_complex_types=False`.
- Native SEA backend (separate connector path) could also call into
  this once exposed through the kernel — closes the same gap there
  for free. Out of scope for the kernel work itself.
- JDBC: when the JDBC driver eventually migrates to the kernel,
  it can drop `DatabricksArray.toString()` / `DatabricksMap.toString()` /
  `DatabricksStruct.toString()` (~200 lines of Java) in favour of
  the kernel option.
- ADBC: leaves the flag off, sees no change.

## Open questions

1. **VARIANT / GEOMETRY / GEOGRAPHY pass-through.** The server
   emits these as text already; passing through is the obvious
   choice. Worth confirming via a probe that the text form Thrift
   delivers matches what SEA delivers, byte-for-byte. If they
   diverge, the kernel needs to normalise.
2. **DECIMAL formatting.** Both Java's `BigDecimal.toString` and
   Rust's `arrow_array::types::Decimal128` formatter emit fixed
   notation, but the precise canonicalisation around trailing
   zeros (e.g. `99.99` vs `99.9900`) may differ. Worth a probe
   before committing.
3. **`TIMESTAMP` UTC offset format.** Java emits
   `2024-01-15T10:30:00.000Z`; some Arrow formatters emit
   `2024-01-15T10:30:00.000+00:00`. Match Thrift exactly.
4. **Per-statement override.** Defer — no current binding asks
   for it. If JDBC ever needs to flip per-statement, revisit.
