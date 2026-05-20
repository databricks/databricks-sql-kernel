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

A new `JsonStringifyingReader` wrapper, applied in
`ResultReaderFactory::create_reader` *after* the existing
`ManifestAugmentingReader` wrap (so it sees fields with
`databricks.type_name` metadata, which it needs to identify
VARIANT / GEOMETRY / GEOGRAPHY columns that share Arrow's `Utf8`
with regular strings):

```
        raw_reader
            └─ ManifestAugmentingReader  (existing)
                  └─ JsonStringifyingReader  (new, when flag is set)
```

The wrapper is pass-through when the flag is off. When on, each
`next_batch` walks the schema once (cached on construction),
identifies complex columns, and re-projects.

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

### New module: `src/result/json_stringify.rs`

Exports `JsonStringifyingReader` and a `json_stringify_batch`
helper. Internally uses `serde_json` for the JSON emission (already
in our dep set via `reqwest`) so we don't hand-roll escaping rules.

### `ResultReaderFactory::create_reader` change

```rust
pub(crate) fn create_reader(
    &self,
    statement_id: &str,
    response: &ExecuteResponse,
) -> Result<Box<dyn ResultReader + Send>> {
    let inner = self.create_inner_reader(statement_id, response)?;
    let augmented = Box::new(ManifestAugmentingReader::new(
        inner,
        response.manifest.as_ref(),
    )?);
    if self.config.complex_types_as_json {
        Ok(Box::new(JsonStringifyingReader::new(augmented)?))
    } else {
        Ok(augmented)
    }
}
```

### Per-batch reshape

For each column whose schema (after `ManifestAugmentingReader`)
indicates a complex type (Arrow native: `List`, `LargeList`,
`FixedSizeList`, `Map`, `Struct`; or `Utf8` with metadata
`databricks.type_name in {VARIANT, GEOMETRY, GEOGRAPHY}`):

1. Walk the column row-by-row.
2. For each value, recurse using the Arrow column's child schema
   and the per-type formatting rules above.
3. Emit a fresh `Utf8` array with one JSON string per row, `None`
   for SQL NULL.

Cache the per-column "is this complex?" decision and the per-row
formatter once at batch construction so the hot path is just
"format value to JSON".

VARIANT / GEOMETRY / GEOGRAPHY are passed through verbatim — the
server already emits them as JSON / WKT text inside `Utf8`, so the
column is already in the correct shape.

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
