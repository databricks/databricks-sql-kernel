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

# Kernel-side JIRA candidates — index

This file indexes kernel-side bug candidates surfaced by the cross-driver Thrift↔SEA
parity audit (`databricks-sql-nodejs/sea-workflow/audits/2026-05-28-cross-driver-audit.md`).

Each candidate is a kernel bug that one or more downstream drivers (NodeJS SEA via the
napi binding, Python `use_kernel=True` via pyo3) inherit. The companion doc
[`sea-feature-matrix.md`](./sea-feature-matrix.md) covers kernel-side **missing-feature** work;
this index covers kernel-side **bugs** in already-implemented surfaces.

The full markdown for each candidate currently lives in the NodeJS workflow tree
under `databricks-sql-nodejs/sea-workflow/jira-candidates/`. This index is the
kernel-repo-local pointer; when a candidate is promoted to a tracked JIRA, update the
**JIRA** column below.

## Conventions

- One row per kernel-side bug candidate.
- "Promoted to JIRA" rows keep their entry here for traceability; they do not get
  removed from the index.
- A candidate is in scope for this index when the kernel is the canonical layer to
  fix — i.e. cross-driver oracles (Python `use_sea=True`, which bypasses the kernel)
  confirm the bug is on the kernel path and not on the SEA endpoint.

## Candidates

| Filed | Title | Severity | Layer | JIRA | Driver-test reproducer |
|---|---|---|---|---|---|
| 2026-05-29 | [Connection-time `QUERY_TAGS` silently dropped via session_conf forwarding](../../databricks-sql-nodejs/sea-workflow/jira-candidates/2026-05-29-kernel-connection-time-querytags-drop.md) | Medium | `session.rs` / SEA session-open payload construction | _not yet filed_ | `databricks-driver-test/tests/nodejs/test/lifecycle/ConnectionTimeQueryTagsTests.test.ts` (C1 cluster) |
| 2026-05-29 | [Empty-result manifest decoding loses INTERVAL / GEOMETRY / GEOGRAPHY / TIMESTAMP_NTZ type fidelity](../../databricks-sql-nodejs/sea-workflow/jira-candidates/2026-05-29-kernel-empty-result-manifest-fidelity.md) | Medium | `result_stream.rs` / `sea/manifest.rs` | _not yet filed_ | `databricks-driver-test/tests/nodejs/test/data-types/EmptyResultSchemaFidelityTests.test.ts` (C4 cluster) |

## Related candidates (informational — not kernel-side)

The audit also surfaced candidates that are **not** kernel bugs but are listed here for
cross-reference so future kernel work has the full picture:

- `2026-05-29-nodejs-sea-empty-result-string-degradation.md` — NodeJS-SEA decode-path
  loses DECIMAL / ARRAY / MAP / STRUCT for 0-row results. Tier-A companion to the
  kernel-side Tier-B candidate above; fix is in the NodeJS adapter.
- `2026-05-26-thrift-querytimeout-not-honoured.md` — NodeJS Thrift accepts `queryTimeout`
  but silently ignores it on the wire. NodeJS-side bug; kernel-side row 2.17 in
  `sea-feature-matrix.md` is the parallel missing-feature work.
- `2026-05-26-query-tags-conf-clamp.md` — workspace-level `QUERY_TAGS` clamp that
  affects the pecotesting warehouse. Workspace-policy issue, not a driver/kernel bug.
- `2026-05-22-list-tables-sigsegv.md` — possible kernel-side mid-stream SIGSEGV in
  `list_tables`; needs bisection. Filed earlier; listed for awareness. If reproduced
  on current kernel `main`, promote into this index.

## How to add a new candidate

1. Author the full candidate markdown under
   `databricks-sql-nodejs/sea-workflow/jira-candidates/YYYY-MM-DD-short-slug.md`.
2. Add a row to the table in this file with: filed date, title (linked), severity,
   suspected kernel layer, JIRA placeholder, driver-test reproducer path.
3. Keep the candidate in scope for this index only if cross-driver oracles confirm the
   bug is kernel-side. NodeJS-only or Python-only bugs belong in their respective
   repos' issue trackers.
