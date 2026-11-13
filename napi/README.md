# `@databricks/databricks-sql-kernel` (napi binding)

Node.js binding over the Databricks SQL Kernel Rust crate. Published to
npm as [`@databricks/databricks-sql-kernel`](https://www.npmjs.com/package/@databricks/databricks-sql-kernel),
the umbrella loader that re-exports the per-platform native packages.

The binding itself lives under `src/`; its public TypeScript surface is
declared in `index.d.ts`.

`Statement` and `AsyncResultHandle` expose `isVolumeOperation` and
`completeVolumeOperation(allowedPaths, removeNotFoundIsSuccess?)`. Completion
returns `not_volume`, `needs_input_stream` or `completed` and preserves the
instruction rows. The caller opts into transfer by supplying allowed local
roots; both lexical paths and resolved symlink targets must stay inside them.
The Node binding does not supply stream-upload bytes.

---

## Building

The cdylib is built by the napi-rs CLI. The published umbrella ships
both a generated multi-platform loader (`index.js`) and its type
declarations (`index.d.ts`) — both files are **committed**, not
`.gitignore`d, because the publish workflow uses `@napi-rs/cli@3`
which no longer emits the loader.

### Regenerating the committed loader

Whenever the `#[napi]` surface in `napi/src/` changes (new function,
new class, new `string_enum`, new `#[napi(object)]` shape) you MUST
regenerate the loader and recommit both files:

```bash
cd napi
npx --yes @napi-rs/cli@2.18.4 build --platform \
    --js-package-name @databricks/databricks-sql-kernel --release
git add napi/index.js napi/index.d.ts
git commit -s --amend --no-edit   # or a fresh commit; up to you
```

The version pin (`@2.18.4`) is load-bearing — a bare `napi build`
resolves to `@napi-rs/cli@3`, which produces a different artifact
shape and breaks consumers. Always cite the pinned form.

The per-target list emitted in `index.js` (18 triples covering
`linux-{x64,arm64}-{gnu,musl}` + `darwin-{x64,arm64,universal}` +
`win32-{x64,arm64,ia32}` + `android-{arm64,arm-eabi}` + freebsd) is
the napi-rs default matrix; it does NOT track the `napi.targets` list
in `package.json` (that's a publish-time concern). So the only thing
that should force a regeneration is a change to the `#[napi]` surface
in Rust.

### CI drift guard

`.github/workflows/napi-checks.yml` runs
`.github/scripts/check-napi-loader-drift.sh` on every PR that touches
`napi/**`. The script parses napi/src/**.rs for top-level `#[napi]`
items and compares against the destructured names in `napi/index.js`,
failing CI if a contributor adds a new `#[napi]` symbol without
recommitting the regenerated loader.

The guard is a **surface audit**, not a full regenerate-and-diff: it
catches the "new export missing from loader" bug class (which already
occurred once, see PR #146) but doesn't catch formatting drift in
`index.d.ts` or signature changes to existing functions. A full
`napx napi build` + `git diff --exit-code` step would catch those too
but currently needs an npm proxy on the protected runner (npm support
in `setup-jfrog` is a "future" item per the action docs). When that
lands, swap the drift script for the full regenerate-and-diff.

---

## Per-platform native packages

The umbrella `package.json` declares `napi.targets` covering the eight
triples we publish. The release workflow
(`peco-databricks-sql-kernel-nodejs.yml` in
`databricks/secure-public-registry-releases-eng`) builds each per-target
package and publishes them as
`@databricks/databricks-sql-kernel-<triple>` (e.g.
`@databricks/databricks-sql-kernel-linux-x64-gnu`). The umbrella's
loader `require()`s the matching package at runtime via
`optionalDependencies` wiring (set up by the same workflow).

Consumers do NOT install the per-platform packages directly — they
`npm install @databricks/databricks-sql-kernel` and let npm resolve the
matching `optionalDependencies` entry for their platform.

---

## Changelogs

- `napi/CHANGELOG.md` — released versions of the npm package.
- `napi/NEXT_CHANGELOG.md` — unreleased changes staged for the next
  release.
- `napi/release-notes.txt` — human-readable summary, mirrors the
  format the jdbc driver uses.

The Rust crate (`../CHANGELOG.md`) and Python wheel
(`../pyo3/CHANGELOG.md`) ship on independent cadences; entries for
this napi binding go ONLY in this directory's files.
