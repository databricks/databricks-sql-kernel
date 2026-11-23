# Releasing Databricks SQL Kernel

This document is for repository maintainers. Releases are built, scanned, and
published by CI from a version tag; do not publish packages from a workstation.

## Prepare the release

1. Open a release pull request that updates the version in the root, PyO3, and
   NAPI package manifests and refreshes their lockfiles. Keep the standalone
   ADBC crate unpublished (`publish = false`); only update its kernel dependency
   requirement and lockfile.
2. Move each component's staged entries from `NEXT_CHANGELOG.md`,
   `pyo3/NEXT_CHANGELOG.md`, and `napi/NEXT_CHANGELOG.md` into the corresponding
   `CHANGELOG.md`. Add each release section as
   `## [v<version>] - <YYYY-MM-DD>`, then reset the staged file to its empty
   template. Do not add a component section when that component has no changes.
3. Run the repository checks and package validation:

   ```bash
   make check
   cargo check --locked --manifest-path pyo3/Cargo.toml
   cargo check --locked --manifest-path napi/Cargo.toml
   cargo package --locked --list
   cargo package --locked
   ```

   CI additionally builds and tests the PyO3 and NAPI packages and runs license,
   advisory, SBOM, and artifact scans.

## Tag and publish

After the release pull request is approved, CI is green, and the commit is on
`main`, manually dispatch the `Cargo Publish` workflow against that release
commit with `dry_run=true`. Manual dispatch skips the workflow's tag/version
check, so confirm that the selected ref is the intended release commit and that
its root `Cargo.toml` contains the intended version.

After the dry run succeeds, create an annotated `v<version>` tag on the release
commit. The tag must exactly match the root `Cargo.toml` version. Pushing the tag
immediately runs a real `cargo publish --registry jfrog`; the tag-triggered path
is not a dry run.

The tag triggers the repository's internal Cargo publication workflow. Public
packages are released through the hardened workflows in
`databricks/secure-public-registry-releases-eng`:

- `peco-databricks-sql-kernel-rust.yml` publishes the kernel crate to crates.io.
- `peco-databricks-sql-kernel-python.yml` publishes Python wheels and the source
  distribution to PyPI.
- `peco-databricks-sql-kernel-nodejs.yml` publishes the NAPI packages to npm.

Run each applicable public-package workflow in dry-run mode first. A real
publication requires the configured release-environment approval. The workflows
verify the tag and manifest versions, build on protected runners, scan the
assembled artifacts, publish from hardened runners, and create or update the
GitHub release from the component changelogs.

Rust ADBC is experimental and has no publication workflow. Do not publish it or
attach ADBC artifacts to a release.
