# Live and cross-driver validation

Offline tests run from the repository root with:

```text
cargo test -p databricks-adbc --features ffi
```

The ignored live smoke test reads credentials only at runtime:

```text
DATABRICKS_HOST=<workspace URL>
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse ID>
DATABRICKS_TOKEN_FILE=<path to a token file>
DATABRICKS_TEST_CATALOG=<optional metadata-test catalog; defaults to samples>
cargo test -p databricks-adbc --test live -- --ignored --exact select_one_fetches_the_complete_result
```

The test never prints or persists the token. Keep the credential file outside this repository.

Cross-driver CI integration is maintained in `databricks-driver-test`. It should
check out this repository and build the `adbc/` workspace member rather than
copying driver source into the test repository:

1. Point a temporary harness dependency at this crate; do not copy the driver into the test repository.
2. Keep the live-only mitmproxy runner and serial execution contract.
3. Run test groups classified as required or applicable for Rust ADBC.
4. Treat historical Rust statuses as evidence only.
5. Record semantic incompatibilities in the applicability ledger instead of modifying driver behavior to satisfy language- or transport-specific assertions.

Kernel-gap cases remain isolated until approved.
