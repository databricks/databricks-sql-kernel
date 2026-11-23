# Contributing to Databricks SQL Kernel

We welcome contributions to Databricks SQL Kernel. Use
[GitHub Issues](https://github.com/databricks/databricks-sql-kernel/issues) to
report bugs or discuss features, and
[GitHub Pull Requests](https://github.com/databricks/databricks-sql-kernel/pulls)
to propose changes.

## Communication

Before starting a major feature or a change to user-facing behavior, open an
issue or contact [eng-oss-sql-driver@databricks.com](mailto:eng-oss-sql-driver@databricks.com)
so that maintainers and contributors can align on the design. Small bug fixes
and documentation improvements can be submitted directly as pull requests.

## Contribution workflow

1. Fork and clone the repository.
2. Create a branch for your change.
3. Add tests for behavioral changes.
4. Run the checks described below.
5. Commit with a DCO sign-off: `git commit -s`.
6. Push your branch and open a pull request.

Maintainers will review the pull request. Please respond to feedback and keep
the change focused on the issue it addresses.

## Build and test

Install the stable Rust toolchain, then run:

```bash
cargo build
cargo test --workspace
make check
```

`make check` runs formatting, Clippy, tests, and native ABI smoke tests. The
experimental ADBC workspace is validated separately by that target.

End-to-end tests that require a live Databricks connection are ignored by
default. Do not include credentials, connection strings, customer data, or
unredacted logs in issues or pull requests.

## Developer Certificate of Origin

Contributors must sign off every commit to certify that they have the right to
submit the contribution under this project's license. The complete terms are
in [DCO](DCO).

Configure your real name and email in Git, then sign commits with:

```bash
git commit -s -m "Your commit message"
```
