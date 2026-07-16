# NEXT CHANGELOG — Rust crate (`databricks-sql-kernel`)

This file stages unreleased changes for the next Rust crate release.
The Python (pyo3) and Node.js (napi) bindings stage their own
unreleased changes under [`pyo3/NEXT_CHANGELOG.md`](pyo3/NEXT_CHANGELOG.md)
and [`napi/NEXT_CHANGELOG.md`](napi/NEXT_CHANGELOG.md). Only land entries
here that affect the Rust crate's public API or behaviour; binding-only
changes go in their respective binding's NEXT_CHANGELOG.

## [Unreleased]

### Added

- Cooperative cancel token for the blocking C-ABI entry points
  (`kernel_cancel_token_new` / `_cancel` / `_is_cancelled` / `_free`), plus
  cancellable variants `kernel_session_open_cancellable` and
  `kernel_result_stream_next_batch_cancellable`. A host creates a token before a
  blocking call, passes it in, and fires it from another thread when its
  deadline elapses; the call drops its in-flight request future (a real
  `reqwest` abort) and returns `Cancelled`. A null token is byte-for-byte the
  plain path, so existing symbols and the default path are unchanged. Additive
  only — ABI unchanged.
- C ABI: `kernel_abi_version()` + the `DATABRICKS_KERNEL_ABI_VERSION` header
  macro — a consumer that links a prebuilt library can compare the two once at
  load time and refuse to run on a mismatch, guarding against a stale
  header/`.a` layout drift. Bumped in lockstep on any layout/meaning change.
- C ABI: `kernel_session_close_blocking()` — close a session and block until the
  server-side `DeleteSession` completes (reusing `Session::close().await`),
  instead of the fire-and-forget `kernel_session_close`, for a host that wants
  close to behave like the synchronous `DeleteSession` RPC the direct-Rust and
  pyo3/napi consumers get. Consumes the handle like the fire-and-forget sibling.
- C ABI: `kernel_session_config_set_tls_client_certificate()` — expose the
  crate's existing mTLS client cert/key (`TlsConfig::client_cert_pem` /
  `client_key_pem`) over the C ABI as a single paired setter. Both halves
  required together (cert-without-key is unrepresentable); the private key is
  never logged.
- C ABI: `kernel_session_config_set_cloudfetch_enabled()` — toggle CloudFetch
  (maps to `ResultConfig::cloudfetch_enabled`); `false` forces the inline-Arrow
  path (`can_cloud_download=false`) for all result sizes. `None` (unset) keeps
  the kernel default (on).
- C ABI: `kernel_set_log_callback(cb, ctx, level)` + the `kernel_log_callback`
  typedef — a reverse-call log sink so a C-ABI host (ODBC / Go) that can't
  install a Rust `tracing` subscriber still receives the kernel's
  `databricks*`-targeted log events, routed into its own logger instead of only
  reaching stderr via `kernel_init_logging`. Events are captured by a
  process-global layer, filtered by `level` (explicit level > `RUST_LOG` > the
  `warn` default — the same precedence as `kernel_init_logging`, so a host maps
  one knob), enqueued on a bounded channel (drop-on-full — logs never
  back-pressure a kernel hot path), and delivered from a dedicated drain thread.
  Mutually exclusive with `kernel_init_logging` (one process-global subscriber;
  the first to install wins, the other returns `Internal`); a NULL callback is
  rejected with `InvalidArgument`.

### Updated

### Fixed

---
*Note: When making changes, please add your change under the appropriate
section with a brief description. At release time, move the entries here
into a new versioned section at the top of `CHANGELOG.md` (heading format
`## [v<version>] - <YYYY-MM-DD>`) and reset this file back to the empty
template above.*
