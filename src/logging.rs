// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Logging configuration for the Databricks SQL Kernel.
//!
//! Initializes a `tracing-subscriber` with file or stderr output.
//! This path is for **direct-Rust and C-ABI (ODBC / Go) consumers**.
//!
//! ## Configuration priority
//!
//! 1. Kernel `LoggingConfig` (highest, when wired through `Session`)
//! 2. `RUST_LOG` environment variable
//! 3. Default: `warn`
//!
//! ## Usage
//!
//! ```bash
//! # Via environment variable. The kernel emits on the `::`-delimited
//! # target `databricks::sql::kernel`; `EnvFilter` matches by
//! # `::`-delimited segment prefix, so the underscore crate-name form
//! # (`databricks_sql_kernel=debug`) would match nothing.
//! RUST_LOG=databricks::sql::kernel=debug ./my_app
//! ```
//!
//! ## Relationship to the pyo3 / Python path
//!
//! The Python binding does **not** install this subscriber. The
//! `tracing` crate is built with its `log` feature (see `Cargo.toml`),
//! so when no `tracing` `Subscriber` is installed, every `tracing::*`
//! event also emits a `log::Record`. The pyo3 crate installs
//! [`pyo3_log`] as the `log` consumer and forwards records into
//! Python's `logging` module under the `databricks.sql.kernel` logger
//! (see `pyo3/src/logging.rs`).
//!
//! These two paths are **mutually exclusive**: installing the
//! subscriber below *suppresses* the `log` records pyo3 relies on
//! (that is the documented behaviour of the `tracing/log` feature, as
//! opposed to `log-always`). A single process therefore uses exactly
//! one — `init_logging` here for embedded Rust, or `pyo3_log` for
//! Python. Do not call `init_logging` from the pyo3 crate.

#![allow(dead_code)]

use std::ffi::{c_void, CString};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    fmt::{self, time::SystemTime},
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

/// Which kind of sink the first `init_*` call installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoggingSink {
    /// A file (or stderr-fallback) `fmt` subscriber, from [`init_logging`].
    FileOrStderr,
    /// A host-callback subscriber, from [`init_callback_logging`]. Carries the
    /// winning callback's identity — the function-pointer and `user_data`
    /// addresses — so a *later, different* callback registration can be told it
    /// did not win (install is first-call-wins) instead of a misleading success
    /// that would leave a host believing a dropped callback is live. A later
    /// registration of the *same* identity is the installed sink, so it still
    /// succeeds.
    Callback { callback: usize, user_data: usize },
}

/// Outcome of the process-global logging install, recorded once by the
/// **first** `init_*` call ("first call wins") and returned verbatim to every
/// later call. Replaces a lossy `bool` so a later initializer can report
/// precisely what happened: whether *its* kind of sink won, whether a
/// *different* kernel sink already won (so its own request had no effect), or
/// whether another component owns the global tracing slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoggingOutcome {
    /// The kernel installed a subscriber. `sink` is which kind; `file_honored`
    /// is false only when a requested log file could not be opened and logging
    /// fell back to stderr (meaningful for [`LoggingSink::FileOrStderr`]).
    Installed {
        sink: LoggingSink,
        file_honored: bool,
    },
    /// No kernel subscriber was installed: another component already owns the
    /// process-global tracing slot.
    Foreign,
}

/// Records the outcome of the first `init_*` call; later calls return this
/// same first-call outcome ("first call wins").
static LOGGING_INITIALIZED: OnceLock<LoggingOutcome> = OnceLock::new();

/// Default log file name created inside a destination that names a
/// directory (see [`resolve_log_file`]).
const DEFAULT_LOG_FILE_NAME: &str = "databricks-sql-kernel.log";

/// Resolved logging configuration.
///
/// `filter` is a **complete `EnvFilter` directive** (e.g.
/// `databricks::sql::kernel=debug`) — NOT a bare level. The two entry
/// points build it differently: the public `LoggingConfig.filter` already
/// carries a directive and is used verbatim, while the C ABI's bare level
/// is expanded to `{KERNEL_LOG_TARGET}={level}` by
/// [`filter_from_level`] before it reaches here.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogConfig {
    /// Full `EnvFilter` directive. If unset, the kernel honours `RUST_LOG`,
    /// falling back to `{KERNEL_LOG_TARGET}=warn`.
    pub filter: Option<String>,
    /// Log destination. If it names an existing directory (or ends with a
    /// path separator), a log file is created *inside* it; otherwise it is
    /// taken as the log file path. If unset, logs go to stderr.
    pub file: Option<PathBuf>,
}

/// Expand a bare level ("OFF"/"ERROR"/"WARN"/"INFO"/"DEBUG"/"TRACE") into a
/// full `EnvFilter` directive targeting the kernel's own log target.
///
/// The directive MUST target `KERNEL_LOG_TARGET` ("databricks::sql::kernel")
/// verbatim — `EnvFilter` matches by `::`-delimited segment prefix, so a
/// single token like "databricks_sql_kernel" would match NOTHING and
/// silently drop every kernel event.
pub(crate) fn filter_from_level(level: &str) -> String {
    format!(
        "{}={}",
        crate::klog::KERNEL_LOG_TARGET,
        level.to_lowercase()
    )
}

/// Resolve the shared default filter for every logging destination: honor
/// `RUST_LOG`, then fall back to warnings from the kernel target.
fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter_from_level("warn")))
}

/// Resolve a configured log destination to the actual file path to open.
///
/// A destination that names an **existing directory**, or that ends with a
/// path separator (a caller's directory-form intent even if the directory
/// does not exist yet), resolves to `<dir>/databricks-sql-kernel.log`
/// rather than being opened as a file — opening a directory as a file
/// would fail. This mirrors the reference driver's `LogPath` (directory)
/// vs `LogFile` (file) semantics on the kernel's single `file` field.
///
/// Any other value is returned unchanged and taken as the log file path.
pub(crate) fn resolve_log_file(path: &Path) -> PathBuf {
    // A trailing separator is a directory-form signal that survives even
    // when the directory does not exist yet: `Path::is_dir` would be false,
    // but the caller clearly meant "a directory". `components()` drops a
    // trailing separator, so a path like `/var/log/` yields a final
    // component of `log` — detect the raw trailing separator on the string.
    let has_trailing_sep = path
        .to_str()
        .map(|s| s.ends_with(std::path::MAIN_SEPARATOR) || s.ends_with('/'))
        .unwrap_or(false);

    if path.is_dir() || has_trailing_sep {
        path.join(DEFAULT_LOG_FILE_NAME)
    } else {
        path.to_path_buf()
    }
}

/// Initialize the tracing subscriber.
///
/// Uses `OnceLock` to ensure this runs at most once per process — the
/// **first** call configures logging and installs the subscriber; every
/// later call is a no-op that returns that **first call's** outcome (the
/// subscriber can only be installed once, so a later call's `config` — its
/// filter / file — has no effect and its return does not describe its own
/// arguments).
///
/// Returns the [`LoggingOutcome`] recorded by the **first** call: the
/// installed sink kind plus whether a requested file was honoured, or
/// [`LoggingOutcome::Foreign`] when another component already owns the global
/// tracing slot (so no kernel subscriber was installed). Callers that cannot
/// see stderr (GUI / BI hosts) can inspect it to surface a degradation.
pub(crate) fn init_logging(config: &LogConfig) -> LoggingOutcome {
    *LOGGING_INITIALIZED.get_or_init(|| {
        // Use the caller's full directive verbatim; otherwise fall back to
        // RUST_LOG, then to a warn default on the kernel's own target.
        let filter = if let Some(ref directive) = config.filter {
            EnvFilter::new(directive.clone())
        } else {
            default_env_filter()
        };

        // Resolve a directory-form destination to a file inside it, then try
        // to open it; on failure fall back to stderr WITHIN this same call so
        // logging still works (rather than committing the OnceLock with no
        // subscriber and locking out all later attempts). Return `false` only
        // to signal the requested file wasn't honoured.
        let file = config.file.as_ref().and_then(|path| {
            let resolved = resolve_log_file(path);
            match std::fs::OpenOptions::new().create(true).append(true).open(&resolved) {
                Ok(f) => Some(f),
                Err(e) => {
                    eprintln!("databricks-sql-kernel: failed to open log file {}: {e}; falling back to stderr", resolved.display());
                    None
                }
            }
        });
        let file_requested_ok = config.file.is_none() || file.is_some();

        // `try_init` returns Err if a global subscriber was already
        // installed by another component — in which case NO kernel
        // subscriber is installed and logs won't flow, so that must fold
        // into the outcome (not be swallowed with `.ok()`).
        let installed = if let Some(file) = file {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .with_writer(file)
                        .with_target(false)
                        .with_ansi(false)
                        .with_timer(SystemTime),
                )
                .try_init()
                .is_ok()
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .with_writer(std::io::stderr)
                        .with_target(false)
                        .with_timer(SystemTime),
                )
                .try_init()
                .is_ok()
        };
        if installed {
            LoggingOutcome::Installed {
                sink: LoggingSink::FileOrStderr,
                file_honored: file_requested_ok,
            }
        } else {
            LoggingOutcome::Foreign
        }
    })
}

/// Collect a tracing event's message and event-local fields into one line.
#[derive(Default)]
struct CallbackMessageVisitor {
    message: String,
    fields: String,
}

impl CallbackMessageVisitor {
    fn finish(self) -> String {
        if self.fields.is_empty() {
            self.message
        } else if self.message.is_empty() {
            self.fields
        } else {
            format!("{} {}", self.message, self.fields)
        }
    }

    fn push_field(&mut self, name: &str, value: String) {
        if !self.fields.is_empty() {
            self.fields.push(' ');
        }
        self.fields.push_str(name);
        self.fields.push('=');
        self.fields.push_str(&value);
    }
}

impl Visit for CallbackMessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.push_field(field.name(), format!("{value:?}"));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.push_field(field.name(), value.to_owned());
        }
    }
}

fn callback_cstring(value: &str) -> CString {
    CString::new(value).unwrap_or_else(|_| {
        let sanitized = value.replace('\0', "?");
        CString::new(sanitized).expect("replacing NUL produces a valid CString")
    })
}

struct CallbackLogLayer {
    callback: crate::c_abi::KernelLogCallback,
    user_data: usize,
}

thread_local! {
    /// Set while this thread is inside a callback delivery. If the host
    /// callback re-enters the kernel logging path (emits a kernel tracing
    /// event — a contract violation), the nested dispatch lands back in
    /// [`CallbackLogLayer::on_event`] on the same thread; this flag lets it
    /// drop the nested record instead of recursing without bound.
    static IN_CALLBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl<S> Layer<S> for CallbackLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // `on_event` runs inline on whatever thread emitted the record —
        // including tokio workers driving async FFI calls — so an unwind here
        // (a misbehaving callback, or a field `Debug` impl) would tear through
        // tracing's dispatch and abort in-flight work. Catch it at this
        // boundary, mirroring the `catch_ffi` / `block_on_ffi` guards on the
        // C-ABI entry points. A caught panic is dropped: it cannot be logged
        // from here without recursing into this same layer.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Reentrancy guard (see `IN_CALLBACK`): drop a nested record rather
            // than recurse. The `Drop` reset runs on the normal path and while
            // unwinding, so the flag never sticks `true`.
            if IN_CALLBACK.with(|active| active.replace(true)) {
                return;
            }
            struct ResetOnDrop;
            impl Drop for ResetOnDrop {
                fn drop(&mut self) {
                    IN_CALLBACK.with(|active| active.set(false));
                }
            }
            let _reset = ResetOnDrop;

            let metadata = event.metadata();
            let mut visitor = CallbackMessageVisitor::default();
            event.record(&mut visitor);

            let level = callback_cstring(&metadata.level().to_string().to_ascii_lowercase());
            let target = callback_cstring(metadata.target());
            let message = callback_cstring(&visitor.finish());

            // Deliver inline so hosts that want the same synchronous sink
            // semantics as their native logger do not lose or reorder records.
            // A host that prefers asynchronous delivery can enqueue in its
            // callback. SAFETY: the caller supplied a valid process-lifetime
            // callback and user-data pointer under `kernel_init_logging_callback`'s
            // contract. The C strings remain alive until the callback returns.
            unsafe {
                (self.callback)(
                    level.as_ptr(),
                    target.as_ptr(),
                    message.as_ptr(),
                    self.user_data as *mut c_void,
                );
            }
        }));
    }
}

/// Install a process-wide subscriber that forwards records to a host callback.
///
/// Shares [`LOGGING_INITIALIZED`] with file/stderr logging, preserving the
/// existing first-call-wins contract across every C and direct-Rust entry
/// point. Returns the [`LoggingOutcome`] of the **first** initializer: when a
/// file/stderr sink already won, that outcome names [`LoggingSink::FileOrStderr`]
/// and this callback was **not** installed (it will receive no records), so the
/// caller must not report success.
pub(crate) fn init_callback_logging(
    filter: Option<String>,
    callback: crate::c_abi::KernelLogCallback,
    user_data: *mut c_void,
) -> LoggingOutcome {
    *LOGGING_INITIALIZED.get_or_init(|| {
        let filter = filter.map_or_else(default_env_filter, EnvFilter::new);

        let installed = tracing_subscriber::registry()
            .with(filter)
            .with(CallbackLogLayer {
                callback,
                user_data: user_data as usize,
            })
            .try_init()
            .is_ok();
        if installed {
            LoggingOutcome::Installed {
                sink: LoggingSink::Callback {
                    callback: callback as usize,
                    user_data: user_data as usize,
                },
                file_honored: true,
            }
        } else {
            LoggingOutcome::Foreign
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_config_default() {
        let config = LogConfig::default();
        assert!(config.filter.is_none());
        assert!(config.file.is_none());
    }

    #[test]
    fn test_log_config_with_values() {
        let config = LogConfig {
            filter: Some("databricks::sql::kernel=debug".to_string()),
            file: Some(PathBuf::from("/tmp/test.log")),
        };
        assert_eq!(
            config.filter.as_deref(),
            Some("databricks::sql::kernel=debug")
        );
        assert_eq!(config.file.as_deref(), Some(Path::new("/tmp/test.log")));
    }

    /// A bare level expands to a full directive targeting the kernel's own
    /// log target — not an underscore-joined token that would match nothing.
    #[test]
    fn filter_from_level_targets_kernel_log_target() {
        assert_eq!(filter_from_level("DEBUG"), "databricks::sql::kernel=debug");
        assert_eq!(filter_from_level("warn"), "databricks::sql::kernel=warn");
        // The result must be a directive EnvFilter accepts.
        let _filter = EnvFilter::new(filter_from_level("INFO"));
    }

    /// A destination that names an existing directory resolves to a log
    /// file created inside it, not opened as a file.
    #[test]
    fn resolve_log_file_directory_resolves_inside() {
        let dir = std::env::temp_dir().join(format!(
            "kt_logtest_dir_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp log dir");

        let resolved = resolve_log_file(&dir);
        assert_eq!(resolved, dir.join(DEFAULT_LOG_FILE_NAME));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A path with a trailing separator is treated as a directory even when
    /// it does not exist yet (the caller's directory-form intent).
    #[test]
    fn resolve_log_file_trailing_separator_resolves_inside() {
        let base = std::env::temp_dir().join("kt_logtest_nonexistent_dir");
        let with_sep = PathBuf::from(format!("{}{}", base.display(), std::path::MAIN_SEPARATOR));
        let resolved = resolve_log_file(&with_sep);
        assert!(
            resolved.ends_with(DEFAULT_LOG_FILE_NAME),
            "trailing-separator destination must resolve to a file inside it, got {resolved:?}"
        );
    }

    /// A plain file path (no trailing separator, not an existing directory)
    /// is taken verbatim as the log file.
    #[test]
    fn resolve_log_file_file_path_unchanged() {
        let file = std::env::temp_dir().join("kt_logtest_explicit.log");
        assert_eq!(resolve_log_file(&file), file);
    }

    /// The filter directive must be built from `KERNEL_LOG_TARGET` so it
    /// actually matches the target the kernel's log macros emit with.
    /// A single-token filter like "databricks_sql_kernel" would match
    /// nothing (EnvFilter uses `::`-delimited prefix matching), silently
    /// dropping every kernel event. This pins the target string the
    /// filter is derived from so a regression to the wrong token is caught
    /// without depending on installing the process-global subscriber.
    #[test]
    fn log_target_is_double_colon_delimited() {
        assert_eq!(crate::klog::KERNEL_LOG_TARGET, "databricks::sql::kernel");
        // The directive we build (see default_env_filter) must use this exact
        // target, not an underscore-joined token.
        let directive = format!("{}=debug", crate::klog::KERNEL_LOG_TARGET);
        assert!(directive.starts_with("databricks::sql::kernel="));
        // EnvFilter must accept the directive we construct.
        let _filter = EnvFilter::new(directive);
    }

    // NOTE: `init_logging`'s once-per-process idempotency is NOT asserted
    // as a unit test here. Calling `init_logging` installs a global
    // `tracing` subscriber for the whole test binary, which suppresses the
    // `log`-bridge records the `klog` tests (in `klog::tests`, same binary)
    // depend on — a subscriber-installing unit test races those tests for
    // process-global logging state (see the CONSTRAINT note in
    // `klog::tests`). The idempotency invariant is instead exercised through
    // the public `Session::open` path in the `v0_logging_wiremock`
    // integration test, whose own binary is free to install a subscriber.
}
