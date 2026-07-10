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
//! # Via environment variable
//! RUST_LOG=databricks_sql_kernel=debug ./my_app
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

use std::sync::OnceLock;
use tracing_subscriber::{
    fmt::{self, time::SystemTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

/// Records the outcome of the first `init_logging` call: `true` if the
/// subscriber was installed (or intentionally skipped for level=OFF),
/// `false` if it could not be installed (e.g. the log file failed to
/// open). Later calls return this same first-call outcome ("first call
/// wins").
static LOGGING_INITIALIZED: OnceLock<bool> = OnceLock::new();

/// Logging configuration passed via ADBC database options.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogConfig {
    /// Log level: "OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE".
    pub level: Option<String>,
    /// Log file path. If unset, logs go to stderr.
    pub file: Option<String>,
}

/// Initialize the tracing subscriber.
///
/// Uses `OnceLock` to ensure this runs at most once per process — the
/// **first** call configures logging and installs the subscriber; every
/// later call is a no-op that returns that **first call's** outcome (the
/// subscriber can only be installed once, so a later call's `config` — its
/// level / file — has no effect and its return does not describe its own
/// arguments).
///
/// Returns `true` if the first call installed the subscriber against the
/// requested destination (or was intentionally skipped for level `OFF`).
/// Returns `false` if that install did not fully honour the request —
/// either the requested log file could not be opened (logging fell back to
/// stderr) or the global subscriber was already installed by another
/// component (so no kernel subscriber was installed). Callers that cannot
/// see stderr (GUI / BI hosts) can use `false` to surface the degradation.
pub(crate) fn init_logging(config: &LogConfig) -> bool {
    *LOGGING_INITIALIZED.get_or_init(|| {
        // Check if the configured level is "OFF" — skip initialization
        // entirely, but treat it as a successful (intentional) outcome.
        if let Some(ref level) = config.level {
            if level.eq_ignore_ascii_case("off") {
                return true;
            }
        }

        // The filter directive MUST target the same string the kernel's
        // log macros emit with — `KERNEL_LOG_TARGET` ("databricks::sql::kernel").
        // `EnvFilter` matches by `::`-delimited segment prefix, so a
        // single token like "databricks_sql_kernel" would match NOTHING
        // and silently drop every kernel event.
        let filter = if let Some(ref level) = config.level {
            EnvFilter::new(format!("{}={}", crate::klog::KERNEL_LOG_TARGET, level.to_lowercase()))
        } else {
            // Fall back to RUST_LOG env var, default to warn
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new(format!("{}=warn", crate::klog::KERNEL_LOG_TARGET))
            })
        };

        // Try the requested file; on failure fall back to stderr WITHIN
        // this same call so logging still works (rather than committing the
        // OnceLock with no subscriber and locking out all later attempts).
        // Return `false` only to signal the requested file wasn't honoured.
        let file = config.file.as_ref().and_then(|path| {
            match std::fs::OpenOptions::new().create(true).append(true).open(path) {
                Ok(f) => Some(f),
                Err(e) => {
                    eprintln!("databricks-sql-kernel: failed to open log file {path}: {e}; falling back to stderr");
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
        // Success only if the subscriber installed AND the requested file
        // (if any) was honoured.
        installed && file_requested_ok
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_config_default() {
        let config = LogConfig::default();
        assert!(config.level.is_none());
        assert!(config.file.is_none());
    }

    #[test]
    fn test_log_config_with_values() {
        let config = LogConfig {
            level: Some("DEBUG".to_string()),
            file: Some("/tmp/test.log".to_string()),
        };
        assert_eq!(config.level.as_deref(), Some("DEBUG"));
        assert_eq!(config.file.as_deref(), Some("/tmp/test.log"));
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
        // The directive we build (see init_logging) must use this exact
        // target, not an underscore-joined token.
        let directive = format!("{}=debug", crate::klog::KERNEL_LOG_TARGET);
        assert!(directive.starts_with("databricks::sql::kernel="));
        // EnvFilter must accept the directive we construct.
        let _filter = EnvFilter::new(directive);
    }

    /// `init_logging` is idempotent in its return value: because the
    /// subscriber installs once per process, every call returns the SAME
    /// (first-call) outcome. We assert only that invariant — not a
    /// specific true/false — because the absolute value depends on whether
    /// some other test in the shared binary installed the global
    /// subscriber first (which would make this call's try_init fail). The
    /// first-call-installs-cleanly case is exercised end-to-end by the
    /// wiremock session tests.
    #[test]
    fn init_logging_return_is_idempotent() {
        let cfg = LogConfig {
            level: Some("warn".to_string()),
            file: None,
        };
        let first = init_logging(&cfg);
        // A later call with a different config must still return the first
        // outcome (its own args have no effect).
        let second = init_logging(&LogConfig {
            level: Some("debug".to_string()),
            file: Some("/nonexistent/dir/should-not-matter.log".to_string()),
        });
        assert_eq!(first, second, "later calls return the first-call outcome");
    }
}
