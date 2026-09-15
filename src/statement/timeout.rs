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

//! Client query-timeout primitives shared by the Rust and C-ABI surfaces.

use crate::client::Transport;
use crate::kernel_error::{Error, ErrorCode, Result};
use crate::klog::warn;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Largest finite timeout, constrained to signed 64-bit nanoseconds.
pub const MAX_CLIENT_QUERY_TIMEOUT_MS: u64 = 9_223_372_036_854;

/// Rust form of [`MAX_CLIENT_QUERY_TIMEOUT_MS`].
pub const MAX_CLIENT_QUERY_TIMEOUT: Duration = Duration::from_millis(MAX_CLIENT_QUERY_TIMEOUT_MS);

pub(crate) const CLIENT_QUERY_TIMEOUT_SQL_STATE: &str = "HYT00";

/// Maximum detached cleanup tasks per session.
const MAX_CONCURRENT_TIMEOUT_CLEANUPS: usize = 64;

/// Independent budget for detached cancel and close.
const TIMEOUT_CLEANUP_BUDGET: Duration = Duration::from_secs(30);

/// Convert a public relative timeout into one absolute monotonic deadline.
///
/// Zero and [`Duration::MAX`] mean unlimited.
pub(crate) fn deadline_from_timeout(timeout: Duration) -> Result<Option<Instant>> {
    deadline_from_timeout_at(timeout, Instant::now())
}

/// Convert a timeout using a caller-supplied entry instant.
pub(crate) fn deadline_from_timeout_at(
    timeout: Duration,
    entered_at: Instant,
) -> Result<Option<Instant>> {
    if timeout.is_zero() || timeout == Duration::MAX {
        return Ok(None);
    }
    if timeout > MAX_CLIENT_QUERY_TIMEOUT {
        return Err(Error::invalid_argument().message(format!(
            "client query timeout exceeds the maximum of {MAX_CLIENT_QUERY_TIMEOUT_MS} ms"
        )));
    }
    entered_at
        .checked_add(timeout)
        .map(Some)
        .ok_or_else(|| Error::invalid_argument().message("client query timeout is out of range"))
}

pub(crate) fn client_query_timeout_error(query_id: Option<&str>) -> Error {
    let mut error = Error::timeout()
        .message("Client query timeout expired")
        .with_sql_state(CLIENT_QUERY_TIMEOUT_SQL_STATE)
        .with_retryable(false);
    if let Some(query_id) = query_id.filter(|id| !id.is_empty()) {
        error = error.with_query_id(query_id);
    }
    error
}

pub(crate) fn is_client_query_timeout(error: &Error) -> bool {
    error.code == ErrorCode::Timeout
        && error.sql_state.as_deref() == Some(CLIENT_QUERY_TIMEOUT_SQL_STATE)
        && !error.retryable
}

/// Per-session admission control for detached cleanup.
pub(crate) struct TimeoutCleanupManager {
    permits: Arc<tokio::sync::Semaphore>,
}

impl Default for TimeoutCleanupManager {
    fn default() -> Self {
        Self::with_capacity(MAX_CONCURRENT_TIMEOUT_CLEANUPS)
    }
}

impl TimeoutCleanupManager {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            permits: Arc::new(tokio::sync::Semaphore::new(capacity)),
        }
    }

    /// Spawn best-effort cleanup without waiting for capacity.
    pub(crate) fn dispatch(
        &self,
        runtime: &tokio::runtime::Handle,
        transport: Arc<dyn Transport>,
        statement_id: Option<String>,
    ) {
        let Some(statement_id) = statement_id.filter(|id| !id.is_empty()) else {
            return;
        };
        let permit = match Arc::clone(&self.permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    statement_id = %statement_id,
                    max_concurrent = MAX_CONCURRENT_TIMEOUT_CLEANUPS,
                    "client query-timeout cleanup saturated; dropping best-effort cancel/close"
                );
                return;
            }
        };

        runtime.spawn(async move {
            let _permit = permit;
            let cleanup = async {
                if let Err(error) = transport.cancel_statement(&statement_id).await {
                    warn!(
                        statement_id = %statement_id,
                        error = %error,
                        "client query-timeout cancel failed; continuing best-effort close"
                    );
                }
                if let Err(error) = transport.close_statement(&statement_id).await {
                    warn!(
                        statement_id = %statement_id,
                        error = %error,
                        "client query-timeout close failed; server will reclaim the statement"
                    );
                }
            };
            if tokio::time::timeout(TIMEOUT_CLEANUP_BUDGET, cleanup)
                .await
                .is_err()
            {
                warn!(
                    statement_id = %statement_id,
                    budget_ms = TIMEOUT_CLEANUP_BUDGET.as_millis(),
                    "client query-timeout cleanup exceeded its background budget"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ExecutionTimeout;

    #[test]
    fn zero_and_duration_max_are_unlimited() {
        for timeout in [Duration::ZERO, Duration::MAX] {
            let deadline = deadline_from_timeout(timeout).unwrap();
            assert_eq!(deadline, None);
            assert_eq!(
                ExecutionTimeout::from_explicit_deadline(deadline),
                ExecutionTimeout::Unlimited
            );
        }
    }

    #[test]
    fn supplied_entry_instant_is_the_deadline_origin() {
        let entered_at = Instant::now();
        let timeout = Duration::from_millis(25);
        assert_eq!(
            deadline_from_timeout_at(timeout, entered_at).unwrap(),
            entered_at.checked_add(timeout)
        );
    }

    #[test]
    fn finite_range_is_shared_with_c_abi() {
        assert!(deadline_from_timeout(MAX_CLIENT_QUERY_TIMEOUT).is_ok());
        let oversized = MAX_CLIENT_QUERY_TIMEOUT
            .checked_add(Duration::from_millis(1))
            .unwrap();
        let error = deadline_from_timeout(oversized).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn timeout_error_has_stable_client_contract() {
        let error = client_query_timeout_error(Some("stmt-123"));
        assert_eq!(error.code, ErrorCode::Timeout);
        assert_eq!(error.sql_state.as_deref(), Some("HYT00"));
        assert!(!error.retryable);
        assert_eq!(error.query_id.as_deref(), Some("stmt-123"));
    }

    #[test]
    fn cleanup_admission_is_bounded_and_non_blocking() {
        let manager = TimeoutCleanupManager::with_capacity(1);
        let permit = Arc::clone(&manager.permits).try_acquire_owned().unwrap();
        assert!(Arc::clone(&manager.permits).try_acquire_owned().is_err());
        drop(permit);
        assert!(Arc::clone(&manager.permits).try_acquire_owned().is_ok());
    }
}
