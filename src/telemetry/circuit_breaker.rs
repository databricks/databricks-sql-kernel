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

use super::exporter::{TelemetryExportResult, TelemetryExporter};
use super::models::TelemetryRequest;
use crate::config::TelemetryConfig;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CircuitBreakerConfig {
    failure_threshold: u32,
    open_timeout: Duration,
}

impl CircuitBreakerConfig {
    fn from_telemetry_config(config: &TelemetryConfig) -> Option<Self> {
        // Degrade a misconfigured breaker (threshold == 0 or timeout == 0) to
        // "no breaker" rather than disabling telemetry entirely. This mirrors
        // `valid_config`, which deliberately does not reject these knobs.
        if !config.circuit_breaker_enabled
            || config.circuit_breaker_threshold == 0
            || config.circuit_breaker_timeout.is_zero()
        {
            return None;
        }

        Some(Self {
            failure_threshold: config.circuit_breaker_threshold,
            open_timeout: config.circuit_breaker_timeout,
        })
    }
}

#[derive(Debug)]
struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<CircuitState>,
}

impl CircuitBreaker {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Mutex::new(CircuitState::Closed {
                consecutive_failures: 0,
            }),
        }
    }

    /// Acquire a permit for one export attempt.
    ///
    /// Returns `Some(guard)` when the export is allowed and `None` when the
    /// circuit is open (or already probing in half-open). The returned
    /// [`ExportGuard`] must be resolved with `record_success`/`record_failure`;
    /// if it is dropped without an outcome — e.g. the flush future is cancelled
    /// by a `tokio::time::timeout` or an aborted timer task — a half-open probe
    /// re-arms the open timeout on drop instead of wedging the shared breaker in
    /// `HalfOpen` forever.
    fn before_export(&self) -> Option<ExportGuard<'_>> {
        let mut state = self.state.lock().expect("poisoned circuit breaker");
        match *state {
            CircuitState::Closed { .. } => Some(ExportGuard::new(self, false)),
            CircuitState::Open { opened_at } => {
                if opened_at.elapsed() >= self.config.open_timeout {
                    *state = CircuitState::HalfOpen;
                    Some(ExportGuard::new(self, true))
                } else {
                    None
                }
            }
            CircuitState::HalfOpen => None,
        }
    }

    /// Re-arm the open timeout after a half-open probe was abandoned without an
    /// outcome (a cancelled/dropped export future). Only acts while the breaker
    /// is still `HalfOpen`, so a concurrently-recorded outcome wins.
    fn rearm_cancelled_probe(&self) {
        let mut state = self.state.lock().expect("poisoned circuit breaker");
        if matches!(*state, CircuitState::HalfOpen) {
            *state = CircuitState::Open {
                opened_at: Instant::now(),
            };
        }
    }

    fn record_success(&self, is_probe: bool) {
        let mut state = self.state.lock().expect("poisoned circuit breaker");
        match *state {
            CircuitState::Closed { .. } => {
                // A success from a closed-era guard confirms health; clear any
                // accumulated consecutive-failure count.
                *state = CircuitState::Closed {
                    consecutive_failures: 0,
                };
            }
            CircuitState::HalfOpen if is_probe => {
                // The half-open probe succeeded; the endpoint is healthy again.
                *state = CircuitState::Closed {
                    consecutive_failures: 0,
                };
            }
            // Mirror `record_failure`'s guard against late outcomes: a
            // non-probe success arriving while the circuit is `Open` (or a
            // probe is already in flight in `HalfOpen`) is a stale outcome from
            // a guard admitted while `Closed`, before a concurrent failure
            // burst opened the circuit. Closing here would clobber the open
            // window and bypass the half-open single-probe recovery gate, so
            // leave the current state untouched.
            CircuitState::HalfOpen | CircuitState::Open { .. } => {}
        }
    }

    fn record_failure(&self, is_probe: bool) {
        let mut state = self.state.lock().expect("poisoned circuit breaker");
        match *state {
            CircuitState::Closed {
                consecutive_failures,
            } => {
                let failures = consecutive_failures.saturating_add(1);
                if failures >= self.config.failure_threshold {
                    *state = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                } else {
                    *state = CircuitState::Closed {
                        consecutive_failures: failures,
                    };
                }
            }
            CircuitState::HalfOpen if is_probe => {
                // A failed half-open probe reopens the circuit, arming a fresh
                // recovery timeout from now.
                *state = CircuitState::Open {
                    opened_at: Instant::now(),
                };
            }
            // Mirror `record_success`'s guard against late outcomes: a non-probe
            // failure arriving while the circuit is `HalfOpen` (or `Open`) is a
            // stale outcome from a guard admitted while `Closed`, before a
            // concurrent failure burst opened the circuit. Re-arming here would
            // rewrite `opened_at` — in `HalfOpen` it would clobber a genuine
            // in-flight probe (defeating a successful recovery and delaying it a
            // full `open_timeout`); in `Open` a burst of such stragglers would
            // each push the recovery timer forward. Leave the state untouched.
            CircuitState::HalfOpen | CircuitState::Open { .. } => {}
        }
    }

    #[cfg(test)]
    fn state_for_test(&self) -> CircuitBreakerState {
        match *self.state.lock().expect("poisoned circuit breaker") {
            CircuitState::Closed { .. } => CircuitBreakerState::Closed,
            CircuitState::Open { .. } => CircuitBreakerState::Open,
            CircuitState::HalfOpen => CircuitBreakerState::HalfOpen,
        }
    }
}

#[derive(Debug)]
enum CircuitState {
    Closed { consecutive_failures: u32 },
    Open { opened_at: Instant },
    HalfOpen,
}

/// RAII permit for a single in-flight export.
///
/// Resolve it with [`ExportGuard::record_success`] / [`ExportGuard::record_failure`]
/// once the export completes. If the guard is dropped without an explicit
/// outcome (the export future was cancelled mid-`await`) and it was a half-open
/// probe, the drop re-arms the open timeout so the shared breaker cannot be
/// pinned in `HalfOpen` forever.
struct ExportGuard<'a> {
    breaker: &'a CircuitBreaker,
    is_probe: bool,
    resolved: bool,
}

impl<'a> ExportGuard<'a> {
    fn new(breaker: &'a CircuitBreaker, is_probe: bool) -> Self {
        Self {
            breaker,
            is_probe,
            resolved: false,
        }
    }

    fn record_success(mut self) {
        self.resolved = true;
        self.breaker.record_success(self.is_probe);
    }

    fn record_failure(mut self) {
        self.resolved = true;
        self.breaker.record_failure(self.is_probe);
    }
}

impl Drop for ExportGuard<'_> {
    fn drop(&mut self) {
        if !self.resolved && self.is_probe {
            self.breaker.rearm_cancelled_probe();
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// Process-global registry of per-host circuit breakers.
///
/// The `breakers` map is keyed by `(host, config)` and holds one entry per
/// distinct host a process talks to. Entries are never evicted, so the map
/// grows monotonically for the process lifetime. This is intentional and
/// bounded in practice: a kernel process connects to a small, stable set of
/// Databricks workspaces, and each entry is tiny (a `String` key plus an
/// `Arc<CircuitBreaker>` holding a `Mutex<CircuitState>`). If a future
/// deployment ever exhibits high host cardinality, add an LRU bound or
/// idle-based eviction here.
#[derive(Default)]
struct CircuitBreakerManager {
    breakers: Mutex<HashMap<(String, CircuitBreakerConfig), Arc<CircuitBreaker>>>,
}

impl CircuitBreakerManager {
    fn global() -> &'static Self {
        static INSTANCE: OnceLock<CircuitBreakerManager> = OnceLock::new();
        INSTANCE.get_or_init(Self::default)
    }

    fn get_or_create(&self, host: &str, config: CircuitBreakerConfig) -> Arc<CircuitBreaker> {
        let mut breakers = self
            .breakers
            .lock()
            .expect("poisoned telemetry circuit breakers");
        Arc::clone(
            breakers
                .entry((host.to_string(), config))
                .or_insert_with(|| Arc::new(CircuitBreaker::new(config))),
        )
    }
}

pub(super) fn maybe_wrap_exporter(
    host: &str,
    config: &TelemetryConfig,
    exporter: Arc<dyn TelemetryExporter>,
) -> Arc<dyn TelemetryExporter> {
    let Some(circuit_config) = CircuitBreakerConfig::from_telemetry_config(config) else {
        return exporter;
    };

    Arc::new(CircuitBreakerTelemetryExporter::new(
        exporter,
        host,
        circuit_config,
    ))
}

struct CircuitBreakerTelemetryExporter {
    inner: Arc<dyn TelemetryExporter>,
    host: String,
    breaker: Arc<CircuitBreaker>,
}

impl CircuitBreakerTelemetryExporter {
    fn new(inner: Arc<dyn TelemetryExporter>, host: &str, config: CircuitBreakerConfig) -> Self {
        Self {
            inner,
            host: host.to_string(),
            breaker: CircuitBreakerManager::global().get_or_create(host, config),
        }
    }
}

impl TelemetryExporter for CircuitBreakerTelemetryExporter {
    fn export<'a>(
        &'a self,
        request: TelemetryRequest,
    ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
        Box::pin(async move {
            let Some(guard) = self.breaker.before_export() else {
                tracing::debug!(
                    host = %self.host,
                    "telemetry circuit breaker open; dropping telemetry batch"
                );
                return Ok(());
            };

            match self.inner.export(request).await {
                Ok(()) => {
                    guard.record_success();
                    Ok(())
                }
                Err(error) => {
                    guard.record_failure();
                    Err(error)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::exporter::{TelemetryExportError, TelemetryExportResult};
    use crate::telemetry::models::TelemetryRequest;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn opens_after_configured_failures_and_drops_while_open() {
        let inner = Arc::new(FailingExporter::default());
        let exporter = CircuitBreakerTelemetryExporter::new(
            inner.clone(),
            unique_host("open"),
            CircuitBreakerConfig {
                failure_threshold: 2,
                open_timeout: Duration::from_secs(60),
            },
        );

        assert!(exporter.export(sample_request()).await.is_err());
        assert_eq!(
            exporter.breaker.state_for_test(),
            CircuitBreakerState::Closed
        );
        assert!(exporter.export(sample_request()).await.is_err());
        assert_eq!(exporter.breaker.state_for_test(), CircuitBreakerState::Open);

        assert!(exporter.export(sample_request()).await.is_ok());
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            2,
            "open circuit should drop without invoking inner exporter"
        );
    }

    #[tokio::test]
    async fn half_open_success_closes_breaker_after_timeout() {
        let inner = Arc::new(ScriptedExporter::new(vec![false, true]));
        let exporter = CircuitBreakerTelemetryExporter::new(
            inner.clone(),
            unique_host("recover"),
            CircuitBreakerConfig {
                failure_threshold: 1,
                open_timeout: Duration::from_millis(10),
            },
        );

        assert!(exporter.export(sample_request()).await.is_err());
        assert_eq!(exporter.breaker.state_for_test(), CircuitBreakerState::Open);

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(exporter.export(sample_request()).await.is_ok());

        assert_eq!(
            exporter.breaker.state_for_test(),
            CircuitBreakerState::Closed
        );
        assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn half_open_failure_reopens_breaker() {
        let inner = Arc::new(FailingExporter::default());
        let exporter = CircuitBreakerTelemetryExporter::new(
            inner.clone(),
            unique_host("reopen"),
            CircuitBreakerConfig {
                failure_threshold: 1,
                open_timeout: Duration::from_millis(10),
            },
        );

        assert!(exporter.export(sample_request()).await.is_err());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(exporter.export(sample_request()).await.is_err());

        assert_eq!(exporter.breaker.state_for_test(), CircuitBreakerState::Open);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancelled_half_open_probe_rearms_open_timeout() {
        // Regression: a half-open probe future dropped mid-`await` (e.g. a
        // flush cancelled by `tokio::time::timeout` or an aborted timer task)
        // must not wedge the shared, process-global breaker in HalfOpen forever.
        let host = unique_host("cancelled-probe");
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            open_timeout: Duration::from_millis(10),
        };

        // Trip the breaker open through a failing exporter.
        let failing = Arc::new(FailingExporter::default());
        let failing_exporter = CircuitBreakerTelemetryExporter::new(failing.clone(), host, config);
        assert!(failing_exporter.export(sample_request()).await.is_err());
        assert_eq!(
            failing_exporter.breaker.state_for_test(),
            CircuitBreakerState::Open
        );

        // A second exporter to the same host+config shares the same breaker.
        let slow = Arc::new(SlowExporter::default());
        let slow_exporter = CircuitBreakerTelemetryExporter::new(slow.clone(), host, config);

        // After the timeout, drive a half-open probe and cancel it mid-flight via
        // `timeout` — this transitions the breaker to HalfOpen and then drops the
        // export future while it awaits the inner exporter, exactly like the
        // flush-timeout / aborted-timer paths.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let cancelled = tokio::time::timeout(
            Duration::from_millis(10),
            slow_exporter.export(sample_request()),
        )
        .await;
        assert!(cancelled.is_err(), "probe should have been cancelled");

        // The breaker must be back to Open (timeout re-armed), not stuck HalfOpen.
        assert_eq!(
            slow_exporter.breaker.state_for_test(),
            CircuitBreakerState::Open
        );
        assert_eq!(
            slow.calls.load(Ordering::SeqCst),
            1,
            "slow probe should have started exactly once"
        );
    }

    #[test]
    fn late_closed_era_success_does_not_clobber_open_circuit() {
        // A guard admitted while `Closed` can still be in flight when a
        // concurrent failure burst opens the circuit. If that stale guard then
        // succeeds late, closing the breaker would re-admit full traffic and
        // bypass the half-open single-probe gate. It must be ignored while Open.
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            open_timeout: Duration::from_secs(60),
        });

        // A closed-era (non-probe) guard is admitted, then a failure opens the
        // circuit before that guard resolves.
        let stale = breaker.before_export().expect("closed admits export");
        assert!(!stale.is_probe);
        breaker.record_failure(false);
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::Open);

        // The stale guard now succeeds late; the breaker must stay Open.
        stale.record_success();
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::Open);
    }

    #[test]
    fn late_probe_success_does_not_reset_freshly_reopened_circuit() {
        // A half-open probe is admitted, the circuit reopens (e.g. a failed
        // second probe or a re-arm), and only then the first probe succeeds
        // late. That stale success must not reset the fresh open window.
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            open_timeout: Duration::from_millis(1),
        });

        breaker.record_failure(false);
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::Open);

        std::thread::sleep(Duration::from_millis(5));
        let probe = breaker.before_export().expect("half-open admits one probe");
        assert!(probe.is_probe);
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::HalfOpen);

        // The circuit reopens underneath the in-flight probe.
        breaker.rearm_cancelled_probe();
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::Open);

        // The stale probe's late success must not clobber the fresh open window.
        probe.record_success();
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::Open);
    }

    #[test]
    fn late_closed_era_failure_does_not_defeat_in_flight_probe() {
        // A guard admitted while `Closed` can still be in flight when a
        // concurrent failure burst opens the circuit and the half-open probe is
        // admitted. If that stale (non-probe) guard then fails late, re-arming
        // the open window would clobber the genuine in-flight probe and delay
        // recovery by a full `open_timeout`. It must be ignored while HalfOpen.
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            open_timeout: Duration::from_millis(1),
        });

        // A closed-era (non-probe) guard is admitted before the circuit opens.
        let stale = breaker.before_export().expect("closed admits export");
        assert!(!stale.is_probe);

        // A concurrent failure opens the circuit; the timeout then elapses and a
        // real half-open probe is admitted.
        breaker.record_failure(false);
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::Open);
        std::thread::sleep(Duration::from_millis(5));
        let probe = breaker.before_export().expect("half-open admits one probe");
        assert!(probe.is_probe);
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::HalfOpen);

        // The stale closed-era guard fails late; it must NOT reopen the circuit
        // or disturb the in-flight probe.
        stale.record_failure();
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::HalfOpen);

        // The genuine probe then succeeds and recovery closes the circuit.
        probe.record_success();
        assert_eq!(breaker.state_for_test(), CircuitBreakerState::Closed);
    }

    #[tokio::test]
    async fn different_hosts_have_independent_breakers() {
        let failing = Arc::new(FailingExporter::default());
        let failing_exporter = CircuitBreakerTelemetryExporter::new(
            failing.clone(),
            unique_host("failing"),
            CircuitBreakerConfig {
                failure_threshold: 1,
                open_timeout: Duration::from_secs(60),
            },
        );
        let healthy = Arc::new(CapturingExporter::default());
        let healthy_exporter = CircuitBreakerTelemetryExporter::new(
            healthy.clone(),
            unique_host("healthy"),
            CircuitBreakerConfig {
                failure_threshold: 1,
                open_timeout: Duration::from_secs(60),
            },
        );

        assert!(failing_exporter.export(sample_request()).await.is_err());
        assert_eq!(
            failing_exporter.breaker.state_for_test(),
            CircuitBreakerState::Open
        );

        assert!(healthy_exporter.export(sample_request()).await.is_ok());
        assert_eq!(
            healthy_exporter.breaker.state_for_test(),
            CircuitBreakerState::Closed
        );
        assert_eq!(healthy.calls.load(Ordering::SeqCst), 1);
    }

    fn sample_request() -> TelemetryRequest {
        TelemetryRequest {
            upload_time: 1,
            proto_logs: vec!["{}".to_string()],
        }
    }

    fn unique_host(suffix: &str) -> &'static str {
        Box::leak(format!("telemetry-{suffix}-{}", std::process::id()).into_boxed_str())
    }

    #[derive(Default)]
    struct FailingExporter {
        calls: AtomicUsize,
    }

    impl TelemetryExporter for FailingExporter {
        fn export<'a>(
            &'a self,
            _request: TelemetryRequest,
        ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(TelemetryExportError::Export("boom".to_string()))
            })
        }
    }

    struct ScriptedExporter {
        calls: AtomicUsize,
        outcomes: Mutex<Vec<bool>>,
    }

    impl ScriptedExporter {
        fn new(outcomes: Vec<bool>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcomes: Mutex::new(outcomes),
            }
        }
    }

    impl TelemetryExporter for ScriptedExporter {
        fn export<'a>(
            &'a self,
            _request: TelemetryRequest,
        ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let ok = self.outcomes.lock().expect("poisoned outcomes").remove(0);
                if ok {
                    Ok(())
                } else {
                    Err(TelemetryExportError::Export("boom".to_string()))
                }
            })
        }
    }

    /// Records that it started, then never resolves — used to hold a half-open
    /// probe open across a cancellation boundary.
    #[derive(Default)]
    struct SlowExporter {
        calls: AtomicUsize,
    }

    impl TelemetryExporter for SlowExporter {
        fn export<'a>(
            &'a self,
            _request: TelemetryRequest,
        ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CapturingExporter {
        calls: AtomicUsize,
    }

    impl TelemetryExporter for CapturingExporter {
        fn export<'a>(
            &'a self,
            _request: TelemetryRequest,
        ) -> Pin<Box<dyn Future<Output = TelemetryExportResult> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }
}
