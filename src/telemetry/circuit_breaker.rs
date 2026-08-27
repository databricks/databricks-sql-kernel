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
        if !config.circuit_breaker_enabled || config.circuit_breaker_threshold == 0 {
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

    fn before_export(&self) -> ExportPermit {
        let mut state = self.state.lock().expect("poisoned circuit breaker");
        match *state {
            CircuitState::Closed { .. } => ExportPermit::Allowed,
            CircuitState::Open { opened_at } => {
                if opened_at.elapsed() >= self.config.open_timeout {
                    *state = CircuitState::HalfOpen;
                    ExportPermit::Allowed
                } else {
                    ExportPermit::Dropped
                }
            }
            CircuitState::HalfOpen => ExportPermit::Dropped,
        }
    }

    fn record_success(&self) {
        let mut state = self.state.lock().expect("poisoned circuit breaker");
        *state = CircuitState::Closed {
            consecutive_failures: 0,
        };
    }

    fn record_failure(&self) {
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
            CircuitState::Open { .. } | CircuitState::HalfOpen => {
                *state = CircuitState::Open {
                    opened_at: Instant::now(),
                };
            }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExportPermit {
    Allowed,
    Dropped,
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
            if self.breaker.before_export() == ExportPermit::Dropped {
                tracing::debug!(
                    host = %self.host,
                    "telemetry circuit breaker open; dropping telemetry batch"
                );
                return Ok(());
            }

            match self.inner.export(request).await {
                Ok(()) => {
                    self.breaker.record_success();
                    Ok(())
                }
                Err(error) => {
                    self.breaker.record_failure();
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
