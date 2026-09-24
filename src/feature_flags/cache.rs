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

//! Process-wide feature-flag cache.
//!
//! This follows the kernel's existing cache patterns: a `OnceLock` owns the
//! process-global instance, a short `Mutex<HashMap<...>>` critical section
//! makes each compound state transition atomic, and an RAII refresh guard
//! publishes a fail-open result if the loading future is cancelled. The OAuth
//! token store uses the same fresh/stale/single-flight lifecycle and guard
//! pattern; this cache adds per-identity keys and async waiter notification.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use rand::RngExt;
use tokio::sync::oneshot;

use super::{ClientIdentity, FeatureFlagValues};

const DEFAULT_TTL: Duration = Duration::from_secs(15 * 60);
pub(crate) const NEGATIVE_TTL: Duration = Duration::from_secs(60);
pub(crate) const TRANSIENT_NEGATIVE_TTL: Duration = Duration::from_secs(5);
const MAX_IDLE_CACHE_ENTRIES: usize = 256;
const MIN_JITTER_FACTOR: f64 = 0.9;
const MAX_JITTER_FACTOR: f64 = 1.0;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey {
    host: String,
    org_id: Option<String>,
    identity: ClientIdentity,
}

impl CacheKey {
    pub(crate) fn new(
        host: impl Into<String>,
        org_id: Option<String>,
        identity: ClientIdentity,
    ) -> Self {
        Self {
            host: host.into(),
            org_id,
            identity,
        }
    }
}

struct CacheEntry {
    values: Option<FeatureFlagValues>,
    refresh_after: Instant,
    refreshing: bool,
    waiters: Vec<oneshot::Sender<Option<FeatureFlagValues>>>,
    last_access: u64,
}

enum CacheCompletion {
    Success(FeatureFlagValues, Duration),
    Backoff(Duration),
}

pub(crate) enum CacheLookup {
    Fresh(FeatureFlagValues),
    Stale {
        values: FeatureFlagValues,
        start_refresh: bool,
    },
    Load,
    /// `Some(values)` completes the shared load. `None` asks the waiter to
    /// retry lookup because the leader failed for caller-local reasons or was
    /// cancelled.
    Wait(oneshot::Receiver<Option<FeatureFlagValues>>),
}

pub(crate) struct FeatureFlagCache {
    entries: Mutex<HashMap<CacheKey, CacheEntry>>,
    // Completed entries are kept within this bound. In-flight entries are
    // never evicted because doing so would break their single-flight waiters,
    // so concurrent cold loads may temporarily exceed it; each completion
    // trims the least-recently-used idle entries back to capacity.
    idle_capacity: usize,
    access_sequence: AtomicU64,
}

impl Default for FeatureFlagCache {
    fn default() -> Self {
        Self::with_idle_capacity(MAX_IDLE_CACHE_ENTRIES)
    }
}

impl FeatureFlagCache {
    fn with_idle_capacity(idle_capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            idle_capacity: idle_capacity.max(1),
            access_sequence: AtomicU64::new(0),
        }
    }

    pub(crate) fn lookup(&self, key: &CacheKey) -> CacheLookup {
        let now = Instant::now();
        let last_access = self.next_access();
        let mut entries = self.lock_entries();

        if let Some(entry) = entries.get_mut(key) {
            entry.last_access = last_access;
            if let Some(values) = entry.values.clone() {
                if now < entry.refresh_after {
                    return CacheLookup::Fresh(values);
                }
                let start_refresh = !entry.refreshing;
                entry.refreshing = true;
                return CacheLookup::Stale {
                    values,
                    start_refresh,
                };
            }

            let (sender, receiver) = oneshot::channel();
            entry.waiters.push(sender);
            return CacheLookup::Wait(receiver);
        }

        self.trim_entries(&mut entries, self.idle_capacity.saturating_sub(1), None);
        entries.insert(
            key.clone(),
            CacheEntry {
                values: None,
                refresh_after: now,
                refreshing: true,
                waiters: Vec::new(),
                last_access,
            },
        );
        CacheLookup::Load
    }

    pub(crate) fn refresh_guard(self: &Arc<Self>, key: CacheKey) -> RefreshGuard {
        RefreshGuard {
            cache: self.clone(),
            key: Some(key),
        }
    }

    fn complete_cached(&self, key: CacheKey, result: CacheCompletion) -> FeatureFlagValues {
        let now = Instant::now();
        let last_access = self.next_access();
        let protected_key = key.clone();
        let mut entries = self.lock_entries();
        let entry = entries.entry(key).or_insert_with(|| CacheEntry {
            values: None,
            refresh_after: now,
            refreshing: true,
            waiters: Vec::new(),
            last_access,
        });
        entry.last_access = last_access;

        let values = match result {
            CacheCompletion::Success(values, ttl) => {
                entry.values = Some(values.clone());
                entry.refresh_after = now + ttl;
                values
            }
            CacheCompletion::Backoff(ttl) => {
                let values = entry.values.clone().unwrap_or_default();
                entry.values = Some(values.clone());
                // Cache cold failures and back off stale refresh failures so a
                // connector-service outage cannot create a connection storm.
                entry.refresh_after = now + ttl;
                values
            }
        };
        entry.refreshing = false;
        let waiters = std::mem::take(&mut entry.waiters);
        self.trim_entries(&mut entries, self.idle_capacity, Some(&protected_key));
        drop(entries);

        for waiter in waiters {
            let _ = waiter.send(Some(values.clone()));
        }
        values
    }

    /// Complete a caller-local failure without publishing a process-wide
    /// negative result. Cold entries are removed and waiters retry so a bad
    /// token, proxy, or cancelled leader cannot suppress a healthy session.
    /// A stale entry keeps its last good values and briefly backs off so a
    /// persistent caller-local failure cannot start a refresh on every read.
    fn complete_uncached_failure(&self, key: CacheKey) -> FeatureFlagValues {
        let mut entries = self.lock_entries();
        let Some(mut entry) = entries.remove(&key) else {
            return FeatureFlagValues::default();
        };
        let values = entry.values.clone().unwrap_or_default();
        let waiters = std::mem::take(&mut entry.waiters);

        if entry.values.is_some() {
            entry.refreshing = false;
            entry.refresh_after = Instant::now() + TRANSIENT_NEGATIVE_TTL;
            entry.last_access = self.next_access();
            entries.insert(key.clone(), entry);
            self.trim_entries(&mut entries, self.idle_capacity, Some(&key));
        }
        drop(entries);

        for waiter in waiters {
            let _ = waiter.send(None);
        }
        values
    }

    fn next_access(&self) -> u64 {
        self.access_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn trim_entries(
        &self,
        entries: &mut HashMap<CacheKey, CacheEntry>,
        target_len: usize,
        protected: Option<&CacheKey>,
    ) {
        while entries.len() > target_len {
            let candidate = entries
                .iter()
                .filter(|(key, entry)| {
                    !entry.refreshing && protected.is_none_or(|protected| *key != protected)
                })
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| key.clone());
            let Some(candidate) = candidate else {
                break;
            };
            entries.remove(&candidate);
        }
    }

    fn lock_entries(&self) -> MutexGuard<'_, HashMap<CacheKey, CacheEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) struct RefreshGuard {
    cache: Arc<FeatureFlagCache>,
    key: Option<CacheKey>,
}

impl RefreshGuard {
    pub(crate) fn success(mut self, values: FeatureFlagValues, ttl: Duration) -> FeatureFlagValues {
        self.cache.complete_cached(
            self.key.take().expect("refresh guard is armed"),
            CacheCompletion::Success(values, ttl),
        )
    }

    pub(crate) fn backoff(self) -> FeatureFlagValues {
        self.backoff_for(NEGATIVE_TTL)
    }

    pub(crate) fn backoff_for(mut self, ttl: Duration) -> FeatureFlagValues {
        self.cache.complete_cached(
            self.key.take().expect("refresh guard is armed"),
            CacheCompletion::Backoff(ttl),
        )
    }

    pub(crate) fn retry(mut self) -> FeatureFlagValues {
        self.cache
            .complete_uncached_failure(self.key.take().expect("refresh guard is armed"))
    }
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            // Cancellation is local to the leader. Wake cold-miss followers
            // and let one retry instead of poisoning the process-wide cache.
            self.cache.complete_uncached_failure(key);
        }
    }
}

pub(crate) fn global_cache() -> Arc<FeatureFlagCache> {
    static CACHE: OnceLock<Arc<FeatureFlagCache>> = OnceLock::new();
    CACHE
        .get_or_init(|| Arc::new(FeatureFlagCache::default()))
        .clone()
}

pub(crate) fn response_ttl(ttl_seconds: Option<i64>) -> Duration {
    let seconds = ttl_seconds
        .filter(|seconds| *seconds > 0)
        .map(|seconds| seconds as f64)
        .unwrap_or(DEFAULT_TTL.as_secs_f64());
    // Refresh no later than the server's advertised TTL. Downward-only jitter
    // spreads callers without extending a rollout or kill-switch lifetime.
    // Missing and non-positive TTLs use the reference-driver default.
    let jitter = rand::rng().random_range(MIN_JITTER_FACTOR..=MAX_JITTER_FACTOR);
    Duration::from_secs_f64(seconds * jitter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feature_flags::ClientType;

    fn key(name: &str) -> CacheKey {
        CacheKey::new(
            format!("https://{name}.example.com"),
            None,
            ClientIdentity::new(ClientType::Go, "1.0.0"),
        )
    }

    fn values() -> FeatureFlagValues {
        FeatureFlagValues::from_entries([("test.flag".to_string(), "test-value".to_string())])
    }

    #[tokio::test]
    async fn cold_miss_is_single_flight_and_wakes_waiters() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("cold");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        let CacheLookup::Wait(waiter) = cache.lookup(&key) else {
            panic!("concurrent cold miss should wait");
        };

        cache
            .refresh_guard(key)
            .success(values(), Duration::from_secs(300));

        assert_eq!(
            waiter
                .await
                .unwrap()
                .expect("successful load should publish values")
                .get("test.flag"),
            Some("test-value")
        );
    }

    #[tokio::test]
    async fn cancelled_cold_load_fails_open_and_wakes_waiters() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("cancelled");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        let CacheLookup::Wait(waiter) = cache.lookup(&key) else {
            panic!("concurrent cold miss should wait");
        };

        drop(cache.refresh_guard(key.clone()));

        assert!(waiter.await.unwrap().is_none());
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
    }

    #[test]
    fn fresh_hit_does_not_start_refresh() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("fresh");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        cache
            .refresh_guard(key.clone())
            .success(values(), Duration::from_secs(300));

        assert!(matches!(cache.lookup(&key), CacheLookup::Fresh(_)));
    }

    #[test]
    fn only_one_stale_reader_owns_background_refresh() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("stale");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        cache
            .refresh_guard(key.clone())
            .success(values(), Duration::ZERO);

        assert!(matches!(
            cache.lookup(&key),
            CacheLookup::Stale {
                start_refresh: true,
                ..
            }
        ));
        assert!(matches!(
            cache.lookup(&key),
            CacheLookup::Stale {
                start_refresh: false,
                ..
            }
        ));

        cache.refresh_guard(key).backoff();
    }

    #[test]
    fn failed_stale_refresh_retains_last_values_and_backs_off() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("stale-failure");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        cache
            .refresh_guard(key.clone())
            .success(values(), Duration::ZERO);
        assert!(matches!(
            cache.lookup(&key),
            CacheLookup::Stale {
                start_refresh: true,
                ..
            }
        ));

        assert_eq!(
            cache.refresh_guard(key.clone()).backoff().get("test.flag"),
            Some("test-value")
        );
        assert!(matches!(cache.lookup(&key), CacheLookup::Fresh(_)));
    }

    #[test]
    fn failed_cold_fetch_is_negatively_cached() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("failure");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        assert!(cache.refresh_guard(key.clone()).backoff().is_empty());
        assert!(matches!(cache.lookup(&key), CacheLookup::Fresh(_)));
    }

    #[tokio::test]
    async fn caller_local_failure_promotes_a_waiter_instead_of_caching_default() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("local-failure");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        let CacheLookup::Wait(waiter) = cache.lookup(&key) else {
            panic!("concurrent cold miss should wait");
        };

        assert!(cache.refresh_guard(key.clone()).retry().is_empty());
        assert!(waiter.await.unwrap().is_none());
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
    }

    #[test]
    fn caller_local_stale_failure_retains_values_and_briefly_backs_off() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("local-stale-failure");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        cache
            .refresh_guard(key.clone())
            .success(values(), Duration::ZERO);
        assert!(matches!(
            cache.lookup(&key),
            CacheLookup::Stale {
                start_refresh: true,
                ..
            }
        ));

        assert_eq!(
            cache.refresh_guard(key.clone()).retry().get("test.flag"),
            Some("test-value")
        );
        assert!(matches!(cache.lookup(&key), CacheLookup::Fresh(_)));
    }

    #[test]
    fn cache_evicts_the_least_recently_used_idle_entry() {
        let cache = Arc::new(FeatureFlagCache::with_idle_capacity(2));
        let first = key("first");
        let second = key("second");
        let third = key("third");

        for key in [&first, &second] {
            assert!(matches!(cache.lookup(key), CacheLookup::Load));
            cache
                .refresh_guard(key.clone())
                .success(values(), Duration::from_secs(300));
        }
        assert!(matches!(cache.lookup(&first), CacheLookup::Fresh(_)));
        assert!(matches!(cache.lookup(&third), CacheLookup::Load));

        let entries = cache.lock_entries();
        assert_eq!(entries.len(), 2);
        assert!(entries.contains_key(&first));
        assert!(!entries.contains_key(&second));
        assert!(entries.contains_key(&third));
    }

    #[test]
    fn in_flight_overflow_shrinks_to_idle_capacity_after_completion() {
        let cache = Arc::new(FeatureFlagCache::with_idle_capacity(2));
        let keys = [key("first"), key("second"), key("third")];

        for key in &keys {
            assert!(matches!(cache.lookup(key), CacheLookup::Load));
        }
        assert_eq!(
            cache.lock_entries().len(),
            3,
            "in-flight loads must not be evicted"
        );

        for key in keys {
            cache
                .refresh_guard(key)
                .success(values(), Duration::from_secs(300));
        }
        assert_eq!(cache.lock_entries().len(), 2);
    }

    #[test]
    fn response_ttl_honors_server_values_with_downward_jitter() {
        for (supplied, advertised) in [
            (None, DEFAULT_TTL),
            (Some(-1), DEFAULT_TTL),
            (Some(1), Duration::from_secs(1)),
            (Some(900), Duration::from_secs(900)),
            (Some(86_400), Duration::from_secs(86_400)),
        ] {
            let ttl = response_ttl(supplied);
            assert!(ttl >= advertised.mul_f64(MIN_JITTER_FACTOR));
            assert!(ttl <= advertised);
        }
    }
}
