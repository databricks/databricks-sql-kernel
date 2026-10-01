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
//! pattern; this cache adds per-workspace keys and async waiter notification.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::FeatureFlagValues;

const DEFAULT_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_IDLE_CACHE_ENTRIES: usize = 256;

/// Stable process-cache partition for one workspace.
///
/// The numeric org/workspace id is preferred when the connection supplied
/// one. Normal warehouse HTTP paths often omit it, so the canonical workspace
/// host is the fallback. Driver type and version deliberately do not
/// participate: one loaded kernel belongs to one outer driver, while a process
/// may still open sessions to multiple workspaces.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum WorkspaceKey {
    Id(String),
    Host(String),
}

impl WorkspaceKey {
    pub(crate) fn new(host: impl Into<String>, workspace_id: Option<String>) -> Self {
        match workspace_id.filter(|id| !id.trim().is_empty()) {
            Some(id) => Self::Id(id),
            None => Self::Host(host.into()),
        }
    }
}

struct CacheEntry {
    values: Option<FeatureFlagValues>,
    refresh_after: Instant,
    refreshing: bool,
    waiters: Vec<oneshot::Sender<FeatureFlagValues>>,
    last_access: u64,
}

pub(crate) enum CacheLookup {
    Fresh(FeatureFlagValues),
    Stale {
        values: FeatureFlagValues,
        start_refresh: bool,
    },
    Load,
    Wait(oneshot::Receiver<FeatureFlagValues>),
}

pub(crate) struct FeatureFlagCache {
    entries: Mutex<HashMap<WorkspaceKey, CacheEntry>>,
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

    pub(crate) fn lookup(&self, key: &WorkspaceKey) -> CacheLookup {
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

    /// Read the latest value already stored for a workspace without starting
    /// network work. Typed getters use this after session-open preparation.
    pub(crate) fn current(&self, key: &WorkspaceKey) -> Option<FeatureFlagValues> {
        let last_access = self.next_access();
        let mut entries = self.lock_entries();
        let entry = entries.get_mut(key)?;
        entry.last_access = last_access;
        entry.values.clone()
    }

    pub(crate) fn refresh_guard(self: &Arc<Self>, key: WorkspaceKey) -> RefreshGuard {
        RefreshGuard {
            cache: self.clone(),
            key: Some(key),
        }
    }

    fn complete_success(
        &self,
        key: WorkspaceKey,
        values: FeatureFlagValues,
        ttl: Duration,
    ) -> FeatureFlagValues {
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
        entry.values = Some(values.clone());
        entry.refresh_after = now + ttl;
        entry.refreshing = false;
        let waiters = std::mem::take(&mut entry.waiters);
        self.trim_entries(&mut entries, self.idle_capacity, Some(&protected_key));
        drop(entries);

        for waiter in waiters {
            let _ = waiter.send(values.clone());
        }
        values
    }

    /// Do not cache failures. A cold failure removes the entry; a refresh
    /// failure retains its last good values with the original expired TTL so
    /// the next session open retries.
    fn complete_failure(&self, key: WorkspaceKey) -> FeatureFlagValues {
        let mut entries = self.lock_entries();
        let Some(mut entry) = entries.remove(&key) else {
            return FeatureFlagValues::default();
        };
        let values = entry.values.clone().unwrap_or_default();
        let waiters = std::mem::take(&mut entry.waiters);

        if entry.values.is_some() {
            entry.refreshing = false;
            entry.last_access = self.next_access();
            entries.insert(key.clone(), entry);
            self.trim_entries(&mut entries, self.idle_capacity, Some(&key));
        }
        drop(entries);

        for waiter in waiters {
            let _ = waiter.send(values.clone());
        }
        values
    }

    fn next_access(&self) -> u64 {
        self.access_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn trim_entries(
        &self,
        entries: &mut HashMap<WorkspaceKey, CacheEntry>,
        target_len: usize,
        protected: Option<&WorkspaceKey>,
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

    fn lock_entries(&self) -> MutexGuard<'_, HashMap<WorkspaceKey, CacheEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) struct RefreshGuard {
    cache: Arc<FeatureFlagCache>,
    key: Option<WorkspaceKey>,
}

impl RefreshGuard {
    pub(crate) fn success(mut self, values: FeatureFlagValues, ttl: Duration) -> FeatureFlagValues {
        self.cache.complete_success(
            self.key.take().expect("refresh guard is armed"),
            values,
            ttl,
        )
    }

    pub(crate) fn failure(mut self) -> FeatureFlagValues {
        self.cache
            .complete_failure(self.key.take().expect("refresh guard is armed"))
    }
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.cache.complete_failure(key);
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
    ttl_seconds
        .filter(|seconds| *seconds > 0)
        .map(|seconds| Duration::from_secs(seconds as u64))
        .unwrap_or(DEFAULT_TTL)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> WorkspaceKey {
        WorkspaceKey::Host(format!("https://{name}.example.com"))
    }

    fn values() -> FeatureFlagValues {
        FeatureFlagValues::from_entries([("test.flag".to_string(), "test-value".to_string())])
    }

    #[test]
    fn workspace_id_is_the_primary_cache_partition() {
        assert_eq!(
            WorkspaceKey::new("https://first.example.com", Some("42".to_string())),
            WorkspaceKey::new("https://alias.example.com", Some("42".to_string()))
        );
        assert_ne!(
            WorkspaceKey::new("https://first.example.com", Some("42".to_string())),
            WorkspaceKey::new("https://first.example.com", Some("43".to_string()))
        );
    }

    #[test]
    fn canonical_host_is_the_fallback_when_workspace_id_is_absent() {
        assert_ne!(
            WorkspaceKey::new("https://first.example.com", None),
            WorkspaceKey::new("https://second.example.com", None)
        );
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

        assert_eq!(waiter.await.unwrap().get("test.flag"), Some("test-value"));
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

        assert!(waiter.await.unwrap().is_empty());
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
    fn only_one_stale_reader_owns_refresh() {
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

        cache.refresh_guard(key).failure();
    }

    #[test]
    fn failed_stale_refresh_retains_values_and_next_lookup_retries() {
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
            cache.refresh_guard(key.clone()).failure().get("test.flag"),
            Some("test-value")
        );
        assert!(matches!(
            cache.lookup(&key),
            CacheLookup::Stale {
                start_refresh: true,
                ..
            }
        ));
        cache.refresh_guard(key).failure();
    }

    #[test]
    fn failed_cold_fetch_is_not_cached() {
        let cache = Arc::new(FeatureFlagCache::default());
        let key = key("failure");
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        assert!(cache.refresh_guard(key.clone()).failure().is_empty());
        assert!(matches!(cache.lookup(&key), CacheLookup::Load));
        cache.refresh_guard(key).failure();
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
    fn response_ttl_uses_server_value_exactly() {
        for (supplied, expected) in [
            (None, DEFAULT_TTL),
            (Some(-1), DEFAULT_TTL),
            (Some(1), Duration::from_secs(1)),
            (Some(900), Duration::from_secs(900)),
            (Some(86_400), Duration::from_secs(86_400)),
        ] {
            assert_eq!(response_ttl(supplied), expected);
        }
    }
}
