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
//! Moka supplies bounded LRU eviction, per-key cold-load single-flight, and
//! cancellation-safe coordination. Expiry is deliberately stored in the value
//! rather than configured on Moka: an expired value remains available as a
//! fail-open fallback while one caller refreshes it.

use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use moka::future::Cache;
use moka::ops::compute::Op;
use moka::policy::EvictionPolicy;

use super::FeatureFlagValues;

const DEFAULT_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_CACHE_ENTRIES: u64 = 256;

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

#[derive(Clone)]
struct CachedFeatureFlags {
    values: FeatureFlagValues,
    refresh_after: Instant,
}

impl CachedFeatureFlags {
    fn new(values: FeatureFlagValues, ttl: Duration) -> Self {
        let now = Instant::now();
        let refresh_after = now
            .checked_add(ttl)
            .or_else(|| now.checked_add(DEFAULT_TTL))
            .unwrap_or(now);
        Self {
            values,
            refresh_after,
        }
    }

    fn is_fresh(&self) -> bool {
        Instant::now() < self.refresh_after
    }
}

pub(crate) enum CacheLookup {
    Fresh(FeatureFlagValues),
    Stale(FeatureFlagValues),
    Missing,
}

pub(crate) struct FeatureFlagCache {
    entries: Cache<WorkspaceKey, CachedFeatureFlags>,
}

impl Default for FeatureFlagCache {
    fn default() -> Self {
        Self {
            entries: Cache::builder()
                // TinyLFU may reject a newly fetched workspace. LRU ensures a
                // session-open fetch is available to cache-only typed getters.
                .eviction_policy(EvictionPolicy::lru())
                .max_capacity(MAX_CACHE_ENTRIES)
                .build(),
        }
    }
}

impl FeatureFlagCache {
    pub(crate) async fn lookup(&self, key: &WorkspaceKey) -> CacheLookup {
        match self.entries.get(key).await {
            Some(entry) if entry.is_fresh() => CacheLookup::Fresh(entry.values),
            Some(entry) => CacheLookup::Stale(entry.values),
            None => CacheLookup::Missing,
        }
    }

    /// Read cached flags without starting network work.
    pub(crate) async fn current(&self, key: &WorkspaceKey) -> Option<FeatureFlagValues> {
        self.entries.get(key).await.map(|entry| entry.values)
    }

    /// Load a missing workspace. Moka coalesces concurrent loads and does not
    /// cache errors.
    pub(crate) async fn load<F, E>(
        &self,
        key: WorkspaceKey,
        loader: F,
    ) -> Result<FeatureFlagValues, Arc<E>>
    where
        F: Future<Output = Result<(FeatureFlagValues, Duration), E>>,
        E: Send + Sync + 'static,
    {
        self.entries
            .try_get_with(key, async move {
                let (values, ttl) = loader.await?;
                Ok(CachedFeatureFlags::new(values, ttl))
            })
            .await
            .map(|entry| entry.values)
    }

    /// Refresh an expired workspace without making concurrent stale readers
    /// wait. A failed refresh leaves the previous entry unchanged.
    pub(crate) async fn refresh<F, E>(
        &self,
        key: WorkspaceKey,
        loader: F,
    ) -> Result<Option<FeatureFlagValues>, E>
    where
        F: Future<Output = Result<(FeatureFlagValues, Duration), E>>,
        E: Send + Sync + 'static,
    {
        let result = self
            .entries
            .entry(key)
            .and_try_compute_if_nobody_else(move |entry| async move {
                if entry.as_ref().is_some_and(|entry| entry.value().is_fresh()) {
                    return Ok(Op::Nop);
                }

                let (values, ttl) = loader.await?;
                Ok(Op::Put(CachedFeatureFlags::new(values, ttl)))
            })
            .await?;

        Ok(result.into_entry().map(|entry| entry.into_value().values))
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

    #[test]
    fn workspace_cache_partition_prefers_id_and_falls_back_to_host() {
        assert_eq!(
            WorkspaceKey::new("https://first.example.com", Some("42".to_string())),
            WorkspaceKey::new("https://alias.example.com", Some("42".to_string()))
        );
        assert_ne!(
            WorkspaceKey::new("https://first.example.com", Some("42".to_string())),
            WorkspaceKey::new("https://first.example.com", Some("43".to_string()))
        );
        assert_ne!(
            WorkspaceKey::new("https://first.example.com", None),
            WorkspaceKey::new("https://second.example.com", None)
        );
    }

    #[test]
    fn response_ttl_uses_server_value() {
        for (supplied, expected) in [
            (None, DEFAULT_TTL),
            (Some(-1), DEFAULT_TTL),
            (Some(1), Duration::from_secs(1)),
            (Some(900), Duration::from_secs(900)),
        ] {
            assert_eq!(response_ttl(supplied), expected);
        }
    }

    #[test]
    fn oversized_ttl_does_not_panic() {
        let entry = CachedFeatureFlags::new(FeatureFlagValues::default(), Duration::MAX);
        assert!(entry.is_fresh());
    }
}
