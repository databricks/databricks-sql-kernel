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

//! Pluggable OAuth token persistence.
//!
//! The U2M flow caches its refresh token so a browser login survives process
//! restarts. By default that is an encrypted on-disk cache ([`DiskTokenCache`],
//! under `~/.config/databricks-sql-kernel/oauth/`), but a caller can inject any
//! [`TokenPersistence`] — a database, an OS keychain, an in-memory store — via
//! the `token_store` field of
//! [`AuthConfig::OAuthU2M`](crate::config::AuthConfig). This is the kernel-side
//! contract the driver bindings map their `OAuthPersistence` (Node) /
//! `experimental_oauth_persistence` (Python) objects onto.

use crate::auth::oauth::cache::TokenCache;
use crate::auth::oauth::token::OAuthToken;
use crate::kernel_error::Result;

/// A store for U2M OAuth tokens, keyed by the `{host, client_id, scopes}`
/// triple.
///
/// The kernel calls [`load`](TokenPersistence::load) before deciding whether to
/// open a browser — a returned token carrying a refresh token is reused and
/// refreshed instead of prompting — and [`save`](TokenPersistence::save) after
/// every acquisition or refresh.
///
/// **Error handling is lenient by contract:** a `load` error is treated as a
/// cache miss (the kernel falls back to a fresh browser login and never fails
/// the connection on it), and a `save` error is logged and swallowed (a failed
/// cache write must not fail an otherwise-successful authentication). So
/// implementations may surface store/transport errors freely.
#[async_trait::async_trait]
pub trait TokenPersistence: Send + Sync + std::fmt::Debug {
    /// Load the cached token for this `{host, client_id, scopes}` key, or
    /// `Ok(None)` on a miss.
    async fn load(
        &self,
        host: &str,
        client_id: &str,
        scopes: &[String],
    ) -> Result<Option<OAuthToken>>;

    /// Persist `token` under this `{host, client_id, scopes}` key.
    async fn save(
        &self,
        host: &str,
        client_id: &str,
        scopes: &[String],
        token: &OAuthToken,
    ) -> Result<()>;
}

/// The default [`TokenPersistence`]: the built-in encrypted on-disk cache.
///
/// Wraps [`TokenCache`] with the resolved passphrase, preserving the kernel's
/// historical caching behavior exactly. [`Session::open`](crate::Session::open)
/// installs this when the caller supplies no custom `token_store` and on-disk
/// caching is enabled.
#[derive(Debug, Clone)]
pub struct DiskTokenCache {
    passphrase: String,
}

impl DiskTokenCache {
    /// Create a disk cache that encrypts entries under `passphrase`.
    pub fn new(passphrase: String) -> Self {
        Self { passphrase }
    }
}

#[async_trait::async_trait]
impl TokenPersistence for DiskTokenCache {
    async fn load(
        &self,
        host: &str,
        client_id: &str,
        scopes: &[String],
    ) -> Result<Option<OAuthToken>> {
        // `TokenCache::load` is synchronous, local, and fast (one small
        // encrypted file). The U2M paths that call this already run inside a
        // `block_in_place` region, so invoking it directly preserves the exact
        // prior behavior without an extra `spawn_blocking` hop.
        Ok(TokenCache::load(host, client_id, scopes, &self.passphrase))
    }

    async fn save(
        &self,
        host: &str,
        client_id: &str,
        scopes: &[String],
        token: &OAuthToken,
    ) -> Result<()> {
        TokenCache::save(host, client_id, scopes, token, &self.passphrase);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// In-memory [`TokenPersistence`] test double: records calls and serves
    /// tokens from a map keyed by `{host}|{client_id}|{scopes}`.
    #[derive(Debug, Default)]
    pub(crate) struct InMemoryTokenStore {
        pub tokens: Mutex<HashMap<String, OAuthToken>>,
        pub loads: Mutex<Vec<String>>,
        pub saves: Mutex<Vec<String>>,
    }

    fn key(host: &str, client_id: &str, scopes: &[String]) -> String {
        format!("{host}|{client_id}|{}", scopes.join(" "))
    }

    #[async_trait::async_trait]
    impl TokenPersistence for InMemoryTokenStore {
        async fn load(
            &self,
            host: &str,
            client_id: &str,
            scopes: &[String],
        ) -> Result<Option<OAuthToken>> {
            let k = key(host, client_id, scopes);
            self.loads.lock().unwrap().push(k.clone());
            Ok(self.tokens.lock().unwrap().get(&k).cloned())
        }

        async fn save(
            &self,
            host: &str,
            client_id: &str,
            scopes: &[String],
            token: &OAuthToken,
        ) -> Result<()> {
            let k = key(host, client_id, scopes);
            self.saves.lock().unwrap().push(k.clone());
            self.tokens.lock().unwrap().insert(k, token.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn in_memory_store_roundtrips() {
        let store = InMemoryTokenStore::default();
        let scopes = vec!["sql".to_string(), "offline_access".to_string()];
        assert!(store
            .load("https://h", "cid", &scopes)
            .await
            .unwrap()
            .is_none());

        let token = OAuthToken::new(
            "access".to_string(),
            "Bearer".to_string(),
            3600,
            Some("refresh".to_string()),
            scopes.clone(),
        );
        store
            .save("https://h", "cid", &scopes, &token)
            .await
            .unwrap();

        let got = store.load("https://h", "cid", &scopes).await.unwrap();
        assert_eq!(got.unwrap().access_token, "access");
        assert_eq!(store.loads.lock().unwrap().len(), 2);
        assert_eq!(store.saves.lock().unwrap().len(), 1);
    }

    #[test]
    fn disk_token_cache_is_debug_and_clone() {
        // Guards the `Debug` bound that `AuthorizationCodeProvider`'s derived
        // `Debug` and `AuthConfig`'s manual `Debug` rely on.
        let c = DiskTokenCache::new("pass".to_string());
        let _ = format!("{c:?}");
        let _ = c.clone();
    }

    /// The trait must stay object-safe (`Arc<dyn TokenPersistence>` is how it
    /// is threaded through `AuthConfig` and the provider).
    #[allow(dead_code)]
    fn assert_object_safe(_: Arc<dyn TokenPersistence>) {}
}
