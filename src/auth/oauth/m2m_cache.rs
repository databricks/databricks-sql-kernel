// Copyright (c) 2026 Databricks, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{oidc::OidcEndpoints, token_store::TokenStore};
use crate::client::http::HttpClientConfig;
use crate::kernel_error::{Error, Result};
use moka::{future::Cache, policy::EvictionPolicy};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::OnceCell;

pub(super) struct SharedM2MState {
    pub endpoints: OnceCell<OidcEndpoints>,
    pub tokens: Arc<TokenStore>,
}

type CacheKey = [u8; 32];

fn cache() -> &'static Cache<CacheKey, Arc<SharedM2MState>> {
    static CACHE: OnceLock<Cache<CacheKey, Arc<SharedM2MState>>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(256)
            .time_to_idle(Duration::from_secs(3600))
            .eviction_policy(EvictionPolicy::lru())
            .build()
    })
}

// Cache only token/discovery state; HTTP clients and runtime handles remain
// connection-owned. The salted digest keeps credentials out of stored keys.
pub(super) async fn shared_state(
    host: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
    token_endpoint: Option<&str>,
    resource: Option<&str>,
    http: &HttpClientConfig,
) -> Result<Arc<SharedM2MState>> {
    let host = reqwest::Url::parse(host)
        .map_err(|_| Error::invalid_argument().message("Invalid M2M workspace URL"))?;
    let mut scopes = scopes.to_vec();
    scopes.sort();
    scopes.dedup();
    let mut headers: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for (name, value) in &http.custom_headers {
        headers
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value);
    }
    let proxy = (
        &http.proxy.url,
        &http.proxy.username,
        &http.proxy.password,
        &http.proxy.bypass_hosts,
    );
    let tls = (
        http.tls.enabled.unwrap_or(true),
        http.tls.allow_self_signed.unwrap_or(false),
        http.tls.allow_hostname_mismatch.unwrap_or(false),
        &http.tls.trusted_certificate_path,
        &http.tls.trusted_certificate_pem,
        &http.tls.client_cert_pem,
        &http.tls.client_key_pem,
    );
    let identity = serde_json::to_vec(&(
        host.as_str().trim_end_matches('/'),
        &http.org_id,
        client_id,
        client_secret,
        scopes,
        token_endpoint,
        resource,
        proxy,
        tls,
        headers,
    ))
    .map_err(|_| Error::internal().message("Could not construct M2M cache identity"))?;
    static SALT: OnceLock<[u8; 32]> = OnceLock::new();
    let mut digest = Sha256::new();
    digest.update(SALT.get_or_init(rand::random));
    digest.update(identity);
    let key: CacheKey = digest.finalize().into();
    Ok(cache()
        .get_with(key, async {
            Arc::new(SharedM2MState {
                endpoints: OnceCell::new(),
                tokens: Arc::new(TokenStore::new()),
            })
        })
        .await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn equivalent_identities_share_state() {
        let http = HttpClientConfig::default();
        let first = shared_state(
            "https://CACHE.example.com:443/",
            "client",
            "secret",
            &["sql".into(), "all-apis".into()],
            None,
            None,
            &http,
        )
        .await
        .unwrap();
        let mut equivalent = http.clone();
        equivalent.read_timeout = Duration::from_secs(5);
        equivalent.tls.allow_self_signed = Some(false);
        let second = shared_state(
            "https://cache.example.com",
            "client",
            "secret",
            &["all-apis".into(), "sql".into(), "sql".into()],
            None,
            None,
            &equivalent,
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn identity_and_transport_changes_isolate_state() {
        let http = HttpClientConfig::default();
        let host = "https://isolated-cache.example.com";
        let scopes = vec!["all-apis".into()];
        let first = shared_state(host, "client", "secret", &scopes, None, None, &http)
            .await
            .unwrap();
        for (host, client, secret, scopes, endpoint, resource) in [
            (
                "https://other.example.com",
                "client",
                "secret",
                scopes.clone(),
                None,
                None,
            ),
            (host, "other", "secret", scopes.clone(), None, None),
            (host, "client", "rotated", scopes.clone(), None, None),
            (host, "client", "secret", vec!["sql".into()], None, None),
            (
                host,
                "client",
                "secret",
                scopes.clone(),
                Some("https://issuer.example.com/token"),
                None,
            ),
            (
                host,
                "client",
                "secret",
                scopes.clone(),
                None,
                Some("azure-resource"),
            ),
        ] {
            let other = shared_state(host, client, secret, &scopes, endpoint, resource, &http)
                .await
                .unwrap();
            assert!(!Arc::ptr_eq(&first, &other));
        }

        let mut variants = vec![http.clone(); 8];
        variants[0].org_id = Some("other-workspace".into());
        variants[1].proxy.url = Some("http://proxy.example.com:8080".into());
        variants[2].proxy.password = Some("proxy-secret".into());
        variants[3].tls.allow_self_signed = Some(true);
        variants[4].tls.allow_hostname_mismatch = Some(true);
        variants[5].tls.trusted_certificate_pem = Some(b"custom CA".to_vec());
        variants[6].tls.client_key_pem = Some(b"client key".to_vec());
        variants[7].custom_headers = vec![("X-Route".into(), "other".into())];
        for variant in variants {
            let other = shared_state(host, "client", "secret", &scopes, None, None, &variant)
                .await
                .unwrap();
            assert!(!Arc::ptr_eq(&first, &other));
        }
    }

    #[tokio::test]
    async fn duplicate_header_values_remain_part_of_identity() {
        let mut http = HttpClientConfig {
            custom_headers: vec![
                ("X-Route".into(), "first".into()),
                ("x-route".into(), "last".into()),
            ],
            ..HttpClientConfig::default()
        };
        let first = shared_state(
            "https://headers.example.com",
            "client",
            "secret",
            &[],
            None,
            None,
            &http,
        )
        .await
        .unwrap();
        http.custom_headers[0].1 = "different".into();
        let other = shared_state(
            "https://headers.example.com",
            "client",
            "secret",
            &[],
            None,
            None,
            &http,
        )
        .await
        .unwrap();
        assert!(!Arc::ptr_eq(&first, &other));
    }
}
