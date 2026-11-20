use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::runtime::{Handle, Id};
use tokio::time::Instant;

use super::http::HttpClientConfig;
use crate::Result;

const MAX_RUNTIMES: usize = 16;
const MAX_CLIENTS: usize = 16;
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const SWEEP_INTERVAL: Duration = Duration::from_secs(15);
const PROXY_ENV: [&str; 8] = [
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

#[derive(PartialEq, Eq)]
struct PoolKey {
    host: String,
    config: HttpClientConfig,
    proxy_env: [Option<OsString>; PROXY_ENV.len()],
    root_file: Option<(OsString, [u8; 32])>,
}

impl PoolKey {
    fn new(host: &str, config: &HttpClientConfig) -> Option<Self> {
        let root_file = match std::env::var_os("SSL_CERT_FILE") {
            Some(path) => {
                let contents = std::fs::read(&path).ok()?;
                Some((path, Sha256::digest(&contents).into()))
            }
            None => None,
        };
        Some(Self {
            host: host.to_owned(),
            config: config.clone(),
            proxy_env: PROXY_ENV.map(std::env::var_os),
            root_file,
        })
    }
}

struct Entry {
    key: PoolKey,
    client: Client,
    last_used: Instant,
}

#[derive(Default)]
struct ClientPool {
    entries: Mutex<Vec<Entry>>,
}

impl ClientPool {
    fn get(&self, key: &PoolKey) -> Option<Client> {
        let now = Instant::now();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        entries
            .iter_mut()
            .find(|entry| entry.key == *key && now.duration_since(entry.last_used) < IDLE_TIMEOUT)
            .map(|entry| {
                entry.last_used = now;
                entry.client.clone()
            })
    }

    fn get_or_build(&self, key: PoolKey, build: impl FnOnce() -> Result<Client>) -> Result<Client> {
        if let Some(client) = self.get(&key) {
            return Ok(client);
        }
        let client = build()?;
        let now = Instant::now();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.key == key && now.duration_since(entry.last_used) < IDLE_TIMEOUT)
        {
            entry.last_used = now;
            return Ok(entry.client.clone());
        }
        let evicted = if entries.len() == MAX_CLIENTS {
            let oldest = entries
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(index, _)| index)
                .unwrap_or(0);
            Some(entries.swap_remove(oldest))
        } else {
            None
        };
        entries.push(Entry {
            key,
            client: client.clone(),
            last_used: now,
        });
        drop(entries);
        drop(evicted);
        Ok(client)
    }

    fn expire(&self, now: Instant) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut expired = Vec::new();
        let mut index = 0;
        while index < entries.len() {
            if now.duration_since(entries[index].last_used) >= IDLE_TIMEOUT {
                expired.push(entries.swap_remove(index));
            } else {
                index += 1;
            }
        }
        drop(entries);
        drop(expired);
    }
}

type Registry = Mutex<HashMap<Id, Weak<ClientPool>>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}

fn runtime_pool(runtime: &Handle) -> Option<Arc<ClientPool>> {
    let mut pools = registry().lock().unwrap_or_else(|error| error.into_inner());
    pools.retain(|_, pool| pool.strong_count() != 0);
    if let Some(pool) = pools.get(&runtime.id()).and_then(Weak::upgrade) {
        return Some(pool);
    }
    if pools.len() == MAX_RUNTIMES {
        return None;
    }
    let pool = Arc::new(ClientPool::default());
    pools.insert(runtime.id(), Arc::downgrade(&pool));
    runtime.spawn(expire_idle_clients(runtime.id(), pool.clone()));
    Some(pool)
}

async fn expire_idle_clients(runtime_id: Id, pool: Arc<ClientPool>) {
    loop {
        tokio::time::sleep(SWEEP_INTERVAL).await;
        pool.expire(Instant::now());
        let mut pools = registry().lock().unwrap_or_else(|error| error.into_inner());
        if Arc::strong_count(&pool) == 1
            && pool
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
        {
            pools.remove(&runtime_id);
            return;
        }
    }
}

pub(super) fn shared_client(
    host: &str,
    config: &HttpClientConfig,
    build: impl FnOnce() -> Result<Client>,
) -> Result<Client> {
    if config.max_connections_per_host == 0
        || config.tls.trusted_certificate_path.is_some()
        || std::env::var_os("SSL_CERT_DIR").is_some()
    {
        return build();
    }
    let Ok(runtime) = Handle::try_current() else {
        return build();
    };
    let Some(key) = PoolKey::new(host, config) else {
        return build();
    };
    let Some(pool) = runtime_pool(&runtime) else {
        return build();
    };
    pool.get_or_build(key, build)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn key(host: &str) -> PoolKey {
        PoolKey {
            host: host.to_owned(),
            config: HttpClientConfig::default(),
            proxy_env: std::array::from_fn(|_| None),
            root_file: None,
        }
    }

    #[tokio::test]
    async fn cache_reuses_only_successful_builds() {
        let pool = ClientPool::default();
        let builds = AtomicUsize::new(0);
        for _ in 0..2 {
            assert!(pool
                .get_or_build(key("host"), || Err(
                    Error::invalid_argument().message("bad TLS")
                ))
                .is_err());
            assert!(pool.entries.lock().unwrap().is_empty());
        }
        for _ in 0..3 {
            pool.get_or_build(key("host"), || {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(Client::new())
            })
            .unwrap();
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn config_host_environment_and_roots_partition_pools() {
        let pool = ClientPool::default();
        pool.get_or_build(key("host"), || Ok(Client::new()))
            .unwrap();
        let check = |changed: PoolKey| assert!(pool.get(&changed).is_none());
        check(key("other-host"));
        for index in 0..PROXY_ENV.len() {
            let mut changed = key("host");
            changed.proxy_env[index] = Some("http://other-proxy".into());
            check(changed);
        }
        let mut changed = key("host");
        changed.root_file = Some(("root.pem".into(), [1; 32]));
        check(changed);
        type ConfigChange = Box<dyn Fn(&mut HttpClientConfig)>;
        let variants: Vec<ConfigChange> = vec![
            Box::new(|config| config.connect_timeout += Duration::from_secs(1)),
            Box::new(|config| config.read_timeout += Duration::from_secs(1)),
            Box::new(|config| config.max_connections_per_host += 1),
            Box::new(|config| config.user_agent.push_str(" other")),
            Box::new(|config| config.org_id = Some("other-org".into())),
            Box::new(|config| {
                config
                    .custom_headers
                    .push(("x-test".into(), "value".into()))
            }),
            Box::new(|config| config.sea_response_compression_enabled = true),
            Box::new(|config| config.proxy.url = Some("http://proxy".into())),
            Box::new(|config| config.proxy.username = Some("user".into())),
            Box::new(|config| config.proxy.password = Some("secret".into())),
            Box::new(|config| config.proxy.bypass_hosts = Some("other-host".into())),
            Box::new(|config| config.tls.enabled = Some(false)),
            Box::new(|config| config.tls.allow_self_signed = Some(true)),
            Box::new(|config| config.tls.allow_hostname_mismatch = Some(true)),
            Box::new(|config| config.tls.trusted_certificate_path = Some("root.pem".into())),
            Box::new(|config| config.tls.trusted_certificate_pem = Some(vec![1])),
            Box::new(|config| config.tls.client_cert_pem = Some(vec![2])),
            Box::new(|config| config.tls.client_key_pem = Some(vec![3])),
        ];
        for modify in variants {
            let mut changed = key("host");
            modify(&mut changed.config);
            check(changed);
        }
        assert!(pool.get(&key("host")).is_some());
    }

    #[tokio::test]
    async fn cache_is_bounded_and_evicts_least_recently_used() {
        let pool = ClientPool::default();
        for index in 0..MAX_CLIENTS {
            pool.get_or_build(key(&index.to_string()), || Ok(Client::new()))
                .unwrap();
        }
        assert!(pool.get(&key("0")).is_some());
        pool.get_or_build(key("new-host"), || Ok(Client::new()))
            .unwrap();
        assert_eq!(pool.entries.lock().unwrap().len(), MAX_CLIENTS);
        assert!(pool.get(&key("0")).is_some());
        assert!(pool.get(&key("1")).is_none());
        assert!(pool.get(&key("new-host")).is_some());
    }

    #[tokio::test]
    async fn idle_timeout_is_monotonic_and_access_refreshes_it() {
        let pool = ClientPool::default();
        pool.get_or_build(key("host"), || Ok(Client::new()))
            .unwrap();
        pool.entries.lock().unwrap()[0].last_used = Instant::now() - IDLE_TIMEOUT;
        assert!(pool.get(&key("host")).is_none());
        pool.get_or_build(key("host"), || Ok(Client::new()))
            .unwrap();
        pool.expire(Instant::now());
        assert_eq!(pool.entries.lock().unwrap().len(), 1);
        let before_access = pool.entries.lock().unwrap()[0].last_used;
        assert!(pool.get(&key("host")).is_some());
        assert!(pool.entries.lock().unwrap()[0].last_used >= before_access);
        pool.expire(Instant::now() + IDLE_TIMEOUT);
        assert!(pool.entries.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_build_never_holds_the_cache_lock() {
        let pool = ClientPool::default();
        pool.get_or_build(key("host"), || {
            assert!(pool.entries.try_lock().is_ok());
            Ok(Client::new())
        })
        .unwrap();
    }

    #[test]
    fn concurrent_builds_publish_one_client_and_do_not_block_each_other() {
        let pool = Arc::new(ClientPool::default());
        let rendezvous = Arc::new((Mutex::new(0), std::sync::Condvar::new()));
        let joins: Vec<_> = (0..8)
            .map(|_| {
                let pool = pool.clone();
                let rendezvous = rendezvous.clone();
                std::thread::spawn(move || {
                    pool.get_or_build(key("host"), || {
                        let client = Client::new();
                        let (entered, ready) = &*rendezvous;
                        let mut entered = entered.lock().unwrap();
                        *entered += 1;
                        ready.notify_all();
                        let (entered, _) = ready
                            .wait_timeout_while(entered, Duration::from_secs(5), |count| *count < 8)
                            .unwrap();
                        assert_eq!(*entered, 8, "another builder was blocked by the cache lock");
                        Ok(client)
                    })
                    .unwrap();
                })
            })
            .collect();
        for join in joins {
            join.join().unwrap();
        }
        assert_eq!(pool.entries.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_drops_idle_clients_and_exits_without_new_opens() {
        let pool = Arc::new(ClientPool::default());
        pool.get_or_build(key("host"), || Ok(Client::new()))
            .unwrap();
        let weak = Arc::downgrade(&pool);
        let cleanup = tokio::spawn(expire_idle_clients(Handle::current().id(), pool));
        tokio::task::yield_now().await;
        tokio::time::advance(IDLE_TIMEOUT + SWEEP_INTERVAL).await;
        cleanup.await.unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn cancelling_cleanup_drops_the_cache() {
        let pool = Arc::new(ClientPool::default());
        pool.get_or_build(key("host"), || Ok(Client::new()))
            .unwrap();
        let weak = Arc::downgrade(&pool);
        let cleanup = tokio::spawn(expire_idle_clients(Handle::current().id(), pool));
        cleanup.abort();
        assert!(cleanup.await.unwrap_err().is_cancelled());
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_retains_recent_clients() {
        let pool = Arc::new(ClientPool::default());
        pool.get_or_build(key("host"), || Ok(Client::new()))
            .unwrap();
        let weak = Arc::downgrade(&pool);
        let cleanup = tokio::spawn(expire_idle_clients(Handle::current().id(), pool));
        tokio::task::yield_now().await;
        tokio::time::advance(SWEEP_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(!cleanup.is_finished());
        assert!(weak.upgrade().is_some());
        tokio::time::advance(IDLE_TIMEOUT).await;
        cleanup.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_does_not_detach_a_cache_held_by_a_builder() {
        let pool = Arc::new(ClientPool::default());
        let cleanup = tokio::spawn(expire_idle_clients(Handle::current().id(), pool.clone()));
        tokio::task::yield_now().await;
        tokio::time::advance(SWEEP_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(!cleanup.is_finished());
        pool.get_or_build(key("host"), || Ok(Client::new()))
            .unwrap();
        drop(pool);
        tokio::time::advance(IDLE_TIMEOUT + SWEEP_INTERVAL).await;
        cleanup.await.unwrap();
    }

    #[test]
    fn dynamic_trust_and_disabled_pooling_bypass_reuse() {
        let marker = "KERNEL_POOL_BYPASS_TEST";
        let Some(mode) = std::env::var_os(marker) else {
            for mode in ["normal", "directory", "unreadable"] {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                child
                    .args([
                        "--exact",
                        "client::pool::tests::dynamic_trust_and_disabled_pooling_bypass_reuse",
                        "--nocapture",
                    ])
                    .env(marker, mode)
                    .env_remove("SSL_CERT_FILE")
                    .env_remove("SSL_CERT_DIR");
                match mode {
                    "directory" => {
                        child.env("SSL_CERT_DIR", "/unreadable/certificate/directory");
                    }
                    "unreadable" => {
                        child.env("SSL_CERT_FILE", "/unreadable/certificate/file");
                    }
                    _ => {}
                }
                assert!(
                    child.status().unwrap().success(),
                    "bypass test mode {mode} failed"
                );
            }
            return;
        };
        let check = |config: HttpClientConfig, expected| {
            let builds = AtomicUsize::new(0);
            for _ in 0..2 {
                shared_client("host", &config, || {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(Client::new())
                })
                .unwrap();
            }
            assert_eq!(builds.load(Ordering::SeqCst), expected);
        };
        check(HttpClientConfig::default(), 2);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            check(
                HttpClientConfig::default(),
                if mode == "normal" { 1 } else { 2 },
            );
            let disabled = HttpClientConfig {
                max_connections_per_host: 0,
                ..HttpClientConfig::default()
            };
            check(disabled, 2);
            let mut dynamic = HttpClientConfig::default();
            dynamic.tls.trusted_certificate_path = Some("root.pem".into());
            check(dynamic, 2);
        });
    }

    #[test]
    fn runtime_registry_is_bounded_and_shutdown_releases_clients() {
        let runtimes: Vec<_> = (0..=MAX_RUNTIMES)
            .map(|_| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            })
            .collect();
        let mut weak_pools = Vec::new();
        for runtime in &runtimes[..MAX_RUNTIMES] {
            let pool = runtime_pool(runtime.handle()).unwrap();
            assert!(Arc::ptr_eq(&pool, &runtime_pool(runtime.handle()).unwrap()));
            pool.get_or_build(key("host"), || Ok(Client::new()))
                .unwrap();
            weak_pools.push(Arc::downgrade(&pool));
        }
        assert!(runtime_pool(runtimes[MAX_RUNTIMES].handle()).is_none());
        drop(runtimes);
        assert!(weak_pools.iter().all(|pool| pool.upgrade().is_none()));
        let replacement = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(runtime_pool(replacement.handle()).is_some());
    }
}
