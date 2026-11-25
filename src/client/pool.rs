use std::collections::HashMap;
use std::ffi::OsString;
use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::runtime::{Handle, Id};
use tokio::time::Instant;

use super::http::{HttpClientConfig, ProxyConfig, TlsConfig};
use crate::Result;

const MAX_RUNTIMES: usize = 16;
const MAX_CLIENTS: usize = 16;
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_AGE: Duration = Duration::from_secs(300);
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
struct TransportKey {
    connect_timeout: Duration,
    read_timeout: Duration,
    max_connections_per_host: usize,
    user_agent: String,
    tls: TlsConfig,
    proxy: ProxyConfig,
}

impl From<&HttpClientConfig> for TransportKey {
    fn from(config: &HttpClientConfig) -> Self {
        // New settings must be classified as transport or per-request state.
        let HttpClientConfig {
            connect_timeout,
            read_timeout,
            max_connections_per_host,
            user_agent,
            tls,
            proxy,
            org_id: _,
            custom_headers: _,
            sea_response_compression_enabled: _,
        } = config;
        Self {
            connect_timeout: *connect_timeout,
            read_timeout: *read_timeout,
            max_connections_per_host: *max_connections_per_host,
            user_agent: user_agent.clone(),
            tls: tls.clone(),
            proxy: proxy.clone(),
        }
    }
}

#[derive(PartialEq, Eq)]
struct PoolKey {
    host: String,
    config: TransportKey,
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
            config: config.into(),
            proxy_env: PROXY_ENV.map(std::env::var_os),
            root_file,
        })
    }
}

struct Entry {
    key: PoolKey,
    client: Client,
    created_at: Instant,
    idle_since: Instant,
}

impl Entry {
    fn reusable(&self, now: Instant) -> bool {
        now.duration_since(self.idle_since) < IDLE_TIMEOUT
            && now.duration_since(self.created_at) < MAX_AGE
    }
}

struct Lease {
    pool: Arc<ClientPool>,
    key: PoolKey,
    created_at: Instant,
}

pub(super) struct PooledClient {
    client: Client,
    lease: Option<Lease>,
}

impl From<Client> for PooledClient {
    fn from(client: Client) -> Self {
        Self {
            client,
            lease: None,
        }
    }
}

impl std::fmt::Debug for PooledClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.client, formatter)
    }
}

impl Deref for PooledClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            lease.pool.put(Entry {
                key: lease.key,
                client: self.client.clone(),
                created_at: lease.created_at,
                idle_since: Instant::now(),
            });
        }
    }
}

#[derive(Default)]
struct PoolState {
    entries: Vec<Entry>,
    closed: bool,
}

impl PoolState {
    fn expire(&mut self, now: Instant) -> Vec<Entry> {
        let mut expired = Vec::new();
        let mut index = 0;
        while index < self.entries.len() {
            if !self.entries[index].reusable(now) {
                expired.push(self.entries.swap_remove(index));
            } else {
                index += 1;
            }
        }
        expired
    }
}

#[derive(Default)]
struct ClientPool {
    state: Mutex<PoolState>,
}

impl ClientPool {
    fn take(&self, key: &PoolKey) -> Option<Entry> {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let expired = state.expire(now);
        let entry = state
            .entries
            .iter()
            .position(|entry| entry.key == *key)
            .map(|index| state.entries.swap_remove(index));
        drop(state);
        drop(expired);
        entry
    }

    fn checkout(
        self: &Arc<Self>,
        key: PoolKey,
        build: impl FnOnce() -> Result<Client>,
    ) -> Result<PooledClient> {
        let (client, created_at) = match self.take(&key) {
            Some(entry) => (entry.client, entry.created_at),
            None => (build()?, Instant::now()),
        };
        Ok(PooledClient {
            client,
            lease: Some(Lease {
                pool: self.clone(),
                key,
                created_at,
            }),
        })
    }

    fn put(&self, entry: Entry) {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut expired = state.expire(now);
        if state.closed || !entry.reusable(now) {
            drop(state);
            drop(expired);
            return;
        }
        if state.entries.len() == MAX_CLIENTS {
            let oldest = state
                .entries
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.idle_since)
                .map(|(index, _)| index)
                .unwrap_or(0);
            expired.push(state.entries.swap_remove(oldest));
        }
        state.entries.push(entry);
        drop(state);
        drop(expired);
    }

    fn expire(&self, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let expired = state.expire(now);
        drop(state);
        drop(expired);
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        let entries = std::mem::take(&mut state.entries);
        drop(state);
        drop(entries);
    }
}

type Registry = Mutex<HashMap<Id, Weak<ClientPool>>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}

fn runtime_pool(runtime: &Handle) -> Option<Arc<ClientPool>> {
    let mut pools = registry().lock().unwrap_or_else(|error| error.into_inner());
    pools.retain(|_, pool| {
        pool.upgrade().is_some_and(|pool| {
            !pool
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .closed
        })
    });
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

struct Cleanup {
    pool: Arc<ClientPool>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.pool.close();
    }
}

fn expire_idle_clients(
    runtime_id: Id,
    pool: Arc<ClientPool>,
) -> impl std::future::Future<Output = ()> {
    let cleanup = Cleanup { pool };
    async move {
        loop {
            tokio::time::sleep(SWEEP_INTERVAL).await;
            cleanup.pool.expire(Instant::now());
            let mut pools = registry().lock().unwrap_or_else(|error| error.into_inner());
            if Arc::strong_count(&cleanup.pool) == 1
                && cleanup
                    .pool
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .entries
                    .is_empty()
            {
                pools.remove(&runtime_id);
                return;
            }
        }
    }
}

pub(super) fn session_client(
    host: &str,
    config: &HttpClientConfig,
    build: impl FnOnce() -> Result<Client>,
) -> Result<PooledClient> {
    if config.max_connections_per_host == 0
        || config.tls.trusted_certificate_path.is_some()
        || std::env::var_os("SSL_CERT_DIR").is_some()
    {
        return build().map(PooledClient::from);
    }
    let Ok(runtime) = Handle::try_current() else {
        return build().map(PooledClient::from);
    };
    let Some(key) = PoolKey::new(host, config) else {
        return build().map(PooledClient::from);
    };
    let Some(pool) = runtime_pool(&runtime) else {
        return build().map(PooledClient::from);
    };
    pool.checkout(key, build)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn key(host: &str) -> PoolKey {
        PoolKey {
            host: host.to_owned(),
            config: (&HttpClientConfig::default()).into(),
            proxy_env: std::array::from_fn(|_| None),
            root_file: None,
        }
    }

    async fn finish_cleanup(cleanup: tokio::task::JoinHandle<()>) {
        tokio::time::timeout(SWEEP_INTERVAL * 2, cleanup)
            .await
            .expect("cleanup did not stop")
            .unwrap();
    }

    #[tokio::test]
    async fn cache_reuses_only_successful_builds_after_return() {
        let pool = Arc::new(ClientPool::default());
        let builds = AtomicUsize::new(0);
        for _ in 0..2 {
            assert!(pool
                .checkout(key("host"), || Err(
                    Error::invalid_argument().message("bad TLS")
                ))
                .is_err());
            assert!(pool.state.lock().unwrap().entries.is_empty());
        }
        let first = pool
            .checkout(key("host"), || {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(Client::new())
            })
            .unwrap();
        assert!(pool.state.lock().unwrap().entries.is_empty());
        assert_eq!(format!("{first:?}"), format!("{:?}", first.client));
        let second = pool
            .checkout(key("host"), || {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(Client::new())
            })
            .unwrap();
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        drop(first);
        let third = pool.checkout(key("host"), || panic!("cache miss")).unwrap();
        let fourth = pool
            .checkout(key("host"), || {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(Client::new())
            })
            .unwrap();
        assert_eq!(builds.load(Ordering::SeqCst), 3);
        drop(third);
        for _ in 0..3 {
            drop(
                pool.checkout(key("host"), || panic!("returned client was not reused"))
                    .unwrap(),
            );
        }
        drop(second);
        drop(fourth);
        assert_eq!(pool.state.lock().unwrap().entries.len(), 3);
    }

    #[tokio::test]
    async fn only_transport_host_environment_and_roots_partition_pools() {
        let original = HttpClientConfig::default();
        let pool = Arc::new(ClientPool::default());
        drop(pool.checkout(key("host"), || Ok(Client::new())).unwrap());
        let check = |changed: PoolKey| assert!(pool.take(&changed).is_none());
        for index in 0..PROXY_ENV.len() {
            let mut changed = key("host");
            changed.proxy_env[index] = Some("http://other-proxy".into());
            assert!(changed != key("host"));
            check(changed);
        }
        let mut changed = key("host");
        changed.root_file = Some(("root.pem".into(), [1; 32]));
        assert!(changed != key("host"));
        check(changed);
        assert!(key("other-host") != key("host"));
        check(key("other-host"));
        type ConfigChange = Box<dyn Fn(&mut HttpClientConfig)>;
        let variants: Vec<ConfigChange> = vec![
            Box::new(|config| config.connect_timeout += Duration::from_secs(1)),
            Box::new(|config| config.read_timeout += Duration::from_secs(1)),
            Box::new(|config| config.max_connections_per_host += 1),
            Box::new(|config| config.user_agent.push_str(" other")),
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
            let mut config = original.clone();
            modify(&mut config);
            assert!(TransportKey::from(&config) != TransportKey::from(&original));
            check(PoolKey {
                config: (&config).into(),
                ..key("host")
            });
        }
    }

    #[tokio::test]
    async fn per_request_settings_each_reuse_the_same_transport() {
        let pool = Arc::new(ClientPool::default());
        drop(pool.checkout(key("host"), || Ok(Client::new())).unwrap());
        for variant in 0..3 {
            let mut per_request = HttpClientConfig::default();
            match variant {
                0 => per_request.org_id = Some("other-org".into()),
                1 => per_request
                    .custom_headers
                    .push(("x-test".into(), "value".into())),
                2 => per_request.sea_response_compression_enabled = true,
                _ => unreachable!(),
            }
            drop(
                pool.checkout(
                    PoolKey {
                        config: (&per_request).into(),
                        ..key("host")
                    },
                    || panic!("per-request setting partitioned the transport"),
                )
                .unwrap(),
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cache_is_bounded_and_evicts_least_recently_used() {
        let pool = Arc::new(ClientPool::default());
        for index in 0..MAX_CLIENTS {
            drop(
                pool.checkout(key(&index.to_string()), || Ok(Client::new()))
                    .unwrap(),
            );
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        drop(pool.checkout(key("0"), || panic!("cache miss")).unwrap());
        drop(
            pool.checkout(key("new-host"), || Ok(Client::new()))
                .unwrap(),
        );
        assert_eq!(pool.state.lock().unwrap().entries.len(), MAX_CLIENTS);
        assert!(pool.take(&key("0")).is_some());
        assert!(pool.take(&key("1")).is_none());
        assert!(pool.take(&key("new-host")).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_is_monotonic_and_return_refreshes_it() {
        let pool = Arc::new(ClientPool::default());
        drop(pool.checkout(key("host"), || Ok(Client::new())).unwrap());
        tokio::time::advance(IDLE_TIMEOUT / 2).await;
        drop(
            pool.checkout(key("host"), || panic!("idle entry expired early"))
                .unwrap(),
        );
        tokio::time::advance(IDLE_TIMEOUT / 2).await;
        drop(
            pool.checkout(key("host"), || {
                panic!("return did not refresh idle timeout")
            })
            .unwrap(),
        );
        tokio::time::advance(IDLE_TIMEOUT).await;
        assert!(pool.take(&key("host")).is_none());
        pool.expire(Instant::now());
        assert!(pool.state.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn cache_access_expires_entries_without_polling_the_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        tokio::time::pause();
        let polls = Arc::new(AtomicUsize::new(0));
        for access in 0..3 {
            let pool = Arc::new(ClientPool::default());
            let cleanup = expire_idle_clients(runtime.handle().id(), pool.clone());
            let task_polls = polls.clone();
            runtime.spawn(async move {
                task_polls.fetch_add(1, Ordering::SeqCst);
                cleanup.await;
            });
            let created_at = Instant::now();
            let mut expired_key = key("expired");
            expired_key.config.tls.client_key_pem = Some(b"expired-private-key".to_vec());
            expired_key.config.proxy.password = Some("expired-proxy-password".into());
            pool.put(Entry {
                key: expired_key,
                client: Client::new(),
                created_at,
                idle_since: created_at,
            });
            // Advance the clock without driving Tokio's tasks or IO/timer driver.
            futures::executor::block_on(tokio::time::advance(if access == 2 {
                MAX_AGE
            } else {
                IDLE_TIMEOUT + SWEEP_INTERVAL
            }));
            if access == 0 {
                assert!(pool.take(&key("unrelated")).is_none());
            } else {
                pool.put(Entry {
                    key: key("returned"),
                    client: Client::new(),
                    created_at: if access == 2 {
                        created_at
                    } else {
                        Instant::now()
                    },
                    idle_since: Instant::now(),
                });
            }
            assert_eq!(
                pool.state.lock().unwrap().entries.len(),
                usize::from(access == 1),
                "cache access {access} retained expired configuration"
            );
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn maximum_age_expires_clients_despite_repeated_checkouts() {
        let pool = Arc::new(ClientPool::default());
        let builds = AtomicUsize::new(0);
        for _ in 0..=MAX_AGE.as_secs() / 30 {
            drop(
                pool.checkout(key("host"), || {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(Client::new())
                })
                .unwrap(),
            );
            tokio::time::advance(Duration::from_secs(30)).await;
        }
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        let active = pool.checkout(key("host"), || panic!("cache miss")).unwrap();
        tokio::time::advance(MAX_AGE).await;
        drop(active);
        pool.expire(Instant::now());
        assert!(pool.state.lock().unwrap().entries.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_build_never_holds_the_cache_lock() {
        let pool = Arc::new(ClientPool::default());
        drop(
            pool.checkout(key("host"), || {
                assert!(pool.state.try_lock().is_ok());
                Ok(Client::new())
            })
            .unwrap(),
        );
    }

    #[test]
    fn concurrent_checkouts_build_exclusive_clients_and_bound_returned_clients() {
        let pool = Arc::new(ClientPool::default());
        let rendezvous = Arc::new((Mutex::new(0), std::sync::Condvar::new()));
        let joins: Vec<_> = (0..MAX_CLIENTS + 4)
            .map(|_| {
                let pool = pool.clone();
                let rendezvous = rendezvous.clone();
                std::thread::spawn(move || {
                    pool.checkout(key("host"), || {
                        let client = Client::new();
                        let (entered, ready) = &*rendezvous;
                        let mut entered = entered.lock().unwrap();
                        *entered += 1;
                        ready.notify_all();
                        let (entered, _) = ready
                            .wait_timeout_while(entered, Duration::from_secs(10), |count| {
                                *count < MAX_CLIENTS + 4
                            })
                            .unwrap();
                        assert_eq!(
                            *entered,
                            MAX_CLIENTS + 4,
                            "another builder was blocked by the cache lock"
                        );
                        Ok(client)
                    })
                    .unwrap()
                })
            })
            .collect();
        let clients: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
        assert!(pool.state.lock().unwrap().entries.is_empty());
        drop(clients);
        assert_eq!(pool.state.lock().unwrap().entries.len(), MAX_CLIENTS);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_drops_idle_clients_and_exits_without_new_opens() {
        let pool = Arc::new(ClientPool::default());
        drop(pool.checkout(key("host"), || Ok(Client::new())).unwrap());
        let weak = Arc::downgrade(&pool);
        let cleanup = tokio::spawn(expire_idle_clients(Handle::current().id(), pool));
        tokio::task::yield_now().await;
        tokio::time::advance(IDLE_TIMEOUT + SWEEP_INTERVAL).await;
        finish_cleanup(cleanup).await;
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn cancelling_cleanup_closes_cache_even_with_active_clients() {
        let pool = Arc::new(ClientPool::default());
        let client = pool.checkout(key("host"), || Ok(Client::new())).unwrap();
        let cleanup = tokio::spawn(expire_idle_clients(Handle::current().id(), pool.clone()));
        cleanup.abort();
        assert!(cleanup.await.unwrap_err().is_cancelled());
        assert!(pool.state.lock().unwrap().closed);
        drop(client);
        assert!(pool.state.lock().unwrap().entries.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_retains_recent_and_checked_out_clients() {
        let pool = Arc::new(ClientPool::default());
        let client = pool.checkout(key("host"), || Ok(Client::new())).unwrap();
        let weak = Arc::downgrade(&pool);
        let cleanup = tokio::spawn(expire_idle_clients(Handle::current().id(), pool.clone()));
        drop(pool);
        tokio::task::yield_now().await;
        tokio::time::advance(IDLE_TIMEOUT + SWEEP_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(!cleanup.is_finished());
        assert!(weak.upgrade().is_some());
        drop(client);
        tokio::time::advance(SWEEP_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(!cleanup.is_finished());
        tokio::time::advance(IDLE_TIMEOUT + SWEEP_INTERVAL).await;
        finish_cleanup(cleanup).await;
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn shutdown_before_cleanup_is_polled_prevents_late_returns() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let pool = Arc::new(ClientPool::default());
        let client = pool.checkout(key("host"), || Ok(Client::new())).unwrap();
        runtime.spawn(expire_idle_clients(runtime.handle().id(), pool.clone()));
        drop(runtime);
        assert!(pool.state.lock().unwrap().closed);
        drop(client);
        assert!(pool.state.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn dynamic_trust_and_disabled_pooling_bypass_reuse() {
        let marker = "KERNEL_POOL_BYPASS_TEST";
        let Some(mode) = std::env::var_os(marker) else {
            for mode in ["normal", "directory", "unreadable", "roots"] {
                let roots = tempfile::NamedTempFile::new().unwrap();
                std::fs::write(roots.path(), b"first roots").unwrap();
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
                    "roots" => {
                        child.env("SSL_CERT_FILE", roots.path());
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
        if mode == "roots" {
            let first = PoolKey::new("host", &HttpClientConfig::default()).unwrap();
            std::fs::write(std::env::var_os("SSL_CERT_FILE").unwrap(), b"second roots").unwrap();
            let second = PoolKey::new("host", &HttpClientConfig::default()).unwrap();
            assert!(first != second);
            return;
        }
        let check = |config: HttpClientConfig, expected| {
            let builds = AtomicUsize::new(0);
            for _ in 0..2 {
                drop(
                    session_client("host", &config, || {
                        builds.fetch_add(1, Ordering::SeqCst);
                        Ok(Client::new())
                    })
                    .unwrap(),
                );
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
        let marker = "KERNEL_POOL_REGISTRY_TEST";
        if std::env::var_os(marker).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "client::pool::tests::runtime_registry_is_bounded_and_shutdown_releases_clients", "--nocapture"])
                .env(marker, "1").status().unwrap();
            assert!(
                status.success(),
                "isolated runtime registry assertions failed"
            );
            return;
        }
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
            drop(pool.checkout(key("host"), || Ok(Client::new())).unwrap());
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
