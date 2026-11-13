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

//! Process-wide DNS cache shared by every kernel HTTP client.
//!
//! Each `Session` owns its own `reqwest::Client`, and reqwest's default
//! resolver runs one uncached `getaddrinfo` per new connection. When hundreds
//! of sessions open at once, the burst of identical lookups overflows local
//! stub resolvers (systemd-resolved drops the excess and glibc retries after
//! its 5 s timeout), so a share of the opens stall for 5 s or more. This
//! resolver runs one lookup per host at a time for the whole process and
//! reuses a successful answer for [`TTL`]. Failures are shared with the
//! callers already waiting on that lookup but are not cached.

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::sync::OnceCell;

/// How long a successful answer is reused. Matches the JVM's default
/// positive-lookup TTL, so the JDBC driver's behaviour is the reference.
const TTL: Duration = Duration::from_secs(30);
/// Bound on distinct cached hosts (workspace + CloudFetch storage hosts).
const MAX_HOSTS: usize = 256;

type Lookup = Arc<dyn Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync>;
type Answer = (Result<Arc<[SocketAddr]>, Arc<io::Error>>, Instant);

pub(crate) struct CachingResolver {
    entries: Mutex<HashMap<String, Arc<OnceCell<Answer>>>>,
    ttl: Duration,
    lookup: Lookup,
}

impl CachingResolver {
    /// The resolver every kernel HTTP client in this process uses.
    pub(crate) fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<CachingResolver>> = OnceLock::new();
        SHARED
            .get_or_init(|| Arc::new(Self::new(TTL, Arc::new(system_lookup))))
            .clone()
    }

    fn new(ttl: Duration, lookup: Lookup) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
            lookup,
        }
    }

    /// The lookup to join for `host`: the one in flight, a fresh cached
    /// answer, or a new lookup when the last one expired or failed.
    fn entry(&self, host: &str) -> Arc<OnceCell<Answer>> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let usable = |cell: &OnceCell<Answer>| match cell.get() {
            None => true,
            Some((Ok(_), at)) => at.elapsed() < self.ttl,
            Some((Err(_), _)) => false,
        };
        if let Some(cell) = entries.get(host) {
            if usable(cell) {
                return Arc::clone(cell);
            }
        }
        if entries.len() >= MAX_HOSTS {
            entries.retain(|_, cell| usable(cell));
            if entries.len() >= MAX_HOSTS {
                entries.clear();
            }
        }
        let cell = Arc::new(OnceCell::new());
        entries.insert(host.to_owned(), Arc::clone(&cell));
        cell
    }
}

impl Resolve for CachingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        let cell = self.entry(&host);
        let lookup = Arc::clone(&self.lookup);
        Box::pin(async move {
            let (answer, _) = cell.get_or_init(|| run_lookup(lookup, host)).await;
            match answer {
                Ok(addrs) => Ok(Box::new(addrs.to_vec().into_iter()) as Addrs),
                Err(e) => Err(io::Error::new(e.kind(), e.to_string()).into()),
            }
        })
    }
}

/// Run the blocking lookup on its own thread, not the caller's blocking
/// pool: other sessions wait on this answer, so it must not be cancelled
/// when the session that started it shuts its runtime down.
async fn run_lookup(lookup: Lookup, host: String) -> Answer {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let spawned = std::thread::Builder::new()
        .name("dbx-sql-kernel-dns".to_owned())
        .spawn(move || {
            let _ = tx.send(lookup(&host));
        });
    let result = match spawned {
        Err(e) => Err(e),
        Ok(_) => match rx.await {
            Ok(Ok(addrs)) if addrs.is_empty() => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "DNS lookup returned no addresses",
            )),
            Ok(result) => result,
            Err(_) => Err(io::Error::other("DNS lookup thread exited")),
        },
    };
    (result.map(Arc::from).map_err(Arc::new), Instant::now())
}

fn system_lookup(host: &str) -> io::Result<Vec<SocketAddr>> {
    Ok((host, 0).to_socket_addrs()?.collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn counting(delay: Duration, fail: bool) -> (Lookup, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let lookup: Lookup = Arc::new(move |_host| {
            c.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(delay);
            if fail {
                Err(io::Error::other("boom"))
            } else {
                Ok(vec!["10.0.0.1:0".parse().unwrap()])
            }
        });
        (lookup, calls)
    }

    async fn resolve(r: &Arc<CachingResolver>, host: &str) -> Result<Vec<SocketAddr>, String> {
        r.resolve(Name::from_str(host).unwrap())
            .await
            .map(|a| a.collect())
            .map_err(|e| e.to_string())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_lookups_for_one_host_share_one_query() {
        let (lookup, calls) = counting(Duration::from_millis(50), false);
        let r = Arc::new(CachingResolver::new(TTL, lookup));
        let tasks: Vec<_> = (0..64)
            .map(|_| {
                let r = Arc::clone(&r);
                tokio::spawn(async move { resolve(&r, "example.com").await })
            })
            .collect();
        for t in tasks {
            assert_eq!(
                t.await.unwrap().unwrap(),
                vec!["10.0.0.1:0".parse().unwrap()]
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        resolve(&r, "example.com").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1, "fresh answer is reused");
        resolve(&r, "other.example.com").await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "hosts are cached separately"
        );
    }

    #[tokio::test]
    async fn expired_answers_are_looked_up_again() {
        let (lookup, calls) = counting(Duration::ZERO, false);
        let r = Arc::new(CachingResolver::new(Duration::from_millis(20), lookup));
        resolve(&r, "example.com").await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        resolve(&r, "example.com").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failures_reach_current_waiters_but_are_not_cached() {
        let (lookup, calls) = counting(Duration::from_millis(50), true);
        let r = Arc::new(CachingResolver::new(TTL, lookup));
        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let r = Arc::clone(&r);
                tokio::spawn(async move { resolve(&r, "example.com").await })
            })
            .collect();
        for t in tasks {
            assert!(t.await.unwrap().unwrap_err().contains("boom"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(resolve(&r, "example.com").await.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a failure is retried by the next caller"
        );
    }

    #[tokio::test]
    async fn empty_answers_are_errors() {
        let lookup: Lookup = Arc::new(|_| Ok(Vec::new()));
        let r = Arc::new(CachingResolver::new(TTL, lookup));
        assert!(resolve(&r, "example.com").await.is_err());
    }

    #[test]
    fn lookup_survives_the_runtime_that_started_it() {
        let (lookup, calls) = counting(Duration::from_millis(100), false);
        let r = Arc::new(CachingResolver::new(TTL, lookup));
        let first = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let r1 = Arc::clone(&r);
        first.spawn(async move { resolve(&r1, "example.com").await });
        std::thread::sleep(Duration::from_millis(20));
        first.shutdown_background();

        let second = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(second.block_on(resolve(&r, "example.com")).is_ok());
        assert!(calls.load(Ordering::SeqCst) <= 2);
    }
}
