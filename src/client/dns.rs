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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::sync::watch;

/// How long a successful answer is reused. Matches the JVM's default
/// positive-lookup TTL, so the JDBC driver's behaviour is the reference.
const TTL: Duration = Duration::from_secs(30);
/// Bound on distinct cached hosts (workspace + CloudFetch storage hosts).
const MAX_HOSTS: usize = 256;

type Lookup = Arc<dyn Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync>;
type Answer = (Result<Arc<[SocketAddr]>, Arc<io::Error>>, Instant);
/// `None` while the lookup thread runs; the thread publishes the answer.
type Entry = watch::Receiver<Option<Answer>>;

pub(crate) struct CachingResolver {
    entries: Mutex<HashMap<String, Entry>>,
    ttl: Duration,
    lookup: Lookup,
    /// Rotates the first address handed out, as round-robin DNS would.
    next: AtomicUsize,
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
            next: AtomicUsize::new(0),
        }
    }

    /// The lookup to join for `host`: the one in flight, a fresh cached
    /// answer, or a new lookup when the last one expired or failed.
    fn entry(&self, host: &str) -> Entry {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let usable = |entry: &Entry| {
            let in_flight = match &*entry.borrow() {
                None => true,
                Some((Ok(_), at)) => return at.elapsed() < self.ttl,
                Some((Err(_), _)) => return false,
            };
            // An unanswered entry whose thread is gone will never be answered.
            in_flight && entry.has_changed().is_ok()
        };
        if let Some(entry) = entries.get(host) {
            if usable(entry) {
                return entry.clone();
            }
        }
        if entries.len() >= MAX_HOSTS {
            entries.retain(|_, entry| usable(entry));
            if entries.len() >= MAX_HOSTS {
                entries.clear();
            }
        }
        let entry = self.start_lookup(host);
        entries.insert(host.to_owned(), entry.clone());
        entry
    }

    /// Run the blocking lookup on its own thread, which publishes the answer
    /// itself: waiters from other sessions depend on it, so it must not be
    /// abandoned when the caller that started it is cancelled or its runtime
    /// shuts down.
    fn start_lookup(&self, host: &str) -> Entry {
        let (tx, rx) = watch::channel(None);
        let tx = Arc::new(tx);
        let thread_tx = Arc::clone(&tx);
        let lookup = Arc::clone(&self.lookup);
        let host = host.to_owned();
        let spawned = std::thread::Builder::new()
            .name("dbx-sql-kernel-dns".to_owned())
            .spawn(move || {
                let result = lookup(&host).and_then(|addrs| {
                    if addrs.is_empty() {
                        Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "DNS lookup returned no addresses",
                        ))
                    } else {
                        Ok(addrs)
                    }
                });
                thread_tx.send_replace(Some((
                    result.map(Arc::from).map_err(Arc::new),
                    Instant::now(),
                )));
            });
        if let Err(e) = spawned {
            tx.send_replace(Some((Err(Arc::new(e)), Instant::now())));
        }
        rx
    }
}

impl Resolve for CachingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let mut entry = self.entry(name.as_str());
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            let result = match entry.wait_for(Option::is_some).await {
                Ok(answer) => answer.as_ref().map(|(result, _)| result.clone()),
                Err(_) => None,
            };
            match result {
                Some(Ok(addrs)) => Ok(Box::new(rotated(addrs, start)) as Addrs),
                Some(Err(e)) => Err(io::Error::new(e.kind(), e.to_string()).into()),
                None => Err(io::Error::other("DNS lookup thread exited").into()),
            }
        })
    }
}

/// `addrs` starting at a different address per call. Only the leading run
/// of one address family rotates, so the family the connector tries first
/// (happy eyeballs) stays the resolver's choice.
fn rotated(addrs: Arc<[SocketAddr]>, start: usize) -> impl Iterator<Item = SocketAddr> + Send {
    let lead = addrs
        .iter()
        .take_while(|a| a.is_ipv4() == addrs[0].is_ipv4())
        .count();
    let offset = start % lead.max(1);
    (0..addrs.len()).map(move |i| {
        if i < lead {
            addrs[(i + offset) % lead]
        } else {
            addrs[i]
        }
    })
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
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn lookup_survives_a_cancelled_caller() {
        let (lookup, calls) = counting(Duration::from_millis(100), false);
        let r = Arc::new(CachingResolver::new(TTL, lookup));
        let timed_out =
            tokio::time::timeout(Duration::from_millis(10), resolve(&r, "example.com")).await;
        assert!(timed_out.is_err(), "first caller is cancelled mid-lookup");
        resolve(&r, "example.com").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn each_call_starts_at_the_next_address_of_the_leading_family() {
        let lookup: Lookup = Arc::new(|_| {
            Ok(["10.0.0.1:0", "10.0.0.2:0", "10.0.0.3:0", "[::1]:0"]
                .iter()
                .map(|a| a.parse().unwrap())
                .collect())
        });
        let r = Arc::new(CachingResolver::new(TTL, lookup));
        let mut firsts = std::collections::HashSet::new();
        for _ in 0..3 {
            let addrs = resolve(&r, "example.com").await.unwrap();
            assert_eq!(addrs.len(), 4);
            assert!(addrs[..3].iter().all(SocketAddr::is_ipv4));
            assert_eq!(addrs[3], "[::1]:0".parse().unwrap());
            firsts.insert(addrs[0]);
        }
        assert_eq!(firsts.len(), 3);
    }
}
