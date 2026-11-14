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
//! reuses a successful answer for [`TTL`]. A lookup that hasn't succeeded
//! after [`HEDGE`], or whose attempt failed, gets another attempt, up to
//! [`MAX_ATTEMPTS`]. The first successful answer reaches every caller waiting
//! on it, so one dropped query doesn't hold the whole burst for glibc's retry.
//! An error reaches them only when every attempt failed, and is not cached.
//!
//! [`TTL`] is fixed: `getaddrinfo` doesn't report the record's own DNS TTL,
//! so that is ignored. The answer is also not invalidated when connecting to
//! it fails, so after a host moves to new addresses, new connections in this
//! process may try the old ones for up to [`TTL`] (the JVM's default caches
//! the same way).

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
/// How long a lookup goes unanswered before another attempt starts; well
/// under glibc's 5 s retry timeout.
const HEDGE: Duration = Duration::from_secs(1);
/// Attempts per lookup, started [`HEDGE`] apart while none has succeeded
/// (or at once when one fails).
const MAX_ATTEMPTS: usize = 3;
/// An unanswered lookup older than this is replaced. A lookup's own threads
/// always answer well before; this covers an entry inherited across `fork()`,
/// whose threads don't exist in the child.
const STUCK_AFTER: Duration = Duration::from_secs(60);
/// Bound on distinct cached hosts (workspace + CloudFetch storage hosts).
const MAX_HOSTS: usize = 256;

type Lookup = Arc<dyn Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync>;
type Answer = (Result<Arc<[SocketAddr]>, Arc<io::Error>>, Instant);
/// `None` while the lookup thread runs; the thread publishes the answer.
type Entry = watch::Receiver<Option<Answer>>;

pub(crate) struct CachingResolver {
    /// Each host's latest lookup and when it started.
    entries: Mutex<HashMap<String, (Entry, Instant)>>,
    ttl: Duration,
    hedge: Duration,
    lookup: Lookup,
    /// Rotates the first address handed out, as round-robin DNS would.
    next: AtomicUsize,
}

impl CachingResolver {
    /// The resolver every kernel HTTP client in this process uses.
    pub(crate) fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<CachingResolver>> = OnceLock::new();
        SHARED
            .get_or_init(|| Arc::new(Self::new(TTL, HEDGE, Arc::new(system_lookup))))
            .clone()
    }

    fn new(ttl: Duration, hedge: Duration, lookup: Lookup) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
            hedge,
            lookup,
            next: AtomicUsize::new(0),
        }
    }

    /// The lookup to join for `host`: one in flight, a fresh cached answer, or
    /// a new lookup when the last one expired or failed.
    fn entry(&self, host: &str) -> Entry {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let usable = |(entry, started): &(Entry, Instant)| match &*entry.borrow() {
            // An unanswered entry whose thread is gone will never be answered.
            None => entry.has_changed().is_ok() && started.elapsed() < STUCK_AFTER,
            Some((Ok(_), at)) => at.elapsed() < self.ttl,
            Some((Err(_), _)) => false,
        };
        if let Some(current) = entries.get(host) {
            if usable(current) {
                return current.0.clone();
            }
        }
        if entries.len() >= MAX_HOSTS {
            entries.retain(|_, current| usable(current));
            if entries.len() >= MAX_HOSTS {
                entries.clear();
            }
        }
        let entry = self.start_lookup(host);
        entries.insert(host.to_owned(), (entry.clone(), Instant::now()));
        entry
    }

    /// Run the lookup off the caller's runtime on a thread that publishes the
    /// answer itself: waiters from other sessions depend on it, so it must not
    /// be abandoned when the caller that started it is cancelled or its
    /// runtime shuts down. See [`first_answer`] for how attempts are made.
    fn start_lookup(&self, host: &str) -> Entry {
        let (tx, rx) = watch::channel(None);
        let lookup = Arc::clone(&self.lookup);
        let hedge = self.hedge;
        let host = host.to_owned();
        let spawned = std::thread::Builder::new()
            .name("dbx-sql-kernel-dns".to_owned())
            .spawn(move || {
                let result = first_answer(&lookup, &host, hedge);
                tx.send_replace(Some((
                    result.map(Arc::from).map_err(Arc::new),
                    Instant::now(),
                )));
            });
        match spawned {
            Ok(_) => rx,
            Err(e) => {
                let (tx, rx) = watch::channel(None);
                tx.send_replace(Some((Err(Arc::new(e)), Instant::now())));
                rx
            }
        }
    }
}

/// The first successful answer from up to [`MAX_ATTEMPTS`] attempts at
/// `lookup(host)`, each on its own thread. Another attempt starts every
/// `hedge` while none has succeeded, and at once when one fails: a fast
/// failure (`EAI_AGAIN` from an overloaded stub) must not fail every waiter
/// while another attempt may still succeed. The last error is returned only
/// once every attempt has failed.
fn first_answer(lookup: &Lookup, host: &str, hedge: Duration) -> io::Result<Vec<SocketAddr>> {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let (mut attempts, mut running) = (0, 0);
    let mut last_error = None;
    // Each pass starts an attempt while under the cap: the first, then one
    // per `hedge` without a success, or one right after a failure.
    loop {
        if attempts < MAX_ATTEMPTS {
            attempts += 1;
            let (sender, lookup, host) = (done_tx.clone(), Arc::clone(lookup), host.to_owned());
            let spawned = std::thread::Builder::new()
                .name("dbx-sql-kernel-dns".to_owned())
                .spawn(move || {
                    // Always report back, so the attempt is never waited on forever.
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| lookup(&host)))
                            .unwrap_or_else(|_| Err(io::Error::other("DNS lookup panicked")));
                    let _ = sender.send(result);
                });
            match spawned {
                Ok(_) => running += 1,
                Err(e) => last_error = Some(e),
            }
        }
        if running == 0 {
            if attempts < MAX_ATTEMPTS {
                continue;
            }
            return Err(last_error.unwrap_or_else(|| io::Error::other("DNS lookup failed")));
        }
        match done_rx.recv_timeout(hedge) {
            Ok(Ok(addrs)) if !addrs.is_empty() => return Ok(addrs),
            Ok(result) => {
                running -= 1;
                last_error = Some(result.err().unwrap_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "DNS lookup returned no addresses")
                }));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            // The coordinator holds a sender, so the channel can't close.
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => unreachable!(),
        }
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
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
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
        let r = Arc::new(CachingResolver::new(
            Duration::from_millis(20),
            HEDGE,
            lookup,
        ));
        resolve(&r, "example.com").await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        resolve(&r, "example.com").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failures_reach_current_waiters_but_are_not_cached() {
        let (lookup, calls) = counting(Duration::from_millis(50), true);
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let r = Arc::clone(&r);
                tokio::spawn(async move { resolve(&r, "example.com").await })
            })
            .collect();
        for t in tasks {
            assert!(t.await.unwrap().unwrap_err().contains("boom"));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            MAX_ATTEMPTS,
            "every attempt failed"
        );
        assert!(resolve(&r, "example.com").await.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2 * MAX_ATTEMPTS,
            "a failure is retried by the next caller"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fast_failure_does_not_beat_an_attempt_that_succeeds() {
        // The first attempt fails at once (EAI_AGAIN from an overloaded
        // stub); the next one answers, and that is what every waiter gets.
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let lookup: Lookup = Arc::new(move |_| {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::Error::other("Temporary failure in name resolution"))
            } else {
                Ok(vec!["10.0.0.1:0".parse().unwrap()])
            }
        });
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
        let tasks: Vec<_> = (0..8)
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
        // A slow scheduler may let another hedge start before the answer arrives.
        assert!((2..=MAX_ATTEMPTS).contains(&calls.load(Ordering::SeqCst)));
    }

    #[tokio::test]
    async fn an_unanswered_entry_past_its_deadline_is_replaced() {
        // After fork() the child keeps an in-flight entry whose lookup
        // threads (and their senders' owners) don't exist there.
        let (lookup, calls) = counting(Duration::ZERO, false);
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
        let (orphan_tx, orphan_rx) = watch::channel(None);
        let long_ago = Instant::now()
            .checked_sub(STUCK_AFTER + Duration::from_secs(1))
            .expect("monotonic clock far enough from its origin");
        r.entries
            .lock()
            .unwrap()
            .insert("example.com".to_owned(), (orphan_rx, long_ago));
        let addrs = tokio::time::timeout(Duration::from_secs(10), resolve(&r, "example.com"))
            .await
            .expect("waited on the orphaned entry")
            .unwrap();
        assert_eq!(addrs, vec!["10.0.0.1:0".parse().unwrap()]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(orphan_tx);
    }

    #[tokio::test]
    async fn empty_answers_are_errors() {
        let lookup: Lookup = Arc::new(|_| Ok(Vec::new()));
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
        assert!(resolve(&r, "example.com").await.is_err());
    }

    #[test]
    fn lookup_survives_the_runtime_that_started_it() {
        let (lookup, calls) = counting(Duration::from_millis(100), false);
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
        let first = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let r1 = Arc::clone(&r);
        first.spawn(async move { resolve(&r1, "example.com").await });
        // Shut down only once that task's lookup is running.
        let deadline = Instant::now() + Duration::from_secs(10);
        while calls.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "the lookup never started");
            std::thread::sleep(Duration::from_millis(1));
        }
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
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
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
        let r = Arc::new(CachingResolver::new(TTL, HEDGE, lookup));
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

    type Release = Arc<(Mutex<bool>, std::sync::Condvar)>;

    /// A lookup whose first `stalled` attempts block until released, like a
    /// dropped query waiting for glibc's retry; later attempts answer at once.
    fn stalling(stalled: usize) -> (Lookup, Arc<AtomicUsize>, Release) {
        let calls = Arc::new(AtomicUsize::new(0));
        let release: Release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (c, rel) = (Arc::clone(&calls), Arc::clone(&release));
        let lookup: Lookup = Arc::new(move |_| {
            if c.fetch_add(1, Ordering::SeqCst) < stalled {
                let (released, wake) = &*rel;
                let _released = wake
                    .wait_while(released.lock().unwrap(), |done| !*done)
                    .unwrap();
            }
            Ok(vec!["10.0.0.1:0".parse().unwrap()])
        });
        (lookup, calls, release)
    }

    fn release(stall: &Release) {
        *stall.0.lock().unwrap() = true;
        stall.1.notify_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callers_waiting_on_a_stalled_lookup_get_the_hedged_answer() {
        // The first query never answers on its own; everyone already waiting
        // on it is answered by the second attempt.
        let (lookup, calls, stall) = stalling(1);
        let r = Arc::new(CachingResolver::new(TTL, Duration::from_millis(50), lookup));
        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let r = Arc::clone(&r);
                tokio::spawn(async move { resolve(&r, "example.com").await })
            })
            .collect();
        for waiter in waiters {
            let addrs = tokio::time::timeout(Duration::from_secs(10), waiter)
                .await
                .expect("held by the stalled query")
                .unwrap()
                .unwrap();
            assert_eq!(addrs, vec!["10.0.0.1:0".parse().unwrap()]);
        }
        // A slow scheduler may let another hedge start before the answer arrives.
        assert!((2..=MAX_ATTEMPTS).contains(&calls.load(Ordering::SeqCst)));
        release(&stall);
    }

    #[tokio::test]
    async fn a_lookup_makes_at_most_max_attempts() {
        let (lookup, calls, stall) = stalling(usize::MAX);
        let r = Arc::new(CachingResolver::new(TTL, Duration::from_millis(10), lookup));
        let r1 = Arc::clone(&r);
        let pending = tokio::spawn(async move { resolve(&r1, "example.com").await });
        let deadline = Instant::now() + Duration::from_secs(10);
        while calls.load(Ordering::SeqCst) < MAX_ATTEMPTS {
            assert!(Instant::now() < deadline, "attempts didn't start");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Ten more hedge intervals pass without another attempt.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS);
        release(&stall);
        pending.await.unwrap().unwrap();
    }
}
