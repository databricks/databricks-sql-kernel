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

//! CRL revocation checking for the `tls-rustls` backend.
//!
//! The server verifier builds the chain with webpki first, so a bad chain never
//! reaches the network. It then fetches the CRLs named by the distribution points
//! of each non-self-issued certificate and hands the current ones to webpki, which
//! checks their signatures and the revocation status. The DER reader here only
//! selects CRLs and reads issuer names and `nextUpdate`. Authenticity is webpki's.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use der::asn1::{AnyRef, GeneralizedTime, UtcTime};
use der::{Decode, Reader, SliceReader, Tagged};
use reqwest::StatusCode;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{
    pem::PemObject, CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName,
    UnixTime,
};
use rustls::{
    CertRevocationListError, CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore,
    SignatureScheme,
};

use super::http::{CrlFetchLimits, ProxyConfig, TlsConfig};
use crate::kernel_error::{Error, Result};
use crate::klog::debug;
use crate::util::runtime::{ambient_rust_min_stack, runtime_thread_stack_size};

const TAG_INTEGER: u8 = 0x02;
const TAG_UTC_TIME: u8 = 0x17;
const TAG_GENERALIZED_TIME: u8 = 0x18;
const TAG_SEQUENCE: u8 = 0x30;
/// Context [0]: explicit version, `distributionPoint`, `fullName`.
const TAG_CONTEXT_0: u8 = 0xA0;
/// Context [2]: `cRLIssuer` (an indirect CRL, which is not supported).
const TAG_CONTEXT_2: u8 = 0xA2;
/// Context [3]: certificate extensions.
const TAG_CONTEXT_3: u8 = 0xA3;
/// Context [6] primitive: a `uniformResourceIdentifier`.
const TAG_URI: u8 = 0x86;
/// OID 2.5.29.31, the CRL distribution points extension (content octets).
const OID_CRL_DISTRIBUTION_POINTS: &[u8] = &[0x55, 0x1D, 0x1F];
/// How long a failed fetch is remembered, so an unreachable endpoint is not waited on again
/// by every handshake.
const FAILED_FETCH_TTL: Duration = Duration::from_secs(60);
/// Caps on the cached CRLs, by count and by total bytes.
const MAX_CACHED_CRLS: usize = 64;
const MAX_CACHED_BYTES: usize = 16 * 1024 * 1024;

/// A verification failure the verifier raises itself. It carries a message for the caller
/// and is classified as a certificate-verification failure.
#[derive(Debug)]
struct VerifierError(String);

impl std::fmt::Display for VerifierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for VerifierError {}

fn verifier_error(message: String) -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::Other(rustls::OtherError(Arc::new(
        VerifierError(message),
    ))))
}

// Tests set a per-thread fetch bound here. The client is built on the calling thread, so
// the bound in effect at construction is the one the fetcher keeps.
#[cfg(test)]
thread_local! {
    pub(crate) static TEST_CRL_LIMITS: std::cell::Cell<Option<CrlFetchLimits>> =
        const { std::cell::Cell::new(None) };
}

// Tests allow CRL hosts on loopback, the address of their CRL server. Production never does.
#[cfg(test)]
thread_local! {
    pub(crate) static TEST_ALLOW_PRIVATE_CRL: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Whether CRL hosts in restricted address ranges are allowed for clients built here.
fn crl_private_allowed() -> bool {
    #[cfg(test)]
    {
        TEST_ALLOW_PRIVATE_CRL.with(std::cell::Cell::get)
    }
    #[cfg(not(test))]
    {
        false
    }
}

/// The CRL fetch bounds: production defaults, or a test override on this thread.
fn crl_fetch_limits() -> CrlFetchLimits {
    #[cfg(test)]
    if let Some(limits) = TEST_CRL_LIMITS.with(std::cell::Cell::get) {
        return limits;
    }
    CrlFetchLimits::default()
}

/// The (tag, content) pairs of the consecutive TLVs in `der`.
fn tlvs(der: &[u8]) -> Option<Vec<(u8, &[u8])>> {
    let mut reader = SliceReader::new(der).ok()?;
    let mut out = Vec::new();
    while !reader.is_finished() {
        let any = AnyRef::decode(&mut reader).ok()?;
        out.push((any.tag().into(), any.value()));
    }
    Some(out)
}

/// The children of the single SEQUENCE that `der` consists of.
fn sequence(der: &[u8]) -> Option<Vec<(u8, &[u8])>> {
    match tlvs(der)?.as_slice() {
        [(TAG_SEQUENCE, body)] => tlvs(body),
        _ => None,
    }
}

/// The fields of a certificate that the revocation check needs.
struct Cert<'a> {
    issuer: &'a [u8],
    subject: &'a [u8],
    spki: &'a [u8],
    crl_uris: Vec<String>,
}

fn parse_cert(der: &[u8]) -> Option<Cert<'_>> {
    let top = sequence(der)?;
    let fields = tlvs(top.first()?.1)?;
    // Optional explicit version, then serialNumber, signature, issuer, validity, subject.
    let base = usize::from(fields.first()?.0 == TAG_CONTEXT_0);
    let issuer = fields.get(base + 2)?.1;
    let subject = fields.get(base + 4)?.1;
    let spki = fields.get(base + 5)?.1;
    let mut crl_uris = Vec::new();
    if let Some(&(_, extensions)) = fields.iter().find(|f| f.0 == TAG_CONTEXT_3) {
        for (_, extension) in sequence(extensions)? {
            let parts = tlvs(extension)?;
            if parts.first()?.1 == OID_CRL_DISTRIBUTION_POINTS {
                crl_uris = distribution_points(parts.last()?.1)?;
            }
        }
    }
    Some(Cert {
        issuer,
        subject,
        spki,
        crl_uris,
    })
}

/// Whether `cert` is one of the configured trust anchors. Matching is by subject
/// and SPKI, as webpki matches anchors, so a same-name certificate with a different
/// key is not an anchor.
fn is_trust_anchor(roots: &RootCertStore, cert: &Cert<'_>) -> bool {
    roots.roots.iter().any(|anchor| {
        anchor.subject.as_ref() == cert.subject
            && anchor.subject_public_key_info.as_ref() == cert.spki
    })
}

/// The http(s) URIs of a CRLDistributionPoints value. Indirect points are skipped.
fn distribution_points(value: &[u8]) -> Option<Vec<String>> {
    let mut uris = Vec::new();
    for (_, point) in sequence(value)? {
        let parts = tlvs(point)?;
        if parts.iter().any(|part| part.0 == TAG_CONTEXT_2) {
            continue;
        }
        for (_, name) in parts.iter().filter(|part| part.0 == TAG_CONTEXT_0) {
            general_names(name, &mut uris)?;
        }
    }
    Some(uris)
}

/// Collects http(s) URIs from a DistributionPointName. Nested wrappers are followed.
/// Directory and relative names are ignored.
fn general_names(der: &[u8], out: &mut Vec<String>) -> Option<()> {
    for (tag, value) in tlvs(der)? {
        match tag {
            TAG_URI => {
                if let Ok(uri) = std::str::from_utf8(value) {
                    let lower = uri.to_ascii_lowercase();
                    if lower.starts_with("http://") || lower.starts_with("https://") {
                        out.push(uri.to_owned());
                    }
                }
            }
            TAG_CONTEXT_0 | TAG_SEQUENCE => general_names(value, out)?,
            _ => {}
        }
    }
    Some(())
}

/// The fields of a CRL that the classification needs.
struct Crl<'a> {
    issuer: &'a [u8],
    next_update: Option<u64>,
}

fn parse_crl(der: &[u8]) -> Option<Crl<'_>> {
    let top = sequence(der)?;
    let fields = tlvs(top.first()?.1)?;
    // Optional version, then signature, issuer, thisUpdate, nextUpdate.
    let base = usize::from(fields.first()?.0 == TAG_INTEGER);
    let issuer = fields.get(base + 1)?.1;
    let next_update = match fields.get(base + 3) {
        Some(&(tag, value)) if tag == TAG_UTC_TIME || tag == TAG_GENERALIZED_TIME => {
            Some(to_unix_secs(tag, value)?)
        }
        _ => None,
    };
    Some(Crl {
        issuer,
        next_update,
    })
}

/// Unix seconds of a UTCTime or GeneralizedTime, given its tag and contents.
fn to_unix_secs(tag: u8, value: &[u8]) -> Option<u64> {
    let mut tlv = vec![tag, u8::try_from(value.len()).ok()?];
    tlv.extend_from_slice(value);
    let at = if tag == TAG_UTC_TIME {
        UtcTime::from_der(&tlv).ok()?.to_unix_duration()
    } else {
        GeneralizedTime::from_der(&tlv).ok()?.to_unix_duration()
    };
    Some(at.as_secs())
}

/// The outcome of checking one fetched CRL against the certificate it covers.
enum Verdict {
    Current,
    Undetermined,
    WrongIssuer,
}

fn classify(der: &[u8], cert_issuer: &[u8], now: u64) -> Verdict {
    if webpki::BorrowedCertRevocationList::from_der(der).is_err() {
        return Verdict::Undetermined;
    }
    match parse_crl(der) {
        Some(crl) if crl.issuer != cert_issuer => Verdict::WrongIssuer,
        Some(Crl {
            next_update: Some(next),
            ..
        }) if now <= next => Verdict::Current,
        _ => Verdict::Undetermined,
    }
}

/// A cached CRL and its nextUpdate in Unix seconds.
type CachedCrl = (Arc<Vec<u8>>, u64);

/// Drops expired CRLs, then the ones that expire soonest until the count and byte caps hold.
fn trim_cache(cache: &mut HashMap<String, CachedCrl>, now: u64) {
    cache.retain(|_, (_, next)| now <= *next);
    while cache.len() > MAX_CACHED_CRLS || cached_bytes(cache) > MAX_CACHED_BYTES {
        let Some(soonest) = cache
            .iter()
            .min_by_key(|(_, (_, next))| *next)
            .map(|(url, _)| url.clone())
        else {
            break;
        };
        cache.remove(&soonest);
    }
}

fn cached_bytes(cache: &HashMap<String, CachedCrl>) -> usize {
    cache.values().map(|(der, _)| der.len()).sum()
}

/// Fetches CRLs through one worker. Each CRL is cached until its `nextUpdate`, within the
/// caps, and a failed fetch is remembered for `FAILED_FETCH_TTL`.
#[derive(Debug)]
struct CrlFetcher {
    proxy: ProxyConfig,
    limits: CrlFetchLimits,
    /// The connection's custom CA roots, so an HTTPS CRL endpoint behind that CA verifies.
    custom_roots: Vec<reqwest::Certificate>,
    /// URL to (CRL bytes, nextUpdate in Unix seconds).
    cache: Mutex<HashMap<String, CachedCrl>>,
    /// URL to the time its last fetch failed.
    failed: Mutex<HashMap<String, Instant>>,
    /// Started on the first fetch, then reused for every chain this fetcher verifies.
    worker: Mutex<Option<CrlWorker>>,
    /// Whether CRL hosts in restricted address ranges are allowed. Only tests set it.
    allow_private: bool,
}

impl CrlFetcher {
    /// The CRL at `url`, from cache while current, otherwise fetched within `budget`.
    /// `None` means the fetch failed, no budget was left, or the URL failed recently.
    fn get(&self, url: &str, now: u64, budget: Duration) -> Option<Arc<Vec<u8>>> {
        {
            let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            trim_cache(&mut cache, now);
            if let Some((der, _)) = cache.get(url) {
                return Some(Arc::clone(der));
            }
        }
        if budget.is_zero() || self.failed_recently(url) {
            return None;
        }
        // The lock covers only starting the worker and queuing the request. The wait for the
        // answer happens after it is released, so concurrent verifications do not serialize.
        let answer = {
            let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
            if worker.is_none() {
                match CrlWorker::start(
                    self.proxy.clone(),
                    self.limits,
                    self.custom_roots.clone(),
                    self.allow_private,
                ) {
                    Ok(started) => *worker = Some(started),
                    Err(reason) => {
                        debug!("CRL worker failed to start: {reason}");
                        return None;
                    }
                }
            }
            match worker.as_ref() {
                Some(started) => started.submit(url, budget),
                None => Err("CRL worker is not running".to_owned()),
            }
        };
        let fetched = match answer {
            Ok(received) => in_runtime_blocking(|| received.recv())
                .unwrap_or_else(|_| Err("CRL worker stopped".to_owned())),
            Err(reason) => Err(reason),
        };
        let der = match fetched {
            Ok(der) => Arc::new(der),
            Err(reason) => {
                debug!("CRL fetch failed for {}: {reason}", redacted(url));
                // A fetch given only a sliver of the chain's budget says little about the
                // endpoint, so only a fetch with most of its time is remembered.
                if budget >= self.limits.timeout / 2 {
                    self.failed
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(url.to_owned(), Instant::now());
                }
                return None;
            }
        };
        self.failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(url);
        if let Some(next) = parse_crl(&der)
            .and_then(|crl| crl.next_update)
            .filter(|next| now <= *next)
        {
            let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            cache.insert(url.to_owned(), (Arc::clone(&der), next));
            trim_cache(&mut cache, now);
        }
        Some(der)
    }

    fn failed_recently(&self, url: &str) -> bool {
        let mut failed = self.failed.lock().unwrap_or_else(PoisonError::into_inner);
        failed.retain(|_, at| at.elapsed() < FAILED_FETCH_TTL);
        failed.contains_key(url)
    }

    fn evict(&self, url: &str) {
        self.cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(url);
    }
}

/// A fetch to run: the URL, the time it may take, and where the answer goes.
type CrlRequest = (
    String,
    Duration,
    std::sync::mpsc::Sender<std::result::Result<Vec<u8>, String>>,
);

/// The thread that runs a fetcher's CRL fetches. It has one current-thread runtime and one
/// reqwest client. Each request runs as its own task, so concurrent fetches overlap.
///
/// Dropping the worker closes its request channel. The thread then finishes its in-flight
/// fetches, each bounded by the per-fetch timeout, and the drop waits at most that long.
/// Trade-off: if a fetch outlives the wait, the thread can still run after the drop. A
/// host that unloads this library then could fault that thread. The wait keeps that window
/// to a fetch that is already past its timeout.
struct CrlWorker {
    requests: Option<tokio::sync::mpsc::UnboundedSender<CrlRequest>>,
    /// Disconnects when the thread has finished.
    exited: std::sync::mpsc::Receiver<()>,
    timeout: Duration,
}

impl std::fmt::Debug for CrlWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CrlWorker")
    }
}

impl CrlWorker {
    fn start(
        proxy: ProxyConfig,
        limits: CrlFetchLimits,
        roots: Vec<reqwest::Certificate>,
        allow_private: bool,
    ) -> std::io::Result<Self> {
        let (requests, mut inbox) = tokio::sync::mpsc::unbounded_channel::<CrlRequest>();
        let (exit_signal, exited) = std::sync::mpsc::channel::<()>();
        std::thread::Builder::new()
            .name("databricks-crl".to_owned())
            .stack_size(runtime_thread_stack_size(ambient_rust_min_stack()))
            .spawn(move || {
                // Dropped when the thread ends, which disconnects `exited`.
                let _exit_signal = exit_signal;
                // The caller's runtime is never re-entered, so a current-thread caller
                // cannot deadlock.
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                let client = build_client(&proxy, limits, &roots);
                runtime.block_on(async move {
                    while let Some((url, budget, answer)) = inbox.recv().await {
                        let client = client.clone();
                        tokio::spawn(async move {
                            let result = match client {
                                Ok(client) => match check_host(&url, allow_private).await {
                                    Ok(()) => fetch(&client, &url, limits.max_bytes, budget).await,
                                    Err(reason) => Err(reason),
                                },
                                Err(reason) => Err(reason),
                            };
                            let _ = answer.send(result);
                        });
                    }
                });
            })?;
        Ok(Self {
            requests: Some(requests),
            exited,
            timeout: limits.timeout,
        })
    }

    /// Queues one fetch. This does not wait for it, so the caller can release any lock
    /// before it waits on the returned receiver.
    fn submit(
        &self,
        url: &str,
        budget: Duration,
    ) -> std::result::Result<std::sync::mpsc::Receiver<std::result::Result<Vec<u8>, String>>, String>
    {
        let (answer, received) = std::sync::mpsc::channel();
        let requests = self
            .requests
            .as_ref()
            .ok_or_else(|| "CRL worker is closed".to_owned())?;
        requests
            .send((url.to_owned(), budget, answer))
            .map_err(|_| "CRL worker is not running".to_owned())?;
        Ok(received)
    }
}

impl Drop for CrlWorker {
    fn drop(&mut self) {
        // Closing the channel ends the worker once its in-flight fetches finish. Wait for
        // that, but no longer than the per-fetch timeout.
        self.requests.take();
        let _ = in_runtime_blocking(|| self.exited.recv_timeout(self.timeout));
    }
}

/// Refuses a CRL URL whose host resolves to a loopback, private, link-local, unspecified,
/// broadcast, multicast or shared (CGNAT) address. A certificate chained to a trusted CA
/// otherwise could point the fetch at an internal endpoint. `allow_private` is set only by
/// tests.
async fn check_host(url: &str, allow_private: bool) -> std::result::Result<(), String> {
    if allow_private {
        return Ok(());
    }
    let parsed = reqwest::Url::parse(url).map_err(|e| e.to_string())?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "CRL URL has no host".to_owned())?;
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "CRL URL has no port".to_owned())?;
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    let addresses = tokio::net::lookup_host((bare, port))
        .await
        .map_err(|e| e.to_string())?;
    for address in addresses {
        if restricted_address(address.ip()) {
            return Err(format!(
                "CRL host {host} resolves to a restricted address {}",
                address.ip()
            ));
        }
    }
    Ok(())
}

fn restricted_address(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            let [a, b, _, _] = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_multicast()
                || (a == 100 && (b & 0xC0) == 64)
        }
        std::net::IpAddr::V6(v6) => {
            let first = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (first & 0xfe00) == 0xfc00
                || (first & 0xffc0) == 0xfe80
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|v4| restricted_address(std::net::IpAddr::V4(v4)))
        }
    }
}

/// Runs a blocking wait. On a multi-thread runtime, block_in_place moves the worker's
/// other tasks to another thread first. It panics on current_thread, so the wait runs as
/// it is there.
fn in_runtime_blocking<T>(wait: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(current) if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
    }
}

/// The client for a chain's CRL fetches. It carries no auth or custom headers, goes
/// through the connection's proxy, trusts the connection's custom CA roots, and is bounded
/// by time and redirects.
fn build_client(
    proxy: &ProxyConfig,
    limits: CrlFetchLimits,
    roots: &[reqwest::Certificate],
) -> std::result::Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(limits.timeout)
        .timeout(limits.timeout)
        // Redirects are not followed. A plaintext CRL response could otherwise send the
        // fetch to an internal endpoint. A 3xx is not a 200, so the status is undetermined.
        .redirect(reqwest::redirect::Policy::none());
    for root in roots {
        builder = builder.add_root_certificate(root.clone());
    }
    if let Some(proxy_url) = &proxy.url {
        let mut via = reqwest::Proxy::all(proxy_url).map_err(|e| e.to_string())?;
        if let Some(username) = &proxy.username {
            via = via.basic_auth(username, proxy.password.as_deref().unwrap_or(""));
        }
        if let Some(bypass) = &proxy.bypass_hosts {
            let normalized = bypass
                .split(',')
                .map(str::trim)
                .collect::<Vec<_>>()
                .join(",");
            via = via.no_proxy(reqwest::NoProxy::from_string(&normalized));
        }
        builder = builder.proxy(via);
    }
    builder.build().map_err(|e| e.to_string())
}

/// One CRL download with `client`, bounded by `budget` and by the size limit.
async fn fetch(
    client: &reqwest::Client,
    url: &str,
    max_bytes: usize,
    budget: Duration,
) -> std::result::Result<Vec<u8>, String> {
    tokio::time::timeout(budget, download(client, url, max_bytes))
        .await
        .unwrap_or_else(|_| Err("CRL fetch timed out".to_owned()))
}

async fn download(
    client: &reqwest::Client,
    url: &str,
    max_bytes: usize,
) -> std::result::Result<Vec<u8>, String> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if response.status() != StatusCode::OK {
        return Err(format!("HTTP {}", response.status()));
    }
    if response
        .content_length()
        .is_some_and(|len| len > max_bytes as u64)
    {
        return Err("CRL larger than the size limit".to_owned());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| e.without_url().to_string())?
    {
        if body.len() + chunk.len() > max_bytes {
            return Err("CRL larger than the size limit".to_owned());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The URL without userinfo, query or fragment, for logging.
fn redacted(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => "<unparsable URL>".to_owned(),
    }
}

/// The server verifier: webpki for the chain, signatures and name, with CRLs.
#[derive(Debug)]
struct RevocationVerifier {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
    /// Webpki verifier without CRLs. Supplies the handshake signature checks.
    signatures: Arc<WebPkiServerVerifier>,
    fetcher: CrlFetcher,
    accept_undetermined: bool,
    allow_hostname_mismatch: bool,
}

impl RevocationVerifier {
    /// Forgives a name-only failure under `allow_hostname_mismatch`. webpki checks
    /// the name last, so the chain has already verified when this happens.
    fn tolerate_name(
        &self,
        result: std::result::Result<ServerCertVerified, rustls::Error>,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match result {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) if self.allow_hostname_mismatch => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }
}

impl RevocationVerifier {
    /// The indices of the presented intermediates on a path that webpki verifies. Subsets
    /// are tried smallest first, and the first one that verifies is the path. An intermediate
    /// the server adds that the path does not need is never returned, so its distribution
    /// points are never fetched. More than six presented intermediates is rejected.
    fn path_intermediates(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<Vec<usize>, rustls::Error> {
        const MAX_PRESENTED: usize = 6;
        let n = intermediates.len();
        if n > MAX_PRESENTED {
            return Err(verifier_error(format!(
                "the server presented {n} intermediates; revocation checking supports at most {MAX_PRESENTED}"
            )));
        }
        for size in 0..=n {
            for mask in 0_usize..(1 << n) {
                if mask.count_ones() as usize != size {
                    continue;
                }
                let picked: Vec<usize> = (0..n).filter(|i| mask & (1 << i) != 0).collect();
                let subset: Vec<CertificateDer<'_>> =
                    picked.iter().map(|&i| intermediates[i].clone()).collect();
                if self
                    .tolerate_name(self.signatures.verify_server_cert(
                        end_entity,
                        &subset,
                        server_name,
                        ocsp_response,
                        now,
                    ))
                    .is_ok()
                {
                    return Ok(picked);
                }
            }
        }
        Err(rustls::Error::InvalidCertificate(
            CertificateError::UnknownIssuer,
        ))
    }
}

impl ServerCertVerifier for RevocationVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        self.tolerate_name(self.signatures.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ))?;

        let now_secs = now.as_secs();
        let path =
            self.path_intermediates(end_entity, intermediates, server_name, ocsp_response, now)?;
        // One deadline bounds every fetch for this chain, so the total wait is at
        // most one per-fetch timeout however many distribution points there are.
        let deadline = Instant::now() + self.fetcher.limits.timeout;
        let mut crls = Vec::new();
        let mut used_urls = Vec::new();
        let mut undetermined = false;
        for cert in std::iter::once(end_entity).chain(path.iter().map(|&i| &intermediates[i])) {
            let Some(parsed) = parse_cert(cert.as_ref()) else {
                undetermined = true;
                continue;
            };
            // Trust anchors are not revocation-checked (webpki does not check them either).
            if is_trust_anchor(&self.roots, &parsed) {
                continue;
            }
            let mut covered = false;
            for url in &parsed.crl_uris {
                // One usable CRL covers the certificate, so the other URLs are not fetched.
                if covered {
                    break;
                }
                let budget = deadline.saturating_duration_since(Instant::now());
                let Some(der) = self.fetcher.get(url, now_secs, budget) else {
                    continue;
                };
                match classify(&der, parsed.issuer, now_secs) {
                    Verdict::Current => {
                        crls.push(CertificateRevocationListDer::from(der.to_vec()));
                        used_urls.push(url.clone());
                        covered = true;
                    }
                    Verdict::Undetermined => self.fetcher.evict(url),
                    Verdict::WrongIssuer => {
                        self.fetcher.evict(url);
                        return Err(rustls::Error::InvalidCertRevocationList(
                            CertRevocationListError::IssuerInvalidForCrl,
                        ));
                    }
                }
            }
            undetermined |= !covered;
        }
        if undetermined && !self.accept_undetermined {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::UnknownRevocationStatus,
            ));
        }

        let mut builder = WebPkiServerVerifier::builder_with_provider(
            Arc::clone(&self.roots),
            Arc::clone(&self.provider),
        )
        .with_crls(crls);
        if self.accept_undetermined {
            builder = builder.allow_unknown_revocation_status();
        }
        let verifier = builder
            .build()
            .map_err(|e| verifier_error(format!("revocation verifier setup: {e}")))?;
        // The final verification sees only the checked path. Webpki then cannot build a
        // path through a presented intermediate whose CRLs were not checked.
        let checked: Vec<CertificateDer<'_>> =
            path.iter().map(|&i| intermediates[i].clone()).collect();
        let result = self.tolerate_name(verifier.verify_server_cert(
            end_entity,
            &checked,
            server_name,
            ocsp_response,
            now,
        ));
        // A CRL that webpki rejects must not stay cached.
        if let Err(rustls::Error::InvalidCertRevocationList(_)) = &result {
            for url in &used_urls {
                self.fetcher.evict(url);
            }
        }
        result
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.signatures.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.signatures.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.signatures.supported_verify_schemes()
    }
}

fn tls_error(e: impl std::fmt::Display) -> Error {
    Error::invalid_argument().message(format!("invalid TLS configuration: {e}"))
}

/// The rustls config for a connection with revocation checking. Trust roots are
/// webpki roots, then the OS store, then custom CAs, as reqwest's rustls assembly
/// uses. ALPN is HTTP/1.1, since the kernel enables no HTTP/2.
///
/// The caller has already validated the PEM bytes and the mTLS identity pair, so
/// they are parsed here without re-checking.
pub(crate) fn client_config(
    tls: &TlsConfig,
    proxy: &ProxyConfig,
    accept_undetermined: bool,
) -> Result<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    // Same assembly and failure rule as reqwest's rustls-tls-native-roots path
    // (reqwest-0.12.28 src/async_impl/client.rs:697-732): entries that do not parse
    // are skipped, and the build fails only when none parse.
    let native = rustls_native_certs::load_native_certs();
    let (mut valid, mut invalid) = (0_usize, 0_usize);
    for cert in native.certs {
        match roots.add(cert) {
            Ok(()) => valid += 1,
            Err(_) => invalid += 1,
        }
    }
    if valid == 0 && invalid > 0 {
        let detail = if native.errors.is_empty() {
            "zero valid certificates found in native root store".to_owned()
        } else {
            native
                .errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        };
        return Err(
            Error::invalid_argument().message(format!("Failed to create HTTP client: {detail}"))
        );
    }
    let mut pems = Vec::new();
    if let Some(path) = &tls.trusted_certificate_path {
        pems.push(std::fs::read(path).map_err(tls_error)?);
    }
    if let Some(pem) = &tls.trusted_certificate_pem {
        pems.push(pem.clone());
    }
    // The same custom CAs, as reqwest certificates, for the CRL client.
    let mut custom_roots = Vec::new();
    for pem in pems {
        for cert in CertificateDer::pem_slice_iter(&pem) {
            roots.add(cert.map_err(tls_error)?).map_err(tls_error)?;
        }
        custom_roots.extend(reqwest::Certificate::from_pem_bundle(&pem).map_err(tls_error)?);
    }

    let roots = Arc::new(roots);
    let signatures =
        WebPkiServerVerifier::builder_with_provider(Arc::clone(&roots), Arc::clone(&provider))
            .build()
            .map_err(tls_error)?;
    let verifier = RevocationVerifier {
        roots,
        provider: Arc::clone(&provider),
        signatures,
        fetcher: CrlFetcher {
            proxy: proxy.clone(),
            limits: crl_fetch_limits(),
            custom_roots,
            cache: Mutex::new(HashMap::new()),
            failed: Mutex::new(HashMap::new()),
            worker: Mutex::new(None),
            allow_private: crl_private_allowed(),
        },
        accept_undetermined,
        allow_hostname_mismatch: tls.allow_hostname_mismatch.unwrap_or(false),
    };

    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));
    let mut config = match (&tls.client_cert_pem, &tls.client_key_pem) {
        (Some(cert), Some(key)) => {
            let chain = CertificateDer::pem_slice_iter(cert)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(tls_error)?;
            let key = PrivateKeyDer::from_pem_slice(key).map_err(tls_error)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(tls_error)?
        }
        _ => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    // Resumed sessions skip `verify_server_cert`, so a certificate revoked after the first
    // handshake would stay accepted. Every handshake therefore runs the check.
    config.resumption = rustls::client::Resumption::disabled();
    Ok(config)
}

#[cfg(test)]
#[path = "tls_revocation_tests.rs"]
mod tests;
