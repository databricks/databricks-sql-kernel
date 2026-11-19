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
const CRL_MAX_REDIRECTS: usize = 3;

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
    self_issued: bool,
    crl_uris: Vec<String>,
}

fn parse_cert(der: &[u8]) -> Option<Cert<'_>> {
    let top = sequence(der)?;
    let fields = tlvs(top.first()?.1)?;
    // Optional explicit version, then serialNumber, signature, issuer, validity, subject.
    let base = usize::from(fields.first()?.0 == TAG_CONTEXT_0);
    let issuer = fields.get(base + 2)?.1;
    let subject = fields.get(base + 4)?.1;
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
        self_issued: issuer == subject,
        crl_uris,
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

/// Fetches CRLs and caches each until its `nextUpdate`.
#[derive(Debug)]
struct CrlFetcher {
    proxy: ProxyConfig,
    limits: CrlFetchLimits,
    /// URL to (CRL bytes, nextUpdate in Unix seconds).
    cache: Mutex<HashMap<String, CachedCrl>>,
}

impl CrlFetcher {
    /// The CRL at `url`, from cache while current, otherwise freshly fetched.
    /// `None` means the fetch failed. Only a CRL with a future `nextUpdate` is cached.
    fn get(&self, url: &str, now: u64) -> Option<Arc<Vec<u8>>> {
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(url)
            .filter(|(_, next)| now <= *next)
            .map(|(der, _)| Arc::clone(der));
        if cached.is_some() {
            return cached;
        }
        self.evict(url);
        let der = Arc::new(fetch_blocking(
            self.proxy.clone(),
            self.limits,
            url.to_owned(),
        )?);
        if let Some(next) = parse_crl(&der)
            .and_then(|crl| crl.next_update)
            .filter(|next| now <= *next)
        {
            self.cache
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(url.to_owned(), (Arc::clone(&der), next));
        }
        Some(der)
    }

    fn evict(&self, url: &str) {
        self.cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(url);
    }
}

/// Runs one fetch on its own thread and runtime. The caller's runtime is never
/// re-entered, so a current-thread caller cannot deadlock.
fn fetch_blocking(proxy: ProxyConfig, limits: CrlFetchLimits, url: String) -> Option<Vec<u8>> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?
            .block_on(fetch(&proxy, &url, limits))
    })
    .join()
    .ok()
    .flatten()
}

/// One CRL download. It carries no auth or custom headers, goes through the
/// connection's proxy, and is bounded by time, size and redirects.
async fn fetch(proxy: &ProxyConfig, url: &str, limits: CrlFetchLimits) -> Option<Vec<u8>> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(limits.timeout)
        .timeout(limits.timeout)
        .redirect(reqwest::redirect::Policy::limited(CRL_MAX_REDIRECTS));
    if let Some(proxy_url) = &proxy.url {
        let mut via = reqwest::Proxy::all(proxy_url).ok()?;
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
    let mut response = builder.build().ok()?.get(url).send().await.ok()?;
    if response.status() != StatusCode::OK
        || response
            .content_length()
            .is_some_and(|len| len > limits.max_bytes as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len() + chunk.len() > limits.max_bytes {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    Some(body)
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
        let mut crls = Vec::new();
        let mut used_urls = Vec::new();
        let mut undetermined = false;
        for cert in std::iter::once(end_entity).chain(intermediates) {
            let Some(parsed) = parse_cert(cert.as_ref()) else {
                undetermined = true;
                continue;
            };
            // Trust anchors are not revocation-checked (webpki does not check them either).
            if parsed.self_issued {
                continue;
            }
            let mut covered = false;
            for url in &parsed.crl_uris {
                let Some(der) = self.fetcher.get(url, now_secs) else {
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
            .map_err(|e| rustls::Error::General(format!("revocation verifier setup: {e}")))?;
        let result = self.tolerate_name(verifier.verify_server_cert(
            end_entity,
            intermediates,
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
    for pem in pems {
        for cert in CertificateDer::pem_slice_iter(&pem) {
            roots.add(cert.map_err(tls_error)?).map_err(tls_error)?;
        }
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
            limits: tls.crl_fetch_limits,
            cache: Mutex::new(HashMap::new()),
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
    Ok(config)
}

#[cfg(test)]
#[path = "tls_revocation_tests.rs"]
mod tests;
