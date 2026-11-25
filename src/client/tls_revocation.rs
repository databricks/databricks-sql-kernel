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

//! Certificate revocation for the `tls-rustls` backend on Windows, through CryptoAPI.
//!
//! Trust stays with webpki. The connection's `WebPkiServerVerifier`, over the kernel's
//! roots, verifies the chain, the name and the signatures, as it does with revocation off.
//! Only after that succeeds is revocation asked of CryptoAPI (`CertGetCertificateChain`).
//! CryptoAPI builds the chain from the leaf, the presented intermediates and the custom CAs,
//! and checks the revocation of every certificate except the root. Its CRL and OCSP fetches
//! use the Windows networking stack and proxy settings, and Windows caches their results.
//!
//! The decision reads the chain-level revocation bits only. The trust bits, including the
//! root's, are ignored.
//!
//! The check is synchronous, because rustls' verifier is. It runs inside the handshake poll
//! and blocks until CryptoAPI returns. On a multi-thread runtime, `block_in_place` lets the
//! worker's other tasks run elsewhere, but the calling task cannot be cancelled until
//! CryptoAPI returns. A caller's connect or operation timeout can therefore be exceeded by
//! up to the revocation budget. This applies only with revocation on. On a `current_thread`
//! runtime the whole runtime waits, because `block_in_place` is not used there.
//!
//! This module is compiled only on Windows with `tls-rustls`. Elsewhere, revocation is
//! rejected at session open.

use std::ptr::{null, null_mut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{pem::PemObject, CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use windows_sys::Win32::Security::Cryptography::{
    CertAddCertificateContextToStore, CertCloseStore, CertCreateCertificateChainEngine,
    CertCreateCertificateContext, CertFreeCertificateChain, CertFreeCertificateChainEngine,
    CertFreeCertificateContext, CertGetCertificateChain, CertOpenStore,
    CertSetCertificateContextProperty, CERT_CHAIN_CONTEXT, CERT_CHAIN_ENGINE_CONFIG,
    CERT_CHAIN_EXCLUSIVE_ENABLE_CA_FLAG, CERT_CHAIN_PARA,
    CERT_CHAIN_REVOCATION_ACCUMULATIVE_TIMEOUT, CERT_CHAIN_REVOCATION_CHECK_CHAIN_EXCLUDE_ROOT,
    CERT_CONTEXT, CERT_OCSP_RESPONSE_PROP_ID, CERT_STORE_ADD_ALWAYS, CERT_STORE_PROV_MEMORY,
    CERT_TRUST_IS_NOT_SIGNATURE_VALID, CERT_TRUST_IS_OFFLINE_REVOCATION,
    CERT_TRUST_IS_PARTIAL_CHAIN, CERT_TRUST_IS_REVOKED, CERT_TRUST_REVOCATION_STATUS_UNKNOWN,
    CRYPT_INTEGER_BLOB, HCERTCHAINENGINE, HCERTSTORE, PKCS_7_ASN_ENCODING, X509_ASN_ENCODING,
};

#[cfg(test)]
use windows_sys::Win32::Foundation::GetLastError;
// The four bits already imported above (OFFLINE_REVOCATION, PARTIAL_CHAIN, REVOKED,
// REVOCATION_STATUS_UNKNOWN) are not repeated here.
#[cfg(test)]
use windows_sys::Win32::Security::Cryptography::{
    CERT_TRUST_IS_CYCLIC, CERT_TRUST_IS_NOT_TIME_VALID, CERT_TRUST_IS_NOT_VALID_FOR_USAGE,
    CERT_TRUST_IS_UNTRUSTED_ROOT, CERT_TRUST_NO_ISSUANCE_CHAIN_POLICY,
};

use super::http::TlsConfig;
use crate::kernel_error::{Error, Result};

/// The budget for CRL and OCSP retrieval for one handshake. CryptoAPI applies it to all of a
/// chain's revocation fetches together (`CERT_CHAIN_REVOCATION_ACCUMULATIVE_TIMEOUT`). One
/// handshake runs at most two chain builds, and the second gets only the time the first left, so
/// the total stays within this budget. It does not bound AIA or other chain-building fetches,
/// which use the per-URL timeout only.
const REVOCATION_TIMEOUT: Duration = Duration::from_secs(10);
/// The chain flags: check every certificate except the root.
const CHAIN_FLAGS: u32 =
    CERT_CHAIN_REVOCATION_CHECK_CHAIN_EXCLUDE_ROOT | CERT_CHAIN_REVOCATION_ACCUMULATIVE_TIMEOUT;

/// The revocation status of a chain, as CryptoAPI reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Good,
    /// CryptoAPI could not determine the status: a fetch failed or the server is offline, the
    /// chain is partial, or no chain could be built at all.
    Undetermined,
    Revoked,
}

/// An in-memory certificate store. Closed on drop.
struct Store(HCERTSTORE);

impl Store {
    fn memory() -> Option<Self> {
        // SAFETY: the memory provider takes no parameters.
        let handle = unsafe { CertOpenStore(CERT_STORE_PROV_MEMORY, 0, 0, 0, null()) };
        (!handle.is_null()).then_some(Self(handle))
    }

    /// Adds a copy of `der`. The store keeps its own reference, so the temporary context is
    /// freed when it drops. Returns false when CryptoAPI refuses the certificate.
    fn add(&self, der: &[u8]) -> bool {
        let Some(context) = Context::new(der) else {
            return false;
        };
        // SAFETY: both handles are live, and no output context is requested.
        unsafe {
            CertAddCertificateContextToStore(self.0, context.0, CERT_STORE_ADD_ALWAYS, null_mut())
                != 0
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // SAFETY: the handle came from `CertOpenStore` and is closed once.
        unsafe { CertCloseStore(self.0, 0) };
    }
}

/// A certificate context, freed on drop.
struct Context(*mut CERT_CONTEXT);

impl Context {
    fn new(der: &[u8]) -> Option<Self> {
        let len = u32::try_from(der.len()).ok()?;
        // SAFETY: `der` is valid for `len` bytes for the duration of the call.
        let context = unsafe {
            CertCreateCertificateContext(X509_ASN_ENCODING | PKCS_7_ASN_ENCODING, der.as_ptr(), len)
        };
        (!context.is_null()).then_some(Self(context))
    }

    /// Attaches a stapled OCSP response to this certificate, as SChannel does, so CryptoAPI can
    /// use it instead of fetching one. CryptoAPI copies the response into the context, so
    /// `response` needs to live only for the call. Returns false when CryptoAPI refuses it, and
    /// CryptoAPI then fetches as usual.
    fn set_ocsp_response(&self, response: &[u8]) -> bool {
        let Ok(len) = u32::try_from(response.len()) else {
            return false;
        };
        let blob = CRYPT_INTEGER_BLOB {
            cbData: len,
            pbData: response.as_ptr().cast_mut(),
        };
        // SAFETY: the context is live. `blob` and `response` are valid for the call, and the
        // property is set with no flags. The API takes a pointer to the blob, not the blob's
        // contents.
        unsafe {
            CertSetCertificateContextProperty(
                self.0,
                CERT_OCSP_RESPONSE_PROP_ID,
                0,
                (&blob as *const CRYPT_INTEGER_BLOB).cast(),
            ) != 0
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: the handle came from `CertCreateCertificateContext` and is freed once.
        unsafe { CertFreeCertificateContext(self.0) };
    }
}

/// A chain built by CryptoAPI. Freed on drop.
struct ChainContext(*mut CERT_CHAIN_CONTEXT);

impl ChainContext {
    /// The revocation status of the chain, read from each simple chain from the leaf up (see
    /// `status_from_chain`), keeping the worst result. The trust bits are not read.
    fn status(&self, custom_cas: &[&[u8]]) -> Status {
        // SAFETY: the chain is live until this guard drops, and every read is of a field CryptoAPI
        // sets. Each simple chain pointer is valid for `cChain`, each element pointer for the
        // `cElement` of its simple chain, and each certificate pointer for its element. The DER
        // bytes stay valid while the chain lives, and are used only within this call.
        unsafe {
            let chain = &*self.0;
            let chain_bits = chain.TrustStatus.dwErrorStatus;
            if chain.cChain == 0 {
                return status_from_chain(chain_bits, &[], custom_cas);
            }
            let mut worst = Status::Good;
            for i in 0..chain.cChain as usize {
                let simple = &**chain.rgpChain.add(i);
                let mut elements: Vec<(&[u8], u32)> = Vec::with_capacity(simple.cElement as usize);
                for j in 0..simple.cElement as usize {
                    let element = &**simple.rgpElement.add(j);
                    let cert = &*element.pCertContext;
                    let der: &[u8] = if cert.pbCertEncoded.is_null() {
                        &[]
                    } else {
                        std::slice::from_raw_parts(cert.pbCertEncoded, cert.cbCertEncoded as usize)
                    };
                    elements.push((der, element.TrustStatus.dwErrorStatus));
                }
                worst = worse(worst, status_from_chain(chain_bits, &elements, custom_cas));
            }
            worst
        }
    }
}

/// The more severe of two statuses: revoked, then undetermined, then good.
fn worse(a: Status, b: Status) -> Status {
    match (a, b) {
        (Status::Revoked, _) | (_, Status::Revoked) => Status::Revoked,
        (Status::Undetermined, _) | (_, Status::Undetermined) => Status::Undetermined,
        _ => Status::Good,
    }
}

/// The status of one simple chain. `elements` are its certificates, from the leaf up, with their
/// trust error bits. `chain_bits` are the chain-level bits, which are used only when no element is
/// a custom CA.
///
/// A custom CA on the chain is a trust anchor. Anchors are not revocation-checked, and neither are
/// the certificates above one, so the elements below the first anchor are read on their own. The
/// chain-level bits are dropped then, because they combine the anchor and the certificates above
/// it. Without a custom CA on the chain, the chain-level and element bits are read as before.
fn status_from_chain(chain_bits: u32, elements: &[(&[u8], u32)], custom_cas: &[&[u8]]) -> Status {
    match elements
        .iter()
        .position(|(der, _)| custom_cas.contains(der))
    {
        Some(anchor) => {
            let bits: Vec<u32> = elements[..anchor].iter().map(|(_, bits)| *bits).collect();
            status_from_bits(&bits)
        }
        None => {
            let mut bits = vec![chain_bits];
            bits.extend(elements.iter().map(|(_, bits)| *bits));
            status_from_bits(&bits)
        }
    }
}

/// Maps the trust error bits of a chain and its elements to a status. Pure, so each case is
/// testable on its own. A revoked bit anywhere makes the chain revoked. A bad signature, unknown
/// or offline revocation, or a partial chain anywhere makes it undetermined: a signature that does
/// not verify is no evidence that the chain is good. Otherwise it is good.
fn status_from_bits(bits: &[u32]) -> Status {
    const UNDETERMINED: u32 = CERT_TRUST_IS_NOT_SIGNATURE_VALID
        | CERT_TRUST_REVOCATION_STATUS_UNKNOWN
        | CERT_TRUST_IS_OFFLINE_REVOCATION
        | CERT_TRUST_IS_PARTIAL_CHAIN;
    if bits.iter().any(|bit| bit & CERT_TRUST_IS_REVOKED != 0) {
        Status::Revoked
    } else if bits.iter().any(|bit| bit & UNDETERMINED != 0) {
        Status::Undetermined
    } else {
        Status::Good
    }
}

#[cfg(test)]
impl ChainContext {
    /// Test-only: the chain's shape and trust bits, by name, for diagnosing a failing run.
    fn describe(&self) -> String {
        // SAFETY: the chain is live for the call. Every read is of a field CryptoAPI sets, and
        // each pointer is valid for the count that bounds it (`cChain`, `cElement`).
        unsafe {
            let chain = &*self.0;
            let mut out = format!(
                "cChain={} error=[{}] info=0x{:x}",
                chain.cChain,
                trust_names(chain.TrustStatus.dwErrorStatus),
                chain.TrustStatus.dwInfoStatus
            );
            for i in 0..chain.cChain as usize {
                let simple = &**chain.rgpChain.add(i);
                out += &format!(
                    " | simple[{i}] cElement={} error=[{}]",
                    simple.cElement,
                    trust_names(simple.TrustStatus.dwErrorStatus)
                );
                for j in 0..simple.cElement as usize {
                    let element = &**simple.rgpElement.add(j);
                    out += &format!(
                        " | element[{j}] error=[{}] info=0x{:x} revocation_info={}",
                        trust_names(element.TrustStatus.dwErrorStatus),
                        element.TrustStatus.dwInfoStatus,
                        !element.pRevocationInfo.is_null()
                    );
                }
            }
            out
        }
    }
}

/// Test-only: the names of the trust error bits set in `bits`.
#[cfg(test)]
fn trust_names(bits: u32) -> String {
    const NAMES: &[(u32, &str)] = &[
        (CERT_TRUST_IS_NOT_TIME_VALID, "NOT_TIME_VALID"),
        (CERT_TRUST_IS_REVOKED, "REVOKED"),
        (CERT_TRUST_IS_NOT_SIGNATURE_VALID, "NOT_SIGNATURE_VALID"),
        (CERT_TRUST_IS_NOT_VALID_FOR_USAGE, "NOT_VALID_FOR_USAGE"),
        (CERT_TRUST_IS_UNTRUSTED_ROOT, "UNTRUSTED_ROOT"),
        (
            CERT_TRUST_REVOCATION_STATUS_UNKNOWN,
            "REVOCATION_STATUS_UNKNOWN",
        ),
        (CERT_TRUST_IS_CYCLIC, "CYCLIC"),
        (CERT_TRUST_IS_PARTIAL_CHAIN, "PARTIAL_CHAIN"),
        (CERT_TRUST_IS_OFFLINE_REVOCATION, "OFFLINE_REVOCATION"),
        (
            CERT_TRUST_NO_ISSUANCE_CHAIN_POLICY,
            "NO_ISSUANCE_CHAIN_POLICY",
        ),
    ];
    let names: Vec<&str> = NAMES
        .iter()
        .filter(|(bit, _)| bits & bit != 0)
        .map(|(_, name)| *name)
        .collect();
    if names.is_empty() {
        "none".to_owned()
    } else {
        names.join(",")
    }
}

impl Drop for ChainContext {
    fn drop(&mut self) {
        // SAFETY: the chain came from `CertGetCertificateChain` and is freed once.
        unsafe { CertFreeCertificateChain(self.0) };
    }
}

/// CryptoAPI's revocation status for the chain that `leaf` builds. The presented
/// intermediates and the custom CAs are in the chain's additional store, so CryptoAPI can
/// reach the issuers that Windows does not trust. Trust is not decided here.
fn cryptoapi_status(
    leaf: &[u8],
    intermediates: &[&[u8]],
    custom_cas: &[&[u8]],
    ocsp: &[u8],
) -> Status {
    // Each handle has a guard, so every early return frees what was opened.
    let Some(store) = Store::memory() else {
        return Status::Undetermined;
    };
    // A certificate that CryptoAPI refuses is skipped, not fatal. Webpki already ignored
    // it, and it cannot hide the leaf's status. A chain that needs it comes out partial,
    // which is undetermined.
    for der in intermediates.iter().chain(custom_cas) {
        store.add(der);
    }
    let Some(leaf) = Context::new(leaf) else {
        return Status::Undetermined;
    };
    // The stapled response is for the leaf. An empty or refused one is ignored, and CryptoAPI
    // fetches as usual.
    if !ocsp.is_empty() {
        let _attached = leaf.set_ocsp_response(ocsp);
    }
    let started = Instant::now();
    let status = chain_status(
        "default engine",
        &leaf,
        &store,
        null_mut(),
        REVOCATION_TIMEOUT,
        custom_cas,
    );
    // With a custom CA, Windows does not trust its root. The default engine can then end the
    // chain at an untrusted root and report no revocation status, so the result is undetermined.
    // Build again with an engine whose exclusive root is the custom CAs, which makes the chain
    // complete for this check. Webpki still decides trust, and only the revocation bits are read.
    // The second build gets only the budget the first left, so one handshake waits at most
    // REVOCATION_TIMEOUT in total. If nothing is left, the first result stands.
    if status != Status::Undetermined || custom_cas.is_empty() {
        return status;
    }
    let remaining = REVOCATION_TIMEOUT.saturating_sub(started.elapsed());
    // Under 1 ms counts as none left: dwUrlRetrievalTimeout is in milliseconds, and 0 there
    // means CryptoAPI's own default timeout, not "no time".
    if remaining.as_millis() == 0 {
        return status;
    }
    let Some(roots) = Store::memory() else {
        return status;
    };
    for der in custom_cas {
        roots.add(der);
    }
    let Some(engine) = ChainEngine::exclusive(&roots) else {
        return status;
    };
    chain_status(
        "exclusive-root engine",
        &leaf,
        &store,
        engine.0,
        remaining,
        custom_cas,
    )
}

/// Builds the chain for `leaf` with `engine` (null is the default engine) and reads its
/// revocation status. `budget` bounds the revocation fetches of this build. `custom_cas` are the
/// trust anchors that the status reading stops at. `label` names the engine in the test-only
/// diagnostics.
fn chain_status(
    label: &str,
    leaf: &Context,
    store: &Store,
    engine: HCERTCHAINENGINE,
    budget: Duration,
    custom_cas: &[&[u8]],
) -> Status {
    // Only the test-only diagnostics below read `label`.
    let _ = label;
    let para = CERT_CHAIN_PARA {
        cbSize: std::mem::size_of::<CERT_CHAIN_PARA>() as u32,
        // Per-URL timeout for chain-building fetches (AIA). With the accumulative flag, the
        // revocation fetches share this budget in total.
        dwUrlRetrievalTimeout: budget.as_millis() as u32,
        ..Default::default()
    };
    let mut chain: *mut CERT_CHAIN_CONTEXT = null_mut();
    // SAFETY: the engine, leaf and store are live for the call, and `para` outlives it. Null
    // time is now.
    let built = unsafe {
        CertGetCertificateChain(
            engine,
            leaf.0,
            null(),
            store.0,
            &para,
            CHAIN_FLAGS,
            null(),
            &mut chain,
        )
    };
    if built == 0 || chain.is_null() {
        // SAFETY: GetLastError reads the calling thread's last error and has no other effect.
        #[cfg(test)]
        eprintln!(
            "revocation diagnostics [{label}]: CertGetCertificateChain failed, GetLastError={}",
            unsafe { GetLastError() }
        );
        return Status::Undetermined;
    }
    let chain = ChainContext(chain);
    #[cfg(test)]
    eprintln!("revocation diagnostics [{label}]: {}", chain.describe());
    chain.status(custom_cas)
}

/// A chain engine whose exclusive root is a store of custom CAs. Freed on drop.
struct ChainEngine(HCERTCHAINENGINE);

impl ChainEngine {
    fn exclusive(roots: &Store) -> Option<Self> {
        let config = CERT_CHAIN_ENGINE_CONFIG {
            cbSize: std::mem::size_of::<CERT_CHAIN_ENGINE_CONFIG>() as u32,
            hExclusiveRoot: roots.0,
            // Without this flag a CA certificate that is not self-signed is not treated as a
            // root, so a custom CA configured as an intermediate would leave the chain partial.
            dwExclusiveFlags: CERT_CHAIN_EXCLUSIVE_ENABLE_CA_FLAG,
            ..Default::default()
        };
        let mut engine: HCERTCHAINENGINE = null_mut();
        // SAFETY: `config` and the root store are live for the call, and `engine` is written
        // only on success.
        let created = unsafe { CertCreateCertificateChainEngine(&config, &mut engine) };
        (created != 0 && !engine.is_null()).then_some(Self(engine))
    }
}

impl Drop for ChainEngine {
    fn drop(&mut self) {
        // SAFETY: the engine came from `CertCreateCertificateChainEngine` and is freed once.
        unsafe { CertFreeCertificateChainEngine(self.0) };
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

fn tls_error(e: impl std::fmt::Display) -> Error {
    Error::invalid_argument().message(format!("invalid TLS configuration: {e}"))
}

/// The server verifier. Webpki decides the chain, the name and the signatures. CryptoAPI
/// decides the revocation of the chain it builds for the same leaf.
#[derive(Debug)]
struct RevocationVerifier {
    /// Webpki over the kernel's roots, without CRLs.
    signatures: Arc<WebPkiServerVerifier>,
    /// The custom CAs. CryptoAPI needs them as issuers when Windows does not trust them.
    custom_cas: Vec<CertificateDer<'static>>,
    accept_undetermined: bool,
    allow_hostname_mismatch: bool,
}

impl RevocationVerifier {
    /// Forgives a name-only failure under `allow_hostname_mismatch`. webpki checks the name
    /// last, so the chain has already verified when this happens.
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
        // Chain, name and validity come first, so a chain webpki rejects never reaches
        // CryptoAPI and its errors are the same as with revocation off.
        self.tolerate_name(self.signatures.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ))?;
        let presented: Vec<&[u8]> = intermediates.iter().map(AsRef::as_ref).collect();
        let custom: Vec<&[u8]> = self.custom_cas.iter().map(AsRef::as_ref).collect();
        let leaf: &[u8] = end_entity.as_ref();
        match in_runtime_blocking(|| cryptoapi_status(leaf, &presented, &custom, ocsp_response)) {
            Status::Good => Ok(ServerCertVerified::assertion()),
            Status::Revoked => Err(rustls::Error::InvalidCertificate(CertificateError::Revoked)),
            Status::Undetermined if self.accept_undetermined => Ok(ServerCertVerified::assertion()),
            Status::Undetermined => Err(rustls::Error::InvalidCertificate(
                CertificateError::UnknownRevocationStatus,
            )),
        }
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

/// The rustls config for a connection with revocation checking. Trust roots are webpki
/// roots, then the OS store, then custom CAs, as reqwest's rustls assembly uses. ALPN is
/// HTTP/1.1, since the kernel enables no HTTP/2.
///
/// The caller has already validated the PEM bytes and the mTLS identity pair, so they are
/// parsed here without re-checking.
pub(crate) fn client_config(tls: &TlsConfig, accept_undetermined: bool) -> Result<ClientConfig> {
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
    let mut custom_cas = Vec::new();
    for pem in pems {
        for cert in CertificateDer::pem_slice_iter(&pem) {
            let cert = cert.map_err(tls_error)?;
            roots.add(cert.clone()).map_err(tls_error)?;
            custom_cas.push(cert);
        }
    }

    let signatures =
        WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
            .build()
            .map_err(tls_error)?;
    let verifier = RevocationVerifier {
        signatures,
        custom_cas,
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
            let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key).map_err(tls_error)?;
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
pub(crate) mod tests;
