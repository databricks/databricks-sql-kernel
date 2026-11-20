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
//! This module is compiled only on Windows with `tls-rustls`. Elsewhere, revocation is
//! rejected at session open.

use std::ptr::{null, null_mut};
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{pem::PemObject, CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use windows_sys::Win32::Security::Cryptography::{
    CertAddCertificateContextToStore, CertCloseStore, CertCreateCertificateContext,
    CertFreeCertificateChain, CertFreeCertificateContext, CertGetCertificateChain, CertOpenStore,
    CERT_CHAIN_CONTEXT, CERT_CHAIN_PARA, CERT_CHAIN_REVOCATION_ACCUMULATIVE_TIMEOUT,
    CERT_CHAIN_REVOCATION_CHECK_CHAIN_EXCLUDE_ROOT, CERT_CONTEXT, CERT_STORE_ADD_ALWAYS,
    CERT_STORE_PROV_MEMORY, CERT_TRUST_IS_OFFLINE_REVOCATION, CERT_TRUST_IS_PARTIAL_CHAIN,
    CERT_TRUST_IS_REVOKED, CERT_TRUST_REVOCATION_STATUS_UNKNOWN, HCERTSTORE, PKCS_7_ASN_ENCODING,
    X509_ASN_ENCODING,
};

use super::http::TlsConfig;
use crate::kernel_error::{Error, Result};

/// The budget for CRL and OCSP retrieval for one chain. CryptoAPI applies it to all of the
/// chain's fetches together (`CERT_CHAIN_REVOCATION_ACCUMULATIVE_TIMEOUT`).
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
    /// freed when it drops.
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
    /// The chain-level revocation status. CryptoAPI combines the status of every element
    /// (the root is not checked, so its bits do not appear). The trust bits are not read.
    fn status(&self) -> Status {
        // SAFETY: the chain is live until this guard drops, and the read does not write.
        let bits = unsafe { (*self.0).TrustStatus.dwErrorStatus };
        if bits & CERT_TRUST_IS_REVOKED != 0 {
            Status::Revoked
        } else if bits
            & (CERT_TRUST_REVOCATION_STATUS_UNKNOWN
                | CERT_TRUST_IS_OFFLINE_REVOCATION
                | CERT_TRUST_IS_PARTIAL_CHAIN)
            != 0
        {
            Status::Undetermined
        } else {
            Status::Good
        }
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
fn cryptoapi_status(leaf: &[u8], intermediates: &[&[u8]], custom_cas: &[&[u8]]) -> Status {
    // Each handle has a guard, so every early return frees what was opened.
    let Some(store) = Store::memory() else {
        return Status::Undetermined;
    };
    for der in intermediates.iter().chain(custom_cas) {
        if !store.add(der) {
            return Status::Undetermined;
        }
    }
    let Some(leaf) = Context::new(leaf) else {
        return Status::Undetermined;
    };
    let para = CERT_CHAIN_PARA {
        cbSize: std::mem::size_of::<CERT_CHAIN_PARA>() as u32,
        dwUrlRetrievalTimeout: REVOCATION_TIMEOUT.as_millis() as u32,
        ..Default::default()
    };
    let mut chain: *mut CERT_CHAIN_CONTEXT = null_mut();
    // SAFETY: the leaf and store are live, and `para` outlives the call. The null chain
    // engine is the default, and null time is now.
    let built = unsafe {
        CertGetCertificateChain(
            null_mut(),
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
        return Status::Undetermined;
    }
    ChainContext(chain).status()
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
        match in_runtime_blocking(|| cryptoapi_status(leaf, &presented, &custom)) {
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
mod tests;
