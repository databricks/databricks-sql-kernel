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

//! Hermetic end-to-end mTLS tests for the Go/ODBC-facing C ABI.
//!
//! The tests generate a CA, server identity, and client identities at runtime,
//! then drive:
//!
//! C ABI setter → kernel `TlsConfig` → reqwest TLS client → rustls test server.
//!
//! No live Databricks workspace or committed private key is required.

use databricks_sql_kernel::c_abi::session::{
    kernel_session_close, kernel_session_config_new, kernel_session_config_set_auth_pat,
    kernel_session_config_set_http_path, kernel_session_config_set_retry_config,
    kernel_session_config_set_tls_client_certificate, kernel_session_config_set_tls_trusted_certs,
    kernel_session_open, kernel_session_t, KernelSessionConfig,
};
use databricks_sql_kernel::c_abi::KernelStatusCode;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::ffi::CString;
use std::ptr;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

struct Identity {
    cert_der: Vec<u8>,
    cert_pem: Vec<u8>,
    key_der: Vec<u8>,
    key_pem: Vec<u8>,
}

struct TestPki {
    ca_der: Vec<u8>,
    ca_pem: Vec<u8>,
    server: Identity,
    client: Identity,
    untrusted_client: Identity,
}

fn certificate_authority() -> (Certificate, Issuer<'static, KeyPair>) {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    (cert, Issuer::new(params, key))
}

fn leaf_identity(
    issuer: &Issuer<'_, KeyPair>,
    subject_alt_names: Vec<String>,
    usage: ExtendedKeyUsagePurpose,
) -> Identity {
    let mut params = CertificateParams::new(subject_alt_names).unwrap();
    params.extended_key_usages.push(usage);
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, issuer).unwrap();

    Identity {
        cert_der: cert.der().to_vec(),
        cert_pem: cert.pem().into_bytes(),
        key_der: key.serialize_der(),
        key_pem: key.serialize_pem().into_bytes(),
    }
}

fn test_pki() -> TestPki {
    let (ca, issuer) = certificate_authority();
    let server = leaf_identity(
        &issuer,
        vec!["localhost".to_string()],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let client = leaf_identity(&issuer, Vec::new(), ExtendedKeyUsagePurpose::ClientAuth);

    let (_other_ca, other_issuer) = certificate_authority();
    let untrusted_client = leaf_identity(
        &other_issuer,
        Vec::new(),
        ExtendedKeyUsagePurpose::ClientAuth,
    );

    TestPki {
        ca_der: ca.der().to_vec(),
        ca_pem: ca.pem().into_bytes(),
        server,
        client,
        untrusted_client,
    }
}

#[derive(Debug)]
enum ServerOutcome {
    PresentedCertificate(Vec<u8>),
    HandshakeRejected(String),
}

struct MtlsServer {
    host: String,
    outcome: mpsc::Receiver<ServerOutcome>,
    thread: thread::JoinHandle<()>,
}

impl MtlsServer {
    fn start(pki: &TestPki) -> Self {
        let mut client_roots = RootCertStore::empty();
        client_roots
            .add(CertificateDer::from(pki.ca_der.clone()))
            .unwrap();

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let client_verifier =
            WebPkiClientVerifier::builder_with_provider(client_roots.into(), provider.clone())
                .build()
                .unwrap();
        let config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(
                vec![CertificateDer::from(pki.server.cert_der.clone())],
                PrivateKeyDer::from(PrivatePkcs8KeyDer::from(pki.server.key_der.clone())),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let (address_tx, address_rx) = mpsc::sync_channel(1);
        let (outcome_tx, outcome_rx) = mpsc::sync_channel(1);
        let server_thread = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
                address_tx.send(listener.local_addr().unwrap()).unwrap();
                let (socket, _) = listener.accept().await.unwrap();

                let mut stream = match acceptor.accept(socket).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        outcome_tx
                            .send(ServerOutcome::HandshakeRejected(error.to_string()))
                            .unwrap();
                        return;
                    }
                };

                let peer_cert = stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|certs| certs.first())
                    .map(|cert| cert.as_ref().to_vec())
                    .expect("client certificate required by the verifier");
                outcome_tx
                    .send(ServerOutcome::PresentedCertificate(peer_cert))
                    .unwrap();

                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }

                let body = br#"{"session_id":"session-mtls","warehouse_id":"test_wh"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        });

        let address = address_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        Self {
            host: format!("https://localhost:{}", address.port()),
            outcome: outcome_rx,
            thread: server_thread,
        }
    }

    fn finish(self) -> ServerOutcome {
        let outcome = self
            .outcome
            .recv_timeout(Duration::from_secs(10))
            .expect("mTLS server did not complete");
        self.thread.join().unwrap();
        outcome
    }
}

unsafe fn open_session(
    host: &str,
    ca_pem: &[u8],
    identity: Option<&Identity>,
) -> (KernelStatusCode, *mut kernel_session_t) {
    let mut config: *mut KernelSessionConfig = ptr::null_mut();
    assert_eq!(
        unsafe { kernel_session_config_new(&mut config) },
        KernelStatusCode::Success
    );

    let host = CString::new(host).unwrap();
    let http_path = CString::new("/sql/1.0/warehouses/test_wh").unwrap();
    assert_eq!(
        unsafe { kernel_session_config_set_http_path(config, host.as_ptr(), http_path.as_ptr()) },
        KernelStatusCode::Success
    );
    let token = CString::new("dapi-test-token").unwrap();
    assert_eq!(
        unsafe { kernel_session_config_set_auth_pat(config, token.as_ptr()) },
        KernelStatusCode::Success
    );
    assert_eq!(
        unsafe {
            kernel_session_config_set_tls_trusted_certs(config, ca_pem.as_ptr(), ca_pem.len())
        },
        KernelStatusCode::Success
    );
    assert_eq!(
        unsafe { kernel_session_config_set_retry_config(config, 1, 1, 0, 0) },
        KernelStatusCode::Success
    );

    if let Some(identity) = identity {
        assert_eq!(
            unsafe {
                kernel_session_config_set_tls_client_certificate(
                    config,
                    identity.cert_pem.as_ptr(),
                    identity.cert_pem.len(),
                    identity.key_pem.as_ptr(),
                    identity.key_pem.len(),
                )
            },
            KernelStatusCode::Success
        );
    }

    let mut session = ptr::null_mut();
    let status = unsafe { kernel_session_open(config, &mut session) };
    (status, session)
}

fn assert_handshake_rejected(outcome: ServerOutcome) {
    match outcome {
        ServerOutcome::HandshakeRejected(message) => {
            assert!(!message.is_empty(), "handshake error should be descriptive");
        }
        ServerOutcome::PresentedCertificate(_) => {
            panic!("server unexpectedly accepted the client identity");
        }
    }
}

#[test]
fn valid_client_certificate_completes_c_abi_mtls_handshake() {
    let pki = test_pki();
    let server = MtlsServer::start(&pki);

    let (status, session) = unsafe { open_session(&server.host, &pki.ca_pem, Some(&pki.client)) };
    assert_eq!(status, KernelStatusCode::Success);
    assert!(!session.is_null());

    match server.finish() {
        ServerOutcome::PresentedCertificate(cert) => {
            assert_eq!(cert, pki.client.cert_der);
        }
        ServerOutcome::HandshakeRejected(error) => {
            panic!("valid client certificate was rejected: {error}");
        }
    }

    assert_eq!(
        unsafe { kernel_session_close(session) },
        KernelStatusCode::Success
    );
}

#[test]
fn missing_client_certificate_fails_c_abi_mtls_handshake() {
    let pki = test_pki();
    let server = MtlsServer::start(&pki);

    let (status, session) = unsafe { open_session(&server.host, &pki.ca_pem, None) };
    assert_ne!(status, KernelStatusCode::Success);
    assert!(session.is_null());
    assert_handshake_rejected(server.finish());
}

#[test]
fn client_certificate_from_wrong_ca_fails_c_abi_mtls_handshake() {
    let pki = test_pki();
    let server = MtlsServer::start(&pki);

    let (status, session) =
        unsafe { open_session(&server.host, &pki.ca_pem, Some(&pki.untrusted_client)) };
    assert_ne!(status, KernelStatusCode::Success);
    assert!(session.is_null());
    assert_handshake_rejected(server.finish());
}
