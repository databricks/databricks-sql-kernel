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

//! Hermetic proof that the `tls-rustls` backend reads the OS system trust
//! store, not only Mozilla's bundled `webpki-roots` (odbc#323).
//!
//! `tls-rustls` is the backend the Linux **aarch64** cross-build selects (to
//! avoid a cross OpenSSL); the x86_64 / i686 builds use `tls-native` (OpenSSL),
//! which reads `/etc/ssl/...`. Before the fix, `tls-rustls = ["reqwest/rustls-tls"]`
//! resolved to `rustls-tls-webpki-roots` — the Mozilla set only — so a server
//! whose chain terminates in a **private CA installed only in the OS trust
//! store** failed the handshake on aarch64 while succeeding on x86_64. The fix
//! adds `reqwest/rustls-tls-native-roots`, which loads the OS store alongside
//! webpki-roots.
//!
//! This test is hermetic: it generates a private CA + server cert at runtime,
//! serves HTTPS from that identity, and points `rustls-native-certs` at the CA
//! via **`SSL_CERT_FILE`** (which `rustls-native-certs` 0.8 honours as the OS
//! trust store on unix). The kernel's default HTTP client — **no** custom CA,
//! **no** `allow_self_signed` — must then complete the handshake. Under the old
//! webpki-roots-only wiring the same client rejects the server as an unknown
//! issuer, so the test fails without the fix and passes with it.
//!
//! Gated on `tls-rustls`: under the default `tls-native` backend the system
//! store is always read and there is nothing arch-specific to prove.
#![cfg(feature = "tls-rustls")]

use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig, TlsConfig};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use std::io::Write;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// A CA plus a server leaf signed by it, all as PEM/DER byte blobs.
struct ServerPki {
    /// The CA certificate, PEM-encoded — what we write to `SSL_CERT_FILE`.
    ca_pem: Vec<u8>,
    server_cert_der: Vec<u8>,
    server_key_der: Vec<u8>,
}

/// Generate a self-signed CA and a `127.0.0.1` server certificate signed by
/// it. Mirrors the PKI helpers in `c_abi_mtls_e2e.rs`, trimmed to the
/// server-auth side (this test does not exercise mTLS).
fn server_pki() -> ServerPki {
    // CA.
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem().into_bytes();
    let issuer = Issuer::new(ca_params, ca_key);

    // Server leaf, SAN = 127.0.0.1 (we connect to the literal address).
    let mut leaf_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    leaf_params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
    leaf_params
        .key_usages
        .push(KeyUsagePurpose::DigitalSignature);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer).unwrap();

    ServerPki {
        ca_pem,
        server_cert_der: leaf_cert.der().to_vec(),
        server_key_der: leaf_key.serialize_der(),
    }
}

/// A minimal one-shot HTTPS server: accepts a single connection, completes the
/// TLS handshake with the given server identity, replies `200 OK`, and
/// reports whether the handshake succeeded.
struct OneShotHttpsServer {
    host: String,
    handshake: mpsc::Receiver<bool>,
    thread: thread::JoinHandle<()>,
}

impl OneShotHttpsServer {
    fn start(pki: &ServerPki) -> Self {
        // Server presents only its leaf; no client-cert verification.
        // `builder_with_provider` (not the bare `builder`) so the test does
        // not depend on a process-level default `CryptoProvider` being
        // installed — the crate's `rustls` is `default-features = false` +
        // `ring`, so there is no `aws-lc-rs` default provider to fall back on.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(pki.server_cert_der.clone())],
                PrivateKeyDer::from(PrivatePkcs8KeyDer::from(pki.server_key_der.clone())),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let (address_tx, address_rx) = mpsc::sync_channel(1);
        let (handshake_tx, handshake_rx) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
                address_tx.send(listener.local_addr().unwrap()).unwrap();
                let (socket, _) = listener.accept().await.unwrap();

                let mut stream = match acceptor.accept(socket).await {
                    Ok(stream) => stream,
                    Err(_) => {
                        // Handshake rejected by the client (unknown issuer) —
                        // the client tore down the connection.
                        let _ = handshake_tx.send(false);
                        return;
                    }
                };
                let _ = handshake_tx.send(true);

                // Drain the request headers, then reply.
                let mut buffer = [0_u8; 1024];
                let _ = stream.read(&mut buffer).await;
                let body = b"ok";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(body).await;
                let _ = stream.shutdown().await;
            });
        });

        let address = address_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        Self {
            // IPv4 literal to dodge platform-dependent localhost/IPv6 resolution.
            host: format!("https://127.0.0.1:{}", address.port()),
            handshake: handshake_rx,
            thread,
        }
    }

    /// Whether the server saw the TLS handshake complete.
    fn handshake_completed(self) -> bool {
        let completed = self
            .handshake
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or(false);
        let _ = self.thread.join();
        completed
    }
}

/// A private-CA server that trusts only what's in the OS trust store
/// (simulated via `SSL_CERT_FILE`) must be reachable by the kernel's default
/// HTTP client under `tls-rustls`. Without `rustls-tls-native-roots` the client
/// only trusts Mozilla's bundled roots and rejects the server — the exact
/// aarch64 divergence odbc#323 describes.
#[tokio::test(flavor = "multi_thread")]
async fn default_client_trusts_ca_from_system_store() {
    let pki = server_pki();

    // Write the CA to a temp file and expose it as the "system" trust store.
    // `rustls-native-certs` (used by `reqwest/rustls-tls-native-roots`) reads
    // `SSL_CERT_FILE` in preference to the platform default on unix.
    let mut ca_file = tempfile::NamedTempFile::new().expect("create temp CA file");
    ca_file.write_all(&pki.ca_pem).expect("write CA PEM");
    ca_file.flush().expect("flush CA PEM");
    std::env::set_var("SSL_CERT_FILE", ca_file.path());

    let server = OneShotHttpsServer::start(&pki);
    let url = server.host.clone();

    // Default TLS: strict validation, NO custom CA, NO self-signed relaxation.
    // The only way the handshake can succeed is if the client picked up our CA
    // from the system trust store.
    let config = HttpClientConfig {
        tls: TlsConfig::default(),
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let client = DatabricksHttpClient::with_default_retry(config).expect("build client");

    let result = client.inner().get(&url).send().await;

    std::env::remove_var("SSL_CERT_FILE");
    let handshake_completed = server.handshake_completed();

    let response = result.unwrap_or_else(|e| {
        panic!(
            "handshake against a private-CA server failed — the `tls-rustls` \
             backend did not read the OS trust store (SSL_CERT_FILE). This is \
             the aarch64 divergence from odbc#323; confirm \
             `reqwest/rustls-tls-native-roots` is enabled. error: {e}"
        )
    });
    assert!(
        handshake_completed,
        "server did not observe a completed TLS handshake"
    );
    assert!(
        response.status().is_success(),
        "expected 200 from the private-CA server, got {}",
        response.status()
    );
}
