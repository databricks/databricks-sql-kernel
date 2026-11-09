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

use super::*;
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

#[derive(Clone, Copy)]
enum CertificateFailure {
    UnknownCa,
    SelfSigned,
    HostnameMismatch,
}

fn test_client(config: HttpClientConfig, overall_timeout: Duration) -> DatabricksHttpClient {
    let retry = RetryConfig {
        max_retries: 2,
        min_wait: Duration::from_millis(1),
        max_wait: Duration::from_millis(1),
        overall_timeout,
        ..Default::default()
    };
    DatabricksHttpClient::new(config, HashMap::from([(RequestCategory::Sea, retry)])).unwrap()
}

async fn assert_certificate_failure(failure: CertificateFailure) {
    for request_type in [RequestType::CreateSession, RequestType::ExecuteStatement] {
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "Retry test CA");
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);

        let name = match failure {
            CertificateFailure::HostnameMismatch => "wrong-host.invalid",
            _ => "127.0.0.1",
        };
        let server_params = CertificateParams::new(vec![name.to_string()]).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let server_cert = match failure {
            CertificateFailure::SelfSigned => server_params.self_signed(&server_key).unwrap(),
            _ => server_params.signed_by(&server_key, &issuer).unwrap(),
        };
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![server_cert.der().clone()],
            PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        )
        .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, mut stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut attempts = 0;
            let mut requests = 0;
            loop {
                let socket = tokio::select! {
                    _ = &mut stopped => return (attempts, requests),
                    accepted = listener.accept() => accepted.unwrap().0,
                };
                attempts += 1;
                if let Ok(mut stream) = acceptor.accept(socket).await {
                    let mut buffer = [0; 1024];
                    if stream.read(&mut buffer).await.unwrap_or(0) > 0 {
                        requests += 1;
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await;
                    }
                }
            }
        });

        let mut config = HttpClientConfig::default();
        if matches!(failure, CertificateFailure::HostnameMismatch) {
            config.tls.trusted_certificate_pem = Some(ca_cert.pem().into_bytes());
        }
        let client = test_client(config, Duration::from_secs(10));
        let request = client
            .inner()
            .post(format!("https://{address}/api/2.0/sql/sessions"))
            .build()
            .unwrap();
        let result = client.execute_without_auth(request, request_type).await;
        shutdown.send(()).unwrap();
        let (attempts, requests) = server.await.unwrap();
        let error = result.expect_err("invalid server certificate must be rejected");
        assert_eq!(attempts, 1, "certificate failure must not retry: {error}");
        assert_eq!(requests, 0, "HTTP request must not reach the TLS server");
        assert!(error.message.contains("after 1 attempts"), "{error}");

        let expected_causes: &[&str] = match failure {
            CertificateFailure::UnknownCa => &[
                "unknownissuer",
                "unable to get local issuer",
                "not trusted",
                "untrusted",
                "unknown root",
                "invalid certificate chain",
            ],
            CertificateFailure::SelfSigned => &[
                "unknownissuer",
                "self-signed",
                "self signed",
                "not trusted",
                "untrusted",
                "unknown root",
                "invalid certificate chain",
            ],
            CertificateFailure::HostnameMismatch => &[
                "notvalidforname",
                "not valid for name",
                "ip address mismatch",
                "hostname mismatch",
                "host name mismatch",
                "name does not match",
                "name mismatch",
                "target principal name is incorrect",
                "name in the certificate is invalid",
            ],
        };
        let message = error.message.to_ascii_lowercase();
        assert!(
            expected_causes.iter().any(|cause| message.contains(cause)),
            "expected certificate cause in error message: {error}"
        );
    }
}

#[tokio::test]
async fn unknown_ca_fails_once_with_certificate_cause() {
    assert_certificate_failure(CertificateFailure::UnknownCa).await;
}

#[tokio::test]
async fn self_signed_fails_once_with_certificate_cause() {
    assert_certificate_failure(CertificateFailure::SelfSigned).await;
}

#[tokio::test]
async fn hostname_mismatch_fails_once_with_certificate_cause() {
    assert_certificate_failure(CertificateFailure::HostnameMismatch).await;
}

#[tokio::test]
async fn tls_handshake_timeout_still_retries() {
    for request_type in [RequestType::CreateSession, RequestType::ExecuteStatement] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, mut stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut sockets = Vec::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => return sockets.len(),
                    accepted = listener.accept() => sockets.push(accepted.unwrap().0),
                }
            }
        });
        let config = HttpClientConfig {
            connect_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let client = test_client(config, Duration::from_secs(10));
        let request = client
            .inner()
            .get(format!("https://{address}/"))
            .build()
            .unwrap();
        let error = client
            .execute_without_auth(request, request_type)
            .await
            .unwrap_err();
        shutdown.send(()).unwrap();
        assert_eq!(server.await.unwrap(), 3, "{error}");
        assert!(error.message.contains("after 3 attempts"), "{error}");
        assert!(
            error.message.contains("timed out"),
            "timeout cause missing: {error}"
        );
    }
}

#[tokio::test]
async fn retry_timeout_preserves_connect_cause() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let client = test_client(HttpClientConfig::default(), Duration::ZERO);
    let request = client
        .inner()
        .get(format!("http://{address}/"))
        .build()
        .unwrap();
    let error = client
        .execute_without_auth(request, RequestType::CreateSession)
        .await
        .unwrap_err();
    assert!(
        error
            .message
            .contains("retry timeout exceeded after 1 attempts"),
        "{error}"
    );
    assert!(
        error.message.to_ascii_lowercase().contains("refused"),
        "connect cause missing: {error}"
    );
}
