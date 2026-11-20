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
    DnsHostnameMismatch,
    Expired,
}

fn test_client(config: HttpClientConfig, overall_timeout: Duration) -> DatabricksHttpClient {
    let retry = RetryConfig {
        max_retries: 2,
        min_wait: Duration::from_millis(1),
        max_wait: Duration::from_millis(1),
        overall_timeout,
        ..Default::default()
    };
    DatabricksHttpClient::new(
        config,
        HashMap::from([
            (RequestCategory::Sea, retry.clone()),
            (RequestCategory::CloudFetch, retry),
        ]),
    )
    .unwrap()
}

async fn download_cloudfetch_chunk(
    client: DatabricksHttpClient,
    url: String,
) -> Result<Option<arrow_array::RecordBatch>> {
    use crate::client::ChunkLinkFetchResult;
    use crate::reader::cloudfetch::{
        ChunkDownloader, ChunkLinkFetcher, StreamingCloudFetchProvider,
    };
    use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
    use crate::types::sea::CompressionCodec;

    #[derive(Debug)]
    struct SingleLink(CloudFetchLink);

    #[async_trait::async_trait]
    impl ChunkLinkFetcher for SingleLink {
        async fn fetch_links(&self, _: i64, _: i64) -> Result<ChunkLinkFetchResult> {
            Ok(ChunkLinkFetchResult {
                links: vec![self.0.clone()],
                ..ChunkLinkFetchResult::end_of_stream()
            })
        }

        async fn refetch_link(&self, _: i64, _: i64) -> Result<CloudFetchLink> {
            panic!("unexpired signed URL must not be refreshed")
        }

        async fn refetch_links_from(&self, _: i64, _: i64) -> Result<Vec<CloudFetchLink>> {
            panic!("unexpired signed URL must not be refreshed")
        }
    }

    let fetcher = Arc::new(SingleLink(CloudFetchLink {
        url,
        chunk_index: 0,
        row_offset: 0,
        row_count: 1,
        byte_count: 0,
        expiration: chrono::Utc::now() + chrono::Duration::hours(1),
        http_headers: HashMap::new(),
        next_chunk_index: None,
    }));
    let downloader = Arc::new(ChunkDownloader::new(
        Arc::new(client),
        CompressionCodec::None,
        0.1,
    ));
    let provider = StreamingCloudFetchProvider::new(
        CloudFetchConfig {
            retry_delay: Duration::from_millis(1),
            ..CloudFetchConfig::default()
        },
        fetcher,
        downloader,
        Some(1),
        tokio::runtime::Handle::current(),
    );
    provider.next_batch().await
}

async fn assert_certificate_failure(failure: CertificateFailure) {
    for request_type in [
        RequestType::CreateSession,
        RequestType::ExecuteStatement,
        RequestType::CloudFetchDownload,
    ] {
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "Retry test CA");
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);

        let name = match failure {
            CertificateFailure::HostnameMismatch | CertificateFailure::DnsHostnameMismatch => {
                "wrong-host.invalid"
            }
            _ => "127.0.0.1",
        };
        let mut server_params = CertificateParams::new(vec![name.to_string()]).unwrap();
        if matches!(failure, CertificateFailure::Expired) {
            server_params.not_before = rcgen::date_time_ymd(2000, 1, 1);
            server_params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        }
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
        if matches!(
            failure,
            CertificateFailure::HostnameMismatch
                | CertificateFailure::DnsHostnameMismatch
                | CertificateFailure::Expired
        ) {
            config.tls.trusted_certificate_pem = Some(ca_cert.pem().into_bytes());
        }
        let mut client = test_client(config, Duration::from_secs(10));
        let host = if matches!(failure, CertificateFailure::DnsHostnameMismatch) {
            // Preserve the DNS name in the URL and SNI while dialing loopback.
            client.client = reqwest::Client::builder()
                .no_proxy()
                .add_root_certificate(
                    reqwest::Certificate::from_pem(ca_cert.pem().as_bytes()).unwrap(),
                )
                .resolve("expected-host.invalid", address)
                .build()
                .unwrap();
            format!("https://expected-host.invalid:{}", address.port())
        } else {
            format!("https://{address}")
        };
        let result = if matches!(request_type, RequestType::CloudFetchDownload) {
            download_cloudfetch_chunk(client, format!("{host}/chunk.arrow"))
                .await
                .map(|_| ())
        } else {
            let request = client
                .inner()
                .post(format!("{host}/api/2.0/sql/sessions"))
                .build()
                .unwrap();
            client
                .execute_without_auth(request, request_type)
                .await
                .map(|_| ())
        };
        shutdown.send(()).unwrap();
        let (attempts, requests) = server.await.unwrap();
        let error = result.expect_err("invalid server certificate must be rejected");
        assert_eq!(attempts, 1, "certificate failure must not retry: {error}");
        assert_eq!(requests, 0, "HTTP request must not reach the TLS server");
        assert!(error.message.contains("after 1 attempts"), "{error}");
        assert!(
            !error.retryable,
            "permanent certificate error must not be retryable: {error}"
        );

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
            CertificateFailure::HostnameMismatch | CertificateFailure::DnsHostnameMismatch => &[
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
            CertificateFailure::Expired => &["expired"],
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
async fn trusted_chain_completes_with_revocation_off() {
    // Revocation off: a chain the client trusts completes on the default path, on every
    // backend, and no revocation check is involved.
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Off test CA");
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);
    let server_key = KeyPair::generate().unwrap();
    let server_cert = CertificateParams::new(vec!["127.0.0.1".to_string()])
        .unwrap()
        .signed_by(&server_key, &issuer)
        .unwrap();
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
    let server = tokio::spawn(async move {
        let socket = listener.accept().await.unwrap().0;
        let mut stream = acceptor.accept(socket).await.unwrap();
        let mut buffer = [0; 1024];
        let _ = stream.read(&mut buffer).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let _ = stream.shutdown().await;
    });

    let mut config = HttpClientConfig::default();
    config.tls.trusted_certificate_pem = Some(ca_cert.pem().into_bytes());
    let client = test_client(config, Duration::from_secs(10));
    let response = client
        .inner()
        .get(format!("https://{address}/"))
        .send()
        .await
        .expect("a trusted chain completes with revocation off");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    server.await.unwrap();
}

#[tokio::test]
async fn hostname_mismatch_fails_once_with_certificate_cause() {
    assert_certificate_failure(CertificateFailure::HostnameMismatch).await;
}

#[tokio::test]
async fn dns_hostname_mismatch_fails_once_with_certificate_cause() {
    assert_certificate_failure(CertificateFailure::DnsHostnameMismatch).await;
}

#[tokio::test]
async fn expired_certificate_fails_once_with_certificate_cause() {
    assert_certificate_failure(CertificateFailure::Expired).await;
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
        assert!(
            error.retryable,
            "handshake timeouts remain retryable: {error}"
        );
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
        error.retryable,
        "connection refusal remains retryable: {error}"
    );
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

#[tokio::test]
async fn dns_failure_preserves_retryable_flag_without_http_retry() {
    use reqwest::dns::{Name, Resolve, Resolving};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct DnsFailure {
        calls: AtomicUsize,
        message: &'static str,
    }

    impl Resolve for DnsFailure {
        fn resolve(&self, _: Name) -> Resolving {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let message = self.message;
            Box::pin(async move {
                Err(Box::new(std::io::Error::other(message))
                    as Box<dyn std::error::Error + Send + Sync>)
            })
        }
    }

    for message in ["nodename nor servname provided, or not known", "NXDOMAIN"] {
        for request_type in [RequestType::CreateSession, RequestType::ExecuteStatement] {
            let resolver = Arc::new(DnsFailure {
                calls: AtomicUsize::new(0),
                message,
            });
            let mut client = test_client(HttpClientConfig::default(), Duration::from_secs(10));
            client.client = reqwest::Client::builder()
                .no_proxy()
                .dns_resolver(resolver.clone())
                .build()
                .unwrap();
            let request = client
                .inner()
                .get("http://missing.invalid/")
                .build()
                .unwrap();
            let error = client
                .execute_without_auth(request, request_type)
                .await
                .unwrap_err();
            assert_eq!(resolver.calls.load(Ordering::SeqCst), 1, "{error}");
            assert!(error.retryable, "preserve the existing DNS flag: {error}");
            assert!(error.message.contains(message), "{error}");
        }
    }
}

#[tokio::test]
async fn cloudfetch_recovers_after_dns_failure() {
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use reqwest::dns::{Addrs, Name, Resolve, Resolving};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Debug)]
    struct FailOnceResolver {
        calls: AtomicUsize,
        message: &'static str,
        address: SocketAddr,
    }

    impl Resolve for FailOnceResolver {
        fn resolve(&self, _: Name) -> Resolving {
            let fail = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            let message = self.message;
            let address = self.address;
            Box::pin(async move {
                if fail {
                    Err(Box::new(std::io::Error::other(message))
                        as Box<dyn std::error::Error + Send + Sync>)
                } else {
                    Ok(Box::new(std::iter::once(address)) as Addrs)
                }
            })
        }
    }

    let expected =
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![42])) as ArrayRef)])
            .unwrap();
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &expected.schema()).unwrap();
        writer.write(&expected).unwrap();
        writer.finish().unwrap();
    }

    for message in ["nodename nor servname provided, or not known", "NXDOMAIN"] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/chunk.arrow"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ipc.clone()))
            .mount(&server)
            .await;
        let resolver = Arc::new(FailOnceResolver {
            calls: AtomicUsize::new(0),
            message,
            address: *server.address(),
        });
        let mut client = test_client(HttpClientConfig::default(), Duration::from_secs(10));
        client.client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(resolver.clone())
            .build()
            .unwrap();
        let url = format!(
            "http://storage.invalid:{}/chunk.arrow",
            server.address().port()
        );
        let batch = download_cloudfetch_chunk(client, url)
            .await
            .expect("CloudFetch must retry after a DNS failure")
            .expect("downloaded chunk must yield a batch");

        assert_eq!(batch, expected, "{message}");
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 2, "{message}");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}
