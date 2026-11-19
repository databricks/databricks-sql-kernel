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

use crate::auth::PersonalAccessToken;
use crate::client::{
    build_retry_configs, DatabricksHttpClient, HttpClientConfig, RequestType, RetryConfig,
};
use crate::config::{AuthConfig, HttpConfig, TelemetryConfig, TlsConfig};
use crate::Session;
use bytes::Bytes;
use http::Version;
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

use super::{resolve_http2, ProxyConfig};

struct HttpsServer {
    url: String,
    certificate: Vec<u8>,
    requests: mpsc::UnboundedReceiver<Version>,
    acceptor: TlsAcceptor,
    task: JoinHandle<()>,
}

impl Drop for HttpsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HttpsServer {
    async fn start(http2: bool) -> Self {
        let identity = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let certificate = identity.cert.pem().into_bytes();
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![identity.cert.der().clone()],
            PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into(),
        )
        .unwrap();
        if http2 {
            tls.alpn_protocols.push(b"h2".to_vec());
        }
        tls.alpn_protocols.push(b"http/1.1".to_vec());
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", listener.local_addr().unwrap());
        let (versions, received_versions) = mpsc::unbounded_channel();
        let proxy_acceptor = acceptor.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let socket = accepted.unwrap().0;
                        let acceptor = acceptor.clone();
                        let versions = versions.clone();
                        connections.spawn(async move {
                            let mut stream = acceptor.accept(socket).await.unwrap();
                            if stream.get_ref().1.alpn_protocol() == Some(b"h2") {
                                let mut connection = h2::server::handshake(stream).await.unwrap();
                                while let Some(request) = connection.accept().await {
                                    let (request, mut response) = request.unwrap();
                                    versions.send(request.version()).unwrap();
                                    let redirect = request.uri().path() == "/redirect";
                                    let response_head = http::Response::builder()
                                        .status(if redirect { 302 } else { 200 })
                                        .header("location", "https://example.invalid/authorize")
                                        .body(())
                                        .unwrap();
                                    response.send_response(response_head, false).unwrap()
                                        .send_data(Bytes::from_static(br#"{"session_id":"http2-test"}"#), true)
                                        .unwrap();
                                }
                            } else {
                                let mut request = Vec::new();
                                let mut byte = [0];
                                while !request.ends_with(b"\r\n\r\n") {
                                    if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                                        return;
                                    }
                                    request.push(byte[0]);
                                }
                                versions.send(Version::HTTP_11).unwrap();
                                let redirect = request.starts_with(b"GET /redirect ");
                                let body = r#"{"session_id":"http2-test"}"#;
                                let response = format!(
                                    "HTTP/1.1 {}\r\nLocation: https://example.invalid/authorize\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    if redirect { "302 Found" } else { "200 OK" }, body.len(), body
                                );
                                stream.write_all(response.as_bytes()).await.unwrap();
                                let _ = stream.shutdown().await;
                            }
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap();
                    }
                }
            }
        });
        Self {
            url,
            certificate,
            requests: received_versions,
            acceptor: proxy_acceptor,
            task,
        }
    }

    async fn assert_version(&mut self, expected: Version) {
        let version = tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(version, expected);
    }

    fn client(&self, enabled: bool, proxy: Option<String>) -> DatabricksHttpClient {
        let mut config = HttpClientConfig {
            read_timeout: Duration::from_secs(5),
            ..HttpClientConfig::default()
        };
        config.tls.trusted_certificate_pem = Some(self.certificate.clone());
        config.proxy.url = proxy;
        let retries = RetryConfig {
            max_retries: 0,
            overall_timeout: Duration::from_secs(5),
            ..RetryConfig::default()
        };
        let client = DatabricksHttpClient::new_with_http2(
            config,
            build_retry_configs(&retries, &Default::default()),
            enabled,
        )
        .unwrap();
        client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        client
    }
}

#[tokio::test]
async fn https_negotiates_http2_with_http1_fallback_and_opt_out() {
    assert!(
        resolve_http2(true, &ProxyConfig::default(), |key| std::env::var_os(key)),
        "unset HTTP(S)_PROXY/ALL_PROXY and lowercase equivalents to test direct HTTP/2"
    );
    for (server_http2, enabled, expected) in [
        (true, true, Version::HTTP_2),
        (false, true, Version::HTTP_11),
        (true, false, Version::HTTP_11),
    ] {
        let mut server = HttpsServer::start(server_http2).await;
        let client = server.client(enabled, None);
        let request = client.inner().get(&server.url).build().unwrap();
        let response = client
            .execute(request, RequestType::CreateSession)
            .await
            .unwrap();
        assert_eq!(response.version(), expected);
        response.bytes().await.unwrap();
        server.assert_version(expected).await;
        assert_eq!(
            client
                .first_redirect_location(&format!("{}/redirect", server.url))
                .await
                .unwrap(),
            Some("https://example.invalid/authorize".into())
        );
        server.assert_version(expected).await;
    }
}

#[tokio::test]
async fn session_http2_opt_out_reaches_the_first_request() {
    assert!(
        resolve_http2(true, &ProxyConfig::default(), |key| std::env::var_os(key)),
        "unset HTTP(S)_PROXY/ALL_PROXY and lowercase equivalents to test direct HTTP/2"
    );
    let mut server = HttpsServer::start(true).await;
    let session = Session::builder()
        .http_path(&server.url, "/sql/1.0/warehouses/test")
        .auth(AuthConfig::Pat {
            token: "test-token".into(),
            identity_federation_client_id: None,
        })
        .http(HttpConfig {
            http2_enabled: false,
            request_timeout: Duration::from_secs(5),
            overall_timeout: Duration::from_secs(5),
            retry_max_retries: 0,
            ..HttpConfig::default()
        })
        .tls(TlsConfig {
            custom_ca_cert: Some(server.certificate.clone()),
            ..TlsConfig::default()
        })
        .telemetry(TelemetryConfig {
            enabled: Some(false),
            ..TelemetryConfig::default()
        })
        .open()
        .await
        .unwrap();
    server.assert_version(Version::HTTP_11).await;
    session.close().await.unwrap();
}

async fn tunnel<S: AsyncRead + AsyncWrite + Unpin>(
    mut downstream: S,
    target: String,
    sent_head: tokio::sync::oneshot::Sender<String>,
) {
    let mut head = Vec::new();
    let mut byte = [0];
    while !head.ends_with(b"\r\n\r\n") {
        downstream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
    }
    let mut upstream = tokio::net::TcpStream::connect(target).await.unwrap();
    downstream
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
        .unwrap();
    sent_head.send(String::from_utf8(head).unwrap()).unwrap();
    let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
}

#[tokio::test]
async fn https_uses_http1_through_an_https_proxy() {
    let mut server = HttpsServer::start(true).await;
    let authority = server.url.strip_prefix("https://").unwrap().to_string();
    let target = authority.clone();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("https://{}", proxy.local_addr().unwrap());
    let (sent_head, received_head) = tokio::sync::oneshot::channel();
    let acceptor = server.acceptor.clone();
    let proxy_task = tokio::spawn(async move {
        let downstream = proxy.accept().await.unwrap().0;
        let downstream = acceptor.accept(downstream).await.unwrap();
        assert_ne!(
            downstream.get_ref().1.alpn_protocol(),
            Some(b"h2".as_slice())
        );
        tunnel(downstream, target, sent_head).await;
    });
    let client = server.client(true, Some(proxy_url));
    let response = client.inner().get(&server.url).send().await.unwrap();
    assert_eq!(response.version(), Version::HTTP_11);
    response.bytes().await.unwrap();
    server.assert_version(Version::HTTP_11).await;
    let head = tokio::time::timeout(Duration::from_secs(5), received_head)
        .await
        .expect("proxy did not receive CONNECT")
        .unwrap();
    assert!(
        head.starts_with(&format!("CONNECT {authority} HTTP/1.1\r\n")),
        "{head}"
    );
    proxy_task.abort();
}

#[test]
fn resolves_http2_from_request_and_proxy_configuration() {
    for (requested, url, bypass, expected) in [
        (true, None, None, true),
        (false, None, None, false),
        (true, Some("https://proxy:8080"), None, false),
        (true, Some("http://proxy:8080"), Some("*"), false),
    ] {
        let proxy = ProxyConfig {
            url: url.map(Into::into),
            bypass_hosts: bypass.map(Into::into),
            ..ProxyConfig::default()
        };
        assert_eq!(resolve_http2(requested, &proxy, |_| None), expected);
    }
    let proxy = ProxyConfig::default();
    for key in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        for (value, expected) in [
            (None, true),
            (Some(""), true),
            (Some("http://proxy:8080"), false),
        ] {
            assert_eq!(
                resolve_http2(true, &proxy, |name| if name == key {
                    value.map(Into::into)
                } else {
                    None
                }),
                expected,
                "{key}={value:?}"
            );
        }
    }
    assert!(resolve_http2(true, &proxy, |name| (name == "NO_PROXY")
        .then(|| "*".into())));
}
