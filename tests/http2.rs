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

use bytes::Bytes;
use databricks_sql_kernel::auth::PersonalAccessToken;
use databricks_sql_kernel::client::{
    build_retry_configs, DatabricksHttpClient, HttpClientConfig, RequestType, RetryConfig,
};
use databricks_sql_kernel::{
    AuthConfig, ConnectionConfig, HttpConfig, ProxyConfig, Session, TelemetryConfig, TlsConfig,
};
use http::Version;
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

struct HttpsServer {
    url: String,
    certificate: Vec<u8>,
    requests: mpsc::UnboundedReceiver<(String, Version)>,
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
                                    versions.send((request.uri().path().to_string(), request.version())).unwrap();
                                    let redirect = request.uri().path() == "/redirect";
                                    let response_head = http::Response::builder()
                                        .status(if redirect { 302 } else { 200 })
                                        .header("location", "https://example.invalid/authorize")
                                        .body(())
                                        .unwrap();
                                    response.send_response(response_head, false).unwrap()
                                        .send_data(Bytes::from_static(b"{\"session_id\":\"session-http2\"}"), true)
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
                                let path = std::str::from_utf8(&request).unwrap().split_whitespace().nth(1).unwrap().to_string();
                                versions.send((path, Version::HTTP_11)).unwrap();
                                let redirect = request.starts_with(b"GET /redirect ");
                                let body = r#"{"session_id":"session-http2"}"#;
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
        let (_, version) = tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(version, expected);
    }

    fn client(&self, enabled: bool, proxy: Option<String>) -> DatabricksHttpClient {
        let mut config = HttpClientConfig {
            http2_enabled: enabled,
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
        let client =
            DatabricksHttpClient::new(config, build_retry_configs(&retries, &Default::default()))
                .unwrap();
        client.set_auth_provider(Arc::new(PersonalAccessToken::new("test-token")));
        client
    }
}

fn run_without_proxy_environment(test_name: &str) -> bool {
    const CHILD: &str = "DATABRICKS_HTTP2_DIRECT_TEST";
    if std::env::var(CHILD).as_deref() == Ok(test_name) {
        return false;
    }
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD, test_name);
    for key in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        command.env_remove(key);
    }
    let output = command.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "{test_name}: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

#[tokio::test]
async fn https_negotiates_http2_and_supports_http1_fallback_and_opt_out() {
    if run_without_proxy_environment(
        "https_negotiates_http2_and_supports_http1_fallback_and_opt_out",
    ) {
        return;
    }
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
        let response = client.inner().get(&server.url).send().await.unwrap();
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
async fn https_uses_http1_through_an_http_proxy() {
    assert_connect_proxy(false).await;
}

#[tokio::test]
async fn https_uses_http1_through_an_https_proxy() {
    assert_connect_proxy(true).await;
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

async fn assert_connect_proxy(proxy_tls: bool) {
    for (server_http2, enabled) in [(true, true), (false, true), (true, false)] {
        let mut server = HttpsServer::start(server_http2).await;
        let authority = server.url.strip_prefix("https://").unwrap().to_string();
        let target = authority.clone();
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!(
            "{}://{}",
            if proxy_tls { "https" } else { "http" },
            proxy.local_addr().unwrap()
        );
        let (sent_head, received_head) = tokio::sync::oneshot::channel();
        let acceptor = server.acceptor.clone();
        let proxy_task = tokio::spawn(async move {
            let downstream = proxy.accept().await.unwrap().0;
            if proxy_tls {
                let downstream = acceptor.accept(downstream).await.unwrap();
                assert_ne!(
                    downstream.get_ref().1.alpn_protocol(),
                    Some(b"h2".as_slice())
                );
                tunnel(downstream, target, sent_head).await;
            } else {
                tunnel(downstream, target, sent_head).await;
            }
        });
        let client = server.client(enabled, Some(proxy_url));
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
}

#[test]
fn environment_proxy_configuration_uses_http1_including_bypassed_hosts() {
    const CHILD: &str = "DATABRICKS_HTTP2_ENV_TEST_PROTOCOL";
    if let Ok(protocol) = std::env::var(CHILD) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let expected = if protocol == "h2" {
                    Version::HTTP_2
                } else {
                    Version::HTTP_11
                };
                let mut server = HttpsServer::start(true).await;
                let client = server.client(true, None);
                let response = client.inner().get(&server.url).send().await.unwrap();
                assert_eq!(response.version(), expected);
                response.bytes().await.unwrap();
                server.assert_version(expected).await;
                client
                    .first_redirect_location(&format!("{}/redirect", server.url))
                    .await
                    .unwrap();
                server.assert_version(expected).await;
            });
        return;
    }

    let keys = [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ];
    for selected in keys.into_iter().map(Some).chain([None]) {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "environment_proxy_configuration_uses_http1_including_bypassed_hosts",
            "--nocapture",
        ]);
        for key in keys {
            command.env(key, "");
        }
        command.env("NO_PROXY", "127.0.0.1").env_remove("no_proxy");
        command.env(CHILD, if selected.is_some() { "http1" } else { "h2" });
        if let Some(key) = selected {
            command.env(key, "http://127.0.0.1:9");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "proxy {selected:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn session_http2_default_and_opt_out_reach_the_https_connection() {
    if run_without_proxy_environment("session_http2_default_and_opt_out_reach_the_https_connection")
    {
        return;
    }
    for enabled in [true, false] {
        let mut server = HttpsServer::start(true).await;
        let mut http = HttpConfig::default();
        assert!(http.http2_enabled);
        http.http2_enabled = enabled;
        http.overall_timeout = Duration::from_secs(5);
        http.retry_max_retries = 0;
        let mut tls = TlsConfig::default();
        tls.custom_ca_cert = Some(server.certificate.clone());
        let session = Session::builder()
            .connection(ConnectionConfig::new(&server.url, "test-warehouse"))
            .auth(AuthConfig::Pat {
                token: "test-token".into(),
                identity_federation_client_id: None,
            })
            .http(http)
            .tls(tls)
            .open()
            .await
            .unwrap();
        let expected = if enabled {
            Version::HTTP_2
        } else {
            Version::HTTP_11
        };
        server.assert_version(expected).await;
        session.close().await.unwrap();
        server.assert_version(expected).await;
    }
}

#[tokio::test]
async fn telemetry_respects_each_sessions_http_protocol_setting_on_the_same_host() {
    if run_without_proxy_environment(
        "telemetry_respects_each_sessions_http_protocol_setting_on_the_same_host",
    ) {
        return;
    }
    for configurations in [
        [(true, false), (false, false)],
        [(true, false), (true, true)],
        [(true, true), (true, false)],
    ] {
        let mut server = HttpsServer::start(true).await;
        let mut sessions = Vec::new();
        for (enabled, proxied) in configurations {
            let mut http = HttpConfig::default();
            http.http2_enabled = enabled;
            http.retry_max_retries = 0;
            http.overall_timeout = Duration::from_secs(5);
            let mut tls = TlsConfig::default();
            tls.custom_ca_cert = Some(server.certificate.clone());
            let mut proxy = ProxyConfig::default();
            if proxied {
                proxy.url = Some("http://127.0.0.1:9".into());
                proxy.bypass_hosts = Some("127.0.0.1".into());
            }
            let mut telemetry = TelemetryConfig::default();
            telemetry.enabled = Some(true);
            telemetry.flush_interval = Duration::from_secs(3600);
            sessions.push(
                Session::builder()
                    .connection(ConnectionConfig::new(&server.url, "test-warehouse"))
                    .auth(AuthConfig::Pat {
                        token: "test-token".into(),
                        identity_federation_client_id: None,
                    })
                    .http(http)
                    .proxy(proxy)
                    .tls(tls)
                    .telemetry(telemetry)
                    .open()
                    .await
                    .unwrap(),
            );
        }
        for session in sessions.into_iter().rev() {
            session.close().await.unwrap();
        }
        let mut telemetry_versions = Vec::new();
        while let Ok((path, version)) = server.requests.try_recv() {
            if path.starts_with("/telemetry") {
                telemetry_versions.push(version);
            }
        }
        assert!(
            telemetry_versions.contains(&Version::HTTP_11),
            "{telemetry_versions:?}"
        );
        assert!(
            telemetry_versions.contains(&Version::HTTP_2),
            "{telemetry_versions:?}"
        );
    }
}

#[test]
fn c_abi_http2_default_and_opt_out_reach_the_https_connection() {
    if run_without_proxy_environment("c_abi_http2_default_and_opt_out_reach_the_https_connection") {
        return;
    }
    use databricks_sql_kernel::c_abi::session::*;
    use databricks_sql_kernel::c_abi::KernelStatusCode;
    use std::ffi::CString;
    use std::ptr;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    for enabled in [None, Some(true), Some(false)] {
        let mut server = runtime.block_on(HttpsServer::start(true));
        let url = CString::new(server.url.clone()).unwrap();
        let certificate = server.certificate.clone();
        {
            // SAFETY: config and session are initialized by the C ABI; input buffers
            // remain alive through the calls that copy them.
            unsafe {
                let mut config = ptr::null_mut();
                assert_eq!(
                    kernel_session_config_new(&mut config),
                    KernelStatusCode::Success
                );
                assert_eq!(
                    kernel_session_config_set_http_path(
                        config,
                        url.as_ptr(),
                        c"/sql/1.0/warehouses/test".as_ptr()
                    ),
                    KernelStatusCode::Success
                );
                assert_eq!(
                    kernel_session_config_set_auth_pat(config, c"test-token".as_ptr()),
                    KernelStatusCode::Success
                );
                assert_eq!(
                    kernel_session_config_set_tls_trusted_certs(
                        config,
                        certificate.as_ptr(),
                        certificate.len()
                    ),
                    KernelStatusCode::Success
                );
                if let Some(enabled) = enabled {
                    assert_eq!(
                        kernel_session_config_set_http2_enabled(config, enabled),
                        KernelStatusCode::Success
                    );
                }
                let mut session = ptr::null_mut();
                assert_eq!(
                    kernel_session_open(config, &mut session),
                    KernelStatusCode::Success
                );
                assert_eq!(kernel_session_close(session), KernelStatusCode::Success);
            }
        }
        let expected = if enabled == Some(false) {
            Version::HTTP_11
        } else {
            Version::HTTP_2
        };
        runtime.block_on(async {
            server.assert_version(expected).await;
            server.assert_version(expected).await;
        });
    }
}
