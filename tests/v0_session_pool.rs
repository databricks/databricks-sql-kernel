use databricks_sql_kernel::{AuthConfig, DefaultOpts, HttpConfig, Session, TlsConfig};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

#[derive(Clone)]
struct RecordedRequest {
    connection: usize,
    method: String,
    headers: Vec<String>,
    body: Value,
}

struct Server {
    host: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded_connections = connections.clone();
        let recorded_requests = requests.clone();
        let sessions = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let connection = recorded_connections.fetch_add(1, Ordering::SeqCst);
                let requests = recorded_requests.clone();
                let sessions = sessions.clone();
                tokio::spawn(async move {
                    let mut socket = BufReader::new(socket);
                    loop {
                        let mut start = String::new();
                        if socket.read_line(&mut start).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let mut headers = Vec::new();
                        let mut length = 0;
                        loop {
                            let mut header = String::new();
                            if socket.read_line(&mut header).await.unwrap_or(0) == 0 {
                                return;
                            }
                            if header == "\r\n" {
                                break;
                            }
                            let lower = header.to_lowercase();
                            if let Some(value) = lower.strip_prefix("content-length:") {
                                length = value.trim().parse().unwrap();
                            }
                            headers.push(header.trim().to_owned());
                        }
                        let mut body = vec![0; length];
                        if socket.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let parts: Vec<_> = start.split_whitespace().collect();
                        let method = parts[0];
                        let route = parts[1];
                        requests.lock().unwrap().push(RecordedRequest {
                            connection,
                            method: method.to_owned(),
                            headers,
                            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
                        });
                        let body = if route.contains("feature-flags") {
                            json!({"flags": [], "ttl_seconds": 900}).to_string()
                        } else if method == "POST" && route.ends_with("/sessions") {
                            json!({"session_id": format!("session-{}", sessions.fetch_add(1, Ordering::SeqCst)), "warehouse_id": "test"}).to_string()
                        } else {
                            String::new()
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                            body.len(), body
                        );
                        if socket
                            .get_mut()
                            .write_all(response.as_bytes())
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        Self {
            host,
            requests,
            task,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn open(
    server: &Server,
    token: &str,
    catalog: &str,
    http: HttpConfig,
    tls: TlsConfig,
) -> Session {
    let mut defaults = DefaultOpts::default();
    defaults.catalog = Some(catalog.to_owned());
    Session::builder()
        .http_path(server.host.clone(), "/sql/1.0/warehouses/test")
        .auth(AuthConfig::Pat {
            token: token.to_owned(),
            identity_federation_client_id: None,
        })
        .defaults(defaults)
        .http(http)
        .tls(tls)
        .open()
        .await
        .unwrap()
}

async fn sessions_reuse_returned_pools_not_active_pools_or_session_state() {
    let server = Server::start().await;
    let first = open(
        &server,
        "dapi_first",
        "first_catalog",
        HttpConfig::default(),
        TlsConfig::default(),
    )
    .await;
    let second = open(
        &server,
        "dapi_second",
        "second_catalog",
        HttpConfig::default(),
        TlsConfig::default(),
    )
    .await;
    assert_ne!(first.session_id(), second.session_id());
    first.close().await.unwrap();
    let third = open(
        &server,
        "dapi_third",
        "third_catalog",
        HttpConfig::default(),
        TlsConfig::default(),
    )
    .await;
    second.close().await.unwrap();
    third.close().await.unwrap();
    let requests = server.requests.lock().unwrap();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect();
    assert_eq!(creates.len(), 3);
    assert_ne!(creates[0].connection, creates[1].connection);
    assert_ne!(creates[1].connection, creates[2].connection);
    for (request, (token, catalog)) in creates.iter().zip([
        ("dapi_first", "first_catalog"),
        ("dapi_second", "second_catalog"),
        ("dapi_third", "third_catalog"),
    ]) {
        assert!(request
            .headers
            .iter()
            .any(|header| header.eq_ignore_ascii_case(&format!("authorization: Bearer {token}"))));
        assert_eq!(request.body["catalog"], catalog);
    }
}

async fn transport_settings_partition_pools_but_per_request_headers_do_not() {
    let server = Server::start().await;
    for variant in 0..5 {
        let mut http = HttpConfig::default();
        let mut tls = TlsConfig::default();
        match variant {
            1 => http
                .custom_headers
                .push(("x-tenant".to_owned(), "other".to_owned())),
            2 => http.request_timeout += Duration::from_secs(1),
            3 => tls.accept_self_signed = true,
            4 => http
                .custom_headers
                .push(("User-Agent".to_owned(), "other-agent".to_owned())),
            _ => {}
        }
        let session = open(&server, "dapi_token", "catalog", http, tls).await;
        session.close().await.unwrap();
    }
    let requests = server.requests.lock().unwrap();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect();
    assert_eq!(creates.len(), 5);
    for left in 0..creates.len() {
        for right in left + 1..creates.len() {
            if (left, right) != (0, 1) {
                assert_ne!(creates[left].connection, creates[right].connection);
            }
        }
    }
    for (index, request) in creates.iter().enumerate() {
        assert_eq!(
            request
                .headers
                .iter()
                .any(|header| header.eq_ignore_ascii_case("x-tenant: other")),
            index == 1
        );
    }
    assert!(creates[4]
        .headers
        .iter()
        .any(|header| header.eq_ignore_ascii_case("user-agent: other-agent")));
}

async fn concurrent_sessions_keep_their_authentication_and_catalogs() {
    let server = Server::start().await;
    let sessions = (0..10).map(|index| {
        let server = &server;
        async move {
            let token = format!("dapi_concurrent_{index}");
            let catalog = format!("catalog_{index}");
            open(
                server,
                &token,
                &catalog,
                HttpConfig::default(),
                TlsConfig::default(),
            )
            .await
        }
    });
    let sessions = futures::future::join_all(sessions).await;
    futures::future::join_all(
        sessions
            .into_iter()
            .map(|session| async move { session.close().await.unwrap() }),
    )
    .await;
    let requests = server.requests.lock().unwrap();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect();
    assert_eq!(creates.len(), 10);
    assert_eq!(
        creates
            .iter()
            .map(|request| request.connection)
            .collect::<HashSet<_>>()
            .len(),
        10
    );
    for index in 0..10 {
        let catalog = format!("catalog_{index}");
        let request = creates
            .iter()
            .find(|request| request.body["catalog"] == catalog)
            .unwrap();
        assert!(request.headers.iter().any(|header| header
            .eq_ignore_ascii_case(&format!("authorization: Bearer dapi_concurrent_{index}"))));
    }
}

async fn separate_runtimes_never_share_clients() {
    let server = Server::start().await;
    let host = server.host.clone();
    tokio::task::spawn_blocking(move || {
        let runtimes: Vec<_> = (0..3)
            .map(|_| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap()
            })
            .collect();
        for index in (0..3).rev().chain(0..3) {
            let runtime = &runtimes[index];
            runtime.block_on(async {
                let session = Session::builder()
                    .http_path(host.clone(), "/sql/1.0/warehouses/test")
                    .auth(AuthConfig::Pat {
                        token: "dapi_runtime".into(),
                        identity_federation_client_id: None,
                    })
                    .defaults({
                        let mut defaults = DefaultOpts::default();
                        defaults.catalog = Some(index.to_string());
                        defaults
                    })
                    .open()
                    .await
                    .unwrap();
                session.close().await.unwrap();
            });
        }
    })
    .await
    .unwrap();
    let requests = server.requests.lock().unwrap();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect();
    assert_eq!(creates.len(), 6);
    let groups: Vec<HashSet<_>> = (0..3)
        .map(|index| {
            let catalog = index.to_string();
            creates
                .iter()
                .filter(|request| request.body["catalog"] == catalog)
                .map(|request| request.connection)
                .collect()
        })
        .collect();
    for left in 0..groups.len() {
        assert!(!groups[left].is_empty());
        for right in left + 1..groups.len() {
            assert!(groups[left].is_disjoint(&groups[right]));
        }
    }
}

async fn changed_root_file_contents_do_not_reuse_old_trust() {
    let server = Server::start().await;
    let first = open(
        &server,
        "dapi_token",
        "catalog",
        HttpConfig::default(),
        TlsConfig::default(),
    )
    .await;
    first.close().await.unwrap();
    let replacement =
        rcgen::generate_simple_self_signed(vec!["replacement.invalid".into()]).unwrap();
    std::fs::write(
        std::env::var_os("SSL_CERT_FILE").unwrap(),
        replacement.cert.pem(),
    )
    .unwrap();
    let second = open(
        &server,
        "dapi_token",
        "catalog",
        HttpConfig::default(),
        TlsConfig::default(),
    )
    .await;
    second.close().await.unwrap();
    let requests = server.requests.lock().unwrap();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST")
        .collect();
    assert_eq!(creates.len(), 2);
    assert_ne!(creates[0].connection, creates[1].connection);
}

#[test]
fn session_pool_end_to_end() {
    if std::env::var_os("KERNEL_POOL_TEST_CHILD").is_none() {
        let roots = tempfile::NamedTempFile::new().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["test.invalid".into()]).unwrap();
        std::fs::write(roots.path(), certificate.cert.pem()).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "session_pool_end_to_end", "--nocapture"])
            .env("KERNEL_POOL_TEST_CHILD", "1")
            .env("SSL_CERT_FILE", roots.path())
            .env_remove("SSL_CERT_DIR")
            .status()
            .unwrap();
        assert!(status.success(), "isolated session pool assertions failed");
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(20), async {
                sessions_reuse_returned_pools_not_active_pools_or_session_state().await;
                concurrent_sessions_keep_their_authentication_and_catalogs().await;
                transport_settings_partition_pools_but_per_request_headers_do_not().await;
                separate_runtimes_never_share_clients().await;
                changed_root_file_contents_do_not_reuse_old_trust().await;
            })
            .await
            .unwrap();
        });
}
