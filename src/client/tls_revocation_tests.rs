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

//! Windows revocation tests. Each fixture generates a root, an intermediate and a leaf with
//! rcgen, serves the CRLs over HTTP on loopback, and serves the leaf over TLS on loopback.
//!
//! The roots are not in the Windows store, so each chain also exercises the custom-CA path.
//! Every fixture has its own CRL URL (its own port and path), so CryptoAPI's cache cannot
//! carry a CRL from one test to another.
//!
//! These tests run only on Windows, in the kernel's Windows CI.

use super::*;
use crate::client::http::{DatabricksHttpClient, HttpClientConfig};
use crate::client::retry::{build_retry_configs, RetryConfig};
use rcgen::{
    date_time_ymd, BasicConstraints, CertificateParams, CertificateRevocationListParams,
    CrlDistributionPoint, DnType, IsCa, Issuer, KeyIdMethod, KeyPair, RevokedCertParams,
    SerialNumber, SigningKey,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::HandshakeKind;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

/// Lower-cased fragment of rustls's `UnknownRevocationStatus` error text.
const UNDETERMINED: &str = "unknownrevocationstatus";
const ROOT_CN: &str = "Revocation Test Root";
const INTERMEDIATE_CN: &str = "Revocation Test Intermediate";
const LEAF_SERIAL: u64 = 1000;
const INTERMEDIATE_SERIAL: u64 = 2000;
const CLIENT_SERIAL: u64 = 3000;
/// (year, month, day) for the CRL's validity bounds. Valid until 2099.
const CURRENT: (i32, u8, u8) = (2099, 1, 1);

/// What the CRL at one distribution point serves.
#[derive(Clone, Copy)]
enum Served {
    /// A current CRL that lists nothing.
    Good,
    /// A current CRL that lists the covered certificate as revoked.
    Revoked,
    /// The distribution point is a closed port, so the fetch fails.
    Refused,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Pass,
    Revoked,
    Undetermined,
}

/// One table row. Each row is checked in strict and accept-undetermined modes.
struct Scenario {
    name: &'static str,
    /// `None` means the leaf has no CRL distribution point.
    leaf: Option<Served>,
    intermediate: Served,
    strict: Outcome,
    accept: Outcome,
}

// One row per line, so the table stays readable. rustfmt would split each row.
#[rustfmt::skip]
const SCENARIOS: &[Scenario] = &[
    Scenario { name: "good chain", leaf: Some(Served::Good), intermediate: Served::Good, strict: Outcome::Pass, accept: Outcome::Pass },
    Scenario { name: "revoked leaf", leaf: Some(Served::Revoked), intermediate: Served::Good, strict: Outcome::Revoked, accept: Outcome::Revoked },
    Scenario { name: "revoked intermediate", leaf: Some(Served::Good), intermediate: Served::Revoked, strict: Outcome::Revoked, accept: Outcome::Revoked },
    Scenario { name: "unreachable leaf CRL", leaf: Some(Served::Refused), intermediate: Served::Good, strict: Outcome::Undetermined, accept: Outcome::Pass },
    Scenario { name: "unreachable intermediate CRL", leaf: Some(Served::Good), intermediate: Served::Refused, strict: Outcome::Undetermined, accept: Outcome::Pass },
    Scenario { name: "leaf without a distribution point", leaf: None, intermediate: Served::Good, strict: Outcome::Undetermined, accept: Outcome::Pass },
];

struct Pki {
    root_der: CertificateDer<'static>,
    root_pem: String,
    root_issuer: Issuer<'static, KeyPair>,
    intermediate_issuer: Issuer<'static, KeyPair>,
    intermediate_der: CertificateDer<'static>,
    leaf_der: CertificateDer<'static>,
    leaf_key_der: Vec<u8>,
    client_cert_pem: String,
    client_key_pem: String,
}

fn ca_params(common_name: &str, serial: u64) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.serial_number = Some(SerialNumber::from(serial));
    params
}

/// The CRL distribution points of the leaf and the intermediate.
fn build_pki(leaf_cdp: Option<Vec<String>>, intermediate_cdp: String, san: &str) -> Pki {
    let root_key = KeyPair::generate().unwrap();
    let root_params = ca_params(ROOT_CN, 1);
    let root_cert = root_params.self_signed(&root_key).unwrap();
    let root_pem = root_cert.pem();
    let root_der = root_cert.der().clone();
    let root_issuer = Issuer::new(root_params, root_key);

    let intermediate_key = KeyPair::generate().unwrap();
    let mut intermediate_params = ca_params(INTERMEDIATE_CN, INTERMEDIATE_SERIAL);
    intermediate_params.crl_distribution_points = vec![CrlDistributionPoint {
        uris: vec![intermediate_cdp],
    }];
    let intermediate_cert = intermediate_params
        .signed_by(&intermediate_key, &root_issuer)
        .unwrap();
    let intermediate_der = intermediate_cert.der().clone();
    let intermediate_issuer = Issuer::new(intermediate_params, intermediate_key);

    let leaf_key = KeyPair::generate().unwrap();
    let mut leaf_params = CertificateParams::new(vec![san.to_owned()]).unwrap();
    leaf_params.serial_number = Some(SerialNumber::from(LEAF_SERIAL));
    if let Some(uris) = leaf_cdp {
        leaf_params.crl_distribution_points = vec![CrlDistributionPoint { uris }];
    }
    let leaf_der = leaf_params
        .signed_by(&leaf_key, &intermediate_issuer)
        .unwrap()
        .der()
        .clone();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params =
        CertificateParams::new(vec!["revocation-test-client".to_owned()]).unwrap();
    client_params.serial_number = Some(SerialNumber::from(CLIENT_SERIAL));
    let client_cert = client_params
        .signed_by(&client_key, &intermediate_issuer)
        .unwrap();

    Pki {
        root_der,
        root_pem,
        root_issuer,
        intermediate_issuer,
        intermediate_der,
        leaf_der,
        leaf_key_der: leaf_key.serialize_der(),
        // The mTLS identity presents the client leaf, then its intermediate.
        client_cert_pem: format!("{}{}", client_cert.pem(), intermediate_cert.pem()),
        client_key_pem: client_key.serialize_pem(),
    }
}

/// A CRL listing `revoked` serials, valid until `CURRENT`.
fn sign_crl<K: SigningKey>(issuer: &Issuer<'_, K>, revoked: &[u64]) -> Vec<u8> {
    CertificateRevocationListParams {
        this_update: date_time_ymd(2020, 1, 1),
        next_update: date_time_ymd(CURRENT.0, CURRENT.1, CURRENT.2),
        crl_number: SerialNumber::from(1_u64),
        issuing_distribution_point: None,
        revoked_certs: revoked
            .iter()
            .map(|serial| RevokedCertParams {
                serial_number: SerialNumber::from(*serial),
                revocation_time: date_time_ymd(2020, 1, 2),
                reason_code: None,
                invalidity_date: None,
            })
            .collect(),
        key_identifier_method: KeyIdMethod::Sha256,
    }
    .signed_by(issuer)
    .unwrap()
    .der()
    .to_vec()
}

/// The route for one CRL. `issuer` signs it, and `covered` is the serial it may revoke.
fn route(served: Served, issuer: &Issuer<'_, KeyPair>, covered: u64) -> Route {
    let body = match served {
        Served::Good => sign_crl(issuer, &[]),
        Served::Revoked => sign_crl(issuer, &[covered]),
        Served::Refused => unreachable!("a refused distribution point has no route"),
    };
    Route { status: 200, body }
}

/// A PKI, its CRL origin server, and the TLS leaf server.
struct Fixture {
    crl: CrlServer,
    pki: Pki,
    addr: SocketAddr,
}

/// Each fixture's CRL paths are unique, so no two tests share a CRL URL.
static FIXTURE_ID: AtomicUsize = AtomicUsize::new(0);

fn fixture(leaf: Option<Served>, intermediate: Served, san: &str) -> Fixture {
    let crl = CrlServer::start();
    let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let leaf_path = format!("/{id}/leaf.crl");
    let intermediate_path = format!("/{id}/intermediate.crl");
    let dead = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let url = |served: Served, path: &str| match served {
        Served::Refused => format!("http://{dead}{path}"),
        _ => crl.url(path),
    };
    let leaf_cdp = leaf.map(|served| vec![url(served, &leaf_path)]);
    let pki = build_pki(leaf_cdp, url(intermediate, &intermediate_path), san);
    if let Some(served @ (Served::Good | Served::Revoked)) = leaf {
        crl.set(
            &leaf_path,
            route(served, &pki.intermediate_issuer, LEAF_SERIAL),
        );
    }
    if let served @ (Served::Good | Served::Revoked) = intermediate {
        crl.set(
            &intermediate_path,
            route(served, &pki.root_issuer, INTERMEDIATE_SERIAL),
        );
    }
    let addr = spawn_tls_server(&pki, false);
    Fixture { crl, pki, addr }
}

/// A TLS server presenting the leaf chain. With `mtls`, it requires a client certificate
/// that chains to the test root.
fn spawn_tls_server(pki: &Pki, mtls: bool) -> SocketAddr {
    let chain = vec![pki.leaf_der.clone(), pki.intermediate_der.clone()];
    let mtls_root = mtls.then(|| pki.root_der.clone());
    spawn_tls_chain(chain, pki.leaf_key_der.clone(), mtls_root)
}

/// A TLS server presenting `chain`, with the leaf key `key_der`. With `mtls_root`, it
/// requires a client certificate that chains to that root.
fn spawn_tls_chain(
    chain: Vec<CertificateDer<'static>>,
    key_der: Vec<u8>,
    mtls_root: Option<CertificateDer<'static>>,
) -> SocketAddr {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap();
    let key = PrivatePkcs8KeyDer::from(key_der).into();
    let config = if let Some(root) = mtls_root {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(root).unwrap();
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .unwrap()
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap()
    };
    let config = Arc::new(config);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for tcp in listener.incoming().flatten() {
            let config = Arc::clone(&config);
            thread::spawn(move || {
                let Ok(conn) = rustls::ServerConnection::new(config) else {
                    return;
                };
                let mut stream = rustls::StreamOwned::new(conn, tcp);
                if read_head(&mut stream).is_some() {
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    );
                    let _ = stream.flush();
                }
            });
        }
    });
    addr
}

/// A TLS server that keeps session state, and records the kind of each handshake it sees.
fn spawn_resuming_server(pki: &Pki) -> (SocketAddr, Arc<Mutex<Vec<HandshakeKind>>>) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![pki.leaf_der.clone(), pki.intermediate_der.clone()],
            PrivatePkcs8KeyDer::from(pki.leaf_key_der.clone()).into(),
        )
        .unwrap();
    config.session_storage = rustls::server::ServerSessionMemoryCache::new(32);
    config.ticketer = rustls::crypto::ring::Ticketer::new().unwrap();
    let config = Arc::new(config);
    let kinds = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let recorded = Arc::clone(&kinds);
    thread::spawn(move || {
        for tcp in listener.incoming().flatten() {
            let config = Arc::clone(&config);
            let recorded = Arc::clone(&recorded);
            thread::spawn(move || {
                let Ok(conn) = rustls::ServerConnection::new(config) else {
                    return;
                };
                let mut stream = rustls::StreamOwned::new(conn, tcp);
                if read_head(&mut stream).is_some() {
                    if let Some(kind) = stream.conn.handshake_kind() {
                        recorded.lock().unwrap().push(kind);
                    }
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    );
                    let _ = stream.flush();
                }
            });
        }
    });
    (addr, kinds)
}

/// Reads one request or response head, up to the blank line.
fn read_head(stream: &mut impl Read) -> Option<String> {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") && head.len() < 8192 {
        match stream.read(&mut byte) {
            Ok(1) => head.push(byte[0]),
            _ => return None,
        }
    }
    Some(String::from_utf8_lossy(&head).into_owned())
}

struct Route {
    status: u16,
    body: Vec<u8>,
}

/// Loopback HTTP server for CRLs. Its route table is filled after the PKI, which holds the
/// URLs, exists. It records the paths it receives.
#[derive(Clone)]
struct CrlServer {
    addr: SocketAddr,
    routes: Arc<Mutex<HashMap<String, Route>>>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl CrlServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server = Self {
            addr: listener.local_addr().unwrap(),
            routes: Arc::new(Mutex::new(HashMap::new())),
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let accept_side = server.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let handler = accept_side.clone();
                thread::spawn(move || handler.handle(stream));
            }
        });
        server
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn set(&self, path: &str, route: Route) {
        self.routes.lock().unwrap().insert(path.to_owned(), route);
    }

    /// The number of requests received so far, for any path.
    fn hits(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    fn handle(&self, mut stream: TcpStream) {
        let Some(head) = read_head(&mut stream) else {
            return;
        };
        let path = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_owned();
        self.requests.lock().unwrap().push(path.clone());
        let (status, body) = match self.routes.lock().unwrap().get(&path) {
            Some(route) => (route.status, route.body.clone()),
            None => (404, Vec::new()),
        };
        let mut response = format!(
            "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(&body);
        let _ = stream.write_all(&response);
    }
}

/// Config that trusts the test root. The revocation mode is passed separately.
fn revocation_config(root_pem: &str) -> HttpClientConfig {
    let mut config = HttpClientConfig::default();
    config.tls.trusted_certificate_pem = Some(root_pem.as_bytes().to_vec());
    // No pooling: each request runs a fresh handshake and revocation check.
    config.max_connections_per_host = 0;
    config
}

/// A client built from `config` and `revocation`, with default retry configs.
fn client(config: HttpClientConfig, revocation: Option<bool>) -> DatabricksHttpClient {
    DatabricksHttpClient::with_revocation(
        config,
        build_retry_configs(&RetryConfig::default(), &HashMap::new()),
        revocation,
    )
    .unwrap()
}

/// One GET to `addr`. `revocation` is `None` (off), `Some(false)` (strict) or
/// `Some(true)` (accept undetermined).
async fn get(
    config: HttpClientConfig,
    revocation: Option<bool>,
    addr: SocketAddr,
) -> std::result::Result<(), String> {
    let client = client(config, revocation);
    client
        .inner()
        .get(format!("https://{addr}/"))
        .send()
        .await
        .map(|_| ())
        .map_err(|error| error_text(&error))
}

fn error_text(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(inner) = source {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        source = inner.source();
    }
    text.to_ascii_lowercase()
}

/// Runs one table row in one mode and checks the outcome.
async fn check(scenario: &Scenario, accept: bool, want: Outcome) {
    let fx = fixture(scenario.leaf, scenario.intermediate, "127.0.0.1");
    let label = format!("{} (accept_undetermined={accept})", scenario.name);
    let result = get(revocation_config(&fx.pki.root_pem), Some(accept), fx.addr).await;
    match (want, &result) {
        (Outcome::Pass, Ok(())) => {}
        (Outcome::Revoked, Err(err)) => assert!(err.contains("revoked"), "{label}: {err}"),
        (Outcome::Undetermined, Err(err)) => assert!(err.contains(UNDETERMINED), "{label}: {err}"),
        _ => panic!("{label}: expected {want:?}, got {result:?}"),
    }
}

#[tokio::test]
async fn revocation_outcomes_follow_the_policy_table() {
    for scenario in SCENARIOS {
        check(scenario, false, scenario.strict).await;
        check(scenario, true, scenario.accept).await;
    }
}

#[tokio::test]
async fn off_accepts_a_revoked_chain_and_fetches_nothing() {
    // The same chain that the table rejects. With revocation off, it is not checked.
    let fx = fixture(Some(Served::Revoked), Served::Good, "127.0.0.1");
    get(revocation_config(&fx.pki.root_pem), None, fx.addr)
        .await
        .expect("revocation off does not check the chain");
    assert_eq!(fx.crl.hits(), 0, "revocation off must not fetch any CRL");
}

#[tokio::test]
async fn self_signed_mode_turns_revocation_off() {
    let fx = fixture(Some(Served::Revoked), Served::Good, "127.0.0.1");
    let mut config = revocation_config(&fx.pki.root_pem);
    config.tls.allow_self_signed = Some(true);
    get(config, Some(false), fx.addr)
        .await
        .expect("allow_self_signed turns revocation off");
    assert_eq!(fx.crl.hits(), 0, "allow_self_signed must not fetch any CRL");
}

#[tokio::test]
async fn hostname_mismatch_is_rejected_unless_allowed() {
    let fx = fixture(Some(Served::Good), Served::Good, "wrong-host.invalid");
    get(revocation_config(&fx.pki.root_pem), Some(false), fx.addr)
        .await
        .expect_err("a name mismatch is rejected by default");
    let mut config = revocation_config(&fx.pki.root_pem);
    config.tls.allow_hostname_mismatch = Some(true);
    get(config, Some(false), fx.addr)
        .await
        .expect("allow_hostname_mismatch passes a good chain with revocation still checked");
}

#[tokio::test]
async fn client_identity_works_with_revocation() {
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let mtls_addr = spawn_tls_server(&fx.pki, true);
    let mut config = revocation_config(&fx.pki.root_pem);
    config.tls.client_cert_pem = Some(fx.pki.client_cert_pem.clone().into_bytes());
    config.tls.client_key_pem = Some(fx.pki.client_key_pem.clone().into_bytes());
    get(config.clone(), Some(false), mtls_addr)
        .await
        .expect("mTLS identity passes with revocation");

    config.tls.client_cert_pem = None;
    config.tls.client_key_pem = None;
    get(config, Some(false), mtls_addr)
        .await
        .expect_err("the mTLS server rejects a client without an identity");
}

#[tokio::test]
async fn every_handshake_runs_the_revocation_check() {
    // A resumed session skips `verify_server_cert`. With resumption disabled on the client,
    // both connections are full handshakes, and so both run the check.
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let (addr, kinds) = spawn_resuming_server(&fx.pki);
    let http = client(revocation_config(&fx.pki.root_pem), Some(false));
    for _ in 0..2 {
        http.inner()
            .get(format!("https://{addr}/"))
            .send()
            .await
            .expect("good chain");
    }
    assert_eq!(
        *kinds.lock().unwrap(),
        vec![HandshakeKind::Full, HandshakeKind::Full],
        "a resumed handshake would skip the revocation check"
    );
}

#[tokio::test]
async fn custom_ca_outside_the_windows_store_is_revocation_checked() {
    // The fixture's root is a custom CA. Windows does not trust it, so the status comes from
    // CryptoAPI's chain through the additional store, and a revoked leaf is still rejected.
    let fx = fixture(Some(Served::Revoked), Served::Good, "127.0.0.1");
    let err = get(revocation_config(&fx.pki.root_pem), Some(false), fx.addr)
        .await
        .expect_err("a revoked leaf under a custom CA is rejected");
    assert!(err.contains("revoked"), "{err}");
    assert!(fx.crl.hits() > 0, "the custom CA chain must reach its CRL");
}

#[tokio::test]
async fn unrelated_presented_certificate_does_not_change_the_result() {
    // The server presents an extra certificate that is not on the chain. It has no CRL
    // distribution point, and CryptoAPI never reaches it, so the good chain still passes.
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let unrelated_key = KeyPair::generate().unwrap();
    let unrelated = ca_params("Revocation Test Unrelated", 9)
        .self_signed(&unrelated_key)
        .unwrap()
        .der()
        .clone();
    let addr = spawn_tls_chain(
        vec![
            fx.pki.leaf_der.clone(),
            fx.pki.intermediate_der.clone(),
            unrelated,
        ],
        fx.pki.leaf_key_der.clone(),
        None,
    );
    get(revocation_config(&fx.pki.root_pem), Some(false), addr)
        .await
        .expect("an unrelated presented certificate does not change the result");
}
