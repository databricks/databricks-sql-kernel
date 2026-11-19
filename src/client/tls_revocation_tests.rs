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

//! Hermetic revocation tests. Each fixture generates a root, an intermediate and a
//! leaf with rcgen, serves the CRLs over HTTP on loopback, and serves the leaf over
//! TLS on loopback. A forward-proxy stub can sit in front of the CRL fetches.

use super::*;
use crate::client::http::{
    CrlFetchLimits, DatabricksHttpClient, HttpClientConfig, RevocationPolicy,
};
use crate::client::retry::{build_retry_configs, RetryConfig};
use rcgen::{
    date_time_ymd, BasicConstraints, CertificateParams, CertificateRevocationListParams,
    CrlDistributionPoint, CustomExtension, DnType, IsCa, Issuer, KeyIdMethod, KeyPair,
    RevokedCertParams, SerialNumber, SigningKey,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::HandshakeKind;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Lower-cased fragment of rustls's `UnknownRevocationStatus` error text.
const UNDETERMINED: &str = "unknownrevocationstatus";
const ROOT_CN: &str = "Revocation Test Root";
const INTERMEDIATE_CN: &str = "Revocation Test Intermediate";
const LEAF_SERIAL: u64 = 1000;
const INTERMEDIATE_SERIAL: u64 = 2000;
const CLIENT_SERIAL: u64 = 3000;
/// (year, month, day) for CRL validity bounds.
type Ymd = (i32, u8, u8);
const CURRENT: Ymd = (2099, 1, 1);
const EXPIRED: Ymd = (2021, 1, 1);

/// What the CRL at one distribution point serves.
#[derive(Clone, Copy)]
enum Served {
    Good,
    /// Lists the covered certificate as revoked.
    Revoked,
    /// Signed with a key other than the issuer's (same issuer name).
    WrongKey,
    /// Signed by the other CA in the chain.
    WrongIssuer,
    Expired,
    Status(u16),
    /// Responds after the fetch timeout.
    Timeout,
    /// Body larger than the fetch size limit.
    Oversize,
    /// Distribution point is a closed port.
    Refused,
    /// Distribution point is not http(s), so there is nothing to fetch.
    NonHttp,
    /// Distribution point is indirect (has a cRLIssuer), so it must be skipped.
    Indirect,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Pass,
    Revoked,
    Undetermined,
    InvalidCrl,
}

#[derive(Clone, Copy)]
enum Relay {
    /// The proxy forwards plain CRL requests to the origin.
    Forward,
    /// The proxy answers plain CRL requests with 502. The origin must not be reached.
    Refuse,
}

/// One table row. Each row is checked in strict and accept-undetermined modes.
struct Scenario {
    name: &'static str,
    /// `None` means the leaf has no CRL distribution point.
    leaf: Option<Served>,
    intermediate: Served,
    relay: Option<Relay>,
    strict: Outcome,
    accept: Outcome,
}

// One row per line, so the table stays readable. rustfmt would split each row.
#[rustfmt::skip]
const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "good chain",
        leaf: Some(Served::Good),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Pass,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "revoked leaf",
        leaf: Some(Served::Revoked),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Revoked,
        accept: Outcome::Revoked,
    },
    Scenario {
        name: "revoked intermediate",
        leaf: Some(Served::Good),
        intermediate: Served::Revoked,
        relay: None,
        strict: Outcome::Revoked,
        accept: Outcome::Revoked,
    },
    Scenario {
        name: "leaf without CDP",
        leaf: None,
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CRL HTTP 503",
        leaf: Some(Served::Status(503)),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CRL refused",
        leaf: Some(Served::Refused),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CRL timeout",
        leaf: Some(Served::Timeout),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CRL oversize",
        leaf: Some(Served::Oversize),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CRL expired",
        leaf: Some(Served::Expired),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CDP not http(s)",
        leaf: Some(Served::NonHttp),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CDP indirect (cRLIssuer)",
        leaf: Some(Served::Indirect),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "intermediate CRL HTTP 503",
        leaf: Some(Served::Good),
        intermediate: Served::Status(503),
        relay: None,
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "leaf CRL wrong key",
        leaf: Some(Served::WrongKey),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::InvalidCrl,
        accept: Outcome::InvalidCrl,
    },
    Scenario {
        name: "leaf CRL wrong issuer",
        leaf: Some(Served::WrongIssuer),
        intermediate: Served::Good,
        relay: None,
        strict: Outcome::InvalidCrl,
        accept: Outcome::InvalidCrl,
    },
    Scenario {
        name: "good chain via proxy",
        leaf: Some(Served::Good),
        intermediate: Served::Good,
        relay: Some(Relay::Forward),
        strict: Outcome::Pass,
        accept: Outcome::Pass,
    },
    Scenario {
        name: "revoked leaf via proxy",
        leaf: Some(Served::Revoked),
        intermediate: Served::Good,
        relay: Some(Relay::Forward),
        strict: Outcome::Revoked,
        accept: Outcome::Revoked,
    },
    Scenario {
        name: "refused by proxy, no bypass",
        leaf: Some(Served::Good),
        intermediate: Served::Good,
        relay: Some(Relay::Refuse),
        strict: Outcome::Undetermined,
        accept: Outcome::Pass,
    },
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

/// The leaf's CRL distribution point.
enum LeafCdp {
    None,
    Uris(Vec<String>),
    /// An indirect point: `uri` as a full name, plus a cRLIssuer.
    Indirect(String),
}

/// A distribution point that is not http(s). Verifiers must ignore it.
const NON_HTTP_CDP: &str = "ldap://127.0.0.1/cn=crl";

/// A CRLDistributionPoints extension holding one indirect point.
fn indirect_cdp(uri: &str) -> CustomExtension {
    let tlv = |tag: u8, content: &[u8]| {
        assert!(content.len() < 128, "short-form DER only");
        let mut out = vec![tag, content.len() as u8];
        out.extend_from_slice(content);
        out
    };
    let full_name = tlv(0xA0, &tlv(0x86, uri.as_bytes()));
    let point_name = tlv(0xA0, &full_name);
    let issuer = tlv(0xA2, &tlv(0x86, b"http://crl-issuer.invalid/"));
    let point = tlv(0x30, &[point_name, issuer].concat());
    CustomExtension::from_oid_content(&[2, 5, 29, 31], tlv(0x30, &point))
}

fn build_pki(leaf_cdp: LeafCdp, intermediate_cdp: String, san: &str) -> Pki {
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
    let intermediate_pem = intermediate_cert.pem();
    let intermediate_der = intermediate_cert.der().clone();
    let intermediate_issuer = Issuer::new(intermediate_params, intermediate_key);

    let leaf_key = KeyPair::generate().unwrap();
    let mut leaf_params = CertificateParams::new(vec![san.to_owned()]).unwrap();
    leaf_params.serial_number = Some(SerialNumber::from(LEAF_SERIAL));
    match leaf_cdp {
        LeafCdp::None => {}
        LeafCdp::Uris(uris) => {
            leaf_params.crl_distribution_points = vec![CrlDistributionPoint { uris }];
        }
        LeafCdp::Indirect(uri) => leaf_params.custom_extensions = vec![indirect_cdp(&uri)],
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
        client_cert_pem: format!("{}{intermediate_pem}", client_cert.pem()),
        client_key_pem: client_key.serialize_pem(),
    }
}

/// A CRL listing `revoked` serials, valid until `next_update`.
fn sign_crl<K: SigningKey>(issuer: &Issuer<'_, K>, revoked: &[u64], next_update: Ymd) -> Vec<u8> {
    CertificateRevocationListParams {
        this_update: date_time_ymd(2020, 1, 1),
        next_update: date_time_ymd(next_update.0, next_update.1, next_update.2),
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

/// The route for one CRL. `issuer` signs it, `covered` is the serial it revokes,
/// and `other` is the other CA (used for `WrongIssuer`).
fn route(
    served: Served,
    issuer: &Issuer<'_, KeyPair>,
    issuer_cn: &str,
    covered: u64,
    other: &Issuer<'_, KeyPair>,
) -> Route {
    let body = |bytes: Vec<u8>| Route {
        status: 200,
        body: bytes,
        delay: Duration::ZERO,
    };
    match served {
        Served::Good => body(sign_crl(issuer, &[], CURRENT)),
        Served::Revoked => body(sign_crl(issuer, &[covered], CURRENT)),
        Served::WrongKey => {
            let impostor = Issuer::new(ca_params(issuer_cn, 0), KeyPair::generate().unwrap());
            body(sign_crl(&impostor, &[covered], CURRENT))
        }
        Served::WrongIssuer => body(sign_crl(other, &[], CURRENT)),
        Served::Expired => body(sign_crl(issuer, &[], EXPIRED)),
        Served::Status(status) => Route {
            status,
            body: Vec::new(),
            delay: Duration::ZERO,
        },
        Served::Timeout => Route {
            status: 200,
            body: sign_crl(issuer, &[], CURRENT),
            delay: Duration::from_secs(3),
        },
        Served::Oversize => body(vec![0; 128 * 1024]),
        Served::Refused | Served::NonHttp | Served::Indirect => {
            unreachable!("this distribution point kind has no route")
        }
    }
}

/// A PKI, its CRL origin server, and the TLS leaf server.
struct Fixture {
    crl: CrlServer,
    pki: Pki,
    addr: SocketAddr,
}

fn fixture(leaf: Option<Served>, intermediate: Served, san: &str) -> Fixture {
    let crl = CrlServer::start();
    let dead = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let cdp = |served: Served, path: &str| match served {
        Served::Refused => format!("http://{dead}{path}"),
        _ => crl.url(path),
    };
    let leaf_cdp = match leaf {
        None => LeafCdp::None,
        Some(Served::NonHttp) => LeafCdp::Uris(vec![NON_HTTP_CDP.to_owned()]),
        Some(Served::Indirect) => LeafCdp::Indirect(crl.url("/leaf.crl")),
        Some(served) => LeafCdp::Uris(vec![cdp(served, "/leaf.crl")]),
    };
    let pki = build_pki(leaf_cdp, cdp(intermediate, "/intermediate.crl"), san);
    // An indirect point names a CRL that is served but must be skipped, so serve a good one.
    let routed = match leaf {
        Some(Served::Indirect) => Some(Served::Good),
        Some(Served::Refused | Served::NonHttp) | None => None,
        other => other,
    };
    if let Some(served) = routed {
        crl.set(
            "/leaf.crl",
            route(
                served,
                &pki.intermediate_issuer,
                INTERMEDIATE_CN,
                LEAF_SERIAL,
                &pki.root_issuer,
            ),
        );
    }
    if !matches!(intermediate, Served::Refused) {
        crl.set(
            "/intermediate.crl",
            route(
                intermediate,
                &pki.root_issuer,
                ROOT_CN,
                INTERMEDIATE_SERIAL,
                &pki.intermediate_issuer,
            ),
        );
    }
    let addr = spawn_tls_server(&pki, false);
    Fixture { crl, pki, addr }
}

/// A TLS server presenting the leaf chain. With `mtls`, it requires a client
/// certificate that chains to the test root.
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

#[derive(Clone)]
struct Route {
    status: u16,
    body: Vec<u8>,
    delay: Duration,
}

/// Loopback HTTP server for CRLs. It records each request head and answers from a
/// route table that can be filled after the PKI (which holds the URLs) exists.
#[derive(Clone)]
struct CrlServer {
    addr: SocketAddr,
    routes: Arc<Mutex<HashMap<String, Route>>>,
    /// Paths that answer `302 Found` with the given `Location`.
    redirects: Arc<Mutex<HashMap<String, String>>>,
    requests: Arc<Mutex<Vec<(String, String)>>>,
}

impl CrlServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server = Self {
            addr: listener.local_addr().unwrap(),
            routes: Arc::new(Mutex::new(HashMap::new())),
            redirects: Arc::new(Mutex::new(HashMap::new())),
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

    fn redirect(&self, path: &str, target: String) {
        self.redirects
            .lock()
            .unwrap()
            .insert(path.to_owned(), target);
    }

    /// Requests received for `path`.
    fn hits(&self, path: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(seen, _)| seen == path)
            .count()
    }

    /// Every request head received, lower-cased.
    fn headers(&self) -> String {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(_, head)| head.to_ascii_lowercase())
            .collect()
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
        self.requests.lock().unwrap().push((path.clone(), head));
        if let Some(target) = self.redirects.lock().unwrap().get(&path).cloned() {
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
            return;
        }
        let route = self
            .routes
            .lock()
            .unwrap()
            .get(&path)
            .cloned()
            .unwrap_or(Route {
                status: 404,
                body: Vec::new(),
                delay: Duration::ZERO,
            });
        if !route.delay.is_zero() {
            thread::sleep(route.delay);
        }
        let mut response = format!(
            "HTTP/1.1 {} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            route.status,
            route.body.len()
        )
        .into_bytes();
        response.extend_from_slice(&route.body);
        // The client may have given up already (timeout case).
        let _ = stream.write_all(&response);
    }
}

/// Loopback forward proxy. Plain requests arrive in absolute form and are recorded.
/// CONNECT is tunnelled, so the TLS leaf also connects through the proxy.
struct ForwardProxy {
    addr: SocketAddr,
    heads: Arc<Mutex<Vec<String>>>,
}

impl ForwardProxy {
    fn start(relay: Relay) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let heads = Arc::new(Mutex::new(Vec::new()));
        let shared = Arc::clone(&heads);
        thread::spawn(move || {
            for client in listener.incoming().flatten() {
                let heads = Arc::clone(&shared);
                thread::spawn(move || handle_proxy_client(client, &heads, relay));
            }
        });
        Self { addr, heads }
    }

    /// Plain (non-CONNECT) request heads the proxy received.
    fn plain_requests(&self) -> Vec<String> {
        self.heads
            .lock()
            .unwrap()
            .iter()
            .filter(|head| !head.starts_with("CONNECT "))
            .cloned()
            .collect()
    }
}

fn handle_proxy_client(mut client: TcpStream, heads: &Mutex<Vec<String>>, relay: Relay) {
    let Some(head) = read_head(&mut client) else {
        return;
    };
    heads.lock().unwrap().push(head.clone());
    let mut request_line = head.lines().next().unwrap_or("").split_whitespace();
    let method = request_line.next().unwrap_or("").to_owned();
    let target = request_line.next().unwrap_or("").to_owned();
    let bad_gateway = b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    if method == "CONNECT" {
        if let Ok(upstream) = TcpStream::connect(&target) {
            if client
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .is_ok()
            {
                tunnel(client, upstream);
            }
        }
        return;
    }
    if matches!(relay, Relay::Refuse) {
        let _ = client.write_all(bad_gateway);
        return;
    }
    // Absolute-form target: http://host:port/path. Forward it in origin form, minus
    // the proxy-only headers, and relay the origin's response back.
    let rest = target.strip_prefix("http://").unwrap_or(&target);
    let (authority, path) = rest
        .split_once('/')
        .map_or((rest, "/".to_owned()), |(authority, path)| {
            (authority, format!("/{path}"))
        });
    let Ok(mut upstream) = TcpStream::connect(authority) else {
        let _ = client.write_all(bad_gateway);
        return;
    };
    let mut forwarded = format!("{method} {path} HTTP/1.1\r\n");
    for line in head.split("\r\n").skip(1) {
        if !line.is_empty() && !line.to_ascii_lowercase().starts_with("proxy-") {
            forwarded.push_str(line);
            forwarded.push_str("\r\n");
        }
    }
    forwarded.push_str("\r\n");
    if upstream.write_all(forwarded.as_bytes()).is_ok() {
        let mut response = Vec::new();
        let _ = upstream.read_to_end(&mut response);
        let _ = client.write_all(&response);
    }
}

/// Copies bytes both ways until either side closes.
fn tunnel(client: TcpStream, upstream: TcpStream) {
    let (Ok(mut client_read), Ok(mut upstream_write)) = (client.try_clone(), upstream.try_clone())
    else {
        return;
    };
    let mut upstream_read = upstream;
    let mut client_write = client;
    let back = thread::spawn(move || {
        let _ = std::io::copy(&mut upstream_read, &mut client_write);
    });
    let _ = std::io::copy(&mut client_read, &mut upstream_write);
    let _ = upstream_write.shutdown(Shutdown::Write);
    let _ = back.join();
}

/// Sets the CRL fetch bounds for clients built on this test's thread.
fn set_fetch_limits(limits: CrlFetchLimits) {
    TEST_CRL_LIMITS.with(|cell| cell.set(Some(limits)));
}

/// The revocation policy for a test client: checking on, and an undetermined status
/// accepted when `accept_undetermined` is set.
fn policy(accept_undetermined: bool) -> Option<RevocationPolicy> {
    Some(RevocationPolicy {
        accept_undetermined,
    })
}

/// Config that trusts the test root and fetches with short bounds. The revocation policy
/// is passed separately, as [`policy`].
fn revocation_config(root_pem: &str) -> HttpClientConfig {
    let mut config = HttpClientConfig::default();
    config.tls.trusted_certificate_pem = Some(root_pem.as_bytes().to_vec());
    set_fetch_limits(CrlFetchLimits {
        timeout: Duration::from_millis(500),
        max_bytes: 64 * 1024,
    });
    // No pooling: each request runs a fresh handshake and revocation check.
    config.max_connections_per_host = 0;
    config
}

/// A client built from `config` and `revocation`, with default retry configs.
fn client(config: HttpClientConfig, revocation: Option<RevocationPolicy>) -> DatabricksHttpClient {
    DatabricksHttpClient::with_revocation(
        config,
        build_retry_configs(&RetryConfig::default(), &HashMap::new()),
        revocation,
    )
    .unwrap()
}

async fn get(
    config: HttpClientConfig,
    revocation: Option<RevocationPolicy>,
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

/// Runs one table row in one policy mode and checks the outcome, the proxy routing,
/// and that the CRL requests carry no auth or connection headers.
async fn check(scenario: &Scenario, accept: bool, want: Outcome) {
    let fx = fixture(scenario.leaf, scenario.intermediate, "127.0.0.1");
    let proxy = scenario.relay.map(ForwardProxy::start);
    let mut config = revocation_config(&fx.pki.root_pem);
    config.custom_headers = vec![("X-Kernel-Secret".to_owned(), "hunter2".to_owned())];
    config.org_id = Some("1234".to_owned());
    if let Some(proxy) = &proxy {
        config.proxy.url = Some(format!("http://{}", proxy.addr));
    }

    let label = format!("{} (accept_undetermined={accept})", scenario.name);
    let result = get(config, policy(accept), fx.addr).await;
    match (want, &result) {
        (Outcome::Pass, Ok(())) => {}
        (Outcome::Revoked, Err(err)) => assert!(err.contains("revoked"), "{label}: {err}"),
        (Outcome::Undetermined, Err(err)) => assert!(err.contains(UNDETERMINED), "{label}: {err}"),
        (Outcome::InvalidCrl, Err(err)) => {
            assert!(
                err.contains("revocation list") && !err.contains(UNDETERMINED),
                "{label}: {err}"
            );
        }
        _ => panic!("{label}: expected {want:?}, got {result:?}"),
    }

    let headers = fx.crl.headers();
    for forbidden in [
        "authorization",
        "x-kernel-secret",
        "x-databricks-org-id",
        "cookie",
    ] {
        assert!(
            !headers.contains(forbidden),
            "{label}: CRL request carried {forbidden}: {headers}"
        );
    }
    match (scenario.relay, proxy) {
        (Some(Relay::Forward), Some(proxy)) => {
            assert!(
                !proxy.plain_requests().is_empty(),
                "{label}: CRL fetches must reach the proxy"
            );
        }
        (Some(Relay::Refuse), Some(proxy)) => {
            assert!(
                !proxy.plain_requests().is_empty(),
                "{label}: CRL fetches must reach the proxy"
            );
            assert_eq!(
                fx.crl.hits("/leaf.crl") + fx.crl.hits("/intermediate.crl"),
                0,
                "{label}: proxy refusal must not bypass to the origin"
            );
        }
        _ => {}
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
async fn current_crl_is_fetched_once_and_a_rejected_one_is_refetched() {
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let http = client(revocation_config(&fx.pki.root_pem), policy(false));
    for _ in 0..2 {
        http.inner()
            .get(format!("https://{}/", fx.addr))
            .send()
            .await
            .expect("good chain");
    }
    assert_eq!(
        fx.crl.hits("/leaf.crl"),
        1,
        "a current CRL is cached until nextUpdate"
    );
    assert_eq!(fx.crl.hits("/intermediate.crl"), 1);

    let fx = fixture(Some(Served::WrongKey), Served::Good, "127.0.0.1");
    let http = client(revocation_config(&fx.pki.root_pem), policy(false));
    for _ in 0..2 {
        assert!(http
            .inner()
            .get(format!("https://{}/", fx.addr))
            .send()
            .await
            .is_err());
    }
    assert_eq!(
        fx.crl.hits("/leaf.crl"),
        2,
        "a CRL that webpki rejects is not served from cache"
    );
}

#[tokio::test]
async fn hostname_mismatch_is_rejected_unless_allowed() {
    let fx = fixture(Some(Served::Good), Served::Good, "wrong-host.invalid");
    get(revocation_config(&fx.pki.root_pem), policy(false), fx.addr)
        .await
        .expect_err("a name mismatch is rejected by default");
    let mut config = revocation_config(&fx.pki.root_pem);
    config.tls.allow_hostname_mismatch = Some(true);
    get(config, policy(false), fx.addr)
        .await
        .expect("allow_hostname_mismatch passes a good chain with revocation still checked");
}

#[tokio::test]
async fn client_identity_and_custom_ca_work_with_revocation() {
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let mtls_addr = spawn_tls_server(&fx.pki, true);
    let mut config = revocation_config(&fx.pki.root_pem);
    config.tls.client_cert_pem = Some(fx.pki.client_cert_pem.clone().into_bytes());
    config.tls.client_key_pem = Some(fx.pki.client_key_pem.clone().into_bytes());
    get(config.clone(), policy(false), mtls_addr)
        .await
        .expect("mTLS identity passes with revocation");

    config.tls.client_cert_pem = None;
    config.tls.client_key_pem = None;
    get(config, policy(false), mtls_addr)
        .await
        .expect_err("the mTLS server rejects a client without an identity");
}

#[tokio::test]
async fn policy_off_and_accept_self_signed_skip_revocation() {
    let fx = fixture(Some(Served::Revoked), Served::Good, "127.0.0.1");
    get(revocation_config(&fx.pki.root_pem), None, fx.addr)
        .await
        .expect("policy off passes a revoked leaf");

    let mut self_signed = revocation_config(&fx.pki.root_pem);
    self_signed.tls.allow_self_signed = Some(true);
    get(self_signed, policy(false), fx.addr)
        .await
        .expect("accept_self_signed turns revocation off");

    assert_eq!(
        fx.crl.hits("/leaf.crl"),
        0,
        "no CRL is fetched when revocation is off"
    );
}

#[tokio::test]
async fn unrelated_appended_certificate_is_never_fetched() {
    // The server appends a certificate that no path needs, with its own distribution point.
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let other_issuer = Issuer::new(
        ca_params("Unrelated Test CA", 9),
        KeyPair::generate().unwrap(),
    );
    let other_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["unrelated.invalid".to_owned()]).unwrap();
    params.crl_distribution_points = vec![CrlDistributionPoint {
        uris: vec![fx.crl.url("/unrelated.crl")],
    }];
    let unrelated = params
        .signed_by(&other_key, &other_issuer)
        .unwrap()
        .der()
        .clone();
    fx.crl.set(
        "/unrelated.crl",
        route(
            Served::Good,
            &other_issuer,
            "Unrelated Test CA",
            9,
            &other_issuer,
        ),
    );
    let chain = vec![
        fx.pki.leaf_der.clone(),
        fx.pki.intermediate_der.clone(),
        unrelated,
    ];
    let addr = spawn_tls_chain(chain, fx.pki.leaf_key_der.clone(), None);
    get(revocation_config(&fx.pki.root_pem), policy(false), addr)
        .await
        .expect("the good path verifies; the appended certificate is not on it");
    assert_eq!(
        fx.crl.hits("/unrelated.crl"),
        0,
        "an unneeded certificate's distribution point is never fetched"
    );
}

#[test]
fn same_name_certificate_with_another_key_is_not_a_trust_anchor() {
    // Anchors match by subject and SPKI, as webpki matches them. The root's name with
    // another key is not an anchor, so its distribution points are still checked.
    let root_key = KeyPair::generate().unwrap();
    let root_der = ca_params(ROOT_CN, 1)
        .self_signed(&root_key)
        .unwrap()
        .der()
        .clone();
    let mut roots = RootCertStore::empty();
    roots.add(root_der.clone()).unwrap();
    let other_key = KeyPair::generate().unwrap();
    let same_name_der = ca_params(ROOT_CN, 2)
        .self_signed(&other_key)
        .unwrap()
        .der()
        .clone();
    let anchor = parse_cert(root_der.as_ref()).unwrap();
    let same_name = parse_cert(same_name_der.as_ref()).unwrap();
    assert!(
        is_trust_anchor(&roots, &anchor),
        "the anchor matches itself"
    );
    assert!(
        !is_trust_anchor(&roots, &same_name),
        "a same-name certificate with another key is not an anchor"
    );
}

#[tokio::test]
async fn one_timeout_budget_bounds_all_fetches_for_a_chain() {
    // Both distribution points hang past the per-fetch timeout. Without a shared budget
    // the two fetches take about two timeouts (about 4 s). With it, the chain takes about
    // one (about 2 s). The bound of 3 s sits between the two, with about a second of slack
    // on each side.
    let fx = fixture(Some(Served::Timeout), Served::Timeout, "127.0.0.1");
    let config = revocation_config(&fx.pki.root_pem);
    set_fetch_limits(CrlFetchLimits {
        timeout: Duration::from_millis(2000),
        max_bytes: 64 * 1024,
    });
    let started = Instant::now();
    let err = get(config, policy(false), fx.addr)
        .await
        .expect_err("undetermined under strict");
    let elapsed = started.elapsed();
    assert!(err.contains(UNDETERMINED), "{err}");
    assert!(
        elapsed < Duration::from_millis(3000),
        "the fetches share one budget, but the check took {elapsed:?}"
    );
}

#[tokio::test]
async fn final_verify_uses_only_the_checked_path() {
    // Two intermediates share a name and key. Only the first is needed. The second is
    // revoked, and its distribution point must neither be fetched nor used by webpki.
    let crl = CrlServer::start();
    let root_key = KeyPair::generate().unwrap();
    let root_params = ca_params(ROOT_CN, 1);
    let root_pem = root_params.self_signed(&root_key).unwrap().pem();
    let root_issuer = Issuer::new(root_params, root_key);

    let inter_key = KeyPair::generate().unwrap();
    let inter_pem = inter_key.serialize_pem();
    let good_der = signed_intermediate(&inter_key, 2001, crl.url("/good.crl"), &root_issuer);
    let revoked_der = signed_intermediate(
        &KeyPair::from_pem(&inter_pem).unwrap(),
        2002,
        crl.url("/revoked.crl"),
        &root_issuer,
    );
    crl.set(
        "/good.crl",
        route(Served::Good, &root_issuer, ROOT_CN, 2001, &root_issuer),
    );
    crl.set(
        "/revoked.crl",
        route(Served::Revoked, &root_issuer, ROOT_CN, 2002, &root_issuer),
    );

    let inter_issuer = Issuer::new(
        ca_params(INTERMEDIATE_CN, 2001),
        KeyPair::from_pem(&inter_pem).unwrap(),
    );
    let mut leaf_params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
    leaf_params.crl_distribution_points = vec![CrlDistributionPoint {
        uris: vec![crl.url("/leaf.crl")],
    }];
    let leaf_key = KeyPair::generate().unwrap();
    let leaf_der = leaf_params
        .signed_by(&leaf_key, &inter_issuer)
        .unwrap()
        .der()
        .clone();
    crl.set(
        "/leaf.crl",
        route(
            Served::Good,
            &inter_issuer,
            INTERMEDIATE_CN,
            LEAF_SERIAL,
            &inter_issuer,
        ),
    );

    let addr = spawn_tls_chain(
        vec![leaf_der, good_der, revoked_der],
        leaf_key.serialize_der(),
        None,
    );
    get(revocation_config(&root_pem), policy(false), addr)
        .await
        .expect("the checked path verifies; the unused revoked intermediate is ignored");
    assert_eq!(
        crl.hits("/revoked.crl"),
        0,
        "an intermediate off the checked path is never fetched"
    );
}

/// A CA certificate with `serial`, a distribution point at `cdp`, signed by `issuer`.
fn signed_intermediate(
    key: &KeyPair,
    serial: u64,
    cdp: String,
    issuer: &Issuer<'_, KeyPair>,
) -> CertificateDer<'static> {
    let mut params = ca_params(INTERMEDIATE_CN, serial);
    params.crl_distribution_points = vec![CrlDistributionPoint { uris: vec![cdp] }];
    params.signed_by(key, issuer).unwrap().der().clone()
}

#[test]
fn redacted_removes_userinfo_query_and_fragment() {
    assert_eq!(
        redacted("https://user:secret@crl.example.com/a.crl?token=abc#frag"),
        "https://crl.example.com/a.crl"
    );
    assert_eq!(
        redacted("http://crl.example.com/ca.crl"),
        "http://crl.example.com/ca.crl"
    );
    assert_eq!(redacted("not a url"), "<unparsable URL>");
}

#[test]
fn intermediate_cap_allows_six_and_rejects_seven() {
    // Six presented intermediates pass the cap and reach path selection, whatever it then
    // decides. Seven fail at the cap with a message that names the limit.
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let mut roots = RootCertStore::empty();
    roots.add(fx.pki.root_der.clone()).unwrap();
    let verifier = RevocationVerifier {
        roots: Arc::new(roots.clone()),
        provider: Arc::new(rustls::crypto::ring::default_provider()),
        signatures: WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .unwrap(),
        fetcher: CrlFetcher {
            proxy: ProxyConfig::default(),
            limits: CrlFetchLimits::default(),
            custom_roots: Vec::new(),
            cache: Mutex::new(HashMap::new()),
            failed: Mutex::new(HashMap::new()),
            worker: Mutex::new(None),
        },
        accept_undetermined: false,
        allow_hostname_mismatch: false,
    };
    let end = &fx.pki.leaf_der;
    let server_name = ServerName::try_from("127.0.0.1").unwrap();
    let now = UnixTime::now();

    // Any outcome other than the cap error is fine here: the path search decides it.
    let six = vec![fx.pki.intermediate_der.clone(); 6];
    if let Err(six_error) = verifier.path_intermediates(end, &six, &server_name, &[], now) {
        assert!(
            !six_error.to_string().contains("supports at most"),
            "six intermediates must pass the cap: {six_error}"
        );
    }

    let seven = vec![fx.pki.intermediate_der.clone(); 7];
    match verifier.path_intermediates(end, &seven, &server_name, &[], now) {
        Err(error @ rustls::Error::InvalidCertificate(_)) => assert!(
            error.to_string().contains(
                "the server presented 7 intermediates; revocation checking supports at most 6"
            ),
            "unexpected message: {error}"
        ),
        other => panic!("seven intermediates must fail at the cap, got {other:?}"),
    }
}

/// A TLS server that allows session resumption and records the kind of each handshake.
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

#[tokio::test]
async fn every_handshake_runs_the_revocation_check() {
    // A resumed session skips `verify_server_cert`. With resumption disabled on the client,
    // both connections are full handshakes, and so both run the check.
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    let (addr, kinds) = spawn_resuming_server(&fx.pki);
    let http = client(revocation_config(&fx.pki.root_pem), policy(false));
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
async fn a_crl_redirect_is_not_followed_and_is_undetermined() {
    // A redirect from the CRL endpoint could send the fetch to an internal address, so it is
    // not followed. The 302 is undetermined: strict rejects, accept passes, and the target
    // is never requested.
    let fx = fixture(Some(Served::Good), Served::Good, "127.0.0.1");
    fx.crl.redirect("/leaf.crl", fx.crl.url("/target.crl"));
    fx.crl.set(
        "/target.crl",
        route(
            Served::Good,
            &fx.pki.intermediate_issuer,
            INTERMEDIATE_CN,
            LEAF_SERIAL,
            &fx.pki.root_issuer,
        ),
    );
    let err = get(revocation_config(&fx.pki.root_pem), policy(false), fx.addr)
        .await
        .expect_err("a redirected CRL is undetermined under strict");
    assert!(err.contains(UNDETERMINED), "{err}");
    get(revocation_config(&fx.pki.root_pem), policy(true), fx.addr)
        .await
        .expect("a redirected CRL passes under accept-undetermined");
    assert_eq!(
        fx.crl.hits("/target.crl"),
        0,
        "the redirect target is never requested"
    );
}

#[tokio::test]
async fn a_failed_crl_fetch_is_not_retried_within_its_ttl() {
    // The leaf's CRL endpoint returns 503. The first handshake waits for it and fails. The
    // second, on the same client, does not ask again.
    let fx = fixture(Some(Served::Status(503)), Served::Good, "127.0.0.1");
    let http = client(revocation_config(&fx.pki.root_pem), policy(false));
    for _ in 0..2 {
        assert!(http
            .inner()
            .get(format!("https://{}/", fx.addr))
            .send()
            .await
            .is_err());
    }
    assert_eq!(
        fx.crl.hits("/leaf.crl"),
        1,
        "a failed fetch is remembered for its TTL"
    );
}

#[tokio::test]
async fn a_usable_crl_stops_the_other_distribution_points() {
    // The leaf names two distribution points. The first serves a usable CRL, so the
    // second, which would hang, is never requested.
    let crl = CrlServer::start();
    let pki = build_pki(
        LeafCdp::Uris(vec![crl.url("/good.crl"), crl.url("/slow.crl")]),
        crl.url("/intermediate.crl"),
        "127.0.0.1",
    );
    crl.set(
        "/good.crl",
        route(
            Served::Good,
            &pki.intermediate_issuer,
            INTERMEDIATE_CN,
            LEAF_SERIAL,
            &pki.root_issuer,
        ),
    );
    crl.set(
        "/slow.crl",
        route(
            Served::Timeout,
            &pki.intermediate_issuer,
            INTERMEDIATE_CN,
            LEAF_SERIAL,
            &pki.root_issuer,
        ),
    );
    crl.set(
        "/intermediate.crl",
        route(
            Served::Good,
            &pki.root_issuer,
            ROOT_CN,
            INTERMEDIATE_SERIAL,
            &pki.intermediate_issuer,
        ),
    );
    let addr = spawn_tls_server(&pki, false);
    get(revocation_config(&pki.root_pem), policy(false), addr)
        .await
        .expect("the usable CRL covers the leaf");
    assert_eq!(
        crl.hits("/slow.crl"),
        0,
        "a second distribution point is not fetched once a CRL is usable"
    );
}

#[test]
fn crl_cache_drops_expired_entries_and_stays_within_its_caps() {
    let mut cache: HashMap<String, CachedCrl> = HashMap::new();
    cache.insert("expired".to_owned(), (Arc::new(vec![0_u8; 8]), 5));
    for i in 0..(MAX_CACHED_CRLS + 10) {
        cache.insert(format!("crl{i}"), (Arc::new(vec![0_u8; 8]), 100 + i as u64));
    }
    trim_cache(&mut cache, 10);
    assert!(
        !cache.contains_key("expired"),
        "expired entries are dropped"
    );
    assert_eq!(
        cache.len(),
        MAX_CACHED_CRLS,
        "the count cap keeps the entries that expire last"
    );
    assert!(cache.contains_key(&format!("crl{}", MAX_CACHED_CRLS + 9)));
    assert!(
        !cache.contains_key("crl0"),
        "the soonest to expire goes first"
    );

    // The byte cap: three 8 MiB entries exceed 16 MiB, so the soonest-expiring one goes.
    let mut big: HashMap<String, CachedCrl> = HashMap::new();
    for i in 0..3_u64 {
        big.insert(
            format!("big{i}"),
            (Arc::new(vec![0_u8; 8 * 1024 * 1024]), 100 + i),
        );
    }
    trim_cache(&mut big, 10);
    assert_eq!(big.len(), 2, "the byte cap keeps the cache under 16 MiB");
    assert!(!big.contains_key("big0"));
}

#[test]
fn verifier_errors_are_certificate_verification_failures() {
    let error = verifier_error("the server presented 7 intermediates".to_owned());
    assert!(crate::client::retry::is_certificate_verification_failure(
        &error
    ));
}
