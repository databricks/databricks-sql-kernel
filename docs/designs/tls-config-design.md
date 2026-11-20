<!--
  Copyright (c) 2026 Databricks, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# TLS Configuration Support Design

## Overview

Add TLS configuration parameters to the Rust ADBC driver, enabling use in environments that require custom certificate authorities, self-signed certificates, or relaxed certificate validation (e.g., development environments with TLS-intercepting proxies). This brings the Rust driver to parity with the C# driver's TLS configuration surface.

## Motivation

Enterprise environments commonly use TLS-intercepting proxies (e.g., mitmproxy, Zscaler, corporate SSL inspection) that present certificates signed by an internal CA. Without the ability to trust custom CAs or relax certificate validation, the driver cannot connect in these environments. Development and testing workflows also benefit from being able to disable certificate checks when working with self-signed certificates.

The C# driver already supports TLS configuration via `adbc.http_options.tls.*` parameters (inherited from the HiveServer2 base class). This design brings the Rust driver to feature parity using the driver's established `databricks.*` namespace.

## Current State

### reqwest's Default TLS Behavior

The driver uses `reqwest` 0.12 with both **native-tls** (OpenSSL on Linux) and **rustls** backends compiled in. By default:

- TLS is always enabled for `https://` URIs (Databricks endpoints always use HTTPS)
- Server certificates are validated against the system trust store
- Hostname verification is enforced
- Self-signed certificates are rejected

There is currently no way to configure any of this behavior via ADBC parameters.

### Existing Architecture

All HTTP requests flow through `DatabricksHttpClient` (`src/client/http.rs`), which wraps a single `reqwest::Client`. TLS configuration only needs to be applied in one place: `DatabricksHttpClient::new()`. This is the same integration point used for proxy configuration.

## Design

### Configuration Parameters

New ADBC database options under the `databricks.http.tls` namespace:

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `databricks.http.tls.enabled` | bool | Enable TLS for connections. When `false`, forces plain HTTP. | `true` |
| `databricks.http.tls.allow_self_signed` | bool | Accept self-signed server certificates (disables all cert validation). | `false` |
| `databricks.http.tls.allow_hostname_mismatch` | bool | Don't verify that the certificate hostname matches the server. | `false` |
| `databricks.http.tls.trusted_certificate_path` | String | Path to a PEM-encoded CA certificate file to add to the trust store. | None |

### Naming Convention

The Rust driver uses the `databricks.*` namespace for all driver-specific options. The C# driver uses `adbc.http_options.tls.*`, but this is inherited from the Apache Arrow HiveServer2 base class — it is **not** part of the official ADBC specification. The ADBC spec only defines standard keys for `uri`, `username`, `password`, and `adbc.connection.*` / `adbc.statement.*` options.

The `databricks.http.tls.*` namespace is consistent with existing Rust driver conventions:
- `databricks.http.proxy.*` — proxy configuration
- `databricks.http.connect_timeout_ms` — connection timeout
- `databricks.http.read_timeout_ms` — read timeout
- `databricks.http.max_retries` — retry count

### Semantic Behavior

#### `tls.enabled`

Controls whether the driver uses HTTPS or HTTP. When set to `false`, the driver will use `http://` instead of `https://` for connections. This is primarily useful for local development against mock servers.

**Note:** Databricks cloud endpoints always require TLS. Setting this to `false` against a real Databricks workspace will fail.

#### `tls.allow_self_signed`

Accepts certificates that are not signed by a trusted CA (self-signed or signed by an unknown CA).

**Important:** reqwest does not support accepting only self-signed certs while still validating the trust chain. Setting this to `true` calls `danger_accept_invalid_certs(true)`, which disables ALL certificate validation (CA trust, expiration, etc.). The driver logs a warning to make this clear.

This matches the Simba ODBC driver's `AllowSelfSignedCert` option. An earlier design considered adding a separate `disable_server_certificate_validation` flag (matching the C# driver), but since reqwest cannot distinguish between the two behaviors, a single option is cleaner.

#### `tls.allow_hostname_mismatch`

Allows the server certificate's Common Name (CN) or Subject Alternative Name (SAN) to not match the hostname being connected to. This is useful when connecting through a proxy that rewrites hostnames.

**Implementation note:** `reqwest::ClientBuilder::danger_accept_invalid_hostnames()` is only available with the **native-tls** backend. Since the project currently compiles both backends, we need to ensure native-tls is used when this option is set. We will add the `native-tls` feature to reqwest to guarantee `danger_accept_invalid_hostnames()` is available at compile time.

#### `tls.trusted_certificate_path`

Adds a custom CA certificate to the trust store (in addition to system roots). The file must be PEM-encoded. This is the most common TLS option — used for corporate proxies with internal CAs.

The certificate file is read at connection creation time (`Database::new_connection()`). If the file doesn't exist or isn't valid PEM, an error is returned immediately.

### Code Changes

#### 1. `TlsConfig` — New config struct

```rust
// src/client/http.rs

/// Configuration for TLS behavior.
///
/// All fields default to the most secure settings. Relaxing any of these
/// options should only be done in development/testing environments.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Enable TLS for connections. When `false`, forces plain HTTP.
    /// Default: true (TLS enabled).
    pub enabled: Option<bool>,
    /// Accept self-signed server certificates.
    /// Note: reqwest disables ALL cert validation when this is true.
    /// Default: false.
    pub allow_self_signed: Option<bool>,
    /// Don't verify that the certificate hostname matches the server.
    /// Default: false.
    pub allow_hostname_mismatch: Option<bool>,
    /// Path to a PEM-encoded CA certificate file to add to the trust store.
    pub trusted_certificate_path: Option<String>,
}
```

Add `pub tls: TlsConfig` to `HttpClientConfig`.

#### 2. `DatabricksHttpClient::new()` — Apply TLS configuration

```rust
// src/client/http.rs — inside DatabricksHttpClient::new()

// Apply TLS configuration
let tls = &config.tls;

// reqwest does not support accepting only self-signed certs while still
// validating the trust chain — danger_accept_invalid_certs(true) disables
// ALL certificate validation (CA trust, expiration, etc.).
if tls.allow_self_signed.unwrap_or(false) {
    warn!("TLS certificate validation is disabled — this is insecure and should only be used in development");
    warn!("allow_self_signed is enabled — note: reqwest does not distinguish self-signed from fully unvalidated; this disables ALL certificate validation");
    builder = builder.danger_accept_invalid_certs(true);
}

if tls.allow_hostname_mismatch.unwrap_or(false) {
    warn!("TLS hostname verification is disabled — this is insecure and should only be used in development");
    builder = builder.danger_accept_invalid_hostnames(true);
}

if let Some(ref cert_path) = tls.trusted_certificate_path {
    let pem = std::fs::read(cert_path).map_err(|e| {
        DatabricksErrorHelper::invalid_argument()
            .message(format!("Failed to read TLS certificate '{}': {}", cert_path, e))
    })?;
    let cert = reqwest::Certificate::from_pem(&pem).map_err(|e| {
        DatabricksErrorHelper::invalid_argument()
            .message(format!("Invalid PEM certificate '{}': {}", cert_path, e))
    })?;
    builder = builder.add_root_certificate(cert);
    debug!("Added custom CA certificate from: {}", cert_path);
}

if tls.enabled == Some(false) {
    builder = builder.https_only(false);
    warn!("TLS is disabled — connections will use plain HTTP");
}
```

#### 3. `Database` — Parse TLS options

```rust
// src/database.rs — inside set_option() match block

"databricks.http.tls.enabled" => {
    self.http_config.tls.enabled = Some(Self::parse_bool_option(&key, &value)?);
    Ok(())
}
"databricks.http.tls.allow_self_signed" => {
    self.http_config.tls.allow_self_signed = Some(Self::parse_bool_option(&key, &value)?);
    Ok(())
}
"databricks.http.tls.allow_hostname_mismatch" => {
    self.http_config.tls.allow_hostname_mismatch =
        Some(Self::parse_bool_option(&key, &value)?);
    Ok(())
}
"databricks.http.tls.trusted_certificate_path" => {
    if let OptionValue::String(v) = value {
        self.http_config.tls.trusted_certificate_path = Some(v);
        Ok(())
    } else {
        Err(DatabricksErrorHelper::set_invalid_option(&key, &value).to_adbc())
    }
}
```

Also add corresponding `get_option_string` and `get_option_double` (for bools as int) entries.

### Cargo.toml Changes

Add the `native-tls` feature to reqwest to guarantee `danger_accept_invalid_hostnames()` is available:

```toml
reqwest = { version = "0.12", features = ["json", "gzip", "stream", "native-tls"] }
```

This is a no-op in terms of binary behavior (native-tls is already compiled in as a transitive dependency), but ensures the method is available at the `reqwest::ClientBuilder` API level.

## Validation

- `tls.enabled`, `tls.allow_self_signed`, `tls.allow_hostname_mismatch` must be valid boolean values ("true"/"false"/"1"/"0"/"yes"/"no" or `Int(0)`/`Int(1)`). Invalid values produce `InvalidArgument` errors via `parse_bool_option()`.
- `tls.trusted_certificate_path` must point to an existing, readable PEM file. Errors are raised at connection creation time, not at `set_option` time.
- Setting `tls.allow_hostname_mismatch = true` logs a `warn!` message.
- Setting `tls.enabled = false` logs a `warn!` message.

## Testing

### Unit Tests (`src/client/http.rs`)

1. **`test_tls_config_default`** — `TlsConfig::default()` has all `None` fields.
2. **`test_http_client_with_allow_self_signed`** — Client builds successfully with `allow_self_signed = true`.
3. **`test_http_client_with_hostname_mismatch_allowed`** — Client builds with `allow_hostname_mismatch = true`.
5. **`test_http_client_with_custom_ca_cert`** — Client builds with a valid PEM cert path.
6. **`test_http_client_with_invalid_cert_path`** — Returns error for nonexistent cert file.
7. **`test_http_client_with_invalid_pem`** — Returns error for non-PEM cert file.
8. **`test_http_client_with_tls_disabled`** — Client builds with `tls.enabled = false`.

### Unit Tests (`src/database.rs`)

9. **`test_set_tls_options`** — All five TLS options parse correctly via `set_option`.
10. **`test_get_tls_options`** — TLS options are readable via `get_option_string` / `get_option_int`.

### E2E Tests (`tests/tls_e2e.rs`)

E2E tests use mitmproxy as a TLS-intercepting proxy. The CI workflow (`.github/workflows/rust-e2e.yml`) starts a `mitmdump` container on port 8080, extracts its CA cert, and verifies traffic routing via mitmdump logs after tests complete.

11. **`test_tls_fails_without_config`** — Connection through mitmproxy fails without any TLS configuration (negative test — verifies TLS interception is detected).
12. **`test_tls_with_trusted_certificate`** — Connection succeeds when `trusted_certificate_path` points to mitmproxy's CA cert.
13. **`test_tls_with_allow_self_signed`** — Connection succeeds with `allow_self_signed = true` (no CA cert needed).

## Cross-Driver Comparison

| Feature | Rust (this design) | C# |
|---------|-------------------|-----|
| TLS toggle | `databricks.http.tls.enabled` | `adbc.http_options.tls.enabled` |
| Self-signed certs / skip cert validation | `databricks.http.tls.allow_self_signed` | `adbc.http_options.tls.allow_self_signed` + `adbc.http_options.tls.disable_server_certificate_validation` |
| Hostname mismatch | `databricks.http.tls.allow_hostname_mismatch` | `adbc.http_options.tls.allow_hostname_mismatch` |
| Custom CA cert | `databricks.http.tls.trusted_certificate_path` | `adbc.http_options.tls.trusted_certificate_path` |
| Cert revocation mode | Opt-in, Windows with tls-rustls only: `check_certificate_revocation`, `accept_undetermined_certificate_revocation` (default off; InvalidArgument elsewhere). Checked by Windows CryptoAPI. | `adbc.http_options.tls.revocation_mode` (ADBC mapping out of scope) |

### Naming Convention Difference

The Rust driver uses `databricks.http.tls.*` while the C# driver uses `adbc.http_options.tls.*`. This is consistent with the broader pattern: both drivers use different namespaces for driver-specific options (e.g., proxy is `databricks.http.proxy.*` in Rust vs `adbc.proxy_options.*` in C#). Neither namespace is part of the official ADBC specification — the `adbc.http_options.*` keys in C# are inherited from the HiveServer2 base class convention.

### Scope Exclusions

- **Certificate revocation is Windows-only**: the check exists only on Windows with `tls-rustls`, and is decided by Windows CryptoAPI (`CertGetCertificateChain`). On every other platform, and on `tls-native` (including Windows), requesting revocation fails session open with `InvalidArgument`, as an unsupported option does. The ODBC driver reads its CheckCertRevocation and AcceptUndeterminedRevocation keys only on Windows; elsewhere it warns and ignores them. The kernel does not port the check to Linux or macOS, and `tls-native` on Windows is not given a CryptoAPI path, because native-tls exposes no verifier hook and this keeps the surface smaller.
- **Trust stays with webpki**: the connection's webpki verifier, over the kernel's roots (webpki roots, the OS store, custom CAs), decides the chain, the name and the signatures, exactly as with revocation off. Only after that succeeds does CryptoAPI decide revocation. CryptoAPI builds its chain from the leaf, the presented intermediates and the custom CAs. It checks every certificate except the root. Only the chain-level revocation bits are read. The trust bits, including the root's, are ignored, so a custom CA that Windows does not trust is still checked, because the custom CAs are in CryptoAPI's additional store.
- **Telemetry cache limit**: the telemetry client cache key covers the host and the effective revocation mode only. Sessions to one host with different custom CA, self-signed, mTLS or proxy settings share a telemetry client, as before revocation was added. Per-session isolation is a follow-up.
- **Stapled OCSP**: a non-empty stapled OCSP response from the server is attached to the leaf certificate (`CERT_OCSP_RESPONSE_PROP_ID`, as SChannel does), so CryptoAPI can use it and skip a fetch for the leaf. rustls's client sends the `status_request` extension (OCSP) by default, so a server that staples gives the response to the check. An empty or refused response is ignored and CryptoAPI fetches as usual. There is no positive stapled-OCSP test yet: it needs an OCSP responder fixture, which is a follow-up.
- **Trust status**: a chain with an invalid signature on any element is undetermined, never good. Strict mode rejects it and accept-undetermined accepts it. A revoked status stays revoked.
- A trust anchor is not revocation-checked, including a self-signed server certificate configured as a custom CA, as in RFC 5280, the JDK PKIX path validator and CryptoAPI's exclude-root flag. The leaf and the other chain certificates are checked.
- **Chain choice**: CryptoAPI may build a different chain from the one webpki verified, for example through a cross-signed issuer. The status then comes from CryptoAPI's chain. The leaf is always checked. A server presenting more intermediates than it needs does not change the result. A presented certificate that CryptoAPI refuses to add is skipped, so it cannot hide the leaf's status; a chain that needs it comes out partial and is undetermined.
- **Strict and accept-undetermined**: a revoked status is rejected in both modes. An undetermined status (a fetch failure, an offline status, a missing revocation URL, or a partial chain) is rejected in strict mode and accepted under accept-undetermined. This matches Simba's semantics. A partial chain is a root that Windows cannot build to and that is not a custom CA, for example a public root that appears only in the webpki bundle. Under strict mode that connection fails.
- **CRL and OCSP fetching**: CryptoAPI retrieves CRLs and OCSP responses through Windows' networking stack (WinHTTP) and its proxy settings, not through the kernel's `ProxyConfig`. Windows caches the results, so a certificate revoked after a CRL was cached may be accepted until the cache refreshes. The kernel adds no CRL cache, worker thread, redirect policy or size limit of its own.
- **Timeout**: CryptoAPI applies one 10 s budget to the CRL and OCSP fetches of a chain (`CERT_CHAIN_REVOCATION_ACCUMULATIVE_TIMEOUT`). When the budget runs out, the status is undetermined. AIA and other chain-building fetches are bounded per URL (`dwUrlRetrievalTimeout`, also 10 s), not in total, so the 10 s total is not guaranteed for a chain that needs them. The kernel has no public knob for either bound.
- **Engine order and budget**: the default chain engine builds first, so a public chain that verifies costs one build. When custom CAs are configured and that build is undetermined, the chain is built again with an exclusive-root engine whose root store is the custom CAs. The second build gets only the time the first left (`dwUrlRetrievalTimeout` set to the remainder), and is skipped when none is left. One handshake therefore waits at most the 10 s budget for revocation fetches in total. The exclusive-root engine is used only for a second build, so public chains are not affected by its trust set.
- **Blocking**: the check is synchronous, because rustls' verifier is. It runs inside the handshake poll and blocks until CryptoAPI returns. On a multi-thread runtime, `block_in_place` moves the worker's other tasks to another thread, but the calling task cannot be cancelled until CryptoAPI returns. A caller's connect or operation timeout can be exceeded by up to the revocation budget, with revocation on only. On a `current_thread` runtime the whole runtime waits.
- **Session resumption**: resumption is disabled whenever revocation is on, because a resumed handshake skips the server certificate verifier, and a certificate revoked after the first handshake would stay accepted.
- **Documented limits**: the check covers the chain that CryptoAPI builds, not every path that webpki might accept. Windows caches revocation data. The check uses Windows' network stack, so proxy settings and firewall rules that the kernel does not control can make a fetch fail.
- **Client certificates (mTLS)**: Not needed per current requirements. Could be added later via `reqwest::ClientBuilder::identity()`.
- **TLS version pinning**: Not needed. reqwest/native-tls negotiate the highest supported version automatically.

## Files Changed

| File | Change |
|------|--------|
| `rust/src/client/http.rs` | Add `TlsConfig` struct, update `HttpClientConfig`, update `DatabricksHttpClient::new()`, add unit tests |
| `rust/src/database.rs` | Parse and get TLS options in `set_option`/`get_option_string`/`get_option_int` |
| `rust/Cargo.toml` | Add `native-tls` feature to reqwest |
| `rust/tests/tls_e2e.rs` | E2E TLS test cases (marked `#[ignore]`) |
| `.github/workflows/rust-e2e.yml` | Add TLS job with mitmproxy sidecar |
| `docs/designs/tls-config-design.md` | This design doc |
