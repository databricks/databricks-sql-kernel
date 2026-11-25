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

//! Retry logic for the Databricks HTTP client.
//!
//! Implements the standardized DBSQL connector retry specification with:
//! - Idempotency-aware retry strategies (idempotent vs non-idempotent)
//! - `Retry-After` header support with min/max clamping
//! - Exponential backoff with random jitter (50ms–750ms)
//! - Cumulative overall timeout
//! - Per-category configuration with global defaults

use rand::RngExt;
use reqwest::StatusCode;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Identifies the type of request being made.
///
/// The HTTP client uses this to look up the correct `RetryConfig` (from category)
/// and select the correct `RetryStrategy` (from idempotency). This is the single
/// source of truth for request classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestType {
    // SEA
    CreateSession,
    CloseSession,
    ExecuteStatement,
    ExecuteMetadataQuery,
    GetStatementStatus,
    CancelStatement,
    CloseStatement,
    GetResultChunks,
    // Telemetry
    TelemetryExport,
    // CloudFetch
    CloudFetchDownload,
    // Auth
    AuthTokenRequest,
    AuthDiscovery,
}

/// Category of request, used to look up per-category retry configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestCategory {
    Sea,
    CloudFetch,
    Auth,
    Telemetry,
}

/// Whether a request is safe to retry after a failure where the server
/// may have processed the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestIdempotency {
    Idempotent,
    NonIdempotent,
}

impl RequestType {
    /// Returns the category for retry config lookup.
    pub fn category(self) -> RequestCategory {
        match self {
            Self::CreateSession
            | Self::CloseSession
            | Self::ExecuteStatement
            | Self::ExecuteMetadataQuery
            | Self::GetStatementStatus
            | Self::CancelStatement
            | Self::CloseStatement
            | Self::GetResultChunks => RequestCategory::Sea,

            Self::TelemetryExport => RequestCategory::Telemetry,

            Self::CloudFetchDownload => RequestCategory::CloudFetch,

            Self::AuthTokenRequest | Self::AuthDiscovery => RequestCategory::Auth,
        }
    }

    /// Returns the idempotency classification for strategy selection.
    pub fn idempotency(self) -> RequestIdempotency {
        match self {
            Self::ExecuteStatement => RequestIdempotency::NonIdempotent,
            _ => RequestIdempotency::Idempotent,
        }
    }
}

/// Configuration for retry behavior. Each request category can have its own config,
/// with unset fields falling back to global defaults.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Minimum wait time per retry. Clamps both exponential backoff floor
    /// and Retry-After minimum.
    pub min_wait: Duration,

    /// Maximum wait time per retry. Clamps both exponential backoff ceiling
    /// and Retry-After maximum.
    pub max_wait: Duration,

    /// Cumulative wall-clock timeout for all retry attempts.
    /// Stops retrying when total elapsed time exceeds this.
    pub overall_timeout: Duration,

    /// Maximum number of retry attempts. A request is attempted at most
    /// `max_retries + 1` times (1 initial + N retries).
    pub max_retries: u32,

    /// Override the set of HTTP codes that are retryable.
    /// When set, this becomes the **exhaustive** set — only these codes are
    /// retried, regardless of the strategy's default logic.
    pub override_retryable_codes: Option<HashSet<u16>>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(60),
            overall_timeout: Duration::from_secs(900),
            max_retries: 5,
            override_retryable_codes: None,
        }
    }
}

/// Builds a per-category retry config map from global defaults and optional overrides.
pub fn build_retry_configs(
    global: &RetryConfig,
    overrides: &HashMap<RequestCategory, RetryConfigOverrides>,
) -> HashMap<RequestCategory, RetryConfig> {
    let categories = [
        RequestCategory::Sea,
        RequestCategory::CloudFetch,
        RequestCategory::Auth,
        RequestCategory::Telemetry,
    ];

    categories
        .into_iter()
        .map(|cat| {
            let config = if let Some(ovr) = overrides.get(&cat) {
                ovr.apply(global)
            } else {
                global.clone()
            };
            (cat, config)
        })
        .collect()
}

/// Per-category overrides. `None` fields inherit from the global config.
#[derive(Debug, Clone, Default)]
pub struct RetryConfigOverrides {
    pub min_wait: Option<Duration>,
    pub max_wait: Option<Duration>,
    pub overall_timeout: Option<Duration>,
    pub max_retries: Option<u32>,
    pub override_retryable_codes: Option<HashSet<u16>>,
}

impl RetryConfigOverrides {
    /// Apply overrides on top of a base config.
    pub fn apply(&self, base: &RetryConfig) -> RetryConfig {
        RetryConfig {
            min_wait: self.min_wait.unwrap_or(base.min_wait),
            max_wait: self.max_wait.unwrap_or(base.max_wait),
            overall_timeout: self.overall_timeout.unwrap_or(base.overall_timeout),
            max_retries: self.max_retries.unwrap_or(base.max_retries),
            override_retryable_codes: self
                .override_retryable_codes
                .clone()
                .or_else(|| base.override_retryable_codes.clone()),
        }
    }
}

// --- Retry Strategies ---

/// Non-retryable HTTP status codes for idempotent requests.
const IDEMPOTENT_NON_RETRYABLE: &[u16] = &[
    400, 401, 403, 404, 405, 409, 410, 411, 412, 413, 414, 415, 416,
];

/// Whether `status` is a rejection that idempotent requests never retry,
/// ignoring any per-session `override_retryable_codes`.
pub(crate) fn is_non_retryable_status(status: u16) -> bool {
    IDEMPOTENT_NON_RETRYABLE.contains(&status)
}

/// Determines whether a request should be retried based on the error type
/// and the request's idempotency classification.
pub struct RetryStrategy {
    idempotency: RequestIdempotency,
    override_codes: Option<HashSet<u16>>,
}

impl RetryStrategy {
    /// Create a strategy for the given request type and config.
    pub fn for_request(request_type: RequestType, config: &RetryConfig) -> Self {
        Self {
            idempotency: request_type.idempotency(),
            override_codes: config.override_retryable_codes.clone(),
        }
    }

    /// Whether the given HTTP status code is retryable.
    ///
    /// For non-idempotent requests, 429/503 are only retryable when the server
    /// sends a `Retry-After` header — this is the explicit signal that it is
    /// safe to retry (per Mehmet's review feedback on the spec).
    pub fn is_retryable_status(&self, status: StatusCode, has_retry_after: bool) -> bool {
        let code = status.as_u16();

        // Success codes are never retryable (the caller checks success first,
        // but guard here for safety if the method is called directly)
        if status.is_success() || status.is_redirection() {
            return false;
        }

        if let Some(ref codes) = self.override_codes {
            return codes.contains(&code);
        }

        match self.idempotency {
            // Idempotent: retry everything except known non-retryable codes
            RequestIdempotency::Idempotent => !is_non_retryable_status(code),
            // Non-idempotent:
            // - 429: always retryable (rate-limited = request was not executed)
            // - 503: only retryable with Retry-After header (ambiguous whether
            //   processing started; require explicit server signal)
            RequestIdempotency::NonIdempotent => code == 429 || (code == 503 && has_retry_after),
        }
    }

    /// Whether the given network/client error is retryable.
    pub fn is_retryable_error(&self, error: &reqwest::Error) -> bool {
        // Certificate verification and permanent DNS failures cannot recover
        // through retry, even though reqwest reports them as connect errors.
        if is_certificate_verification_failure(error) || is_permanent_dns_resolution_failure(error)
        {
            return false;
        }
        match self.idempotency {
            // Idempotent: all network errors are retryable
            RequestIdempotency::Idempotent => {
                error.is_timeout() || error.is_connect() || error.is_request()
            }
            // Non-idempotent: only connection-level errors (request never reached server)
            RequestIdempotency::NonIdempotent => error.is_connect(),
        }
    }
}

pub(crate) fn is_certificate_verification_failure(
    error: &(dyn std::error::Error + 'static),
) -> bool {
    super::error_chain(error).any(|error| {
        #[cfg(feature = "tls-rustls")]
        if matches!(
            error.downcast_ref::<rustls::Error>(),
            Some(rustls::Error::InvalidCertificate(_) | rustls::Error::NoCertificatesPresented)
        ) {
            return true;
        }

        #[cfg(feature = "tls-native")]
        if let Some(error) = error.downcast_ref::<native_tls::Error>() {
            #[cfg(target_vendor = "apple")]
            if let Some(permanent) = apple_certificate_verification_failure(&format!("{error:?}")) {
                return permanent;
            }
            // Restrict platform-message matching to TLS errors so ordinary
            // connect/timeout errors still retry.
            return is_native_certificate_verification_failure(&error.to_string());
        }
        false
    })
}

#[cfg(all(feature = "tls-native", any(target_vendor = "apple", test)))]
fn apple_certificate_verification_failure(debug: &str) -> Option<bool> {
    // native-tls forwards security-framework's Debug, which retains OSStatus
    // even when Display contains only a localized message.
    let code = debug
        .strip_prefix("Error { code: ")?
        .split_once([',', '}'])?
        .0
        .trim()
        .parse::<i32>()
        .ok()?;
    Some(matches!(
        code,
        -9807 // errSSLXCertChainInvalid
            | -9808 // errSSLBadCert
            | -9812 // errSSLUnknownRootCert
            | -9813 // errSSLNoRootCert
            | -9814 // errSSLCertExpired
            | -9815 // errSSLCertNotYetValid
            | -9843 // errSSLHostNameMismatch
            | -25318 // errSecCreateChainFailed
            | -67602 // errSecHostNameMismatch
            | -67609 // errSecInvalidExtendedKeyUsage
            | -67654 // errSecTrustSettingDeny
            | -67808 // errSecVerifyFailed
            | -67818 // errSecCertificateExpired
            | -67819 // errSecCertificateNotValidYet
            | -67820 // errSecCertificateRevoked
            | -67843 // errSecNotTrusted
    ))
}

#[cfg(feature = "tls-native")]
fn is_native_certificate_verification_failure(message: &str) -> bool {
    // native-tls hides SChannel's io::Error, but Display retains the OS code
    // even when Windows localizes the message.
    if let Some(code) = message
        .rsplit_once(" (os error ")
        .and_then(|(_, code)| code.strip_suffix(')'))
        .and_then(|code| code.parse::<i32>().ok())
    {
        // Revocation-check availability errors remain retryable.
        return matches!(
            code as u32,
            0x80090322 // SEC_E_WRONG_PRINCIPAL
                | 0x80090325 // SEC_E_UNTRUSTED_ROOT
                | 0x80090328 // SEC_E_CERT_EXPIRED
                | 0x80092010 // CRYPT_E_REVOKED
                | 0x80096004 // TRUST_E_CERT_SIGNATURE
                | 0x80096019 // TRUST_E_BASIC_CONSTRAINTS
                | 0x800B0101 // CERT_E_EXPIRED
                | 0x800B0102 // CERT_E_VALIDITYPERIODNESTING
                | 0x800B0103 // CERT_E_ROLE
                | 0x800B0104 // CERT_E_PATHLENCONST
                | 0x800B0105 // CERT_E_CRITICAL
                | 0x800B0106 // CERT_E_PURPOSE
                | 0x800B0107 // CERT_E_ISSUERCHAINING
                | 0x800B0108 // CERT_E_MALFORMED
                | 0x800B0109 // CERT_E_UNTRUSTEDROOT
                | 0x800B010A // CERT_E_CHAINING
                | 0x800B010C // CERT_E_REVOKED
                | 0x800B010D // CERT_E_UNTRUSTEDTESTROOT
                | 0x800B010F // CERT_E_CN_NO_MATCH
                | 0x800B0110 // CERT_E_WRONG_USAGE
                | 0x800B0111 // TRUST_E_EXPLICIT_DISTRUST
                | 0x800B0112 // CERT_E_UNTRUSTEDCA
                | 0x800B0113 // CERT_E_INVALID_POLICY
                | 0x800B0114 // CERT_E_INVALID_NAME
        );
    }

    // OpenSSL's X509VerifyResult is hidden; this fallback depends on English
    // messages if no platform status code is available.
    // Match local verification failures, not peer-sent certificate alerts.
    let message = message.to_ascii_lowercase();
    [
        "certificate verify failed",
        "hostname mismatch",
        "host name mismatch",
        "ip address mismatch",
        "invalid certificate chain",
        "certificate for this server is invalid",
        "unknown root certificate",
        "root certificate is not trusted",
        "certificate was not trusted",
        "certificate has expired",
        "certificate is not yet valid",
        "certificate chain was issued by an authority that is not trusted",
        "root certificate which is not trusted",
        "untrusted root",
        "target principal name is incorrect",
        "certificate's cn name does not match",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

/// Heuristic: is this network error a permanent DNS lookup failure (the host
/// name does not exist), as opposed to a temporary resolver/connect error?
///
/// A non-existent host (NXDOMAIN) will never resolve, so it must not be
/// retried. reqwest does not expose a typed DNS-error variant, so we walk the
/// error's source chain and match only messages that identify an unknown host.
/// Generic markers such as `dns error` and `name resolution` are deliberately
/// excluded because they also describe temporary resolver failures.
fn is_permanent_dns_resolution_failure(error: &reqwest::Error) -> bool {
    error_chain_indicates_permanent_dns_failure(error)
}

/// Walk an error's `source()` chain looking for a permanent DNS marker.
///
/// Split out from [`is_permanent_dns_resolution_failure`] so it can be tested
/// with a synthetic error chain; `reqwest::Error` has no public constructor.
fn error_chain_indicates_permanent_dns_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = source {
        let msg = err.to_string().to_ascii_lowercase();
        if msg.contains("name or service not known")
            || msg.contains("nodename nor servname")
            || msg.contains("no such host")
            || msg.contains("non-existent domain")
            || msg.contains("nxdomain")
        {
            return true;
        }
        source = err.source();
    }
    false
}

// --- Backoff Calculator ---

/// Calculate the backoff duration for a retry attempt.
///
/// Honors `Retry-After` header if present, otherwise uses exponential backoff.
/// Both are clamped to `[min_wait, max_wait]` and proportional jitter (5–25% of
/// base wait, minimum 50ms) is added. Proportional jitter scales with the backoff
/// so it remains meaningful at higher retry intervals.
pub fn calculate_backoff(
    config: &RetryConfig,
    attempt: u32,
    retry_after_header: Option<&str>,
) -> Duration {
    // Effective ceiling: never below the floor. A misconfigured
    // `max_wait < min_wait` (a tuning typo, e.g. via `Session::builder().http()`)
    // would otherwise make `Duration::clamp` panic (`Ord::clamp` asserts
    // `min <= max`) on the first retry. Correct the pair by raising max to min
    // rather than aborting the caller's task; the one-time warning is emitted
    // where the per-session `RetryConfig` is built (see `session.rs`), not on
    // this per-retry hot path.
    let max_wait = config.max_wait.max(config.min_wait);

    let base_wait = if let Some(wait) = retry_after_header.and_then(parse_retry_after) {
        // Clamp Retry-After to [min_wait, max_wait]
        wait.clamp(config.min_wait, max_wait)
    } else {
        // Exponential backoff: min_wait * 2^attempt (per spec)
        let exp = config.min_wait.saturating_mul(2u32.saturating_pow(attempt));
        exp.min(max_wait)
    };

    // Proportional jitter: random 5-25% of base_wait, minimum 50ms.
    // Scales with backoff so retries remain spread at higher intervals.
    let base_ms = base_wait.as_millis() as u64;
    let jitter_lo = (base_ms * 5 / 100).max(50);
    let jitter_hi = (base_ms * 25 / 100).max(jitter_lo + 1);
    let jitter_ms = rand::rng().random_range(jitter_lo..=jitter_hi);
    base_wait + Duration::from_millis(jitter_ms)
}

/// Parse the `Retry-After` header value.
///
/// Supports:
/// - Seconds: `Retry-After: 120` → 120 seconds
/// - HTTP-date: `Retry-After: Wed, 25 Mar 2026 10:30:00 GMT` → delta from now
fn parse_retry_after(value: &str) -> Option<Duration> {
    // Try seconds first
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    // Try HTTP-date (RFC 7231 / RFC 2822)
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(value.trim()) {
        let now = chrono::Utc::now();
        let target = date.with_timezone(&chrono::Utc);
        if target > now {
            let delta = target - now;
            return Some(Duration::from_secs(delta.num_seconds() as u64));
        }
        // Date is in the past — treat as 0
        return Some(Duration::ZERO);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn certificate_errors_are_detected_inside_io_errors() {
        for error in [
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
            rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName),
            rustls::Error::InvalidCertificate(rustls::CertificateError::Expired),
            // What the Windows revocation verifier returns for a revoked or, in strict mode,
            // undetermined status: both are permanent and never retried.
            rustls::Error::InvalidCertificate(rustls::CertificateError::Revoked),
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownRevocationStatus),
            rustls::Error::NoCertificatesPresented,
        ] {
            let error = std::io::Error::other(std::io::Error::other(error));
            assert!(is_certificate_verification_failure(&error));
        }
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn peer_certificate_alerts_are_not_local_verification_failures() {
        use rustls::AlertDescription::*;
        for alert in [
            BadCertificate,
            UnsupportedCertificate,
            CertificateRevoked,
            CertificateExpired,
            CertificateUnknown,
            UnknownCA,
            CertificateUnobtainable,
            BadCertificateStatusResponse,
            BadCertificateHashValue,
            CertificateRequired,
        ] {
            let error = std::io::Error::other(rustls::Error::AlertReceived(alert));
            assert!(!is_certificate_verification_failure(&error), "{alert:?}");
        }
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn apple_certificate_status_codes_are_permanent() {
        for debug in [
            "Error { code: -67602, message: \"A host name mismatch has occurred.\" }",
            "Error { code: -67602, message: \"Der Hostname stimmt nicht überein.\" }",
            "Error { code: -67843, message: \"Certificat non approuvé.\" }",
            "Error { code: -67818, message: \"Zertifikat abgelaufen.\" }",
            "Error { code: -67819 }",
            "Error { code: -67820 }",
            "Error { code: -67609 }",
            "Error { code: -67654 }",
            "Error { code: -67808 }",
            "Error { code: -25318 }",
            "Error { code: -9807 }",
            "Error { code: -9808 }",
            "Error { code: -9812 }",
            "Error { code: -9813 }",
            "Error { code: -9814 }",
            "Error { code: -9815 }",
            "Error { code: -9843 }",
        ] {
            assert_eq!(
                apple_certificate_verification_failure(debug),
                Some(true),
                "{debug}"
            );
        }
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn apple_peer_alerts_and_other_errors_are_not_local_verification_failures() {
        for debug in [
            "Error { code: -9825, message: \"Peer bad certificate\" }",
            "Error { code: -9827 }", // errSSLPeerCertRevoked
            "Error { code: -9828 }", // errSSLPeerCertExpired
            "Error { code: -9829 }", // errSSLPeerCertUnknown
            "Error { code: -9831 }", // errSSLPeerUnknownCA
            "Error { code: -9803 }", // errSSLWouldBlock
            "Error { code: -9844 }", // errSSLConnectionRefused
            "Error { code: -36 }",   // errSecIO
            "Error { code: -50, message: \"bad parameter\" }",
        ] {
            assert_eq!(
                apple_certificate_verification_failure(debug),
                Some(false),
                "{debug}"
            );
        }
        for debug in [
            "A host name mismatch has occurred.",
            "Error { message: \"code: -67602\" }",
            "Error { code: invalid }",
            "Error { code: -67602",
            "Os { code: -67602, kind: Other }",
        ] {
            assert_eq!(
                apple_certificate_verification_failure(debug),
                None,
                "{debug}"
            );
        }
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn native_certificate_verification_messages_are_permanent() {
        for message in [
            // OpenSSL.
            "error:0A000086:SSL routines:tls_post_process_server_certificate:certificate verify failed (unable to get local issuer certificate)",
            "certificate verify failed (self-signed certificate)",
            "certificate verify failed (hostname mismatch)",
            "certificate verify failed (IP address mismatch)",
            // Secure Transport.
            "Invalid certificate chain",
            "The certificate for this server is invalid.",
            "Unknown root certificate",
            "The root certificate is not trusted.",
            "The certificate was not trusted.",
            "The certificate has expired.",
            "The certificate is not yet valid.",
            "A host name mismatch has occurred.",
            // SChannel.
            "The certificate chain was issued by an authority that is not trusted. (os error -2146893019)",
            "A certificate chain processed, but terminated in a root certificate which is not trusted by the trust provider. (os error -2146762487)",
            "Untrusted root certificate",
            "The target principal name is incorrect. (os error -2146893022)",
            "The certificate's CN name does not match the passed value. (os error -2146762481)",
            "CERTIFICATE VERIFY FAILED",
        ] {
            assert!(is_native_certificate_verification_failure(message), "{message}");
        }
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn native_peer_alerts_and_other_tls_failures_are_not_local_verification_failures() {
        for message in [
            "sslv3 alert bad certificate",
            "sslv3 alert unsupported certificate",
            "sslv3 alert certificate revoked",
            "sslv3 alert certificate expired",
            "sslv3 alert certificate unknown",
            "tlsv1 alert unknown ca",
            "tlsv1 alert certificate unobtainable",
            "tlsv1 alert bad certificate status response",
            "tlsv1 alert bad certificate hash value",
            "tlsv13 alert certificate required",
            "unexpected eof while reading",
            "wrong version number",
            "SSL routines:tls_process_server_certificate:unexpected eof while reading",
            "Connection reset by peer",
            "TLS-Fehler (os error -2146893018)", // SEC_E_ILLEGAL_MESSAGE
            "TLS-Fehler (os error -2146893048)", // SEC_E_INVALID_TOKEN
            "Verbindung abgelehnt (os error 10061)",
            "Zeitüberschreitung (os error 10060)",
            "The revocation function was unable to check revocation for the certificate. (os error -2146762482)",
            "The revocation function was unable to check revocation because the revocation server was offline. (os error -2146885613)",
            "The certificate's revocation status could not be determined. (os error -2146892973)",
        ] {
            assert!(
                !is_native_certificate_verification_failure(message),
                "{message}"
            );
        }
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn schannel_certificate_policy_failures_are_permanent() {
        for message in [
            "The certificate is not valid for the requested usage. (os error -2146762480)",
            "A certificate was explicitly revoked by its issuer. (os error -2146762484)",
            "The signature of the certificate cannot be verified. (os error -2146869244)",
            "The certificate is revoked. (os error -2146885616)",
            "The basic constraints of the certificate are invalid or missing. (os error -2146869223)",
            "Das Zertifikat wurde ausdrücklich als nicht vertrauenswürdig markiert. (os error -2146762479)",
            "Une extension critique du certificat est inconnue. (os error -2146762491)",
            "The validity periods of the certification chain do not nest correctly. (os error -2146762494)",
            "A certificate that can only be used as an end-entity is being used as a CA or vice versa. (os error -2146762493)",
            "A path length constraint in the certification chain has been violated. (os error -2146762492)",
            "A certificate is being used for a purpose other than the ones specified by its CA. (os error -2146762490)",
            "A parent of a given certificate did not issue that child certificate. (os error -2146762489)",
            "A certificate is missing or has an empty value for an important field. (os error -2146762488)",
            "The certification path terminates with the test root which is not trusted. (os error -2146762483)",
            "The certificate has invalid policy. (os error -2146762477)",
            "The certificate has an invalid name. (os error -2146762476)",
        ] {
            assert!(is_native_certificate_verification_failure(message), "{message}");
        }
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn localized_schannel_certificate_failures_are_permanent() {
        for message in [
            "Die Zertifikatskette wurde von einer nicht vertrauenswürdigen Zertifizierungsstelle ausgestellt. (os error -2146893019)",
            "Zertifikatsfehler (os error -2146762487)",
            "Der Zielprinzipalname ist falsch. (os error -2146893022)",
            "Le nom CN du certificat ne correspond pas à la valeur passée. (os error -2146762481)",
            "Zertifikat abgelaufen (os error -2146893016)",
            "Certificat expiré (os error -2146762495)",
            "Zertifikatskette unvollständig (os error -2146762486)",
            "Autorité de certification non approuvée (os error -2146762478)",
        ] {
            assert!(is_native_certificate_verification_failure(message), "{message}");
        }
    }

    #[test]
    fn ordinary_io_errors_are_not_certificate_failures() {
        for error in [
            std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
            std::io::Error::from(std::io::ErrorKind::TimedOut),
            std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
            std::io::Error::other("certificate verify failed"),
            std::io::Error::from_raw_os_error(-2146893019),
        ] {
            assert!(!is_certificate_verification_failure(&error));
        }
    }

    // --- DNS-resolution detection tests ---

    #[test]
    fn permanent_dns_marker_detected_for_unknown_host_phrasings() {
        use std::io::Error as IoError;
        for msg in [
            "failed to lookup address information: Name or service not known",
            "nodename nor servname provided, or not known",
            "No such host is known. (os error 11001)",
            "dns error: NXDOMAIN",
            "non-existent domain",
        ] {
            let err = IoError::other(msg);
            assert!(
                error_chain_indicates_permanent_dns_failure(&err),
                "expected permanent DNS marker in: {msg}"
            );
        }
    }

    #[test]
    fn temporary_dns_failures_are_not_classified_as_permanent() {
        use std::io::Error as IoError;
        for msg in [
            "dns error: temporary failure in name resolution",
            "failed to lookup address information: Try again",
            "temporary failure in name resolution",
            "server failure",
        ] {
            let err = IoError::other(msg);
            assert!(
                !error_chain_indicates_permanent_dns_failure(&err),
                "temporary DNS failure must not be classified as permanent: {msg}"
            );
        }
    }

    #[test]
    fn non_dns_connect_errors_are_not_flagged() {
        use std::io::{Error as IoError, ErrorKind};
        let refused = IoError::new(
            ErrorKind::ConnectionRefused,
            "Connection refused (os error 111)",
        );
        assert!(!error_chain_indicates_permanent_dns_failure(&refused));
        let reset = IoError::new(ErrorKind::ConnectionReset, "connection reset by peer");
        assert!(!error_chain_indicates_permanent_dns_failure(&reset));
    }

    #[test]
    fn dns_marker_detected_through_nested_source() {
        #[derive(Debug)]
        struct Wrapper(std::io::Error);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // Top-level message has no DNS marker — the marker is only on
                // the inner source, mirroring the reqwest → connector chain.
                write!(f, "error sending request for url (https://bogus.invalid/)")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let inner = std::io::Error::other("dns error: NXDOMAIN");
        assert!(error_chain_indicates_permanent_dns_failure(&Wrapper(inner)));
    }

    // --- RequestType mapping tests ---

    #[test]
    fn test_request_type_category_mapping() {
        assert_eq!(RequestType::CreateSession.category(), RequestCategory::Sea);
        assert_eq!(RequestType::CloseSession.category(), RequestCategory::Sea);
        assert_eq!(
            RequestType::ExecuteStatement.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::ExecuteMetadataQuery.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::GetStatementStatus.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::CancelStatement.category(),
            RequestCategory::Sea
        );
        assert_eq!(RequestType::CloseStatement.category(), RequestCategory::Sea);
        assert_eq!(
            RequestType::GetResultChunks.category(),
            RequestCategory::Sea
        );
        assert_eq!(
            RequestType::CloudFetchDownload.category(),
            RequestCategory::CloudFetch
        );
        assert_eq!(
            RequestType::AuthTokenRequest.category(),
            RequestCategory::Auth
        );
        assert_eq!(RequestType::AuthDiscovery.category(), RequestCategory::Auth);
        assert_eq!(
            RequestType::TelemetryExport.category(),
            RequestCategory::Telemetry
        );
    }

    #[test]
    fn test_request_type_idempotency_mapping() {
        // Only ExecuteStatement is non-idempotent
        assert_eq!(
            RequestType::ExecuteStatement.idempotency(),
            RequestIdempotency::NonIdempotent
        );

        // Everything else is idempotent
        let idempotent_types = [
            RequestType::CreateSession,
            RequestType::CloseSession,
            RequestType::ExecuteMetadataQuery,
            RequestType::GetStatementStatus,
            RequestType::CancelStatement,
            RequestType::CloseStatement,
            RequestType::GetResultChunks,
            RequestType::TelemetryExport,
            RequestType::CloudFetchDownload,
            RequestType::AuthTokenRequest,
            RequestType::AuthDiscovery,
        ];
        for rt in idempotent_types {
            assert_eq!(
                rt.idempotency(),
                RequestIdempotency::Idempotent,
                "{:?} should be idempotent",
                rt
            );
        }
    }

    #[test]
    fn test_execute_statement_is_non_idempotent() {
        assert_eq!(
            RequestType::ExecuteStatement.idempotency(),
            RequestIdempotency::NonIdempotent
        );
    }

    #[test]
    fn test_execute_metadata_query_is_idempotent() {
        assert_eq!(
            RequestType::ExecuteMetadataQuery.idempotency(),
            RequestIdempotency::Idempotent
        );
    }

    // --- Idempotent strategy tests ---

    #[test]
    fn test_idempotent_strategy_retries_5xx() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        // Idempotent: Retry-After presence doesn't matter
        assert!(strategy.is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR, false));
        assert!(strategy.is_retryable_status(StatusCode::BAD_GATEWAY, false));
        assert!(strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, false));
        assert!(strategy.is_retryable_status(StatusCode::GATEWAY_TIMEOUT, false));
    }

    #[test]
    fn test_idempotent_strategy_retries_429() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        assert!(strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, false));
    }

    #[test]
    fn test_idempotent_strategy_no_retry_on_client_errors() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        assert!(!strategy.is_retryable_status(StatusCode::BAD_REQUEST, false));
        assert!(!strategy.is_retryable_status(StatusCode::UNAUTHORIZED, false));
        assert!(!strategy.is_retryable_status(StatusCode::FORBIDDEN, false));
        assert!(!strategy.is_retryable_status(StatusCode::NOT_FOUND, false));
        assert!(!strategy.is_retryable_status(StatusCode::METHOD_NOT_ALLOWED, false));
        assert!(!strategy.is_retryable_status(StatusCode::CONFLICT, false));
        assert!(!strategy.is_retryable_status(StatusCode::GONE, false));
    }

    #[test]
    fn test_idempotent_strategy_does_not_retry_success() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: None,
        };
        // 2xx and 3xx codes are never retryable
        assert!(!strategy.is_retryable_status(StatusCode::OK, false));
        assert!(!strategy.is_retryable_status(StatusCode::CREATED, false));
        assert!(!strategy.is_retryable_status(StatusCode::MOVED_PERMANENTLY, false));
    }

    #[test]
    fn test_idempotent_strategy_override_codes() {
        let mut override_codes = HashSet::new();
        override_codes.insert(503);
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::Idempotent,
            override_codes: Some(override_codes),
        };
        // Only 503 is retryable now
        assert!(strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, false));
        assert!(!strategy.is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR, false));
        assert!(!strategy.is_retryable_status(StatusCode::BAD_GATEWAY, false));
        assert!(!strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, false));
    }

    // --- Non-idempotent strategy tests ---

    #[test]
    fn test_non_idempotent_strategy_429_always_retryable() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        // 429 is always retryable — rate-limited means the request was not executed
        assert!(strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, true));
        assert!(strategy.is_retryable_status(StatusCode::TOO_MANY_REQUESTS, false));
    }

    #[test]
    fn test_non_idempotent_strategy_503_requires_retry_after() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        // 503 with Retry-After: retryable (server explicitly signals retry)
        assert!(strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, true));
        // 503 without Retry-After: NOT retryable (ambiguous whether processing started)
        assert!(!strategy.is_retryable_status(StatusCode::SERVICE_UNAVAILABLE, false));
    }

    #[test]
    fn test_non_idempotent_strategy_no_retry_on_5xx() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        // Even with Retry-After, 500/502/504 are not in the retryable set
        assert!(!strategy.is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR, true));
        assert!(!strategy.is_retryable_status(StatusCode::BAD_GATEWAY, true));
        assert!(!strategy.is_retryable_status(StatusCode::GATEWAY_TIMEOUT, true));
    }

    #[test]
    fn test_non_idempotent_strategy_no_retry_on_client_errors() {
        let strategy = RetryStrategy {
            idempotency: RequestIdempotency::NonIdempotent,
            override_codes: None,
        };
        assert!(!strategy.is_retryable_status(StatusCode::BAD_REQUEST, true));
        assert!(!strategy.is_retryable_status(StatusCode::UNAUTHORIZED, true));
        assert!(!strategy.is_retryable_status(StatusCode::FORBIDDEN, true));
        assert!(!strategy.is_retryable_status(StatusCode::NOT_FOUND, true));
    }

    // --- Backoff calculation tests ---

    #[test]
    fn test_exponential_backoff_increases() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        // Per spec: exp_backoff = 2^attempt * min_wait
        // Proportional jitter: 5-25% of base
        // Attempt 1: 2s base + 100-500ms = 2100-2500ms
        // Attempt 2: 4s base + 200-1000ms = 4200-5000ms
        // Attempt 3: 8s base + 400-2000ms = 8400-10000ms
        let b1 = calculate_backoff(&config, 1, None);
        let b2 = calculate_backoff(&config, 2, None);
        let b3 = calculate_backoff(&config, 3, None);
        assert!(b1 >= Duration::from_millis(2100));
        assert!(b1 <= Duration::from_millis(2500));
        assert!(b2 >= Duration::from_millis(4200));
        assert!(b2 <= Duration::from_millis(5000));
        assert!(b3 >= Duration::from_millis(8400));
        assert!(b3 <= Duration::from_millis(10000));
    }

    #[test]
    fn test_backoff_capped_at_max_wait() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(10),
            ..Default::default()
        };
        // Attempt 10: 2^10 = 1024s, capped to 10s + 25% jitter = 12500ms max
        let backoff = calculate_backoff(&config, 10, None);
        assert!(backoff <= Duration::from_millis(12_500));
    }

    #[test]
    fn test_backoff_respects_min_wait() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(2),
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        // Attempt 1: min_wait * 2^1 = 4s + 5% jitter min = 4200ms
        let backoff = calculate_backoff(&config, 1, None);
        assert!(backoff >= Duration::from_millis(4200));
    }

    #[test]
    fn test_retry_after_header_seconds() {
        let config = RetryConfig::default();
        let backoff = calculate_backoff(&config, 1, Some("5"));
        // 5s clamped to [1s, 60s] = 5s + 5-25% jitter = 5250-6250ms
        assert!(backoff >= Duration::from_millis(5250));
        assert!(backoff <= Duration::from_millis(6250));
    }

    #[test]
    fn test_retry_after_clamped_to_min_max() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(3),
            max_wait: Duration::from_secs(10),
            ..Default::default()
        };
        // Retry-After: 1 should be clamped up to min_wait (3s) + 5% = 3150ms
        let backoff = calculate_backoff(&config, 1, Some("1"));
        assert!(backoff >= Duration::from_millis(3150));

        // Retry-After: 120 should be clamped down to max_wait (10s) + 25% = 12500ms
        let backoff = calculate_backoff(&config, 1, Some("120"));
        assert!(backoff <= Duration::from_millis(12_500));
    }

    #[test]
    fn test_calculate_backoff_max_below_min_does_not_panic() {
        // ERRORRECOV-014: a tuning typo where max_wait < min_wait must be
        // CORRECTED, not fatal. `Duration`'s `Ord::clamp` panics
        // (`assertion failed: min <= max`) when min > max, so the backoff must
        // treat the effective ceiling as `max_wait.max(min_wait)` — i.e. raise
        // max up to min. The bound stays a tuning hint; a typo must never abort
        // the caller's task the first time a transient 503 fires.
        let config = RetryConfig {
            min_wait: Duration::from_secs(3),
            max_wait: Duration::from_secs(1), // deliberately below min
            ..Default::default()
        };

        // Retry-After path: 5s clamps to [3s, effective-max=3s] = 3s + jitter.
        let backoff = calculate_backoff(&config, 1, Some("5"));
        assert!(
            backoff >= Duration::from_secs(3),
            "Retry-After backoff {backoff:?} should be >= min_wait (3s)"
        );

        // Exponential path: base 3s*2 capped to effective-max 3s + jitter.
        let backoff = calculate_backoff(&config, 1, None);
        assert!(
            backoff >= Duration::from_secs(3),
            "exponential backoff {backoff:?} should be >= min_wait (3s)"
        );
    }

    #[test]
    fn test_retry_after_invalid_falls_back_to_exponential() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            ..Default::default()
        };
        // Invalid Retry-After should fall back to exponential: 2s + 5-25% = 2100-2500ms
        let backoff = calculate_backoff(&config, 1, Some("not-a-number"));
        assert!(backoff >= Duration::from_millis(2100));
        assert!(backoff <= Duration::from_millis(2500));
    }

    #[test]
    fn test_proportional_jitter() {
        let config = RetryConfig {
            min_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        // Attempt 1: 2s base → jitter 5-25% = 100-500ms → total 2100-2500ms
        for _ in 0..100 {
            let backoff = calculate_backoff(&config, 1, None);
            assert!(backoff >= Duration::from_millis(2100));
            assert!(backoff <= Duration::from_millis(2500));
        }

        // Attempt 5: 32s base → jitter 5-25% = 1600-8000ms → total 33600-40000ms
        // Jitter scales significantly at higher backoffs
        for _ in 0..100 {
            let backoff = calculate_backoff(&config, 5, None);
            assert!(backoff >= Duration::from_millis(33600));
            assert!(backoff <= Duration::from_millis(40000));
        }
    }

    #[test]
    fn test_jitter_minimum_50ms() {
        // With a very small base wait, jitter floor should be 50ms
        let config = RetryConfig {
            min_wait: Duration::from_millis(10),
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        // Attempt 1: 20ms base → 5% = 1ms, but clamped to 50ms min
        for _ in 0..100 {
            let backoff = calculate_backoff(&config, 1, None);
            assert!(backoff >= Duration::from_millis(70)); // 20ms + 50ms
        }
    }

    // --- RetryConfig tests ---

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.min_wait, Duration::from_secs(1));
        assert_eq!(config.max_wait, Duration::from_secs(60));
        assert_eq!(config.overall_timeout, Duration::from_secs(900));
        assert_eq!(config.max_retries, 5);
        assert!(config.override_retryable_codes.is_none());
    }

    #[test]
    fn test_category_config_overrides_global() {
        let global = RetryConfig::default();
        let mut overrides = HashMap::new();
        overrides.insert(
            RequestCategory::Auth,
            RetryConfigOverrides {
                max_retries: Some(3),
                overall_timeout: Some(Duration::from_secs(30)),
                ..Default::default()
            },
        );

        let configs = build_retry_configs(&global, &overrides);

        // Auth should have overridden values
        let auth = &configs[&RequestCategory::Auth];
        assert_eq!(auth.max_retries, 3);
        assert_eq!(auth.overall_timeout, Duration::from_secs(30));
        // But inherit min_wait/max_wait from global
        assert_eq!(auth.min_wait, Duration::from_secs(1));
        assert_eq!(auth.max_wait, Duration::from_secs(60));

        // SEA should have global defaults
        let sea = &configs[&RequestCategory::Sea];
        assert_eq!(sea.max_retries, 5);
        assert_eq!(sea.overall_timeout, Duration::from_secs(900));
    }

    // --- parse_retry_after tests ---

    #[test]
    fn test_parse_retry_after_seconds() {
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("0"), Some(Duration::from_secs(0)));
        assert_eq!(parse_retry_after(" 5 "), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_parse_retry_after_http_date() {
        // Use a future date relative to test execution
        let future = chrono::Utc::now() + chrono::Duration::seconds(30);
        let formatted = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let result = parse_retry_after(&formatted);
        assert!(result.is_some());
        let duration = result.unwrap();
        // Allow some tolerance for test execution time
        assert!(
            duration.as_secs() >= 28 && duration.as_secs() <= 32,
            "Expected ~30s, got {:?}",
            duration
        );
    }

    #[test]
    fn test_parse_retry_after_past_date() {
        // A date in the past should return Duration::ZERO
        let past = chrono::Utc::now() - chrono::Duration::seconds(60);
        let formatted = past.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let result = parse_retry_after(&formatted);
        assert_eq!(result, Some(Duration::ZERO));
    }

    #[test]
    fn test_parse_retry_after_invalid() {
        assert_eq!(parse_retry_after("not-a-number"), None);
        assert_eq!(parse_retry_after(""), None);
    }
}
