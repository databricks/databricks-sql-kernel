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

//! Minimal JWT claim inspection for token federation.
//!
//! Token federation needs to read the `iss` (issuer) claim of an access
//! token to decide whether it was minted by the target Databricks workspace
//! (pass through) or by a foreign identity provider (exchange it for an
//! in-house token). See [`crate::auth::oauth::federation`].
//!
//! This module intentionally does **not** verify the JWT signature. The
//! workspace validates the token on the exchange call; here we only peek at
//! the payload to route the decision. Doing our own verification would
//! require the issuer's signing keys and add a JWT dependency for no benefit.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::Deserialize;

/// The subset of JWT payload claims federation cares about.
#[derive(Debug, Deserialize)]
struct Claims {
    /// Issuer — the party that minted the token, e.g.
    /// `https://<workspace-host>/oidc` for an in-house Databricks token.
    iss: Option<String>,
}

/// Decodes the payload of a JWT without verifying its signature and returns
/// the `iss` claim.
///
/// Returns `None` when the input is not a well-formed JWT (e.g. a PAT
/// `dapi…` token, or any opaque string), when the payload is not valid JSON,
/// or when the `iss` claim is absent or empty. Callers treat `None` as "not a
/// federatable token — use it as-is".
pub(crate) fn issuer(token: &str) -> Option<String> {
    // A JWT is `header.payload.signature`; the payload is the middle segment.
    let payload_b64 = token.split('.').nth(1)?;
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    let claims: Claims = serde_json::from_slice(&payload_bytes).ok()?;
    claims.iss.filter(|iss| !iss.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a JWT-shaped string `header.payload.signature` where the payload
    /// is `claims` serialized and base64url-encoded (no padding). The header
    /// and signature segments are arbitrary since we never inspect them.
    fn jwt_with_payload(claims: serde_json::Value) -> String {
        let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
        format!("header.{payload}.signature")
    }

    #[test]
    fn extracts_issuer() {
        let token = jwt_with_payload(serde_json::json!({
            "iss": "https://adb-123.4.azuredatabricks.net/oidc",
            "exp": 1_736_274_838i64,
        }));
        assert_eq!(
            issuer(&token).as_deref(),
            Some("https://adb-123.4.azuredatabricks.net/oidc")
        );
    }

    #[test]
    fn missing_issuer_claim_is_none() {
        let token = jwt_with_payload(serde_json::json!({ "exp": 1_736_274_838i64 }));
        assert_eq!(issuer(&token), None);
    }

    #[test]
    fn empty_issuer_claim_is_none() {
        let token = jwt_with_payload(serde_json::json!({ "iss": "" }));
        assert_eq!(issuer(&token), None);
    }

    #[test]
    fn non_jwt_pat_token_is_none() {
        // A Databricks PAT is an opaque `dapi…` string, not a JWT.
        assert_eq!(issuer("dapi1234567890abcdef"), None);
    }

    #[test]
    fn two_segment_string_is_none() {
        assert_eq!(issuer("header.payload"), None);
    }

    #[test]
    fn garbage_payload_is_none() {
        // Middle segment is not valid base64url / not JSON.
        assert_eq!(issuer("header.!!!not-base64!!!.sig"), None);
    }

    #[test]
    fn valid_base64_but_not_json_is_none() {
        let payload = URL_SAFE_NO_PAD.encode("this is not json");
        let token = format!("header.{payload}.sig");
        assert_eq!(issuer(&token), None);
    }
}
