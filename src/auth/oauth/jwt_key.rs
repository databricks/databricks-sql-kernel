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

//! Private-key loading and JWT client-assertion signing.
//!
//! This module is the net-new crypto behind the JWT private-key
//! client-credentials flow (see [`super::jwt_m2m`]). It provides the two
//! capabilities the kernel lacked — loading a PEM/PKCS#8 private key off
//! disk and producing a signed JWT.
//!
//! # Algorithm coverage
//!
//! We sign with the `jsonwebtoken` crate, which is backed by `ring`
//! (already in the dependency graph via rustls, so no new heavyweight
//! dependency). Supported algorithms: `RS256/384/512`, `PS256/384/512`,
//! `ES256`, `ES384`. Default is `RS256`.
//!
//! `ES512` (P-521) is **not** supported: `ring` has no P-521, and the
//! only Rust path to it is the `openssl` crate, which would reintroduce
//! the system-OpenSSL / Node module-load clash the kernel deliberately
//! avoids (see the TLS notes in `Cargo.toml`). We reject an unsupported
//! algorithm loudly at construction time rather than silently falling
//! back to `RS256`: a silent fallback would sign with the wrong algorithm
//! and surface as an opaque server-side `invalid_client` rejection with no
//! hint at the real cause.
//!
//! # Key format coverage
//!
//! Keys are loaded from PEM. Unencrypted PKCS#8 (`BEGIN PRIVATE KEY`),
//! SEC1 EC (`BEGIN EC PRIVATE KEY`), and PKCS#1 RSA (`BEGIN RSA PRIVATE
//! KEY`) are handed to `jsonwebtoken` directly. Passphrase-encrypted
//! PKCS#8 (`BEGIN ENCRYPTED PRIVATE KEY`) is first decrypted via the
//! `pkcs8` crate (PBES2: PBKDF2/scrypt + AES-CBC — what modern OpenSSL
//! emits) and re-encoded to an unencrypted PKCS#8 PEM in memory before
//! signing. Legacy encryption schemes BouncyCastle also accepts (PBES1,
//! 3DES) are not covered; the fix is a one-line re-encrypt to a modern
//! cipher, which the caller controls.

use std::path::Path;

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use zeroize::Zeroizing;

use crate::kernel_error::{Error, Result};

/// PEM label for an unencrypted PKCS#8 private key.
const PEM_LABEL_PRIVATE_KEY: &str = "PRIVATE KEY";
/// PEM label for a passphrase-encrypted PKCS#8 private key.
const PEM_LABEL_ENCRYPTED_PRIVATE_KEY: &str = "ENCRYPTED PRIVATE KEY";

/// Whether a signing key is RSA- or EC-based. Determines which
/// `jsonwebtoken::EncodingKey` constructor and which algorithm family
/// applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Rsa,
    Ec,
}

/// A loaded private key plus the JWT signing algorithm to use with it.
///
/// The key is parsed once (from a PEM file) and cached in the provider,
/// so signing does no per-request key loading. The [`EncodingKey`] is opaque
/// signing material; it does not implement `Debug`, so this struct
/// intentionally does not derive it (and the provider redacts around it).
pub(crate) struct JwtSigningKey {
    encoding_key: EncodingKey,
    algorithm: Algorithm,
}

impl JwtSigningKey {
    /// Load and prepare a signing key from a PEM file.
    ///
    /// * `key_path` — path to the PEM-encoded private key.
    /// * `passphrase` — passphrase for an encrypted PKCS#8 key, or `None`
    ///   for an unencrypted key.
    /// * `algorithm` — JWT algorithm name (e.g. `"RS256"`, `"ES256"`).
    ///   `None` defaults to `RS256`.
    ///
    /// Returns an error if the file cannot be read, the algorithm is
    /// unsupported, the key cannot be decrypted/parsed, or the key type
    /// does not match the algorithm family (e.g. an EC key with `RS256`).
    pub(crate) fn load(
        key_path: &str,
        passphrase: Option<&str>,
        algorithm: Option<&str>,
    ) -> Result<Self> {
        let algorithm = parse_algorithm(algorithm)?;
        let kind = key_kind_for(algorithm);

        let pem = read_key_pem(key_path, passphrase)?;
        let encoding_key = build_encoding_key(&pem, kind)?;

        Ok(Self {
            encoding_key,
            algorithm,
        })
    }

    /// Sign the given claims into a serialized (compact) JWT.
    ///
    /// The `kid` (key id) is written into the JWT header alongside the
    /// algorithm so the authorization server can select the matching
    /// registered public key.
    pub(crate) fn sign<C: Serialize>(&self, kid: &str, claims: &C) -> Result<String> {
        let mut header = Header::new(self.algorithm);
        header.kid = Some(kid.to_string());
        jsonwebtoken::encode(&header, claims, &self.encoding_key)
            .map_err(|e| Error::invalid_argument().message(format!("Error signing the JWT: {e}")))
    }
}

/// Map a JWT algorithm name onto a supported [`Algorithm`].
///
/// `None` defaults to `RS256`. Unsupported values — including `ES512`,
/// which `ring` cannot sign — are rejected loudly rather than silently
/// coerced.
fn parse_algorithm(algorithm: Option<&str>) -> Result<Algorithm> {
    let Some(name) = algorithm else {
        return Ok(Algorithm::RS256);
    };
    match name {
        "RS256" => Ok(Algorithm::RS256),
        "RS384" => Ok(Algorithm::RS384),
        "RS512" => Ok(Algorithm::RS512),
        "PS256" => Ok(Algorithm::PS256),
        "PS384" => Ok(Algorithm::PS384),
        "PS512" => Ok(Algorithm::PS512),
        "ES256" => Ok(Algorithm::ES256),
        "ES384" => Ok(Algorithm::ES384),
        other => Err(Error::invalid_argument().message(format!(
            "unsupported JWT algorithm '{other}'. Supported: RS256, RS384, RS512, \
             PS256, PS384, PS512, ES256, ES384 (ES512 is not supported)"
        ))),
    }
}

/// The key family required by a given signing algorithm.
fn key_kind_for(algorithm: Algorithm) -> KeyKind {
    match algorithm {
        Algorithm::ES256 | Algorithm::ES384 => KeyKind::Ec,
        // RS* and PS* are all RSA. EdDSA / HS* are unreachable here
        // (parse_algorithm never yields them), so treat the remainder
        // as RSA.
        _ => KeyKind::Rsa,
    }
}

/// Read the private-key PEM from disk, decrypting it first if a
/// passphrase is supplied.
///
/// Returns an unencrypted PEM string ready for `jsonwebtoken`, wrapped in
/// [`Zeroizing`] so the plaintext private-key bytes are wiped from the
/// heap on drop (consistent with the token-cache crypto's `zeroize`
/// posture — see `Cargo.toml`). When a passphrase is given, the on-disk
/// key is expected to be an encrypted PKCS#8 (`BEGIN ENCRYPTED PRIVATE
/// KEY`); it is decrypted and re-encoded to an unencrypted PKCS#8 PEM in
/// memory.
fn read_key_pem(key_path: &str, passphrase: Option<&str>) -> Result<Zeroizing<String>> {
    // A missing/unreadable file and a parse failure both surface under the
    // same "Failed to parse private key: …" message. Wrap the raw file
    // contents in `Zeroizing`: for an unencrypted key this string *is* the
    // plaintext private key.
    let raw = Zeroizing::new(std::fs::read_to_string(Path::new(key_path)).map_err(|e| {
        Error::invalid_argument().message(format!("Failed to parse private key: {e}"))
    })?);

    match passphrase {
        Some(pass) => decrypt_pkcs8_pem(&raw, pass),
        None => Ok(raw),
    }
}

/// Decrypt a passphrase-protected PKCS#8 PEM and return an unencrypted
/// PKCS#8 PEM, wrapped in [`Zeroizing`] so the decrypted private key is
/// wiped from the heap on drop.
fn decrypt_pkcs8_pem(encrypted_pem: &str, passphrase: &str) -> Result<Zeroizing<String>> {
    use pkcs8::der::pem::LineEnding;
    use pkcs8::{EncryptedPrivateKeyInfo, SecretDocument};

    // Parse the PEM wrapper. The label must be ENCRYPTED PRIVATE KEY;
    // anything else means the caller passed a passphrase for a key that
    // is not an encrypted PKCS#8 (a common misconfiguration).
    // All failures below surface under a single "Cannot decrypt private JWT
    // key …" message.
    let (label, doc) = SecretDocument::from_pem(encrypted_pem).map_err(|e| {
        Error::invalid_argument().message(format!("Cannot decrypt private JWT key {e}"))
    })?;
    if label != PEM_LABEL_ENCRYPTED_PRIVATE_KEY {
        return Err(Error::invalid_argument().message(format!(
            "Cannot decrypt private JWT key: a passphrase was supplied but the key \
             PEM is labelled '{label}', not '{PEM_LABEL_ENCRYPTED_PRIVATE_KEY}'; only \
             encrypted PKCS#8 keys use a passphrase"
        )));
    }

    let encrypted = EncryptedPrivateKeyInfo::try_from(doc.as_bytes()).map_err(|e| {
        Error::invalid_argument().message(format!("Cannot decrypt private JWT key {e}"))
    })?;

    let decrypted: SecretDocument = encrypted.decrypt(passphrase).map_err(|e| {
        Error::invalid_argument().message(format!(
            "Cannot decrypt private JWT key (wrong passphrase, or an unsupported \
             encryption scheme — only modern PBES2 with PBKDF2/scrypt + AES is \
             supported): {e}"
        ))
    })?;

    // `to_pem` already returns a `Zeroizing<String>`; keep it wrapped
    // (rather than `.to_string()`-ing into a bare String) so the decrypted
    // plaintext PEM is zeroized on drop.
    decrypted
        .to_pem(PEM_LABEL_PRIVATE_KEY, LineEnding::LF)
        .map_err(|e| {
            Error::invalid_argument().message(format!("Cannot decrypt private JWT key {e}"))
        })
}

/// Build a `jsonwebtoken::EncodingKey` from an unencrypted PEM, choosing
/// the RSA or EC constructor to match the algorithm family.
fn build_encoding_key(pem: &str, kind: KeyKind) -> Result<EncodingKey> {
    let pem_bytes = pem.as_bytes();
    let result = match kind {
        KeyKind::Rsa => EncodingKey::from_rsa_pem(pem_bytes),
        KeyKind::Ec => EncodingKey::from_ec_pem(pem_bytes),
    };
    result.map_err(|e| {
        let expected = match kind {
            KeyKind::Rsa => "an RSA key (for an RS*/PS* algorithm)",
            KeyKind::Ec => "an EC key (for an ES* algorithm)",
        };
        // Keep the "Failed to parse private key: …" prefix and add an
        // actionable hint about the key-type/algorithm mismatch.
        Error::invalid_argument().message(format!(
            "Failed to parse private key: expected {expected}; check that the key \
             type matches the configured algorithm: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_algorithm_defaults_to_rs256() {
        assert_eq!(parse_algorithm(None).unwrap(), Algorithm::RS256);
    }

    #[test]
    fn parse_algorithm_maps_supported_names() {
        for (name, alg) in [
            ("RS256", Algorithm::RS256),
            ("RS384", Algorithm::RS384),
            ("RS512", Algorithm::RS512),
            ("PS256", Algorithm::PS256),
            ("PS384", Algorithm::PS384),
            ("PS512", Algorithm::PS512),
            ("ES256", Algorithm::ES256),
            ("ES384", Algorithm::ES384),
        ] {
            assert_eq!(parse_algorithm(Some(name)).unwrap(), alg, "{name}");
        }
    }

    #[test]
    fn parse_algorithm_rejects_es512_loudly() {
        let err = parse_algorithm(Some("ES512")).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("ES512"), "{msg}");
        assert!(msg.contains("not supported"), "{msg}");
    }

    #[test]
    fn parse_algorithm_rejects_unknown() {
        assert!(parse_algorithm(Some("HS256")).is_err());
        assert!(parse_algorithm(Some("nonsense")).is_err());
    }

    #[test]
    fn key_kind_matches_family() {
        assert_eq!(key_kind_for(Algorithm::RS256), KeyKind::Rsa);
        assert_eq!(key_kind_for(Algorithm::PS512), KeyKind::Rsa);
        assert_eq!(key_kind_for(Algorithm::ES256), KeyKind::Ec);
        assert_eq!(key_kind_for(Algorithm::ES384), KeyKind::Ec);
    }

    /// Assert that a `JwtSigningKey::load` result is an error whose debug
    /// string contains `needle`. `JwtSigningKey` (the Ok variant) is not
    /// `Debug`, so `expect_err()` can't be used directly.
    fn assert_load_err(result: Result<JwtSigningKey>, needle: &str) {
        match result {
            Ok(_) => panic!("expected load error containing {needle:?}"),
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(msg.contains(needle), "error {msg:?} missing {needle:?}");
            }
        }
    }

    use crate::auth::oauth::test_keys;

    /// Write a PEM string to a temp file and return (tempdir guard, path).
    /// The guard must be kept alive for the file to exist.
    fn key_file(pem: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.pem");
        std::fs::write(&path, pem).unwrap();
        let p = path.to_str().unwrap().to_string();
        (dir, p)
    }

    #[test]
    fn loads_unencrypted_rsa_key() {
        let (_d, path) = key_file(&test_keys::rsa_pkcs8_pem());
        JwtSigningKey::load(&path, None, Some("RS256")).expect("unencrypted RSA key should load");
    }

    #[test]
    fn loads_and_decrypts_pbes2_encrypted_key() {
        let (_d, path) = key_file(&test_keys::rsa_encrypted_pkcs8_pem());
        JwtSigningKey::load(&path, Some(test_keys::TEST_KEY_PASSPHRASE), Some("RS256"))
            .expect("encrypted PKCS#8 key should decrypt and load");
    }

    #[test]
    fn wrong_passphrase_fails_with_clear_error() {
        let (_d, path) = key_file(&test_keys::rsa_encrypted_pkcs8_pem());
        assert_load_err(
            JwtSigningKey::load(&path, Some("wrong-pass"), Some("RS256")),
            "decrypt",
        );
    }

    #[test]
    fn passphrase_on_unencrypted_key_is_rejected() {
        let (_d, path) = key_file(&test_keys::rsa_pkcs8_pem());
        assert_load_err(
            JwtSigningKey::load(&path, Some("x"), Some("RS256")),
            "ENCRYPTED PRIVATE KEY",
        );
    }

    #[test]
    fn ec_algorithm_with_rsa_key_fails() {
        // Key/algorithm family mismatch must be caught at load time.
        let (_d, path) = key_file(&test_keys::rsa_pkcs8_pem());
        assert_load_err(
            JwtSigningKey::load(&path, None, Some("ES256")),
            "Failed to parse private key",
        );
    }

    #[test]
    fn loads_ec_p256_key_and_signs_es256() {
        // An EC key signs an ES256 JWT.
        let (_d, path) = key_file(&test_keys::ec_p256_pkcs8_pem());
        let key = JwtSigningKey::load(&path, None, Some("ES256"))
            .expect("EC P-256 key should load for ES256");

        #[derive(serde::Serialize)]
        struct Claims {
            sub: String,
        }
        let jwt = key
            .sign("ec-kid", &Claims { sub: "c".into() })
            .expect("sign");
        use base64::Engine;
        let header_json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(jwt.split('.').next().unwrap())
            .unwrap();
        let header: serde_json::Value = serde_json::from_slice(&header_json).unwrap();
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["kid"], "ec-kid");
    }

    #[test]
    fn signs_a_jwt_with_kid_header() {
        let (_d, path) = key_file(&test_keys::rsa_pkcs8_pem());
        let key = JwtSigningKey::load(&path, None, Some("RS256")).unwrap();

        #[derive(serde::Serialize)]
        struct Claims {
            sub: String,
        }
        let jwt = key
            .sign(
                "my-kid",
                &Claims {
                    sub: "client-1".into(),
                },
            )
            .expect("sign");

        // A compact JWS is header.payload.signature.
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3, "expected compact JWS");

        // Decode the header and confirm alg + kid.
        use base64::Engine;
        let header_json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[0])
            .unwrap();
        let header: serde_json::Value = serde_json::from_slice(&header_json).unwrap();
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["kid"], "my-kid");
    }

    #[test]
    fn build_encoding_key_rejects_garbage() {
        // `EncodingKey` (the Ok variant) is not `Debug`, so match rather
        // than `unwrap_err()`.
        match build_encoding_key("not a pem", KeyKind::Rsa) {
            Ok(_) => panic!("garbage PEM should not parse"),
            Err(e) => assert!(format!("{e:?}").contains("Failed to parse private key")),
        }
    }
}
