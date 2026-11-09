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

//! Test-only private-key generation for the JWT auth tests.
//!
//! Keys are generated **in-process** (via the `rsa` / `ring`
//! dev-dependencies) rather than committed as PEM fixtures. Two reasons:
//!
//! 1. The repo's secret scanner blocks committed `BEGIN PRIVATE KEY`
//!    material, even test keys.
//! 2. Generating at test time keeps the suite self-contained and needs no
//!    `openssl` CLI, which the repo deliberately avoids (not present on
//!    Windows — see the note in `client/http.rs`).
//!
//! The RSA key is generated once per test binary and shared via a
//! `OnceLock`, because RSA-2048 key generation is the slow part; the
//! encrypted and EC variants are cheap to derive on demand.
//!
//! (Module is gated `#[cfg(test)]` at its declaration in `mod.rs`.)

use std::sync::OnceLock;

use pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
// Use the `rand_core` re-exported by `rsa` (v0.6) so keygen matches the
// RNG trait version the RSA keygen API expects — and so we don't
// pull in a second top-level `rand` that would clash with the kernel's
// runtime `rand` (v0.10) in test builds.
use rsa::rand_core::OsRng;
use rsa::RsaPrivateKey;

/// Passphrase used for the encrypted-key fixture.
pub(crate) const TEST_KEY_PASSPHRASE: &str = "test-passphrase";

/// Cached RSA-2048 key (generation is the expensive step; do it once).
fn shared_rsa_key() -> &'static RsaPrivateKey {
    static KEY: OnceLock<RsaPrivateKey> = OnceLock::new();
    KEY.get_or_init(|| RsaPrivateKey::new(&mut OsRng, 2048).expect("generate test RSA key"))
}

/// An unencrypted RSA private key as PKCS#8 PEM (`BEGIN PRIVATE KEY`).
pub(crate) fn rsa_pkcs8_pem() -> String {
    shared_rsa_key()
        .to_pkcs8_pem(LineEnding::LF)
        .expect("encode RSA PKCS#8 PEM")
        .to_string()
}

/// The RSA public key matching [`rsa_pkcs8_pem`], as SPKI PEM
/// (`BEGIN PUBLIC KEY`), for verifying a signature produced by the
/// private key.
pub(crate) fn rsa_public_spki_pem() -> String {
    use rsa::RsaPublicKey;
    RsaPublicKey::from(shared_rsa_key())
        .to_public_key_pem(LineEnding::LF)
        .expect("encode RSA public SPKI PEM")
}

/// The shared RSA key encrypted as a passphrase-protected PKCS#8
/// (`BEGIN ENCRYPTED PRIVATE KEY`) under [`TEST_KEY_PASSPHRASE`].
///
/// Uses the `pkcs8` crate's default PBES2 scheme (scrypt + AES-256-CBC).
/// Scrypt is one of the two KDFs the production decrypt path supports, so
/// this exercises the real encrypt→decrypt round-trip. (Note: OpenSSL's
/// `-v2 aes-256-cbc` default is *PBKDF2* rather than scrypt; the pkcs8
/// crate decrypts both, and the PBKDF2 constructor is not exposed for
/// encryption here, so the fixture uses the crate default.)
pub(crate) fn rsa_encrypted_pkcs8_pem() -> String {
    shared_rsa_key()
        .to_pkcs8_encrypted_pem(&mut OsRng, TEST_KEY_PASSPHRASE, LineEnding::LF)
        .expect("encrypt RSA PKCS#8")
        .to_string()
}

/// A fresh P-256 EC private key as PKCS#8 PEM (for `ES256`).
pub(crate) fn ec_p256_pkcs8_pem() -> String {
    let key = ring::signature::EcdsaKeyPair::generate_pkcs8(
        &ring::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        &ring::rand::SystemRandom::new(),
    )
    .expect("generate test EC key");
    pkcs8::SecretDocument::try_from(key.as_ref())
        .expect("parse EC PKCS#8")
        .to_pem("PRIVATE KEY", LineEnding::LF)
        .expect("encode EC PKCS#8 PEM")
        .to_string()
}
