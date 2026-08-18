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

//! Passphrase-based encryption for the on-disk OAuth token cache.
//!
//! The cached payload contains a long-lived refresh token, so it is
//! encrypted at rest rather than written as plaintext JSON. The scheme is
//! chosen for **wire compatibility with the JDBC driver's**
//! `EncryptedFileTokenCache`, so both drivers protect the same file the
//! same way and neither is the weak link:
//!
//! * AES-256-CBC with PKCS#7 padding
//! * key = PBKDF2-HMAC-SHA256(passphrase, salt, 65_536 iterations, 256 bits)
//! * payload = `base64(salt ‖ iv ‖ ciphertext)`, with a random 16-byte
//!   salt and 16-byte IV generated per save
//!
//! Deliberately NOT authenticated encryption. AES-GCM would be the better
//! default for new code, but the format is fixed by JDBC compatibility;
//! CBC gives confidentiality without integrity, so a tampered file is
//! detected only indirectly — corrupt padding or non-JSON plaintext makes
//! the load fail, and the caller treats that as a cache miss and
//! re-authenticates. That is an acceptable failure mode here because the
//! file is `0o600` in a `0o700` directory and a local attacker who can
//! rewrite it can generally also read the passphrase, so integrity adds
//! little over confidentiality for this specific threat model.

use aes::cipher::block_padding::Pkcs7;
use aes::cipher::{BlockModeDecrypt, BlockModeEncrypt, KeyIvInit};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hmac::Hmac;
use sha2::Sha256;
use zeroize::Zeroize;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

/// Random per-file salt length, in bytes. Matches JDBC's `SALT_SIZE`.
const SALT_LEN: usize = 16;
/// AES block size / IV length, in bytes. Matches JDBC's `IV_SIZE`.
const IV_LEN: usize = 16;
/// AES-256 key length, in bytes (JDBC's `KEY_LENGTH` of 256 bits).
const KEY_LEN: usize = 32;
/// PBKDF2 iteration count. Matches JDBC's `ITERATION_COUNT`; changing it
/// would make previously written cache files undecryptable.
const PBKDF2_ITERATIONS: u32 = 65_536;

/// Fixed component of the machine-local fallback passphrase. Changing this
/// invalidates every cache file written without an explicit passphrase
/// (which degrades to a cache miss and one re-authentication, not an error).
const DERIVED_PASSPHRASE_TAG: &str = "databricks-sql-kernel/token-cache/v1";

/// Passphrase used when the caller enabled the token cache but supplied no
/// passphrase of their own.
///
/// # Why this exists
///
/// The reference ODBC driver ships `EnableTokenCache=1` with an empty
/// `TokenCachePassPhrase` and still caches, so refusing to cache (or failing
/// the connection) would break drop-in compatibility. But writing the
/// refresh token as plaintext is not acceptable either. So we encrypt under
/// a key derived from machine-local material instead.
///
/// # What this actually protects against — and what it does not
///
/// Be clear-eyed about this: a key derived from data available on the
/// machine is **obfuscation against the wrong reader, not defence against a
/// local attacker.** It protects against:
///
/// * the cache file being copied off the box — into a backup, a container
///   image layer, a support bundle, a synced home directory — and read
///   somewhere else;
/// * a different user on the same host reading the bytes (though the
///   `0o600` file mode in a `0o700` directory is the real control there);
/// * casual inspection: `cat`-ing the file, or a log/telemetry scraper that
///   slurps config directories, no longer yields a usable credential.
///
/// It does **not** protect against an attacker who can already run code as
/// this user on this machine: they can derive the same key by construction.
/// A caller-supplied passphrase is the only thing that defends against
/// that, which is why the doc on [`crate::config::TokenCacheConfig`] says
/// supplying one is strictly stronger.
///
/// # Composition
///
/// The username plus a fixed version tag. Deliberately *not* the hostname:
/// containers get a fresh random hostname per start, which would silently
/// make the cache unreadable on every restart — the exact "why do I keep
/// getting a browser prompt" failure this whole feature exists to avoid.
/// Deliberately not a random per-install file either: that is a second piece
/// of state to create, protect, and fail to create, and it would buy nothing
/// over the file mode we already set.
pub(crate) fn derived_passphrase() -> String {
    // No username available (unusual — a daemon with no passwd entry) is not
    // a failure: fall back to the tag alone. The cache is then readable by
    // anything that knows the tag, which is no worse than the plaintext this
    // replaces, and the file mode still applies.
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME")) // Windows
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_default();
    format!("{DERIVED_PASSPHRASE_TAG}\u{1f}{user}")
}

/// Derive the AES key from `passphrase` and a per-file `salt`.
///
/// Returned in a [`Zeroizing`]-style wrapper so the key material is wiped
/// when it goes out of scope rather than lingering in freed memory.
fn derive_key(passphrase: &str, salt: &[u8]) -> Key {
    let mut key = Key([0u8; KEY_LEN]);
    // Infallible for HMAC-SHA256: `pbkdf2` only errors for output lengths
    // its PRF cannot produce, and 32 bytes is always valid here.
    pbkdf2::pbkdf2::<Hmac<Sha256>>(passphrase.as_bytes(), salt, PBKDF2_ITERATIONS, &mut key.0)
        .expect("PBKDF2-HMAC-SHA256 supports a 32-byte output");
    key
}

/// AES key material that is zeroed on drop.
struct Key([u8; KEY_LEN]);

impl Drop for Key {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Encrypt `plaintext` under `passphrase`, returning the base64 payload
/// `base64(salt ‖ iv ‖ ciphertext)`.
///
/// A fresh random salt and IV are drawn per call, so encrypting the same
/// token twice yields different payloads (and reusing a key/IV pair —
/// which is what makes CBC leak equality of plaintext prefixes — cannot
/// happen across saves).
pub(super) fn encrypt(plaintext: &[u8], passphrase: &str) -> String {
    let mut salt = [0u8; SALT_LEN];
    let mut iv = [0u8; IV_LEN];
    rand::fill(&mut salt);
    rand::fill(&mut iv);

    let key = derive_key(passphrase, &salt);
    let ciphertext =
        Aes256CbcEnc::new(&key.0.into(), &iv.into()).encrypt_padded_vec::<Pkcs7>(plaintext);

    let mut combined = Vec::with_capacity(SALT_LEN + IV_LEN + ciphertext.len());
    combined.extend_from_slice(&salt);
    combined.extend_from_slice(&iv);
    combined.extend_from_slice(&ciphertext);
    BASE64.encode(&combined)
}

/// Decrypt a payload produced by [`encrypt`] (or by JDBC's
/// `EncryptedFileTokenCache`).
///
/// Returns `None` — never an error — when the payload is not valid
/// base64, is too short to carry a salt and IV, or fails to decrypt under
/// `passphrase` (wrong passphrase, truncation, or tampering). Every one
/// of those is reported the same way on purpose: the caller's only
/// sensible response is to treat the cache as a miss and re-authenticate,
/// and distinguishing "wrong passphrase" from "corrupt file" in a return
/// value would invite callers to branch on it.
pub(super) fn decrypt(payload: &[u8], passphrase: &str) -> Option<Vec<u8>> {
    // Tolerate surrounding whitespace / trailing newline: the payload may
    // have been written by another tool or hand-inspected.
    let trimmed: Vec<u8> = payload
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    let decoded = BASE64.decode(&trimmed).ok()?;

    // Need a full salt + IV, plus at least one AES block of ciphertext.
    if decoded.len() < SALT_LEN + IV_LEN + IV_LEN {
        return None;
    }
    let (salt, rest) = decoded.split_at(SALT_LEN);
    let (iv, ciphertext) = rest.split_at(IV_LEN);

    let key = derive_key(passphrase, salt);
    let iv: [u8; IV_LEN] = iv.try_into().ok()?;
    Aes256CbcDec::new(&key.0.into(), &iv.into())
        .decrypt_padded_vec::<Pkcs7>(ciphertext)
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PASSPHRASE: &str = "correct horse battery staple";

    #[test]
    fn derived_passphrase_is_deterministic_and_non_empty() {
        // Determinism is load-bearing: a non-reproducible fallback key means
        // every process writes a cache the next one cannot read, which shows
        // up as a browser prompt on every connection — the exact failure the
        // cache exists to prevent.
        let a = derived_passphrase();
        let b = derived_passphrase();
        assert_eq!(a, b);
        assert!(!a.trim().is_empty());
        // Must not degenerate to just the tag with an empty user component,
        // in the normal case where a username IS available.
        if std::env::var("USER").is_ok() {
            assert_ne!(a, format!("{DERIVED_PASSPHRASE_TAG}\u{1f}"));
        }
    }

    #[test]
    fn derived_passphrase_round_trips_a_token() {
        // The end-to-end property the reference-compatible default depends
        // on: a cache written with no caller passphrase must be readable back
        // by the same machine, and must not be plaintext.
        let plaintext = br#"{"refresh_token":"rt-derived"}"#;
        let payload = encrypt(plaintext, &derived_passphrase());
        assert!(!payload.contains("rt-derived"));
        assert_eq!(
            decrypt(payload.as_bytes(), &derived_passphrase()).as_deref(),
            Some(plaintext.as_slice())
        );
        // And a different key must not open it, so the fallback is a real key
        // and not a constant everyone shares.
        assert_eq!(decrypt(payload.as_bytes(), DERIVED_PASSPHRASE_TAG), None);
    }

    /// Cross-language interop: decrypt a payload produced by the JDBC
    /// driver's own crypto parameters.
    ///
    /// The fixture is a real AES-256-CBC + PBKDF2-HMAC-SHA256(65_536)
    /// payload generated by running the JDBC `EncryptedFileTokenCache`
    /// encrypt path under OpenJDK 21. Checked in as a literal (rather
    /// than shelling out to `java`) so the guarantee is enforced on every
    /// `cargo test` run, including in CI where no JDK is present. If this
    /// test fails, the two drivers can no longer read each other's cache
    /// files.
    #[test]
    fn decrypts_payload_produced_by_jdbc() {
        const JDBC_PASSPHRASE: &str = "s3cr3t-passphrase";
        const JDBC_PAYLOAD: &str = "pV8vntLCNMrc77eRRwlc844XOEVPPQKZ1LLmBvdqFxd/6E8cY3JUxR6c7mPEyBvb3mZbk294oJ8NGGrNSnJebVBEDr0c/MlypDWZzOUJ6ZTHZ8FAasa9Sdj8UgOSDDVy6+XvRIWzhqSInOeagKyI+Q==";
        const EXPECTED: &str =
            r#"{"access_token":"at-123","refresh_token":"rt-456","token_type":"Bearer"}"#;

        let decrypted = decrypt(JDBC_PAYLOAD.as_bytes(), JDBC_PASSPHRASE)
            .expect("JDBC-produced payload must decrypt");
        assert_eq!(String::from_utf8_lossy(&decrypted), EXPECTED);
    }

    #[test]
    fn roundtrip_recovers_plaintext() {
        let plaintext = br#"{"access_token":"a","refresh_token":"r"}"#;
        let payload = encrypt(plaintext, PASSPHRASE);
        assert_eq!(
            decrypt(payload.as_bytes(), PASSPHRASE).as_deref(),
            Some(plaintext.as_slice())
        );
    }

    #[test]
    fn ciphertext_does_not_leak_plaintext() {
        // The whole point of encrypting at rest: the refresh token must not
        // be recoverable by reading the file.
        let payload = encrypt(b"refresh_token=SUPERSECRET", PASSPHRASE);
        assert!(!payload.contains("SUPERSECRET"));
        // ...and not merely base64-hidden.
        let decoded = BASE64.decode(payload.as_bytes()).expect("valid base64");
        assert!(!String::from_utf8_lossy(&decoded).contains("SUPERSECRET"));
    }

    #[test]
    fn same_plaintext_encrypts_differently_each_time() {
        // Fresh salt + IV per save, so identical tokens must not produce
        // identical files (which would leak that the token was unchanged).
        let a = encrypt(b"same", PASSPHRASE);
        let b = encrypt(b"same", PASSPHRASE);
        assert_ne!(a, b);
        // Both still decrypt.
        assert_eq!(
            decrypt(a.as_bytes(), PASSPHRASE).as_deref(),
            Some(&b"same"[..])
        );
        assert_eq!(
            decrypt(b.as_bytes(), PASSPHRASE).as_deref(),
            Some(&b"same"[..])
        );
    }

    #[test]
    fn wrong_passphrase_fails_to_decrypt() {
        let payload = encrypt(b"secret", PASSPHRASE);
        assert_eq!(decrypt(payload.as_bytes(), "wrong passphrase"), None);
    }

    #[test]
    fn tampered_ciphertext_is_rejected() {
        // Use a full plaintext block so PKCS#7 appends a full padding block,
        // giving us at least two ciphertext blocks. In CBC mode, flipping a
        // byte in the previous ciphertext block flips the corresponding byte
        // in the final plaintext block after decrypt. The final block is all
        // padding, so this makes the padding invalid deterministically.
        let payload = encrypt(&[0u8; IV_LEN], PASSPHRASE);
        let mut decoded = BASE64.decode(payload.as_bytes()).expect("valid base64");
        let previous_ciphertext_block_last_byte = decoded.len() - IV_LEN - 1;
        decoded[previous_ciphertext_block_last_byte] ^= 0xff;
        let tampered = BASE64.encode(&decoded);
        assert_eq!(decrypt(tampered.as_bytes(), PASSPHRASE), None);
    }

    #[test]
    fn malformed_payloads_return_none_not_panic() {
        // Each of these hits a different guard; none may panic.
        assert_eq!(decrypt(b"", PASSPHRASE), None); // empty
        assert_eq!(decrypt(b"!!!not base64!!!", PASSPHRASE), None); // bad base64
        assert_eq!(
            decrypt(BASE64.encode([0u8; 8]).as_bytes(), PASSPHRASE),
            None
        ); // too short
           // Exactly salt+IV but no ciphertext block.
        assert_eq!(
            decrypt(
                BASE64.encode([0u8; SALT_LEN + IV_LEN]).as_bytes(),
                PASSPHRASE
            ),
            None
        );
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        let payload = encrypt(b"secret", PASSPHRASE);
        let padded = format!("\n  {payload}  \n");
        assert_eq!(
            decrypt(padded.as_bytes(), PASSPHRASE).as_deref(),
            Some(&b"secret"[..])
        );
    }

    #[test]
    fn payload_layout_matches_jdbc() {
        // salt ‖ iv ‖ ciphertext, with ciphertext a whole number of AES
        // blocks. Locking the layout down keeps the format readable by
        // JDBC's EncryptedFileTokenCache.
        let payload = encrypt(b"x", PASSPHRASE);
        let decoded = BASE64.decode(payload.as_bytes()).expect("valid base64");
        assert!(decoded.len() > SALT_LEN + IV_LEN);
        let ciphertext_len = decoded.len() - SALT_LEN - IV_LEN;
        assert_eq!(ciphertext_len % IV_LEN, 0);
    }

    #[test]
    fn empty_passphrase_still_encrypts() {
        // An empty passphrase is weak but well-defined; it must not panic,
        // and the caller (not this layer) is responsible for requiring a
        // non-empty passphrase.
        let payload = encrypt(b"secret", "");
        assert_eq!(
            decrypt(payload.as_bytes(), "").as_deref(),
            Some(&b"secret"[..])
        );
        assert_eq!(decrypt(payload.as_bytes(), PASSPHRASE), None);
    }
}
