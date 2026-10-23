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

//! File-based token cache for U2M OAuth tokens.
//!
//! This module provides disk persistence for OAuth tokens across connections.
//! The cache is separate from the Python SDK cache to avoid compatibility issues.
//! Cache I/O errors are logged as warnings but never block authentication.
//!
//! The payload is **always encrypted** under a caller-supplied passphrase
//! (see [`super::cache_crypto`]) because it carries a long-lived refresh
//! token. Whether caching happens at all, and which passphrase is used, is
//! decided upstream by [`crate::config::TokenCacheConfig`]; reaching this
//! module already implies caching is enabled and a passphrase was supplied.

use super::cache_crypto;
use super::token::OAuthToken;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

/// Cache key used for generating the cache filename.
/// This struct is serialized to JSON and then hashed with SHA-256 to produce a unique filename.

#[derive(Debug, Serialize, Deserialize)]
struct CacheKey {
    host: String,
    client_id: String,
    scopes: Vec<String>,
}

impl CacheKey {
    /// Creates a new cache key from the given parameters.
    fn new(host: &str, client_id: &str, scopes: &[String]) -> Self {
        Self {
            host: host.to_string(),
            client_id: client_id.to_string(),
            scopes: scopes.to_vec(),
        }
    }

    /// Generates a deterministic cache filename by hashing the cache key.
    /// The filename is the SHA-256 hash of the JSON-serialized cache key.
    fn to_filename(&self) -> String {
        // Serialize to JSON (with sorted keys for determinism)
        let json = serde_json::to_string(self).expect("CacheKey serialization should not fail");

        // Hash with SHA-256
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        let result = hasher.finalize();

        // Encode the digest bytes as a lowercase hex filename.
        format!("{}.json", hex::encode(result))
    }
}

/// File-based token cache for U2M OAuth tokens.
///
/// The cache provides persistent storage for OAuth tokens across connections.
/// Cache files are stored in `~/.config/databricks-sql-kernel/oauth/` with permissions
/// set to `0o600` (owner read/write only) for security.
///
/// Cache I/O errors are logged as warnings and never block authentication.
pub struct TokenCache;

impl TokenCache {
    /// Returns the cache directory path.
    /// Location: `~/.config/databricks-sql-kernel/oauth/`
    fn cache_dir() -> Option<PathBuf> {
        Self::cache_dir_under(dirs::config_dir()?)
    }

    /// Returns the cache directory beneath an explicit config root.
    ///
    /// Split out from [`Self::cache_dir`] so tests can point the cache at a
    /// temp directory without touching `XDG_CONFIG_HOME` — process-global
    /// env state that would race against tests running in parallel.
    fn cache_dir_under(config_root: PathBuf) -> Option<PathBuf> {
        let mut path = config_root;
        path.push("databricks-sql-kernel");
        path.push("oauth");
        Some(path)
    }

    /// Returns the full cache file path for the given cache key.
    fn cache_file_path(host: &str, client_id: &str, scopes: &[String]) -> Option<PathBuf> {
        Self::cache_dir().map(|dir| Self::cache_file_path_in(&dir, host, client_id, scopes))
    }

    /// Returns the cache file path for a key inside an explicit directory.
    fn cache_file_path_in(
        dir: &std::path::Path,
        host: &str,
        client_id: &str,
        scopes: &[String],
    ) -> PathBuf {
        dir.join(CacheKey::new(host, client_id, scopes).to_filename())
    }

    /// Loads a cached token if available.
    ///
    /// Returns `None` if:
    /// - The cache file doesn't exist
    /// - The cache file is corrupted or can't be read
    /// - The cache directory can't be determined
    ///
    /// Expired tokens with a valid `refresh_token` are still returned
    /// (refresh will be attempted by the token store).
    ///
    /// # Arguments
    ///
    /// * `host` - The Databricks workspace host
    /// * `client_id` - The OAuth client ID
    /// * `scopes` - The list of OAuth scopes
    ///
    /// # Returns
    ///
    /// `Some(OAuthToken)` if a cached token is found, `None` otherwise.
    pub fn load(
        host: &str,
        client_id: &str,
        scopes: &[String],
        passphrase: &str,
    ) -> Option<OAuthToken> {
        let cache_path = Self::cache_file_path(host, client_id, scopes)?;
        Self::load_from(&cache_path, passphrase)
    }

    /// [`Self::load`] against an explicit path, so tests can exercise the
    /// real decrypt-and-parse logic without depending on the user's config
    /// directory.
    fn load_from(cache_path: &std::path::Path, passphrase: &str) -> Option<OAuthToken> {
        // Check if cache file exists
        if !cache_path.exists() {
            crate::klog::debug!("Token cache miss: file does not exist at {:?}", cache_path);
            return None;
        }

        // Read and decrypt the cache file
        let raw = match fs::read(cache_path) {
            Ok(raw) => raw,
            Err(e) => {
                crate::klog::warn!("Failed to read token cache at {:?}: {}", cache_path, e);
                return None;
            }
        };

        // A decrypt failure is reported as a miss, not an error: the usual
        // cause is a passphrase change (or a file written under a different
        // one), and the recovery — re-authenticate and overwrite — is the
        // same as for a genuinely absent cache. Logged at WARN because it
        // also covers a truncated or tampered file, which is worth seeing.
        let plaintext = match cache_crypto::decrypt(&raw, passphrase) {
            Some(plaintext) => plaintext,
            None => {
                crate::klog::warn!(
                    "Token cache at {:?} could not be decrypted (wrong passphrase, or the file \
                     is corrupt); ignoring it and re-authenticating",
                    cache_path
                );
                return None;
            }
        };

        match serde_json::from_slice::<OAuthToken>(&plaintext) {
            Ok(token) => {
                crate::klog::debug!("Token cache hit: loaded from {:?}", cache_path);
                Some(token)
            }
            Err(e) => {
                crate::klog::warn!("Token cache corrupted at {:?}, ignoring: {}", cache_path, e);
                None
            }
        }
    }

    /// Saves a token to the cache.
    ///
    /// Creates the cache directory if it doesn't exist and writes the token
    /// to disk with `0o600` permissions (owner read/write only).
    ///
    /// Cache I/O errors are logged as warnings but never propagate as errors.
    ///
    /// # Arguments
    ///
    /// * `host` - The Databricks workspace host
    /// * `client_id` - The OAuth client ID
    /// * `scopes` - The list of OAuth scopes
    /// * `token` - The token to save
    pub fn save(
        host: &str,
        client_id: &str,
        scopes: &[String],
        token: &OAuthToken,
        passphrase: &str,
    ) {
        let cache_path = match Self::cache_file_path(host, client_id, scopes) {
            Some(path) => path,
            None => {
                crate::klog::warn!("Cannot determine cache directory, token will not be cached");
                return;
            }
        };
        Self::save_to(&cache_path, token, passphrase);
    }

    /// [`Self::save`] against an explicit path, so tests can exercise the
    /// real encrypt-and-write logic (including the permission tightening)
    /// without writing into the user's config directory.
    fn save_to(cache_path: &std::path::Path, token: &OAuthToken, passphrase: &str) {
        // Create cache directory if it doesn't exist, with 700 permissions (owner only)
        if let Some(parent) = cache_path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                crate::klog::warn!("Failed to create cache directory {:?}: {}", parent, e);
                return;
            }
            // Tighten directory permissions to 700 (owner read/write/execute only)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
            }
        }

        // Serialize token to JSON, then encrypt. `to_string` rather than
        // `to_string_pretty`: the bytes are ciphertext, so the pretty-printed
        // whitespace is unreadable anyway and only pads the payload.
        let json = match serde_json::to_string(token) {
            Ok(j) => j,
            Err(e) => {
                crate::klog::warn!("Failed to serialize token for cache: {}", e);
                return;
            }
        };
        let json = cache_crypto::encrypt(json.as_bytes(), passphrase);

        // Write to a temporary file first, then rename for atomicity
        let temp_path = cache_path.with_extension("tmp");

        // Write to temp file
        match File::create(&temp_path) {
            Ok(mut file) => {
                if let Err(e) = file.write_all(json.as_bytes()) {
                    crate::klog::warn!("Failed to write token cache to {:?}: {}", temp_path, e);
                    let _ = fs::remove_file(&temp_path); // Clean up temp file
                    return;
                }
            }
            Err(e) => {
                crate::klog::warn!("Failed to create temp cache file {:?}: {}", temp_path, e);
                return;
            }
        }

        // Set file permissions to 0o600 (owner read/write only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(e) = fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o600)) {
                crate::klog::warn!("Failed to set cache file permissions: {}", e);
                let _ = fs::remove_file(&temp_path); // Clean up temp file
                return;
            }
        }

        // Atomically rename temp file to final cache file
        if let Err(e) = fs::rename(&temp_path, cache_path) {
            crate::klog::warn!(
                "Failed to rename temp cache file {:?} to {:?}: {}",
                temp_path,
                cache_path,
                e
            );
            let _ = fs::remove_file(&temp_path); // Clean up temp file
            return;
        }

        crate::klog::debug!("Token cached successfully at {:?}", cache_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use std::fs;
    use tempfile::TempDir;

    // Helper function to create a test token
    fn create_test_token() -> OAuthToken {
        OAuthToken::new(
            "test-access-token".to_string(),
            "Bearer".to_string(),
            3600, // 1 hour
            Some("test-refresh-token".to_string()),
            vec!["all-apis".to_string(), "offline_access".to_string()],
        )
    }

    /// End-to-end exercise of the REAL save / load pair.
    ///
    /// The other tests in this module hand-build the cache file, so none of
    /// them would notice if `save` stopped encrypting or `load` stopped
    /// decrypting. This one drives the actual write/read path and asserts on
    /// the bytes that land on disk.
    ///
    /// Uses the path-injecting `save_to` / `load_from` rather than
    /// redirecting `XDG_CONFIG_HOME`: the env var is process-global, and
    /// mutating it here raced against the U2M tests that also read the
    /// cache, failing them only under a full parallel run.
    #[test]
    fn save_then_load_roundtrips_and_file_is_encrypted() {
        const PASSPHRASE: &str = "test-passphrase";

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = TokenCache::cache_dir_under(temp_dir.path().to_path_buf())
            .expect("cache dir under an explicit root always resolves");
        let cache_path = TokenCache::cache_file_path_in(
            &cache_dir,
            "https://roundtrip.cloud.databricks.com",
            "client-roundtrip",
            &["all-apis".to_string()],
        );

        let token = create_test_token();
        TokenCache::save_to(&cache_path, &token, PASSPHRASE);

        // 1. The round trip preserves the token.
        let loaded =
            TokenCache::load_from(&cache_path, PASSPHRASE).expect("saved token should load back");
        assert_eq!(loaded.access_token, token.access_token);
        assert_eq!(loaded.refresh_token, token.refresh_token);

        // 2. The file on disk does NOT contain the tokens in the clear. This
        //    is the actual security property being added.
        let on_disk = fs::read(&cache_path).expect("cache file should exist");
        let as_text = String::from_utf8_lossy(&on_disk);
        assert!(
            !as_text.contains("test-refresh-token"),
            "refresh token must not be readable on disk"
        );
        assert!(
            !as_text.contains("test-access-token"),
            "access token must not be readable on disk"
        );

        // 3. A different passphrase must not decrypt it — the file is
        //    genuinely keyed, not merely obfuscated.
        assert!(
            TokenCache::load_from(&cache_path, "wrong-passphrase").is_none(),
            "load with the wrong passphrase must miss, not succeed"
        );

        // 4. The cache file is owner-only (0o600): encryption is not an
        //    excuse to widen the permissions the plaintext version had.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&cache_path)
                .expect("cache file metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "cache file must stay owner-only");
        }
    }

    #[test]
    fn test_cache_key_deterministic() {
        // Same inputs should produce the same filename
        let key1 = CacheKey::new(
            "https://example.cloud.databricks.com",
            "client-123",
            &["all-apis".to_string(), "offline_access".to_string()],
        );
        let key2 = CacheKey::new(
            "https://example.cloud.databricks.com",
            "client-123",
            &["all-apis".to_string(), "offline_access".to_string()],
        );

        assert_eq!(key1.to_filename(), key2.to_filename());

        // Different inputs should produce different filenames
        let key3 = CacheKey::new(
            "https://other.cloud.databricks.com",
            "client-123",
            &["all-apis".to_string()],
        );

        assert_ne!(key1.to_filename(), key3.to_filename());
    }

    #[test]
    fn test_cache_key_filename_format() {
        let key = CacheKey::new(
            "https://example.cloud.databricks.com",
            "client-123",
            &["all-apis".to_string()],
        );

        let filename = key.to_filename();

        // Should be a hex string followed by .json
        assert!(filename.ends_with(".json"));
        // SHA-256 produces 64 hex characters + 5 for ".json" = 69 total
        assert_eq!(filename.len(), 69);
    }

    #[test]
    fn test_cache_save_load_roundtrip() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().join("cache");

        // Mock the cache_dir function to return our temp directory
        // We'll use a workaround by directly constructing the cache path
        let cache_key = CacheKey::new(
            "https://example.cloud.databricks.com",
            "client-123",
            &["all-apis".to_string()],
        );
        let cache_path = cache_dir.join(cache_key.to_filename());
        let cache_parent = cache_path.parent().unwrap();
        fs::create_dir_all(cache_parent).expect("Failed to create cache dir");

        // Create a token
        let original_token = create_test_token();

        // Save the token manually (simulating TokenCache::save behavior)
        let json = serde_json::to_string_pretty(&original_token).unwrap();
        let mut file = File::create(&cache_path).unwrap();
        file.write_all(json.as_bytes()).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&cache_path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        // Load the token back
        let contents = fs::read_to_string(&cache_path).unwrap();
        let loaded_token: OAuthToken = serde_json::from_str(&contents).unwrap();

        // Verify all fields match
        assert_eq!(loaded_token.access_token, original_token.access_token);
        assert_eq!(loaded_token.token_type, original_token.token_type);
        assert_eq!(loaded_token.refresh_token, original_token.refresh_token);
        assert_eq!(loaded_token.expires_at, original_token.expires_at);
        assert_eq!(loaded_token.scopes, original_token.scopes);
        assert_eq!(
            loaded_token.stale_threshold_seconds,
            original_token.stale_threshold_seconds
        );
    }

    #[test]
    fn test_cache_missing_file() {
        // TokenCache::load should return None for missing files
        let result = TokenCache::load(
            "https://nonexistent.cloud.databricks.com",
            "client-999",
            &["all-apis".to_string()],
            "any-passphrase",
        );

        assert!(
            result.is_none(),
            "Load should return None for missing cache"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_cache_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().join("cache");

        // Create cache file path
        let cache_key = CacheKey::new(
            "https://example.cloud.databricks.com",
            "client-456",
            &["all-apis".to_string()],
        );
        let cache_path = cache_dir.join(cache_key.to_filename());
        let cache_parent = cache_path.parent().unwrap();
        fs::create_dir_all(cache_parent).expect("Failed to create cache dir");

        // Create a token and save it
        let token = create_test_token();
        let json = serde_json::to_string_pretty(&token).unwrap();

        // Write file
        let mut file = File::create(&cache_path).unwrap();
        file.write_all(json.as_bytes()).unwrap();

        // Set permissions to 0o600
        fs::set_permissions(&cache_path, fs::Permissions::from_mode(0o600)).unwrap();

        // Verify permissions
        let metadata = fs::metadata(&cache_path).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(
            permissions.mode() & 0o777,
            0o600,
            "Cache file should have 0o600 permissions"
        );
    }

    #[test]
    fn test_cache_corrupted_file() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().join("cache");

        // Create cache file path
        let cache_key = CacheKey::new(
            "https://example.cloud.databricks.com",
            "client-789",
            &["all-apis".to_string()],
        );
        let cache_path = cache_dir.join(cache_key.to_filename());
        let cache_parent = cache_path.parent().unwrap();
        fs::create_dir_all(cache_parent).expect("Failed to create cache dir");

        // Write corrupted JSON
        let mut file = File::create(&cache_path).unwrap();
        file.write_all(b"{ this is not valid json }").unwrap();

        // Load should return None (not error) for corrupted files
        let contents = fs::read_to_string(&cache_path).unwrap();
        let result = serde_json::from_str::<OAuthToken>(&contents);

        assert!(
            result.is_err(),
            "Corrupted JSON should fail deserialization"
        );
        // In the actual TokenCache::load, this error is caught and None is returned
    }

    #[test]
    fn test_cache_expired_token_with_refresh_token() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache_dir = temp_dir.path().join("cache");

        // Create an expired token with a refresh token
        let mut expired_token = create_test_token();
        expired_token.expires_at = Utc::now() - Duration::seconds(100); // Already expired

        // Create cache file path
        let cache_key = CacheKey::new(
            "https://example.cloud.databricks.com",
            "client-expired",
            &["all-apis".to_string()],
        );
        let cache_path = cache_dir.join(cache_key.to_filename());
        let cache_parent = cache_path.parent().unwrap();
        fs::create_dir_all(cache_parent).expect("Failed to create cache dir");

        // Save the expired token
        let json = serde_json::to_string_pretty(&expired_token).unwrap();
        let mut file = File::create(&cache_path).unwrap();
        file.write_all(json.as_bytes()).unwrap();

        // Load should still return the expired token (with refresh_token)
        // The token store will attempt to refresh it
        let contents = fs::read_to_string(&cache_path).unwrap();
        let loaded_token: OAuthToken = serde_json::from_str(&contents).unwrap();

        assert!(loaded_token.is_expired());
        assert!(loaded_token.refresh_token.is_some());
        assert_eq!(
            loaded_token.refresh_token,
            Some("test-refresh-token".to_string())
        );
    }
}
