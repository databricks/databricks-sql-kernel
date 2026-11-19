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

//! OAuth 2.0 authentication for Databricks.
//!
//! This module provides OAuth 2.0 authentication support including:
//! - Token lifecycle management and refresh logic
//! - M2M (Machine-to-Machine) client credentials flow
//! - U2M (User-to-Machine) authorization code + PKCE flow
//! - OIDC endpoint discovery
//! - File-based token caching

pub(crate) mod azure;
pub mod cache;
pub(crate) mod cache_crypto;
pub mod callback;
pub mod federation;
pub(crate) mod jwt;
pub(crate) mod jwt_key;
pub mod jwt_m2m;
pub mod m2m;
pub mod oidc;
#[cfg(test)]
pub(crate) mod test_keys;
pub mod token;
pub(crate) mod token_store;
pub mod u2m;

// Re-export the main types
pub use cache::TokenCache;
pub use callback::CallbackServer;
pub use federation::FederationProvider;
pub use jwt_m2m::JwtClientCredentialsProvider;
pub use m2m::ClientCredentialsProvider;
pub use oidc::OidcEndpoints;
pub use token::OAuthToken;
pub use u2m::AuthorizationCodeProvider;

use crate::client::http::DatabricksHttpClient;
use crate::kernel_error::{Error, Result};
use std::sync::{Arc, Weak};

/// OAuth providers hold the HTTP client weakly because the client owns its
/// auth provider: a strong reference would form a cycle that keeps the client,
/// the provider and its tokio runtime handle alive after the session closes.
pub(crate) fn upgrade_http_client(
    http_client: &Weak<DatabricksHttpClient>,
) -> Result<Arc<DatabricksHttpClient>> {
    http_client
        .upgrade()
        .ok_or_else(|| Error::invalid_state().message("OAuth HTTP client is no longer available"))
}
