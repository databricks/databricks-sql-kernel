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

//! Authentication mechanisms for the Databricks ADBC driver.

pub mod config;
pub mod oauth;
pub mod pat;

pub use config::{AuthConfig, AuthType};
pub use oauth::{
    AuthorizationCodeProvider, ClientCredentialsProvider, FederationProvider,
    JwtClientCredentialsProvider,
};
pub use pat::PersonalAccessToken;

use crate::kernel_error::Result;
use std::fmt::Debug;

/// Trait for authentication providers.
pub trait AuthProvider: Send + Sync + Debug {
    /// Returns the authorization header value for HTTP requests.
    fn get_auth_header(&self) -> Result<String>;

    /// Extra headers to attach **alongside** `Authorization` on authenticated
    /// requests (never on the unauthenticated token/OIDC calls). Default: none.
    ///
    /// Overridden by the Azure service-principal provider, which always emits
    /// the `X-Databricks-Azure-SP-Management-Token` header (and the
    /// `X-Databricks-Azure-Workspace-Resource-Id` header when a workspace
    /// resource id is configured) so the control plane can authorize a service
    /// principal that has an Azure RBAC role but is not a workspace member —
    /// matching the Thrift connector. These are dynamic (the management token
    /// refreshes), so they cannot be static `custom_headers`. Called on every
    /// authenticated request, so implementations must be cheap / internally
    /// cached.
    fn extra_headers(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
}
