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

use databricks_sql_kernel::client::{DatabricksHttpClient, HttpClientConfig};
use std::time::Duration;

#[test]
fn http_client_config_accepts_exhaustive_struct_literals() {
    let config = HttpClientConfig {
        sea_response_compression_enabled: false,
        connect_timeout: Duration::from_secs(30),
        read_timeout: Duration::from_secs(60),
        max_connections_per_host: 100,
        user_agent: "test-client".into(),
        proxy: Default::default(),
        tls: Default::default(),
        org_id: None,
        custom_headers: Vec::new(),
    };
    DatabricksHttpClient::with_default_retry(config).unwrap();
}
