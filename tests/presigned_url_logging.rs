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

//! A failed CloudFetch request must not print the presigned URL's query (its
//! credential) in the returned error, the retry warning or the debug request
//! line.
//!
//! This is its own test binary because it captures `tracing` output. In the
//! unit-test binary, tests that open a session install a process-global
//! subscriber, which made a thread-local capture there miss events.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use databricks_sql_kernel::client::http::{DatabricksHttpClient, HttpClientConfig};
use databricks_sql_kernel::client::retry::{build_retry_configs, RequestType, RetryConfig};

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn request_error_and_logs_drop_presigned_query() {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    )
    .expect("the only subscriber in this binary");

    // Nothing listens on a port whose listener was dropped.
    let address = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let retry = RetryConfig {
        min_wait: Duration::from_millis(10),
        max_wait: Duration::from_millis(50),
        overall_timeout: Duration::from_secs(10),
        max_retries: 1,
        override_retryable_codes: None,
    };
    let client = DatabricksHttpClient::new(
        HttpClientConfig::default(),
        build_retry_configs(&retry, &HashMap::new()),
    )
    .unwrap();
    let request = client
        .inner()
        .get(format!(
            "http://{address}/results_0?sig=SECRET&X-Amz-Signature=SECRET"
        ))
        .build()
        .unwrap();

    let error = client
        .execute_without_auth(request, RequestType::CloudFetchDownload)
        .await
        .expect_err("closed port must fail");
    assert!(!error.to_string().contains("SECRET"), "{error}");
    assert!(
        error
            .message
            .contains(&format!("http://{address}/results_0)")),
        "{error}"
    );

    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("before retry"), "{logs}");
    assert!(logs.contains("/results_0 (attempt 2/2)"), "{logs}");
    assert!(!logs.contains("SECRET"), "{logs}");
}
