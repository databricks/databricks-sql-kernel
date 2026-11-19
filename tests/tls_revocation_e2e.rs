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

//! Live hook for the revocation policy. Ignored by default.
//!
//! ```sh
//! DATABRICKS_TEST_REVOCATION=strict cargo test --no-default-features \
//!   --features tls-rustls --test tls_revocation_e2e -- --ignored --nocapture
//! ```
//!
//! `DATABRICKS_TEST_REVOCATION` is `off`, `strict` or `accept_undetermined`.

mod common;

use common::env_var;
use databricks_sql_kernel::{AuthConfig, Session, TlsConfig};

#[tokio::test]
#[ignore]
async fn select_one_with_revocation_policy_from_env() {
    let (check, accept) = match std::env::var("DATABRICKS_TEST_REVOCATION").as_deref() {
        Ok("strict") => (Some(true), Some(false)),
        Ok("accept_undetermined") => (Some(true), Some(true)),
        Ok("off") | Err(_) => (None, None),
        Ok(other) => panic!(
            "DATABRICKS_TEST_REVOCATION must be off, strict or accept_undetermined; got {other:?}"
        ),
    };
    // `TlsConfig` is non_exhaustive, so set the fields on a default value.
    let mut tls = TlsConfig::default();
    tls.check_certificate_revocation = check;
    tls.accept_undetermined_certificate_revocation = accept;

    let session = Session::builder()
        .http_path(
            env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST required"),
            env_var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required"),
        )
        .auth(AuthConfig::Pat {
            token: env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN required"),
            identity_federation_client_id: None,
        })
        .tls(tls)
        .open()
        .await
        .expect("open session with the revocation policy from the environment");

    let mut statement = session.statement();
    statement.spec().sql("SELECT 1 AS one");
    let mut executed = statement.execute().await.expect("execute SELECT 1");
    let mut rows = 0;
    while let Some(batch) = executed
        .result_stream_mut()
        .next_batch()
        .await
        .expect("next batch")
    {
        if let databricks_sql_kernel::ResultBatch::Arrow(batch) = batch {
            rows += batch.num_rows();
        }
    }
    assert_eq!(rows, 1, "SELECT 1 returns one row");
    executed.close().await.expect("close statement");
    session.close().await.expect("close session");
}
