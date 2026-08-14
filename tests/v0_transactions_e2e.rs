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

//! End-to-end tests for transaction control (`Session::set_autocommit` /
//! `commit` / `rollback`) against a live Databricks workspace.
//!
//! Real multi-statement transactions require a catalog-managed table
//! (`USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')`)
//! and a schema the test principal can create in. The test uses
//! `main.oss_odbc_probe` and skips (rather than fails) if it can't create there.
//!
//! Run with:
//!
//! ```sh
//! set -a && source ~/.databricks/pecotesting-creds && set +a
//! cargo test --test v0_transactions_e2e -- --ignored
//! ```

use databricks_sql_kernel::{AuthConfig, ResultStream, Session};
use std::env;

const SCHEMA: &str = "main.oss_odbc_probe";
const TABLE: &str = "main.oss_odbc_probe.kernel_txn_test";

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty())
}

async fn live_session(ignore_transactions: bool) -> Session {
    let host = env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST required");
    let path = env_var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required");
    let token = env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN required");

    Session::builder()
        .http_path(host, path)
        .auth(AuthConfig::Pat {
            token,
            identity_federation_client_id: None,
        })
        .ignore_transactions(ignore_transactions)
        .open()
        .await
        .expect("Session::open against live workspace")
}

/// Run a statement to completion, discarding its result. Returns Err on
/// failure so callers can decide to skip vs. assert.
async fn run(session: &Session, sql: &str) -> databricks_sql_kernel::Result<()> {
    let mut stmt = session.statement();
    stmt.spec().sql(sql);
    stmt.execute().await.map(|_| ())
}

async fn count(session: &Session) -> i64 {
    let mut stmt = session.statement();
    stmt.spec().sql(format!("SELECT COUNT(*) FROM {TABLE}"));
    let mut exec = stmt.execute().await.expect("count query");
    let stream: &mut ResultStream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");
    row.get_i64(0).unwrap().unwrap_or(0)
}

/// Create the catalog-managed test table; returns false (→ skip) if the
/// principal lacks CREATE on the schema or the table type is unsupported.
async fn setup_table(session: &Session) -> bool {
    if run(session, &format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        .await
        .is_err()
    {
        eprintln!("skip: cannot create schema {SCHEMA}");
        return false;
    }
    let create = format!(
        "CREATE TABLE IF NOT EXISTS {TABLE} (id INT) USING DELTA \
         TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')"
    );
    if run(session, &create).await.is_err() {
        eprintln!("skip: cannot create catalog-managed table {TABLE}");
        return false;
    }
    run(session, &format!("DELETE FROM {TABLE}"))
        .await
        .expect("clear table");
    true
}

/// With ignore_transactions = false, ROLLBACK must actually undo an insert
/// on a catalog-managed table.
#[tokio::test]
#[ignore]
async fn rollback_undoes_insert_when_transactions_enabled() {
    let session = live_session(/* ignore_transactions = */ false).await;
    if !setup_table(&session).await {
        return; // environment can't host the test; skip.
    }

    assert_eq!(count(&session).await, 0, "table should start empty");

    session
        .set_autocommit(false)
        .await
        .expect("set_autocommit(false)");
    assert!(!session.autocommit(), "autocommit should be off");

    run(&session, &format!("INSERT INTO {TABLE} VALUES (777)"))
        .await
        .expect("insert");

    session.rollback().await.expect("rollback");
    assert_eq!(
        count(&session).await,
        0,
        "ROLLBACK must undo the insert when transactions are enabled"
    );

    // COMMIT path: insert then commit persists.
    run(&session, &format!("INSERT INTO {TABLE} VALUES (888)"))
        .await
        .expect("insert 2");
    session.commit().await.expect("commit");
    assert_eq!(count(&session).await, 1, "COMMIT must persist the insert");

    // Cleanup.
    run(&session, &format!("DELETE FROM {TABLE}")).await.ok();
    session.commit().await.ok();
    session.close().await.expect("session close");
}

/// With ignore_transactions = true (the default), the control calls are
/// no-ops: they succeed but do not begin a transaction, so an insert is
/// auto-committed and a subsequent ROLLBACK does nothing.
#[tokio::test]
#[ignore]
async fn transactions_are_noop_when_ignored() {
    let session = live_session(/* ignore_transactions = */ true).await;
    if !setup_table(&session).await {
        return;
    }

    // No-ops all succeed.
    session
        .set_autocommit(false)
        .await
        .expect("set_autocommit no-op");
    // Autocommit state is unchanged (still reports the default `true`),
    // because the call was ignored.
    assert!(
        session.autocommit(),
        "autocommit should stay true when transactions are ignored"
    );

    run(&session, &format!("INSERT INTO {TABLE} VALUES (999)"))
        .await
        .expect("insert");
    session.rollback().await.expect("rollback no-op");
    assert_eq!(
        count(&session).await,
        1,
        "ROLLBACK is a no-op when transactions are ignored — the auto-committed row survives"
    );

    run(&session, &format!("DELETE FROM {TABLE}")).await.ok();
    session.close().await.expect("session close");
}
