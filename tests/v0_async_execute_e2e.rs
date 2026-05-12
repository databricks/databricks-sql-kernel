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

//! End-to-end tests for `Statement::submit` / `ExecutedAsyncStatement`
//! against a live Databricks workspace.
//!
//! Run with:
//!
//! ```sh
//! set -a && source ~/.databricks/pecotesting-creds && set +a
//! cargo test --test v0_async_execute_e2e -- --ignored
//! ```

use databricks_sql_kernel::{
    AuthConfig, ExecutedStatementHandle, ResultBatch, ResultStream, Session, StatementStatus,
    TypedValue,
};
use std::env;

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty())
}

async fn live_session() -> Session {
    let host = env_var("DATABRICKS_HOST").expect("DATABRICKS_HOST required");
    let path = env_var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH required");
    let token = env_var("DATABRICKS_TOKEN").expect("DATABRICKS_TOKEN required");

    Session::builder()
        .http_path(host, path)
        .auth(AuthConfig::Pat { token })
        .open()
        .await
        .expect("Session::open against live workspace")
}

async fn drain_rows(stream: &mut ResultStream) -> usize {
    let mut total = 0;
    while let Some(rb) = stream.next_batch().await.expect("next_batch") {
        let ResultBatch::Arrow(rb) = rb else {
            panic!("expected Arrow batch in v0");
        };
        total += rb.num_rows();
    }
    total
}

#[tokio::test]
#[ignore]
async fn submit_returns_quickly_for_short_query() {
    // The wait_timeout on submit is 0s, so even an instantly-
    // available result should come back fast. We don't assert a
    // strict bound (Dogfood RTTs vary) — just that submit returns
    // a handle without surfacing an error.
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec = stmt.submit().await.expect("submit SELECT 1");
    assert!(!async_exec.statement_id().is_empty(), "statement_id set");

    // Server may have completed already; status should be terminal-
    // adjacent (Succeeded, or Pending/Running on its way there).
    let _ = async_exec.status().await.expect("status");
    async_exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn await_result_round_trips_select_one() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let async_exec = stmt.submit().await.expect("submit");

    let mut stream = async_exec.await_result().await.expect("await_result");
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");
    assert_eq!(row.get_i64(0).unwrap(), Some(1));

    async_exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn await_result_round_trips_parameterised() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT ?, ?")
        .param(1, TypedValue::Int64(42))
        .unwrap()
        .param(2, TypedValue::String("hello".into()))
        .unwrap();
    let async_exec = stmt.submit().await.expect("submit");
    let mut stream = async_exec.await_result().await.expect("await_result");

    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");
    assert_eq!(row.get_i64(0).unwrap(), Some(42));
    assert_eq!(row.get_str(1).unwrap(), Some("hello"));

    async_exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn status_reports_progress_for_range() {
    // Submit a query that likely takes more than one server-side
    // wait window. Status should report Pending/Running at least
    // briefly before terminal.
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT count(*) FROM range(10000000)");
    let async_exec = stmt.submit().await.expect("submit");

    // Single status check — not asserting a specific state because
    // the server might already have finished on a warm warehouse.
    // What we *do* assert is the call succeeds and returns one of
    // the known variants.
    let s = async_exec.status().await.expect("status");
    let ok = matches!(
        s,
        StatementStatus::Pending
            | StatementStatus::Running
            | StatementStatus::Succeeded
            | StatementStatus::Closed
    );
    assert!(ok, "unexpected initial status: {s:?}");

    // Drain to completion via await_result. Wrap in a 60s outer
    // timeout — Dogfood is generally fast, but a slow workspace
    // shouldn't be able to hang CI when this test runs.
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        async_exec.await_result(),
    )
    .await
    .expect("await_result must finish within 60s")
    .expect("await_result must succeed");
    let rows = drain_rows(&mut stream).await;
    assert!(rows > 0, "count(*) returns at least one row");

    async_exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn select_from_nonexistent_table_returns_sql_error_via_await_result() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT * FROM main.default.__no_such_table_for_v0_async_e2e");
    let async_exec = stmt.submit().await.expect("submit");
    let err = match async_exec.await_result().await {
        Ok(_) => panic!("await_result must surface server-side SqlError"),
        Err(e) => e,
    };
    use databricks_sql_kernel::ErrorCode;
    assert_eq!(err.code, ErrorCode::SqlError, "{err:?}");
    assert!(
        err.sql_state.is_some(),
        "expected sql_state populated: {err:?}",
    );

    async_exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn fetch_result_after_status_succeeded() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 99 AS v");
    let async_exec = stmt.submit().await.expect("submit");

    // Caller-driven poll until terminal.
    for _ in 0..50 {
        match async_exec.status().await.expect("status") {
            StatementStatus::Succeeded | StatementStatus::Closed => break,
            StatementStatus::Failed(e) => panic!("unexpected failure: {e:?}"),
            StatementStatus::Cancelled => panic!("unexpected cancel"),
            StatementStatus::Pending | StatementStatus::Running => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            // `StatementStatus` is `#[non_exhaustive]`; future
            // variants fall through here without changing test
            // behaviour.
            _ => panic!("unknown status variant"),
        }
    }

    let mut stream = async_exec.fetch_result().await.expect("fetch_result");
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");
    assert_eq!(row.get_i64(0).unwrap(), Some(99));

    async_exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn re_submit_invalidates_prior_async_handle() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let async_exec_x = stmt.submit().await.expect("submit X");
    let id_x = async_exec_x.statement_id().to_owned();

    let async_exec_y = stmt.submit().await.expect("submit Y");
    assert_ne!(async_exec_y.statement_id(), id_x, "fresh statement_id");

    use databricks_sql_kernel::ErrorCode;
    let err = async_exec_x
        .cancel()
        .await
        .expect_err("stale async handle must reject cancel");
    assert_eq!(err.code, ErrorCode::InvalidStatementHandle);

    async_exec_y.close().await.expect("close Y");
    session.close().await.expect("session close");
}
