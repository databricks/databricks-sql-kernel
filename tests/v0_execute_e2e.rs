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

//! End-to-end tests for `Statement::execute` against a live
//! Databricks workspace.
//!
//! Run with:
//!
//! ```sh
//! set -a && source ~/.databricks/pecotesting-creds && set +a
//! cargo test --test v0_execute_e2e -- --ignored
//! ```

use databricks_sql_kernel::{
    AuthConfig, ErrorCode, ExecutedStatementHandle, ResultBatch, ResultStream, Session, TypedValue,
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
async fn select_one_round_trip() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let mut exec = stmt.execute().await.expect("execute SELECT 1");

    let stream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");
    assert_eq!(row.get_i64(0).unwrap(), Some(1));
    assert!(stream.next_row_in_batch().is_none());
    assert!(stream.next_batch().await.unwrap().is_none());

    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn select_with_positional_params() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT ?, ?, ?")
        .param(1, TypedValue::Int64(42))
        .unwrap()
        .param(2, TypedValue::String("hello".into()))
        .unwrap()
        .param(3, TypedValue::Bool(true))
        .unwrap();
    let mut exec = stmt.execute().await.expect("execute with params");

    let stream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");
    assert_eq!(row.get_i64(0).unwrap(), Some(42));
    assert_eq!(row.get_str(1).unwrap(), Some("hello"));
    assert_eq!(row.get_bool(2).unwrap(), Some(true));
    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn multi_batch_range_succeeds() {
    let session = live_session().await;
    let mut stmt = session.statement();
    // Large enough to typically span multiple batches via CloudFetch,
    // but constrained so a tiny warehouse can still complete it.
    stmt.spec().sql("SELECT * FROM range(100000)");
    let mut exec = stmt.execute().await.expect("execute range");

    let rows = drain_rows(exec.result_stream_mut()).await;
    assert_eq!(rows, 100_000, "expected 100k rows from range(100000)");
    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn select_from_nonexistent_table_returns_sql_error() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT * FROM main.default.__no_such_table_for_v0_e2e");
    let err = stmt.execute().await.err().expect("must fail with SqlError");
    assert_eq!(err.code, ErrorCode::SqlError, "{err:?}");
    assert!(
        err.sql_state.is_some(),
        "expected sql_state to be populated: {err:?}"
    );
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn re_execute_invalidates_prior_handle() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1");
    let exec_a = stmt.execute().await.expect("execute A");
    let id_a = exec_a.statement_id().to_owned();

    let exec_b = stmt.execute().await.expect("execute B");
    assert_ne!(exec_b.statement_id(), id_a, "fresh statement_id expected");

    let err = exec_a
        .cancel()
        .await
        .expect_err("stale handle must reject cancel");
    assert_eq!(err.code, ErrorCode::InvalidStatementHandle);

    exec_b.close().await.expect("close B");
    session.close().await.expect("session close");
}

#[tokio::test]
#[ignore]
async fn type_roundtrip_select_bound_value() {
    let session = live_session().await;
    let cases: Vec<(&str, TypedValue)> = vec![
        ("BIGINT", TypedValue::Int64(42)),
        ("STRING", TypedValue::String("hello".into())),
        ("BOOLEAN", TypedValue::Bool(true)),
        ("DOUBLE", TypedValue::Float64(2.5)),
    ];
    for (label, value) in cases {
        let mut stmt = session.statement();
        stmt.spec().sql("SELECT ?").param(1, value.clone()).unwrap();
        let mut exec = stmt
            .execute()
            .await
            .unwrap_or_else(|e| panic!("execute {label}: {e:?}"));
        let stream = exec.result_stream_mut();
        stream.next_batch().await.unwrap().unwrap();
        let row = stream.next_row_in_batch().expect("a row");
        match (&value, label) {
            (TypedValue::Int64(v), _) => assert_eq!(row.get_i64(0).unwrap(), Some(*v)),
            (TypedValue::String(v), _) => assert_eq!(row.get_str(0).unwrap(), Some(v.as_str())),
            (TypedValue::Bool(v), _) => assert_eq!(row.get_bool(0).unwrap(), Some(*v)),
            (TypedValue::Float64(v), _) => {
                let got = row.get_f64(0).unwrap().expect("float value");
                assert!((got - *v).abs() < f64::EPSILON, "{label}: {got} vs {v}");
            }
            _ => unreachable!(),
        }
        exec.close().await.expect("close");
    }
    session.close().await.expect("session close");
}
