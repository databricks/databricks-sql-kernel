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

//! E2E repro for issue #187: `Metadata::list_tables` must treat an empty
//! `table_types` slice (`Some(&[])`) the same as `None` — match ALL table
//! types — per the JDBC/ADBC contract (spec METADATA-035), not "match none".
//!
//! Run with:
//! ```sh
//! cargo test --test v0_metadata_empty_types_e2e -- --ignored --nocapture
//! ```

mod common;

use databricks_sql_kernel::{Identifier, LikePattern, ResultBatch, ResultStream};

async fn count_rows(mut stream: ResultStream) -> usize {
    let mut rows = 0;
    while let Some(rb) = stream.next_batch().await.expect("next_batch") {
        let ResultBatch::Arrow(rb) = rb else {
            panic!("expected Arrow batch in v0");
        };
        rows += rb.num_rows();
    }
    rows
}

#[tokio::test]
#[ignore]
async fn empty_table_types_matches_all_like_none() {
    let session = common::live_session().await;

    // `samples.information_schema` is a UC-managed schema present in every
    // workspace and exposes many objects (a mix of views), so both the
    // `None` and `Some(&[])` calls should return a positive, equal count.
    let catalog = || Some(Identifier::new("samples").unwrap());
    let schema = || Some(LikePattern::new("information_schema").unwrap());

    let none_rows = count_rows(
        session
            .metadata()
            .list_tables(catalog(), schema(), None, None)
            .await
            .expect("list_tables(None)"),
    )
    .await;

    let empty_rows = count_rows(
        session
            .metadata()
            .list_tables(catalog(), schema(), None, Some(&[]))
            .await
            .expect("list_tables(Some(&[]))"),
    )
    .await;

    assert!(
        none_rows > 0,
        "precondition: samples.information_schema should expose tables via None filter; got 0"
    );
    // The bug: with the unfixed kernel, `Some(&[])` drops every row → 0,
    // while `None` returns `none_rows`. This assertion is red pre-fix.
    assert_eq!(
        empty_rows, none_rows,
        "empty table_types slice must match all types (same as None): \
         None returned {none_rows} rows, Some(&[]) returned {empty_rows}"
    );

    session.close().await.expect("close");
}
