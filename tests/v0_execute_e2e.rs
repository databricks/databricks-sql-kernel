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

/// **End-to-end coverage of every `Row::get_*` accessor against
/// server-emitted Arrow.** Issues a single `SELECT` that emits one
/// column per supported Databricks SQL type, then walks every
/// accessor to confirm:
///
/// - Each accessor reads the correct value back from the
///   corresponding server-emitted Arrow type.
/// - The `NULL` column returns `Ok(None)` for every accessor type.
///
/// If a wire-format detail of the kernel's reader path regresses (an
/// Arrow type, a decimal scale, a date encoding, …) this test fails
/// at the specific column rather than as a generic "execute()
/// returned wrong values".
#[tokio::test]
#[ignore]
async fn select_returns_all_types_correctly() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql(
        "SELECT \
             CAST(true AS BOOLEAN)                AS c_bool, \
             CAST(127 AS TINYINT)                 AS c_i8, \
             CAST(32000 AS SMALLINT)              AS c_i16, \
             CAST(2000000000 AS INT)              AS c_i32, \
             CAST(9000000000 AS BIGINT)           AS c_i64, \
             CAST(1.5 AS FLOAT)                   AS c_f32, \
             CAST(2.5 AS DOUBLE)                  AS c_f64, \
             CAST('-123.45' AS DECIMAL(10,2))     AS c_dec, \
             CAST('hello' AS STRING)              AS c_str, \
             X'CAFE'                              AS c_bin, \
             CAST('2026-05-11' AS DATE)           AS c_date, \
             CAST('2026-05-11 12:34:56' AS TIMESTAMP) AS c_ts, \
             CAST(NULL AS INT)                    AS c_null",
    );
    let mut exec = stmt.execute().await.expect("execute SELECT all-types");

    let stream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");

    // Primitives.
    assert_eq!(row.get_bool(0).unwrap(), Some(true), "c_bool");
    assert_eq!(row.get_i8(1).unwrap(), Some(127), "c_i8");
    assert_eq!(row.get_i16(2).unwrap(), Some(32000), "c_i16");
    assert_eq!(row.get_i32(3).unwrap(), Some(2_000_000_000), "c_i32");
    assert_eq!(row.get_i64(4).unwrap(), Some(9_000_000_000), "c_i64");
    assert!(
        (row.get_f32(5).unwrap().unwrap() - 1.5_f32).abs() < f32::EPSILON,
        "c_f32"
    );
    assert!(
        (row.get_f64(6).unwrap().unwrap() - 2.5_f64).abs() < f64::EPSILON,
        "c_f64"
    );

    // Widening: get_i64 should also handle the i8/i16/i32 columns.
    assert_eq!(row.get_i64(1).unwrap(), Some(127));
    assert_eq!(row.get_i64(2).unwrap(), Some(32000));
    assert_eq!(row.get_i64(3).unwrap(), Some(2_000_000_000));

    // Decimal — canonical string form preserves scale.
    assert_eq!(row.get_decimal(7).unwrap().as_deref(), Some("-123.45"));

    // String.
    assert_eq!(row.get_str(8).unwrap(), Some("hello"));

    // Binary — `X'CAFE'` literal is two bytes.
    assert_eq!(row.get_bytes(9).unwrap(), Some(&[0xCA, 0xFE][..]));

    // Date — chrono NaiveDate.
    assert_eq!(
        row.get_date(10).unwrap(),
        Some(chrono::NaiveDate::from_ymd_opt(2026, 5, 11).unwrap()),
    );

    // Timestamp — chrono NaiveDateTime.
    assert_eq!(
        row.get_timestamp(11).unwrap(),
        Some(
            chrono::NaiveDate::from_ymd_opt(2026, 5, 11)
                .unwrap()
                .and_hms_opt(12, 34, 56)
                .unwrap()
        ),
    );

    // NULL — every accessor that's type-compatible returns Ok(None).
    // The column is INT, so get_i64 should succeed-as-None and
    // get_i32 / get_i16 / get_i8 should reject the type before
    // even seeing the NULL.
    assert_eq!(row.get_i64(12).unwrap(), None, "c_null via get_i64");
    assert_eq!(row.get_i32(12).unwrap(), None, "c_null via get_i32");

    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

/// **End-to-end coverage of every `TypedValue` variant as a bound
/// parameter.** Issues `SELECT ?, ?, ?, …` binding one of each
/// supported `TypedValue` variant, then walks the corresponding
/// `Row` accessor to confirm the wire payload round-trips
/// end-to-end.
///
/// Binary is excluded — SEA's Statement Execution API does not
/// accept `BINARY` as a parameter type. The kernel rejects
/// `TypedValue::Binary` at execute time with an error message
/// pointing callers at the `unhex(?)` workaround; the round-trip
/// is exercised through that workaround in this test.
#[tokio::test]
#[ignore]
async fn bind_all_typed_values_round_trip() {
    let session = live_session().await;
    let date = chrono::NaiveDate::from_ymd_opt(2026, 5, 11).unwrap();
    let ts = date.and_hms_opt(12, 34, 56).unwrap();
    // TIMESTAMP_LTZ binding uses a tz-aware UTC datetime. Chose a
    // non-midnight wall-clock so a silent tz-conversion bug would
    // shift the hours visibly.
    let ts_ltz = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        date.and_hms_opt(8, 15, 0).unwrap(),
        chrono::Utc,
    );
    let variant_json = r#"{"k":1,"arr":[true,null,"x"]}"#;
    let mut stmt = session.statement();
    stmt.spec()
        .sql(
            "SELECT \
                 ? AS c_bool, \
                 ? AS c_i64, \
                 ? AS c_f64, \
                 ? AS c_str, \
                 unhex(?) AS c_bin, \
                 CAST(? AS DECIMAL(10,2)) AS c_dec, \
                 CAST(? AS DATE) AS c_date, \
                 CAST(? AS TIMESTAMP) AS c_ts, \
                 CAST(? AS TIMESTAMP_NTZ) AS c_ts_ntz, \
                 CAST(? AS TIMESTAMP_LTZ) AS c_ts_ltz, \
                 PARSE_JSON(?) AS c_variant, \
                 ? AS c_null",
        )
        .param(1, TypedValue::Bool(true))
        .unwrap()
        .param(2, TypedValue::Int64(9_000_000_000))
        .unwrap()
        .param(3, TypedValue::Float64(2.5))
        .unwrap()
        .param(4, TypedValue::String("hello".into()))
        .unwrap()
        // Binary workaround per the doc on TypedValue::Binary: bind
        // the hex string and let SQL `unhex(?)` rehydrate to bytes
        // server-side.
        .param(5, TypedValue::String("cafe".into()))
        .unwrap()
        .param(
            6,
            TypedValue::Decimal {
                value: "-123.45".into(),
                precision: 10,
                scale: 2,
            },
        )
        .unwrap()
        .param(7, TypedValue::Date(date))
        .unwrap()
        .param(8, TypedValue::Timestamp(ts))
        .unwrap()
        // TIMESTAMP_NTZ — same wall-clock as `ts`, distinct type name
        // on the wire. Server returns naive datetime; `get_timestamp`
        // reads it back as `NaiveDateTime`.
        .param(9, TypedValue::TimestampNtz(ts))
        .unwrap()
        // TIMESTAMP_LTZ — tz-aware. Verified by comparing the
        // underlying `naive_utc()` wall-clock via `get_timestamp`
        // (the row API doesn't distinguish LTZ from TIMESTAMP at
        // value level — the type-name distinction lives in column
        // metadata, not in the accessor).
        .param(10, TypedValue::TimestampLtz(ts_ltz))
        .unwrap()
        // VARIANT round-trip: bind the JSON text, wrap with
        // `PARSE_JSON(?)` server-side so the resulting column is a
        // proper VARIANT (not a STRING). The kernel reads VARIANT
        // back as Arrow Utf8 with `databricks.type_name="VARIANT"`
        // metadata on the schema field; `get_str` reads the JSON
        // text. Server JSON canonicalisation removes whitespace, so
        // we compare against a canonical form.
        .param(11, TypedValue::Variant(variant_json.into()))
        .unwrap()
        .param(12, TypedValue::Null)
        .unwrap();
    let mut exec = stmt.execute().await.expect("execute bind-all-types");

    let stream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");

    assert_eq!(row.get_bool(0).unwrap(), Some(true));
    assert_eq!(row.get_i64(1).unwrap(), Some(9_000_000_000));
    assert!((row.get_f64(2).unwrap().unwrap() - 2.5).abs() < f64::EPSILON);
    assert_eq!(row.get_str(3).unwrap(), Some("hello"));
    assert_eq!(
        row.get_bytes(4).unwrap(),
        Some(&[0xCA, 0xFE][..]),
        "BINARY round-trip via the documented `unhex(?)` workaround",
    );
    assert_eq!(row.get_decimal(5).unwrap().as_deref(), Some("-123.45"));
    assert_eq!(row.get_date(6).unwrap(), Some(date));
    assert_eq!(row.get_timestamp(7).unwrap(), Some(ts));
    // TIMESTAMP_NTZ reads back through the same `get_timestamp`
    // accessor (the row API doesn't distinguish at value level;
    // the type-name distinction is in column metadata).
    assert_eq!(row.get_timestamp(8).unwrap(), Some(ts));
    // TIMESTAMP_LTZ comes back as a tz-aware UTC value via the
    // existing timestamp accessor. The wall-clock equals what we
    // bound (i.e. no silent tz drift).
    assert_eq!(row.get_timestamp(9).unwrap(), Some(ts_ltz.naive_utc()));
    // VARIANT readback: server returns the JSON canonical form
    // (key order preserved from input, whitespace stripped). Our
    // input has no whitespace, so the canonical form equals the
    // input. `get_str` reads the Utf8 Arrow column; the type
    // distinction is in the column metadata.
    assert_eq!(row.get_str(10).unwrap(), Some(variant_json));
    // NULL parameter — server returns SQL NULL; STRING is the default
    // type for TypedValue::Null on the wire.
    assert_eq!(row.get_str(11).unwrap(), None);

    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

/// Verifies the kernel handles every SQL-text use of BINARY
/// documented at
/// https://docs.databricks.com/aws/en/sql/language-manual/data-types/binary-type
/// — `X'...'` literals (odd and even length, empty), and
/// `CAST(string AS BINARY)`. These all flow through the normal
/// execute path; only the SEA parameter-array bind layer rejects
/// BINARY (covered by the next test).
#[tokio::test]
#[ignore]
async fn binary_sql_literals_and_casts_work() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec().sql(
        "SELECT \
             X'1ABF'                       AS even_hex, \
             X'1'                          AS odd_hex_pads_leading_zero, \
             X''                           AS empty_binary, \
             CAST('Spark' AS BINARY)       AS cast_string",
    );
    let mut exec = stmt.execute().await.expect("execute binary SQL literals");

    let stream = exec.result_stream_mut();
    stream.next_batch().await.unwrap().unwrap();
    let row = stream.next_row_in_batch().expect("one row");

    assert_eq!(row.get_bytes(0).unwrap(), Some(&[0x1A, 0xBF][..]));
    assert_eq!(
        row.get_bytes(1).unwrap(),
        Some(&[0x01][..]),
        "X'1' should pad to a leading zero per the BINARY type docs",
    );
    assert_eq!(row.get_bytes(2).unwrap(), Some(&[][..]));
    assert_eq!(
        row.get_bytes(3).unwrap(),
        Some(&[0x53, 0x70, 0x61, 0x72, 0x6B][..]),
        "CAST('Spark' AS BINARY) → UTF-8 bytes of 'Spark'",
    );

    exec.close().await.expect("close");
    session.close().await.expect("session close");
}

/// Verifies the kernel rejects `TypedValue::Binary` at execute time
/// (live wire) with a message pointing at the `unhex(?)` workaround
/// — so callers see a typed error before any HTTP round-trip rather
/// than a confusing SEA error from the server. This is the **only**
/// BINARY-related case the kernel doesn't support today: the SEA
/// parameters-array bind layer. SQL-text uses of BINARY (literals,
/// casts, result columns) work fine — see
/// `binary_sql_literals_and_casts_work` above.
#[tokio::test]
#[ignore]
async fn bind_binary_typed_value_is_rejected_at_execute_time() {
    let session = live_session().await;
    let mut stmt = session.statement();
    stmt.spec()
        .sql("SELECT ?")
        .param(1, TypedValue::Binary(vec![0xCA, 0xFE]))
        .unwrap();
    let err = match stmt.execute().await {
        Ok(_) => panic!("Binary must be rejected before wire"),
        Err(e) => e,
    };
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert!(err.message.contains("unhex"), "{}", err.message);
    session.close().await.expect("session close");
}

/// Repro for issue #19: `CloudFetchResultReader::next_batch` calls
/// `runtime_handle.block_on` from a sync trait method. When the
/// kernel is driven from inside an enclosing tokio runtime (e.g.
/// the PyO3 wrapper's `runtime.block_on`, or a `#[tokio::test]`
/// here), the inner `block_on` is nested under the same runtime
/// and tokio panics with "Cannot start a runtime from within a
/// runtime."
///
/// `range(100000)` is too small to flip the server to CloudFetch
/// disposition (it inlines the Arrow stream), so the existing
/// `multi_batch_range_succeeds` test never reaches the
/// `block_on` path. `range(1000000)` reliably triggers external
/// links on Dogfood.
///
/// Gated to `flavor = "multi_thread"` because the eventual fix
/// uses `tokio::task::block_in_place`, which is a no-op on
/// current-thread runtimes. The PyO3 wrapper is multi-thread, so
/// matching that here is the realistic shape.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn cloudfetch_drain_does_not_nest_block_on() {
    let session = live_session().await;
    let mut stmt = session.statement();
    // Large enough to force external-links disposition (the
    // CloudFetchResultReader path) on Dogfood. If a future
    // server-side change moves the inline/external threshold,
    // bump this.
    stmt.spec().sql("SELECT * FROM range(1000000)");
    let mut exec = stmt.execute().await.expect("execute range(1000000)");

    let rows = drain_rows(exec.result_stream_mut()).await;
    assert_eq!(
        rows, 1_000_000,
        "expected 1M rows; if this drain panicked with \
         'Cannot start a runtime from within a runtime' the fix has regressed"
    );
    exec.close().await.expect("close");
    session.close().await.expect("session close");
}
