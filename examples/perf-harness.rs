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

//! Local-only perf harness for the kernel. Not part of mainline.
//!
//! Run from the project root with `~/.databricks/perf-creds.env`
//! sourced (or env vars otherwise set):
//!
//! ```sh
//! set -a && source ~/.databricks/perf-creds.env && set +a
//! cargo run --release --example perf-harness -- --iters 10
//! ```
//!
//! Knobs (env or CLI):
//!
//! - `DATABRICKS_HOST`            (required) workspace URL
//! - `DATABRICKS_HTTP_PATH`       (required) `/sql/1.0/warehouses/{id}`
//! - `DATABRICKS_TOKEN`           (required) PAT
//! - `--iters N`                  number of iterations per workload (default 10)
//! - `--workload {select1|range|tpch_q1}` (default `select1`)
//! - `--range-n N`                row count for the `range` workload (default 100_000)
//! - `RUST_LOG`                   tracing-subscriber filter (default `info`)
//!
//! Emits one CSV-ish line per iteration on stdout for trivial parsing,
//! plus a final aggregate (p50/p95/p99/mean/min/max in ms) on stderr.
//!
//! With `RUST_LOG=databricks_sql_kernel=debug` you get the kernel's
//! span output too — useful for attributing time to the instrumented
//! boundaries (sea.call_execute_api, sea.wait_for_completion,
//! cloudfetch.download, arrow.parse_ipc, result_stream.next_batch).

use databricks_sql_kernel::{AuthConfig, ExecutedStatementHandle, ResultBatch, Session};
use std::env;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tracing subscriber gated by RUST_LOG. Default `info` is quiet
    // enough not to drown out the per-iteration CSV; bump to
    // `databricks_sql_kernel=debug` to see span enter/exit lines.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse(env::args().skip(1).collect());
    let host = require_env("DATABRICKS_HOST")?;
    let path = require_env("DATABRICKS_HTTP_PATH")?;
    let token = require_env("DATABRICKS_TOKEN")?;

    eprintln!(
        "perf-harness: workload={} iters={} host={}",
        args.workload, args.iters, host
    );

    let session = Session::builder()
        .http_path(host, path)
        .auth(AuthConfig::Pat { token })
        .open()
        .await?;

    println!("iter,workload,wall_ms,rows");
    let mut samples = Vec::with_capacity(args.iters);
    for i in 0..args.iters {
        let (wall_ms, rows) = match args.workload.as_str() {
            "select1" => run_select_1(&session).await?,
            "range" => run_range(&session, args.range_n).await?,
            "tpch_q1" => run_tpch_q1(&session).await?,
            other => return Err(format!("unknown workload: {other}").into()),
        };
        println!("{i},{},{wall_ms:.3},{rows}", args.workload);
        samples.push(wall_ms);
    }

    session.close().await?;
    print_aggregate(&samples);
    Ok(())
}

async fn run_select_1(session: &Session) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let mut stmt = session.statement();
    stmt.spec().sql("SELECT 1 AS n");
    let t0 = Instant::now();
    let mut exec = stmt.execute().await?;
    let stream = exec.result_stream_mut();
    let mut rows = 0;
    while let Some(rb) = stream.next_batch().await? {
        let ResultBatch::Arrow(rb) = rb else {
            return Err("non-Arrow batch in v0".into());
        };
        rows += rb.num_rows();
    }
    let wall = t0.elapsed();
    exec.close().await?;
    Ok((wall.as_secs_f64() * 1000.0, rows))
}

async fn run_range(
    session: &Session,
    n: usize,
) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    let mut stmt = session.statement();
    stmt.spec().sql(&format!("SELECT * FROM range({n})"));
    let t0 = Instant::now();
    let mut exec = stmt.execute().await?;
    let stream = exec.result_stream_mut();
    let mut rows = 0;
    while let Some(rb) = stream.next_batch().await? {
        let ResultBatch::Arrow(rb) = rb else {
            return Err("non-Arrow batch in v0".into());
        };
        rows += rb.num_rows();
    }
    let wall = t0.elapsed();
    exec.close().await?;
    Ok((wall.as_secs_f64() * 1000.0, rows))
}

async fn run_tpch_q1(session: &Session) -> Result<(f64, usize), Box<dyn std::error::Error>> {
    // TPC-H Q1 against samples.tpch.tinyint / lineitem. samples.tpch
    // is pre-populated in every workspace; sf1 by default.
    let sql = "
        SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
        FROM samples.tpch.lineitem
        WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    ";
    let mut stmt = session.statement();
    stmt.spec().sql(sql);
    let t0 = Instant::now();
    let mut exec = stmt.execute().await?;
    let stream = exec.result_stream_mut();
    let mut rows = 0;
    while let Some(rb) = stream.next_batch().await? {
        let ResultBatch::Arrow(rb) = rb else {
            return Err("non-Arrow batch in v0".into());
        };
        rows += rb.num_rows();
    }
    let wall = t0.elapsed();
    exec.close().await?;
    Ok((wall.as_secs_f64() * 1000.0, rows))
}

struct Args {
    workload: String,
    iters: usize,
    range_n: usize,
}

impl Args {
    fn parse(argv: Vec<String>) -> Self {
        let mut iters = 10usize;
        let mut workload = "select1".to_string();
        let mut range_n = 100_000usize;
        let mut it = argv.into_iter();
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--iters" => iters = it.next().and_then(|s| s.parse().ok()).unwrap_or(iters),
                "--workload" => workload = it.next().unwrap_or(workload),
                "--range-n" => range_n = it.next().and_then(|s| s.parse().ok()).unwrap_or(range_n),
                _ => eprintln!("perf-harness: ignoring unknown arg: {arg}"),
            }
        }
        Self {
            workload,
            iters,
            range_n,
        }
    }
}

fn require_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("required env var {key} is unset or empty").into())
}

fn print_aggregate(samples: &[f64]) {
    if samples.is_empty() {
        return;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let p = |q: f64| {
        let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
        sorted[idx]
    };
    eprintln!(
        "aggregate (ms): n={} min={:.3} p50={:.3} p95={:.3} p99={:.3} mean={:.3} max={:.3}",
        samples.len(),
        sorted[0],
        p(0.50),
        p(0.95),
        p(0.99),
        mean,
        *sorted.last().unwrap()
    );
}
