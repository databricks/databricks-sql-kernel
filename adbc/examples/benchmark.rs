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

//! Client-observed Rust ADBC overhead benchmark against the direct Rust kernel.

use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Statement as _};
use databricks_adbc::{Connection, Driver};
use databricks_sql_kernel::{AuthConfig, ResultBatch, Session};
use serde_json::{json, Value};

const DEFAULT_SIZES: &str = "0.25,0.5,1,2,3,4,5,10,25,50,100";
const PAYLOAD_BYTES: u64 = 1024;
const LOGICAL_BYTES_PER_ROW: u64 = PAYLOAD_BYTES + 8;

fn kernel_revision() -> String {
    let Ok(output) = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
    else {
        return "unavailable".into();
    };
    if !output.status.success() {
        return "unavailable".into();
    }
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Implementation {
    Adbc,
    Kernel,
}

impl Implementation {
    fn label(self) -> &'static str {
        match self {
            Self::Adbc => "rust-adbc",
            Self::Kernel => "rust-kernel-direct",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Adbc => 0,
            Self::Kernel => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Observation {
    latency_ms: f64,
    rows: usize,
    arrow_bytes: usize,
    batches: usize,
}
impl Observation {
    fn shape(self) -> (usize, usize, usize) {
        (self.rows, self.arrow_bytes, self.batches)
    }
}

struct LiveConfig {
    host: String,
    http_path: String,
    token: String,
}
struct KernelClient {
    runtime: tokio::runtime::Runtime,
    session: Session,
}

impl KernelClient {
    fn open(config: &LiveConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let session = runtime.block_on(
            Session::builder()
                .http_path(&config.host, &config.http_path)
                .auth(AuthConfig::Pat {
                    token: config.token.clone(),
                    identity_federation_client_id: None,
                })
                .open(),
        )?;
        Ok(Self { runtime, session })
    }

    fn observe(&self, sql: &str) -> Result<Observation, Box<dyn std::error::Error>> {
        let mut statement = self.session.statement();
        statement.spec().sql(sql);
        let started = Instant::now();
        let mut executed = self.runtime.block_on(statement.execute())?;
        let (mut rows, mut arrow_bytes, mut batches) = (0, 0, 0);
        while let Some(batch) = self.runtime.block_on(executed.next_batch())? {
            match batch {
                ResultBatch::Arrow(batch) => {
                    rows += batch.num_rows();
                    arrow_bytes += batch.get_array_memory_size();
                    batches += 1;
                }
                _ => return Err("kernel returned a non-Arrow result batch".into()),
            }
        }
        Ok(Observation {
            latency_ms: started.elapsed().as_secs_f64() * 1000.0,
            rows,
            arrow_bytes,
            batches,
        })
    }
}

fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("{name} must be set").into())
}

fn parse_positive_usize(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()?;
    if value == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}

fn parse_sizes(raw: &str) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let sizes: Vec<_> = raw
        .split(',')
        .map(|value| {
            let size = value.trim().parse::<f64>()?;
            if !size.is_finite() || size <= 0.0 {
                return Err(format!("invalid logical size {value:?}").into());
            }
            Ok(size)
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()?;
    if sizes.is_empty() {
        return Err("at least one logical size is required".into());
    }
    Ok(sizes)
}

fn sizes() -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    parse_sizes(&env::var("DATABRICKS_BENCH_SIZES_MIB").unwrap_or_else(|_| DEFAULT_SIZES.into()))
}

fn query(size_mib: f64) -> String {
    let target_bytes = (size_mib * 1024.0 * 1024.0).ceil() as u64;
    let rows = target_bytes.div_ceil(LOGICAL_BYTES_PER_ROW);
    format!("SELECT id, repeat('x', {PAYLOAD_BYTES}) AS payload FROM range({rows})")
}

fn order_for(repetition: usize) -> [Implementation; 2] {
    if repetition.is_multiple_of(2) {
        [Implementation::Adbc, Implementation::Kernel]
    } else {
        [Implementation::Kernel, Implementation::Adbc]
    }
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    let rank = fraction * (sorted.len() - 1) as f64;
    let (lower, upper) = (rank.floor() as usize, rank.ceil() as usize);
    sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower as f64)
}

fn paired_overhead(adbc: Observation, kernel: Observation) -> (f64, f64) {
    (
        adbc.latency_ms - kernel.latency_ms,
        adbc.latency_ms / kernel.latency_ms,
    )
}

fn paired_sample_csv(size_mib: f64, repetition: usize, overhead_ms: f64, ratio: f64) -> String {
    [
        "rust-adbc-vs-kernel".into(),
        size_mib.to_string(),
        "paired_sample".into(),
        repetition.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        format!("{overhead_ms:.3}"),
        format!("{ratio:.6}"),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    ]
    .join(",")
}

fn paired_summary_csv(
    size_mib: f64,
    p50_overhead_ms: f64,
    p95_overhead_ms: f64,
    p50_ratio: f64,
    p95_ratio: f64,
) -> String {
    [
        "rust-adbc-vs-kernel".into(),
        size_mib.to_string(),
        "paired_summary".into(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        format!("{p50_overhead_ms:.3}"),
        format!("{p95_overhead_ms:.3}"),
        format!("{p50_ratio:.6}"),
        format!("{p95_ratio:.6}"),
    ]
    .join(",")
}

fn open_adbc(config: &LiveConfig) -> Result<Connection, Box<dyn std::error::Error>> {
    let mut driver = Driver;
    let database = driver.new_database_with_opts([
        (
            OptionDatabase::Uri,
            OptionValue::String(config.host.clone()),
        ),
        (
            OptionDatabase::Other("adbc.databricks.http_path".into()),
            OptionValue::String(config.http_path.clone()),
        ),
        (
            OptionDatabase::Other("adbc.databricks.auth.type".into()),
            OptionValue::String("pat".into()),
        ),
        (
            OptionDatabase::Other("adbc.databricks.auth.token".into()),
            OptionValue::String(config.token.clone()),
        ),
    ])?;
    Ok(database.new_connection()?)
}

fn observe_adbc(
    connection: &mut Connection,
    sql: &str,
) -> Result<Observation, Box<dyn std::error::Error>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    let started = Instant::now();
    let reader = statement.execute()?;
    let (mut rows, mut arrow_bytes, mut batches) = (0, 0, 0);
    for batch in reader {
        let batch = batch?;
        rows += batch.num_rows();
        arrow_bytes += batch.get_array_memory_size();
        batches += 1;
    }
    Ok(Observation {
        latency_ms: started.elapsed().as_secs_f64() * 1000.0,
        rows,
        arrow_bytes,
        batches,
    })
}

fn load_live_config() -> Result<LiveConfig, Box<dyn std::error::Error>> {
    let token = fs::read_to_string(required_env("DATABRICKS_TOKEN_FILE")?)?
        .trim()
        .to_owned();
    if token.is_empty() {
        return Err("DATABRICKS_TOKEN_FILE is empty".into());
    }
    Ok(LiveConfig {
        host: required_env("DATABRICKS_HOST")?,
        http_path: required_env("DATABRICKS_HTTP_PATH")?,
        token,
    })
}

fn output_prefix() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let prefix = PathBuf::from(required_env("DATABRICKS_BENCH_OUTPUT_PREFIX")?);
    if prefix.as_os_str().is_empty() {
        return Err("DATABRICKS_BENCH_OUTPUT_PREFIX must not be empty".into());
    }
    Ok(prefix)
}

fn output_path(prefix: &Path, extension: &str) -> PathBuf {
    let mut value = prefix.as_os_str().to_owned();
    value.push(format!(".{extension}"));
    PathBuf::from(value)
}

fn dry_run(sizes: &[f64], warmups: usize, samples: usize) -> Value {
    json!({
        "mode": "dry-run",
        "logical_bytes_per_row": LOGICAL_BYTES_PER_ROW,
        "warmups_per_implementation": warmups,
        "samples_per_implementation": samples,
        "ordering": "AB/BA alternated by repetition within every size and phase",
        "timed_region": "execute through complete Arrow fetch",
        "matrix": sizes.iter().map(|size| json!({
            "size_mib": size, "sql": query(*size),
            "warmup_orders": (0..warmups).map(|i| order_for(i).map(Implementation::label)).collect::<Vec<_>>(),
            "sample_orders": (0..samples).map(|i| order_for(i).map(Implementation::label)).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

fn observe(
    implementation: Implementation,
    adbc: &mut Connection,
    kernel: &KernelClient,
    sql: &str,
) -> Result<Observation, Box<dyn std::error::Error>> {
    match implementation {
        Implementation::Adbc => observe_adbc(adbc, sql),
        Implementation::Kernel => kernel.observe(sql),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dry = env::args().any(|arg| arg == "--dry-run");
    let warmups = parse_positive_usize("DATABRICKS_BENCH_WARMUPS", 5)?;
    let samples = parse_positive_usize("DATABRICKS_BENCH_SAMPLES", 30)?;
    let sizes = sizes()?;
    if dry {
        println!(
            "{}",
            serde_json::to_string_pretty(&dry_run(&sizes, warmups, samples))?
        );
        return Ok(());
    }

    let prefix = output_prefix()?;
    let config = load_live_config()?;
    let mut adbc = open_adbc(&config)?;
    let kernel = KernelClient::open(&config)?;
    let started_at_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let mut csv = BufWriter::new(File::create(output_path(&prefix, "csv"))?);
    writeln!(
        csv,
        "implementation,size_mib,phase,repetition,position,latency_ms,rows,arrow_bytes,batches,p50_ms,p95_ms,overhead_ms,ratio,p50_overhead_ms,p95_overhead_ms,p50_ratio,p95_ratio"
    )?;
    let (mut json_samples, mut summaries) = (Vec::new(), Vec::new());
    let (mut paired_samples, mut paired_summaries) = (Vec::new(), Vec::new());

    for size_mib in &sizes {
        let sql = query(*size_mib);
        for repetition in 0..warmups {
            let mut shapes = Vec::new();
            for (position, implementation) in order_for(repetition).into_iter().enumerate() {
                let value = observe(implementation, &mut adbc, &kernel, &sql)?;
                shapes.push(value.shape());
                writeln!(
                    csv,
                    "{},{size_mib},warmup,{},{},{:.3},{},{},{},,,,,,,,,",
                    implementation.label(),
                    repetition + 1,
                    position + 1,
                    value.latency_ms,
                    value.rows,
                    value.arrow_bytes,
                    value.batches
                )?;
            }
            if shapes[0] != shapes[1] {
                return Err(format!("result shape mismatch during {size_mib} MiB warmup").into());
            }
        }

        let mut observations = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
        for repetition in 0..samples {
            let mut shapes = Vec::new();
            for (position, implementation) in order_for(repetition).into_iter().enumerate() {
                let value = observe(implementation, &mut adbc, &kernel, &sql)?;
                shapes.push(value.shape());
                observations[implementation.index()].push(value);
                writeln!(
                    csv,
                    "{},{size_mib},sample,{},{},{:.3},{},{},{},,,,,,,,,",
                    implementation.label(),
                    repetition + 1,
                    position + 1,
                    value.latency_ms,
                    value.rows,
                    value.arrow_bytes,
                    value.batches
                )?;
                json_samples.push(json!({"implementation": implementation.label(), "size_mib": size_mib,
                    "repetition": repetition + 1, "position": position + 1, "latency_ms": value.latency_ms,
                    "rows": value.rows, "arrow_bytes": value.arrow_bytes, "batches": value.batches}));
            }
            if shapes[0] != shapes[1] {
                return Err(format!("result shape mismatch during {size_mib} MiB sample").into());
            }
        }

        for implementation in [Implementation::Adbc, Implementation::Kernel] {
            let values = &observations[implementation.index()];
            let mut latencies: Vec<_> = values.iter().map(|v| v.latency_ms).collect();
            latencies.sort_by(f64::total_cmp);
            let shape = values[0];
            let p50 = percentile(&latencies, 0.50);
            let p95 = percentile(&latencies, 0.95);
            writeln!(
                csv,
                "{},{size_mib},summary,,,,{},{},{},{:.3},{:.3},,,,,,",
                implementation.label(),
                shape.rows,
                shape.arrow_bytes,
                shape.batches,
                p50,
                p95
            )?;
            summaries.push(
                json!({"implementation": implementation.label(), "size_mib": size_mib,
                "p50_ms": p50, "p95_ms": p95,
                "rows": shape.rows, "arrow_bytes": shape.arrow_bytes, "batches": shape.batches}),
            );
        }

        let mut overheads = Vec::with_capacity(samples);
        let mut ratios = Vec::with_capacity(samples);
        for (repetition, (adbc, kernel)) in observations[0]
            .iter()
            .zip(observations[1].iter())
            .enumerate()
        {
            let (overhead_ms, ratio) = paired_overhead(*adbc, *kernel);
            overheads.push(overhead_ms);
            ratios.push(ratio);
            writeln!(
                csv,
                "{}",
                paired_sample_csv(*size_mib, repetition + 1, overhead_ms, ratio)
            )?;
            paired_samples.push(json!({
                "size_mib": size_mib,
                "repetition": repetition + 1,
                "first": order_for(repetition)[0].label(),
                "adbc_latency_ms": adbc.latency_ms,
                "kernel_latency_ms": kernel.latency_ms,
                "overhead_ms": overhead_ms,
                "ratio": ratio,
            }));
        }
        overheads.sort_by(f64::total_cmp);
        ratios.sort_by(f64::total_cmp);
        let p50_overhead_ms = percentile(&overheads, 0.50);
        let p95_overhead_ms = percentile(&overheads, 0.95);
        let p50_ratio = percentile(&ratios, 0.50);
        let p95_ratio = percentile(&ratios, 0.95);
        writeln!(
            csv,
            "{}",
            paired_summary_csv(
                *size_mib,
                p50_overhead_ms,
                p95_overhead_ms,
                p50_ratio,
                p95_ratio,
            )
        )?;
        paired_summaries.push(json!({
            "size_mib": size_mib,
            "p50_overhead_ms": p50_overhead_ms,
            "p95_overhead_ms": p95_overhead_ms,
            "p50_ratio": p50_ratio,
            "p95_ratio": p95_ratio,
        }));
        csv.flush()?;
    }

    let report = json!({"schema_version": 1, "metadata": {
        "started_at_unix_ms": started_at_unix_ms, "host": config.host, "http_path": config.http_path,
        "credentials": "token read from DATABRICKS_TOKEN_FILE; value not retained",
        "driver_version": env!("CARGO_PKG_VERSION"),
        "kernel_revision": kernel_revision(),
        "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "client_os": env::consts::OS, "client_arch": env::consts::ARCH,
        "logical_bytes_per_row": LOGICAL_BYTES_PER_ROW, "sizes_mib": sizes,
        "warmups_per_implementation": warmups, "samples_per_implementation": samples,
        "ordering": "AB/BA alternated by repetition within every size and phase",
        "timed_region": "execute through complete Arrow fetch",
        "connections": "one persistent connection/session per implementation"},
        "queries": sizes.iter().map(|s| json!({"size_mib": s, "sql": query(*s)})).collect::<Vec<_>>(),
        "summaries": summaries, "samples": json_samples,
        "paired_summaries": paired_summaries, "paired_samples": paired_samples});
    serde_json::to_writer_pretty(
        BufWriter::new(File::create(output_path(&prefix, "json"))?),
        &report,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_sizes_match_matrix() {
        assert_eq!(
            parse_sizes(DEFAULT_SIZES).unwrap(),
            vec![0.25, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 100.0]
        );
    }
    #[test]
    fn query_is_deterministic() {
        assert_eq!(
            query(0.25),
            "SELECT id, repeat('x', 1024) AS payload FROM range(255)"
        );
        assert_eq!(query(0.25), query(0.25));
    }
    #[test]
    fn ordering_alternates() {
        assert_eq!(order_for(0), [Implementation::Adbc, Implementation::Kernel]);
        assert_eq!(order_for(1), [Implementation::Kernel, Implementation::Adbc]);
    }
    #[test]
    fn percentile_interpolates() {
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.50), 2.5);
        assert!((percentile(&[1.0, 2.0, 3.0, 4.0], 0.95) - 3.85).abs() < 1e-12);
    }
    #[test]
    fn paired_overhead_preserves_signed_difference_and_ratio() {
        let adbc = Observation {
            latency_ms: 12.0,
            rows: 1,
            arrow_bytes: 2,
            batches: 3,
        };
        let kernel = Observation {
            latency_ms: 10.0,
            ..adbc
        };
        assert_eq!(paired_overhead(adbc, kernel), (2.0, 1.2));
        let faster_adbc = Observation {
            latency_ms: 8.0,
            ..adbc
        };
        assert_eq!(paired_overhead(faster_adbc, kernel), (-2.0, 0.8));
    }
    #[test]
    fn paired_csv_rows_match_the_declared_schema() {
        let sample = paired_sample_csv(0.25, 1, -2.0, 0.8);
        let summary = paired_summary_csv(0.25, -1.0, 2.0, 0.9, 1.1);
        assert_eq!(sample.split(',').count(), 17);
        assert_eq!(summary.split(',').count(), 17);
        assert_eq!(sample.split(',').nth(11), Some("-2.000"));
        assert_eq!(summary.split(',').nth(13), Some("-1.000"));
    }
    #[test]
    fn dry_run_is_secret_free_and_complete() {
        let p = dry_run(&[0.25], 5, 30);
        let s = serde_json::to_string(&p).unwrap();
        assert!(!s.contains("token"));
        assert_eq!(p["matrix"][0]["warmup_orders"].as_array().unwrap().len(), 5);
        assert_eq!(
            p["matrix"][0]["sample_orders"].as_array().unwrap().len(),
            30
        );
    }
}
