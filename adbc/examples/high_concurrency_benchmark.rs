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

//! Paired high-concurrency benchmark for the Rust ADBC and direct-kernel paths.

use std::env;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection as _, Database as _, Driver as _, Statement as _};
use databricks_adbc::{Connection, Driver};
use databricks_sql_kernel::{AuthConfig, ResultBatch, Session};
use serde_json::{json, Value};

const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_SIZE_MIB: f64 = 5.0;
const DEFAULT_WARMUPS: usize = 2;
const DEFAULT_SAMPLES: usize = 10;
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

#[derive(Clone)]
struct LiveConfig {
    host: String,
    http_path: String,
    token: String,
}

#[derive(Clone, Copy, Debug)]
struct Observation {
    latency_ms: f64,
    rows: usize,
    arrow_bytes: usize,
    batches: usize,
}

struct KernelClient {
    runtime: tokio::runtime::Runtime,
    session: Session,
}

impl KernelClient {
    fn open(config: &LiveConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
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

    fn observe(&self, sql: &str) -> Result<Observation, String> {
        let mut statement = self.session.statement();
        statement.spec().sql(sql);
        let started = Instant::now();
        let mut executed = self
            .runtime
            .block_on(statement.execute())
            .map_err(|error| error.to_string())?;
        let (mut rows, mut arrow_bytes, mut batches) = (0, 0, 0);
        while let Some(batch) = self
            .runtime
            .block_on(executed.next_batch())
            .map_err(|error| error.to_string())?
        {
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

fn parse_positive_f64(name: &str, default: f64) -> Result<f64, Box<dyn std::error::Error>> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<f64>()?;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("{name} must be positive and finite").into());
    }
    Ok(value)
}

fn query(size_mib: f64) -> String {
    let target_bytes = (size_mib * 1024.0 * 1024.0).ceil() as u64;
    let rows = target_bytes.div_ceil(LOGICAL_BYTES_PER_ROW);
    format!("SELECT id, repeat('x', {PAYLOAD_BYTES}) AS payload FROM range({rows})")
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

fn observe_adbc(connection: &mut Connection, sql: &str) -> Result<Observation, String> {
    let mut statement = connection
        .new_statement()
        .map_err(|error| error.to_string())?;
    statement
        .set_sql_query(sql)
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let reader = statement.execute().map_err(|error| error.to_string())?;
    let (mut rows, mut arrow_bytes, mut batches) = (0, 0, 0);
    for batch in reader {
        let batch = batch.map_err(|error| error.to_string())?;
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

fn run_adbc_wave(clients: &mut [Connection], sql: &str) -> Result<(f64, Vec<Observation>), String> {
    let barrier = Arc::new(Barrier::new(clients.len() + 1));
    let started = Instant::now();
    let observations = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(clients.len());
        for client in clients {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                observe_adbc(client, sql)
            }));
        }
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "ADBC worker panicked".to_string())?
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    Ok((started.elapsed().as_secs_f64() * 1000.0, observations))
}

fn run_kernel_wave(clients: &[KernelClient], sql: &str) -> Result<(f64, Vec<Observation>), String> {
    let barrier = Arc::new(Barrier::new(clients.len() + 1));
    let started = Instant::now();
    let observations = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(clients.len());
        for client in clients {
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                barrier.wait();
                client.observe(sql)
            }));
        }
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "kernel worker panicked".to_string())?
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    Ok((started.elapsed().as_secs_f64() * 1000.0, observations))
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    let rank = fraction * (sorted.len() - 1) as f64;
    let (lower, upper) = (rank.floor() as usize, rank.ceil() as usize);
    sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower as f64)
}

fn validate_shapes(observations: &[Observation]) -> Result<(usize, usize, usize), String> {
    let first = observations
        .first()
        .ok_or_else(|| "wave did not produce observations".to_string())?;
    let shape = (first.rows, first.arrow_bytes, first.batches);
    if observations
        .iter()
        .any(|value| (value.rows, value.arrow_bytes, value.batches) != shape)
    {
        return Err("result shape differed within a concurrent wave".into());
    }
    Ok(shape)
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

fn dry_run(concurrency: usize, size_mib: f64, warmups: usize, samples: usize) -> Value {
    json!({
        "mode": "dry-run",
        "concurrency": concurrency,
        "size_mib": size_mib,
        "warmups_per_implementation": warmups,
        "samples_per_implementation": samples,
        "sql": query(size_mib),
        "timed_region": "barrier release through complete Arrow fetch by every concurrent client",
        "connections": "one persistent connection/session per concurrent client and implementation",
        "ordering": "ADBC/kernel order alternates by repetition",
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let concurrency = parse_positive_usize("DATABRICKS_BENCH_CONCURRENCY", DEFAULT_CONCURRENCY)?;
    let size_mib = parse_positive_f64("DATABRICKS_BENCH_SIZE_MIB", DEFAULT_SIZE_MIB)?;
    let warmups = parse_positive_usize("DATABRICKS_BENCH_WARMUPS", DEFAULT_WARMUPS)?;
    let samples = parse_positive_usize("DATABRICKS_BENCH_SAMPLES", DEFAULT_SAMPLES)?;
    if env::args().any(|argument| argument == "--dry-run") {
        println!(
            "{}",
            serde_json::to_string_pretty(&dry_run(concurrency, size_mib, warmups, samples))?
        );
        return Ok(());
    }

    let config = load_live_config()?;
    let output = PathBuf::from(required_env("DATABRICKS_BENCH_OUTPUT")?);
    let sql = query(size_mib);
    let started_at_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let mut adbc_clients = (0..concurrency)
        .map(|_| open_adbc(&config))
        .collect::<Result<Vec<_>, _>>()?;
    let kernel_clients = (0..concurrency)
        .map(|_| KernelClient::open(&config))
        .collect::<Result<Vec<_>, _>>()?;

    let mut observations = Vec::new();
    for repetition in 0..(warmups + samples) {
        let phase = if repetition < warmups {
            "warmup"
        } else {
            "sample"
        };
        let sample = repetition.saturating_sub(warmups) + 1;
        let adbc_first = repetition.is_multiple_of(2);
        for implementation in if adbc_first {
            ["rust-adbc", "rust-kernel-direct"]
        } else {
            ["rust-kernel-direct", "rust-adbc"]
        } {
            let (wall_latency_ms, values) = if implementation == "rust-adbc" {
                run_adbc_wave(&mut adbc_clients, &sql)?
            } else {
                run_kernel_wave(&kernel_clients, &sql)?
            };
            let shape = validate_shapes(&values)?;
            observations.push(json!({
                "phase": phase,
                "repetition": if phase == "warmup" { repetition + 1 } else { sample },
                "implementation": implementation,
                "wall_latency_ms": wall_latency_ms,
                "query_latency_ms": values.iter().map(|value| value.latency_ms).collect::<Vec<_>>(),
                "rows_per_query": shape.0,
                "arrow_bytes_per_query": shape.1,
                "batches_per_query": shape.2,
            }));
        }
    }

    let mut summaries = Vec::new();
    for implementation in ["rust-adbc", "rust-kernel-direct"] {
        let mut latencies = observations
            .iter()
            .filter(|value| value["phase"] == "sample" && value["implementation"] == implementation)
            .map(|value| {
                value["wall_latency_ms"]
                    .as_f64()
                    .expect("latency is numeric")
            })
            .collect::<Vec<_>>();
        latencies.sort_by(f64::total_cmp);
        summaries.push(json!({
            "implementation": implementation,
            "p50_wall_latency_ms": percentile(&latencies, 0.50),
            "p95_wall_latency_ms": percentile(&latencies, 0.95),
            "throughput_queries_per_second_at_p50": concurrency as f64 * 1000.0 / percentile(&latencies, 0.50),
        }));
    }
    let shapes = observations
        .iter()
        .map(|value| {
            (
                value["rows_per_query"].as_u64(),
                value["arrow_bytes_per_query"].as_u64(),
                value["batches_per_query"].as_u64(),
            )
        })
        .collect::<Vec<_>>();
    if shapes.iter().any(|shape| shape != &shapes[0]) {
        return Err("result shape differed between ADBC and direct-kernel waves".into());
    }

    let mut paired_samples = Vec::with_capacity(samples);
    let (mut overheads, mut ratios) = (Vec::with_capacity(samples), Vec::with_capacity(samples));
    for repetition in 1..=samples {
        let wall_latency = |implementation: &str| {
            observations
                .iter()
                .find(|value| {
                    value["phase"] == "sample"
                        && value["repetition"] == repetition
                        && value["implementation"] == implementation
                })
                .and_then(|value| value["wall_latency_ms"].as_f64())
                .expect("every measured repetition has both implementations")
        };
        let adbc_latency_ms = wall_latency("rust-adbc");
        let kernel_latency_ms = wall_latency("rust-kernel-direct");
        let overhead_ms = adbc_latency_ms - kernel_latency_ms;
        let ratio = adbc_latency_ms / kernel_latency_ms;
        overheads.push(overhead_ms);
        ratios.push(ratio);
        paired_samples.push(json!({
            "repetition": repetition,
            "first": if (repetition + warmups - 1).is_multiple_of(2) { "rust-adbc" } else { "rust-kernel-direct" },
            "adbc_wall_latency_ms": adbc_latency_ms,
            "kernel_wall_latency_ms": kernel_latency_ms,
            "overhead_ms": overhead_ms,
            "ratio": ratio,
        }));
    }
    overheads.sort_by(f64::total_cmp);
    ratios.sort_by(f64::total_cmp);
    let report = json!({
        "schema_version": 1,
        "metadata": {
            "started_at_unix_ms": started_at_unix_ms,
            "completed_at_unix_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            "host": config.host,
            "http_path": config.http_path,
            "credentials": "token read from DATABRICKS_TOKEN_FILE; value not retained",
            "driver_version": env!("CARGO_PKG_VERSION"),
            "kernel_revision": kernel_revision(),
            "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "concurrency": concurrency,
            "size_mib": size_mib,
            "warmups_per_implementation": warmups,
            "samples_per_implementation": samples,
            "timed_region": "barrier release through complete Arrow fetch by every concurrent client",
            "connections": "one persistent connection/session per concurrent client and implementation",
            "ordering": "ADBC/kernel order alternates by repetition",
        },
        "query": sql,
        "summaries": summaries,
        "paired_summary": {
            "p50_wall_overhead_ms": percentile(&overheads, 0.50),
            "p95_wall_overhead_ms": percentile(&overheads, 0.95),
            "p50_wall_ratio": percentile(&ratios, 0.50),
            "p95_wall_ratio": percentile(&ratios, 0.95),
        },
        "paired_samples": paired_samples,
        "observations": observations,
    });
    serde_json::to_writer_pretty(BufWriter::new(File::create(output)?), &report)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_query_uses_requested_logical_size() {
        assert_eq!(
            query(0.25),
            "SELECT id, repeat('x', 1024) AS payload FROM range(255)"
        );
    }

    #[test]
    fn dry_run_records_concurrency_contract() {
        let plan = dry_run(16, 5.0, 2, 10);
        assert_eq!(plan["concurrency"], 16);
        assert_eq!(plan["warmups_per_implementation"], 2);
        assert_eq!(plan["samples_per_implementation"], 10);
        assert_eq!(
            plan["ordering"],
            "ADBC/kernel order alternates by repetition"
        );
    }
}
