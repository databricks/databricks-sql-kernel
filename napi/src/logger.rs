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

//! `tracing` → JS `DBSQLLogger` bridge via `ThreadsafeFunction`.
//!
//! The kernel emits its diagnostics through the `tracing` crate (targets
//! `databricks::sql::kernel` from the `klog!` shim, plus module-path targets
//! like `databricks_sql_kernel::session`). In a Node process nothing installs
//! a `tracing` `Subscriber`, so those events are otherwise **dropped** — the
//! Node driver's `DBSQLLogger` only ever saw JS-side lines.
//!
//! This module closes that gap so logs from all three layers (Node driver,
//! this napi shim, and the Rust kernel) land in the **same** `DBSQLLogger`
//! sink — i.e. the same file when the caller configures one.
//!
//! ## Shape
//!
//! - `initKernelLogging(callback, level)` installs a process-global
//!   [`KernelLogLayer`] (once) and records the JS `callback` as the current
//!   sink. The layer captures `databricks*`-targeted events, filters them by
//!   an atomic level, formats each into a [`LogRecord`], and pushes it onto a
//!   **bounded** channel (drop-on-full — logs are advisory and must never
//!   block or back-pressure a kernel hot path).
//! - A dedicated drain thread batches whatever is queued and forwards each
//!   batch to JS via a single `ThreadsafeFunction::call` in `NonBlocking`
//!   mode, amortising the per-call cost so the ~46 hot-path events don't eat
//!   the SELECT-1 latency.
//! - `setKernelLogLevel(level)` retargets the atomic filter live.
//!
//! ## Process-global, last-writer-wins
//!
//! A `tracing` global default `Subscriber` can be installed only once per
//! process, so the bridge is process-global (matching the Python connector's
//! `pyo3_log` model). The most recent `initKernelLogging` call wins for both
//! the sink and the level. Single-client apps — the overwhelming common case —
//! are unaffected.

// Every item here is reachable only through the `#[napi]` entry points, which
// the lib-*test* target's dead-code pass doesn't treat as roots (the real
// cdylib build does, via the napi registration ctors). Scope the allow to the
// test cfg so genuine dead code is still caught in the production build.
#![cfg_attr(test, allow(dead_code))]

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Mutex, Once};

use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Env;
use napi_derive::napi;
use once_cell::sync::OnceCell;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// One kernel log event, as handed to JS. `level` is a lower-case string
/// (`error`/`warn`/`info`/`debug`/`trace`) the Node side maps onto its
/// `LogLevel`; `target` is the originating `tracing` target (e.g.
/// `databricks::sql::kernel`); `message` is the rendered event plus any
/// structured `key=value` fields.
#[napi(object)]
pub struct LogRecord {
    pub level: String,
    pub target: String,
    pub message: String,
}

/// Channel depth. Bursts beyond this are dropped rather than blocking the
/// emitting kernel task — see module docs.
const CHANNEL_CAPACITY: usize = 4096;

/// Max records forwarded per `ThreadsafeFunction::call`.
const MAX_BATCH: usize = 512;

const LEVEL_OFF: u8 = 0;
const LEVEL_ERROR: u8 = 1;
const LEVEL_WARN: u8 = 2;
const LEVEL_INFO: u8 = 3;
const LEVEL_DEBUG: u8 = 4;
const LEVEL_TRACE: u8 = 5;

/// Numeric severity, low = more severe. `0` means "off / emit nothing".
/// The layer emits an event when `severity(event) <= LEVEL` (and `LEVEL > 0`).
static LEVEL: AtomicU8 = AtomicU8::new(LEVEL_WARN);

/// Producer end, populated once on first `initKernelLogging`.
static SENDER: OnceCell<SyncSender<LogRecord>> = OnceCell::new();

/// Current JS sink. Last-writer-wins; read by the drain thread per batch.
static SINK: OnceCell<Mutex<Option<ThreadsafeFunction<Vec<LogRecord>>>>> = OnceCell::new();

/// Guards the one-time subscriber install + channel + drain-thread spawn.
static INSTALL: Once = Once::new();

fn parse_level(s: &str) -> u8 {
    match s.trim().to_ascii_lowercase().as_str() {
        "off" | "silent" | "none" => LEVEL_OFF,
        "error" => LEVEL_ERROR,
        "warn" | "warning" => LEVEL_WARN,
        "info" => LEVEL_INFO,
        "debug" => LEVEL_DEBUG,
        "trace" => LEVEL_TRACE,
        // Unknown → conservative default so a typo never floods the sink.
        _ => LEVEL_WARN,
    }
}

fn severity(level: &Level) -> u8 {
    match *level {
        Level::ERROR => LEVEL_ERROR,
        Level::WARN => LEVEL_WARN,
        Level::INFO => LEVEL_INFO,
        Level::DEBUG => LEVEL_DEBUG,
        Level::TRACE => LEVEL_TRACE,
    }
}

fn level_label(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => "error",
        Level::WARN => "warn",
        Level::INFO => "info",
        Level::DEBUG => "debug",
        Level::TRACE => "trace",
    }
}

/// Returns whether an event at `level` should currently be forwarded.
fn should_emit(level: &Level) -> bool {
    let configured = LEVEL.load(Ordering::Relaxed);
    configured != LEVEL_OFF && severity(level) <= configured
}

/// Collects a `tracing` event's primary `message` plus any structured fields
/// into a single line: `"<message> key1=v1 key2=v2"`.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl MessageVisitor {
    fn finish(self) -> String {
        if self.fields.is_empty() {
            self.message
        } else if self.message.is_empty() {
            self.fields
        } else {
            format!("{} {}", self.message, self.fields)
        }
    }

    fn push_field(&mut self, name: &str, value: String) {
        if !self.fields.is_empty() {
            self.fields.push(' ');
        }
        self.fields.push_str(name);
        self.fields.push('=');
        self.fields.push_str(&value);
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // The primary message is recorded under the synthetic `message` field;
        // its `Debug` is the already-rendered string (no extra quoting).
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.push_field(field.name(), format!("{value:?}"));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.push_field(field.name(), value.to_owned());
        }
    }
}

/// Process-global `tracing` Layer: filters to `databricks*` targets at the
/// configured level, formats each event, and enqueues it (drop-on-full).
struct KernelLogLayer;

impl<S> Layer<S> for KernelLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();

        // Only kernel / napi diagnostics — never another crate that happens to
        // share the global subscriber. `databricks::sql::kernel` (klog shim),
        // `databricks_sql_kernel::*` (module paths), and the napi crate's own
        // `databricks_sql_kernel_napi` breadcrumbs all share the prefix.
        if !meta.target().starts_with("databricks") {
            return;
        }
        if !should_emit(meta.level()) {
            return;
        }

        let Some(tx) = SENDER.get() else {
            return;
        };

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        let record = LogRecord {
            level: level_label(meta.level()).to_owned(),
            target: meta.target().to_owned(),
            message: visitor.finish(),
        };

        // Drop-on-full / disconnected: logs are advisory, never block a kernel
        // task waiting on JS-side back-pressure.
        let _ = tx.try_send(record);
    }
}

/// Drain loop (own OS thread): block for one record, opportunistically gather
/// the rest of the burst, then forward the whole batch to JS in one call.
fn drain_loop(rx: Receiver<LogRecord>) {
    let mut buf: Vec<LogRecord> = Vec::with_capacity(64);
    // `recv()` errors only once the (process-global) sender is dropped, which
    // never happens in practice; if the channel is ever torn down, exit the
    // thread cleanly rather than spin.
    while let Ok(first) = rx.recv() {
        buf.push(first);
        while let Ok(next) = rx.try_recv() {
            buf.push(next);
            if buf.len() >= MAX_BATCH {
                break;
            }
        }
        let batch = std::mem::take(&mut buf);
        if let Some(sink) = SINK.get() {
            if let Ok(guard) = sink.lock() {
                if let Some(tsfn) = guard.as_ref() {
                    // NonBlocking: enqueue onto the JS event loop; never park
                    // this thread on JS back-pressure.
                    let _ = tsfn.call(Ok(batch), ThreadsafeFunctionCallMode::NonBlocking);
                }
            }
        }
    }
}

/// Install (idempotently) the kernel→JS log bridge and set its level.
///
/// `callback` is invoked with **an array of [`LogRecord`]s** (`(err, records)`)
/// for each forwarded batch. `level` is one of
/// `off`/`error`/`warn`/`info`/`debug`/`trace` (case-insensitive); unknown
/// values fall back to `warn`.
///
/// Safe to call more than once: the process-global subscriber is installed on
/// the first call only, while every call refreshes the sink + level (last
/// writer wins — see module docs).
#[napi(js_name = "initKernelLogging")]
pub fn init_kernel_logging(
    env: Env,
    mut callback: ThreadsafeFunction<Vec<LogRecord>>,
    level: String,
) -> napi::Result<()> {
    LEVEL.store(parse_level(&level), Ordering::Relaxed);

    // Don't let the bridge keep the Node event loop (and thus the process)
    // alive on its own — logs are advisory; a clean exit must not hang on a
    // live `ThreadsafeFunction`.
    callback.unref(&env)?;

    let sink = SINK.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = sink.lock() {
        *guard = Some(callback);
    }

    INSTALL.call_once(|| {
        let (tx, rx) = sync_channel::<LogRecord>(CHANNEL_CAPACITY);
        // `set` only fails if already set, which `Once` precludes.
        let _ = SENDER.set(tx);

        let _ = std::thread::Builder::new()
            .name("kernel-log-drain".to_owned())
            .spawn(move || drain_loop(rx));

        // Install our layer as the process-global subscriber. Ignore the error
        // if something already set one (e.g. a host that installed its own).
        let subscriber = tracing_subscriber::registry().with(KernelLogLayer);
        let _ = tracing::subscriber::set_global_default(subscriber);
    });

    Ok(())
}

/// Live-retarget the bridge's level (one of
/// `off`/`error`/`warn`/`info`/`debug`/`trace`, case-insensitive).
#[napi(js_name = "setKernelLogLevel")]
pub fn set_kernel_log_level(level: String) {
    LEVEL.store(parse_level(&level), Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_level_maps_known_and_unknown() {
        assert_eq!(parse_level("off"), LEVEL_OFF);
        assert_eq!(parse_level("ERROR"), LEVEL_ERROR);
        assert_eq!(parse_level("Warn"), LEVEL_WARN);
        assert_eq!(parse_level("info"), LEVEL_INFO);
        assert_eq!(parse_level("debug"), LEVEL_DEBUG);
        assert_eq!(parse_level(" trace "), LEVEL_TRACE);
        assert_eq!(parse_level("bogus"), LEVEL_WARN); // conservative fallback
    }

    #[test]
    fn severity_ordering_is_low_is_severe() {
        assert!(severity(&Level::ERROR) < severity(&Level::WARN));
        assert!(severity(&Level::WARN) < severity(&Level::INFO));
        assert!(severity(&Level::INFO) < severity(&Level::DEBUG));
        assert!(severity(&Level::DEBUG) < severity(&Level::TRACE));
    }

    #[test]
    fn should_emit_respects_configured_level() {
        LEVEL.store(LEVEL_INFO, Ordering::Relaxed);
        assert!(should_emit(&Level::ERROR)); // more severe than info → emit
        assert!(should_emit(&Level::INFO));
        assert!(!should_emit(&Level::DEBUG)); // less severe than info → skip

        LEVEL.store(LEVEL_OFF, Ordering::Relaxed);
        assert!(!should_emit(&Level::ERROR)); // off → nothing

        LEVEL.store(LEVEL_TRACE, Ordering::Relaxed);
        assert!(should_emit(&Level::TRACE));

        // Restore default so test-ordering can't leak state.
        LEVEL.store(LEVEL_WARN, Ordering::Relaxed);
    }

    #[test]
    fn visitor_combines_message_and_fields() {
        let mut v = MessageVisitor {
            message: "hello".to_owned(),
            ..Default::default()
        };
        v.push_field("statement_id", "abc-123".to_owned());
        v.push_field("attempt", "2".to_owned());
        let out = v.finish();
        assert_eq!(out, "hello statement_id=abc-123 attempt=2");
    }

    #[test]
    fn visitor_fields_only_when_no_message() {
        let mut v = MessageVisitor::default();
        v.push_field("k", "v".to_owned());
        assert_eq!(v.finish(), "k=v");
    }
}
