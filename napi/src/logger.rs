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

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Mutex, Once};

use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Env;
use napi_derive::napi;
use once_cell::sync::OnceCell;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
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

/// Number of records dropped because the bounded channel was full (or the
/// drain never started). Surfaced via [`kernel_logging_stats`] so a caller
/// can detect log loss during a burst — logs are advisory and never block,
/// but the loss must be *observable*.
static DROPPED: AtomicU64 = AtomicU64::new(0);

const INSTALL_PENDING: u8 = 0;
const INSTALL_OK: u8 = 1;
const INSTALL_FAILED: u8 = 2;

/// Outcome of the one-time subscriber install (see [`init_kernel_logging`]).
/// `INSTALL_FAILED` means another global `tracing` subscriber was already
/// set, or the drain thread could not be spawned — in either case kernel
/// logs will not reach the JS sink, and [`kernel_logging_stats`] reports it.
static INSTALL_STATUS: AtomicU8 = AtomicU8::new(INSTALL_PENDING);

/// Producer end, populated once on first `initKernelLogging` — and only
/// after the drain thread is confirmed running, so a live producer never
/// outlives its consumer.
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

/// Targets the bridge forwards: the `klog!` shim's `databricks::sql::kernel`,
/// kernel module paths (`databricks_sql_kernel::*`), and the napi crate's own
/// `databricks_sql_kernel_napi` breadcrumbs. A raw `starts_with` (not a
/// `::`-segmented `Targets` filter) so the underscore module-path variants
/// match too.
fn is_kernel_target(target: &str) -> bool {
    target.starts_with("databricks")
}

/// The configured kernel level as a `tracing` [`LevelFilter`], used for
/// `max_level_hint` so the process-global level gate tracks the kernel's
/// level instead of being pinned wide-open at `TRACE`.
fn current_level_filter() -> LevelFilter {
    match LEVEL.load(Ordering::Relaxed) {
        LEVEL_OFF => LevelFilter::OFF,
        LEVEL_ERROR => LevelFilter::ERROR,
        LEVEL_WARN => LevelFilter::WARN,
        LEVEL_INFO => LevelFilter::INFO,
        LEVEL_DEBUG => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    }
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

/// Per-layer [`Filter`] that gates the bridge — the fix for the HIGH perf
/// finding. As a *per-layer* filter (not `Layer::enabled`/`register_callsite`,
/// which a `Registry` overrides back to "always interested"), its
/// `callsite_enabled` contributes to the **global** interest cache:
///
/// - foreign callsites (`hyper`, `rustls`, `quinn`, …) cache as
///   [`Interest::never`] → zero-cost no-ops, never dispatched;
/// - kernel callsites return [`Interest::sometimes`] so the runtime level
///   (an atomic flipped by `setKernelLogLevel`) is re-checked in `enabled`;
/// - `max_level_hint` tracks the configured level so the process-global gate
///   isn't pinned wide-open at `TRACE` (paired with `rebuild_interest_cache`).
struct KernelFilter;

/// Per-callsite interest for a target: kernel callsites re-evaluate per
/// event (`sometimes`, for the dynamic level); foreign callsites cache off
/// forever (`never`). Extracted from [`KernelFilter::callsite_enabled`] so
/// it's unit-testable without fabricating a `&'static Metadata`.
fn callsite_interest_for(target: &str) -> Interest {
    if is_kernel_target(target) {
        Interest::sometimes()
    } else {
        Interest::never()
    }
}

impl<S> tracing_subscriber::layer::Filter<S> for KernelFilter {
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        callsite_interest_for(meta.target())
    }

    fn enabled(&self, meta: &Metadata<'_>, _ctx: &Context<'_, S>) -> bool {
        is_kernel_target(meta.target()) && should_emit(meta.level())
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(current_level_filter())
    }
}

impl<S> Layer<S> for KernelLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // The per-layer `KernelFilter` already excludes foreign targets and
        // below-level events, so `on_event` only sees kernel events to forward.
        let meta = event.metadata();

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
        // task waiting on JS-side back-pressure. Count the loss so it's
        // observable via `kernelLoggingStats()` (the channel is drop-newest).
        if tx.try_send(record).is_err() {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Drain loop (own OS thread): block for one record, opportunistically gather
/// the rest of the burst, then forward the whole batch to JS in one call.
fn drain_loop(rx: Receiver<LogRecord>) {
    let mut buf: Vec<LogRecord> = Vec::with_capacity(MAX_BATCH);
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
        // `replace` (not `take`) so the reused buffer keeps its capacity
        // across batches instead of reallocating from zero each drain.
        let batch = std::mem::replace(&mut buf, Vec::with_capacity(MAX_BATCH));
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
    //
    // napi-rs 3 soft-deprecates `unref` in favour of a weak
    // (`Weak = true`) `ThreadsafeFunction`, but that const-generic would
    // have to be threaded through both this argument type and the `SINK`
    // static that stores it, and the `#[napi]` arg-extraction macro does
    // not accept the weak form as a parameter type. `unref` is still fully
    // supported in the 3.x line, so keep it and silence the one warning.
    #[allow(deprecated)]
    callback.unref(&env)?;

    let sink = SINK.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = sink.lock() {
        *guard = Some(callback);
    }

    INSTALL.call_once(|| {
        let (tx, rx) = sync_channel::<LogRecord>(CHANNEL_CAPACITY);

        // Spawn the drain thread FIRST and publish `SENDER` only once it is
        // confirmed running — so a live producer never outlives its consumer
        // (F4). If the OS refuses the thread (EAGAIN/ENOMEM), `rx` is dropped
        // with the failed closure, `SENDER` stays unset (so `on_event`
        // early-returns and produces nothing), and we record the failure.
        match std::thread::Builder::new()
            .name("kernel-log-drain".to_owned())
            .spawn(move || drain_loop(rx))
        {
            Ok(_) => {
                // `set` only fails if already set, which `Once` precludes.
                let _ = SENDER.set(tx);
                // Install our layer as the process-global subscriber. Capture
                // the result: if a subscriber was already installed (e.g. an
                // embedding host set its own), our layer never runs and kernel
                // logs won't reach JS — surface that via the status (F3).
                let subscriber =
                    tracing_subscriber::registry().with(KernelLogLayer.with_filter(KernelFilter));
                let status = match tracing::subscriber::set_global_default(subscriber) {
                    Ok(()) => INSTALL_OK,
                    Err(_) => INSTALL_FAILED,
                };
                INSTALL_STATUS.store(status, Ordering::Release);
            }
            Err(_) => {
                INSTALL_STATUS.store(INSTALL_FAILED, Ordering::Release);
            }
        }
    });

    // Apply the (possibly updated) level to the global max-level gate by
    // re-querying `max_level_hint` / `register_callsite` for every callsite.
    // Cheap and safe to call even before any subscriber is installed.
    tracing::callsite::rebuild_interest_cache();

    Ok(())
}

/// Snapshot of the bridge's runtime state for observability.
///
/// `installed` is `true` only when the process-global subscriber was
/// successfully installed by *this* bridge (and the drain thread started);
/// `false` means another global subscriber was already set or the drain
/// thread could not be spawned, so kernel logs are NOT reaching the JS sink.
/// `dropped` is the cumulative count of records discarded because the
/// bounded channel was full during a burst (drop-newest) — a nonzero,
/// growing value signals the sink can't keep up.
#[napi(object)]
pub struct KernelLoggingStats {
    pub installed: bool,
    pub dropped: i64,
}

/// Return the bridge's [`KernelLoggingStats`]. Safe to call before
/// `initKernelLogging` (reports `installed: false`, `dropped: 0`).
#[napi(js_name = "kernelLoggingStats")]
pub fn kernel_logging_stats() -> KernelLoggingStats {
    KernelLoggingStats {
        installed: INSTALL_STATUS.load(Ordering::Acquire) == INSTALL_OK,
        // Saturating into the JS-safe `i64`; a real drop count never
        // approaches `i64::MAX`.
        dropped: DROPPED.load(Ordering::Relaxed).min(i64::MAX as u64) as i64,
    }
}

/// Live-retarget the bridge's level (one of
/// `off`/`error`/`warn`/`info`/`debug`/`trace`, case-insensitive).
#[napi(js_name = "setKernelLogLevel")]
pub fn set_kernel_log_level(level: String) {
    LEVEL.store(parse_level(&level), Ordering::Relaxed);
    // The level feeds `max_level_hint`, which `tracing` caches; rebuild so
    // the new level takes effect on the global gate (and re-confirms kernel
    // callsites' interest) rather than lagging until the next callsite
    // registers.
    tracing::callsite::rebuild_interest_cache();
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

    // ── F1: callsite-filtering regression guards (the HIGH finding) ──
    //
    // The fix is the per-layer `KernelFilter`. These assert its decision
    // logic *directly* (rather than through `with_default` + the
    // process-global callsite-interest cache / `LevelFilter::current()`
    // atomic, which is shared across the parallel test binary and so makes
    // macro-based assertions order-dependent). The "before" state — the
    // unfiltered layer forcing a global TRACE gate and dispatching every
    // foreign callsite — was confirmed empirically while developing the fix.

    #[test]
    fn foreign_callsites_cache_off_kernel_callsites_reevaluate() {
        // The crux of the perf fix: foreign crate callsites (`hyper`,
        // `rustls`, …) are `never` — cached off so they never dispatch —
        // while kernel callsites are `sometimes` so the dynamic level is
        // re-checked per event.
        assert!(callsite_interest_for("hyper::client::pool").is_never());
        assert!(callsite_interest_for("rustls::conn").is_never());
        assert!(callsite_interest_for("quinn::connection").is_never());
        assert!(callsite_interest_for("databricks::sql::kernel").is_sometimes());
        assert!(callsite_interest_for("databricks_sql_kernel::session").is_sometimes());
        assert!(callsite_interest_for("databricks_sql_kernel_napi").is_sometimes());
    }

    #[test]
    fn max_level_hint_tracks_level_and_is_not_pinned_to_trace() {
        // The other half of the fix: the global max-level gate tracks the
        // configured kernel level instead of the default `None` (which
        // `tracing` treats as TRACE, re-enabling every crate's dispatch).
        let saved = LEVEL.load(Ordering::Relaxed);
        LEVEL.store(LEVEL_WARN, Ordering::Relaxed);
        assert_eq!(current_level_filter(), LevelFilter::WARN);
        LEVEL.store(LEVEL_OFF, Ordering::Relaxed);
        assert_eq!(current_level_filter(), LevelFilter::OFF);
        LEVEL.store(LEVEL_DEBUG, Ordering::Relaxed);
        assert_eq!(current_level_filter(), LevelFilter::DEBUG);
        LEVEL.store(LEVEL_TRACE, Ordering::Relaxed);
        assert_eq!(current_level_filter(), LevelFilter::TRACE);
        LEVEL.store(saved, Ordering::Relaxed);
    }

    #[test]
    fn enabled_decision_requires_kernel_target_and_level() {
        // The `Filter::enabled` decision (target prefix AND level), tested
        // as the pure predicate it delegates to.
        let saved = LEVEL.load(Ordering::Relaxed);
        LEVEL.store(LEVEL_INFO, Ordering::Relaxed);
        let enabled = |target: &str, level: &Level| is_kernel_target(target) && should_emit(level);
        assert!(enabled("databricks::sql::kernel", &Level::INFO));
        assert!(enabled("databricks::sql::kernel", &Level::ERROR));
        assert!(!enabled("databricks::sql::kernel", &Level::DEBUG)); // below level
        assert!(!enabled("hyper::client", &Level::ERROR)); // foreign target
        LEVEL.store(saved, Ordering::Relaxed);
    }

    #[test]
    fn stats_default_before_install() {
        // Safe to query before `initKernelLogging`: not installed, no drops.
        let stats = kernel_logging_stats();
        assert!(!stats.installed);
        assert_eq!(stats.dropped, 0);
    }

    // F10: drift guard. The bridge routes on the `databricks` prefix; the
    // kernel stamps every `klog!` with `KERNEL_LOG_TARGET`. If that constant
    // ever changed to a non-`databricks` prefix, kernel logs would silently
    // stop reaching the sink — this pins the two together (the kernel side
    // has its own `klog::tests` guard on the exact string).
    #[test]
    fn filter_prefix_still_matches_kernel_emit_target() {
        assert!(
            is_kernel_target(databricks_sql_kernel::KERNEL_LOG_TARGET),
            "napi log filter prefix must still match the kernel's emit target ({})",
            databricks_sql_kernel::KERNEL_LOG_TARGET
        );
    }

    // F7: exercise the `Visit` dispatch (`record_debug` / `record_str` and
    // the `message`-field special-casing) through a *real* `tracing` event,
    // which the direct `push_field`/`finish` tests bypass. Uses a local
    // capture layer (no global install, no channel) so it's deterministic.
    #[test]
    fn visitor_renders_message_str_and_debug_fields_from_real_event() {
        use std::sync::Mutex;
        static CAP: Mutex<Option<String>> = Mutex::new(None);

        struct CaptureLayer;
        impl<S> Layer<S> for CaptureLayer
        where
            S: Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
                let mut v = MessageVisitor::default();
                event.record(&mut v);
                *CAP.lock().unwrap() = Some(v.finish());
            }
        }

        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(CaptureLayer),
            || {
                // `msg` → `message` field via record_debug (rendered, unquoted);
                // `name` (&str) → record_str; `attempt` (i64) → record_debug.
                tracing::error!(target: "databricks::sql::kernel", name = "abc", attempt = 2, "boom");
            },
        );

        let got = CAP.lock().unwrap().take().expect("event captured");
        assert!(got.starts_with("boom"), "message first, got: {got}");
        assert!(got.contains("name=abc"), "record_str field, got: {got}");
        assert!(got.contains("attempt=2"), "record_debug field, got: {got}");
    }
}
